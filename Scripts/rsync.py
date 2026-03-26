#!/usr/bin/env python3
"""
rsync.py — Arch Linux mirror sync script

Features:
  - Fetches mirror list from archlinux.org/mirrors/status/json/
  - Filters rsync-only mirrors with 100% completion
  - Ranks by lowest delay, falls back through candidates on failure
  - Compares lastupdate timestamp before running a full rsync
  - Adaptive sync timeout: tracks historical sync durations and kills
    a stalled rsync if it exceeds mean + N*stddev
  - Single-instance locking via a lockfile + fcntl
  - Cleans up stale .~tmp~ dirs left by older rsync versions
  - Persists sync-time history across runs (JSON sidecar file)
"""

import fcntl
import json
import logging
import math
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# CONFIG — edit these
# ---------------------------------------------------------------------------

# Local directory where the repo will be stored, e.g. "/srv/repo/archlinux"
TARGET: str = "/var/www/html"

# Lockfile path
LOCK_FILE: str = "/var/lock/syncrepo.lck"

# File to persist sync-time history (stored alongside this script if not set)
HISTORY_FILE: str = "/var/lib/syncrepo_history.json"

# Bandwidth limit passed to rsync (KiB/s). 0 = unlimited.
BWLIMIT: int = 0

# Stage 1: how many mirrors (by sync delay) to TTFB-test
TTFB_POOL_SIZE: int = 100

# Mirrors with a sync delay greater than this are excluded entirely,
# regardless of how good their TTFB or speed looks.
# 3600 = 1 hour; mirrors older than this risk serving stale packages.
MAX_MIRROR_DELAY_SECS: int = 300

# Stage 2: how many top-TTFB mirrors to speed-test with curl
SPEED_TEST_POOL_SIZE: int = 10

# Stage 3: how many top-speed mirrors to actually try for syncing
MAX_MIRROR_CANDIDATES: int = 5

# Mirror status JSON endpoint
MIRROR_STATUS_URL: str = "https://archlinux.org/mirrors/status/json/"

# rsync flags
RSYNC_FLAGS: list[str] = [
    "-rlptH",
    "--safe-links", 
    "--delete",
    "--delay-updates"
    ]
# Adaptive timeout: kill rsync if it runs longer than mean + STDDEV_FACTOR * stddev
# Set to 0 to disable adaptive timeout (a hard floor is always applied via --timeout).
STDDEV_FACTOR: float = 2.5

# Minimum number of historical samples before adaptive timeout kicks in
MIN_HISTORY_SAMPLES: int = 3

# Absolute floor / ceiling for the adaptive timeout (seconds)
ADAPTIVE_TIMEOUT_MIN: int = 300    # 5 minutes
ADAPTIVE_TIMEOUT_MAX: int = 7200   # 2 hours

# Maximum sync-time samples to keep per mirror URL
MAX_HISTORY_PER_MIRROR: int = 20

# ---------------------------------------------------------------------------
# Speed test config
# ---------------------------------------------------------------------------

# Speed test uses a real rsync transfer to /dev/null — same protocol and
# code path as the actual sync, so the measurement is honest.
#
# extra.files.tar.gz is the test target:
#   - Always present at this exact path on every Arch mirror
#   - ~50 MB — large enough for a stable reading, small enough to be fast
#   - Not a symlink, not excluded by our rsync flags
#   - Transfers in ~5s on a 10 MB/s link
SPEED_TEST_FILE: str = "extra/os/x86_64/extra.files.tar.gz"

# Hard wall-clock limit for each speed-test rsync (seconds).
# If a mirror hasn't finished transferring the test file within this time
# we kill it, compute MB/s from however much arrived, and move on.
SPEED_TEST_TIMEOUT: float = 30.0

# Mirrors that deliver less than this (MB/s) are skipped outright.
SPEED_TEST_MIN_MBPS: float = 1.0

# ---------------------------------------------------------------------------
# Mirror cache (fast-path)
# ---------------------------------------------------------------------------

# File where the last successfully-used mirror URL and election timestamp
# are stored between runs.
MIRROR_CACHE_FILE: str = "/var/lib/syncrepo_mirror_cache.json"

# If the cached mirror passes a speed test above this threshold (MB/s) it
# is reused directly — the full TTFB + speed-election pipeline is skipped.
# Set higher than SPEED_TEST_MIN_MBPS so we only shortcut on a genuinely
# fast mirror.  Tune to taste (e.g. 5.0 for lenient, 15.0 for strict).
MIRROR_CACHE_MIN_MBPS: float = 10.0

# Re-run the full election after this many hours even if the cached mirror
# keeps passing the speed check.  Ensures we don't stick to a mirror that
# has quietly become mediocre compared to newer/faster options.
MIRROR_CACHE_TTL_HOURS: float = 24.0

# If the mirror's remote lastsync timestamp is older than this many seconds
# relative to now, skip the speed test and go straight to syncing.
# This avoids wasting ~5s on a speed test when we already know data is stale.
MIRROR_STALE_THRESHOLD_SECS: int = 300  # 5 minutes

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("syncrepo")


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------

_lock_fh = None  # kept open so the lock is held for the process lifetime


def acquire_lock() -> None:
    global _lock_fh
    _lock_fh = open(LOCK_FILE, "w")
    try:
        fcntl.flock(_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error("Another instance is already running (lock: %s). Exiting.", LOCK_FILE)
        sys.exit(1)
    _lock_fh.write(str(os.getpid()))
    _lock_fh.flush()
    log.info("Lock acquired.")


# ---------------------------------------------------------------------------
# Mirror selection
# ---------------------------------------------------------------------------

def fetch_mirror_list() -> list[dict]:
    """Download and return the Arch Linux mirror status JSON."""
    log.info("Fetching mirror list from %s", MIRROR_STATUS_URL)
    with urllib.request.urlopen(MIRROR_STATUS_URL, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    return data.get("urls", [])


def select_rsync_mirrors(mirrors: list[dict]) -> list[dict]:
    """
    Filter mirrors to:
      - protocol == 'rsync'
      - completion_pct == 1.0
      - active == True
    Then sort by delay ascending (lowest = most up-to-date).
    """
    candidates = [
        m for m in mirrors
        if m.get("protocol") == "rsync"
        and m.get("completion_pct") == 1.0
        and m.get("active", False)
        and m.get("delay") is not None
        and m.get("isos", False)
        and m["delay"] <= MAX_MIRROR_DELAY_SECS
    ]
    candidates.sort(key=lambda m: m["delay"])
    log.info(
        "Found %d valid rsync mirrors (delay <= %ds, with ISOs); taking top %d for TTFB test.",
        len(candidates), MAX_MIRROR_DELAY_SECS, TTFB_POOL_SIZE,
    )
    return candidates[:TTFB_POOL_SIZE]


# ---------------------------------------------------------------------------
# lastupdate check
# ---------------------------------------------------------------------------

def _http_get(rsync_url: str, filename: str) -> str | None:
    """
    Fetch a small text file from a mirror over HTTPS (falls back to HTTP).
    rsync_url is the base rsync:// URL for the mirror.
    """
    base = rsync_url.rstrip("/")
    for scheme in ("https", "http"):
        url = base.replace("rsync://", f"{scheme}://", 1) + f"/{filename}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                return resp.read().decode().strip()
        except Exception:
            continue
    return None


def read_local_file(filename: str) -> str | None:
    path = Path(TARGET) / filename
    return path.read_text().strip() if path.exists() else None


def needs_sync(mirror_url: str) -> bool:
    """
    Return True if a sync is needed.

    Logic (both checks must pass to skip sync):
      1. lastupdate local == lastupdate remote  →  upstream content unchanged
      2. lastsync   local == lastsync   remote  →  mirror has nothing new for us

    If either file is missing or unreadable we err on the side of syncing.
    """
    # --- lastupdate ---
    local_lastupdate = read_local_file("lastupdate")
    if local_lastupdate is None:
        log.info("No local lastupdate; sync required.")
        return True

    remote_lastupdate = _http_get(mirror_url, "lastupdate")
    if remote_lastupdate is None:
        log.warning("Could not fetch remote lastupdate; proceeding with sync.")
        return True

    if local_lastupdate != remote_lastupdate:
        log.info(
            "lastupdate mismatch — local: %s, remote: %s. Sync required.",
            local_lastupdate, remote_lastupdate,
        )
        return True

    # lastupdate matches — now check lastsync to catch partially-synced mirrors
    # and cases where our local tree lags behind the mirror's last pull.
    local_lastsync = read_local_file("lastsync")
    if local_lastsync is None:
        log.info("No local lastsync; sync required.")
        return True

    remote_lastsync = _http_get(mirror_url, "lastsync")
    if remote_lastsync is None:
        log.warning("Could not fetch remote lastsync; proceeding with sync.")
        return True

    if local_lastsync != remote_lastsync:
        log.info(
            "lastsync mismatch — local: %s, remote: %s. Mirror has newer data; sync required.",
            local_lastsync, remote_lastsync,
        )
        return True

    log.info(
        "lastupdate (%s) and lastsync (%s) both match. No sync needed.",
        local_lastupdate, local_lastsync,
    )
    return False


# ---------------------------------------------------------------------------
# Sync-time history (adaptive timeout)
# ---------------------------------------------------------------------------

def load_history() -> dict:
    path = Path(HISTORY_FILE)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def save_history(history: dict) -> None:
    try:
        Path(HISTORY_FILE).write_text(json.dumps(history, indent=2))
    except Exception as exc:
        log.warning("Could not save history file: %s", exc)


# ---------------------------------------------------------------------------
# Mirror cache (fast-path helpers)
# ---------------------------------------------------------------------------

def load_mirror_cache() -> dict:
    """Return the cached mirror state, or {} if absent/corrupt."""
    try:
        return json.loads(Path(MIRROR_CACHE_FILE).read_text())
    except Exception:
        return {}


def save_mirror_cache(url: str) -> None:
    """Persist url as the current best mirror with an election timestamp."""
    try:
        Path(MIRROR_CACHE_FILE).write_text(json.dumps({
            "url": url,
            "elected_at": time.time(),
        }, indent=2))
    except Exception as exc:
        log.warning("Could not save mirror cache: %s", exc)


def _remote_lastsync_age(url: str) -> float | None:
    """
    Fetch the remote lastsync timestamp and return how many seconds ago it was.
    Returns None if the file cannot be fetched or parsed.
    """
    raw = _http_get(url, "lastsync")
    if raw is None:
        return None
    try:
        return time.time() - float(raw.strip())
    except ValueError:
        return None


def cached_mirror_still_good() -> str | None:
    """
    Fast-path check. Decision tree:

      1. No cache / TTL expired                     → None (full re-election)
      2. Speed test fails (< MIRROR_CACHE_MIN_MBPS) → None (full re-election)
      3. Speed test passes + lastsync stale          → None (full re-election,
         (> MIRROR_STALE_THRESHOLD_SECS)                mirror has fallen behind)
      4. Speed test passes + lastsync fresh          → url  (fast path, sync now)

    Speed test is always mandatory — it is the gate that determines whether
    we trust this mirror at all.  The lastsync age is a secondary check that
    ensures we don't keep syncing from a mirror that is fast but out of date.
    """
    cache = load_mirror_cache()
    url = cache.get("url")
    elected_at = cache.get("elected_at", 0)

    if not url:
        log.info("No cached mirror; running full election.")
        return None

    age_hours = (time.time() - elected_at) / 3600
    if age_hours >= MIRROR_CACHE_TTL_HOURS:
        log.info(
            "Cached mirror %s is %.1fh old (TTL=%.1fh); re-electing.",
            url, age_hours, MIRROR_CACHE_TTL_HOURS,
        )
        return None

    log.info("Cached mirror: %s (elected %.1fh ago) — speed-testing …", url, age_hours)

    # Speed test is always first and always mandatory
    mbps = speed_test_mirror(url)
    if mbps < MIRROR_CACHE_MIN_MBPS:
        log.info(
            "Cached mirror too slow: %.2f MB/s < %.1f MB/s. Re-electing.",
            mbps, MIRROR_CACHE_MIN_MBPS,
        )
        return None

    log.info("Speed test passed: %.2f MB/s. Checking lastsync age …", mbps)

    # Mirror is fast — now check if its data is fresh enough to be worth syncing from
    lastsync_age = _remote_lastsync_age(url)
    if lastsync_age is None:
        log.warning("Could not fetch remote lastsync; re-electing to be safe.")
        return None

    if lastsync_age > MIRROR_STALE_THRESHOLD_SECS:
        log.info(
            "Mirror lastsync is %.0fs old (> %ds threshold) — mirror is stale. Re-electing.",
            lastsync_age, MIRROR_STALE_THRESHOLD_SECS,
        )
        return None

    log.info(
        "Mirror lastsync is %.0fs old (<= %ds). Fast path OK.",
        lastsync_age, MIRROR_STALE_THRESHOLD_SECS,
    )
    return url


def record_sync_duration(history: dict, mirror_url: str, duration: float) -> None:
    history.setdefault(mirror_url, [])
    history[mirror_url].append(duration)
    # Trim to last N samples
    history[mirror_url] = history[mirror_url][-MAX_HISTORY_PER_MIRROR:]


def compute_adaptive_timeout(history: dict, mirror_url: str) -> int | None:
    """
    Return an adaptive timeout (seconds) based on past sync durations for this
    mirror, or None if there is insufficient history.
    """
    if STDDEV_FACTOR == 0:
        return None
    samples = history.get(mirror_url, [])
    if len(samples) < MIN_HISTORY_SAMPLES:
        return None
    mean = sum(samples) / len(samples)
    variance = sum((x - mean) ** 2 for x in samples) / len(samples)
    stddev = math.sqrt(variance)
    timeout = int(mean + STDDEV_FACTOR * stddev)
    timeout = max(ADAPTIVE_TIMEOUT_MIN, min(ADAPTIVE_TIMEOUT_MAX, timeout))
    log.info(
        "Adaptive timeout for %s: %.0fs (mean=%.0fs, stddev=%.0fs, factor=%.1f)",
        mirror_url, timeout, mean, stddev, STDDEV_FACTOR,
    )
    return timeout


# ---------------------------------------------------------------------------
# TTFB testing (parallel)
# ---------------------------------------------------------------------------

def _ttfb_one(mirror: dict) -> tuple[dict, float]:
    """
    Measure time-to-first-byte for one mirror via a curl HEAD request
    against the ISO URL.  Returns (mirror, ttfb_seconds); uses 9999.0 on
    failure so failed mirrors sort to the back.
    """
    for http_url in _rsync_to_http_url(mirror["url"].rstrip("/") + "/", SPEED_TEST_FILE):
        cmd = [
            "curl",
            "--silent",
            "--fail",
            "--head",                           # HEAD only — no body transfer
            "--location",
            "--max-time", "8",                  # give up after 8s per mirror
            "--write-out", "%{time_starttransfer}",  # TTFB in seconds
            "--output", "/dev/null",
            http_url,
        ]
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=12,
            )
            if result.returncode == 0:
                ttfb = float(result.stdout.strip())
                if ttfb > 0:
                    return mirror, ttfb
        except Exception:
            pass
    return mirror, 9999.0


def rank_mirrors_by_ttfb(candidates: list[dict]) -> list[dict]:
    """
    Run TTFB checks against all candidates in parallel (ThreadPoolExecutor),
    log a sorted table, and return mirrors ordered by TTFB ascending.
    Mirrors that fail entirely (ttfb=9999) are dropped.
    """
    import concurrent.futures

    log.info(
        "TTFB-testing %d mirrors in parallel (HEAD %s) …",
        len(candidates), SPEED_TEST_FILE,
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        futures = {pool.submit(_ttfb_one, m): m for m in candidates}
        results: list[tuple[dict, float]] = []
        for f in concurrent.futures.as_completed(futures):
            mirror, ttfb = f.result()
            results.append((mirror, ttfb))

    results.sort(key=lambda t: t[1])

    col = 58
    log.info("  %-*s  %s", col, "Mirror", "TTFB")
    log.info("  %s  %s", "-" * col, "-" * 8)
    for mirror, ttfb in results:
        url = mirror["url"].rstrip("/") + "/"
        if ttfb >= 9999:
            log.info("  %-*s  UNREACHABLE", col, url)
        else:
            log.info("  %-*s  %.3fs", col, url, ttfb)

    reachable = [m for m, t in results if t < 9999]
    log.info(
        "%d/%d mirrors reachable; taking top %d for speed test.",
        len(reachable), len(candidates), SPEED_TEST_POOL_SIZE,
    )
    return reachable[:SPEED_TEST_POOL_SIZE]


# ---------------------------------------------------------------------------
# Speed testing (rsync-native)
# ---------------------------------------------------------------------------

def _rsync_to_http_url(rsync_url: str, path: str) -> list[str]:
    """Build [https, http] URLs from an rsync:// base — used by TTFB tests."""
    base = rsync_url.rstrip("/")
    return [
        base.replace("rsync://", "https://", 1) + "/" + path,
        base.replace("rsync://", "http://",  1) + "/" + path,
    ]


def speed_test_mirror(mirror_url: str) -> float:
    """
    Measure real rsync throughput by transferring SPEED_TEST_FILE to
    /dev/null and timing the transfer.

    Why rsync and not curl:
      - rsync has its own connection overhead, delta-transfer negotiation,
        and checksum phases — HTTP throughput does not predict rsync speed.
      - This uses identical flags to the real sync so the number is honest.

    The file (extra.files.tar.gz, ~50 MB) is present on every mirror at a
    fixed path, is not a symlink, and is not excluded by our rsync flags.

    If the transfer is not done within SPEED_TEST_TIMEOUT seconds we
    SIGTERM the process and compute MB/s from however many bytes arrived
    (reported by rsync --stats on stdout).

    Returns MB/s, or 0.0 on any failure.
    """
    url = mirror_url.rstrip("/") + "/" + SPEED_TEST_FILE
    cmd = [
        "rsync",
        "--no-motd",
        "--stats",       # prints "Total bytes received: N" at the end
        "--timeout=10",  # stall guard per rsync (no data for 10s → exit)
        url,
        "/dev/null",
    ]

    start = time.monotonic()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            stdout, _ = proc.communicate(timeout=SPEED_TEST_TIMEOUT)
            completed = True
        except subprocess.TimeoutExpired:
            proc.terminate()
            try:
                stdout, _ = proc.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, _ = proc.communicate()
            completed = False
    except Exception as exc:
        log.debug("speed_test_mirror: failed for %s: %s", mirror_url, exc)
        return 0.0

    elapsed = time.monotonic() - start
    if elapsed <= 0:
        return 0.0

    # Parse "Total bytes received: 52,428,800" from --stats output
    bytes_received = 0
    for line in stdout.splitlines():
        line = line.strip()
        if line.startswith("Total bytes received"):
            try:
                bytes_received = int(line.split(":")[1].strip().replace(",", ""))
            except (IndexError, ValueError):
                pass
            break

    if bytes_received == 0:
        log.debug(
            "speed_test_mirror: no bytes received from %s (exit=%d, timeout=%s)",
            mirror_url, proc.returncode, not completed,
        )
        return 0.0

    return (bytes_received / elapsed) / (1024 * 1024)


def rank_mirrors_by_speed(candidates: list[dict]) -> list[tuple[dict, float]]:
    """
    rsync-speed-test every candidate and return them sorted fastest-first,
    dropping any below SPEED_TEST_MIN_MBPS.
    """
    log.info(
        "Speed-testing %d mirrors via rsync (%s, timeout %.0fs) …",
        len(candidates), SPEED_TEST_FILE, SPEED_TEST_TIMEOUT,
    )
    col = 58
    log.info("  %-*s  %s", col, "Mirror", "Speed")
    log.info("  %s  %s", "-" * col, "-" * 12)

    results: list[tuple[dict, float]] = []
    for mirror in candidates:
        url = mirror["url"].rstrip("/") + "/"
        mbps = speed_test_mirror(url)
        if mbps >= SPEED_TEST_MIN_MBPS:
            log.info("  %-*s  %.2f MB/s", col, url, mbps)
            results.append((mirror, mbps))
        else:
            log.info(
                "  %-*s  SKIP (%.2f MB/s < min %.1f MB/s)",
                col, url, mbps, SPEED_TEST_MIN_MBPS,
            )

    results.sort(key=lambda t: t[1], reverse=True)

    if results:
        log.info("Best mirror: %s @ %.2f MB/s", results[0][0]["url"], results[0][1])
    else:
        log.warning(
            "No mirrors passed the %.1f MB/s threshold; falling back to TTFB order.",
            SPEED_TEST_MIN_MBPS,
        )

    return results


# ---------------------------------------------------------------------------
# rsync execution
# ---------------------------------------------------------------------------

def build_rsync_cmd(source_url: str) -> list[str]:
    cmd = ["rsync"] + RSYNC_FLAGS
    if BWLIMIT > 0:
        cmd.append(f"--bwlimit={BWLIMIT}")
    # Verbose progress if running interactively
    try:
        if os.isatty(sys.stdout.fileno()):
            cmd += ["-h", "-v", "--progress"]
        else:
            cmd.append("--quiet")
    except Exception:
        cmd.append("--quiet")
    cmd += [source_url, TARGET]
    return cmd


def run_rsync(source_url: str, timeout_secs: int | None) -> bool:
    """
    Run rsync. If timeout_secs is set and rsync exceeds it, kill the process
    and return False. Returns True on success.
    """
    cmd = build_rsync_cmd(source_url)
    log.info("Running: %s", " ".join(cmd))
    start = time.monotonic()

    proc = subprocess.Popen(cmd)
    try:
        proc.wait(timeout=timeout_secs)
    except subprocess.TimeoutExpired:
        elapsed = time.monotonic() - start
        log.warning(
            "rsync exceeded adaptive timeout of %ds after %.0fs — killing process.",
            timeout_secs, elapsed,
        )
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        return False

    elapsed = time.monotonic() - start

    if proc.returncode == 0:
        log.info("rsync completed successfully in %.0fs.", elapsed)
        return True
    else:
        log.warning("rsync exited with code %d after %.0fs.", proc.returncode, elapsed)
        return False


def sync_lastsync(source_url: str) -> None:
    """Sync only the lastsync file (keeps mirror stats updated on Arch infra)."""
    cmd = [
        "rsync", "--no-motd", "--quiet",
        f"{source_url.rstrip('/')}/lastsync",
        f"{TARGET}/lastsync",
    ]
    try:
        subprocess.run(cmd, timeout=60, check=False)
    except Exception as exc:
        log.warning("Could not sync lastsync file: %s", exc)


# ---------------------------------------------------------------------------
# Cleanup stale tmp dirs (rsync < 3.2.3 bug)
# ---------------------------------------------------------------------------

def cleanup_tmp_dirs() -> None:
    target_path = Path(TARGET)
    count = 0
    for p in target_path.rglob(".~tmp~"):
        try:
            if p.is_dir():
                import shutil
                shutil.rmtree(p)
                count += 1
        except Exception as exc:
            log.warning("Could not remove %s: %s", p, exc)
    if count:
        log.info("Removed %d stale .~tmp~ director%s.", count, "ies" if count != 1 else "y")


# ---------------------------------------------------------------------------
# Sync loop (shared by fast path and full election)
# ---------------------------------------------------------------------------

def _try_sync(ordered_urls: list[str], history: dict, on_failure_clear_cache: bool) -> None:
    """
    Try each URL in order.  On first success: save mirror cache + history.
    If on_failure_clear_cache is True (fast-path call) and every URL fails,
    clear the cache so the next run triggers a full re-election, then exit(1).
    """
    synced = False
    for url in ordered_urls:
        log.info("Trying mirror: %s", url)

        # --- lastupdate + lastsync check ---
        if not needs_sync(url):
            sync_lastsync(url)
            log.info("Nothing to do.")
            save_mirror_cache(url)
            synced = True
            break

        # --- Adaptive timeout ---
        adaptive_timeout = compute_adaptive_timeout(history, url)

        # --- Run rsync ---
        start = time.monotonic()
        success = run_rsync(url, timeout_secs=adaptive_timeout)
        duration = time.monotonic() - start

        if success:
            record_sync_duration(history, url, duration)
            save_history(history)
            sync_lastsync(url)
            save_mirror_cache(url)
            log.info("Sync complete in %.0fs via %s", duration, url)
            synced = True
            break
        else:
            log.warning("Mirror %s failed or timed out. Trying next …", url)
            record_sync_duration(history, url, duration)
            save_history(history)

    if not synced:
        if on_failure_clear_cache:
            log.warning("Cached mirror failed; clearing cache so next run re-elects.")
            try:
                Path(MIRROR_CACHE_FILE).unlink(missing_ok=True)
            except Exception:
                pass
        log.error("All %d mirror(s) failed. Giving up.", len(ordered_urls))
        sys.exit(1)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    acquire_lock()

    Path(TARGET).mkdir(parents=True, exist_ok=True)
    cleanup_tmp_dirs()

    history = load_history()

    # -------------------------------------------------------------------------
    # Fast path: reuse the cached mirror if it is recent and still fast enough
    # -------------------------------------------------------------------------
    fast_url = cached_mirror_still_good()
    if fast_url:
        _try_sync([fast_url], history, on_failure_clear_cache=True)
        return

    # -------------------------------------------------------------------------
    # Full election: fetch list → TTFB filter → speed test → sync
    # -------------------------------------------------------------------------
    log.info("Running full mirror election …")

    # --- 1. Fetch mirror list ---
    try:
        all_mirrors = fetch_mirror_list()
    except Exception as exc:
        log.error("Failed to fetch mirror list: %s", exc)
        sys.exit(1)

    # --- 2. Filter by delay/completion → top TTFB_POOL_SIZE candidates ---
    delay_ranked = select_rsync_mirrors(all_mirrors)
    if not delay_ranked:
        log.error("No suitable rsync mirrors found. Exiting.")
        sys.exit(1)

    # --- 3. TTFB test all candidates in parallel → top SPEED_TEST_POOL_SIZE ---
    ttfb_ranked = rank_mirrors_by_ttfb(delay_ranked)
    if not ttfb_ranked:
        log.warning("All mirrors unreachable via TTFB test; falling back to delay order.")
        ttfb_ranked = delay_ranked[:SPEED_TEST_POOL_SIZE]

    # --- 4. Speed-test the TTFB survivors → top MAX_MIRROR_CANDIDATES ---
    speed_ranked = rank_mirrors_by_speed(ttfb_ranked)
    if not speed_ranked:
        log.warning("No mirrors passed the speed threshold; falling back to TTFB order.")
        ordered_urls = [m["url"].rstrip("/") + "/" for m in ttfb_ranked[:MAX_MIRROR_CANDIDATES]]
    else:
        ordered_urls = [m["url"].rstrip("/") + "/" for m, _ in speed_ranked[:MAX_MIRROR_CANDIDATES]]

    # --- 5. Try mirrors fastest-first, cache the winner ---
    _try_sync(ordered_urls, history, on_failure_clear_cache=False)


if __name__ == "__main__":
    main()
