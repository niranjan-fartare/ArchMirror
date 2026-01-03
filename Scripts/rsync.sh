#!/bin/bash

find /var/www/html -type d -name "*tmp*" -exec rm -r {} +

packages=("bc" "jq")

# Ensure script is run as root
if [[ $EUID -ne 0 ]]; then
    echo "Please run as root (sudo)."
    exit 1
fi

echo "Checking required packages..."

for pkg in "${packages[@]}"; do
    if dpkg -s "$pkg" >/dev/null 2>&1; then
        #echo "[OK] $pkg is already installed."
    else
        echo "[MISSING] $pkg is not installed. Installing..."
        apt update -y
        apt install -y "$pkg"

        if dpkg -s "$pkg" >/dev/null 2>&1; then
            echo "[DONE] $pkg installed successfully."
        else
            echo "[ERROR] Failed to install $pkg."
        fi
    fi
done

echo "All checks complete."

# Function to get the fastest mirror from Arch Linux API
get_fastest_mirror() {
    local api_url="https://archlinux.org/mirrors/status/tier/1/json/"
    local max_delay=600  # 10 mins max delay
    local top_n=5       # Test top 10 mirrors
    local temp_dir="/tmp/mirror_speed_test"
    
    echo "Fetching mirror list from Arch Linux API..." >&2
    
    # Fetch and parse mirror data
    local mirror_data
    mirror_data=$(curl -s "$api_url" 2>/dev/null)
    
    if [ $? -ne 0 ] || [ -z "$mirror_data" ]; then
        echo "Error: Failed to fetch mirror data from API" >&2
        return 1
    fi
    
    # Extract rsync mirrors with low delay using jq
    local mirrors
    mirrors=$(echo "$mirror_data" | jq -r --arg max_delay "$max_delay" '
        .urls[] | 
        select(.protocol == "rsync" and 
               .active == true and 
               .completion_pct == 1.0 and 
               .last_sync != null and 
               .delay != null and 
               .delay <= ($max_delay | tonumber)) |
        "\(.delay)|\(.url)|\(.country)"
    ' | sort -n | head -n "$top_n")
    
    if [ -z "$mirrors" ]; then
        echo "Error: No suitable rsync mirrors found" >&2
        return 1
    fi
    
    echo "Found $(echo "$mirrors" | wc -l) suitable mirrors. Testing connection speed..." >&2
    
    mkdir -p "$temp_dir"
    
    local best_mirror=""
    local best_time=999999
    
    # Test each mirror
    while IFS='|' read -r delay url country; do
        local mirror_name=$(echo "$url" | awk -F/ '{print $3}')
        echo "Testing $country - $mirror_name (delay: ${delay}s)..." >&2
        
        # Test connection speed with rsync --list-only
        local start_time=$(date +%s.%N)
        timeout 10 rsync --list-only --timeout=10 "${url}iso/latest/" > /dev/null 2>&1
        local rsync_status=$?
        local end_time=$(date +%s.%N)
        
        if [ $rsync_status -eq 0 ]; then
            local elapsed=$(echo "$end_time - $start_time" | bc)
            echo "  ✓ Success in ${elapsed}s" >&2
            
            # Check if this is the fastest
            local is_faster=$(echo "$elapsed < $best_time" | bc)
            if [ "$is_faster" -eq 1 ]; then
                best_time=$elapsed
                best_mirror=$url
                echo "  → New fastest mirror!" >&2
            fi
        else
            echo "  ✗ Failed or timeout" >&2
        fi
    done <<< "$mirrors"
    
    rm -rf "$temp_dir"
    
    if [ -z "$best_mirror" ]; then
        echo "Error: No accessible mirrors found" >&2
        return 1
    fi
    
    echo "Selected fastest mirror (${best_time}s response): $best_mirror" >&2
    echo "$best_mirror"
    return 0
}

(
    flock -n 200 || { echo "Lock acquisition failed. Another sync process is running."; exit 1; }
    echo "Lock acquired. Starting mirror sync process."
    
    # Fallback mirrors in case API fails
    FALLBACK_MIRRORS=(
        "rsync://frankfurt.mirror.pkgbuild.com/packages/"
        "rsync://mirror.moson.org/arch/"
        "rsync://mirror.peeres-telecom.fr/archlinux/"
        "rsync://archlinux.thaller.ws/archlinux/"
        "rsync://rsync.osbeck.com/archlinux/"
        "rsync://arch.mirror.constant.com/archlinux/"
    )
    
    LOCAL_MIRROR="/var/www/html"
    
    # Try to get fastest mirror from API
    best_mirror=$(get_fastest_mirror)
    
    if [ $? -eq 0 ] && [ -n "$best_mirror" ]; then
        mirror_name=$(echo "$best_mirror" | awk -F/ '{print $3}')
        echo "Successfully selected mirror: $mirror_name"
        echo "Mirror URL: $best_mirror"
    else
        echo "Failed to get mirror from API. Falling back to manual check..."
        
        # Fallback to old method
        TEMP_DIR="/tmp/mirror_check"
        mkdir -p "$TEMP_DIR"
        echo "Created temp directory: $TEMP_DIR"
        
        latest_time=0
        best_mirror=""
        
        echo "Checking fallback mirrors for latest timestamp..."
        for mirror in "${FALLBACK_MIRRORS[@]}"; do
            mirror_name=$(echo "$mirror" | awk -F/ '{print $3}')
            echo "Checking mirror: $mirror_name"
            
            timeout 5 rsync -q "$mirror/lastsync" "$TEMP_DIR/$mirror_name.lastsync" 2>/dev/null
            timeout_status=$?
            
            if [ $timeout_status -eq 124 ]; then
                echo "Timeout occurred while checking $mirror_name"
                continue
            fi
            
            if [ -f "$TEMP_DIR/$mirror_name.lastsync" ]; then
                timestamp=$(cat "$TEMP_DIR/$mirror_name.lastsync")
                echo "Mirror $mirror_name timestamp: $timestamp ($(date -d @$timestamp '+%Y-%m-%d %H:%M:%S'))"
                
                if [ "$timestamp" -gt "$latest_time" ]; then
                    latest_time=$timestamp
                    best_mirror=$mirror
                    echo "New best mirror: $mirror_name with timestamp $timestamp"
                fi
            else
                echo "Failed to retrieve lastsync from $mirror_name"
            fi
        done
        
        echo "Cleaning up temporary files..."
        rm -f "$TEMP_DIR"/*.lastsync
        
        if [ -z "$best_mirror" ]; then
            best_mirror=${FALLBACK_MIRRORS[0]}
            echo "No valid mirrors found. Defaulting to ${FALLBACK_MIRRORS[0]}"
        else
            mirror_name=$(echo "$best_mirror" | awk -F/ '{print $3}')
            echo "Selected best mirror: $mirror_name with timestamp $latest_time ($(date -d @$latest_time '+%Y-%m-%d %H:%M:%S'))"
        fi
    fi
    
    echo "Starting rsync from $best_mirror to $LOCAL_MIRROR"
    rsync -rlptH --safe-links --delete --delay-updates --progress --stats "$best_mirror" "$LOCAL_MIRROR"
    rsync_status=$?
    echo "Rsync completed with status: $rsync_status"
    
    echo "Running footer script"
    bash /root/Scripts/footer.sh
    footer_status=$?
    echo "Footer script completed with status: $footer_status"
    
    echo "Sync process completed"
) 200>/tmp/rsync-mirror.lock
