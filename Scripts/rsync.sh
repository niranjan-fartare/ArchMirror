#!/bin/bash
(
    flock -n 200 || { echo "Lock acquisition failed. Another sync process is running."; exit 1; }
    echo "Lock acquired. Starting mirror sync process."

    MIRRORS=(
        "rsync://mirror.moson.org/arch/"
        "rsync://mirror.peeres-telecom.fr/archlinux/"
        "rsync://archlinux.thaller.ws/archlinux/"
        "rsync://rsync.osbeck.com/archlinux/"
        "rsync://arch.mirror.constant.com/archlinux/"
    )

    LOCAL_MIRROR="/var/www/html"
    TEMP_DIR="/tmp/mirror_check"
    mkdir -p "$TEMP_DIR"
    echo "Created temp directory: $TEMP_DIR"
    
    latest_time=0
    best_mirror=""
    
    echo "Checking mirrors for latest timestamp..."
    for mirror in "${MIRRORS[@]}"; do
        mirror_name=$(echo "$mirror" | awk -F/ '{print $3}')
        echo "Checking mirror: $mirror_name"
        
        # Use timeout command to limit rsync to 5 seconds
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
        best_mirror=${MIRRORS[0]}
        echo "No valid mirrors found. Defaulting to ${MIRRORS[0]}"
    else
        mirror_name=$(echo "$best_mirror" | awk -F/ '{print $3}')
        echo "Selected best mirror: $mirror_name with timestamp $latest_time ($(date -d @$latest_time '+%Y-%m-%d %H:%M:%S'))"
    fi
    
    echo "Starting rsync from $best_mirror to $LOCAL_MIRROR"
    rsync -rlptH --safe-links --delete-delay --delay-updates --progress --stats "$best_mirror" "$LOCAL_MIRROR"
    rsync_status=$?
    echo "Rsync completed with status: $rsync_status"
    
    echo "Running footer script"
    bash /root/Scripts/footer.sh
    footer_status=$?
    echo "Footer script completed with status: $footer_status"
    
    echo "Sync process completed"
) 200>/tmp/rsync-mirror.lock
