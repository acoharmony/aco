#!/bin/bash
# cron_process_incoming.sh - Cron-friendly wrapper for processing incoming files
# Only runs if WSL is active and there are new files to process

set -e

# Configuration
SCRIPT_DIR="$(dirname "$0")"
MAIN_SCRIPT="$SCRIPT_DIR/01_process_incoming.sh"
SOURCE_DIR="$HOME/4icli"
LOCK_FILE="/tmp/process_incoming.lock"
LAST_RUN_FILE="$HOME/4icli/.last_process_run"
LOG_FILE="$HOME/4icli/logs/cron_process.log"
DOWNLOADED_FILES_LOG="$HOME/4icli/.downloaded_files"

# Logging function
log_cron() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] CRON: $1" | tee -a "$LOG_FILE"
}

# Check if WSL is properly running (can access Windows drives)
check_wsl_status() {
    # Check if we can access Windows C: drive (basic WSL functionality test)
    if [ -d "/mnt/c" ] && [ "$(ls -A /mnt/c 2>/dev/null | wc -l)" -gt 0 ]; then
        return 0
    else
        log_cron "WSL not properly initialized or Windows drives not accessible"
        return 1
    fi
}

# Check if reach_reports_25 is mounted and accessible
check_reach_reports_mount() {
    if [ -d "/home/care/reach_reports_25" ] && [ "$(ls -A /home/care/reach_reports_25 2>/dev/null | wc -l)" -gt 0 ]; then
        return 0
    else
        log_cron "reach_reports_25 not mounted or not accessible"
        return 1
    fi
}

# Function to check if a file has already been downloaded
is_file_already_downloaded() {
    local filename="$1"
    [ -f "$DOWNLOADED_FILES_LOG" ] && grep -q "^$filename$" "$DOWNLOADED_FILES_LOG"
}

# Function to mark a file as downloaded
mark_file_as_downloaded() {
    local filename="$1"
    echo "$filename" >> "$DOWNLOADED_FILES_LOG"
}

# Function to clean up old entries from downloaded files log (keep only last 7 days worth)
cleanup_downloaded_files_log() {
    if [ -f "$DOWNLOADED_FILES_LOG" ]; then
        local temp_file="/tmp/downloaded_files_cleanup.$$"
        local seven_days_ago=$(date -d "7 days ago" +%Y-%m-%d)
        
        # Keep only recent entries (this is a simple cleanup - in practice, we might want more sophisticated logic)
        # For now, we'll keep the file size manageable by keeping only the last 1000 entries
        if [ $(wc -l < "$DOWNLOADED_FILES_LOG") -gt 1000 ]; then
            tail -500 "$DOWNLOADED_FILES_LOG" > "$temp_file"
            mv "$temp_file" "$DOWNLOADED_FILES_LOG"
            log_cron "Cleaned up downloaded files log, kept last 500 entries"
        fi
    fi
}

# Function to get list of new files (not already downloaded)
get_new_files_list() {
    local yesterday=$(date -d "yesterday" +%Y-%m-%d)
    local temp_file="/tmp/4icli_view_output.$$"
    
    cd "$HOME/4icli" || {
        log_cron "Failed to change to 4icli directory"
        return 1
    }
    
    # Get list of available files
    if ./4icli datahub -a D0259 -v --updatedAfter "$yesterday" > "$temp_file" 2>&1; then
        # Extract filenames from the output and check if they're already downloaded
        local new_files=()
        local total_files=0
        
        while IFS= read -r line; do
            # Look for lines with file information (format: "X of Y - filename")
            if [[ "$line" =~ ^[[:space:]]*[0-9]+[[:space:]]of[[:space:]][0-9]+[[:space:]]-[[:space:]](.+)$ ]]; then
                local filename="${BASH_REMATCH[1]}"
                total_files=$((total_files + 1))
                
                if ! is_file_already_downloaded "$filename"; then
                    new_files+=("$filename")
                fi
            fi
        done < "$temp_file"
        
        rm -f "$temp_file"
        
        if [ ${#new_files[@]} -gt 0 ]; then
            log_cron "Found ${#new_files[@]} new files out of $total_files total files available remotely"
            for file in "${new_files[@]}"; do
                log_cron "  New file: $file"
            done
            return 0
        else
            log_cron "All $total_files remote files have already been downloaded"
            return 1
        fi
    else
        log_cron "Failed to check remote files"
        rm -f "$temp_file"
        return 1
    fi
}

# Check if there are new files available remotely using 4icli view
check_for_new_files_remote() {
    log_cron "Checking for new files remotely using 4icli view..."
    get_new_files_list
}

# Check if there are new files to process locally
check_for_local_files() {
    # Find files in source directory that haven't been processed
    local file_count=0
    
    for file in "$SOURCE_DIR"/*; do
        [ -f "$file" ] || continue
        
        local filename=$(basename "$file")
        
        # Skip special files, log files, and directories (same logic as main script)
        [[ "$filename" == "4icli" || "$filename" == "config.txt" || "$filename" == "moves.log" || "$filename" == "cron_process.log" || "$filename" == "logs" || "$filename" == "scripts" || "$filename" == "docs" || "$filename" == "src" || "$filename" == "tests" ]] && continue
        
        # Skip any .log files
        [[ "$filename" =~ \.log$ ]] && continue
        
        # Count files that match processing patterns
        if [[ "$filename" =~ \.(txt|xlsx|zip|csv)$ ]]; then
            ((file_count++))
        fi
    done
    
    if [ "$file_count" -gt 0 ]; then
        log_cron "Found $file_count local files to process"
        return 0
    else
        log_cron "No local files found to process"
        return 1
    fi
}

# Download new files using 4icli and track them
download_new_files() {
    local yesterday=$(date -d "yesterday" +%Y-%m-%d)
    local temp_file="/tmp/4icli_download_output.$$"
    
    log_cron "Downloading new files using 4icli..."
    
    cd "$HOME/4icli" || {
        log_cron "Failed to change to 4icli directory"
        return 1
    }
    
    # Download files updated since yesterday and capture output
    if ./4icli datahub -a D0259 -d --updatedAfter "$yesterday" > "$temp_file" 2>&1; then
        # Log the download output
        cat "$temp_file" | tee -a "$LOG_FILE"
        
        # Extract and track downloaded filenames
        while IFS= read -r line; do
            # Look for lines indicating successful downloads (format: "Downloading X of Y - filename")
            if [[ "$line" =~ ^Downloading[[:space:]]+[0-9]+[[:space:]]of[[:space:]]+[0-9]+[[:space:]]-[[:space:]](.+)$ ]]; then
                local filename="${BASH_REMATCH[1]}"
                mark_file_as_downloaded "$filename"
                log_cron "Marked as downloaded: $filename"
            fi
        done < "$temp_file"
        
        rm -f "$temp_file"
        log_cron "Successfully downloaded new files"
        return 0
    else
        # Log the error output
        cat "$temp_file" | tee -a "$LOG_FILE"
        rm -f "$temp_file"
        log_cron "Failed to download files"
        return 1
    fi
}

# Check if another instance is running
check_lock() {
    if [ -f "$LOCK_FILE" ]; then
        local lock_pid=$(cat "$LOCK_FILE" 2>/dev/null)
        if [ -n "$lock_pid" ] && kill -0 "$lock_pid" 2>/dev/null; then
            log_cron "Another instance is running (PID: $lock_pid), skipping"
            return 1
        else
            log_cron "Stale lock file found, removing"
            rm -f "$LOCK_FILE"
        fi
    fi
    return 0
}

# Create lock file
create_lock() {
    echo $$ > "$LOCK_FILE"
}

# Remove lock file
remove_lock() {
    rm -f "$LOCK_FILE"
}

# Update last run timestamp
update_last_run() {
    date +%s > "$LAST_RUN_FILE"
}

# Main execution
main() {
    log_cron "Starting cron processing check"
    
    # Check if another instance is running
    if ! check_lock; then
        exit 0
    fi
    
    # Create lock
    create_lock
    trap remove_lock EXIT
    
    # Check WSL status
    if ! check_wsl_status; then
        log_cron "WSL not ready, skipping processing"
        exit 0
    fi
    
    # Check if reach_reports is mounted
    if ! check_reach_reports_mount; then
        log_cron "reach_reports_25 not mounted, attempting to mount..."
        if [ -f "/home/care/scripts/mount_reach_reports_25.sh" ]; then
            if /home/care/scripts/mount_reach_reports_25.sh; then
                log_cron "Successfully mounted reach_reports_25"
            else
                log_cron "Failed to mount reach_reports_25, skipping processing"
                exit 0
            fi
        else
            log_cron "Mount script not found, skipping processing"
            exit 0
        fi
    fi
    
    # Check for local files first (quick check)
    local has_local_files=false
    if check_for_local_files; then
        has_local_files=true
        log_cron "Local files found, will process them"
    fi
    
    # Clean up old entries from downloaded files log
    cleanup_downloaded_files_log
    
    # Check for new files remotely (only if no local files)
    local has_remote_files=false
    if [ "$has_local_files" = false ]; then
        if check_for_new_files_remote; then
            has_remote_files=true
            # Download the new files
            if ! download_new_files; then
                log_cron "Failed to download new files, exiting"
                exit 1
            fi
        else
            log_cron "No new files to download (all available files already downloaded)"
        fi
    else
        log_cron "Skipping remote file check because local files found"
    fi
    
    # Exit if no files to process
    if [ "$has_local_files" = false ] && [ "$has_remote_files" = false ]; then
        log_cron "No files to process (local or remote), exiting"
        exit 0
    fi
    
    # Run the main processing script
    log_cron "Running main processing script"
    if [ -x "$MAIN_SCRIPT" ]; then
        if "$MAIN_SCRIPT"; then
            log_cron "Processing completed successfully"
            update_last_run
        else
            log_cron "Processing failed with exit code $?"
            exit 1
        fi
    else
        log_cron "Main script not found or not executable: $MAIN_SCRIPT"
        exit 1
    fi
    
    log_cron "Cron processing completed"
}

# Run main function
main "$@"
