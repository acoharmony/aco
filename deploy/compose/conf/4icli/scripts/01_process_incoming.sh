#!/bin/bash
# process_incoming.sh - Process files downloaded by cron jobs
# Moves files from ~/4icli to program_data/raw/reach and reach_reports_XX
# Based on patterns from 01_move_files.sh with added copy to reach_reports

# Set directories
SOURCE_DIR="$HOME/4icli"
PROGRAM_DATA_DIR="$HOME/workspace/raw/reach"
REACH_REPORTS_24_DIR="$HOME/reach_reports_24"
REACH_REPORTS_25_DIR="$HOME/reach_reports_25"
LOG_FILE="$HOME/4icli/logs/moves.log"

# Log function
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
    return 0
}

# Function to determine the appropriate reach_reports directory based on filename
get_reach_reports_dir() {
    local filename="$1"

    # Check for explicit year patterns
    if [[ "$filename" =~ \.PY2024\. ]]; then
        echo "$REACH_REPORTS_24_DIR"
    elif [[ "$filename" =~ \.PY2025\. ]]; then
        echo "$REACH_REPORTS_25_DIR"
    elif [[ "$filename" =~ \.D25 ]]; then
        # D25 indicates 2025
        echo "$REACH_REPORTS_25_DIR"
    elif [[ "$filename" =~ \.D24 ]]; then
        # D24 indicates 2024
        echo "$REACH_REPORTS_24_DIR"
    else
        # Default to 2025 if no year indicator found
        echo "$REACH_REPORTS_25_DIR"
    fi
}

# Function to standardize file names for reach_reports (human-readable names with consistent dates)
standardize_filename() {
    local filename="$1"
    local file_type="$2"  # PALMR, PBVAR, CCLF, etc.
    
    # Extract file extension
    local ext="${filename##*.}"
    
    # Handle system code files (P.D0259.XXXXX.DYYMMDD.THHMMSS.ext or REACH.D0259.XXXXX.DYYMMDD.THHMMSS.ext)
    if [[ "$filename" =~ ^(P|REACH)\.D[0-9]+\. ]]; then
        # Extract date from system code (DYYMMDD format)
        if [[ "$filename" =~ \.D([0-9]{6})\. ]]; then
            local date_code="${BASH_REMATCH[1]}"
            local year="20${date_code:0:2}"
            local month="${date_code:2:2}"
            local day="${date_code:4:2}"
            local std_date="$month.$day.$year"  # Format as MM.DD.YYYY for human readability
            
            case "$file_type" in
                "PALMR")
                    echo "Provider Alignment Report $std_date.$ext"
                    ;;
                "PBVAR")
                    echo "Voluntary Alignment Response File $std_date.$ext"
                    ;;
                "CCLF")
                    echo "Claim and Claim Line Feed File $std_date.$ext"
                    ;;
                "MEXPR")
                    echo "Monthly Expenditure Report $std_date.$ext"
                    ;;
                "BNMR")
                    echo "D0259 PY2025 Benchmark Report $std_date.$ext"
                    ;;
                "PYRED")
                    echo "Provider Specific Payment Reduction Report $std_date.$ext"
                    ;;
                *)
                    echo "$filename"  # Keep original if unknown type
                    ;;
            esac
            return 0
        fi
    fi
    
    # Handle existing human-readable files with inconsistent date formats
    case "$file_type" in
        "BAR")
            # Standardize Beneficiary Alignment Report dates
            if [[ "$filename" =~ Beneficiary\ Alignment\ Report.*([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{2,4}) ]]; then
                local month="${BASH_REMATCH[1]}"
                local day="${BASH_REMATCH[2]}"
                local year="${BASH_REMATCH[3]}"
                
                # Convert 2-digit year to 4-digit
                if [[ ${#year} -eq 2 ]]; then
                    year="20$year"
                fi
                
                # Pad month and day with leading zeros
                month=$(printf "%02d" "$month")
                day=$(printf "%02d" "$day")
                
                echo "Beneficiary Alignment Report $year.$month.$day.$ext"
            else
                echo "$filename"
            fi
            ;;
        "PALMR")
            # Standardize Provider Alignment Report dates
            if [[ "$filename" =~ Provider\ Alignment\ Report.*([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{4}) ]]; then
                local month="${BASH_REMATCH[1]}"
                local day="${BASH_REMATCH[2]}"
                local year="${BASH_REMATCH[3]}"
                
                month=$(printf "%02d" "$month")
                day=$(printf "%02d" "$day")
                
                echo "Provider Alignment Report $year.$month.$day.$ext"
            else
                echo "$filename"
            fi
            ;;
        "PBVAR")
            # Standardize Voluntary Alignment Response File dates
            if [[ "$filename" =~ (Signed\ Attestation\ )?Voluntary\ Alignment\ Response\ File.*([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{4}) ]]; then
                local month="${BASH_REMATCH[2]}"
                local day="${BASH_REMATCH[3]}"
                local year="${BASH_REMATCH[4]}"
                
                month=$(printf "%02d" "$month")
                day=$(printf "%02d" "$day")
                
                echo "Voluntary Alignment Response File $year.$month.$day.$ext"
            else
                echo "$filename"
            fi
            ;;
        "ALTERNATIVE")
            # Standardize Alternative Payment Arrangement Report dates
            if [[ "$filename" =~ Alternative\ Payment\ Arrangement\ Report.*([0-9]{1,2})\.([0-9]{1,2})\.([0-9]{4}) ]]; then
                local month="${BASH_REMATCH[1]}"
                local day="${BASH_REMATCH[2]}"
                local year="${BASH_REMATCH[3]}"
                
                month=$(printf "%02d" "$month")
                day=$(printf "%02d" "$day")
                
                echo "Alternative Payment Arrangement Report $year.$month.$day.$ext"
            else
                echo "$filename"
            fi
            ;;
        *)
            # For other types, just return the original filename
            echo "$filename"
            ;;
    esac
}

# Function to handle moving and copying files with proper naming
move_and_copy() {
    local src="$1"
    local dest_dir="$2"
    local copy_dir="${3:-}"  # Optional copy destination (for reach_reports)
    local file_type="${4:-}"  # Optional file type for standardization (PALMR, PBVAR, etc.)
    local filename=$(basename "$src")

    # Create destination directory if it doesn't exist
    if [ ! -d "$dest_dir" ]; then
        if ! mkdir -p "$dest_dir"; then
            log "ERROR: Failed to create directory: $dest_dir"
            return 1
        fi
        log "Created directory: $dest_dir"
    fi

    # Check if file already exists in destination
    local dest_file="$dest_dir/$filename"
    if [ -f "$dest_file" ]; then
        # For program_data, keep the original filename (overwrite)
        log "File $filename exists in $dest_dir, keeping original name"
    fi

    # Move the file (will overwrite if exists)
    if ! mv -f "$src" "$dest_file"; then
        log "ERROR: Failed to move $filename from $src to $dest_dir"
        return 1
    fi
    log "MOVED: $filename from $(dirname "$src") to $dest_dir"

    # Copy to reach_reports if copy_dir is specified
    if [ -n "$copy_dir" ]; then
        if [ ! -d "$copy_dir" ]; then
            if ! mkdir -p "$copy_dir"; then
                log "ERROR: Failed to create copy directory: $copy_dir"
                return 1
            fi
            log "Created directory: $copy_dir"
        fi

        # For reach_reports, use standardized filename if file_type is provided
        local copy_filename="$filename"
        if [ -n "$file_type" ]; then
            copy_filename=$(standardize_filename "$filename" "$file_type")
            log "Standardized filename: $filename -> $copy_filename"
        fi
        
        # Add timestamp to filename if it exists in copy_dir
        if [ -f "$copy_dir/$copy_filename" ]; then
            local timestamp=$(date +"%Y%m%d_%H%M%S")
            local base="${copy_filename%.*}"
            local ext="${copy_filename##*.}"
            copy_filename="${base}_${timestamp}.${ext}"
            log "File exists in $copy_dir, copying as $copy_filename"
        fi

        if ! cp "$dest_file" "$copy_dir/$copy_filename"; then
            log "ERROR: Failed to copy $filename to $copy_dir as $copy_filename"
            return 1
        fi
        log "COPIED: $filename to $copy_dir as $copy_filename"
    fi

    return 0
}

# Extract nested zip files
extract_nested_zips() {
    local dir="$1"

    # Find and process all zip files in the directory
    find "$dir" -name "*.zip" -type f | while read -r zip_file; do
        log "Extracting nested zip: $(basename "$zip_file")"

        # Create a temporary directory for extraction
        local temp_dir="${zip_file%.*}_extracted"
        mkdir -p "$temp_dir"

        # Extract the zip file
        if unzip -q -o "$zip_file" -d "$temp_dir"; then
            # Move all files from temp_dir to the parent directory
            find "$temp_dir" -mindepth 1 -maxdepth 1 -exec mv -f {} "$dir" \;
            # Remove the empty temp directory
            rmdir "$temp_dir"
            # Remove the processed zip file
            rm -f "$zip_file"

            # Recursively process any newly extracted zip files
            extract_nested_zips "$dir"
        else
            log "WARNING: Failed to extract $zip_file"
            rm -rf "$temp_dir"
        fi
    done
}

# Process CCLF files
process_cclf() {
    local file="$1"
    local filename=$(basename "$file")
    local month=$(date +"%b.").$(date +%Y)
    local dest_dir="$PROGRAM_DATA_DIR/claims/cclf"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir="$reach_reports_base/Claim and Claim Line Feed File (CCLF)/CCLF Delivered in $month"

    # Create destination directories if they don't exist
    mkdir -p "$dest_dir" "$copy_dir" 2>/dev/null

    # Move and copy the file
    if ! move_and_copy "$file" "$dest_dir" "$copy_dir"; then
        return 1
    fi

    log "Processing CCLF file: $filename"

    # Extract the ZIP file
    if ! unzip -q -o "$dest_dir/$filename" -d "$dest_dir"; then
        log "ERROR: Failed to extract $filename"
        return 1
    fi

    # Process any nested zip files
    extract_nested_zips "$dest_dir"

    log "Extracted $filename to $dest_dir"
    return 0
}

# Process PALMR files (Provider Alignment Report)
process_palmr() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/alignment/palmr"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir="$reach_reports_base/Provider Alignment Report (PAR)"

    if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "PALMR"; then
        return 1
    fi
    log "Processed Provider Alignment Report: $filename"
    return 0
}

# Process PBVAR files (Paper Based Voluntary Alignment Response)
process_pbvar() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/alignment/pbvar"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir="$reach_reports_base/Voluntary Alignment Response Files"

    if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "PBVAR"; then
        return 1
    fi
    log "Processed Voluntary Alignment Response: $filename"
    return 0
}

# Process ALGC/ALGR files (Beneficiary Alignment Report)
process_beneficiary_alignment() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/alignment/beneficiary"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir="$reach_reports_base/Beneficiary Alignment Reports (BAR)"

    if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "BAR"; then
        return 1
    fi
    log "Processed Beneficiary Alignment Report: $filename"
    return 0
}

# Process BLQQR files (Quality Reports) - unpack and keep only CSV files
process_blqqr() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/quality/blqqr"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir="$reach_reports_base/Quality Reports"
    
    # Create destination directory
    if [ ! -d "$dest_dir" ]; then
        if ! mkdir -p "$dest_dir"; then
            log "ERROR: Failed to create directory: $dest_dir"
            return 1
        fi
        log "Created directory: $dest_dir"
    fi
    
    # Extract the zip file to a temporary directory
    local temp_dir="/tmp/${filename%.*}_extract"
    mkdir -p "$temp_dir"
    
    log "Extracting $filename to find CSV files"
    if ! unzip -q -o "$file" -d "$temp_dir"; then
        log "ERROR: Failed to extract $filename"
        rm -rf "$temp_dir"
        return 1
    fi
    
    # Find and move only CSV files to destination
    local csv_count=0
    find "$temp_dir" -name "*.csv" -type f | while read -r csv_file; do
        local csv_name=$(basename "$csv_file")
        if ! mv "$csv_file" "$dest_dir/$csv_name"; then
            log "ERROR: Failed to move CSV file: $csv_name"
        else
            log "Moved CSV file: $csv_name to $dest_dir"
            ((csv_count++))
        fi
    done
    
    # Copy CSV files to reach_reports if copy_dir specified
    if [ -n "$copy_dir" ] && [ -d "$copy_dir" ] || mkdir -p "$copy_dir"; then
        find "$dest_dir" -name "*.csv" -type f -newer "$file" | while read -r csv_file; do
            local csv_name=$(basename "$csv_file")
            if ! cp "$csv_file" "$copy_dir/$csv_name"; then
                log "ERROR: Failed to copy CSV file to reach_reports: $csv_name"
            else
                log "Copied CSV file to reach_reports: $csv_name"
            fi
        done
    fi
    
    # Clean up
    rm -rf "$temp_dir"
    rm -f "$file"
    
    log "Processed BLQQR Quality Report: $filename"
    return 0
}

# Process TPARC files (Weekly claims reduction)
process_tparc() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/claims/weekly"

    if ! move_and_copy "$file" "$dest_dir"; then  # No copy to reach_reports
        return 1
    fi
    log "Processed Weekly Claims Reduction: $filename"
    return 0
}

# Process shadow bundle files
process_shadow() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/claims/shadow"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir=""

    # Handle SBMON (monthly) files - unzip them
    if [[ "$filename" =~ \.SBMON\. ]]; then
        # Extract the month from the filename (e.g., 02 from D250328)
        local month_num=${filename#*D25}
        month_num=${month_num:0:2}

        # Map month number to month name
        local month_name=$(date -d "${month_num}/01" +"%b" 2>/dev/null || date -j -f "%m" "$month_num" "+%b" 2>/dev/null)
        if [ $? -ne 0 ]; then
            # If we couldn't parse the date, use the current month as fallback
            month_name=$(date +"%b")
        fi

        # Create the destination directory for the monthly files
        local monthly_dir="$reach_reports_base/Shadow Bundles/Consolidated Monthly File - ${month_name} 20${filename:9:2}"

        # Move the zip file to program_data
        if ! mv "$file" "$dest_dir/$filename"; then
            log "ERROR: Failed to move $filename to $dest_dir"
            return 1
        fi

        # Unzip the file directly to program_data/claims/shadow
        log "Extracting $filename to $dest_dir"
        if ! unzip -q -o "$dest_dir/$filename" -d "$dest_dir"; then
            log "ERROR: Failed to extract $filename"
            return 1
        fi

        # Copy extracted files to reach_reports
        if [ -d "$monthly_dir" ] || mkdir -p "$monthly_dir"; then
            # Copy all extracted files to the monthly directory
            if ! cp "$dest_dir"/* "$monthly_dir/" 2>/dev/null; then
                log "WARNING: No files to copy from $dest_dir to $monthly_dir"
            else
                log "Copied extracted files to $monthly_dir"
            fi
        else
            log "ERROR: Failed to create directory $monthly_dir"
            return 1
        fi

        log "Processed Monthly Shadow Bundle: $filename"
        return 0
    fi

    # Handle SBNABP files - just move and copy without renaming
    if ! move_and_copy "$file" "$dest_dir" "$reach_reports_base/Shadow Bundles"; then
        return 1
    fi
    log "Processed Shadow Bundle: $filename"
    return 0
}

process_expenditure() {
    local file="$1"
    local filename=$(basename "$file")
    local dest_dir="$PROGRAM_DATA_DIR/expenditure"
    local reach_reports_base=$(get_reach_reports_dir "$filename")
    local copy_dir=""

    # Special handling for MEXPR files - copy to Monthly Expenditure Reports
    if [[ "$filename" =~ MEXPR ]]; then
        copy_dir="$reach_reports_base/Monthly Expenditure Reports"

        if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "MEXPR"; then
            return 1
        fi
        log "Processed Monthly Expenditure Report: $filename"
    # Special handling for BNMR files - copy to Benchmark Reports
    elif [[ "$filename" =~ BNMR ]]; then
        copy_dir="$reach_reports_base/Benchmark Reports (+ Settlement)"

        if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "BNMR"; then
            return 1
        fi
        log "Processed Benchmark Report: $filename"
    # Special handling for PYRED files - copy to reach_reports
    elif [[ "$filename" =~ PYRED ]]; then
        copy_dir="$reach_reports_base/Provider Specific Payment Reduction Report"

        if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "PYRED"; then
            return 1
        fi
        log "Processed Provider Specific Payment Reduction Report: $filename"
    # Special handling for ALTPR files - copy to Alternative Payment Arrangement Reports
    elif [[ "$filename" =~ ALTPR ]]; then
        copy_dir="$reach_reports_base/Alternative Payment Arrangement Reports"

        if ! move_and_copy "$file" "$dest_dir" "$copy_dir" "ALTERNATIVE"; then
            return 1
        fi
        log "Processed Alternative Payment Arrangement Report: $filename"
    else
        # Other expenditure files - no copy to reach_reports
        if ! move_and_copy "$file" "$dest_dir"; then
            return 1
        fi
        log "Processed Expenditure File: $filename"
    fi

    return 0
}

# Main processing
log "Starting file processing"
log "Source: $SOURCE_DIR"
log "Destination: $PROGRAM_DATA_DIR"
log "REACH Reports 24: $REACH_REPORTS_24_DIR"
log "REACH Reports 25: $REACH_REPORTS_25_DIR"
log "Log file: $LOG_FILE"

# Process each file in the source directory
for file in "$SOURCE_DIR"/*; do
    [ -f "$file" ] || continue

    filename=$(basename "$file")

    # Skip special files, log files, and directories
    [[ "$filename" == "4icli" || "$filename" == "config.txt" || "$filename" == "moves.log" || "$filename" == "cron_process.log" || "$filename" == "logs" || "$filename" == "scripts" || "$filename" == "docs" || "$filename" == "src" || "$filename" == "tests" ]] && continue
    
    # Skip any .log files
    [[ "$filename" =~ \.log$ ]] && continue

    log "Processing: $filename"

    # Determine file type and process
    if [[ "$filename" =~ ^CCLF ]] || [[ "$filename" =~ \.ACO\.ZCY ]]; then
        process_cclf "$file"
    elif [[ "$filename" =~ \.BLQQR\. ]]; then
        process_blqqr "$file"
    elif [[ "$filename" =~ \.P\.D[0-9]+\.PALMR ]] || [[ "$filename" =~ ^P\.D[0-9]+\.PALMR ]]; then
        process_palmr "$file"
    elif [[ "$filename" =~ \.P\.D[0-9]+\.PBVAR ]] || [[ "$filename" =~ ^P\.D[0-9]+\.PBVAR ]]; then
        process_pbvar "$file"
    elif [[ "$filename" =~ \.P\.D[0-9]+\.(ALGC|ALGR) ]] || [[ "$filename" =~ ^P\.D[0-9]+\.(ALGC|ALGR) ]]; then
        process_beneficiary_alignment "$file"
    elif [[ "$filename" =~ TPARC ]] && [[ "$filename" =~ \.(txt|csv)$ ]]; then
        process_tparc "$file"
    elif [[ "$filename" =~ SBNABP|SBMON ]]; then
        process_shadow "$file"
    elif [[ "$filename" =~ (PYRED|ALTRPR|ALTPR|BNMR|MEXPR|PAER|PLARU|PRBR) ]]; then
        process_expenditure "$file"
    else
        log "WARNING: Unrecognized file type: $filename"
        if ! move_and_copy "$file" "$PROGRAM_DATA_DIR/other"; then
            log "ERROR: Failed to move unhandled file: $filename"
        fi
    fi
done

log "Processing complete"
exit 0
