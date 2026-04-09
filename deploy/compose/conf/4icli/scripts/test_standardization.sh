#!/bin/bash
# Test the standardize_filename function

# Source the function from the main script
source /home/care/4icli/scripts/01_process_incoming.sh

echo "=== Testing Filename Standardization ==="
echo

# Test the MEXPR file that was in the logs
test_file="REACH.D0259.MEXPR.07.PY2025.D250806.T1104510.xlsx"
echo "Original filename: $test_file"
echo "File type: MEXPR"

# Extract just the standardize_filename function and test it
standardize_filename() {
    local filename="$1"
    local file_type="$2"
    
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
    
    # If no pattern matches, return original filename
    echo "$filename"
}

standardized=$(standardize_filename "$test_file" "MEXPR")
echo "Standardized filename: $standardized"

echo
echo "Expected: Monthly Expenditure Report 08.06.2025.xlsx"
echo "Actual:   $standardized"

if [[ "$standardized" == "Monthly Expenditure Report 08.06.2025.xlsx" ]]; then
    echo "[OK] SUCCESS: Standardization working correctly!"
else
    echo "[ERROR] FAILED: Standardization not working as expected"
    
    # Debug the regex matching
    echo
    echo "=== Debug Information ==="
    echo "Testing regex patterns:"
    
    if [[ "$test_file" =~ ^(P|REACH)\.D[0-9]+\. ]]; then
        echo "[OK] First pattern matches: ^(P|REACH)\.D[0-9]+\."
        
        if [[ "$test_file" =~ \.D([0-9]{6})\. ]]; then
            echo "[OK] Date pattern matches: \.D([0-9]{6})\."
            echo "  Date code: ${BASH_REMATCH[1]}"
            local date_code="${BASH_REMATCH[1]}"
            echo "  Year: 20${date_code:0:2}"
            echo "  Month: ${date_code:2:2}"
            echo "  Day: ${date_code:4:2}"
        else
            echo "[ERROR] Date pattern does not match: \.D([0-9]{6})\."
        fi
    else
        echo "[ERROR] First pattern does not match: ^(P|REACH)\.D[0-9]+\."
    fi
fi
