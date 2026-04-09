#!/bin/bash
# Test all filename standardizations

echo "=== Testing All Filename Standardizations ==="
echo

# Test function
test_standardization() {
    local filename="$1"
    local file_type="$2"
    local expected="$3"
    
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
    
    # If no pattern matches, return original filename
    echo "$filename"
}

# Test cases
echo "1. Testing MEXPR (Monthly Expenditure Report):"
mexpr_file="REACH.D0259.MEXPR.07.PY2025.D250806.T1104510.xlsx"
mexpr_result=$(test_standardization "$mexpr_file" "MEXPR")
mexpr_expected="Monthly Expenditure Report 08.06.2025.xlsx"
echo "   Original: $mexpr_file"
echo "   Result:   $mexpr_result"
echo "   Expected: $mexpr_expected"
if [[ "$mexpr_result" == "$mexpr_expected" ]]; then
    echo "   [OK] SUCCESS"
else
    echo "   [ERROR] FAILED"
fi
echo

echo "2. Testing BNMR (Benchmark Report):"
bnmr_file="REACH.D0259.BNMR.PY2025.D250729.T1445290.xlsx"
bnmr_result=$(test_standardization "$bnmr_file" "BNMR")
bnmr_expected="D0259 PY2025 Benchmark Report 07.29.2025.xlsx"
echo "   Original: $bnmr_file"
echo "   Result:   $bnmr_result"
echo "   Expected: $bnmr_expected"
if [[ "$bnmr_result" == "$bnmr_expected" ]]; then
    echo "   [OK] SUCCESS"
else
    echo "   [ERROR] FAILED"
fi
echo

echo "=== Summary ==="
echo "These files will be:"
echo "• Moved to workspace with original names (for data processing)"
echo "• Copied to reach_reports with human-readable names (for human use)"
echo
echo "Workspace paths:"
echo "• /home/care/workspace/raw/reach/expenditure/$mexpr_file"
echo "• /home/care/workspace/raw/reach/expenditure/$bnmr_file"
echo
echo "reach_reports paths:"
echo "• /home/care/reach_reports_25/Monthly Expenditure Reports/$mexpr_expected"
echo "• /home/care/reach_reports_25/Benchmark Reports (+ Settlement)/$bnmr_expected"
