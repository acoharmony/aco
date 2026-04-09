#!/bin/bash
# Test script to verify the duplicate download fix

# Configuration
DOWNLOADED_FILES_LOG="$HOME/4icli/.downloaded_files"
LOG_FILE="$HOME/4icli/logs/cron_process.log"

echo "=== Testing Duplicate Download Fix ==="
echo "Date: $(date)"
echo

# Show current downloaded files log
echo "1. Current downloaded files log:"
if [ -f "$DOWNLOADED_FILES_LOG" ]; then
    echo "   File exists with $(wc -l < "$DOWNLOADED_FILES_LOG") entries:"
    echo "   Last 5 entries:"
    tail -5 "$DOWNLOADED_FILES_LOG" | sed 's/^/   /'
else
    echo "   No downloaded files log exists yet"
fi
echo

# Show recent cron log entries
echo "2. Recent cron log entries:"
if [ -f "$LOG_FILE" ]; then
    echo "   Last 10 entries:"
    tail -10 "$LOG_FILE" | sed 's/^/   /'
else
    echo "   No cron log file exists yet"
fi
echo

# Simulate adding test files to downloaded log
echo "3. Testing file tracking functions:"
echo "   Adding test files to downloaded log..."

# Source the functions from the cron script
source /home/care/4icli/scripts/cron_process_incoming.sh

# Test the functions
test_file1="REACH.D0259.MEXPR.07.PY2025.D250806.T1104510.xlsx"
test_file2="REACH.D0259.BNMR.PY2025.D250729.T1445290.xlsx"

echo "   Testing is_file_already_downloaded for: $test_file1"
if is_file_already_downloaded "$test_file1"; then
    echo "   [OK] File is already marked as downloaded"
else
    echo "   [ERROR] File is NOT marked as downloaded"
    echo "   Adding it now..."
    mark_file_as_downloaded "$test_file1"
    echo "   [OK] File marked as downloaded"
fi

echo "   Testing is_file_already_downloaded for: $test_file2"
if is_file_already_downloaded "$test_file2"; then
    echo "   [OK] File is already marked as downloaded"
else
    echo "   [ERROR] File is NOT marked as downloaded"
    echo "   Adding it now..."
    mark_file_as_downloaded "$test_file2"
    echo "   [OK] File marked as downloaded"
fi

echo
echo "4. Updated downloaded files log:"
if [ -f "$DOWNLOADED_FILES_LOG" ]; then
    echo "   File now has $(wc -l < "$DOWNLOADED_FILES_LOG") entries:"
    echo "   Last 5 entries:"
    tail -5 "$DOWNLOADED_FILES_LOG" | sed 's/^/   /'
else
    echo "   Still no downloaded files log"
fi

echo
echo "=== Test Complete ==="
echo "The fix should now prevent downloading the same files multiple times."
echo "Next cron run should show 'All X remote files have already been downloaded'"
