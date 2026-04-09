# ACO REACH File Processing Scripts

## 01_process_incoming.sh

This script processes incoming ACO REACH files by:
1. Moving files from `~/4icli` to appropriate subdirectories in `~/program_data/raw/reach`
2. Copying files to their corresponding locations in `~/reach_reports_25`
3. Extracting ZIP files when necessary
4. Logging all actions

### Supported File Types

#### Alignment Files
- `P.D*.PALMR.*` → `program_data/raw/reach/alignment/palmr/` and `reach_reports_25/Provider Alignment Report (PAR)/`
- `P.D*.PBVAR.*` → `program_data/raw/reach/alignment/pbvar/` and `reach_reports_25/Voluntary Alignment Response Files/`
- `P.D*.TPARC.*` → `program_data/raw/reach/alignment/bar/` and `reach_reports_25/Beneficiary Alignment Reports (BAR)/`

#### CCLF Files
- `CCLF*.*.ZIP` → Extracted to `program_data/raw/reach/claims/cclf/CCLF[number]/`
- Also copied to `reach_reports_25/Claim and Claim Line Feed File (CCLF)/CCLF Delivered in [Month].[Year]/`

#### Other Files
- Risk Adjustment Payment Reports (`P.D*.RAP*V*.*`) → `program_data/raw/reach/expenditure/` and `reach_reports_25/Risk Score Reports/`
- Beneficiary Level Quarterly Quality Reports (`P.D*.BLQQR.*`) → `program_data/raw/reach/quality/blqqr/`
- Unrecognized files → `program_data/raw/reach/other/`

### Logging

Logs are stored in `~/4icli/logs/process_incoming_YYYYMMDD_HHMMSS.log`

### Usage

```bash
# Make the script executable
chmod +x 01_process_incoming.sh

# Run the script
./01_process_incoming.sh
```

The script is designed to be run manually or via cron after new files are downloaded.
