# 4ICLI API Map

## Overview
4ICLI is a Node.js-based CLI tool (v0.0.20) built with the oclif framework that interfaces with 4Innovation's DataHub API to download ACO REACH program files.

## Architecture
- **Runtime**: Node.js v12.18.1 (embedded)
- **Framework**: oclif CLI framework
- **Package**: Self-contained ELF 64-bit Linux executable
- **Snapshot Path**: `/snapshot/Pipelines_4icli_master/`

## Commands Structure

### 1. `configure`
Configure the 4icli with your access credentials.

**Usage**: `4icli configure`
- Stores API credentials for authentication
- Interactive credential setup
- Credentials are persisted locally

### 2. `rotate`
Rotate your API credentials.

**Usage**: `4icli rotate`
- Updates/refreshes API credentials
- Security feature for credential rotation

### 3. `datahub`
Access and download files from DataHub.

**Usage**: `4icli datahub [OPTIONS]`

#### Options:

##### Basic Operations:
- `-l, --list` - List datahub folders and file types
- `-v, --view` - View list of files available for download
- `-d, --download` - Download files

##### Filtering Parameters:
- `-a, --apm=<APM_ID>` - APM Entity ID
- `-y, --year=<YEAR>` - Performance year (default: 2025)
- `-c, --category=<CATEGORY>` - DataHub category to download from:
  - `Beneficiary List`
  - `CCLF` (Claim and Claim Line Feed Files)
  - `Reports`
  - `Monthly Exclusion Files`

##### File Selection:
- `-f, --file=<FILE_TYPE_CODE>` - File Type code to download a single file type (default: 113)

##### Date Filters:
- `--createdAfter=<DATE>` - Get files created after date (format: YYYY-MM-DD)
- `--createdBetween=<DATE1,DATE2>` - Get files created between dates
- `--createdWithinLastMonth` - Get files created within the last month
- `--createdWithinLastWeek` - Get files created within the last week
- `--updatedAfter=<DATE>` - Get files updated after date
- `--updatedBetween=<DATE1,DATE2>` - Get files updated between dates

### 4. `help`
Display help for 4icli.

**Usage**: `4icli help [COMMAND]`

## API Data Structures

### File Type Codes
The system uses numeric codes to identify file types:
- Default file type code: `113`
- File types are referenced internally as:
  - `fileTypeCode` - Single file type code
  - `fileTypeCodes` - Multiple file type codes
  - `fileTypeDesc` - File type description

### Internal Properties
- `fileTypes` - Array of available file types
- Job processing: `start(fileType)` - Initiates download job
- Query parameter: `?_type=<fileType>` (when not "All")

## Download Workflow

1. **Authentication**: Uses stored credentials from `configure` command
2. **File Listing**: Query DataHub for available files based on filters
3. **Job Creation**: Creates download job for selected files
4. **Download Process**:
   - Shows progress: `Starting job for [ <fileType> ] files`
   - Downloads to local `~/4icli` directory
5. **Post-Processing**: Your shell scripts then organize files into:
   - `~/program_data/raw/reach/` - Organized by type
   - `~/reach_reports_25/` - For reporting purposes

## File Categories Mapping

Based on your shell scripts, the downloaded files are organized as:

### Alignment Files
- `P.D*.PALMR.*` → Provider Alignment Report (PAR)
- `P.D*.PBVAR.*` → Voluntary Alignment Response Files
- `P.D*.TPARC.*` → Beneficiary Alignment Reports (BAR)

### CCLF Files
- `CCLF*.*.ZIP` → Claim and Claim Line Feed Files

### Other Files
- `P.D*.RAP*V*.*` → Risk Adjustment Payment Reports
- `P.D*.BLQQR.*` → Beneficiary Level Quarterly Quality Reports

## HTTP Methods Support
The binary supports standard HTTP operations:
- GET - Retrieve data/files
- POST - Submit data/authentication
- PUT - Update resources
- DELETE - Remove resources
- PATCH - Partial updates

## Security Features
- Credential storage and management
- Credential rotation capability
- SSL/TLS support (OpenSSL integration)
- Authentication tokens for API access

## Notes
- The binary is compiled with debugging symbols (not stripped)
- Uses dynamic linking with standard Linux libraries
- Includes OpenSSL cryptographic functions
- Built-in JSON parsing and HTTP client capabilities
- Supports batch/queue processing for multiple file downloads