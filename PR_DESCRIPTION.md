# Release Setup Workflow for HTAN2 v8+

## Overview

This PR introduces a complete workflow for setting up new release versions (v8+) of HTAN2 projects, including folder creation, permission management, and schema binding. It also includes significant improvements to schema binding reliability and error reporting.

## Key Features

### 🗂️ Complete Folder Setup Workflow
- **New GitHub Action**: `Setup Folders and Bind Schemas` - A unified workflow that:
  - Creates folders for `v{version}_ingest`, `v{version}_staging`, and `v{version}_release`
  - Sets appropriate access permissions for each folder type
  - Generates schema binding configurations with real Synapse IDs
  - Downloads and binds schemas from the HTAN2 data model repository

### 🔧 Schema Binding Improvements

#### Fixed Critical Issues
- **Schema File Matching**: Fixed incorrect schema file matching that was causing wrong schemas to be bound (e.g., `BulkWESLevel1` was matching `BulkWESLevel3`)
- **Record-Based Schemas**: Added support for processing record-based schemas (`Demographics`, `Biospecimen`, `Diagnosis`, etc.) which were previously ignored
- **Biospecimen Mapping**: Fixed mapping from `Biospecimen` → `HTAN.BiospecimenData-v1.0.0-schema.json`
- **Special Schema Names**: Added proper mappings for:
  - `scRNA_seqLevel1/2/3_4` → `HTAN.scRNALevel1/2/3_4-v1.0.0-schema.json`
  - `SpatialTranscriptomicsLevel*` → `HTAN.SpatialLevel*-v1.0.0-schema.json`
  - `DigitalPathology` → `HTAN.DigitalPathologyData-v1.0.0-schema.json`

#### Enhanced Error Handling
- **Timeout Management**: Increased timeout for `scRNA_seqLevel3_4` from 5 to 15 minutes (schema is large)
- **Better Error Messages**: Added detailed error messages with diagnostic information for timeout failures
- **Comprehensive Reporting**: Enhanced binding results with:
  - Successful bindings count and details
  - Failed bindings with specific error messages
  - Skipped schemas with reasons
  - Summary artifacts uploaded to GitHub Actions

### 📁 Multi-Folder Type Support

- **All Folder Types**: Schema binding now works for `ingest`, `staging`, and `release` folders (previously only `staging`)
- **Flexible Filtering**: Added command-line options to `bind_schemas_workflow.py`:
  - `--schema-filter`: Bind only specific schemas
  - `--folder-type-filter`: Bind only to specific folder types
  - `--schema-version`: Specify schema version
  - `--config-file`: Use custom config file

### 🛠️ Code Organization

- **Refactored Structure**: Organized scripts into `scripts/manage/` and `scripts/setup/` directories
- **Shared Package**: Created `htan2_synapse` package with shared utilities:
  - `config.py`: Centralized configuration (team IDs, module lists)
  - `projects.py`: Project loading utilities
  - `teams.py`: Team lookup functions
  - `permissions.py`: Permission setting logic
  - `folders.py`: Folder creation utilities
- **Reduced Duplication**: Consolidated common logic to reduce code duplication

### 📚 Documentation

- **Consolidated README**: Merged all documentation into a single comprehensive `README.md`
- **Workflow Guides**: Added quick start guides for folder setup and schema binding
- **Troubleshooting**: Added troubleshooting section with common issues and solutions

## Files Changed

### New Files
- `.github/workflows/setup-folders-and-bind-schemas.yml` - Complete setup workflow
- `scripts/manage/setup_folders.py` - Master script for folder setup
- `scripts/manage/create_project_folders.py` - Folder creation logic
- `scripts/manage/update_folder_permissions.py` - Permission management
- `scripts/manage/update_schema_bindings.py` - Schema binding config generation
- `scripts/bind_failed_and_missing_folders.sh` - Helper script for retrying failed bindings
- `htan2_synapse/` - Shared Python package

### Modified Files
- `scripts/bind_schemas_workflow.py` - Added filtering, improved matching, record-based support
- `.github/workflows/bind-schemas-to-projects.yml` - Enhanced error reporting
- `merge_schema_bindings.py` - Fixed record-based schema handling
- `README.md` - Consolidated documentation

### Removed Files
- `WORKFLOW.md` - Merged into README
- `SCHEMA_BINDING_SETUP.md` - Merged into README
- `CONFIG_STRUCTURE.md` - Merged into README
- One-time cleanup scripts

## Usage

### Setting Up a New Version

1. **Via GitHub Actions** (Recommended):
   - Go to Actions → "Setup Folders and Bind Schemas"
   - Select `release-setup` branch
   - Click "Run workflow"
   - Enter version number (e.g., `8` for v8)
   - Enter schema version (e.g., `v1.0.0`)
   - Run the workflow

2. **Via Command Line**:
   ```bash
   python scripts/manage/setup_folders.py --version 8
   ```

### Binding Schemas

1. **Via GitHub Actions**:
   - Actions → "Bind Schemas to HTAN2 Projects"
   - Enter schema version (e.g., `v1.0.0`)
   - Run the workflow

2. **Via Command Line** (with filters):
   ```bash
   # Bind only failed schemas to release and ingest
   python scripts/bind_schemas_workflow.py \
     --schema-filter scRNA_seqLevel3_4 Biospecimen \
     --folder-type-filter v8_release v8_ingest \
     --schema-version v1.0.0
   ```

## Testing

- ✅ Tested folder creation for v8 (ingest, staging, release)
- ✅ Tested permission setting for all folder types
- ✅ Tested schema binding with v1.0.0 schemas
- ✅ Verified record-based schemas are processed
- ✅ Verified correct schema file matching
- ✅ Tested error handling and reporting

## Breaking Changes

None - this is a new feature branch that adds functionality without breaking existing workflows.

## Next Steps

After merging:
1. Test the workflow on a new version (e.g., v9)
2. Monitor schema binding results for any issues
3. Consider adding automated tests for schema matching logic

## Related Issues

- Fixes schema binding issues where wrong schemas were being matched
- Fixes missing record-based schema bindings
- Adds support for ingest and release folder schema bindings

