# HTAN2 Synapse Project Setup

> [!NOTE]
> These scripts are intended for internal use by the HTAN DCC.  
> Resulting projects will not be publicly accessible

## Overview

This repository manages HTAN2 Synapse project setup, including:
- **Project and Team Creation** - Initial project setup
- **Folder Structure Creation** - Standardized folder hierarchies for data ingestion
- **Access Control** - Permission management for different folder types
- **Schema Binding** - Binding JSON schemas to project folders

## Workflow

The workflow has two main phases:

1. **Folder Setup** (One-time per version) - Creates folders, sets permissions, generates config
2. **Schema Binding** (Manual trigger) - Binds schemas to folders when you're ready

## Phase 1: Folder Setup (One-Time Per Version)

### Option 1: Complete Setup via GitHub Action (Recommended)

The easiest way to set up folders and bind schemas in one go:

1. Go to GitHub → Actions → "Setup Folders and Bind Schemas"
2. Click "Run workflow"
3. Enter:
   - **Version**: `9` (creates v9_ingest, v9_staging, v9_release)
   - **Schema version**: `v2.0.0` (or the schema version you want)
   - **Data model repo**: `ncihtan/htan2-data-model`
4. Click "Run workflow"

This single action will:
1. ✅ Create all folders (v9_ingest, v9_staging, v9_release) with all modules
2. ✅ Set access permissions for all folders
3. ✅ Update schema binding config with real Synapse IDs
4. ✅ Download schemas from the specified version
5. ✅ Bind schemas to all v9 folders (ingest, staging, release)

### Option 2: Local Setup

```bash
# Complete setup for version 8
python scripts/manage/setup_folders.py --version 8
```

This single command does everything:
1. ✅ Creates all folders (v8_ingest, v8_staging, v8_release) with all modules
2. ✅ Sets access permissions for all folders
3. ✅ Updates schema binding file with real Synapse IDs
4. ✅ Merges all folder bindings (ingest, staging, release) into `schema_binding_config.yml`

### What Gets Generated

After running the setup script, you'll have:

- **`schema_binding_v8.yml`** - Schema binding mappings with real IDs (ingest, staging, release)
- **`schema_binding_config.yml`** - Updated with v8 bindings for all folder types (used by GitHub Action)

**Note**: `folder_structure_v8.yml` is generated but not tracked in git (it's regenerated when needed).

### Manual Steps (Alternative)

If you prefer to run steps individually:

```bash
# 1. Create folders
python scripts/manage/create_project_folders.py --version 8

# 2. Set permissions
python scripts/manage/update_folder_permissions.py --version 8 --folder-type ingest
python scripts/manage/update_folder_permissions.py --version 8 --folder-type staging
python scripts/manage/update_folder_permissions.py --version 8 --folder-type release

# 3. Update schema bindings with real IDs (for all folder types)
python scripts/manage/update_schema_bindings.py --version 8 --folder-type ingest
python scripts/manage/update_schema_bindings.py --version 8 --folder-type staging
python scripts/manage/update_schema_bindings.py --version 8 --folder-type release

# 4. Merge into config (all folder types)
python merge_schema_bindings.py \
  --schema-binding-file schema_binding_v8.yml
```

## Phase 2: Schema Binding

### For New Releases

1. **Run the "Bind Schemas to HTAN2 Projects" GitHub Action**
   - Go to GitHub → Actions → "Bind Schemas to HTAN2 Projects" → "Run workflow"
   - Enter the requested inputs (schema version, data model repo, etc.)
   - Click "Run workflow"

2. **Extract Fileview IDs**
   ```bash
   python scripts/manage/update_fileview_ids.py
   ```
   This adds fileview IDs to the config for BigQuery (b1q).

### For Updating Existing Folders to Newer Data Model Version

**Important**: Each folder type (e.g., v8_ingest) takes approximately 3 hours to run, so trigger them separately.

1. **Run the "Bind Schemas to HTAN2 Projects" GitHub Action** for each folder type:
   - First: Filter to `v8_ingest` only
   - Then: Filter to `v8_staging` only  
   - Finally: Filter to `v8_release` only

2. **After all bindings complete, extract Fileview IDs**
   ```bash
   python scripts/manage/update_fileview_ids.py
   ```

## Project Structure

```
htan2_project_setup/
├── htan2_synapse/          # Shared utilities package
│   ├── config.py           # Team IDs, module definitions
│   ├── projects.py         # Project loading utilities
│   ├── teams.py            # Team utilities
│   ├── permissions.py     # Permission setting logic
│   └── folders.py          # Folder creation utilities
│
├── scripts/
│   ├── setup/              # One-time setup scripts
│   │   ├── create_projects.py
│   │   ├── create_teams.py
│   │   └── create_team_table.py
│   │
│   ├── manage/             # Operational scripts
│   │   ├── setup_folders.py          # Master setup script
│   │   ├── create_project_folders.py
│   │   ├── update_folder_permissions.py
│   │   ├── update_schema_bindings.py
│   │   ├── update_fileview_ids.py    # Extract fileview IDs from wikis
│   │   └── verify_permissions.py
│   │
│   ├── bind_schemas_workflow.py      # Schema binding workflow
│   └── synapse_json_schema_bind.py   # Schema binding utility
│
├── .github/workflows/
│   ├── setup-folders-and-bind-schemas.yml  # Complete setup workflow
│   └── bind-schemas-to-projects.yml        # Schema binding only workflow
│
├── projects.yml                      # Project names and IDs
├── schema_binding_config.yml         # Master schema binding config (includes fileview_ids)
└── schema_binding_v8.yml             # v8 schema bindings (generated)
```

## Key Files

- **`projects.yml`** - Project names and Synapse IDs
- **`schema_binding_config.yml`** - Master config for schema binding (used by GitHub Action)
  - Contains all version bindings (v8_ingest, v8_staging, v8_release, v9_ingest, etc.)
  - Includes `fileview_id` fields for each bound schema (extracted via `update_fileview_ids.py`)
  - Updated automatically when new versions are set up
- **`schema_binding_{version}.yml`** - Schema binding mappings (ingest, staging, release, per version)

## Access Permissions

### v{version}_ingest/
- **HTAN DCC Admins**: Admin
- **HTAN DCC**: Edit/Delete
- **ACT**: Edit/Delete
- **Contributors**: Edit/Delete
- **Others**: View Only

### v{version}_staging/
- **HTAN DCC Admins**: Admin
- **HTAN DCC**: Edit/Delete
- **ACT**: Edit/Delete
- **Contributors**: Modify (no Create/Delete)
- **Others**: View Only

### v{version}_release/
- **HTAN DCC Admins**: Admin
- **HTAN DCC**: View Only
- **ACT**: View Only
- **Contributors**: View Only
- **Others**: View Only

## Key Points

1. **Folder Setup is One-Time** - Run `setup_folders.py` once per version
2. **New Releases** - Run bind schemas action, then update fileview IDs script
3. **Updating Existing Folders** - Run bind schemas action separately for each folder type (ingest, staging, release) due to ~3 hour runtime per folder type
4. **Fileview IDs for BigQuery** - After bindings complete, run `update_fileview_ids.py` to extract fileview IDs and add them to the config for b1q
5. **Config Contains Fileview IDs** - `schema_binding_config.yml` includes `fileview_id` fields for each bound schema

## Prerequisites

- Python 3.x
- Synapse Python Client (`synapseclient`)
- PyYAML

Install dependencies:
```bash
pip install -r requirements.txt
```

## Troubleshooting

### Folders Created But No Schemas Bound

1. Check that `schema_binding_config.yml` has the folder mappings
2. Verify the schema version exists in htan2-data-model
3. Check GitHub Action logs for errors

### Schema Binding Fails

1. Verify schemas are registered in Synapse (handled by htan2-data-model)
2. Check that folder IDs in config are correct
3. Verify Synapse credentials in GitHub secrets

### Need to Update Permissions

```bash
python scripts/manage/update_folder_permissions.py --version 8 --folder-type staging
```

### Need to Rebind Schemas

Just trigger the GitHub Action again - it will rebind all schemas from the config.

### Need to Update Fileview IDs

After schema bindings are complete and verified, extract fileview IDs:

```bash
python scripts/manage/update_fileview_ids.py
```

This updates `schema_binding_config.yml` with `fileview_id` fields extracted from wikis.

## Related Repositories

- [htan2-data-model](https://github.com/ncihtan/htan2-data-model) - Schema definition and generation
