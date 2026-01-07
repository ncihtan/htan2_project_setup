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
5. ✅ Bind schemas to all v9_staging folders

### Option 2: Local Setup

```bash
# Complete setup for version 8
python scripts/manage/setup_folders.py --version 8
```

This single command does everything:
1. ✅ Creates all folders (v8_ingest, v8_staging, v8_release) with all modules
2. ✅ Sets access permissions for all folders
3. ✅ Updates schema binding file with real Synapse IDs
4. ✅ Merges staging folder bindings into `schema_binding_config.yml`

### What Gets Generated

After running the setup script, you'll have:

- **`folder_structure_v8.yml`** - Complete folder structure with all Synapse IDs
- **`schema_binding_v8.yml`** - Schema binding mappings with real IDs (staging only)
- **`schema_binding_config.yml`** - Updated with v8_staging bindings (used by GitHub Action)

### Manual Steps (Alternative)

If you prefer to run steps individually:

```bash
# 1. Create folders
python scripts/manage/create_project_folders.py --version 8

# 2. Set permissions
python scripts/manage/update_folder_permissions.py --version 8 --folder-type ingest
python scripts/manage/update_folder_permissions.py --version 8 --folder-type staging
python scripts/manage/update_folder_permissions.py --version 8 --folder-type release

# 3. Update schema bindings with real IDs
python scripts/manage/update_schema_bindings.py --version 8 --folder-type staging

# 4. Merge into config
python merge_schema_bindings.py \
  --schema-binding-file schema_binding_v8.yml \
  --folder-type-filter v8_staging
```

## Phase 2: Schema Binding (Manual)

Once folders are set up and `schema_binding_config.yml` is configured, schema binding is done manually when you're ready.

### How It Works

1. **Decide When to Bind** - You choose when to bind schemas (e.g., after schema release, after testing, etc.)
2. **Manual Trigger** - Go to GitHub → Actions → "Bind Schemas to HTAN2 Projects" → "Run workflow"
3. **Enter Schema Version** - Specify which schema version to bind (e.g., `v1.0.0`)
4. **Download Schemas** - Action downloads schemas from `ncihtan/htan2-data-model/JSON_Schemas/v1.0.0/`
5. **Bind Schemas** - Action reads `schema_binding_config.yml` and binds schemas to all listed folders
6. **Create Fileviews** - Action creates fileviews and wiki pages for each bound schema

### Manual Trigger Steps

1. Go to GitHub → Actions → "Bind Schemas to HTAN2 Projects"
2. Click "Run workflow"
3. Enter:
   - Schema version: `v1.0.0` (or the version you want)
   - Data model repo: `ncihtan/htan2-data-model`
4. Click "Run workflow" button

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
│   │   └── verify_permissions.py
│   │
│   └── schema/             # Schema binding
│       ├── bind_schemas_workflow.py
│       └── synapse_json_schema_bind.py
│
├── .github/workflows/
│   └── bind-schemas-to-projects.yml  # GitHub Action for schema binding
│
├── projects.yml                      # Project names and IDs
├── schema_binding_config.yml         # Master schema binding config
├── folder_structure_v8.yml           # v8 folder structure (generated)
└── schema_binding_v8.yml             # v8 schema bindings (generated)
```

## Key Files

- **`projects.yml`** - Project names and Synapse IDs
- **`schema_binding_config.yml`** - Master config for schema binding (used by GitHub Action)
  - Contains all version bindings (v8_staging, v9_staging, etc.)
  - Updated automatically when new versions are set up
- **`folder_structure_{version}.yml`** - Complete folder hierarchy with Synapse IDs (per version)
- **`schema_binding_{version}.yml`** - Schema binding mappings (staging only, per version)

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
2. **Schema Binding is Manual** - You control when to bind schemas via GitHub Actions UI
3. **Only Staging Gets Schemas** - Schemas are only bound to `{version}_staging/` folders
4. **Config is Cumulative** - `schema_binding_config.yml` contains all versions
5. **Action Uses Config** - GitHub Action reads `schema_binding_config.yml` to know where to bind
6. **Full Control** - You decide which schema version to bind and when

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

## Related Repositories

- [htan2-data-model](https://github.com/ncihtan/htan2-data-model) - Schema definition and generation
