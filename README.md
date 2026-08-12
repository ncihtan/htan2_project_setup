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
- **Curation Tasks** - Creating curation tasks and fileviews for metadata entry

## Complete Workflow

For each new data release cycle (e.g., v9):

```
1. Create folders         →  setup_folders.py or GHA
2. Bind schemas           →  GHA: "Bind Schemas to HTAN2 Projects"
3. Create curation tasks  →  GHA: "Create Curation Tasks"  ← auto-creates fileviews
4. Update config IDs      →  update_fileview_ids.py (run by GHA in step 3)
```

---

## Phase 1: Folder Setup

### GitHub Actions (Recommended)

1. Go to **Actions → "Setup Folders and Bind Schemas"** → Run workflow
2. Enter:
   - **Version**: `9` (creates v9_ingest, v9_staging, v9_release)
   - **Schema version**: `v2.0.0`
   - **Data model repo**: `ncihtan/htan2-data-model`

### Local

```bash
python scripts/manage/setup_folders.py --version 8
```

This creates all folders (ingest/staging/release), sets permissions, and updates `schema_binding_config.yml`.

---

## Phase 2: Schema Binding

### GitHub Actions (Recommended)

Go to **Actions → "Bind Schemas to HTAN2 Projects"** → Run workflow.

> Each folder type takes ~3 hours. For updates, run ingest/staging/release separately using the folder type filter.

### Local

```bash
python scripts/bind_schemas_workflow.py --schema-version v1.0.0
```

---

## Phase 3: Curation Tasks

Curation tasks enable metadata entry in the Synapse curator UI. Creating a task automatically creates the associated EntityView (file-based) or RecordSet (record-based).

### GitHub Actions (Recommended)

Go to **Actions → "Create Curation Tasks"** → Run workflow.

Inputs:
- **subfolder_filter**: e.g. `v8_ingest` (leave empty for all)
- **project_name**: e.g. `HTAN2_CRC` (leave empty for all)
- **record_based_only**: only create Clinical/Biospecimen tasks
- **dry_run**: preview without creating
- **force**: skip the task-exists check (use to replace old-style tasks)

After creating tasks, the workflow automatically runs `update_fileview_ids.py` and commits the updated `schema_binding_config.yml`.

### Local

```bash
# Create tasks for a specific project and folder type
python scripts/manage/create_curation_tasks_from_config.py \
  --project-name HTAN2_CRC \
  --subfolder-filter v8_ingest

# Then update the config with the new IDs
python scripts/manage/update_fileview_ids.py \
  --project-name HTAN2_CRC \
  --subfolder-filter v8_ingest
```

> **Note**: If a project has old-style tasks that are blocking new curator tasks, delete them first with `scripts/manage/delete_all_curation_tasks_and_fileviews.py`, then re-run with `--force`.

### RecordSet upsert keys (primary keys)

Every record-based task creates a RecordSet whose **upsert key** is its primary key: Synapse
treats an incoming row whose key matches an existing row as an *update* rather than an
*insert*. An under-specified key therefore does not error — it silently merges rows that
should be distinct.

Keys are declared per schema in [`htan2_synapse/module_registry.yml`](htan2_synapse/module_registry.yml)
and validated when `module_structure.yml` is regenerated: every key must be a **required**
property of the data-model schema, so generation fails loudly on a typo, a case mismatch, or
a nullable column. Tables holding several rows per parent (Diagnosis, Therapy, FollowUp,
MolecularTest, ChannelMetadata, SpatialPanel, MolecularAssignment) carry discriminator
columns alongside the parent ID.

Tasks created before this was registry-driven were keyed on whichever property sorted first
in the schema's `required` array — Demographics was keyed on `ETHNIC_GROUP`. Fixing the
creation path does not touch RecordSets that already exist, but they can be **patched in
place** — no task deletion, no recreation, RecordSet contents and grids untouched:

```bash
# Report only — writes nothing
python scripts/manage/fix_recordset_upsert_keys.py

# Include a scan for rows that collide under the corrected key (slow: reads every CSV)
python scripts/manage/fix_recordset_upsert_keys.py --verify-rows

# Apply
python scripts/manage/fix_recordset_upsert_keys.py --apply --subfolder-filter v9_ingest
```

`--verify-rows` is worth running once: it reports rows that cannot be told apart under the
corrected key, which is the only way to see whether a wrong key already merged real records.

### Grid session authorization (shared vs independent curation)

> **Known gap, handled out-of-band — not yet part of this repo's tooling.**

A curation task with no authorization recommendation makes the curator client create a
**private grid session per user**, so contributors each work alone rather than on a shared
grid. The mode is a property of the task (`suggested_authorization_mode`):

- `SESSION_OWNER` — only the session owner, or members of the owner's team, can join
- `SOURCE_BENEFACTOR` — anyone with EDIT (UPDATE) on the curated data's benefactor can join

`SOURCE_BENEFACTOR` is generally what HTAN wants: the per-atlas contributors team already
holds UPDATE on the ingest folders, so it grants that team a shared session with no
project→team mapping to maintain.

It is **not in any released Synapse client** (checked through v4.13.0) — only on `develop` —
so it is currently applied by a local one-off script run from a dev virtualenv, and is
deliberately absent from `requirements.txt` and the workflows. Fold it into
`create_curation_tasks_from_config.py` once it ships in a release.

> Until then: the released client does not serialize `suggested_authorization_mode`, and
> creating a task whose `dataType` already exists *updates* that task. So if "Create Curation
> Tasks" recreates a task, the mode is silently dropped and has to be re-applied.

---

## Folder Structure

Each release version (v8, v9, …) has three folder types:

```
vN_ingest/    Active data ingestion
vN_staging/   Review and validation
vN_release/   Finalized, locked data
```

Each folder type contains:

```
vN_ingest/
├── Clinical/
│   ├── Demographics/
│   ├── Diagnosis/
│   ├── Therapy/
│   ├── FollowUp/
│   ├── MolecularTest/
│   ├── Exposure/
│   ├── FamilyHistory/
│   └── VitalStatus/
├── Biospecimen/
├── WES/
│   ├── Level_1/
│   ├── Level_2/
│   └── Level_3/
├── scRNA_seq/
│   ├── Level_1/
│   ├── Level_2/
│   └── Level_3_4/
├── SpatialOmics/
│   ├── Level_1/
│   ├── Level_3/
│   ├── Level_4/
│   └── Panel/
└── Imaging/
    ├── DigitalPathology/
    └── MultiplexMicroscopy/
        ├── Level_2/
        ├── Level_3/
        └── Level_4/
```

Schemas are bound to leaf subfolders only (not top-level folders like `Clinical/`).

---

## Access Permissions

| Folder type | DCC Admins | DCC | ACT | Contributors | Others |
|---|---|---|---|---|---|
| `vN_ingest` | Admin | Edit/Delete | Edit/Delete | Edit/Delete | View |
| `vN_staging` | Admin | Edit/Delete | Edit/Delete | Modify | View |
| `vN_release` | Admin | View | View | View | View |

---

## Config Files

### `schema_binding_config.yml`

Master config used by all workflows. Contains per-project folder IDs and view IDs for every schema and folder type:

```yaml
schema_bindings:
  file_based:
    BulkWESLevel1:
      projects:
        - name: HTAN2_CRC
          subfolder: v8_ingest/WES/Level_1
          synapse_id: syn72098904      # upload folder
          fileview_id: syn72243189     # EntityView (populated by update_fileview_ids.py)
  record_based:
    Demographics:
      projects:
        - name: HTAN2_CRC
          subfolder: v8_ingest/Clinical/Demographics
          synapse_id: syn72101963      # upload folder
          fileview_id: syn72115896     # RecordSet (populated by update_fileview_ids.py)
```

### Other files

- **`projects.yml`** - Project names and Synapse IDs
- **`schema_binding_{version}.yml`** - Version-specific binding mappings (generated, not tracked)

---

## Project Structure

```
htan2_project_setup/
├── htan2_synapse/                    # Shared utilities package
│   ├── config.py                     # Team IDs, module definitions
│   ├── projects.py
│   ├── teams.py
│   ├── permissions.py
│   └── folders.py
│
├── scripts/
│   ├── setup/                        # One-time setup scripts
│   │   ├── create_projects.py
│   │   ├── create_teams.py
│   │   └── create_team_table.py
│   │
│   ├── manage/                       # Operational scripts
│   │   ├── setup_folders.py          # Master setup (folders + permissions + config)
│   │   ├── create_project_folders.py
│   │   ├── update_folder_permissions.py
│   │   ├── update_schema_bindings.py
│   │   ├── create_curation_tasks_from_config.py  # Create curation tasks + fileviews
│   │   ├── update_fileview_ids.py    # Discover and save fileview/recordset IDs
│   │   ├── fix_recordset_upsert_keys.py  # Repair primary keys on existing RecordSets
│   │   ├── set_curation_task_authorization_mode.py  # Shared grid sessions (dev client only)
│   │   ├── delete_all_curation_tasks_and_fileviews.py
│   │   └── testing/                  # Testing/one-off scripts (not for production)
│   │
│   ├── bind_schemas_workflow.py      # Schema binding orchestration
│   └── synapse_json_schema_bind.py  # Low-level schema binding utility
│
├── .github/workflows/
│   ├── setup-folders-and-bind-schemas.yml
│   ├── bind-schemas-to-projects.yml
│   └── create-curation-tasks.yml    # Create tasks + update config
│
├── merge_schema_bindings.py         # Merge version configs into master
├── check_curation_task_schemas.py   # Inspect schema versions on tasks
├── projects.yml
└── schema_binding_config.yml
```

---

## Prerequisites

```bash
pip install -r requirements.txt
# requirements include synapseclient[curator] — the [curator] extra is required
```

---

## Troubleshooting

### Schema not bound to folder

Run schema binding first:
```bash
python scripts/bind_schemas_workflow.py --schema-version v1.0.0 \
  --folder-type-filter v8_ingest
```

### Curation task already exists but fileview IDs are missing

The existing task may be an old-style task (pre-curator extension). Delete it and recreate:
```bash
python scripts/manage/delete_all_curation_tasks_and_fileviews.py --project-id <syn_id>
python scripts/manage/create_curation_tasks_from_config.py \
  --project-name <name> --subfolder-filter v8_ingest
python scripts/manage/update_fileview_ids.py --project-name <name>
```

Or re-run the "Create Curation Tasks" GitHub Action with **force** enabled.

### Need to check which schema version is bound to tasks

```bash
python check_curation_task_schemas.py <project_id>
```

### Schema was rebound but curation task still uses the old schema version

Curation tasks cache the schema URI at creation time. Rebinding the schema to the
folder does **not** update the task — you must delete the old task and create a new one.

Use the "Create Curation Tasks" GitHub Action with **delete_first** enabled (optionally
scoped with **project_name**). This will:
1. Delete existing tasks and their fileviews
2. Create new tasks against the current bound schema
3. Commit updated fileview IDs to config

Or locally:
```bash
python scripts/manage/delete_all_curation_tasks_and_fileviews.py \
  --all-from-config --project-name HTAN2_CRC

python scripts/manage/create_curation_tasks_from_config.py \
  --project-name HTAN2_CRC --subfolder-filter v8_ingest

python scripts/manage/update_fileview_ids.py \
  --project-name HTAN2_CRC --subfolder-filter v8_ingest
```

### Need to rebind schemas

Trigger the "Bind Schemas to HTAN2 Projects" GitHub Action again.

### Need to update permissions

```bash
python scripts/manage/update_folder_permissions.py --version 8 --folder-type staging
```

---

## Related Repositories

- [htan2-data-model](https://github.com/ncihtan/htan2-data-model) - Schema definition and generation
