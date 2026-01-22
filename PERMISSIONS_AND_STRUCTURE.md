# HTAN2 Folder Structure and Access Permissions

## Folder Structure

Each project has three main folder types, each containing the same module structure:

```
{Project}/
├── v{version}_ingest/
│   ├── Clinical/
│   │   ├── Demographics/
│   │   ├── Diagnosis/
│   │   ├── Therapy/
│   │   ├── FollowUp/
│   │   ├── MolecularTest/
│   │   ├── Exposure/
│   │   ├── FamilyHistory/
│   │   └── VitalStatus/
│   ├── Biospecimen/
│   ├── WES/
│   │   ├── Level_1/
│   │   ├── Level_2/
│   │   └── Level_3/
│   ├── scRNA_seq/
│   │   ├── Level_1/
│   │   ├── Level_2/
│   │   └── Level_3_4/
│   ├── Imaging/
│   │   ├── DigitalPathology/
│   │   └── MultiplexMicroscopy/
│   │       ├── Level_2/
│   │       ├── Level_3/
│   │       └── Level_4/
│   └── SpatialOmics/
│       ├── Level_1/
│       ├── Level_3/
│       ├── Level_4/
│       └── Panel/
│
├── v{version}_staging/
│   └── [Same structure as ingest]
│
└── v{version}_release/
    └── [Same structure as ingest]
```

## Access Permissions by Folder Type

### v{version}_ingest/

**Purpose**: Initial data upload area where contributors can upload and manage their data.

| Role | Permissions | Access Type |
|------|-------------|-------------|
| **HTAN DCC Admins** | Full Admin | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE`, `MODERATE`, `CHANGE_PERMISSIONS`, `CHANGE_SETTINGS` |
| **HTAN DCC** | Edit/Delete | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE` |
| **ACT** (Access & Compliance Team) | Edit/Delete | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE` |
| **Contributors** (e.g., HTAN2_Ovarian_contributors) | Edit/Delete | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE` |
| **All Other Users** | View Only | `READ`, `DOWNLOAD` (inherited from project) |

**Use Case**: Contributors upload raw data files here. DCC and ACT can manage and curate.

---

### v{version}_staging/

**Purpose**: Curated data area where DCC reviews and prepares data for release. Contributors can modify existing files but cannot create new ones.

| Role | Permissions | Access Type |
|------|-------------|-------------|
| **HTAN DCC Admins** | Full Admin | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE`, `MODERATE`, `CHANGE_PERMISSIONS`, `CHANGE_SETTINGS` |
| **HTAN DCC** | Edit/Delete | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE` |
| **ACT** (Access & Compliance Team) | Edit/Delete | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE` |
| **Contributors** (e.g., HTAN2_Ovarian_contributors) | **Modify Only** | `READ`, `DOWNLOAD`, `UPDATE` |
| **All Other Users** | View Only | `READ`, `DOWNLOAD` (inherited from project) |

**Use Case**: DCC curates and validates data. Contributors can update existing files but cannot add new files or delete.

**Note**: Contributors have `UPDATE` permission but **NOT** `CREATE` or `DELETE`. This allows them to modify existing files but prevents them from adding new files or removing files.

---

### v{version}_release/

**Purpose**: Final released data area. Read-only for everyone except DCC Admins.

| Role | Permissions | Access Type |
|------|-------------|-------------|
| **HTAN DCC Admins** | Full Admin | `READ`, `DOWNLOAD`, `CREATE`, `UPDATE`, `DELETE`, `MODERATE`, `CHANGE_PERMISSIONS`, `CHANGE_SETTINGS` |
| **HTAN DCC** | View Only | `READ`, `DOWNLOAD` |
| **ACT** (Access & Compliance Team) | View Only | `READ`, `DOWNLOAD` |
| **Contributors** (e.g., HTAN2_Ovarian_contributors) | View Only | `READ`, `DOWNLOAD` |
| **All Other Users** | View Only | `READ`, `DOWNLOAD` |

**Use Case**: Final released data that is publicly accessible (within the project). Only DCC Admins can modify.

**Note**: Permissions are set explicitly using ACL (Access Control List) to break inheritance and ensure strict read-only access.

---

## Team IDs

- **HTAN DCC Admins Team ID**: `3497313`
- **HTAN DCC Team ID**: `3391844`
- **ACT Team ID**: `464532`
- **Contributor Teams**: Dynamically found based on project name (e.g., `HTAN2_Ovarian_contributors`)

## Module Definitions

### Record-Based Modules (Clinical Data)

These modules contain structured data records:

- **Clinical**: Contains 8 subfolders for different clinical data types
  - Demographics
  - Diagnosis
  - Therapy
  - FollowUp
  - MolecularTest
  - Exposure
  - FamilyHistory
  - VitalStatus

- **Biospecimen**: Single folder (no subfolders)
  - Contains BiospecimenData schema

### File-Based Modules (Assay Data)

These modules contain file-based assay data with levels:

- **WES** (Whole Exome Sequencing)
  - Level_1
  - Level_2
  - Level_3

- **scRNA_seq** (Single-Cell RNA Sequencing)
  - Level_1
  - Level_2
  - Level_3_4 (combined level)

- **Imaging**
  - **DigitalPathology**: Single folder (no subfolders)
  - **MultiplexMicroscopy**: Contains 3 levels
    - Level_2
    - Level_3
    - Level_4

- **SpatialOmics**
  - Level_1
  - Level_3
  - Level_4
  - Panel

## Schema Binding

All three folder types (ingest, staging, release) receive schema bindings:

- **Record-based schemas** are bound to their respective folders (e.g., `Demographics` schema → `Clinical/Demographics/`)
- **File-based schemas** are bound to their respective folders (e.g., `BulkWESLevel1` schema → `WES/Level_1/`)
- **Biospecimen** schema is bound to the `Biospecimen/` folder
- **DigitalPathology** schema is bound to `Imaging/DigitalPathology/`
- **MultiplexMicroscopy** schemas are bound to their respective level folders

## Permission Summary Table

| Folder Type | DCC Admins | DCC | ACT | Contributors | Others |
|-------------|------------|-----|-----|--------------|--------|
| **ingest** | Admin | Edit/Delete | Edit/Delete | Edit/Delete | View |
| **staging** | Admin | Edit/Delete | Edit/Delete | **Modify Only** | View |
| **release** | Admin | View Only | View Only | View Only | View |

**Key Difference**: 
- **staging** contributors can only **modify** (UPDATE) existing files, not create or delete
- **release** is read-only for everyone except DCC Admins


