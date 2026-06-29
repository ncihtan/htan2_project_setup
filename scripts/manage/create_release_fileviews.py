#!/usr/bin/env python3
"""Bootstrap release fileviews for all release folders.

Discovers v8_release folder synIDs from Synapse, upserts them into
schema_binding_config.yml, then:

  File-based:   creates an EntityView scoped to the release folder,
                copying column definitions from the paired staging EntityView.

  Record-based: adds the release entry to config with fileview_id=None.
                promote_staging_to_release.py populates fileview_id when
                a folder's records are approved for release.

Run once before the first release promotion, or to pick up new schemas/projects.
Existing release entries are overwritten (synapse_id updated, fileview_id preserved).

Usage:
  python scripts/manage/create_release_fileviews.py [--dry-run] [--project-name HTAN2_CRC]
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import synapseclient
import yaml

sys.path.insert(0, ".")
from htan2_synapse import (
    load_projects,
    RECORD_BASED_MODULES,
    FILE_BASED_MODULES,
    IMAGING_SUBFOLDERS,
    IMAGING_RECORD_BASED_SUBFOLDERS,
    SPATIAL_RECORD_BASED_SUBFOLDERS,
)

CONFIG_PATH = "schema_binding_config.yml"
VERSION = "v8"

# Schema name map: (module, level) → schema_name, matching update_schema_bindings.py logic
def _schema_name(module, level):
    if module == "WES":
        return {"Level_1": "BulkWESLevel1", "Level_2": "BulkWESLevel2", "Level_3": "BulkWESLevel3"}.get(level, level)
    if module == "scRNA_seq":
        return f"scRNA_seqLevel{level.replace('Level_', '').replace('_', '_')}"
    if module == "SpatialOmics":
        return f"SpatialOmicsLevel{level.replace('Level_', '')}"
    if module == "MultiplexMicroscopy":
        return f"MultiplexMicroscopy{level.replace('_', '')}"
    return level


def save_config(config, path):
    with open(path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def find_folder_id(syn, parent_id, folder_name):
    """Find a folder synID by name under a parent. Returns None if not found."""
    try:
        for child in syn.getChildren(parent_id, includeTypes=["folder"]):
            if child["name"] == folder_name:
                return child["id"]
    except Exception as e:
        print(f"    WARNING: Could not list children of {parent_id}: {e}")
    return None


def get_ev_column_ids(syn, ev_id):
    """Return column ID list from an existing EntityView."""
    ev_data = syn.restGET(f"/entity/{ev_id}")
    return ev_data.get("columnIds", [])


def create_release_ev(syn, name, parent_id, scope_id, col_ids):
    """Create an EntityView in Synapse scoped to a release folder."""
    body = {
        "name": name,
        "parentId": parent_id,
        "concreteType": "org.sagebionetworks.repo.model.table.EntityView",
        "scopeIds": [scope_id.replace("syn", "")],
        "viewTypeMask": 1,
        "columnIds": col_ids,
    }
    return syn.restPOST("/entity", body=json.dumps(body))


def upsert_release_entry(config, section, schema_name, project_name, subfolder, synapse_id):
    """Add or overwrite a release entry in config. Returns the live entry dict."""
    schema_data = config["schema_bindings"][section].setdefault(schema_name, {"projects": []})
    for entry in schema_data["projects"]:
        if entry["name"] == project_name and "release" in entry.get("subfolder", ""):
            entry["synapse_id"] = synapse_id
            entry["subfolder"] = subfolder
            entry.setdefault("fileview_id", None)
            return entry
    entry = {"name": project_name, "subfolder": subfolder, "synapse_id": synapse_id, "fileview_id": None}
    schema_data["projects"].append(entry)
    return entry


def build_staging_ev_map(config):
    """Return {(schema_name, project_name): staging_ev_id} for file-based schemas."""
    result = {}
    for schema_name, schema_data in config["schema_bindings"].get("file_based", {}).items():
        for entry in schema_data.get("projects", []):
            if "staging" in entry.get("subfolder", "") and entry.get("fileview_id"):
                result[(schema_name, entry["name"])] = entry["fileview_id"]
    return result


def discover_and_upsert(syn, config, projects, dry_run, project_filter):
    """Walk Synapse v8_release folders, upsert into config.

    Returns list of (section, schema_name, entry_dict) — entries are live config
    dicts for real runs, or standalone dicts for dry runs.
    """
    discovered = []

    for project_name, project_id in sorted(projects.items()):
        if project_filter and project_name != project_filter:
            continue
        print(f"\n  {project_name} ({project_id})")

        release_root = find_folder_id(syn, project_id, f"{VERSION}_release")
        if not release_root:
            print(f"    WARNING: no {VERSION}_release folder found, skipping")
            continue

        def record(section, schema_name, subfolder_path, folder_id):
            if dry_run:
                entry = {"name": project_name, "subfolder": subfolder_path,
                         "synapse_id": folder_id, "fileview_id": None}
            else:
                entry = upsert_release_entry(config, section, schema_name,
                                             project_name, subfolder_path, folder_id)
            discovered.append((section, schema_name, entry))
            print(f"    {subfolder_path}: {folder_id}")

        # Clinical (record-based)
        clinical_id = find_folder_id(syn, release_root, "Clinical")
        if clinical_id:
            for subfolder in RECORD_BASED_MODULES.get("Clinical", []):
                fid = find_folder_id(syn, clinical_id, subfolder)
                if fid:
                    record("record_based", subfolder,
                           f"{VERSION}_release/Clinical/{subfolder}", fid)

        # Biospecimen (record-based)
        bio_id = find_folder_id(syn, release_root, "Biospecimen")
        if bio_id:
            record("record_based", "Biospecimen",
                   f"{VERSION}_release/Biospecimen", bio_id)

        # WES, scRNA_seq (file-based levels)
        for module in ["WES", "scRNA_seq"]:
            module_id = find_folder_id(syn, release_root, module)
            if not module_id:
                continue
            for level in FILE_BASED_MODULES.get(module, []):
                fid = find_folder_id(syn, module_id, level)
                if fid:
                    record("file_based", _schema_name(module, level),
                           f"{VERSION}_release/{module}/{level}", fid)

        # SpatialOmics: file-based levels + Panel (record-based)
        so_id = find_folder_id(syn, release_root, "SpatialOmics")
        if so_id:
            for level in FILE_BASED_MODULES.get("SpatialOmics", []):
                fid = find_folder_id(syn, so_id, level)
                if fid:
                    record("file_based", _schema_name("SpatialOmics", level),
                           f"{VERSION}_release/SpatialOmics/{level}", fid)
            for rs_sub in SPATIAL_RECORD_BASED_SUBFOLDERS.get("SpatialOmics", []):
                fid = find_folder_id(syn, so_id, rs_sub)
                if fid:
                    schema_name = "SpatialPanel" if rs_sub == "Panel" else rs_sub
                    record("record_based", schema_name,
                           f"{VERSION}_release/SpatialOmics/{rs_sub}", fid)

        # Imaging
        imaging_id = find_folder_id(syn, release_root, "Imaging")
        if imaging_id:
            dp_id = find_folder_id(syn, imaging_id, "DigitalPathology")
            if dp_id:
                record("file_based", "DigitalPathology",
                       f"{VERSION}_release/Imaging/DigitalPathology", dp_id)

            mm_id = find_folder_id(syn, imaging_id, "MultiplexMicroscopy")
            if mm_id:
                for level in IMAGING_SUBFOLDERS.get("MultiplexMicroscopy", []):
                    fid = find_folder_id(syn, mm_id, level)
                    if fid:
                        record("file_based", _schema_name("MultiplexMicroscopy", level),
                               f"{VERSION}_release/Imaging/MultiplexMicroscopy/{level}", fid)
                for rs_sub in IMAGING_RECORD_BASED_SUBFOLDERS.get("MultiplexMicroscopy", []):
                    fid = find_folder_id(syn, mm_id, rs_sub)
                    if fid:
                        record("record_based", rs_sub,
                               f"{VERSION}_release/Imaging/MultiplexMicroscopy/{rs_sub}", fid)

    return discovered


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap release fileviews (EntityViews for file-based, entries for record-based)."
    )
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating anything in Synapse")
    parser.add_argument("--project-name", help="Limit to one project (e.g. HTAN2_CRC)")
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    pat = os.environ.get("SYNAPSE_PAT")
    if pat:
        syn.login(authToken=pat, silent=True)
    else:
        syn.login(silent=True)

    with open(args.config) as f:
        config = yaml.safe_load(f)

    projects = load_projects("projects.yml")
    staging_ev_map = build_staging_ev_map(config)

    # Phase 1: discover release folders and upsert into config
    print("\n=== Discovering release folders in Synapse ===")
    discovered = discover_and_upsert(syn, config, projects, args.dry_run, args.project_name)

    if not args.dry_run and discovered:
        save_config(config, args.config)

    # Phase 2: create EntityViews for file-based entries
    created = skipped = errors = 0

    print("\n=== File-based: creating release EntityViews ===")
    for section, schema_name, entry in discovered:
        if section != "file_based":
            continue

        label = f"  {entry['name']}/{entry['subfolder']} ({entry['synapse_id']})"
        if entry.get("fileview_id"):
            print(f"{label}: already has fileview_id {entry['fileview_id']}, skipping")
            skipped += 1
            continue

        staging_ev_id = staging_ev_map.get((schema_name, entry["name"]))
        if not staging_ev_id:
            print(f"{label}: WARNING — no staging EntityView for ({schema_name}, {entry['name']}), skipping")
            errors += 1
            continue

        col_ids = get_ev_column_ids(syn, staging_ev_id)
        ev_name = f"{schema_name}_Release_Fileview"

        if args.dry_run:
            print(f"{label}: [DRY RUN] would create EntityView '{ev_name}' ({len(col_ids)} cols from {staging_ev_id})")
            created += 1
            continue

        try:
            result = create_release_ev(syn, ev_name, entry["synapse_id"], entry["synapse_id"], col_ids)
            new_id = result["id"]
            entry["fileview_id"] = new_id
            print(f"{label}: created EntityView {new_id}")
            created += 1
            save_config(config, args.config)
        except Exception as e:
            if "409" in str(e) or "already exists" in str(e).lower():
                existing_id = syn.findEntityId(ev_name, parent=entry["synapse_id"])
                if existing_id:
                    entry["fileview_id"] = existing_id
                    print(f"{label}: found existing EntityView {existing_id}")
                    created += 1
                    save_config(config, args.config)
                else:
                    print(f"{label}: ERROR — 409 but could not find existing EV")
                    errors += 1
            else:
                print(f"{label}: ERROR — {e}")
                errors += 1

    # Phase 3: report record-based entries (fileview_id set by promote script)
    print("\n=== Record-based: entries upserted (fileview_id set at release promotion) ===")
    for section, schema_name, entry in discovered:
        if section != "record_based":
            continue
        action = "[DRY RUN] would upsert" if args.dry_run else "upserted"
        print(f"  {entry['name']}/{entry['subfolder']} ({entry['synapse_id']}): {action}")

    rb_count = sum(1 for s, _, _ in discovered if s == "record_based")
    print(
        f"\nSummary: {len(discovered)} release folders discovered "
        f"({len(discovered) - rb_count} file-based, {rb_count} record-based), "
        f"{created} EVs created, {skipped} skipped, {errors} errors"
    )
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
