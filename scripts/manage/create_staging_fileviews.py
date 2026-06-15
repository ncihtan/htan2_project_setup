#!/usr/bin/env python3
"""Bootstrap staging fileviews for all staging folders.

For file-based schemas: creates an EntityView scoped to the staging folder,
  copying column definitions from the paired ingest EntityView.

For record-based schemas: sets fileview_id to the ingest RecordSet synID
  (no Synapse entity created — the ingest RS is the source of truth).

Also migrates any legacy promoted_snapshot entries to the new versioned synID
format (e.g. syn74149099.8) in fileview_id.

Run once before the first promotion, or to pick up new schemas/projects.

Usage:
  python scripts/manage/create_staging_fileviews.py [--dry-run] [--project-name htan2-testing1]
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import synapseclient
import yaml

CONFIG_PATH = "schema_binding_config.yml"


def save_config(config, path):
    with open(path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def build_ingest_ev_map(config):
    """Return maps from project name to ingest folder synID and ingest fileview_id."""
    file_map = {}   # (schema_name, project_name) -> (ingest_folder_id, ingest_ev_id)
    record_map = {} # (schema_name, project_name) -> ingest_rs_id
    for schema_name, schema_data in config["schema_bindings"].get("file_based", {}).items():
        by_project = defaultdict(dict)
        for entry in schema_data.get("projects", []):
            if "ingest" in entry.get("subfolder", ""):
                by_project[entry["name"]]["folder"] = entry["synapse_id"]
                by_project[entry["name"]]["ev"] = entry.get("fileview_id")
        for project, ids in by_project.items():
            if ids.get("folder") and ids.get("ev"):
                file_map[(schema_name, project)] = (ids["folder"], ids["ev"])
    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        by_project = defaultdict(dict)
        for entry in schema_data.get("projects", []):
            if "ingest" in entry.get("subfolder", ""):
                by_project[entry["name"]]["rs"] = entry.get("fileview_id")
        for project, ids in by_project.items():
            if ids.get("rs"):
                record_map[(schema_name, project)] = ids["rs"]
    return file_map, record_map


def get_ev_column_ids(syn, ev_id):
    """Return list of column ID strings from an existing EntityView."""
    ev_data = syn.restGET(f"/entity/{ev_id}")
    return ev_data.get("columnIds", [])


def create_staging_ev(syn, name, parent_id, scope_id, col_ids, view_type_mask=1):
    """Create an EntityView in Synapse. Returns the new entity dict."""
    body = {
        "name": name,
        "parentId": parent_id,
        "concreteType": "org.sagebionetworks.repo.model.table.EntityView",
        "scopeIds": [scope_id.replace("syn", "")],
        "viewTypeMask": view_type_mask,
        "columnIds": col_ids,
    }
    return syn.restPOST("/entity", body=json.dumps(body))


def main():
    parser = argparse.ArgumentParser(
        description="Bootstrap staging fileviews (EntityViews for file-based, RS refs for record-based)."
    )
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating anything")
    parser.add_argument("--project-name", help="Limit to one project (e.g. htan2-testing1)")
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    pat = os.environ.get("SYNAPSE_PAT")
    if pat:
        syn.login(authToken=pat, silent=True)
    else:
        syn.login(silent=True)

    with open(args.config) as f:
        config = yaml.safe_load(f)

    file_map, record_map = build_ingest_ev_map(config)
    created = skipped = migrated = errors = 0

    # ── File-based: create staging EntityViews ────────────────────────────────
    print("\n=== File-based schemas ===")
    for schema_name, schema_data in config["schema_bindings"].get("file_based", {}).items():
        for entry in schema_data.get("projects", []):
            if "staging" not in entry.get("subfolder", ""):
                continue
            if args.project_name and entry["name"] != args.project_name:
                continue

            label = f"  {entry['name']}/{entry['subfolder']} ({entry['synapse_id']})"
            existing = entry.get("fileview_id")

            if existing and not str(existing).startswith("syn75244"):
                # Already has a non-legacy staging EV ID — skip
                print(f"{label}: already has fileview_id {existing}, skipping")
                skipped += 1
                continue

            ingest_info = file_map.get((schema_name, entry["name"]))
            if not ingest_info:
                print(f"{label}: WARNING — no paired ingest EntityView found, skipping")
                errors += 1
                continue

            _ingest_folder, ingest_ev_id = ingest_info
            col_ids = get_ev_column_ids(syn, ingest_ev_id)
            ev_name = f"{schema_name}_Staging_Fileview"

            if args.dry_run:
                print(f"{label}: [DRY RUN] would create EntityView '{ev_name}' ({len(col_ids)} cols from {ingest_ev_id})")
                created += 1
                continue

            try:
                result = create_staging_ev(syn, ev_name, entry["synapse_id"], entry["synapse_id"], col_ids)
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

    # ── Record-based: point fileview_id at ingest RecordSet ──────────────────
    print("\n=== Record-based schemas ===")
    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        for entry in schema_data.get("projects", []):
            if "staging" not in entry.get("subfolder", ""):
                continue
            if args.project_name and entry["name"] != args.project_name:
                continue

            label = f"  {entry['name']}/{entry['subfolder']} ({entry['synapse_id']})"
            ingest_rs_id = record_map.get((schema_name, entry["name"]))
            if not ingest_rs_id:
                print(f"{label}: WARNING — no ingest RecordSet found, skipping")
                errors += 1
                continue

            # Migrate legacy promoted_snapshot to versioned fileview_id
            old_snap = entry.pop("promoted_snapshot", None)
            if old_snap:
                versioned = f"{old_snap['synid']}.{old_snap['version']}"
                entry["fileview_id"] = versioned
                print(f"{label}: migrated promoted_snapshot → fileview_id {versioned}")
                migrated += 1
                if not args.dry_run:
                    save_config(config, args.config)
                continue

            existing = entry.get("fileview_id")
            if existing and existing != ingest_rs_id:
                # Has a versioned ID already (synid.version) — leave it, promotion will update
                print(f"{label}: has fileview_id {existing}, skipping")
                skipped += 1
                continue

            if existing == ingest_rs_id:
                print(f"{label}: already points to ingest RS {ingest_rs_id}, skipping")
                skipped += 1
                continue

            if args.dry_run:
                print(f"{label}: [DRY RUN] would set fileview_id → {ingest_rs_id}")
                created += 1
                continue

            entry["fileview_id"] = ingest_rs_id
            print(f"{label}: set fileview_id → {ingest_rs_id}")
            created += 1
            save_config(config, args.config)

    print(f"\nSummary: {created} created/set, {migrated} migrated, {skipped} skipped, {errors} errors")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
