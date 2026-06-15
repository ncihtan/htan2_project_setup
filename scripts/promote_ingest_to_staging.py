#!/usr/bin/env python3
"""Promote files and RecordSet snapshots from ingest to staging.

Reads from two BigQuery tables:
  - Files table:   File_EntityId, Folder_EntityId per file ready to promote
  - Records table: Folder_EntityId per ingest folder whose RecordSet is ready to snapshot

For files: re-parents each file to the matching staging folder (synID preserved).
  Also snapshots the staging EntityView and stores the versioned synID (e.g. syn123.4)
  in the staging entry's fileview_id.

For RecordSets: reads the current versionNumber of the ingest RecordSet and stores
  the versioned synID (e.g. syn456.8) in the staging entry's fileview_id.

Pre-requisite: run create_staging_fileviews.py once before first promotion to
  create staging EntityViews for file-based folders and set record-based fileview_ids.

Usage:
  python scripts/promote_ingest_to_staging.py \\
      --files-table   htan2-dcc.htan2_medallion_gold.<files_table_name> \\
      --records-table htan2-dcc.htan2_medallion_gold.<records_table_name> \\
      [--dry-run] [--no-annotate] [--skip-files] [--skip-records] [--project-name htan2-testing1]
"""

import argparse
import json
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import synapseclient
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

CONFIG_PATH = "schema_binding_config.yml"


def load_config(config_path):
    with open(config_path) as f:
        return yaml.safe_load(f)


def save_config(config, config_path):
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def build_folder_map(config):
    """Return {ingest_folder_synid: staging_folder_synid} for all schema types."""
    result = {}
    for section in ("file_based", "record_based"):
        for schema_data in config["schema_bindings"].get(section, {}).values():
            by_project = defaultdict(dict)
            for entry in schema_data.get("projects", []):
                subfolder = entry["subfolder"]
                if "ingest" in subfolder:
                    by_project[entry["name"]]["ingest"] = entry["synapse_id"]
                elif "staging" in subfolder:
                    by_project[entry["name"]]["staging"] = entry["synapse_id"]
            for ids in by_project.values():
                if "ingest" in ids and "staging" in ids:
                    result[ids["ingest"]] = ids["staging"]
    return result


def get_project_ingest_folder_ids(config, project_name):
    """Return the set of ingest folder synIDs belonging to a given project name."""
    ids = set()
    for section in ("file_based", "record_based"):
        for schema_data in config["schema_bindings"].get(section, {}).values():
            for entry in schema_data.get("projects", []):
                if entry["name"] == project_name and "ingest" in entry.get("subfolder", ""):
                    ids.add(entry["synapse_id"])
    return ids


def build_staging_fileview_map(config):
    """Return {ingest_folder_synid: (staging_ev_id, staging_entry)} for file-based schemas."""
    result = {}
    for schema_name, schema_data in config["schema_bindings"].get("file_based", {}).items():
        by_project = defaultdict(dict)
        for entry in schema_data.get("projects", []):
            subfolder = entry["subfolder"]
            if "ingest" in subfolder:
                by_project[entry["name"]]["ingest_folder"] = entry["synapse_id"]
            elif "staging" in subfolder:
                fv = entry.get("fileview_id")
                # fileview_id may be a plain synID (unversioned) or synID.version — strip version
                staging_ev_id = str(fv).split(".")[0] if fv else None
                by_project[entry["name"]]["staging_ev"] = staging_ev_id
                by_project[entry["name"]]["staging_entry"] = entry
        for project, ids in by_project.items():
            ingest_folder = ids.get("ingest_folder")
            if ingest_folder:
                result[ingest_folder] = (ids.get("staging_ev"), ids.get("staging_entry"))
    return result


def build_ingest_recordset_map(config):
    """Return {ingest_folder_synid: (ingest_rs_id, staging_entry)} for record-based schemas."""
    result = {}
    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        by_project = defaultdict(dict)
        for entry in schema_data.get("projects", []):
            subfolder = entry["subfolder"]
            if "ingest" in subfolder:
                by_project[entry["name"]]["ingest_folder"] = entry["synapse_id"]
                by_project[entry["name"]]["ingest_rs"] = entry.get("fileview_id")
            elif "staging" in subfolder:
                by_project[entry["name"]]["staging_entry"] = entry
        for project, ids in by_project.items():
            ingest_folder = ids.get("ingest_folder")
            ingest_rs = ids.get("ingest_rs")
            if ingest_folder and ingest_rs:
                result[ingest_folder] = (ingest_rs, ids.get("staging_entry"))
            elif ingest_folder and not ingest_rs:
                log.warning(f"Missing ingest RecordSet ID for {schema_name}/{project}")
    return result


def move_entity(syn, entity_id, new_parent_id):
    """Re-parent a Synapse entity via syn.move() (preserves synID, no file version bump)."""
    syn.move(entity_id, new_parent_id)


def annotate_entity(syn, entity_id, ingest_folder_id):
    """Add promoted_at and promoted_from annotations, merging with any existing annotations."""
    now_iso = datetime.now(timezone.utc).isoformat()
    existing = syn.restGET(f"/entity/{entity_id}/annotations2")
    existing.setdefault("annotations", {}).update({
        "promoted_at": {"value": [now_iso], "type": "STRING"},
        "promoted_from": {"value": [ingest_folder_id], "type": "ENTITYID"},
    })
    syn.restPUT(f"/entity/{entity_id}/annotations2", body=json.dumps(existing))


def promote_files(syn, bq_client, files_table, folder_map, dry_run, annotate, project_folder_ids=None):
    """Move files from ingest folders to staging folders."""
    query = f"SELECT File_EntityId, Folder_EntityId, File_Name FROM `{files_table}`"
    rows = list(bq_client.query(query))
    log.info(f"BQ files table: {len(rows)} files to evaluate")
    counts = {"moved": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    for row in rows:
        file_id = row.File_EntityId
        ingest_folder_id = row.Folder_EntityId
        if project_folder_ids is not None and ingest_folder_id not in project_folder_ids:
            continue
        file_name = getattr(row, "File_Name", file_id)

        staging_folder_id = folder_map.get(ingest_folder_id)
        if not staging_folder_id:
            log.warning(
                f"No staging folder mapping for ingest folder {ingest_folder_id} "
                f"(file {file_id} / {file_name}) — skipping"
            )
            counts["no_mapping"] += 1
            continue

        try:
            entity = syn.get(file_id, downloadFile=False)
        except Exception as e:
            log.error(f"Could not fetch {file_id} ({file_name}): {e}")
            counts["error"] += 1
            continue

        if entity.parentId == staging_folder_id:
            log.debug(f"{file_id} ({file_name}) already in staging, skipping")
            counts["skipped"] += 1
            continue

        if dry_run:
            log.info(f"[DRY RUN] Would move {file_id} ({file_name}) → {staging_folder_id}")
            counts["moved"] += 1
            continue

        try:
            move_entity(syn, file_id, staging_folder_id)
            log.info(f"Moved {file_id} ({file_name}) → {staging_folder_id}")
            if annotate:
                try:
                    annotate_entity(syn, file_id, ingest_folder_id)
                except Exception as ae:
                    log.warning(f"Could not annotate {file_id}: {ae}")
            counts["moved"] += 1
        except Exception as e:
            log.error(f"Failed to move {file_id} ({file_name}): {e}")
            counts["error"] += 1

    return counts


def snapshot_staging_fileviews(syn, bq_client, files_table, staging_fv_map, dry_run, project_folder_ids=None):
    """Snapshot staging EntityViews for file-based folders in the BQ files table.

    For each distinct ingest Folder_EntityId in the BQ files table, creates a snapshot
    version of the corresponding staging EntityView and stores the versioned synID
    (e.g. syn123.4) in the staging config entry's fileview_id.
    Config entries are updated in place — caller saves config after this returns.
    """
    query = f"SELECT DISTINCT Folder_EntityId FROM `{files_table}`"
    rows = list(bq_client.query(query))
    ingest_folder_ids = {row.Folder_EntityId for row in rows}
    log.info(f"BQ files table: {len(ingest_folder_ids)} distinct ingest folder(s) for EV snapshot")

    if project_folder_ids is not None:
        ingest_folder_ids = {f for f in ingest_folder_ids if f in project_folder_ids}
        log.info(f"After project filter: {len(ingest_folder_ids)} folder(s)")

    counts = {"snapshotted": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    for ingest_folder_id in sorted(ingest_folder_ids):
        mapping = staging_fv_map.get(ingest_folder_id)
        if not mapping:
            counts["no_mapping"] += 1
            continue

        staging_ev_id, staging_entry = mapping
        if not staging_ev_id:
            log.warning(f"No staging EntityView for ingest folder {ingest_folder_id} — run create_staging_fileviews.py first")
            counts["no_mapping"] += 1
            continue

        if dry_run:
            log.info(f"[DRY RUN] Would snapshot staging EV {staging_ev_id} (ingest folder {ingest_folder_id})")
            counts["snapshotted"] += 1
            continue

        try:
            version = syn.create_snapshot_version(staging_ev_id)
            versioned_id = f"{staging_ev_id}.{version}"
            if staging_entry is not None:
                staging_entry["fileview_id"] = versioned_id
            log.info(f"Snapshotted staging EV {versioned_id} (ingest folder {ingest_folder_id})")
            counts["snapshotted"] += 1
        except Exception as e:
            log.error(f"Failed to snapshot staging EV {staging_ev_id}: {e}")
            counts["error"] += 1

    return counts


def snapshot_ingest_recordsets(syn, bq_client, rs_table, ingest_recordset_map, dry_run, project_folder_ids=None):
    """Record the current version of each ready ingest RecordSet as the staging snapshot.

    Reads the current versionNumber from the ingest RecordSet and stores the versioned
    synID (e.g. syn456.8) in the staging entry's fileview_id.
    Config entries are updated in place — caller saves config after this returns.
    """
    query = f"SELECT DISTINCT Folder_EntityId FROM `{rs_table}`"
    rows = list(bq_client.query(query))
    folder_ids = {row.Folder_EntityId for row in rows}
    log.info(f"BQ records table: {len(folder_ids)} ingest folder(s) ready to snapshot")

    if project_folder_ids is not None:
        folder_ids = {f for f in folder_ids if f in project_folder_ids}
        log.info(f"After project filter: {len(folder_ids)} folder(s)")

    counts = {"snapshotted": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    for ingest_folder_id in sorted(folder_ids):
        mapping = ingest_recordset_map.get(ingest_folder_id)
        if not mapping:
            log.warning(f"No ingest RecordSet mapping for folder {ingest_folder_id} — skipping")
            counts["no_mapping"] += 1
            continue

        ingest_rs_id, staging_entry = mapping

        try:
            entity_data = syn.restGET(f"/entity/{ingest_rs_id}")
            version = entity_data.get("versionNumber")
        except Exception as e:
            log.error(f"Could not fetch ingest RecordSet {ingest_rs_id}: {e}")
            counts["error"] += 1
            continue

        versioned_id = f"{ingest_rs_id}.{version}"

        # Idempotency: skip if staging fileview_id already matches this versioned ID
        existing_fv = (staging_entry or {}).get("fileview_id")
        if existing_fv == versioned_id:
            log.info(f"Already snapshotted {versioned_id}, skipping")
            counts["skipped"] += 1
            continue

        if dry_run:
            log.info(f"[DRY RUN] Would snapshot {versioned_id} (folder {ingest_folder_id})")
            counts["snapshotted"] += 1
            continue

        if staging_entry is not None:
            staging_entry["fileview_id"] = versioned_id
        log.info(f"Snapshotted {versioned_id} (folder {ingest_folder_id})")
        counts["snapshotted"] += 1

    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Promote ingest files and record snapshots to staging",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying Synapse or config",
    )
    parser.add_argument(
        "--no-annotate", action="store_true",
        help="Skip adding promoted_at / promoted_from annotations to moved files",
    )
    parser.add_argument(
        "--bq-project", default="htan2-dcc",
        help="GCP project for BigQuery client (default: htan2-dcc)",
    )
    parser.add_argument(
        "--files-table",
        help="Fully-qualified BQ files table (project.dataset.table)",
    )
    parser.add_argument(
        "--records-table",
        help="Fully-qualified BQ records table — drives which ingest folders to snapshot",
    )
    parser.add_argument(
        "--config", default=CONFIG_PATH,
        help=f"Path to schema_binding_config.yml (default: {CONFIG_PATH})",
    )
    parser.add_argument("--skip-files", action="store_true", help="Skip file promotion")
    parser.add_argument("--skip-records", action="store_true", help="Skip RecordSet snapshotting")
    parser.add_argument(
        "--project-name",
        help="Limit to one project (e.g. htan2-testing1). Filters both files and snapshots.",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info("DRY RUN — no changes will be made to Synapse or config")

    if not args.skip_files and not args.files_table:
        parser.error("--files-table is required unless --skip-files is set")
    if not args.skip_records and not args.records_table:
        parser.error("--records-table is required unless --skip-records is set")

    syn = synapseclient.Synapse()
    pat = os.environ.get("SYNAPSE_PAT")
    if pat:
        syn.login(authToken=pat, silent=True)
    else:
        syn.login(silent=True)

    from google.cloud import bigquery
    bq_client = bigquery.Client(project=args.bq_project)

    config = load_config(args.config)
    folder_map = build_folder_map(config)
    staging_fv_map = build_staging_fileview_map(config)
    ingest_recordset_map = build_ingest_recordset_map(config)
    log.info(
        f"Config loaded: {len(folder_map)} ingest→staging folder pairs, "
        f"{len(staging_fv_map)} staging file EVs, "
        f"{len(ingest_recordset_map)} ingest RecordSets"
    )

    project_folder_ids = None
    if args.project_name:
        project_folder_ids = get_project_ingest_folder_ids(config, args.project_name)
        if not project_folder_ids:
            log.error(f"No ingest folders found for project '{args.project_name}' in config")
            sys.exit(1)
        log.info(f"Filtering to project '{args.project_name}': {len(project_folder_ids)} ingest folders")

    file_counts  = {"moved": 0, "skipped": 0, "no_mapping": 0, "error": 0}
    ev_counts    = {"snapshotted": 0, "skipped": 0, "no_mapping": 0, "error": 0}
    snap_counts  = {"snapshotted": 0, "skipped": 0, "no_mapping": 0, "error": 0}
    config_dirty = False

    if not args.skip_files:
        file_counts = promote_files(
            syn, bq_client, args.files_table, folder_map,
            dry_run=args.dry_run,
            annotate=not args.no_annotate,
            project_folder_ids=project_folder_ids,
        )
        ev_counts = snapshot_staging_fileviews(
            syn, bq_client, args.files_table, staging_fv_map,
            dry_run=args.dry_run,
            project_folder_ids=project_folder_ids,
        )
        if not args.dry_run and ev_counts["snapshotted"] > 0:
            config_dirty = True

    if not args.skip_records:
        snap_counts = snapshot_ingest_recordsets(
            syn, bq_client, args.records_table, ingest_recordset_map,
            dry_run=args.dry_run,
            project_folder_ids=project_folder_ids,
        )
        if not args.dry_run and snap_counts["snapshotted"] > 0:
            config_dirty = True

    if config_dirty:
        save_config(config, args.config)
        log.info("Config updated with versioned fileview IDs — commit schema_binding_config.yml to persist")

    prefix = "[DRY RUN] " if args.dry_run else ""
    log.info(
        f"\n{prefix}=== Promotion Summary ===\n"
        f"  Files moved:                  {file_counts['moved']}\n"
        f"  Files already in staging:     {file_counts['skipped']}\n"
        f"  Staging EVs snapshotted:      {ev_counts['snapshotted']}\n"
        f"  RecordSets snapshotted:       {snap_counts['snapshotted']}\n"
        f"  RecordSets already current:   {snap_counts['skipped']}\n"
        f"  Items with no mapping:        {file_counts['no_mapping'] + ev_counts['no_mapping'] + snap_counts['no_mapping']}\n"
        f"  Errors:                       {file_counts['error'] + ev_counts['error'] + snap_counts['error']}"
    )

    if file_counts["error"] + ev_counts["error"] + snap_counts["error"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
