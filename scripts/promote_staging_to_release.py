#!/usr/bin/env python3
"""Promote files and RecordSet snapshots from staging to release.

Reads from two BigQuery tables:
  - Files table:   File_EntityId, Folder_EntityId (staging folder) per file ready to release
  - Records table: Folder_EntityId (ingest folder) per folder whose staged RecordSet
                   is approved for the current release

For files: re-parents each file from the staging folder to the matching release folder
  (synID preserved). Release EntityViews update automatically since they query by parentId.

For RecordSets: copies the staging entry's fileview_id (the approved versioned snapshot,
  e.g. syn74149099.8) into the release entry's fileview_id. No new Synapse entity is
  created — the release entry simply points at the same pinned ingest RS version.

Pre-requisite: run create_release_fileviews.py once before first promotion to
  create release EntityViews and upsert release folder entries into config.

Usage:
  python scripts/promote_staging_to_release.py \\
      --files-table   htan2-dcc.htan2_medallion_gold.<files_table_name> \\
      --records-table htan2-dcc.htan2_medallion_gold.<records_table_name> \\
      [--dry-run] [--no-annotate] [--skip-files] [--skip-records] [--project-name HTAN2_CRC]
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
    """Return {staging_folder_synid: release_folder_synid} for all schema types."""
    result = {}
    for section in ("file_based", "record_based"):
        for schema_data in config["schema_bindings"].get(section, {}).values():
            by_project = defaultdict(dict)
            for entry in schema_data.get("projects", []):
                subfolder = entry["subfolder"]
                if "staging" in subfolder:
                    by_project[entry["name"]]["staging"] = entry["synapse_id"]
                elif "release" in subfolder:
                    by_project[entry["name"]]["release"] = entry["synapse_id"]
            for ids in by_project.values():
                if "staging" in ids and "release" in ids:
                    result[ids["staging"]] = ids["release"]
    return result


def build_staging_recordset_map(config):
    """Return {staging_folder_synid: (staging_fv_id, release_entry)} for record-based schemas.

    Keyed on staging folder synID because the BQ release records table references
    staging folders (not ingest folders).
    """
    result = {}
    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        by_project = defaultdict(dict)
        for entry in schema_data.get("projects", []):
            subfolder = entry["subfolder"]
            if "staging" in subfolder:
                by_project[entry["name"]]["staging_folder"] = entry["synapse_id"]
                by_project[entry["name"]]["staging_fv"] = entry.get("fileview_id")
            elif "release" in subfolder:
                by_project[entry["name"]]["release_entry"] = entry
        for project, ids in by_project.items():
            staging_folder = ids.get("staging_folder")
            staging_fv = ids.get("staging_fv")
            release_entry = ids.get("release_entry")
            if staging_folder and staging_fv:
                result[staging_folder] = (staging_fv, release_entry)
            elif staging_folder and not staging_fv:
                log.warning(
                    f"No staging fileview_id for {schema_name}/{project} "
                    "— staging may not have been promoted yet"
                )
    return result


def get_project_folder_ids(config, project_name, tier):
    """Return synIDs of folders for a given tier ('ingest', 'staging', 'release') and project."""
    ids = set()
    for section in ("file_based", "record_based"):
        for schema_data in config["schema_bindings"].get(section, {}).values():
            for entry in schema_data.get("projects", []):
                if entry["name"] == project_name and tier in entry.get("subfolder", ""):
                    ids.add(entry["synapse_id"])
    return ids


def move_entity(syn, entity_id, new_parent_id):
    """Re-parent a Synapse entity (preserves synID, no file version bump)."""
    syn.move(entity_id, new_parent_id)


def annotate_entity(syn, entity_id, staging_folder_id):
    """Add released_at and released_from annotations, merging with existing."""
    now_iso = datetime.now(timezone.utc).isoformat()
    existing = syn.restGET(f"/entity/{entity_id}/annotations2")
    existing.setdefault("annotations", {}).update({
        "released_at": {"value": [now_iso], "type": "STRING"},
        "released_from": {"value": [staging_folder_id], "type": "ENTITYID"},
    })
    syn.restPUT(f"/entity/{entity_id}/annotations2", body=json.dumps(existing))


def promote_files(syn, bq_client, files_table, folder_map, dry_run, annotate, project_folder_ids=None):
    """Move files from staging folders to release folders."""
    query = f"SELECT File_EntityId, Folder_EntityId, File_Name FROM `{files_table}`"
    rows = list(bq_client.query(query))
    log.info(f"BQ files table: {len(rows)} files to evaluate")
    counts = {"moved": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    for row in rows:
        file_id = row.File_EntityId
        staging_folder_id = row.Folder_EntityId
        if project_folder_ids is not None and staging_folder_id not in project_folder_ids:
            continue
        file_name = getattr(row, "File_Name", file_id)

        release_folder_id = folder_map.get(staging_folder_id)
        if not release_folder_id:
            log.warning(
                f"No release folder mapping for staging folder {staging_folder_id} "
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

        if entity.parentId == release_folder_id:
            log.debug(f"{file_id} ({file_name}) already in release, skipping")
            counts["skipped"] += 1
            continue

        if dry_run:
            log.info(f"[DRY RUN] Would move {file_id} ({file_name}) → {release_folder_id}")
            counts["moved"] += 1
            continue

        try:
            move_entity(syn, file_id, release_folder_id)
            log.info(f"Moved {file_id} ({file_name}) → {release_folder_id}")
            if annotate:
                try:
                    annotate_entity(syn, file_id, staging_folder_id)
                except Exception as ae:
                    log.warning(f"Could not annotate {file_id}: {ae}")
            counts["moved"] += 1
        except Exception as e:
            log.error(f"Failed to move {file_id} ({file_name}): {e}")
            counts["error"] += 1

    return counts


def snapshot_staging_recordsets(syn, bq_client, rs_table, staging_recordset_map, dry_run, project_folder_ids=None):
    """Copy the staging fileview_id (approved snapshot) into the release entry's fileview_id.

    The staging entry's fileview_id (e.g. syn74149099.8) is the ingest RecordSet version
    that was approved at staging time. Copying it to the release entry records that the
    same snapshot is approved for release. Config is updated in place — caller saves config.
    """
    query = f"SELECT DISTINCT Folder_EntityId FROM `{rs_table}`"
    rows = list(bq_client.query(query))
    folder_ids = {row.Folder_EntityId for row in rows}
    log.info(f"BQ records table: {len(folder_ids)} ingest folder(s) approved for release")

    if project_folder_ids is not None:
        folder_ids = {f for f in folder_ids if f in project_folder_ids}
        log.info(f"After project filter: {len(folder_ids)} folder(s)")

    counts = {"snapshotted": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    for staging_folder_id in sorted(folder_ids):
        mapping = staging_recordset_map.get(staging_folder_id)
        if not mapping:
            log.warning(f"No staging mapping for staging folder {staging_folder_id} — skipping")
            counts["no_mapping"] += 1
            continue

        staging_fv_id, release_entry = mapping

        # Idempotency: skip if release already points at this staging snapshot
        existing_fv = (release_entry or {}).get("fileview_id")
        if existing_fv == staging_fv_id:
            log.info(f"Already released {staging_fv_id}, skipping")
            counts["skipped"] += 1
            continue

        if dry_run:
            log.info(f"[DRY RUN] Would release {staging_fv_id} (staging folder {staging_folder_id})")
            counts["snapshotted"] += 1
            continue

        if release_entry is not None:
            release_entry["fileview_id"] = staging_fv_id
        log.info(f"Released {staging_fv_id} (staging folder {staging_folder_id})")
        counts["snapshotted"] += 1

    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Promote staging files and record snapshots to release",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying Synapse or config",
    )
    parser.add_argument(
        "--no-annotate", action="store_true",
        help="Skip adding released_at / released_from annotations to moved files",
    )
    parser.add_argument("--bq-project", default="htan2-dcc")
    parser.add_argument(
        "--files-table",
        help="Fully-qualified BQ files table (project.dataset.table)",
    )
    parser.add_argument(
        "--records-table",
        help="Fully-qualified BQ records table — drives which ingest folders to release",
    )
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--skip-files", action="store_true", help="Skip file promotion")
    parser.add_argument("--skip-records", action="store_true", help="Skip RecordSet release")
    parser.add_argument(
        "--project-name",
        help="Limit to one project (e.g. htan2-testing1). Filters both files and records.",
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
    staging_recordset_map = build_staging_recordset_map(config)
    log.info(
        f"Config loaded: {len(folder_map)} staging→release folder pairs, "
        f"{len(staging_recordset_map)} staging RecordSet refs"
    )

    # --project-name filter: both files and records use staging folder IDs
    staging_folder_ids = None
    if args.project_name:
        staging_folder_ids = get_project_folder_ids(config, args.project_name, "staging")
        if not staging_folder_ids:
            log.error(f"No staging folders found for project '{args.project_name}' in config")
            sys.exit(1)
        log.info(
            f"Filtering to project '{args.project_name}': {len(staging_folder_ids)} staging folders"
        )

    file_counts = {"moved": 0, "skipped": 0, "no_mapping": 0, "error": 0}
    snap_counts = {"snapshotted": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    if not args.skip_files:
        file_counts = promote_files(
            syn, bq_client, args.files_table, folder_map,
            dry_run=args.dry_run,
            annotate=not args.no_annotate,
            project_folder_ids=staging_folder_ids,
        )

    if not args.skip_records:
        snap_counts = snapshot_staging_recordsets(
            syn, bq_client, args.records_table, staging_recordset_map,
            dry_run=args.dry_run,
            project_folder_ids=staging_folder_ids,
        )
        if not args.dry_run and snap_counts["snapshotted"] > 0:
            save_config(config, args.config)
            log.info("Config updated with release fileview IDs — commit schema_binding_config.yml to persist")

    prefix = "[DRY RUN] " if args.dry_run else ""
    log.info(
        f"\n{prefix}=== Release Summary ===\n"
        f"  Files moved:                  {file_counts['moved']}\n"
        f"  Files already in release:     {file_counts['skipped']}\n"
        f"  RecordSets released:          {snap_counts['snapshotted']}\n"
        f"  RecordSets already current:   {snap_counts['skipped']}\n"
        f"  Items with no mapping:        {file_counts['no_mapping'] + snap_counts['no_mapping']}\n"
        f"  Errors:                       {file_counts['error'] + snap_counts['error']}"
    )

    if file_counts["error"] + snap_counts["error"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
