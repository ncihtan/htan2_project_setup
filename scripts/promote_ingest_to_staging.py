#!/usr/bin/env python3
"""Promote files and RecordSet rows from ingest to staging.

Reads from two BigQuery tables:
  - Files table:    File_EntityId, Folder_EntityId per file ready to promote
  - RecordSets table: Folder_EntityId, RecordSet_Row_Index per row ready to promote

For files: re-parents each file to the matching staging folder (synID preserved, no version bump).
For RecordSet rows: copies specified rows from the ingest RecordSet into the staging RecordSet
  (snapshot semantics — ingest rows are left in place).

Bootstrap requirement: staging-side RecordSets must already exist.
Run create-staging-recordsets.yml before this script.

Usage:
  python scripts/promote_ingest_to_staging.py \\
      --files-table   htan2-dcc.htan2_medallion_gold.<files_table_name> \\
      --records-table htan2-dcc.htan2_medallion_gold.gold_STAGING_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release \\
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


def build_recordset_map(config):
    """Return {ingest_folder_synid: (ingest_rs_id, staging_rs_id)} for record-based schemas.

    Logs a warning for any record-based entry missing a staging RecordSet ID
    (indicates bootstrap has not been run for that folder).
    """
    result = {}
    missing_bootstrap = []
    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        by_project = defaultdict(dict)
        for entry in schema_data.get("projects", []):
            subfolder = entry["subfolder"]
            if "ingest" in subfolder:
                by_project[entry["name"]]["ingest_folder"] = entry["synapse_id"]
                by_project[entry["name"]]["ingest_rs"] = entry.get("fileview_id")
            elif "staging" in subfolder:
                by_project[entry["name"]]["staging_rs"] = entry.get("fileview_id")
        for project, ids in by_project.items():
            ingest_folder = ids.get("ingest_folder")
            ingest_rs = ids.get("ingest_rs")
            staging_rs = ids.get("staging_rs")
            if ingest_folder and ingest_rs and staging_rs:
                result[ingest_folder] = (ingest_rs, staging_rs)
            elif ingest_folder:
                if not ingest_rs:
                    log.warning(f"Missing ingest RecordSet ID for {schema_name}/{project}")
                if not staging_rs:
                    missing_bootstrap.append(f"{schema_name}/{project}")
    if missing_bootstrap:
        log.warning(
            f"{len(missing_bootstrap)} staging RecordSet(s) not yet bootstrapped "
            f"(run create-curation-tasks.yml with subfolder_filter=v8_staging first): "
            + ", ".join(missing_bootstrap[:5])
            + (" ..." if len(missing_bootstrap) > 5 else "")
        )
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


def _get_upsert_key(df):
    """Return the first upsert-key column present in df, or None."""
    for key in ("HTAN_Participant_ID", "HTAN_Biospecimen_ID", "HTAN_Panel_ID"):
        if key in df.columns:
            return key
    return None


def promote_recordset_rows(syn, bq_client, rs_table, recordset_map, dry_run, project_folder_ids=None):
    """Copy RecordSet rows from ingest RecordSets to staging RecordSets.

    RecordSet_Row_Index is treated as the Synapse table ROW_ID.
    Rows already present in staging (matched by upsert key) are skipped.
    """
    query = f"SELECT Folder_EntityId, RecordSet_Row_Index FROM `{rs_table}`"
    rows = list(bq_client.query(query))
    log.info(f"BQ recordsets table: {len(rows)} rows to evaluate")

    by_folder = defaultdict(list)
    for row in rows:
        if project_folder_ids is not None and row.Folder_EntityId not in project_folder_ids:
            continue
        by_folder[row.Folder_EntityId].append(str(row.RecordSet_Row_Index))

    counts = {"copied": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    for ingest_folder_id, row_indexes in by_folder.items():
        mapping = recordset_map.get(ingest_folder_id)
        if not mapping:
            log.warning(f"No RecordSet mapping for ingest folder {ingest_folder_id} — skipping {len(row_indexes)} rows")
            counts["no_mapping"] += len(row_indexes)
            continue

        ingest_rs_id, staging_rs_id = mapping

        # Fetch specified rows from ingest RecordSet
        try:
            ids_str = ",".join(row_indexes)
            result = syn.tableQuery(f"SELECT * FROM {ingest_rs_id} WHERE ROW_ID IN ({ids_str})")
            df = result.asDataFrame()
        except Exception as e:
            log.error(f"Failed to query ingest RecordSet {ingest_rs_id}: {e}")
            counts["error"] += len(row_indexes)
            continue

        if df.empty:
            log.warning(f"No rows found in {ingest_rs_id} for ROW_IDs: {ids_str}")
            continue

        # Skip rows already present in staging (idempotency)
        upsert_key = _get_upsert_key(df)
        if upsert_key:
            try:
                existing_df = syn.tableQuery(
                    f"SELECT {upsert_key} FROM {staging_rs_id}"
                ).asDataFrame()
                already_present = set(existing_df[upsert_key].dropna())
                new_mask = ~df[upsert_key].isin(already_present)
                n_skipped = int((~new_mask).sum())
                if n_skipped:
                    log.info(
                        f"Skipping {n_skipped} row(s) already in staging RecordSet {staging_rs_id}"
                    )
                    counts["skipped"] += n_skipped
                df = df[new_mask]
            except Exception as e:
                log.warning(
                    f"Could not check staging RecordSet {staging_rs_id} for duplicates ({e}) "
                    f"— proceeding without idempotency check"
                )

        if df.empty:
            continue

        df = df.drop(columns=["ROW_ID", "ROW_VERSION"], errors="ignore")

        if dry_run:
            log.info(
                f"[DRY RUN] Would copy {len(df)} row(s): {ingest_rs_id} → {staging_rs_id}"
            )
            counts["copied"] += len(df)
            continue

        try:
            syn.store(synapseclient.Table(staging_rs_id, df))
            log.info(f"Copied {len(df)} row(s): {ingest_rs_id} → {staging_rs_id}")
            counts["copied"] += len(df)
        except Exception as e:
            log.error(f"Failed to copy rows to staging RecordSet {staging_rs_id}: {e}")
            counts["error"] += len(df)

    return counts


def main():
    parser = argparse.ArgumentParser(
        description="Promote ingest files and RecordSet rows to staging",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview changes without modifying Synapse",
    )
    parser.add_argument(
        "--no-annotate", action="store_true",
        help="Skip adding promoted_at / promoted_from annotations to moved files",
    )
    parser.add_argument(
        "--bq-project", default="htan2-dcc",
        help="GCP project used for BigQuery client authentication (default: htan2-dcc)",
    )
    parser.add_argument(
        "--files-table",
        help="Fully-qualified BQ files table (project.dataset.table)",
    )
    parser.add_argument(
        "--records-table",
        help="Fully-qualified BQ recordsets table (project.dataset.table)",
    )
    parser.add_argument(
        "--config", default=CONFIG_PATH,
        help=f"Path to schema_binding_config.yml (default: {CONFIG_PATH})",
    )
    parser.add_argument("--skip-files", action="store_true", help="Skip file promotion")
    parser.add_argument("--skip-records", action="store_true", help="Skip RecordSet row promotion")
    parser.add_argument(
        "--project-name",
        help="Limit promotion to one project (e.g. htan2-testing1). Filters both files and rows.",
    )
    args = parser.parse_args()

    if args.dry_run:
        log.info("DRY RUN — no changes will be made to Synapse")

    if not args.skip_files and not args.files_table:
        parser.error("--files-table is required unless --skip-files is set")
    if not args.skip_records and not args.records_table:
        parser.error("--records-table is required unless --skip-records is set")

    # Synapse login
    syn = synapseclient.Synapse()
    pat = os.environ.get("SYNAPSE_PAT")
    if pat:
        syn.login(authToken=pat, silent=True)
    else:
        syn.login(silent=True)

    # BigQuery client (uses Application Default Credentials from `gcloud auth` or GHA env)
    from google.cloud import bigquery
    bq_client = bigquery.Client(project=args.bq_project)

    # Build config-derived lookup maps
    config = load_config(args.config)
    folder_map = build_folder_map(config)
    recordset_map = build_recordset_map(config)
    log.info(
        f"Config loaded: {len(folder_map)} ingest→staging folder pairs, "
        f"{len(recordset_map)} RecordSet pairs"
    )

    project_folder_ids = None
    if args.project_name:
        project_folder_ids = get_project_ingest_folder_ids(config, args.project_name)
        if not project_folder_ids:
            log.error(f"No ingest folders found for project '{args.project_name}' in config")
            sys.exit(1)
        log.info(f"Filtering to project '{args.project_name}': {len(project_folder_ids)} ingest folders")

    file_counts = {"moved": 0, "skipped": 0, "no_mapping": 0, "error": 0}
    row_counts = {"copied": 0, "skipped": 0, "no_mapping": 0, "error": 0}

    if not args.skip_files:
        file_counts = promote_files(
            syn, bq_client, args.files_table, folder_map,
            dry_run=args.dry_run,
            annotate=not args.no_annotate,
            project_folder_ids=project_folder_ids,
        )

    if not args.skip_records:
        row_counts = promote_recordset_rows(
            syn, bq_client, args.records_table, recordset_map,
            dry_run=args.dry_run,
            project_folder_ids=project_folder_ids,
        )

    prefix = "[DRY RUN] " if args.dry_run else ""
    log.info(
        f"\n{prefix}=== Promotion Summary ===\n"
        f"  Files moved:                      {file_counts['moved']}\n"
        f"  Files already in staging:         {file_counts['skipped']}\n"
        f"  RecordSet rows copied:            {row_counts['copied']}\n"
        f"  RecordSet rows already in staging:{row_counts['skipped']}\n"
        f"  Items with no folder mapping:     {file_counts['no_mapping'] + row_counts['no_mapping']}\n"
        f"  Errors:                           {file_counts['error'] + row_counts['error']}"
    )

    if file_counts["error"] + row_counts["error"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
