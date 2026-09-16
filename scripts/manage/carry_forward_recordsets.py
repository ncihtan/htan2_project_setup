#!/usr/bin/env python3
"""Carry forward record-based metadata from a prior release into a new version's recordsets.

Record-based metadata (Clinical, Biospecimen, SpatialPanel, ChannelMetadata) is cumulative
across releases. When a new version is set up, its recordsets are created EMPTY (just the
schema template header). This tool seeds them with the rows from the previous release so
curators continue from the last release's data instead of a blank set.

For each record-based schema, per project, it:
  1. reads all rows from the SOURCE recordset (default: v8_release entry's recordSetId),
  2. maps columns to the TARGET recordset's schema (default: v9_ingest), copying the overlap,
     leaving new fields blank, and REPORTING any source-only columns (nothing dropped silently),
  3. writes the mapped rows into the target recordset (a new version; upsert-keyed server-side).

Source/target recordsets are discovered from schema_binding_config.yml: on a record-based
entry, `fileview_id` holds the recordSetId (see update_fileview_ids.py). Source and target are
matched by (schema_name, project_name).

REQUIRES the target v9 recordsets to already exist — run in this order:
  1. setup_folders.py (creates folders + binds schemas)
  2. create_curation_tasks_from_config.py (creates the empty v9 recordsets + tasks + grids)
  3. fix_recordset_upsert_keys.py (if the targets predate registry-driven upsert keys)
  4. this tool (seeds them)

Step 3 comes first because write_recordset_rows below deliberately preserves the target's
existing upsert keys, so a wrongly-keyed target stays wrongly keyed after seeding. Note that
the v8_ingest -> v9_ingest carry-forward was observed to preserve every row even into
wrongly-keyed targets (2078 -> 2078, 6533 -> 6533), so this is about the target being correct
afterwards, not about rows being lost during the copy.

The tool only SEEDS existing recordsets; it does not create bare ones, so the curation
task / grid / schema binding stay intact.

Usage:
  python scripts/manage/carry_forward_recordsets.py --dry-run
  python scripts/manage/carry_forward_recordsets.py --schema Biospecimen --project-name HTAN2_CRC
  python scripts/manage/carry_forward_recordsets.py --source v8_release --target v9_ingest
"""

import argparse
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import synapseclient
import yaml
from synapseclient.models import RecordSet

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

CONFIG_PATH = "schema_binding_config.yml"


def map_columns(
    src_df: pd.DataFrame, target_cols: List[str]
) -> Tuple[pd.DataFrame, List[str], List[str]]:
    """Reindex source rows onto the target schema's columns.

    Returns (out_df, dropped, blanked):
      - out_df:  src rows with exactly `target_cols`; overlapping columns keep their values,
                 target-only columns are added empty.
      - dropped: source columns with no target home (reported, NOT written).
      - blanked: target columns absent from the source (left empty for curators to fill).

    Pure function — no Synapse, unit-tested.
    """
    src_cols = list(src_df.columns)
    dropped = [c for c in src_cols if c not in target_cols]
    blanked = [c for c in target_cols if c not in src_cols]
    out_df = src_df.reindex(columns=target_cols)
    return out_df, dropped, blanked


def load_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def index_record_entries(config: dict, prefix: str) -> Dict[Tuple[str, str], dict]:
    """{(schema_name, project_name): entry} for record-based entries whose subfolder
    starts with `prefix` (e.g. 'v8_release')."""
    out = {}
    record_based = config.get("schema_bindings", {}).get("record_based", {})
    for schema_name, schema_config in record_based.items():
        for entry in schema_config.get("projects", []):
            if entry.get("subfolder", "").startswith(prefix):
                out[(schema_name, entry["name"])] = entry
    return out


def read_recordset_rows(syn, record_set_id: str) -> pd.DataFrame:
    """Download a recordset's CSV and return its rows as a DataFrame."""
    with tempfile.TemporaryDirectory() as td:
        rs = RecordSet(id=record_set_id, path=td).get(synapse_client=syn)
        return pd.read_csv(rs.path)


def read_recordset_columns(syn, record_set_id: str) -> List[str]:
    """Return just the column names of a recordset (header only)."""
    with tempfile.TemporaryDirectory() as td:
        rs = RecordSet(id=record_set_id, path=td).get(synapse_client=syn)
        return list(pd.read_csv(rs.path, nrows=0).columns)


def write_recordset_rows(syn, record_set_id: str, df: pd.DataFrame) -> None:
    """Store a DataFrame as a new version of an existing recordset.

    Fetch the existing recordset first (without downloading its CSV — download_file is a
    dataclass field, not a get() argument) so its upsert_keys and schema binding are
    preserved, then point it at the new CSV and store a new version.

    Preserving upsert_keys means this inherits whatever key the target already has, right or
    wrong — see the ordering note in the module docstring.
    """
    with tempfile.TemporaryDirectory() as td:
        csv_path = os.path.join(td, "records.csv")
        df.to_csv(csv_path, index=False)
        rs = RecordSet(id=record_set_id, download_file=False).get(synapse_client=syn)
        rs.path = csv_path
        rs.store(synapse_client=syn)


def carry_forward(syn, config, source_prefix, target_prefix, schema_filter,
                  project_filter, dry_run) -> Tuple[list, list]:
    """Seed target recordsets from source recordsets. Returns (report_rows, skips)."""
    sources = index_record_entries(config, source_prefix)
    targets = index_record_entries(config, target_prefix)

    report = []
    skips = []

    for key in sorted(targets):
        schema_name, project_name = key
        if schema_filter and schema_name not in schema_filter:
            continue
        if project_filter and project_name != project_filter:
            continue

        target_entry = targets[key]
        target_rs = target_entry.get("fileview_id")
        source_entry = sources.get(key)
        source_rs = source_entry.get("fileview_id") if source_entry else None

        label = f"{schema_name}/{project_name}"

        if not target_rs:
            skips.append(f"{label}: no target recordSetId ({target_prefix}) — "
                         "run create_curation_tasks_from_config.py for the new version first")
            continue
        if not source_entry or not source_rs:
            skips.append(f"{label}: no source recordset in {source_prefix} — nothing to carry (skipped)")
            continue

        try:
            src_df = read_recordset_rows(syn, source_rs)
        except Exception as e:
            skips.append(f"{label}: could not read source {source_rs}: {e}")
            continue

        if src_df.empty:
            skips.append(f"{label}: source {source_rs} has 0 rows — skipped")
            continue

        try:
            target_cols = read_recordset_columns(syn, target_rs)
        except Exception as e:
            skips.append(f"{label}: could not read target {target_rs}: {e}")
            continue

        out_df, dropped, blanked = map_columns(src_df, target_cols)

        report.append({
            "label": label, "source_rs": source_rs, "target_rs": target_rs,
            "rows": len(out_df),
            "copied": [c for c in src_df.columns if c in target_cols],
            "blanked": blanked, "dropped": dropped,
        })

        if not dry_run:
            write_recordset_rows(syn, target_rs, out_df)

    return report, skips


def print_report(report, skips, source_prefix, target_prefix, dry_run):
    mode = "DRY RUN — nothing written" if dry_run else "LIVE"
    print("=" * 80)
    print(f"Carry forward records: {source_prefix} → {target_prefix}   [{mode}]")
    print("=" * 80)
    for r in report:
        verb = "would copy" if dry_run else "copied"
        print(f"\n{r['label']}: {verb} {r['rows']} row(s)  "
              f"[{r['source_rs']} → {r['target_rs']}]")
        print(f"    columns copied : {len(r['copied'])}")
        if r["blanked"]:
            print(f"    new/blank cols : {len(r['blanked'])}  {r['blanked']}")
        if r["dropped"]:
            print(f"    ⚠ dropped cols : {len(r['dropped'])}  {r['dropped']}")
    if skips:
        print("\n" + "-" * 80)
        print("Skipped:")
        for s in skips:
            print(f"  - {s}")
    print("\n" + "=" * 80)
    total_dropped = sorted({c for r in report for c in r["dropped"]})
    print(f"Pairs processed: {len(report)}   Skipped: {len(skips)}")
    if total_dropped:
        print(f"⚠ Distinct source columns with no target home (review before a live run): {total_dropped}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Carry forward record data from a prior release into new-version recordsets.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--source", default="v8_release",
                        help="Source subfolder prefix (default: v8_release)")
    parser.add_argument("--target", default="v9_ingest",
                        help="Target subfolder prefix (default: v9_ingest)")
    parser.add_argument("--schema", action="append",
                        help="Only carry these schema(s); repeatable (e.g. --schema Biospecimen)")
    parser.add_argument("--project-name", help="Only this project (e.g. HTAN2_CRC)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview row counts and column mapping without writing")
    args = parser.parse_args()

    config = load_config(args.config)

    syn = synapseclient.Synapse()
    auth_token = os.environ.get("SYNAPSE_PAT")
    username = os.environ.get("SYNAPSE_USERNAME")
    if auth_token:
        syn.login(authToken=auth_token)
    elif username:
        syn.login(username)
    else:
        syn.login()

    report, skips = carry_forward(
        syn, config, args.source, args.target,
        set(args.schema) if args.schema else None,
        args.project_name, args.dry_run,
    )
    print_report(report, skips, args.source, args.target, args.dry_run)


if __name__ == "__main__":
    main()
