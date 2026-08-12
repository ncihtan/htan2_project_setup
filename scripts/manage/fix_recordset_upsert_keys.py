#!/usr/bin/env python3
"""Repair the upsert keys on RecordSets that already exist in Synapse.

A RecordSet's upsert key is its primary key: Synapse treats an incoming row whose key
matches an existing row as an UPDATE rather than an INSERT. Tasks created before the
registry-driven key lookup landed were keyed off whatever property happened to sort first
in the schema's `required` array (Demographics ended up keyed on ETHNIC_GROUP), so rows
that should be distinct can overwrite one another.

Fixing create_curation_tasks_from_config.py only helps RecordSets created from now on.
This script brings the already-created ones in line with module_registry.yml.

RecordSets are discovered from schema_binding_config.yml: on a record-based entry,
`fileview_id` holds the recordSetId (see update_fileview_ids.py).

Run the report first — it writes nothing without --apply:

  python scripts/manage/fix_recordset_upsert_keys.py
  python scripts/manage/fix_recordset_upsert_keys.py --verify-rows --subfolder-filter v8_release
  python scripts/manage/fix_recordset_upsert_keys.py --apply --subfolder-filter v9_ingest

Order matters: repair keys BEFORE carry_forward_recordsets.py. Carry-forward preserves the
target's existing upsert keys, so seeding a wrongly-keyed RecordSet collapses the rows it
writes.
"""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd
import synapseclient
import yaml
from synapseclient.models import RecordSet

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import htan2_synapse.config as config  # noqa: E402

CONFIG_PATH = "schema_binding_config.yml"


def iter_record_entries(cfg, schema_filter, project_filter, subfolder_filter):
    """Yield (schema_name, entry) for record-based config entries matching the filters."""
    record_based = cfg.get("schema_bindings", {}).get("record_based", {})
    for schema_name, schema_config in record_based.items():
        if schema_filter and schema_name not in schema_filter:
            continue
        for entry in schema_config.get("projects", []):
            if project_filter and entry.get("name") != project_filter:
                continue
            if subfolder_filter and subfolder_filter not in entry.get("subfolder", ""):
                continue
            yield schema_name, entry


def find_key_collisions(syn, record_set_id, keys):
    """Rows already in the RecordSet that share a key tuple under `keys`.

    A non-zero count means the data cannot be represented under the corrected key: those
    rows were either already merged by the wrong key, or the corrected key is still too
    coarse. Either way a human needs to look before the RecordSet is written to again.
    Returns (row_count, collision_count) or None if the rows could not be read.
    """
    try:
        with tempfile.TemporaryDirectory() as td:
            rs = RecordSet(id=record_set_id, path=td).get(synapse_client=syn)
            df = pd.read_csv(rs.path)
    except Exception:
        return None
    if df.empty:
        return 0, 0
    present = [k for k in keys if k in df.columns]
    if len(present) != len(keys):
        return len(df), None
    return len(df), int(df.duplicated(subset=present).sum())


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=CONFIG_PATH, help="Config file path")
    parser.add_argument("--apply", action="store_true",
                        help="Write the corrected keys. Without this the script only reports.")
    parser.add_argument("--schema", action="append", dest="schemas",
                        help="Only this schema_name (repeatable)")
    parser.add_argument("--project-name", help="Only this project")
    parser.add_argument("--subfolder-filter",
                        help="Only subfolders containing this string (e.g. v9_ingest)")
    parser.add_argument("--verify-rows", action="store_true",
                        help="Download each RecordSet and report rows that collide under the "
                             "corrected key. Slow — it reads every RecordSet's CSV.")
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    auth_token = os.environ.get("SYNAPSE_PAT")
    username = os.environ.get("SYNAPSE_USERNAME")
    if auth_token:
        syn.login(authToken=auth_token)
    elif username:
        syn.login(username)
    else:
        syn.login()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    correct = wrong = fixed = skipped = errors = 0
    collisions = []

    for schema_name, entry in iter_record_entries(
        cfg, set(args.schemas) if args.schemas else None, args.project_name, args.subfolder_filter
    ):
        label = f"{schema_name}/{entry.get('name')}/{entry.get('subfolder')}"
        record_set_id = entry.get("fileview_id")
        if not record_set_id:
            print(f"  ⚠  {label}: no recordSetId in config — run update_fileview_ids.py")
            skipped += 1
            continue

        try:
            expected = config.upsert_keys_for_schema(schema_name)
            rs = RecordSet(id=record_set_id, download_file=False).get(synapse_client=syn)
            current = list(rs.upsert_keys or [])

            if current == expected:
                correct += 1
                continue

            wrong += 1
            print(f"  ✗ {label} ({record_set_id}): {current or '(none)'} → {expected}")

            if args.verify_rows:
                result = find_key_collisions(syn, record_set_id, expected)
                if result is None:
                    print("      could not read rows to check for collisions")
                else:
                    rows, dupes = result
                    if dupes is None:
                        print(f"      {rows} row(s); key columns absent from the CSV — inspect manually")
                    elif dupes:
                        print(f"      ⚠  {rows} row(s), {dupes} collide under the corrected key")
                        collisions.append((label, rows, dupes))
                    else:
                        print(f"      {rows} row(s), no collisions under the corrected key")

            if args.apply:
                rs.upsert_keys = expected
                rs.store(synapse_client=syn)
                fixed += 1

        except Exception as e:
            print(f"  ❌ {label}: {e}")
            errors += 1

    print(f"\nSummary: {correct} already correct, {wrong} wrong, {fixed} fixed, "
          f"{skipped} skipped, {errors} errors")

    if collisions:
        print(f"\n⚠  {len(collisions)} RecordSet(s) hold rows that collide under the corrected key. "
              f"The wrong key may have already merged distinct records — check these against the "
              f"contributor's source data before curating further:")
        for label, rows, dupes in collisions:
            print(f"     {label}: {dupes}/{rows} row(s)")

    if wrong and not args.apply:
        print("\nRe-run with --apply to write the corrected keys.")

    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
