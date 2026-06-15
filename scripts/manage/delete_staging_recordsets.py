#!/usr/bin/env python3
"""Delete all staging RecordSets from Synapse and clear their fileview_id from config.

One-time cleanup before switching to snapshot-based staging promotion.
Saves config after each deletion so partial runs are resumable.

Usage:
  python scripts/manage/delete_staging_recordsets.py [--dry-run] [--project-name htan2-testing1]
"""

import argparse
import os
import sys

import synapseclient
import yaml

CONFIG_PATH = "schema_binding_config.yml"


def save_config(config, path):
    with open(path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def main():
    parser = argparse.ArgumentParser(
        description="Delete staging RecordSets from Synapse and clear fileview_id from config."
    )
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Preview without deleting anything")
    parser.add_argument(
        "--project-name",
        help="Limit to one project (e.g. htan2-testing1). Leave empty to process all.",
    )
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    pat = os.environ.get("SYNAPSE_PAT")
    if pat:
        syn.login(authToken=pat, silent=True)
    else:
        syn.login(silent=True)

    with open(args.config) as f:
        config = yaml.safe_load(f)

    deleted = skipped = errors = 0

    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        print(f"\n{schema_name}")
        for entry in schema_data.get("projects", []):
            if "staging" not in entry.get("subfolder", ""):
                continue
            if args.project_name and entry["name"] != args.project_name:
                continue

            label = f"  {entry['name']}/{entry['subfolder']} ({entry['synapse_id']})"
            rs_id = entry.get("fileview_id")

            if not rs_id:
                print(f"{label}: no RecordSet ID, nothing to delete")
                skipped += 1
                continue

            if args.dry_run:
                print(f"{label}: [DRY RUN] would delete RecordSet {rs_id}")
                deleted += 1
                continue

            try:
                syn.delete(rs_id)
                entry["fileview_id"] = None
                print(f"{label}: deleted RecordSet {rs_id}")
                deleted += 1
                save_config(config, args.config)
            except Exception as e:
                if "404" in str(e) or "not found" in str(e).lower():
                    entry["fileview_id"] = None
                    print(f"{label}: {rs_id} already gone, clearing from config")
                    deleted += 1
                    save_config(config, args.config)
                else:
                    print(f"{label}: ERROR — {e}")
                    errors += 1

    print(f"\nSummary: {deleted} deleted/cleared, {skipped} skipped, {errors} errors")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
