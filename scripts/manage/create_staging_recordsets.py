#!/usr/bin/env python3
"""Create staging RecordSets without curation tasks.

One-time bootstrap for staging folders: creates a RecordSet entity (versioned CSV)
for each record-based staging entry in schema_binding_config.yml that doesn't yet
have a fileview_id. Writes the new synIDs back to the config immediately after each
creation so partial runs are recoverable.

Unlike create_curation_tasks_from_config.py, this creates ONLY the RecordSet —
no CurationTask and no Grid. Staging data is promoted programmatically, so the
curator submission UI is not needed.

Pre-requisite: schemas must be bound to staging folders before running this.
Run setup-folders-and-bind-schemas.yml with subfolder_filter=v8_staging first.

Usage:
  python scripts/manage/create_staging_recordsets.py [--dry-run] [--project-name htan2-testing1]
"""

import argparse
import os
import sys
import tempfile

import pandas as pd
import synapseclient
import yaml

try:
    from synapseclient.models.recordset import RecordSet
    from synapseclient.models import Folder
except ImportError:
    print("Error: synapseclient[curator] >= 4.0.0 required. Run: pip install 'synapseclient[curator]'")
    sys.exit(1)

CONFIG_PATH = "schema_binding_config.yml"

CLINICAL_SCHEMAS = {
    "Demographics", "Diagnosis", "Therapy", "FollowUp",
    "MolecularTest", "Exposure", "FamilyHistory", "VitalStatus",
}


def get_bound_schema_uri(syn, folder_id):
    try:
        folder = Folder(id=folder_id)
        binding = folder.get_schema(synapse_client=syn)
        return binding.json_schema_version_info.id if binding else None
    except Exception:
        return None


def get_schema_upsert_keys(syn, schema_uri):
    try:
        schema = syn.restGET(f"/schema/type/registered/{schema_uri.split('/')[-1]}")
        for key in ("HTAN_Participant_ID", "HTAN_Biospecimen_ID", "HTAN_Subject_ID"):
            if key in schema.get("properties", {}):
                return [key]
        required = schema.get("required", [])
        if required:
            return [required[0]]
    except Exception:
        pass
    return []


def get_schema_properties(syn, schema_uri):
    """Return ordered list of property names from a registered JSON schema."""
    try:
        schema = syn.restGET(f"/schema/type/registered/{schema_uri.split('/')[-1]}")
        return list(schema.get("properties", {}).keys())
    except Exception:
        return []


def make_empty_csv(columns):
    """Write an empty CSV with the given column headers to a temp file. Returns path."""
    df = pd.DataFrame(columns=columns)
    tmp = tempfile.NamedTemporaryFile(suffix=".csv", mode="w", delete=False)
    df.to_csv(tmp, index=False)
    tmp.close()
    return tmp.name


def save_config(config, config_path):
    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def main():
    parser = argparse.ArgumentParser(
        description="Create staging RecordSets (no curation tasks). "
                    "Pre-requisite: schemas must already be bound to staging folders."
    )
    parser.add_argument("--config", default=CONFIG_PATH)
    parser.add_argument("--dry-run", action="store_true", help="Preview without creating anything")
    parser.add_argument(
        "--project-name",
        help="Limit to one project (e.g. htan2-testing1). Leave empty to process all.",
    )
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    pat = os.environ.get("SYNAPSE_PAT")
    if pat:
        syn.login(authToken=pat)
    else:
        syn.login()

    with open(args.config) as f:
        config = yaml.safe_load(f)

    created = skipped = errors = 0

    for schema_name, schema_data in config["schema_bindings"].get("record_based", {}).items():
        print(f"\n{schema_name}")
        for entry in schema_data.get("projects", []):
            if "staging" not in entry.get("subfolder", ""):
                continue
            if args.project_name and entry["name"] != args.project_name:
                continue

            folder_id = entry["synapse_id"]
            project_name = entry["name"]
            subfolder = entry["subfolder"]
            label = f"{project_name}/{subfolder} ({folder_id})"

            if entry.get("fileview_id"):
                print(f"  ✓  {label}: already has RecordSet {entry['fileview_id']}, skipping")
                skipped += 1
                continue

            schema_uri = get_bound_schema_uri(syn, folder_id)
            if not schema_uri:
                print(
                    f"  ⚠  {label}: no schema bound — "
                    f"run setup-folders-and-bind-schemas.yml with subfolder_filter=v8_staging first"
                )
                skipped += 1
                continue

            upsert_keys = get_schema_upsert_keys(syn, schema_uri)
            if not upsert_keys:
                upsert_keys = (
                    ["HTAN_Participant_ID"]
                    if any(x in schema_name for x in CLINICAL_SCHEMAS)
                    else ["HTAN_Biospecimen_ID"]
                )

            properties = get_schema_properties(syn, schema_uri)
            if not properties:
                print(f"  ❌ {label}: could not read schema properties from {schema_uri}")
                errors += 1
                continue

            rs_name = f"{schema_name}_Staging_Records"

            if args.dry_run:
                print(
                    f"  [DRY RUN] Would create RecordSet '{rs_name}' in {folder_id} "
                    f"(upsert_key={upsert_keys}, {len(properties)} columns)"
                )
                created += 1
                continue

            tmp_csv = None
            try:
                tmp_csv = make_empty_csv(properties)
                rs = RecordSet(
                    name=rs_name,
                    parent_id=folder_id,
                    description=f"HTAN {schema_name} staging records for {project_name}",
                    path=tmp_csv,
                    upsert_keys=upsert_keys,
                )
                rs = rs.store(synapse_client=syn)

                try:
                    rs.bind_schema(
                        json_schema_uri=schema_uri,
                        enable_derived_annotations=False,
                        synapse_client=syn,
                    )
                except Exception as bind_err:
                    print(f"    ⚠ Schema bind failed (non-fatal): {bind_err}")

                entry["fileview_id"] = rs.id
                print(f"  ✅ {label}: created RecordSet {rs.id}")
                created += 1

                # Write config after each creation — partial runs are safely resumable
                save_config(config, args.config)

            except Exception as e:
                print(f"  ❌ {label}: {e}")
                errors += 1
            finally:
                if tmp_csv and os.path.exists(tmp_csv):
                    os.unlink(tmp_csv)

    print(f"\nSummary: {created} created, {skipped} skipped, {errors} errors")
    if created > 0 and not args.dry_run:
        print("Config updated in place — commit schema_binding_config.yml to persist the new RecordSet IDs.")
    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
