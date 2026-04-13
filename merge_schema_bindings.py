#!/usr/bin/env python3
"""
Helper script to merge schema binding structure into schema_binding_config.yml.
Takes the generated schema_binding_{version}.yml and merges it into
the existing schema_binding_config.yml file.
"""

import argparse
import sys
import yaml


def load_yaml(file_path: str) -> dict:
    try:
        with open(file_path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return {}


def save_yaml(file_path: str, data: dict):
    with open(file_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def merge_section(existing: dict, incoming: dict, folder_type_filter: str = None) -> None:
    """Merge one schema section (file_based or record_based) in-place."""
    for schema_name, schema_config in incoming.items():
        existing.setdefault(schema_name, {"projects": []})

        projects_to_add = schema_config.get("projects", [])
        if folder_type_filter:
            projects_to_add = [
                p for p in projects_to_add
                if p.get("subfolder", "").startswith(folder_type_filter)
            ]

        existing_by_key = {
            (p["name"], p["subfolder"]): p
            for p in existing[schema_name]["projects"]
        }

        for project in projects_to_add:
            key = (project["name"], project["subfolder"])
            if key not in existing_by_key:
                existing[schema_name]["projects"].append(project)
            else:
                existing_entry = existing_by_key[key]
                if existing_entry.get("synapse_id") != project["synapse_id"]:
                    # Folder was recreated — clear stale view IDs so they get rediscovered
                    existing_entry.pop("fileview_id", None)
                    existing_entry.pop("record_set_id", None)
                existing_entry["synapse_id"] = project["synapse_id"]


def merge_schema_bindings(existing_config: dict, new_bindings: dict, folder_type_filter: str = None):
    existing_config.setdefault("schema_bindings", {"file_based": {}, "record_based": {}})

    new_sb = new_bindings.get("schema_bindings", {})

    # Convert old list format to dict if needed
    if isinstance(existing_config["schema_bindings"].get("record_based"), list):
        existing_config["schema_bindings"]["record_based"] = {}

    merge_section(
        existing_config["schema_bindings"].setdefault("file_based", {}),
        new_sb.get("file_based", {}),
        folder_type_filter,
    )
    merge_section(
        existing_config["schema_bindings"].setdefault("record_based", {}),
        new_sb.get("record_based", {}),
        folder_type_filter,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Merge schema binding structure into schema_binding_config.yml"
    )
    parser.add_argument("--schema-binding-file", required=True, help="Path to schema_binding_{version}.yml")
    parser.add_argument("--config-file", default="schema_binding_config.yml")
    parser.add_argument("--folder-type-filter", help="Only merge bindings for this folder type (e.g., v8_staging)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be merged without writing")
    args = parser.parse_args()

    existing_config = load_yaml(args.config_file)
    new_bindings = load_yaml(args.schema_binding_file)

    if not new_bindings:
        print(f"Error: No data found in {args.schema_binding_file}")
        sys.exit(1)

    new_sb = new_bindings.get("schema_bindings", {})

    if args.dry_run:
        print("=" * 80)
        print(f"DRY RUN — source: {args.schema_binding_file}  target: {args.config_file}")
        if args.folder_type_filter:
            print(f"Folder type filter: {args.folder_type_filter}")
        print("=" * 80)

        for label, section_key in (("File-based", "file_based"), ("Record-based", "record_based")):
            print(f"\n{label} schemas to add/update:")
            for schema_name, schema_config in new_sb.get(section_key, {}).items():
                projects = schema_config.get("projects", [])
                if args.folder_type_filter:
                    projects = [p for p in projects if p.get("subfolder", "").startswith(args.folder_type_filter)]
                if projects:
                    print(f"  - {schema_name}: {len(projects)} projects")

        print("\nRun without --dry-run to apply.")
        return

    merge_schema_bindings(existing_config, new_bindings, args.folder_type_filter)
    save_yaml(args.config_file, existing_config)
    print(f"✓ Successfully merged schema bindings into {args.config_file}")
    if args.folder_type_filter:
        print(f"  (Filtered to {args.folder_type_filter} folders only)")


if __name__ == "__main__":
    main()
