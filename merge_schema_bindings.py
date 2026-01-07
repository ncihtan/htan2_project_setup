#!/usr/bin/env python3
"""
Helper script to merge schema binding structure into schema_binding_config.yml.
This script takes the generated schema_binding_{version}.yml and merges it into
the existing schema_binding_config.yml file.
"""

import yaml
import argparse
import sys
from pathlib import Path


def load_yaml(file_path: str) -> dict:
    """Load YAML file."""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return {}


def save_yaml(file_path: str, data: dict):
    """Save YAML file."""
    with open(file_path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)


def merge_schema_bindings(existing_config: dict, new_bindings: dict, folder_type_filter: str = None):
    """
    Merge new schema bindings into existing config.
    
    Args:
        existing_config: Existing schema_binding_config.yml content
        new_bindings: New bindings from schema_binding_{version}.yml
        folder_type_filter: If provided, only merge bindings for this folder type (e.g., "v8_staging")
    """
    if "schema_bindings" not in existing_config:
        existing_config["schema_bindings"] = {
            "file_based": {},
            "record_based": {}
        }
    
    new_schema_bindings = new_bindings.get("schema_bindings", {})
    
    # Merge file-based schemas
    for schema_name, schema_config in new_schema_bindings.get("file_based", {}).items():
        if schema_name not in existing_config["schema_bindings"]["file_based"]:
            existing_config["schema_bindings"]["file_based"][schema_name] = {"projects": []}
        
        # Filter by folder type if specified
        projects_to_add = schema_config.get("projects", [])
        if folder_type_filter:
            projects_to_add = [
                p for p in projects_to_add 
                if p.get("subfolder", "").startswith(folder_type_filter)
            ]
        
        # Add projects (avoid duplicates)
        existing_projects = {
            (p["name"], p["subfolder"]): p 
            for p in existing_config["schema_bindings"]["file_based"][schema_name]["projects"]
        }
        
        for project in projects_to_add:
            key = (project["name"], project["subfolder"])
            if key not in existing_projects:
                existing_config["schema_bindings"]["file_based"][schema_name]["projects"].append(project)
            else:
                # Update synapse_id if it changed
                existing_projects[key]["synapse_id"] = project["synapse_id"]
    
    # Merge record-based schemas
    # Handle case where record_based is a list (old format) vs dict (new format)
    if isinstance(existing_config["schema_bindings"]["record_based"], list):
        # Convert old format (list) to new format (dict)
        existing_config["schema_bindings"]["record_based"] = {}
    
    for schema_name, schema_config in new_schema_bindings.get("record_based", {}).items():
        if schema_name not in existing_config["schema_bindings"]["record_based"]:
            existing_config["schema_bindings"]["record_based"][schema_name] = {"projects": []}
        
        # Filter by folder type if specified
        projects_to_add = schema_config.get("projects", [])
        if folder_type_filter:
            projects_to_add = [
                p for p in projects_to_add 
                if p.get("subfolder", "").startswith(folder_type_filter)
            ]
        
        # Add projects (avoid duplicates)
        existing_projects = {
            (p["name"], p["subfolder"]): p 
            for p in existing_config["schema_bindings"]["record_based"][schema_name]["projects"]
        }
        
        for project in projects_to_add:
            key = (project["name"], project["subfolder"])
            if key not in existing_projects:
                existing_config["schema_bindings"]["record_based"][schema_name]["projects"].append(project)
            else:
                # Update synapse_id if it changed
                existing_projects[key]["synapse_id"] = project["synapse_id"]


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Merge schema binding structure into schema_binding_config.yml"
    )
    parser.add_argument(
        "--schema-binding-file",
        type=str,
        required=True,
        help="Path to schema_binding_{version}.yml file to merge"
    )
    parser.add_argument(
        "--config-file",
        type=str,
        default="schema_binding_config.yml",
        help="Path to schema_binding_config.yml file (default: schema_binding_config.yml)"
    )
    parser.add_argument(
        "--folder-type-filter",
        type=str,
        help="Only merge bindings for this folder type (e.g., v8_staging). If not specified, merges all."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be merged without actually merging"
    )
    
    args = parser.parse_args()
    
    # Load files
    existing_config = load_yaml(args.config_file)
    new_bindings = load_yaml(args.schema_binding_file)
    
    if not new_bindings:
        print(f"Error: No data found in {args.schema_binding_file}")
        sys.exit(1)
    
    if args.dry_run:
        print("=" * 80)
        print("DRY RUN MODE - Showing what would be merged")
        print("=" * 80)
        print()
        print(f"Source file: {args.schema_binding_file}")
        print(f"Target file: {args.config_file}")
        if args.folder_type_filter:
            print(f"Folder type filter: {args.folder_type_filter}")
        print()
        
        # Show what would be added
        new_schema_bindings = new_bindings.get("schema_bindings", {})
        
        print("File-based schemas to add/update:")
        for schema_name, schema_config in new_schema_bindings.get("file_based", {}).items():
            projects = schema_config.get("projects", [])
            if args.folder_type_filter:
                projects = [p for p in projects if p.get("subfolder", "").startswith(args.folder_type_filter)]
            if projects:
                print(f"  - {schema_name}: {len(projects)} projects")
        
        print("\nRecord-based schemas to add/update:")
        for schema_name, schema_config in new_schema_bindings.get("record_based", {}).items():
            projects = schema_config.get("projects", [])
            if args.folder_type_filter:
                projects = [p for p in projects if p.get("subfolder", "").startswith(args.folder_type_filter)]
            if projects:
                print(f"  - {schema_name}: {len(projects)} projects")
        
        print("\nRun without --dry-run to actually merge.")
    else:
        # Merge
        merge_schema_bindings(existing_config, new_bindings, args.folder_type_filter)
        
        # Save
        save_yaml(args.config_file, existing_config)
        print(f"✓ Successfully merged schema bindings into {args.config_file}")
        if args.folder_type_filter:
            print(f"  (Filtered to {args.folder_type_filter} folders only)")


if __name__ == "__main__":
    main()


