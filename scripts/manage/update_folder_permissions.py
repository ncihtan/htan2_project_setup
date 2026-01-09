#!/usr/bin/env python3
"""
Script to update permissions for existing folders.
This script can be used to update permissions for folders that were already created.
"""

import synapseclient
import yaml
import argparse

# Import shared utilities
from htan2_synapse import (
    set_folder_permissions,
)


def update_permissions_for_folder_structure(syn, folder_structure_file: str, version: str, folder_types: list, dry_run: bool = False):
    """
    Update permissions for folders listed in folder_structure file.
    
    Args:
        syn: Synapse client
        folder_structure_file: Path to folder_structure YAML file
        version: Version prefix (e.g., "v8")
        folder_types: List of folder types to update (e.g., ["v8_ingest", "v8_staging", "v8_release"])
        dry_run: If True, only show what would be updated
    """
    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No permissions will be updated")
        print("=" * 80)
        print()
    
    with open(folder_structure_file, 'r') as f:
        data = yaml.safe_load(f)
    
    projects = data[version]["projects"]
    
    for project_name, project_data in sorted(projects.items()):
        print(f"\n{'='*80}")
        print(f"Processing: {project_name}")
        print(f"{'='*80}")
        
        folders = project_data.get("folders", {})
        
        for folder_type in folder_types:
            if folder_type not in folders:
                print(f"  ⚠ {folder_type}/ not found in folder structure")
                continue
            
            folder_id = folders[folder_type].get("synapse_id")
            if not folder_id:
                print(f"  ⚠ No synapse_id found for {folder_type}/")
                continue
            
            print(f"\n  Updating permissions for {folder_type}/ ({folder_id})...")
            
            if not dry_run:
                set_folder_permissions(syn, folder_id, folder_type, version, project_name)
            else:
                print(f"    [DRY RUN] Would update permissions for {folder_type}/")
    
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="Update permissions for existing folders in HTAN2 projects"
    )
    parser.add_argument(
        "--version",
        type=str,
        default="v8",
        help="Version prefix (e.g., v8, v1). Default: v8"
    )
    parser.add_argument(
        "--folder-type",
        type=str,
        nargs="+",
        default=["v8_ingest", "v8_staging", "v8_release"],
        help="Folder types to update (e.g., v8_ingest v8_staging v8_release). Default: all"
    )
    parser.add_argument(
        "--folder-structure-file",
        type=str,
        default="folder_structure_v8.yml",
        help="Path to folder structure YAML file. Default: folder_structure_v8.yml"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - show what would be updated without making changes"
    )
    
    args = parser.parse_args()
    
    # Adjust folder types based on version
    if args.version:
        folder_types = []
        for ft in args.folder_type:
            if ft.startswith("v"):
                folder_types.append(ft)
            else:
                folder_types.append(f"{args.version}_{ft}")
    else:
        folder_types = args.folder_type
    
    # Login to Synapse
    syn = synapseclient.Synapse()
    syn.login()
    
    print("="*80)
    print("HTAN2 Folder Permissions Update")
    print("="*80)
    print(f"Version: {args.version}")
    print(f"Folder Types: {', '.join(folder_types)}")
    print(f"Folder Structure File: {args.folder_structure_file}")
    if args.dry_run:
        print("Mode: DRY RUN")
    print("="*80)
    print()
    
    update_permissions_for_folder_structure(
        syn,
        args.folder_structure_file,
        args.version,
        folder_types,
        args.dry_run
    )


if __name__ == "__main__":
    main()
