#!/usr/bin/env python3
"""
Script to rename existing "SpatialTranscriptomics" folders to "SpatialOmics" in Synapse.
This script finds all folders named "SpatialTranscriptomics" and renames them to "SpatialOmics".
"""

import synapseclient
import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directories to path to import htan2_synapse
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from htan2_synapse import load_projects


def find_folders_to_rename(syn, projects: Dict[str, str], version: str, folder_types: List[str]) -> List[Dict]:
    """
    Find all "SpatialTranscriptomics" folders that need to be renamed.
    
    Returns a list of dicts with:
    - project_name: Name of the project
    - folder_type: Type of folder (v8_ingest, v8_staging, etc.)
    - folder_id: Synapse ID of the SpatialTranscriptomics folder
    - parent_id: Synapse ID of the parent folder
    """
    folders_to_rename = []
    
    for project_name, project_id in sorted(projects.items()):
        print(f"\n{'='*80}")
        print(f"Checking: {project_name} ({project_id})")
        print(f"{'='*80}")
        
        for folder_type in folder_types:
            # Find the folder_type folder (e.g., v8_ingest)
            try:
                children = list(syn.getChildren(project_id, includeTypes=['folder']))
                folder_type_id = None
                for child in children:
                    if child['name'] == folder_type:
                        folder_type_id = child['id']
                        break
                
                if not folder_type_id:
                    print(f"  ⚠ {folder_type}/ not found, skipping")
                    continue
                
                # Find the SpatialTranscriptomics folder
                module_children = list(syn.getChildren(folder_type_id, includeTypes=['folder']))
                for child in module_children:
                    if child['name'] == "SpatialTranscriptomics":
                        folders_to_rename.append({
                            "project_name": project_name,
                            "folder_type": folder_type,
                            "folder_id": child['id'],
                            "parent_id": folder_type_id
                        })
                        print(f"  ✓ Found SpatialTranscriptomics folder: {child['id']}")
                        break
                else:
                    print(f"  ℹ SpatialTranscriptomics folder not found in {folder_type}/")
                    
            except Exception as e:
                print(f"  ✗ Error checking {folder_type}/: {e}")
    
    return folders_to_rename


def rename_folder(syn, folder_id: str, new_name: str, dry_run: bool = False) -> bool:
    """
    Rename a folder in Synapse.
    
    Args:
        syn: Synapse client
        folder_id: Synapse ID of the folder to rename
        new_name: New name for the folder
        dry_run: If True, only show what would be renamed
    
    Returns:
        True if successful, False otherwise
    """
    try:
        if dry_run:
            print(f"    [DRY RUN] Would rename {folder_id} to '{new_name}'")
            return True
        
        # Get the folder entity
        folder = syn.get(folder_id)
        
        # Check if it's already renamed
        if folder.name == new_name:
            print(f"    ℹ Folder {folder_id} already named '{new_name}', skipping")
            return True
        
        # Rename it
        folder.name = new_name
        folder = syn.store(folder)
        print(f"    ✓ Renamed {folder_id} to '{new_name}'")
        return True
        
    except Exception as e:
        print(f"    ✗ Failed to rename {folder_id}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Rename 'SpatialTranscriptomics' folders to 'SpatialOmics' in Synapse"
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
        help="Folder types to check (e.g., v8_ingest v8_staging v8_release). Default: all"
    )
    parser.add_argument(
        "--projects-file",
        type=str,
        default="projects.yml",
        help="Path to projects YAML file. Default: projects.yml"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - show what would be renamed without making changes"
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
    print("HTAN2 SpatialTranscriptomics → SpatialOmics Rename")
    print("="*80)
    print(f"Version: {args.version}")
    print(f"Folder Types: {', '.join(folder_types)}")
    print(f"Projects File: {args.projects_file}")
    if args.dry_run:
        print("Mode: DRY RUN")
    print("="*80)
    print()
    
    # Load projects
    projects = load_projects(args.projects_file)
    
    # Find folders to rename
    print("\n" + "="*80)
    print("Finding folders to rename...")
    print("="*80)
    folders_to_rename = find_folders_to_rename(syn, projects, args.version, folder_types)
    
    if not folders_to_rename:
        print("\n✓ No folders found to rename.")
        return
    
    print(f"\n✓ Found {len(folders_to_rename)} folder(s) to rename")
    
    # Rename folders
    print("\n" + "="*80)
    print("Renaming folders...")
    print("="*80)
    
    success_count = 0
    for folder_info in folders_to_rename:
        print(f"\n  {folder_info['project_name']} / {folder_info['folder_type']} / SpatialTranscriptomics")
        if rename_folder(syn, folder_info['folder_id'], "SpatialOmics", args.dry_run):
            success_count += 1
    
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)
    print(f"Successfully renamed: {success_count}/{len(folders_to_rename)}")
    print("="*80)


if __name__ == "__main__":
    main()

