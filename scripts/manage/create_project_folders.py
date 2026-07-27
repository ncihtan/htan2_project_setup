#!/usr/bin/env python3
"""
Script to create standardized folder structures for HTAN2 projects.
Creates folders for all modules (file-based and record-based) with proper access controls.
"""

import os
import synapseclient
import yaml
import argparse
from typing import Dict, List

# Import shared utilities
import sys
from pathlib import Path
# Add parent directories to path to import htan2_synapse
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from htan2_synapse import (
    load_projects,
    set_folder_permissions,
    create_folder,
    RECORD_BASED_MODULES,
    FILE_BASED_MODULES,
    IMAGING_SUBFOLDERS,
    IMAGING_RECORD_BASED_SUBFOLDERS,
    SPATIAL_RECORD_BASED_SUBFOLDERS,
)




def create_project_folders(syn, projects: Dict[str, str], version: str, folder_types: List[str], dry_run: bool = False):
    """
    Create folder structure for all projects.
    
    Args:
        syn: Synapse client
        projects: Dictionary mapping project names to Synapse IDs
        version: Version prefix (e.g., "v8")
        folder_types: List of folder types to create (e.g., ["v8_ingest"])
        dry_run: If True, only print what would be created without actually creating
    """
    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No folders will be created")
        print("=" * 80)
        print()
    
    all_projects_structure = {}
    
    for project_name, project_id in sorted(projects.items()):
        print(f"\n{'='*80}")
        print(f"Processing: {project_name} ({project_id})")
        print(f"{'='*80}")
        
        project_structure = {}
        
        # Create each version folder type
        for folder_type in folder_types:
            print(f"\nCreating {folder_type}/ folder...")
            
            if dry_run:
                print(f"  [DRY RUN] Would create: {folder_type}/")
                folder_id = f"synXXXXXXX"
            else:
                folder_id = create_folder(syn, project_id, folder_type)
                if folder_id:
                    # Set permissions
                    set_folder_permissions(syn, folder_id, folder_type, version, project_name)
                    print(f"  ✓ Set permissions for {folder_type}/")
            
            if not folder_id:
                print(f"  ✗ Skipping {folder_type}/ due to creation failure")
                continue
            
            project_structure[folder_type] = {"synapse_id": folder_id, "modules": {}}
            
            # Create record-based modules (Clinical, Biospecimen, etc.)
            for module_name, subfolders in RECORD_BASED_MODULES.items():
                print(f"\n  Creating {module_name}/ module...")
                
                if dry_run:
                    print(f"    [DRY RUN] Would create: {module_name}/")
                    module_id = f"synXXXXXXX"
                else:
                    module_id = create_folder(syn, folder_id, module_name)
                
                if not module_id:
                    continue
                
                project_structure[folder_type]["modules"][module_name] = {
                    "synapse_id": module_id,
                    "subfolders": {}
                }
                
                # Create subfolders (if any)
                if subfolders:
                    for subfolder in subfolders:
                        if dry_run:
                            print(f"      [DRY RUN] Would create: {subfolder}/")
                            subfolder_id = f"synXXXXXXX"
                        else:
                            subfolder_id = create_folder(syn, module_id, subfolder)
                        
                        if subfolder_id:
                            project_structure[folder_type]["modules"][module_name]["subfolders"][subfolder] = subfolder_id
            
            # Create file-based modules (Assays)
            for module_name, subfolders in FILE_BASED_MODULES.items():
                print(f"\n  Creating {module_name}/ module...")
                
                if dry_run:
                    print(f"    [DRY RUN] Would create: {module_name}/")
                    module_id = f"synXXXXXXX"
                else:
                    module_id = create_folder(syn, folder_id, module_name)
                
                if not module_id:
                    continue
                
                project_structure[folder_type]["modules"][module_name] = {
                    "synapse_id": module_id,
                    "subfolders": {}
                }
                
                # Special handling for Imaging module
                if module_name == "Imaging":
                    # Create DigitalPathology and MultiplexMicroscopy as subfolders
                    for imaging_subfolder in subfolders:
                        if dry_run:
                            print(f"      [DRY RUN] Would create: {imaging_subfolder}/")
                            imaging_subfolder_id = f"synXXXXXXX"
                        else:
                            imaging_subfolder_id = create_folder(syn, module_id, imaging_subfolder)
                        
                        if not imaging_subfolder_id:
                            continue
                        
                        # Store the imaging subfolder
                        if imaging_subfolder not in project_structure[folder_type]["modules"][module_name]["subfolders"]:
                            project_structure[folder_type]["modules"][module_name]["subfolders"][imaging_subfolder] = {
                                "synapse_id": imaging_subfolder_id,
                                "subfolders": {}
                            }
                        
                        # Create subfolders for MultiplexMicroscopy
                        if imaging_subfolder == "MultiplexMicroscopy":
                            for level in IMAGING_SUBFOLDERS["MultiplexMicroscopy"]:
                                if dry_run:
                                    print(f"        [DRY RUN] Would create: {level}/")
                                    level_id = f"synXXXXXXX"
                                else:
                                    level_id = create_folder(syn, imaging_subfolder_id, level)

                                if level_id:
                                    project_structure[folder_type]["modules"][module_name]["subfolders"][imaging_subfolder]["subfolders"][level] = level_id

                            for rb_subfolder in IMAGING_RECORD_BASED_SUBFOLDERS.get("MultiplexMicroscopy", []):
                                if dry_run:
                                    print(f"        [DRY RUN] Would create: {rb_subfolder}/")
                                    rb_id = f"synXXXXXXX"
                                else:
                                    rb_id = create_folder(syn, imaging_subfolder_id, rb_subfolder)

                                if rb_id:
                                    project_structure[folder_type]["modules"][module_name]["subfolders"][imaging_subfolder].setdefault("record_based_subfolders", {})[rb_subfolder] = rb_id
                else:
                    # Create subfolders (if any) for other modules
                    if subfolders:
                        for subfolder in subfolders:
                            if dry_run:
                                print(f"      [DRY RUN] Would create: {subfolder}/")
                                subfolder_id = f"synXXXXXXX"
                            else:
                                subfolder_id = create_folder(syn, module_id, subfolder)

                            if subfolder_id:
                                project_structure[folder_type]["modules"][module_name]["subfolders"][subfolder] = subfolder_id

                    # Create record-based subfolders (e.g. SpatialOmics/Panel)
                    for rb_subfolder in SPATIAL_RECORD_BASED_SUBFOLDERS.get(module_name, []):
                        if dry_run:
                            print(f"      [DRY RUN] Would create: {rb_subfolder}/")
                            rb_id = f"synXXXXXXX"
                        else:
                            rb_id = create_folder(syn, module_id, rb_subfolder)
                        if rb_id:
                            project_structure[folder_type]["modules"][module_name].setdefault("record_based_subfolders", {})[rb_subfolder] = rb_id
        
        all_projects_structure[project_name] = project_structure
    
    # Save folder structure to YAML
    output_file = f"folder_structure_{version}.yml"
    # Structure: {version: {projects: {project_name: {synapse_id, folders: {...}}}}}
    output_data = {
        version: {
            "projects": {}
        }
    }
    
    for project_name, project_id in sorted(projects.items()):
        project_data = {
            "synapse_id": project_id,
            "folders": all_projects_structure.get(project_name, {})
        }
        
        output_data[version]["projects"][project_name] = project_data
    
    with open(output_file, 'w') as f:
        yaml.dump(output_data, f, default_flow_style=False, sort_keys=False)
    print(f"\n{'='*80}")
    print(f"Folder structure saved to: {output_file}")
    if dry_run:
        print("  (Note: Contains placeholder IDs in dry-run mode)")
    print(f"{'='*80}")

    # NOTE: The canonical schema_binding_{version}.yml (with real Synapse IDs and the
    # single-source-of-truth schema names) is produced by update_schema_bindings.py, which
    # setup_folders.py runs right after folder creation. This script used to emit its own
    # schema-binding file here with a divergent naming convention; that has been removed to
    # keep one source of truth (htan2_synapse.iter_binding_targets).

    return all_projects_structure


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Create folder structure for HTAN2 projects",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create v8 folders (ingest, staging, release) for all projects
  python create_project_folders.py --version v8

  # Create v8 folders using just the number
  python create_project_folders.py --version 8

  # Create only v8_ingest folders
  python create_project_folders.py --version v8 --folder-type ingest

  # Create specific folder types
  python create_project_folders.py --version v8 --folder-type ingest --folder-type staging
        """
    )
    parser.add_argument(
        "--version",
        type=str,
        default="v8",
        help="Version number or prefix (e.g., '8' or 'v8'). Will create folders like {version}_ingest, {version}_staging, {version}_release. Default: v8"
    )
    parser.add_argument(
        "--folder-type",
        type=str,
        action="append",
        choices=["ingest", "staging", "release"],
        help="Folder type to create (ingest, staging, or release). Can be specified multiple times. If not specified, all three types will be created."
    )
    parser.add_argument(
        "--projects-file",
        type=str,
        default="projects.yml",
        help="Path to projects.yml file (default: projects.yml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - show what would be created without actually creating"
    )
    
    args = parser.parse_args()
    
    # Normalize version: if it's just a number, prepend 'v'
    version = args.version
    if version and not version.startswith('v'):
        # Check if it's a number
        try:
            int(version)
            version = f"v{version}"
        except ValueError:
            # Not a number, use as-is
            pass
    
    # Determine folder types
    if args.folder_type:
        # User specified specific folder types
        folder_types = [f"{version}_{ft}" for ft in args.folder_type]
    else:
        # Default: create all three folder types (ingest, staging, release)
        folder_types = [f"{version}_ingest", f"{version}_staging", f"{version}_release"]
    
    # Load projects
    projects = load_projects(args.projects_file)
    
    if not projects:
        print("No projects found. Please check projects.yml file.")
        return
    
    # Login to Synapse
    if not args.dry_run:
        print("Logging in to Synapse...")
        syn = synapseclient.Synapse()
        auth_token = os.environ.get("SYNAPSE_PAT")
        username = os.environ.get("SYNAPSE_USERNAME")
        if auth_token:
            syn.login(authToken=auth_token)
        elif username:
            syn.login(username)
        else:
            syn.login()
        print("✓ Logged in successfully\n")
    else:
        syn = None
    
    # Print summary
    print("="*80)
    print("HTAN2 Folder Creation")
    print("="*80)
    print(f"Version: {version}")
    print(f"Folder Types: {', '.join(folder_types)}")
    print(f"Projects: {len(projects)}")
    if args.dry_run:
        print("Mode: DRY RUN")
    print("="*80)
    print()
    
    # Create folders
    create_project_folders(syn, projects, version, folder_types, dry_run=args.dry_run)
    
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()

