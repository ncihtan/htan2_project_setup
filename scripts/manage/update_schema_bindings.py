#!/usr/bin/env python3
"""
Script to update schema binding file with real Synapse IDs from created folders.
This script queries Synapse to get the actual folder IDs and updates schema_binding_{version}.yml.
"""

import os
import synapseclient
import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, Optional

# Add parent directories to path to import htan2_synapse
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from htan2_synapse import (
    load_projects,
    RECORD_BASED_MODULES,
    FILE_BASED_MODULES,
    IMAGING_SUBFOLDERS,
    IMAGING_RECORD_BASED_SUBFOLDERS,
    SPATIAL_RECORD_BASED_SUBFOLDERS,
    iter_binding_targets,
)


def find_folder_id(syn, parent_id: str, folder_name: str) -> Optional[str]:
    """Find a folder ID by name under a parent."""
    try:
        children = list(syn.getChildren(parent_id, includeTypes=['folder']))
        for child in children:
            if child['name'] == folder_name:
                return child['id']
        return None
    except Exception as e:
        print(f"  ⚠ Warning: Could not find folder '{folder_name}' under {parent_id}: {e}")
        return None


def get_folder_structure_from_synapse(syn, projects: Dict[str, str], version: str, folder_types: list) -> Dict:
    """
    Query Synapse to get the actual folder structure with real IDs.
    
    Returns a structure similar to folder_structure_{version}.yml
    """
    structure = {version: {"projects": {}}}
    
    for project_name, project_id in sorted(projects.items()):
        print(f"\n{'='*80}")
        print(f"Querying: {project_name} ({project_id})")
        print(f"{'='*80}")
        
        project_structure = {
            "synapse_id": project_id,
            "folders": {}
        }
        
        for folder_type in folder_types:
            print(f"\n  Finding {folder_type}/...")
            folder_id = find_folder_id(syn, project_id, folder_type)
            
            if not folder_id:
                print(f"  ⚠ {folder_type}/ not found, skipping")
                continue
            
            print(f"  ✓ Found {folder_type}/: {folder_id}")
            
            folder_structure = {
                "synapse_id": folder_id,
                "modules": {}
            }
            
            # Record-based modules
            for module_name, subfolders in RECORD_BASED_MODULES.items():
                module_id = find_folder_id(syn, folder_id, module_name)
                if module_id:
                    print(f"    ✓ Found {module_name}/: {module_id}")
                    module_structure = {
                        "synapse_id": module_id,
                        "subfolders": {}
                    }
                    
                    for subfolder_name in subfolders:
                        subfolder_id = find_folder_id(syn, module_id, subfolder_name)
                        if subfolder_id:
                            module_structure["subfolders"][subfolder_name] = subfolder_id
                            print(f"      ✓ Found {subfolder_name}/: {subfolder_id}")
                    
                    folder_structure["modules"][module_name] = module_structure
                else:
                    print(f"    ⚠ {module_name}/ not found")
            
            # File-based modules
            for module_name, subfolders in FILE_BASED_MODULES.items():
                if module_name == "Imaging":
                    # Special handling for Imaging
                    module_id = find_folder_id(syn, folder_id, module_name)
                    if module_id:
                        print(f"    ✓ Found {module_name}/: {module_id}")
                        module_structure = {
                            "synapse_id": module_id,
                            "subfolders": {}
                        }
                        
                        for imaging_subfolder in subfolders:
                            imaging_subfolder_id = find_folder_id(syn, module_id, imaging_subfolder)
                            if imaging_subfolder_id:
                                print(f"      ✓ Found {imaging_subfolder}/: {imaging_subfolder_id}")
                                imaging_structure = {
                                    "synapse_id": imaging_subfolder_id,
                                    "subfolders": {}
                                }
                                
                                # Check for Imaging subfolders (e.g., MultiplexMicroscopy levels)
                                if imaging_subfolder in IMAGING_SUBFOLDERS:
                                    for level in IMAGING_SUBFOLDERS[imaging_subfolder]:
                                        level_id = find_folder_id(syn, imaging_subfolder_id, level)
                                        if level_id:
                                            imaging_structure["subfolders"][level] = level_id
                                            print(f"        ✓ Found {level}/: {level_id}")

                                # Record-based subfolders nested under Imaging modules (e.g., ChannelMetadata)
                                for record_subfolder in IMAGING_RECORD_BASED_SUBFOLDERS.get(imaging_subfolder, []):
                                    record_id = find_folder_id(syn, imaging_subfolder_id, record_subfolder)
                                    if record_id:
                                        imaging_structure["subfolders"][record_subfolder] = record_id
                                        print(f"        ✓ Found {record_subfolder}/: {record_id}")

                                module_structure["subfolders"][imaging_subfolder] = imaging_structure
                        
                        folder_structure["modules"][module_name] = module_structure
                else:
                    # Regular file-based modules (WES, scRNA_seq, SpatialOmics)
                    module_id = find_folder_id(syn, folder_id, module_name)
                    if module_id:
                        print(f"    ✓ Found {module_name}/: {module_id}")
                        module_structure = {
                            "synapse_id": module_id,
                            "subfolders": {}
                        }
                        
                        for subfolder_name in subfolders:
                            subfolder_id = find_folder_id(syn, module_id, subfolder_name)
                            if subfolder_id:
                                module_structure["subfolders"][subfolder_name] = subfolder_id
                                print(f"      ✓ Found {subfolder_name}/: {subfolder_id}")

                        # Record-based subfolders nested under file-based modules (e.g., SpatialPanel)
                        for record_subfolder in SPATIAL_RECORD_BASED_SUBFOLDERS.get(module_name, []):
                            record_id = find_folder_id(syn, module_id, record_subfolder)
                            if record_id:
                                module_structure["subfolders"][record_subfolder] = record_id
                                print(f"      ✓ Found {record_subfolder}/: {record_id}")

                        folder_structure["modules"][module_name] = module_structure
            
            project_structure["folders"][folder_type] = folder_structure
        
        structure[version]["projects"][project_name] = project_structure
    
    return structure


def _resolve_folder_id(modules: Dict, path_segments: list):
    """Walk the discovered folder structure for a canonical path (e.g.
    ["Imaging", "MultiplexMicroscopy", "Level_2"]) and return its synapse_id, or None.

    Handles the two value shapes get_folder_structure_from_synapse produces: an
    intermediate node is a dict {synapse_id, subfolders}, a leaf level is a plain id.
    """
    node = None
    children = modules  # top-level module dicts
    for seg in path_segments:
        if seg not in children:
            return None
        val = children[seg]
        node = val
        children = val.get("subfolders", {}) if isinstance(val, dict) else {}
    return node.get("synapse_id") if isinstance(node, dict) else node


def generate_schema_binding_from_structure(structure: Dict, version: str, folder_types: list) -> Dict:
    """
    Generate schema binding structure from the discovered folder structure.

    Canonical schema names and file-vs-record routing come from the single source of
    truth (htan2_synapse.iter_binding_targets); this function only attaches the real
    Synapse IDs discovered under each project/folder_type.
    """
    schema_bindings = {"schema_bindings": {"file_based": {}, "record_based": {}}}
    targets = list(iter_binding_targets())

    projects_data = structure[version]["projects"]

    for project_name, project_data in projects_data.items():
        folders = project_data.get("folders", {})

        for folder_type in folder_types:
            if folder_type not in folders:
                continue

            modules = folders[folder_type].get("modules", {})

            for target in targets:
                folder_id = _resolve_folder_id(modules, target["path"].split("/"))
                if not folder_id:
                    continue

                section = "file_based" if target["kind"] == "file" else "record_based"
                schema_name = target["schema_name"]
                schema_bindings["schema_bindings"][section].setdefault(
                    schema_name, {"projects": []}
                )["projects"].append({
                    "name": project_name,
                    "subfolder": f"{folder_type}/{target['path']}",
                    "synapse_id": folder_id,
                })

    return schema_bindings


def main():
    parser = argparse.ArgumentParser(
        description="Update schema binding file with real Synapse IDs from created folders",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Update schema_binding_v8.yml with real IDs (all folder types)
  python update_schema_bindings.py --version v8

  # Update only for staging folders
  python update_schema_bindings.py --version v8 --folder-type staging

  # Dry run to see what would be updated
  python update_schema_bindings.py --version v8 --dry-run
        """
    )
    parser.add_argument(
        "--version",
        type=str,
        required=True,
        help="Version prefix (e.g., v8)"
    )
    parser.add_argument(
        "--folder-type",
        type=str,
        nargs="+",
        choices=["ingest", "staging", "release"],
        help="Folder types to update (ingest, staging, release). If not specified, all types will be updated."
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
        help="Dry run mode - show what would be updated without actually updating"
    )
    
    args = parser.parse_args()
    
    # Normalize version
    version = args.version
    if not version.startswith('v'):
        try:
            int(version)
            version = f"v{version}"
        except ValueError:
            # If the version is not an integer, leave it as provided (e.g., custom tags like 'v8-beta').
            pass
    
    # Determine folder types
    if args.folder_type:
        folder_types = [f"{version}_{ft}" for ft in args.folder_type]
    else:
        folder_types = [f"{version}_ingest", f"{version}_staging", f"{version}_release"]
    
    # Load projects
    projects = load_projects(args.projects_file)
    if not projects:
        print("No projects found. Please check projects.yml file.")
        sys.exit(1)
    
    print("="*80)
    print("Update Schema Bindings with Real Synapse IDs")
    print("="*80)
    print(f"Version: {version}")
    print(f"Folder Types: {', '.join(folder_types)}")
    print(f"Projects: {len(projects)}")
    if args.dry_run:
        print("Mode: DRY RUN")
    print("="*80)
    print()
    
    if not args.dry_run:
        # Login to Synapse
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

        # Query Synapse for folder structure
        print("Querying Synapse for folder structure...")
        structure = get_folder_structure_from_synapse(syn, projects, version, folder_types)

        # Generate schema binding structure
        print("\n" + "="*80)
        print("Generating schema binding structure...")
        print("="*80)
        schema_binding_data = generate_schema_binding_from_structure(structure, version, folder_types)

        # Save to file
        schema_binding_file = f"schema_binding_{version}.yml"
        with open(schema_binding_file, 'w') as f:
            yaml.dump(schema_binding_data, f, default_flow_style=False, sort_keys=False)

        print(f"\n✓ Schema binding file updated: {schema_binding_file}")
        print(f"\nNext steps:")
        print(f"1. Review {schema_binding_file}")
        print(f"2. Merge into schema_binding_config.yml (typically only staging folders):")
        print(f"   python merge_schema_bindings.py --schema-binding-file {schema_binding_file} --folder-type-filter {version}_staging")
        print(f"3. Bind schemas using:")
        print(f"   python scripts/bind_schemas_workflow.py")
    else:
        print("DRY RUN: Would query Synapse and update schema_binding_{version}.yml")
        print("Run without --dry-run to actually update.")
    
    print("\n" + "="*80)
    print("COMPLETE")
    print("="*80)


if __name__ == "__main__":
    main()


