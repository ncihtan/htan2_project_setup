#!/usr/bin/env python3
"""
Script to update schema binding file with real Synapse IDs from created folders.
This script queries Synapse to get the actual folder IDs and updates schema_binding_{version}.yml.
"""

import synapseclient
import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, Optional

# Add parent directories to path to import htan2_synapse
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from htan2_synapse import load_projects, RECORD_BASED_MODULES, FILE_BASED_MODULES, IMAGING_SUBFOLDERS


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
                                
                                module_structure["subfolders"][imaging_subfolder] = imaging_structure
                        
                        folder_structure["modules"][module_name] = module_structure
                else:
                    # Regular file-based modules (WES, scRNA_seq, SpatialTranscriptomics)
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
            
            project_structure["folders"][folder_type] = folder_structure
        
        structure[version]["projects"][project_name] = project_structure
    
    return structure


def generate_schema_binding_from_structure(structure: Dict, version: str, folder_types: list) -> Dict:
    """
    Generate schema binding structure from folder structure.
    Similar to generate_schema_binding_structure but uses real folder structure.
    """
    schema_bindings = {
        "schema_bindings": {
            "file_based": {},
            "record_based": {}
        }
    }
    
    projects_data = structure[version]["projects"]
    
    for project_name, project_data in projects_data.items():
        folders = project_data.get("folders", {})
        
        for folder_type in folder_types:
            if folder_type not in folders:
                continue
            
            folder_data = folders[folder_type]
            modules = folder_data.get("modules", {})
            
            # Record-based schemas
            if "Clinical" in modules:
                clinical_data = modules["Clinical"]
                for subfolder_name, subfolder_id in clinical_data.get("subfolders", {}).items():
                    schema_name = subfolder_name
                    subfolder_path = f"{folder_type}/Clinical/{subfolder_name}"
                    
                    if schema_name not in schema_bindings["schema_bindings"]["record_based"]:
                        schema_bindings["schema_bindings"]["record_based"][schema_name] = {"projects": []}
                    
                    schema_bindings["schema_bindings"]["record_based"][schema_name]["projects"].append({
                        "name": project_name,
                        "subfolder": subfolder_path,
                        "synapse_id": subfolder_id
                    })
            
            if "Biospecimen" in modules:
                biospecimen_data = modules["Biospecimen"]
                biospecimen_id = biospecimen_data.get("synapse_id")
                if biospecimen_id:
                    schema_name = "Biospecimen"
                    subfolder_path = f"{folder_type}/Biospecimen"
                    
                    if schema_name not in schema_bindings["schema_bindings"]["record_based"]:
                        schema_bindings["schema_bindings"]["record_based"][schema_name] = {"projects": []}
                    
                    schema_bindings["schema_bindings"]["record_based"][schema_name]["projects"].append({
                        "name": project_name,
                        "subfolder": subfolder_path,
                        "synapse_id": biospecimen_id
                    })
            
            # File-based schemas
            for module_name in ["WES", "scRNA_seq", "SpatialTranscriptomics"]:
                if module_name in modules:
                    module_data = modules[module_name]
                    for subfolder_name, subfolder_id in module_data.get("subfolders", {}).items():
                        # Map subfolder names to schema names
                        schema_name_map = {
                            "Level_1": f"Bulk{module_name}Level1" if module_name == "WES" else f"{module_name}Level1",
                            "Level_2": f"Bulk{module_name}Level2" if module_name == "WES" else f"{module_name}Level2",
                            "Level_3": f"Bulk{module_name}Level3" if module_name == "WES" else f"{module_name}Level3",
                            "Level_3_4": f"{module_name}Level3_4",
                            "Level_4": f"{module_name}Level4",
                            "Panel": f"SpatialPanel"
                        }
                        
                        schema_name = schema_name_map.get(subfolder_name, subfolder_name)
                        subfolder_path = f"{folder_type}/{module_name}/{subfolder_name}"
                        
                        if schema_name not in schema_bindings["schema_bindings"]["file_based"]:
                            schema_bindings["schema_bindings"]["file_based"][schema_name] = {"projects": []}
                        
                        schema_bindings["schema_bindings"]["file_based"][schema_name]["projects"].append({
                            "name": project_name,
                            "subfolder": subfolder_path,
                            "synapse_id": subfolder_id
                        })
            
            # Imaging schemas
            if "Imaging" in modules:
                imaging_data = modules["Imaging"]
                for imaging_subfolder_name, imaging_subfolder_data in imaging_data.get("subfolders", {}).items():
                    if imaging_subfolder_name == "DigitalPathology":
                        schema_name = "DigitalPathology"
                        subfolder_id = imaging_subfolder_data.get("synapse_id")
                        subfolder_path = f"{folder_type}/Imaging/DigitalPathology"
                    elif imaging_subfolder_name == "MultiplexMicroscopy":
                        # MultiplexMicroscopy has levels
                        for level_name, level_id in imaging_subfolder_data.get("subfolders", {}).items():
                            schema_name = f"MultiplexMicroscopy{level_name.replace('_', '')}"
                            subfolder_path = f"{folder_type}/Imaging/MultiplexMicroscopy/{level_name}"
                            
                            if schema_name not in schema_bindings["schema_bindings"]["file_based"]:
                                schema_bindings["schema_bindings"]["file_based"][schema_name] = {"projects": []}
                            
                            schema_bindings["schema_bindings"]["file_based"][schema_name]["projects"].append({
                                "name": project_name,
                                "subfolder": subfolder_path,
                                "synapse_id": level_id
                            })
                        continue
                    else:
                        continue
                    
                    if schema_name not in schema_bindings["schema_bindings"]["file_based"]:
                        schema_bindings["schema_bindings"]["file_based"][schema_name] = {"projects": []}
                    
                    schema_bindings["schema_bindings"]["file_based"][schema_name]["projects"].append({
                        "name": project_name,
                        "subfolder": subfolder_path,
                        "synapse_id": subfolder_id
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

