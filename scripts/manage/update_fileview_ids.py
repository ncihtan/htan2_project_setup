#!/usr/bin/env python3
"""
Script to extract fileview IDs from wikis and add them to schema binding config files.

This script reads schema_binding_config.yml, extracts fileview IDs from wikis for each
bound schema entity, and updates the config file with fileview_id fields.

Usage:
    python scripts/manage/update_fileview_ids.py --config-file schema_binding_config.yml
"""

import synapseclient
import yaml
import argparse
import sys
import re
from pathlib import Path
from typing import Dict, Optional

# Add parent directories to path to import htan2_synapse
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def extract_fileview_id_from_wiki(syn, entity_id: str) -> Optional[str]:
    """Extract fileview ID from existing wiki if it exists."""
    try:
        wiki = syn.getWiki(entity_id)
        wiki_content = wiki.markdown
        
        # Look for fileview ID patterns in the wiki
        # Pattern 1: "Fileview ID: syn12345678"
        match = re.search(r'Fileview ID:\s*(syn\d+)', wiki_content, re.IGNORECASE)
        if match:
            return match.group(1)
        
        # Pattern 2: "syn12345678" in a link or text
        match = re.search(r'(syn\d{8,})', wiki_content)
        if match:
            return match.group(1)
            
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        if e.response.status_code == 404:
            # No wiki exists
            return None
    except Exception as e:
        print(f"  ⚠ Warning: Could not get wiki for {entity_id}: {e}")
    
    return None


def find_fileview_in_entity(syn, entity_id: str) -> Optional[str]:
    """Find a fileview associated with an entity by checking its children."""
    try:
        # Get all children and check their types
        children = list(syn.getChildren(entity_id))
        
        for child in children:
            try:
                # Get the full entity to check its type
                entity = syn.get(child['id'], downloadFile=False)
                # Check if it's a fileview (EntityViewSchema)
                if hasattr(entity, 'concreteType'):
                    if 'EntityView' in entity.concreteType:
                        return entity.id
            except Exception:
                # Skip if we can't get the entity
                continue
                
    except Exception as e:
        print(f"  ⚠ Warning: Could not find fileview for {entity_id}: {e}")
    
    return None


def update_fileview_ids_in_config(syn, config_file: str, dry_run: bool = False):
    """
    Update schema binding config with fileview IDs extracted from wikis.
    
    Args:
        syn: Synapse client
        config_file: Path to schema binding config YAML file
        dry_run: If True, only show what would be updated
    """
    print("="*80)
    print("Update Fileview IDs in Schema Binding Config")
    print("="*80)
    print(f"Config file: {config_file}")
    if dry_run:
        print("Mode: DRY RUN")
    print("="*80)
    print()
    
    # Load config
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    schema_bindings = config.get('schema_bindings', {})
    file_based = schema_bindings.get('file_based', {})
    record_based = schema_bindings.get('record_based', {})
    
    total_updated = 0
    total_not_found = 0
    total_errors = 0
    
    # Process file-based schemas
    print("\n" + "="*80)
    print("Processing file-based schemas...")
    print("="*80)
    
    for schema_name, schema_config in file_based.items():
        projects = schema_config.get('projects', [])
        print(f"\n{schema_name}: {len(projects)} project(s)")
        
        for project in projects:
            entity_id = project.get('synapse_id')
            project_name = project.get('name', 'Unknown')
            
            if not entity_id:
                continue
            
            # Try to extract fileview ID from wiki first
            fileview_id = extract_fileview_id_from_wiki(syn, entity_id)
            
            # If not found in wiki, try to find it as a child entity
            if not fileview_id:
                fileview_id = find_fileview_in_entity(syn, entity_id)
            
            if fileview_id:
                if not dry_run:
                    project['fileview_id'] = fileview_id
                print(f"  ✅ {project_name} ({entity_id}): {fileview_id}")
                total_updated += 1
            else:
                print(f"  ⚠️  {project_name} ({entity_id}): No fileview found")
                total_not_found += 1
    
    # Process record-based schemas
    print("\n" + "="*80)
    print("Processing record-based schemas...")
    print("="*80)
    
    for schema_name, schema_config in record_based.items():
        projects = schema_config.get('projects', [])
        print(f"\n{schema_name}: {len(projects)} project(s)")
        
        for project in projects:
            entity_id = project.get('synapse_id')
            project_name = project.get('name', 'Unknown')
            
            if not entity_id:
                continue
            
            # Try to extract fileview ID from wiki first
            fileview_id = extract_fileview_id_from_wiki(syn, entity_id)
            
            # If not found in wiki, try to find it as a child entity
            if not fileview_id:
                fileview_id = find_fileview_in_entity(syn, entity_id)
            
            if fileview_id:
                if not dry_run:
                    project['fileview_id'] = fileview_id
                print(f"  ✅ {project_name} ({entity_id}): {fileview_id}")
                total_updated += 1
            else:
                print(f"  ⚠️  {project_name} ({entity_id}): No fileview found")
                total_not_found += 1
    
    # Save updated config
    if not dry_run and total_updated > 0:
        print("\n" + "="*80)
        print("Saving updated config...")
        print("="*80)
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        
        print(f"✅ Updated {config_file} with {total_updated} fileview ID(s)")
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"✅ Fileview IDs found and updated: {total_updated}")
    print(f"⚠️  No fileview found: {total_not_found}")
    print(f"❌ Errors: {total_errors}")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description="Extract fileview IDs from wikis and update schema binding config"
    )
    parser.add_argument(
        "--config-file",
        type=str,
        default="schema_binding_config.yml",
        help="Path to schema binding config YAML file (default: schema_binding_config.yml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - show what would be updated without making changes"
    )
    
    args = parser.parse_args()
    
    if not Path(args.config_file).exists():
        print(f"Error: Config file not found: {args.config_file}")
        sys.exit(1)
    
    # Login to Synapse
    syn = synapseclient.Synapse()
    syn.login()
    
    print("✅ Successfully logged in to Synapse\n")
    
    # Update fileview IDs
    update_fileview_ids_in_config(syn, args.config_file, args.dry_run)


if __name__ == "__main__":
    main()

