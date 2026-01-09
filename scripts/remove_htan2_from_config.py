#!/usr/bin/env python3
"""
Script to remove HTAN2 (syn63296487) project entries from schema_binding_config.yml.
This project should not have schema bindings.
"""

import yaml
import sys
from pathlib import Path


def remove_htan2_entries(config_file: str = "schema_binding_config.yml"):
    """Remove all HTAN2 project entries from the config."""
    
    # Load config
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    if 'schema_bindings' not in config:
        print("No schema_bindings found in config")
        return
    
    removed_count = 0
    
    # Process file-based schemas
    if 'file_based' in config['schema_bindings']:
        for schema_name, schema_config in config['schema_bindings']['file_based'].items():
            if 'projects' in schema_config:
                original_count = len(schema_config['projects'])
                schema_config['projects'] = [
                    p for p in schema_config['projects'] 
                    if p.get('name') != 'HTAN2'
                ]
                removed = original_count - len(schema_config['projects'])
                if removed > 0:
                    removed_count += removed
                    print(f"  Removed {removed} HTAN2 entry(ies) from {schema_name}")
    
    # Process record-based schemas
    if 'record_based' in config['schema_bindings']:
        for schema_name, schema_config in config['schema_bindings']['record_based'].items():
            if 'projects' in schema_config:
                original_count = len(schema_config['projects'])
                schema_config['projects'] = [
                    p for p in schema_config['projects'] 
                    if p.get('name') != 'HTAN2'
                ]
                removed = original_count - len(schema_config['projects'])
                if removed > 0:
                    removed_count += removed
                    print(f"  Removed {removed} HTAN2 entry(ies) from {schema_name}")
    
    # Save updated config
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    
    print(f"\n✓ Removed {removed_count} total HTAN2 entries from {config_file}")
    print(f"✓ Updated config saved")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Remove HTAN2 project entries from schema_binding_config.yml"
    )
    parser.add_argument(
        '--config-file',
        default='schema_binding_config.yml',
        help='Path to schema_binding_config.yml (default: schema_binding_config.yml)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be removed without actually removing'
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        print("DRY RUN MODE - Would remove HTAN2 entries:")
        # Load and count
        with open(args.config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        count = 0
        for schema_type in ['file_based', 'record_based']:
            if schema_type in config.get('schema_bindings', {}):
                for schema_name, schema_config in config['schema_bindings'][schema_type].items():
                    htan2_entries = [
                        p for p in schema_config.get('projects', [])
                        if p.get('name') == 'HTAN2'
                    ]
                    if htan2_entries:
                        count += len(htan2_entries)
                        print(f"  Would remove {len(htan2_entries)} HTAN2 entry(ies) from {schema_name}")
        
        print(f"\nTotal: {count} entries would be removed")
    else:
        print("Removing HTAN2 entries from schema_binding_config.yml...")
        remove_htan2_entries(args.config_file)

