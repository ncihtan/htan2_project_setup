#!/usr/bin/env python3
"""
Script to bind schemas to project subfolders as part of the GitHub Actions workflow.
This script processes the schema_binding_config.yml and binds both file-based and 
record-based schemas to projects. Collects results and continues on errors to provide 
a complete summary.
"""

import yaml
import subprocess
import os
import sys
import json
import re
from typing import List, Optional


def map_schema_name_to_file(schema_name: str, schema_version: str = "v1.0.0") -> str:
    """
    Map schema name from config to expected schema file name pattern.
    
    Args:
        schema_name: Schema name from config (e.g., "BulkWESLevel1", "scRNA_seqLevel1")
        schema_version: Schema version (e.g., "v1.0.0")
    
    Returns:
        Expected file name pattern (e.g., "HTAN.BulkWESLevel1-v1.0.0-schema.json")
    """
    # Remove version suffix if present
    version_suffix = f"-{schema_version}"
    
    # Mapping rules: config name -> file name pattern
    # Handle special cases first
    if schema_name == "DigitalPathology":
        return f"HTAN.DigitalPathologyData{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel1":
        return f"HTAN.scRNALevel1{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel2":
        return f"HTAN.scRNALevel2{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel3_4":
        return f"HTAN.scRNALevel3_4{version_suffix}-schema.json"
    elif schema_name.startswith("SpatialTranscriptomics"):
        # Convert SpatialTranscriptomicsLevel1 -> SpatialLevel1
        level_part = schema_name.replace("SpatialTranscriptomics", "Spatial")
        return f"HTAN.{level_part}{version_suffix}-schema.json"
    else:
        # Default: add HTAN prefix
        return f"HTAN.{schema_name}{version_suffix}-schema.json"


def find_schema_file(schema_name: str, files: List[str], schema_version: str = "v1.0.0") -> Optional[str]:
    """
    Find the matching schema file for a given schema name.
    
    Args:
        schema_name: Schema name from config
        files: List of available schema files
        schema_version: Schema version
    
    Returns:
        Path to schema file if found, None otherwise
    """
    # Get expected file pattern
    expected_pattern = map_schema_name_to_file(schema_name, schema_version)
    expected_name = os.path.basename(expected_pattern)
    
    # Try exact match first (case-insensitive)
    for file in files:
        if file.lower() == expected_name.lower():
            return f'schemas/{file}'
    
    # Try pattern matching: extract the core schema name from expected pattern
    # e.g., "HTAN.BulkWESLevel1-v1.0.0-schema.json" -> "bulkweslevel1"
    expected_core = re.sub(r'^htan\.', '', expected_name.lower())
    expected_core = re.sub(r'-v\d+\.\d+\.\d+-schema\.json$', '', expected_core)
    
    # Also extract from schema_name (config name)
    schema_core = schema_name.lower().replace('_', '')
    
    # Try to find file that matches the core pattern
    for file in files:
        file_lower = file.lower()
        file_core = re.sub(r'^htan\.', '', file_lower)
        file_core = re.sub(r'-v\d+\.\d+\.\d+-schema\.json$', '', file_core)
        
        # Exact core match (preferred)
        if file_core == expected_core:
            return f'schemas/{file}'
        
        # Also try matching with schema_core (config name without underscores)
        if file_core == schema_core:
            return f'schemas/{file}'
    
    # If no exact match, return None (don't do fuzzy matching to avoid wrong matches)
    return None


def main():
    """Main function to process schema bindings."""
    
    # Load configuration
    with open('schema_binding_config.yml', 'r') as f:
        config = yaml.safe_load(f)

    file_based_schemas = config['schema_bindings'].get('file_based', {})
    record_based_schemas = config['schema_bindings'].get('record_based', {})
    organization_name = os.environ.get('ORGANIZATION_NAME', 'HTAN2Organization')
    
    # Get schema version from environment or default to v1.0.0
    schema_version = os.environ.get('SCHEMA_VERSION', 'v1.0.0')

    # Track results
    results = {
        'successful': [],
        'failed': [],
        'skipped': []
    }

    print("="*80)
    print("Schema Binding Workflow")
    print("="*80)
    print(f"Organization: {organization_name}")
    print(f"File-based schemas: {len(file_based_schemas)}")
    print(f"Record-based schemas: {len(record_based_schemas)}")
    print("="*80)
    print()

    # Process file-based schemas
    for schema_name, schema_config in file_based_schemas.items():
        print(f'\n{"="*80}')
        print(f'Processing schema: {schema_name}')
        print(f'{"="*80}')
        
        # Find the schema file
        schema_file = None
        print(f'Looking for schema files matching: {schema_name}')
        print(f'Expected file pattern: {map_schema_name_to_file(schema_name, schema_version)}')
        print(f'Available files in schemas directory:')
        try:
            files = os.listdir('schemas')
            for file in files:
                print(f'  - {file}')
        except FileNotFoundError:
            print('  ❌ schemas directory not found!')
            results['skipped'].append({
                'schema': schema_name,
                'reason': 'Schemas directory not found',
                'projects': [p['name'] for p in schema_config.get('projects', [])]
            })
            continue
        
        # Use the improved matching function
        schema_file = find_schema_file(schema_name, files, schema_version)
        
        if not schema_file:
            print(f'❌ Schema file for {schema_name} not found')
            print(f'   Expected pattern: {map_schema_name_to_file(schema_name, schema_version)}')
            results['skipped'].append({
                'schema': schema_name,
                'reason': f'Schema file not found in schemas directory',
                'projects': [p['name'] for p in schema_config.get('projects', [])]
            })
            continue
            
        print(f'✅ Found schema file: {schema_file}')
        
        # Process each project for this schema
        projects = schema_config.get('projects', [])
        print(f'Found {len(projects)} project(s) for this schema')
        
        for project in projects:
            project_name = project['name']
            synapse_id = project['synapse_id']
            subfolder = project.get('subfolder', 'N/A')
            
            print(f'\n  Binding {schema_name} to {project_name}')
            print(f'    Folder: {subfolder}')
            print(f'    Synapse ID: {synapse_id}')
            
            # Run the binding script
            cmd = [
                'python', 'scripts/synapse_json_schema_bind.py',
                '-p', schema_file,
                '-t', synapse_id,
                '-n', organization_name,
                '--create_fileview'
            ]
            
            try:
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True, 
                    check=True,
                    timeout=300  # 5 minute timeout per binding
                )
                print(f'    ✅ Successfully bound')
                results['successful'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'schema_file': schema_file
                })
                # Print first few lines of output for debugging
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines[:5]:
                    if line.strip():
                        print(f'      {line}')
            except subprocess.TimeoutExpired:
                error_msg = 'Binding timed out after 5 minutes'
                print(f'    ❌ Failed: {error_msg}')
                results['failed'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'error': error_msg
                })
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.strip() if e.stderr else str(e)
                print(f'    ❌ Failed: {error_msg}')
                results['failed'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'error': error_msg[:500]  # Limit error message length
                })
            except Exception as e:
                error_msg = str(e)
                print(f'    ❌ Failed: {error_msg}')
                results['failed'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'error': error_msg[:500]
                })

    # Process record-based schemas (same logic as file-based)
    print("\n" + "="*80)
    print("PROCESSING RECORD-BASED SCHEMAS")
    print("="*80)
    print()
    
    for schema_name, schema_config in record_based_schemas.items():
        print(f'\n{"="*80}')
        print(f'Processing schema: {schema_name}')
        print(f'{"="*80}')
        
        # Find the schema file
        schema_file = None
        print(f'Looking for schema files matching: {schema_name}')
        print(f'Expected file pattern: {map_schema_name_to_file(schema_name, schema_version)}')
        print(f'Available files in schemas directory:')
        try:
            files = os.listdir('schemas')
            for file in files:
                print(f'  - {file}')
        except FileNotFoundError:
            print('  ❌ schemas directory not found!')
            results['skipped'].append({
                'schema': schema_name,
                'reason': 'Schemas directory not found',
                'projects': [p['name'] for p in schema_config.get('projects', [])]
            })
            continue
        
        # Use the improved matching function
        schema_file = find_schema_file(schema_name, files, schema_version)
        
        if not schema_file:
            print(f'❌ Schema file for {schema_name} not found')
            print(f'   Expected pattern: {map_schema_name_to_file(schema_name, schema_version)}')
            results['skipped'].append({
                'schema': schema_name,
                'reason': f'Schema file not found in schemas directory',
                'projects': [p['name'] for p in schema_config.get('projects', [])]
            })
            continue
            
        print(f'✅ Found schema file: {schema_file}')
        
        # Process each project for this schema
        projects = schema_config.get('projects', [])
        print(f'Found {len(projects)} project(s) for this schema')
        
        for project in projects:
            project_name = project['name']
            synapse_id = project['synapse_id']
            subfolder = project.get('subfolder', 'N/A')
            
            print(f'\n  Binding {schema_name} to {project_name}')
            print(f'    Folder: {subfolder}')
            print(f'    Synapse ID: {synapse_id}')
            
            # Run the binding script
            cmd = [
                'python', 'scripts/synapse_json_schema_bind.py',
                '-p', schema_file,
                '-t', synapse_id,
                '-n', organization_name,
                '--create_fileview'
            ]
            
            try:
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True, 
                    check=True,
                    timeout=300  # 5 minute timeout per binding
                )
                print(f'    ✅ Successfully bound')
                results['successful'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'schema_file': schema_file
                })
                # Print first few lines of output for debugging
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines[:5]:
                    if line.strip():
                        print(f'      {line}')
            except subprocess.TimeoutExpired:
                error_msg = 'Binding timed out after 5 minutes'
                print(f'    ❌ Failed: {error_msg}')
                results['failed'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'error': error_msg
                })
            except subprocess.CalledProcessError as e:
                error_msg = e.stderr.strip() if e.stderr else str(e)
                print(f'    ❌ Failed: {error_msg}')
                results['failed'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'error': error_msg[:500]  # Limit error message length
                })
            except Exception as e:
                error_msg = str(e)
                print(f'    ❌ Failed: {error_msg}')
                results['failed'].append({
                    'schema': schema_name,
                    'project': project_name,
                    'synapse_id': synapse_id,
                    'subfolder': subfolder,
                    'error': error_msg[:500]
                })

    # Print summary
    print("\n" + "="*80)
    print("BINDING SUMMARY")
    print("="*80)
    print(f"✅ Successful: {len(results['successful'])}")
    print(f"❌ Failed: {len(results['failed'])}")
    print(f"⏭️  Skipped: {len(results['skipped'])}")
    print("="*80)
    
    # Print failed bindings
    if results['failed']:
        print("\n❌ FAILED BINDINGS:")
        print("-" * 80)
        for failure in results['failed']:
            print(f"  Schema: {failure['schema']}")
            print(f"  Project: {failure['project']} ({failure['synapse_id']})")
            print(f"  Subfolder: {failure['subfolder']}")
            print(f"  Error: {failure['error']}")
            print()
    
    # Print skipped schemas
    if results['skipped']:
        print("\n⏭️  SKIPPED SCHEMAS:")
        print("-" * 80)
        for skipped in results['skipped']:
            print(f"  Schema: {skipped['schema']}")
            print(f"  Reason: {skipped['reason']}")
            print(f"  Projects affected: {', '.join(skipped['projects'])}")
            print()
    
    # Save results to JSON file for GitHub Action
    with open('binding_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults saved to: binding_results.json")
    
    # Exit with error code if there were failures
    if results['failed']:
        print(f"\n⚠️  Warning: {len(results['failed'])} binding(s) failed")
        print("Check the failed bindings above for details")
        # Don't exit with error code - we want to see all results
        # sys.exit(1)
    
    return results


if __name__ == "__main__":
    main()
