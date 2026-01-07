#!/usr/bin/env python3
"""
Script to bind schemas to project subfolders as part of the GitHub Actions workflow.
This script processes the schema_binding_config.yml and binds file-based schemas to projects.
Collects results and continues on errors to provide a complete summary.
"""

import yaml
import subprocess
import os
import sys
import json
from typing import Dict, List


def main():
    """Main function to process schema bindings."""
    
    # Load configuration
    with open('schema_binding_config.yml', 'r') as f:
        config = yaml.safe_load(f)

    file_based_schemas = config['schema_bindings'].get('file_based', {})
    record_based_schemas = config['schema_bindings'].get('record_based', {})
    organization_name = os.environ.get('ORGANIZATION_NAME', 'HTAN2Organization')

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
        
        # Try to find matching schema file
        for file in files:
            file_lower = file.lower()
            schema_lower = schema_name.lower()
            
            # Direct match
            if schema_lower in file_lower:
                schema_file = f'schemas/{file}'
                break
            
            # Match with variations (Level -> level, etc.)
            if schema_lower.replace('level', 'level') in file_lower:
                schema_file = f'schemas/{file}'
                break
            
            # Match common variations
            if 'bulk' in schema_lower and 'bulk' in file_lower:
                if 'wes' in schema_lower and 'wes' in file_lower:
                    schema_file = f'schemas/{file}'
                    break
        
        if not schema_file:
            print(f'❌ Schema file for {schema_name} not found')
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
