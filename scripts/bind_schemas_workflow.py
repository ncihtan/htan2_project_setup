#!/usr/bin/env python3
"""
Script to bind schemas to project subfolders as part of the GitHub Actions workflow.
This script processes the schema_binding_config.yml and binds file-based schemas to projects.
"""

import yaml
import subprocess
import os
import sys


def main():
    """Main function to process schema bindings."""
    
    # Load configuration
    with open('schema_binding_config.yml', 'r') as f:
        config = yaml.safe_load(f)

    file_based_schemas = config['schema_bindings']['file_based']
    organization_name = os.environ.get('ORGANIZATION_NAME', 'HTAN2Organization')

    for schema_name, schema_config in file_based_schemas.items():
        print(f'Processing schema: {schema_name}')
        
        # Find the schema file
        schema_file = None
        for file in os.listdir('schemas'):
            if schema_name.lower() in file.lower() or schema_name.replace('Level', 'Level').lower() in file.lower():
                schema_file = f'schemas/{file}'
                break
        
        if not schema_file:
            print(f'❌ Schema file for {schema_name} not found')
            continue
            
        print(f'✅ Found schema file: {schema_file}')
        
        # Process each project for this schema
        for project in schema_config['projects']:
            project_name = project['name']
            synapse_id = project['synapse_id']
            subfolder = project['subfolder']
            
            print(f'Binding {schema_name} to {project_name} ({synapse_id})')
            
            # Run the binding script
            cmd = [
                'python', 'scripts/synapse_json_schema_bind.py',
                '-p', schema_file,
                '-t', synapse_id,
                '-n', organization_name,
                '--create_fileview'
            ]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                print(f'✅ Successfully bound {schema_name} to {project_name}')
                print(result.stdout)
            except subprocess.CalledProcessError as e:
                print(f'❌ Failed to bind {schema_name} to {project_name}: {e}')
                print(f'Error output: {e.stderr}')
                sys.exit(1)


if __name__ == "__main__":
    main()
