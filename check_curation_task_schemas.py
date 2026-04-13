#!/usr/bin/env python3
"""Check which schema version is bound to each curation task."""

import argparse
import json
import synapseclient

def get_bound_schema_info(syn, entity_id):
    """Get bound schema information for an entity (folder or RecordSet)."""
    try:
        binding = syn.restGET(f"/entity/{entity_id}/schema/binding")
        if binding:
            schema_info = binding.get('jsonSchemaVersionInfo', {})
            schema_id = schema_info.get('$id', 'N/A')
            schema_name = schema_info.get('schemaName', 'N/A')
            version = schema_info.get('version', 'N/A')
            
            # Try to extract version from schema ID if version is N/A
            # Format: Organization-SchemaName-Version or Organization-SchemaName-vVersion
            if version == 'N/A' and schema_id != 'N/A':
                parts = schema_id.split('-')
                if len(parts) >= 3:
                    # Last part might be version
                    potential_version = parts[-1]
                    # Check if it matches version pattern (e.g., "1.2.0" or "v1.2.0")
                    if potential_version.startswith('v'):
                        version = potential_version[1:]
                    elif '.' in potential_version:
                        version = potential_version
            
            return {
                'schema_id': schema_id,
                'schema_name': schema_name,
                'version': version,
                'bound': True
            }
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        if e.response.status_code == 404:
            return {'bound': False, 'error': 'No schema binding'}
        else:
            return {'bound': False, 'error': str(e)}
    except Exception as e:
        return {'bound': False, 'error': str(e)}
    return {'bound': False, 'error': 'Unknown'}

def check_tasks(syn, project_id, task_filter=None):
    """Check schema versions for all curation tasks in a project."""
    print(f"Project ID: {project_id}\n")
    print("=" * 100)
    
    # List all tasks for the project
    tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
    
    if task_filter:
        tasks['page'] = [t for t in tasks.get('page', []) if task_filter.lower() in t.get('dataType', '').lower()]
    
    if not tasks.get('page'):
        print("No tasks found.")
        return
    
    print(f"Found {len(tasks['page'])} task(s)\n")
    
    for task in tasks.get('page', []):
        task_id = task.get('taskId')
        data_type = task.get('dataType', 'N/A')
        
        print(f"Task: {data_type} (ID: {task_id})")
        
        try:
            # Get full task details
            task_details = syn.restGET(f"/curation/task/{task_id}")
            task_props = task_details.get('taskProperties', {})
            concrete_type = task_props.get('concreteType', '')
            
            if 'RecordBased' in concrete_type:
                # For record-based tasks, schema is bound to the RecordSet
                record_set_id = task_props.get('recordSetId')
                print(f"  Type: Record-based")
                print(f"  RecordSet ID: {record_set_id}")
                
                if record_set_id:
                    schema_info = get_bound_schema_info(syn, record_set_id)
                    if schema_info.get('bound'):
                        print(f"  ✅ Schema: {schema_info['schema_name']} v{schema_info['version']}")
                        print(f"     Schema ID: {schema_info['schema_id']}")
                    else:
                        print(f"  ❌ No schema bound: {schema_info.get('error', 'Unknown')}")
            
            elif 'FileBased' in concrete_type:
                # For file-based tasks, schema is bound to the folder
                folder_id = task_props.get('uploadFolderId')
                print(f"  Type: File-based")
                print(f"  Folder ID: {folder_id}")
                
                if folder_id:
                    schema_info = get_bound_schema_info(syn, folder_id)
                    if schema_info.get('bound'):
                        print(f"  ✅ Schema: {schema_info['schema_name']} v{schema_info['version']}")
                        print(f"     Schema ID: {schema_info['schema_id']}")
                    else:
                        print(f"  ❌ No schema bound: {schema_info.get('error', 'Unknown')}")
            else:
                print(f"  Type: Unknown ({concrete_type})")
        
        except Exception as e:
            print(f"  ❌ Error getting task details: {e}")
        
        print()

def main():
    parser = argparse.ArgumentParser(description='Check schema versions bound to curation tasks')
    parser.add_argument('project_id', help='Synapse project ID')
    parser.add_argument('--filter', help='Filter tasks by data type (case-insensitive)')
    args = parser.parse_args()
    
    syn = synapseclient.Synapse()
    syn.login()
    
    check_tasks(syn, args.project_id, args.filter)

if __name__ == '__main__':
    main()

