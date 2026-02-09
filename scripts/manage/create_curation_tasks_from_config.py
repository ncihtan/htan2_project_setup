#!/usr/bin/env python3
"""Create curation tasks for all schema-bound folders from config."""

import argparse
import yaml
import json
import synapseclient

try:
    from synapseclient.extensions.curator import (
        create_record_based_metadata_task,
        create_file_based_metadata_task
    )
    CURATOR_AVAILABLE = True
except ImportError:
    CURATOR_AVAILABLE = False


def get_bound_schema_uri(syn, folder_id):
    try:
        binding = syn.restGET(f"/entity/{folder_id}/schema/binding")
        return binding.get('jsonSchemaVersionInfo', {}).get('$id') if binding else None
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        return None if e.response.status_code == 404 else None
    except:
        return None


def get_schema_properties(syn, schema_uri):
    """Get schema properties to determine upsert keys."""
    try:
        schema = syn.restGET(f"/schema/type/registered/{schema_uri.split('/')[-1]}")
        properties = schema.get('properties', {})
        # Common primary keys for HTAN schemas
        for key in ['HTAN_Participant_ID', 'HTAN_Biospecimen_ID', 'HTAN_Subject_ID']:
            if key in properties:
                return [key]
        # Fallback: use first required property if available
        required = schema.get('required', [])
        if required:
            return [required[0]]
        return []
    except:
        return []


def task_exists(syn, project_id, data_type):
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
        return any(t.get('dataType') == data_type for t in tasks.get('page', []))
    except:
        return False


def get_project_id(syn, folder_id):
    current = syn.get(folder_id, downloadFile=False).parentId
    while current:
        entity = syn.get(current, downloadFile=False)
        if entity.concreteType == 'org.sagebionetworks.repo.model.Project':
            return current
        current = entity.parentId
    return None


def process_projects(syn, projects, schema_name, is_record_based=False, dry_run=False, 
                     folder_id_filter=None, project_name_filter=None, subfolder_filter=None):
    created = skipped = errors = 0
    
    for project in projects:
        if folder_id_filter and project.get('synapse_id') != folder_id_filter:
            continue
        if project_name_filter and project.get('name') != project_name_filter:
            continue
        if subfolder_filter and subfolder_filter not in project.get('subfolder', ''):
            continue
        
        folder_id = project.get('synapse_id')
        fileview_id = project.get('fileview_id')
        subfolder = project.get('subfolder', 'N/A')
        project_name = project.get('name', 'Unknown')
        
        if not folder_id or (not is_record_based and not fileview_id):
            errors += 1
            continue
        
        try:
            project_id = get_project_id(syn, folder_id)
            schema_uri = get_bound_schema_uri(syn, folder_id)
            
            if not schema_uri:
                errors += 1
                continue
            
            data_type = f"{schema_name}_{subfolder.replace('/', '_').replace('-', '_')}"
            
            if task_exists(syn, project_id, data_type):
                skipped += 1
                continue
            
            if dry_run:
                print(f"  [DRY RUN] {data_type} for {folder_id}")
                created += 1
            else:
                try:
                    if is_record_based:
                        upsert_keys = get_schema_properties(syn, schema_uri)
                        if not upsert_keys:
                            upsert_keys = ["HTAN_Participant_ID"] if "Clinical" in schema_name else ["HTAN_Biospecimen_ID"]
                        
                        if CURATOR_AVAILABLE:
                            create_record_based_metadata_task(
                                synapse_client=syn,
                                project_id=project_id,
                                folder_id=folder_id,
                                record_set_name=f"{data_type}_Records",
                                record_set_description=f"HTAN {schema_name} metadata records for {project_name}/{subfolder}",
                                curation_task_name=data_type,
                                upsert_keys=upsert_keys,
                                instructions=f"HTAN {schema_name} curation for {project_name}/{subfolder}",
                                schema_uri=schema_uri,
                                bind_schema_to_record_set=True,
                            )
                        else:
                            syn.restPOST("/curation/task", body=json.dumps({
                                "dataType": data_type,
                                "projectId": project_id,
                                "instructions": f"HTAN {schema_name} curation for {project_name}/{subfolder}",
                                "taskProperties": {
                                    "concreteType": "org.sagebionetworks.repo.model.curation.metadata.RecordBasedMetadataTaskProperties",
                                    "recordSetId": folder_id,
                                    "upsertKeys": upsert_keys,
                                },
                            }))
                    else:
                        # Use REST API for file-based tasks (curator extension has signature issues)
                        syn.restPOST("/curation/task", body=json.dumps({
                            "dataType": data_type,
                            "projectId": project_id,
                            "instructions": f"HTAN {schema_name} curation for {project_name}/{subfolder}",
                            "taskProperties": {
                                "concreteType": "org.sagebionetworks.repo.model.curation.metadata.FileBasedMetadataTaskProperties",
                                "uploadFolderId": folder_id,
                                "fileViewId": fileview_id,
                            },
                        }))
                    print(f"  ✅ {data_type}")
                    created += 1
                except Exception as e:
                    print(f"  ❌ {data_type}: {e}")
                    errors += 1
        except Exception as e:
            print(f"  ❌ {project_name}/{subfolder}: {e}")
            errors += 1
    
    return created, skipped, errors


def main():
    parser = argparse.ArgumentParser(description='Create curation tasks from schema binding config')
    parser.add_argument('--config', default='schema_binding_config.yml', help='Config file path')
    parser.add_argument('--dry-run', action='store_true', help='Preview without creating tasks')
    parser.add_argument('--folder-id', help='Only process this specific folder ID')
    parser.add_argument('--project-name', help='Only process projects with this name')
    parser.add_argument('--subfolder-filter', help='Only process subfolders containing this string')
    parser.add_argument('--list-tasks', help='List all curation tasks for a project ID')
    args = parser.parse_args()
    
    if args.list_tasks:
        syn = synapseclient.Synapse()
        syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
        syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
        syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
        syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
        syn.login()
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": args.list_tasks}))
        for task in tasks.get('page', []):
            print(f"{task.get('taskId')}: {task.get('dataType')}")
        return
    
    syn = synapseclient.Synapse()
    syn.repoEndpoint = 'https://repo-prod.prod.sagebase.org/repo/v1'
    syn.authEndpoint = 'https://repo-prod.prod.sagebase.org/auth/v1'
    syn.fileHandleEndpoint = 'https://repo-prod.prod.sagebase.org/file/v1'
    syn.portalEndpoint = 'https://repo-prod.prod.sagebase.org/portal/v1'
    syn.login()
    
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    schema_bindings = config.get('schema_bindings', {})
    total_created = total_skipped = total_errors = 0
    
    for schema_name, schema_config in schema_bindings.get('file_based', {}).items():
        projects = schema_config.get('projects', [])
        created, skipped, errors = process_projects(syn, projects, schema_name, is_record_based=False,
                                                   dry_run=args.dry_run, folder_id_filter=args.folder_id,
                                                   project_name_filter=args.project_name, subfolder_filter=args.subfolder_filter)
        total_created += created
        total_skipped += skipped
        total_errors += errors
    
    for schema_name, schema_config in schema_bindings.get('record_based', {}).items():
        projects = schema_config.get('projects', [])
        created, skipped, errors = process_projects(syn, projects, schema_name, is_record_based=True,
                                                   dry_run=args.dry_run, folder_id_filter=args.folder_id,
                                                   project_name_filter=args.project_name, subfolder_filter=args.subfolder_filter)
        total_created += created
        total_skipped += skipped
        total_errors += errors
    
    print(f"\nSummary: {total_created} created, {total_skipped} skipped, {total_errors} errors")


if __name__ == '__main__':
    main()
