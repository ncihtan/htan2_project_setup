#!/usr/bin/env python3
"""Create curation tasks for v8_ingest folders in htan2-testing1 based on bound schemas."""

import argparse
import json
import synapseclient
from typing import Dict, Optional, Tuple

try:
    from synapseclient.extensions.curator import create_record_based_metadata_task
    CURATOR_AVAILABLE = True
except ImportError:
    CURATOR_AVAILABLE = False


def get_bound_schema_uri(syn, folder_id):
    """Get the schema URI bound to a folder."""
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


def get_project_id(syn, folder_id):
    """Get the top-level project ID for a folder."""
    current = syn.get(folder_id, downloadFile=False).parentId
    while current:
        entity = syn.get(current, downloadFile=False)
        if entity.concreteType == 'org.sagebionetworks.repo.model.Project':
            return current
        current = entity.parentId
    return None


def task_exists(syn, project_id, data_type):
    """Check if a curation task with the given data type already exists."""
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
        return any(t.get('dataType') == data_type for t in tasks.get('page', []))
    except:
        return False


def is_record_based(schema_uri: str, folder_path: str) -> bool:
    """Determine if a schema is record-based based on schema URI and folder path."""
    # Record-based schemas: Clinical subfolders and Biospecimen
    record_based_patterns = [
        'Demographics', 'Diagnosis', 'Therapy', 'FollowUp', 'MolecularTest',
        'Exposure', 'FamilyHistory', 'VitalStatus', 'Biospecimen'
    ]
    
    # Check schema URI
    for pattern in record_based_patterns:
        if pattern in schema_uri:
            return True
    
    # Check folder path
    if 'Clinical/' in folder_path or '/Biospecimen' in folder_path:
        return True
    
    return False


def extract_schema_name(schema_uri: str) -> str:
    """Extract a simple schema name from URI (e.g., HTAN2Organization-BulkWESLevel1-1.2.0 -> BulkWESLevel1)."""
    # Format: HTAN2Organization-SchemaName-Version
    parts = schema_uri.split('-')
    if len(parts) >= 2:
        # Remove organization prefix
        return parts[1]
    return schema_uri.split('/')[-1]


def get_child_by_name(syn, parent_id, name):
    """Find a direct child folder by name."""
    for child in syn.getChildren(parent_id):
        if child.get('name') == name:
            return child.get('id')
    return None


def traverse_and_create_tasks(syn, folder_id: str, project_id: str, folder_path: str = "", 
                              dry_run: bool = False) -> Tuple[int, int, int]:
    """Recursively traverse folders and create curation tasks for schema-bound folders."""
    created = skipped = errors = 0
    
    # Check if this folder has a bound schema
    schema_uri = get_bound_schema_uri(syn, folder_id)
    
    if schema_uri:
        # Extract schema name for task naming
        schema_name = extract_schema_name(schema_uri)
        data_type = schema_name  # Simple naming
        
        # Check if task already exists
        if task_exists(syn, project_id, data_type):
            print(f"  ⏭️  Skipping {folder_path}: task '{data_type}' already exists")
            skipped += 1
        else:
            is_record = is_record_based(schema_uri, folder_path)
            
            print(f"\n  📋 Creating task for {folder_path}")
            print(f"     Schema: {schema_uri}")
            print(f"     Type: {'Record-based' if is_record else 'File-based'}")
            
            if dry_run:
                print(f"     [DRY RUN] Would create: {data_type}")
                created += 1
            else:
                try:
                    if is_record:
                        # Record-based task
                        upsert_keys = get_schema_properties(syn, schema_uri)
                        if not upsert_keys:
                            # Default based on schema name
                            if "Clinical" in schema_uri or any(x in schema_uri for x in ['Demographics', 'Diagnosis', 'Therapy']):
                                upsert_keys = ["HTAN_Participant_ID"]
                            else:
                                upsert_keys = ["HTAN_Biospecimen_ID"]
                        
                        if CURATOR_AVAILABLE:
                            create_record_based_metadata_task(
                                synapse_client=syn,
                                project_id=project_id,
                                folder_id=folder_id,
                                record_set_name=f"{schema_name}_Records",
                                record_set_description=f"HTAN {schema_name} metadata records for htan2-testing1/v8_ingest/{folder_path}",
                                curation_task_name=data_type,
                                upsert_keys=upsert_keys,
                                instructions=f"HTAN {schema_name} curation for htan2-testing1/v8_ingest/{folder_path}",
                                schema_uri=schema_uri,
                                bind_schema_to_record_set=True,
                            )
                        else:
                            syn.restPOST("/curation/task", body=json.dumps({
                                "dataType": data_type,
                                "projectId": project_id,
                                "instructions": f"HTAN {schema_name} curation for htan2-testing1/v8_ingest/{folder_path}",
                                "taskProperties": {
                                    "concreteType": "org.sagebionetworks.repo.model.curation.metadata.RecordBasedMetadataTaskProperties",
                                    "recordSetId": folder_id,
                                    "upsertKeys": upsert_keys,
                                },
                            }))
                    else:
                        # File-based task - need to create a fileview first
                        try:
                            # Get schema JSON to create fileview
                            schema_id_part = schema_uri.split('/')[-1]
                            schema_json = syn.restGET(f"/schema/type/registered/{schema_id_part}")
                            
                            # Create fileview with columns from schema
                            from synapseclient.table import EntityViewSchema, EntityViewType, Column
                            
                            properties = schema_json.get('properties', {})
                            if not properties and '$defs' in schema_json:
                                # Try to get properties from $defs
                                for def_name, def_content in schema_json.get('$defs', {}).items():
                                    if 'properties' in def_content:
                                        properties = def_content['properties']
                                        break
                            
                            if not properties:
                                print(f"     ❌ No properties found in schema, skipping fileview creation")
                                errors += 1
                                return created, skipped, errors
                            
                            # Create columns from schema properties
                            columns = []
                            for prop_name, prop_def in properties.items():
                                column_type = "STRING"
                                if "type" in prop_def:
                                    if isinstance(prop_def["type"], list):
                                        types = [t for t in prop_def["type"] if t != "null"]
                                        if types:
                                            column_type = "STRING" if types[0] == "string" else "INTEGER" if types[0] == "integer" else "DOUBLE" if types[0] == "number" else "BOOLEAN" if types[0] == "boolean" else "STRING"
                                    else:
                                        column_type = "STRING" if prop_def["type"] == "string" else "INTEGER" if prop_def["type"] == "integer" else "DOUBLE" if prop_def["type"] == "number" else "BOOLEAN" if prop_def["type"] == "boolean" else "STRING"
                                
                                column = Column(name=prop_name, columnType=column_type, maximumSize=100 if column_type == "STRING" else None)
                                columns.append(column)
                            
                            # Create fileview
                            fileview_schema = EntityViewSchema(
                                name=f"{schema_name} Fileview",
                                description=f"Fileview for {schema_name} schema",
                                parent=project_id,
                                scopes=[folder_id],
                                viewType=EntityViewType.FILE,
                                columns=columns
                            )
                            fileview = syn.store(fileview_schema)
                            fileview_id = fileview.id
                            
                            print(f"     ✅ Created fileview: {fileview_id}")
                            
                            # Now create the file-based task
                            syn.restPOST("/curation/task", body=json.dumps({
                                "dataType": data_type,
                                "projectId": project_id,
                                "instructions": f"HTAN {schema_name} curation for htan2-testing1/v8_ingest/{folder_path}",
                                "taskProperties": {
                                    "concreteType": "org.sagebionetworks.repo.model.curation.metadata.FileBasedMetadataTaskProperties",
                                    "uploadFolderId": folder_id,
                                    "fileViewId": fileview_id,
                                },
                            }))
                            print(f"     ✅ Created: {data_type}")
                            created += 1
                        except Exception as e:
                            print(f"     ❌ Error creating file-based task: {e}")
                            errors += 1
                    
                    if is_record:
                        print(f"     ✅ Created: {data_type}")
                        created += 1
                except Exception as e:
                    print(f"     ❌ Error: {e}")
                    errors += 1
    
    # Recursively process children
    try:
        for child in syn.getChildren(folder_id):
            if child.get('type') == 'org.sagebionetworks.repo.model.Folder':
                child_name = child.get('name')
                child_id = child.get('id')
                child_path = f"{folder_path}/{child_name}" if folder_path else child_name
                c, s, e = traverse_and_create_tasks(syn, child_id, project_id, child_path, dry_run)
                created += c
                skipped += s
                errors += e
    except Exception as e:
        print(f"  ⚠️  Error processing children of {folder_path}: {e}")
    
    return created, skipped, errors


def main():
    parser = argparse.ArgumentParser(
        description="Create curation tasks for v8_ingest folders in htan2-testing1"
    )
    parser.add_argument(
        "--project-id",
        default="syn63834783",
        help="Testing project Synapse ID (default: syn63834783 for htan2-testing1)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode - show what would be created without actually creating"
    )
    args = parser.parse_args()
    
    syn = synapseclient.Synapse()
    syn.login()
    
    project_id = args.project_id
    
    # Find v8_ingest folder
    v8_ingest_id = get_child_by_name(syn, project_id, "v8_ingest")
    if not v8_ingest_id:
        print(f"❌ Could not find v8_ingest folder in project {project_id}")
        return
    
    print("=" * 80)
    print(f"Creating curation tasks for v8_ingest in project {project_id}")
    print(f"v8_ingest folder: {v8_ingest_id}")
    if args.dry_run:
        print("Mode: DRY RUN")
    print("=" * 80)
    
    # Traverse and create tasks
    created, skipped, errors = traverse_and_create_tasks(
        syn, v8_ingest_id, project_id, "v8_ingest", args.dry_run
    )
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Created:  {created}")
    print(f"Skipped:  {skipped}")
    print(f"Errors:   {errors}")
    print("=" * 80)


if __name__ == '__main__':
    main()

