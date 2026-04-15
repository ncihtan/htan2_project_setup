#!/usr/bin/env python3
"""Create curation tasks for all schema-bound folders from config.

New workflow (issue #9):
  1. Create folders
  2. Run this script — creates tasks and fileviews automatically via the curator extension
  3. Run update_fileview_ids.py — discovers and saves the new fileview IDs to config

Requires: pip install 'synapseclient[curator]'
"""

import argparse
import json
import os
import sys
import yaml
import synapseclient

try:
    from synapseclient.extensions.curator import (
        create_record_based_metadata_task,
        create_file_based_metadata_task,
    )
except ImportError:
    print(
        "Error: synapseclient curator extension not installed.\n"
        "Run: pip install 'synapseclient[curator]'"
    )
    sys.exit(1)

from synapseclient.models import Folder


def get_bound_schema_uri(syn, folder_id):
    try:
        folder = Folder(id=folder_id)
        binding = folder.get_schema(synapse_client=syn)
        return binding.json_schema_version_info.id if binding else None
    except synapseclient.core.exceptions.SynapseHTTPError as e:
        return None if e.response.status_code == 404 else None
    except Exception:
        return None


def get_schema_upsert_keys(syn, schema_uri):
    try:
        schema = syn.restGET(f"/schema/type/registered/{schema_uri.split('/')[-1]}")
        properties = schema.get('properties', {})
        for key in ['HTAN_Participant_ID', 'HTAN_Biospecimen_ID', 'HTAN_Subject_ID']:
            if key in properties:
                return [key]
        required = schema.get('required', [])
        if required:
            return [required[0]]
    except Exception:
        pass
    return []


def get_project_id(syn, folder_id):
    entity = syn.get(folder_id, downloadFile=False)
    current = getattr(entity, 'parentId', None)
    while current:
        entity = syn.get(current, downloadFile=False)
        if entity.concreteType == 'org.sagebionetworks.repo.model.Project':
            return current
        current = getattr(entity, 'parentId', None)
    return None


def find_existing_task(syn, project_id, data_type):
    """Return the existing task dict if one exists for this dataType, else None."""
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
        for t in tasks.get('page', []):
            if t.get('dataType') == data_type:
                return t
    except Exception:
        pass
    return None


def process_projects(syn, projects, schema_name, is_record_based=False, dry_run=False,
                     folder_id_filter=None, project_name_filter=None, subfolder_filter=None,
                     force=False):
    created = skipped = errors = 0

    for project in projects:
        if folder_id_filter and project.get('synapse_id') != folder_id_filter:
            continue
        if project_name_filter and project.get('name') != project_name_filter:
            continue
        if subfolder_filter and subfolder_filter not in project.get('subfolder', ''):
            continue

        folder_id = project.get('synapse_id')
        subfolder = project.get('subfolder', 'N/A')
        project_name = project.get('name', 'Unknown')

        if not folder_id:
            errors += 1
            continue

        try:
            schema_uri = get_bound_schema_uri(syn, folder_id)
            if not schema_uri:
                print(
                    f"  ⚠  {project_name}/{subfolder} ({folder_id}): no schema bound — "
                    f"run scripts/bind_schemas_workflow.py first, then retry"
                )
                skipped += 1
                continue

            project_id = get_project_id(syn, folder_id)
            if not project_id:
                print(f"  ❌ {project_name}/{subfolder}: could not resolve project ID")
                errors += 1
                continue

            data_type = schema_name
            existing = find_existing_task(syn, project_id, data_type)
            if existing and not force:
                task_id = existing.get('taskId', '?')
                print(
                    f"  ⚠  {project_name}/{subfolder}: task '{data_type}' already exists "
                    f"(taskId={task_id}). If fileview IDs are missing this may be an old-style "
                    f"REST task — delete it first with delete_all_curation_tasks_and_fileviews.py, "
                    f"or re-run with --force to skip this check."
                )
                skipped += 1
                continue

            task_type = "record-based" if is_record_based else "file-based"
            if dry_run:
                print(f"  [DRY RUN] {data_type} ({task_type}) for {project_name}/{subfolder}")
                created += 1
                continue

            if is_record_based:
                upsert_keys = get_schema_upsert_keys(syn, schema_uri)
                if not upsert_keys:
                    clinical_schemas = {
                        'Demographics', 'Diagnosis', 'Therapy', 'FollowUp',
                        'MolecularTest', 'Exposure', 'FamilyHistory', 'VitalStatus',
                    }
                    upsert_keys = (
                        ["HTAN_Participant_ID"]
                        if any(x in schema_name for x in clinical_schemas)
                        else ["HTAN_Biospecimen_ID"]
                    )

                create_record_based_metadata_task(
                    synapse_client=syn,
                    project_id=project_id,
                    folder_id=folder_id,
                    record_set_name=f"{schema_name}_Records",
                    record_set_description=f"HTAN {schema_name} metadata records for {project_name}/{subfolder}",
                    curation_task_name=data_type,
                    upsert_keys=upsert_keys,
                    instructions=f"HTAN {schema_name} curation for {project_name}/{subfolder}",
                    schema_uri=schema_uri,
                    bind_schema_to_record_set=True,
                )
            else:
                create_file_based_metadata_task(
                    synapse_client=syn,
                    folder_id=folder_id,
                    curation_task_name=data_type,
                    instructions=f"HTAN {schema_name} curation for {project_name}/{subfolder}",
                    entity_view_name=f"{schema_name} Fileview",
                    schema_uri=schema_uri,
                    attach_wiki=False,
                )

            print(f"  ✅ {data_type} ({task_type}) for {project_name}/{subfolder}")
            created += 1

        except Exception as e:
            print(f"  ❌ {project_name}/{subfolder}: {e}")
            errors += 1

    return created, skipped, errors


def main():
    parser = argparse.ArgumentParser(
        description='Create curation tasks from schema binding config. '
                    'Tasks automatically create fileviews; run update_fileview_ids.py afterwards.'
    )
    parser.add_argument('--config', default='schema_binding_config.yml', help='Config file path')
    parser.add_argument('--dry-run', action='store_true', help='Preview without creating tasks')
    parser.add_argument('--folder-id', help='Only process this specific folder ID')
    parser.add_argument('--project-name', help='Only process projects with this name')
    parser.add_argument('--subfolder-filter', help='Only process subfolders containing this string (e.g. v8_ingest)')
    parser.add_argument('--record-based-only', action='store_true', help='Only create record-based tasks')
    parser.add_argument('--force', action='store_true',
                        help='Skip the task-exists check and attempt creation even if a task already exists')
    parser.add_argument('--list-tasks', metavar='PROJECT_ID', help='List curation tasks for a project and exit')
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    auth_token = os.environ.get("SYNAPSE_PAT")
    username = os.environ.get("SYNAPSE_USERNAME")
    if auth_token:
        syn.login(authToken=auth_token)
    elif username:
        syn.login(username)
    else:
        syn.login()

    if args.list_tasks:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": args.list_tasks}))
        for task in tasks.get('page', []):
            print(f"{task.get('taskId')}: {task.get('dataType')}")
        return

    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    schema_bindings = config.get('schema_bindings', {})
    total_created = total_skipped = total_errors = 0

    if not args.record_based_only:
        for schema_name, schema_config in schema_bindings.get('file_based', {}).items():
            print(f"\n{schema_name}")
            c, s, e = process_projects(
                syn, schema_config.get('projects', []), schema_name,
                is_record_based=False, dry_run=args.dry_run,
                folder_id_filter=args.folder_id,
                project_name_filter=args.project_name,
                subfolder_filter=args.subfolder_filter,
                force=args.force,
            )
            total_created += c
            total_skipped += s
            total_errors += e

    for schema_name, schema_config in schema_bindings.get('record_based', {}).items():
        print(f"\n{schema_name}")
        c, s, e = process_projects(
            syn, schema_config.get('projects', []), schema_name,
            is_record_based=True, dry_run=args.dry_run,
            folder_id_filter=args.folder_id,
            project_name_filter=args.project_name,
            subfolder_filter=args.subfolder_filter,
            force=args.force,
        )
        total_created += c
        total_skipped += s
        total_errors += e

    print(f"\nSummary: {total_created} created, {total_skipped} skipped, {total_errors} errors")
    if total_created > 0 and not args.dry_run:
        print("Next step: run scripts/manage/update_fileview_ids.py to capture the new fileview IDs")


if __name__ == '__main__':
    main()
