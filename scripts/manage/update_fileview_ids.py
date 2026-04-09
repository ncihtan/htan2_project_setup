#!/usr/bin/env python3
"""
Script to extract fileview IDs from folder children and add them to schema binding config.

Run this after creating curation tasks (e.g. via create_curation_tasks_from_config.py using the
curator extension). The curator's create_file_based_metadata_task creates the EntityView (fileview);
this script discovers those fileviews and updates the config.

Usage:
    python scripts/manage/update_fileview_ids.py
    python scripts/manage/update_fileview_ids.py --subfolder-filter v8_ingest
"""

import json
import synapseclient
import yaml
import argparse
import sys
from pathlib import Path
from typing import Dict, Optional

# Add parent directories to path to import htan2_synapse
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def find_fileview_in_entity(syn, entity_id: str) -> Optional[str]:
    """Find a fileview associated with an entity by checking its children."""
    try:
        children = list(syn.getChildren(entity_id))

        for child in children:
            try:
                entity = syn.get(child['id'], downloadFile=False)
                if hasattr(entity, 'concreteType') and 'EntityView' in entity.concreteType:
                    return entity.id
            except Exception:
                continue

    except Exception as e:
        print(f"  ⚠ Warning: Could not find fileview for {entity_id}: {e}")

    return None


def fileview_exists(syn, fileview_id: str) -> bool:
    """Return True only if the fileview entity exists (not deleted)."""
    try:
        syn.get(fileview_id, downloadFile=False)
        return True
    except Exception:
        return False


def get_folder_to_view_map_from_tasks(syn, project_id: str) -> Dict[str, str]:
    """
    List curation tasks for a project and build a map:
      folder_id -> fileViewId (file-based) or recordSetId (record-based).
    This is the most reliable way to find the view/RecordSet because the
    curation task knows exactly which entity it uses.
    """
    out: Dict[str, str] = {}
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
    except Exception:
        return out
    for task in tasks.get("page", []):
        task_id = task.get("taskId")
        if not task_id:
            continue
        try:
            details = syn.restGET(f"/curation/task/{task_id}")
            props = details.get("taskProperties", {})
            ct = props.get("concreteType", "")
            if "FileBased" in ct:
                folder_id = props.get("uploadFolderId")
                fv_id = props.get("fileViewId") or props.get("fileviewId")
                if folder_id and fv_id:
                    out[folder_id] = fv_id
            elif "RecordBased" in ct:
                rs_id = props.get("recordSetId")
                if rs_id:
                    try:
                        rs_ent = syn.get(rs_id, downloadFile=False)
                        # Old REST tasks set recordSetId = folder_id (a Folder entity).
                        # Skip these — there's no actual RecordSet, so we can't map correctly.
                        if 'Folder' in getattr(rs_ent, 'concreteType', ''):
                            continue
                        parent = getattr(rs_ent, 'parentId', rs_id)
                        out[parent] = rs_id
                    except Exception:
                        pass
        except Exception:
            continue
    return out


def update_fileview_ids_in_config(
    syn,
    config_file: str,
    dry_run: bool = False,
    subfolder_filter: Optional[str] = None,
    project_name_filter: Optional[str] = None,
):
    """
    Update schema binding config with fileview IDs from wikis or folder children.
    Only writes IDs for fileviews that exist (skips deleted IDs from wikis).
    """
    print("=" * 80)
    print("Update Fileview IDs in Schema Binding Config")
    print("=" * 80)
    print(f"Config file: {config_file}")
    if subfolder_filter:
        print(f"Subfolder filter: {subfolder_filter}")
    if dry_run:
        print("Mode: DRY RUN")
    print("=" * 80)
    print()

    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    schema_bindings = config.get('schema_bindings', {})
    file_based = schema_bindings.get('file_based', {})
    record_based = schema_bindings.get('record_based', {})

    total_updated = 0
    total_not_found = 0
    total_errors = 0

    # Build folder_id -> fileview/recordset map from curation tasks.
    # If filtering to a single project name, resolve its Synapse project ID
    # and query tasks once. Otherwise build per-project caches on demand.
    task_maps: Dict[str, Dict[str, str]] = {}  # project_synapse_id -> folder->view map

    def get_task_map_for_folder(folder_id: str) -> Dict[str, str]:
        """Get or build the task-based folder->view map for the project containing folder_id."""
        try:
            entity = syn.get(folder_id, downloadFile=False)
            pid = getattr(entity, 'projectId', None)
            if not pid:
                current = getattr(entity, 'parentId', None)
                while current:
                    ent = syn.get(current, downloadFile=False)
                    if getattr(ent, 'concreteType', '') == 'org.sagebionetworks.repo.model.Project':
                        pid = current
                        break
                    current = getattr(ent, 'parentId', None)
            if pid and pid not in task_maps:
                print(f"  (caching task map for project {pid})")
                task_maps[pid] = get_folder_to_view_map_from_tasks(syn, pid)
            return task_maps.get(pid, {})
        except Exception:
            return {}

    def process_project(project: dict) -> None:
        nonlocal total_updated, total_not_found
        entity_id = project.get('synapse_id')
        project_name = project.get('name', 'Unknown')
        subfolder = project.get('subfolder', '')
        if project_name_filter and project_name != project_name_filter:
            return
        if subfolder_filter and subfolder_filter not in subfolder:
            return
        if not entity_id:
            return

        # 1. Try curation task map (most reliable: task knows its own fileview/recordset)
        tmap = get_task_map_for_folder(entity_id)
        view_id = tmap.get(entity_id)
        if view_id:
            if not dry_run:
                project['fileview_id'] = view_id
            print(f"  ✅ {project_name} ({entity_id}): {view_id} (from task)")
            total_updated += 1
            return

        # 2. Try direct children (EntityView or RecordSet living under the folder)
        found_id = find_fileview_in_entity(syn, entity_id)
        if found_id and fileview_exists(syn, found_id):
            if not dry_run:
                project['fileview_id'] = found_id
            print(f"  ✅ {project_name} ({entity_id}): {found_id} (from children)")
            total_updated += 1
            return

        # Nothing found
        if found_id:
            print(f"  ⚠️  {project_name} ({entity_id}): found {found_id} but entity no longer exists (deleted?)")
        else:
            print(f"  ⚠️  {project_name} ({entity_id}): no fileview_id found — run create_curation_tasks_from_config.py first")
        total_not_found += 1

    print("\n" + "=" * 80)
    print("Processing file-based schemas...")
    print("=" * 80)

    for schema_name, schema_config in file_based.items():
        projects = schema_config.get('projects', [])
        to_process = [p for p in projects if not subfolder_filter or subfolder_filter in p.get('subfolder', '')]
        if not to_process:
            continue
        print(f"\n{schema_name}: {len(to_process)} project(s)")
        for project in to_process:
            process_project(project)

    print("\n" + "=" * 80)
    print("Processing record-based schemas...")
    print("=" * 80)

    for schema_name, schema_config in record_based.items():
        projects = schema_config.get('projects', [])
        to_process = [p for p in projects if not subfolder_filter or subfolder_filter in p.get('subfolder', '')]
        if not to_process:
            continue
        print(f"\n{schema_name}: {len(to_process)} project(s)")
        for project in to_process:
            process_project(project)

    if not dry_run and total_updated > 0:
        print("\n" + "=" * 80)
        print("Saving updated config...")
        print("=" * 80)
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False, sort_keys=False)
        print(f"✅ Updated {config_file} with {total_updated} fileview ID(s)")

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✅ Fileview IDs found and updated: {total_updated}")
    print(f"⚠️  No fileview found: {total_not_found}")
    print(f"❌ Errors: {total_errors}")
    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="Extract fileview IDs from wikis/children and update schema binding config"
    )
    parser.add_argument(
        "--config-file",
        type=str,
        default="schema_binding_config.yml",
        help="Path to schema binding config YAML file",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be updated without making changes")
    parser.add_argument(
        "--subfolder-filter",
        type=str,
        default=None,
        help="Only process projects whose subfolder contains this string (e.g. v8_ingest)",
    )
    parser.add_argument(
        "--project-name",
        type=str,
        default=None,
        help="Only update entries with this project name (e.g. htan2-testing1)",
    )
    args = parser.parse_args()

    if not Path(args.config_file).exists():
        print(f"Error: Config file not found: {args.config_file}")
        sys.exit(1)

    syn = synapseclient.Synapse()
    syn.login()
    print("✅ Successfully logged in to Synapse\n")
    update_fileview_ids_in_config(
        syn,
        args.config_file,
        args.dry_run,
        subfolder_filter=args.subfolder_filter,
        project_name_filter=args.project_name,
    )


if __name__ == "__main__":
    main()
