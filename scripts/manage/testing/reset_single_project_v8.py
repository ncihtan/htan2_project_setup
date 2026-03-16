#!/usr/bin/env python3
"""
Reset v8 folders and curation tasks for a single HTAN2 project.

Usage:
    python scripts/manage/reset_single_project_v8.py \\
        --project-name htan2-testing1 \\
        --project-id syn63834783 \\
        --version 8
"""

import argparse
import json
from typing import List

import synapseclient

# Ensure we can import the local htan2_synapse package when run as a script
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from htan2_synapse import (  # type: ignore
    set_folder_permissions,
    create_folder,
    RECORD_BASED_MODULES,
    FILE_BASED_MODULES,
)


def delete_all_tasks_for_project(syn: synapseclient.Synapse, project_id: str) -> int:
    """Delete all curation tasks for a single project."""
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
    except Exception as e:
        print(f"❌ Error listing tasks for {project_id}: {e}")
        return 0

    deleted = 0
    for task in tasks.get("page", []):
        task_id = task.get("taskId")
        data_type = task.get("dataType", "N/A")
        try:
            syn.restDELETE(f"/curation/task/{task_id}")
            print(f"  ✅ Deleted task: {data_type} (ID: {task_id})")
            deleted += 1
        except Exception as e:
            print(f"  ❌ Error deleting task {task_id}: {e}")
    return deleted


def delete_v8_folders(syn: synapseclient.Synapse, project_id: str, version: str) -> List[str]:
    """Delete any existing v{version}_ingest/staging/release folders in the project."""
    prefix = f"v{version}" if not str(version).startswith("v") else version
    target_names = {f"{prefix}_ingest", f"{prefix}_staging", f"{prefix}_release"}

    deleted_ids: List[str] = []

    try:
        children = syn.restGET(f"/entity/{project_id}/children?includeTypes=folder")
    except Exception as e:
        print(f"❌ Error listing children for {project_id}: {e}")
        return deleted_ids

    for child in children.get("page", []):
        name = child.get("name")
        eid = child.get("id")
        if name in target_names:
            try:
                print(f"  🗑  Deleting folder tree: {name} ({eid})")
                syn.delete(eid)
                deleted_ids.append(eid)
            except Exception as e:
                print(f"  ❌ Error deleting folder {eid}: {e}")

    return deleted_ids


def recreate_v8_folders(
    syn: synapseclient.Synapse,
    project_id: str,
    project_name: str,
    version: str,
) -> None:
    """Recreate v{version}_ingest/staging/release folder trees using standard layout."""
    prefix = f"v{version}" if not str(version).startswith("v") else version
    folder_types = [f"{prefix}_ingest", f"{prefix}_staging", f"{prefix}_release"]

    for folder_type in folder_types:
        print(f"\nCreating {folder_type}/ for {project_name} ({project_id})")
        root_id = create_folder(syn, project_id, folder_type)
        if not root_id:
            print(f"  ✗ Skipping {folder_type}/ due to creation failure")
            continue

        # Set root permissions using shared helper
        set_folder_permissions(syn, root_id, folder_type, prefix, project_name)
        print(f"  ✓ Set permissions for {folder_type}/")

        # Record-based modules (Clinical, Biospecimen, etc.)
        for module_name, subfolders in RECORD_BASED_MODULES.items():
            print(f"  Creating {module_name}/ module...")
            module_id = create_folder(syn, root_id, module_name)
            if not module_id:
                continue

            # Clinical-style subfolders
            if subfolders:
                for subfolder in subfolders:
                    sub_id = create_folder(syn, module_id, subfolder)
                    if not sub_id:
                        continue

            # Biospecimen has no subfolders; folder itself is the binding target

        # File-based modules (assays)
        for module_name, subfolders in FILE_BASED_MODULES.items():
            print(f"  Creating {module_name}/ module...")
            module_id = create_folder(syn, root_id, module_name)
            if not module_id:
                continue

            # Leaf assay level folders (e.g., Level_1, Level_2, etc.)
            if subfolders:
                for subfolder in subfolders:
                    sub_id = create_folder(syn, module_id, subfolder)
                    if not sub_id:
                        continue


def main():
    parser = argparse.ArgumentParser(
        description="Reset v8 folders and curation tasks for a single project only."
    )
    parser.add_argument("--project-name", required=True, help="Project name (e.g., htan2-testing1)")
    parser.add_argument("--project-id", required=True, help="Synapse project ID (e.g., syn63834783)")
    parser.add_argument(
        "--version",
        default="8",
        help="Version number, default 8 (creates v8_ingest/staging/release)",
    )
    args = parser.parse_args()

    version = str(args.version)

    syn = synapseclient.Synapse()
    syn.login()

    print("=" * 80)
    print(f"Resetting v{version} for single project")
    print("=" * 80)
    print(f"Project: {args.project_name} ({args.project_id})")
    print(f"Version: {version}")
    print("=" * 80)

    # 1) Delete all curation tasks for this project
    print("\n🧹 Step 1: Deleting curation tasks...")
    deleted_tasks = delete_all_tasks_for_project(syn, args.project_id)
    print(f"Deleted {deleted_tasks} task(s)")

    # 2) Delete any existing v{version}_ingest/staging/release roots
    print("\n🧹 Step 2: Deleting existing v{version}_* folders...")
    deleted_roots = delete_v8_folders(syn, args.project_id, version)
    print(f"Deleted {len(deleted_roots)} v{version}_* root folder(s)")

    # 3) Recreate fresh v{version}_ingest/staging/release trees
    print("\n📁 Step 3: Recreating folder structure...")
    recreate_v8_folders(syn, args.project_id, args.project_name, version)

    print("\n✅ Done resetting v{version} structure for this project only.")


if __name__ == "__main__":
    main()


