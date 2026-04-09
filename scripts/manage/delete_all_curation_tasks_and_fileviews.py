#!/usr/bin/env python3
"""
Delete all curation tasks in a project and their associated fileviews.
Use before recreating tasks/fileviews (e.g. with curator extension).

- Lists all curation tasks for the project.
- For each file-based task, records its fileViewId.
- Deletes each curation task.
- Deletes each associated fileview entity (unique IDs only).

Record-based tasks are deleted but their RecordSets are NOT deleted (they hold data).
"""

import argparse
import json
import synapseclient
from typing import List, Optional, Set, Tuple


def get_projects_from_config(
    config_path: str, project_name_filter: Optional[str] = None
) -> List[Tuple[str, str]]:
    """
    Get (project_name, folder_id) from schema_binding_config for all projects.
    Optionally filter by project name (e.g. HTAN2_Ovarian). Folder IDs are used to resolve to project IDs later.
    """
    import yaml
    from pathlib import Path

    with Path(config_path).open("r") as f:
        config = yaml.safe_load(f)
    out: List[Tuple[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for section in ("file_based", "record_based"):
        for schema_config in config.get("schema_bindings", {}).get(section, {}).values():
            for proj in schema_config.get("projects", []):
                name = (proj.get("name") or "").strip()
                sid = proj.get("synapse_id")
                if not name or not sid or not str(sid).startswith("syn"):
                    continue
                if project_name_filter and name != project_name_filter:
                    continue
                key = (name, str(sid))
                if key not in seen:
                    seen.add(key)
                    out.append((name, str(sid)))
    return out


def get_project_id(syn: synapseclient.Synapse, folder_id: str) -> Optional[str]:
    """Resolve folder to its top-level project ID."""
    try:
        entity = syn.get(folder_id, downloadFile=False)
        current = getattr(entity, "parentId", None)
        while current:
            entity = syn.get(current, downloadFile=False)
            if getattr(entity, "concreteType", "") == "org.sagebionetworks.repo.model.Project":
                return current
            current = getattr(entity, "parentId", None)
    except Exception:
        pass
    return None


def delete_tasks_and_fileviews(syn: synapseclient.Synapse, project_id: str, dry_run: bool = False) -> Tuple[int, int]:
    """Delete all curation tasks in project and fileviews linked to file-based tasks. Returns (tasks_deleted, fileviews_deleted)."""
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
    except Exception as e:
        print(f"  ❌ List tasks failed: {e}")
        return 0, 0

    page = tasks.get("page", [])
    fileview_ids = set()

    for task in page:
        task_id = task.get("taskId")
        data_type = task.get("dataType", "N/A")
        try:
            details = syn.restGET(f"/curation/task/{task_id}")
            props = details.get("taskProperties", {})
            ct = props.get("concreteType", "")
            if "FileBased" in ct:
                fv_id = props.get("fileViewId") or props.get("fileviewId")
                if fv_id:
                    fileview_ids.add(fv_id)
        except Exception as e:
            print(f"  ⚠ Could not get task {task_id} details: {e}")

        if dry_run:
            print(f"  [DRY RUN] Would delete task: {data_type} ({task_id})")
            continue
        try:
            syn.restDELETE(f"/curation/task/{task_id}")
            print(f"  ✅ Deleted task: {data_type} ({task_id})")
        except Exception as e:
            print(f"  ❌ Delete task {task_id}: {e}")

    tasks_deleted = 0 if dry_run else len(page)
    fileviews_deleted = 0

    for fv_id in fileview_ids:
        if dry_run:
            print(f"  [DRY RUN] Would delete fileview: {fv_id}")
            fileviews_deleted += 1
            continue
        try:
            syn.delete(fv_id)
            print(f"  ✅ Deleted fileview: {fv_id}")
            fileviews_deleted += 1
        except Exception as e:
            print(f"  ❌ Delete fileview {fv_id}: {e}")

    return tasks_deleted, fileviews_deleted


def main():
    parser = argparse.ArgumentParser(
        description="Delete all curation tasks and their fileviews in a project (or all projects from config)."
    )
    parser.add_argument("--project-id", help="Single project ID (e.g. syn63834783)")
    parser.add_argument(
        "--all-from-config",
        action="store_true",
        help="Resolve all folder IDs in schema_binding_config to project IDs and run for each unique project",
    )
    parser.add_argument("--config", default="schema_binding_config.yml", help="Config path for --all-from-config")
    parser.add_argument(
        "--project-name",
        help="Only run for this project name from config (e.g. HTAN2_Ovarian, htan2-testing1)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only print what would be deleted")
    args = parser.parse_args()

    if not args.project_id and not args.all_from_config:
        parser.error("Provide --project-id or --all-from-config")

    syn = synapseclient.Synapse()
    syn.login()

    if args.all_from_config:
        entries = get_projects_from_config(args.config, project_name_filter=args.project_name)
        # Resolve each unique folder_id to project_id once (avoids hundreds of duplicate API calls)
        unique_folder_ids = set(fid for _, fid in entries)
        folder_to_project: dict[str, str | None] = {}
        for fid in unique_folder_ids:
            folder_to_project[fid] = get_project_id(syn, fid)
        project_id_to_names: dict[str, set[str]] = {}
        for name, folder_id in entries:
            pid = folder_to_project.get(folder_id)
            if pid:
                project_id_to_names.setdefault(pid, set()).add(name)
        if not project_id_to_names:
            print("No projects found in config (or none resolved).")
            return
        print(f"Using config: {args.config}")
        if args.project_name:
            print(f"Filtered to project name: {args.project_name}")
        print(f"Resolved {len(project_id_to_names)} unique project(s) from config.\n")
        for project_id in sorted(project_id_to_names):
            names = ", ".join(sorted(project_id_to_names[project_id]))
            print(f"--- Project {project_id} ({names}) ---")
            delete_tasks_and_fileviews(syn, project_id, dry_run=args.dry_run)
    else:
        print(f"Project {args.project_id}")
        delete_tasks_and_fileviews(syn, args.project_id, dry_run=args.dry_run)

    if args.dry_run:
        print("\n[DRY RUN] No changes made.")


if __name__ == "__main__":
    main()
