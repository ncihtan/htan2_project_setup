#!/usr/bin/env python3
"""
Discover all schema-bound folders, fileviews, and RecordSets in htan2-testing1 v8_ingest,
then update schema_binding_config.yml:

1. Add htan2-testing1 entries for SpatialOmicsLevel1, SpatialOmicsLevel3, SpatialOmicsLevel4
   (synapse_id + fileview_id) — Spatial Omics is the same as repo's SpatialLevel1/3/4.
2. Add htan2-testing1 entries for all record_based (Clinical + Biospecimen) with
   synapse_id, fileview_id, and record_set_id.
3. Add record_set_id field to all record_based project entries (discover where possible).

Uses: Synapse API for bindings, getChildren to find fileviews/RecordSets, and optional
curation task list to get recordSetId for record-based tasks.
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import synapseclient
import yaml


TESTING_PROJECT_ID = "syn63834783"
TESTING_PROJECT_NAME = "htan2-testing1"

# Subfolder prefix for v8 top-level folders (same as other projects in config)
V8_FOLDER_NAMES = ("v8_ingest", "v8_staging", "v8_release")


def get_child_by_name(syn: synapseclient.Synapse, parent_id: str, name: str) -> Optional[str]:
    for child in syn.getChildren(parent_id):
        if child.get("name") == name:
            return child.get("id")
    return None


def get_descendant_by_path(syn: synapseclient.Synapse, root_id: str, rel_path: str) -> Optional[str]:
    current = root_id
    for part in rel_path.split("/"):
        if not part:
            continue
        child_id = get_child_by_name(syn, current, part)
        if not child_id:
            return None
        current = child_id
    return current


def _get_view_scope_ids(entity: Any) -> List[str]:
    """Return list of scope IDs from an EntityView (scope or scopeIds)."""
    scope = getattr(entity, "scope", None)
    if scope is not None and isinstance(scope, list):
        return [str(s) for s in scope]
    scope_ids = getattr(entity, "scopeIds", None)
    if scope_ids is not None and isinstance(scope_ids, list):
        return [str(s) for s in scope_ids]
    return []


def _scope_contains(scope_ids: List[str], folder_id: str) -> bool:
    """True if folder_id is in scope (scopeIds can be with or without 'syn' prefix)."""
    folder_numeric = folder_id.replace("syn", "") if folder_id.startswith("syn") else folder_id
    for s in scope_ids:
        s_str = str(s)
        if folder_id == s_str or folder_numeric == s_str or folder_id == "syn" + s_str:
            return True
    return False


def find_fileview_under_folder(syn: synapseclient.Synapse, folder_id: str) -> Optional[str]:
    """Return a fileview (EntityView) that is a direct child of folder_id."""
    for child in syn.getChildren(folder_id):
        try:
            entity = syn.get(child["id"], downloadFile=False)
            if getattr(entity, "concreteType", "") and "EntityView" in entity.concreteType:
                return entity.id
        except Exception:
            continue
    return None


def find_fileview_scoped_to_folder(
    syn: synapseclient.Synapse, container_id: str, folder_id: str
) -> Optional[str]:
    """
    Find a fileview whose scope includes folder_id by scanning container's descendants.
    Fileviews are often stored under the project or v8_ingest with scope=[folder_id].
    """
    try:
        for child in syn.getChildren(container_id):
            cid = child.get("id")
            try:
                entity = syn.get(cid, downloadFile=False)
            except Exception:
                continue
            ct = getattr(entity, "concreteType", "") or ""
            if "EntityView" not in ct:
                continue
            scope_ids = _get_view_scope_ids(entity)
            if _scope_contains(scope_ids, folder_id):
                return entity.id
        # One level deeper: e.g. v8_ingest -> SpatialOmics -> Level_1's siblings might have views
        for child in syn.getChildren(container_id):
            for grandchild in syn.getChildren(child.get("id", "")):
                gid = grandchild.get("id")
                try:
                    entity = syn.get(gid, downloadFile=False)
                except Exception:
                    continue
                ct = getattr(entity, "concreteType", "") or ""
                if "EntityView" not in ct:
                    continue
                scope_ids = _get_view_scope_ids(entity)
                if _scope_contains(scope_ids, folder_id):
                    return entity.id
    except Exception:
        pass
    return None


def find_record_set_under_folder(syn: synapseclient.Synapse, folder_id: str) -> Optional[str]:
    """Return a RecordSet entity that is a direct child of folder_id."""
    for child in syn.getChildren(folder_id):
        try:
            entity = syn.get(child["id"], downloadFile=False)
            ct = getattr(entity, "concreteType", "") or ""
            if "RecordSet" in ct:
                return entity.id
        except Exception:
            continue
    return None


def get_record_set_ids_from_curation_tasks(
    syn: synapseclient.Synapse, project_id: str
) -> Dict[str, str]:
    """
    Map dataType (e.g. 'Demographics') to recordSetId by listing project curation tasks.
    Returns dict: data_type -> record_set_id (folder_id -> record_set_id not needed; we have path).
    """
    out: Dict[str, str] = {}
    try:
        tasks = syn.restPOST("/curation/task/list", body=json.dumps({"projectId": project_id}))
        for task in tasks.get("page", []):
            task_id = task.get("taskId")
            data_type = task.get("dataType")
            if not task_id or not data_type:
                continue
            try:
                details = syn.restGET(f"/curation/task/{task_id}")
                props = details.get("taskProperties", {})
                if "RecordBased" in (props.get("concreteType") or ""):
                    rs_id = props.get("recordSetId")
                    if rs_id:
                        out[data_type] = rs_id
            except Exception:
                continue
    except Exception:
        pass
    return out


def get_v8_subfolders_per_schema(config: Dict[str, Any]) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """
    From config, get the exact subfolder strings used by other projects (v8_ingest/..., v8_staging/..., v8_release/...).
    Returns (file_based_schema -> [subfolders], record_based_schema -> [subfolders]).
    """
    sb = config.get("schema_bindings", {})
    fb_subs: Dict[str, List[str]] = {}
    rb_subs: Dict[str, List[str]] = {}
    for schema_name, schema_cfg in sb.get("file_based", {}).items():
        seen = set()
        for p in schema_cfg.get("projects", []):
            sub = (p.get("subfolder") or "").strip()
            if sub and any(sub.startswith(f"{t}/") for t in V8_FOLDER_NAMES) and sub not in seen:
                seen.add(sub)
        if seen:
            fb_subs[schema_name] = sorted(seen)
    for schema_name, schema_cfg in sb.get("record_based", {}).items():
        seen = set()
        for p in schema_cfg.get("projects", []):
            sub = (p.get("subfolder") or "").strip()
            if sub and any(sub.startswith(f"{t}/") for t in V8_FOLDER_NAMES) and sub not in seen:
                seen.add(sub)
        if seen:
            rb_subs[schema_name] = sorted(seen)
    return fb_subs, rb_subs


def discover_testing_by_config_subfolders(
    syn: synapseclient.Synapse,
    project_id: str,
    fb_subfolders: Dict[str, List[str]],
    rb_subfolders: Dict[str, List[str]],
) -> Tuple[Dict[Tuple[str, str], Dict[str, str]], Dict[Tuple[str, str], Dict[str, str]]]:
    """
    For each (schema_name, subfolder) in the config's v8 subfolders, discover folder_id and fileview_id
    (and record_set_id for record-based) under the testing project. Uses same folder layout: v8_ingest, v8_staging, v8_release.
    Returns:
        file_based: (schema_name, subfolder) -> { synapse_id, fileview_id }
        record_based: (schema_name, subfolder) -> { synapse_id, fileview_id?, record_set_id }
    """
    task_record_sets = get_record_set_ids_from_curation_tasks(syn, project_id)

    def data_type_for_rel_path(rel_path: str) -> str:
        if rel_path == "Biospecimen":
            return "Biospecimen"
        return rel_path.split("/")[-1]

    roots: Dict[str, Optional[str]] = {}
    for name in V8_FOLDER_NAMES:
        roots[name] = get_child_by_name(syn, project_id, name)

    file_based: Dict[Tuple[str, str], Dict[str, str]] = {}
    for schema_name, subfolders in fb_subfolders.items():
        for subfolder in subfolders:
            if "/" not in subfolder:
                continue
            folder_type, _, rel_path = subfolder.partition("/")
            root_id = roots.get(folder_type)
            if not root_id:
                continue
            folder_id = get_descendant_by_path(syn, root_id, rel_path)
            if not folder_id:
                continue
            fv_id = find_fileview_under_folder(syn, folder_id)
            if not fv_id:
                fv_id = find_fileview_scoped_to_folder(syn, root_id, folder_id)
            if not fv_id:
                fv_id = find_fileview_scoped_to_folder(syn, project_id, folder_id)
            file_based[(schema_name, subfolder)] = {"synapse_id": folder_id, "fileview_id": fv_id or ""}

    record_based: Dict[Tuple[str, str], Dict[str, str]] = {}
    for schema_name, subfolders in rb_subfolders.items():
        for subfolder in subfolders:
            if "/" not in subfolder:
                continue
            folder_type, _, rel_path = subfolder.partition("/")
            root_id = roots.get(folder_type)
            if not root_id:
                continue
            folder_id = get_descendant_by_path(syn, root_id, rel_path)
            if not folder_id:
                continue
            rs_id = find_record_set_under_folder(syn, folder_id)
            if not rs_id and task_record_sets:
                data_type = data_type_for_rel_path(rel_path)
                rs_id = task_record_sets.get(data_type) or ""
            fv_id = find_fileview_under_folder(syn, folder_id)
            if not fv_id:
                fv_id = find_fileview_scoped_to_folder(syn, root_id, folder_id)
            if not fv_id:
                fv_id = find_fileview_scoped_to_folder(syn, project_id, folder_id)
            record_based[(schema_name, subfolder)] = {
                "synapse_id": folder_id,
                "fileview_id": fv_id or "",
                "record_set_id": rs_id or "",
            }

    return file_based, record_based


def add_testing_entries_to_config(
    config: Dict[str, Any],
    file_based: Dict[Tuple[str, str], Dict[str, str]],
    record_based: Dict[Tuple[str, str], Dict[str, str]],
) -> None:
    """Add or update htan2-testing1 entries with the exact same subfolder strings as other projects."""
    sb = config.setdefault("schema_bindings", {})
    fb = sb.setdefault("file_based", {})
    rb = sb.setdefault("record_based", {})

    def is_v8_subfolder(sub: str) -> bool:
        return any((sub or "").startswith(f"{t}/") for t in V8_FOLDER_NAMES)

    # Remove old testing entries that don't use v8_ingest/v8_staging/v8_release subfolders
    for schema_cfg in list(fb.values()) + list(rb.values()):
        projects = schema_cfg.get("projects", [])
        to_remove = [i for i, p in enumerate(projects) if p.get("name") == TESTING_PROJECT_NAME and not is_v8_subfolder(p.get("subfolder") or "")]
        for i in reversed(to_remove):
            projects.pop(i)

    for (schema_name, subfolder), ids in file_based.items():
        if schema_name not in fb:
            continue
        projects = fb[schema_name].setdefault("projects", [])
        existing = next(
            (p for p in projects if p.get("name") == TESTING_PROJECT_NAME and p.get("subfolder") == subfolder),
            None,
        )
        entry = {
            "name": TESTING_PROJECT_NAME,
            "subfolder": subfolder,
            "synapse_id": ids["synapse_id"],
            "fileview_id": ids.get("fileview_id") or "",
        }
        if existing:
            existing.update(entry)
        else:
            projects.append(entry)

    for (schema_name, subfolder), ids in record_based.items():
        if schema_name not in rb:
            continue
        projects = rb[schema_name].setdefault("projects", [])
        rs_id = (ids.get("record_set_id") or "").strip()
        fv_id = (ids.get("fileview_id") or "").strip()
        entry = {
            "name": TESTING_PROJECT_NAME,
            "subfolder": subfolder,
            "synapse_id": ids["synapse_id"],
        }
        if fv_id:
            entry["fileview_id"] = fv_id
        if rs_id:
            entry["record_set_id"] = rs_id
        existing = next(
            (p for p in projects if p.get("name") == TESTING_PROJECT_NAME and p.get("subfolder") == subfolder),
            None,
        )
        if existing:
            existing.update(entry)
        else:
            projects.append(entry)

    for schema_cfg in rb.values():
        for proj in schema_cfg.get("projects", []):
            if not (proj.get("record_set_id") or "").strip():
                proj.pop("record_set_id", None)
            if not (proj.get("fileview_id") or "").strip():
                proj.pop("fileview_id", None)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Discover testing v8_ingest IDs and update schema_binding_config with Spatial Omics + record_set_id."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("schema_binding_config.yml"),
        help="Path to schema_binding_config.yml",
    )
    parser.add_argument(
        "--project-id",
        default=TESTING_PROJECT_ID,
        help="Testing project Synapse ID",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print only, do not write config.")
    args = parser.parse_args()

    syn = synapseclient.Synapse()
    syn.login()

    config_path = args.config
    if not config_path.is_file():
        raise SystemExit(f"Config not found: {config_path}")

    with config_path.open("r") as f:
        config = yaml.safe_load(f)

    fb_subfolders, rb_subfolders = get_v8_subfolders_per_schema(config)
    print("Using same subfolders as other projects (v8_ingest, v8_staging, v8_release)...")
    print(f"  File-based schemas: {len(fb_subfolders)} with {sum(len(v) for v in fb_subfolders.values())} subfolders")
    print(f"  Record-based schemas: {len(rb_subfolders)} with {sum(len(v) for v in rb_subfolders.values())} subfolders")

    print("Discovering htan2-testing1 folders, fileviews, and RecordSets for each...")
    file_based, record_based = discover_testing_by_config_subfolders(
        syn, args.project_id, fb_subfolders, rb_subfolders
    )

    print("\nFile-based:")
    for (schema_name, subfolder), ids in sorted(file_based.items()):
        print(f"  {schema_name}  {subfolder}  folder={ids.get('synapse_id')}  fileview={ids.get('fileview_id') or 'n/a'}")

    print("\nRecord-based:")
    for (schema_name, subfolder), ids in sorted(record_based.items()):
        print(f"  {schema_name}  {subfolder}  folder={ids.get('synapse_id')}  record_set={ids.get('record_set_id') or 'n/a'}  fileview={ids.get('fileview_id') or 'n/a'}")

    add_testing_entries_to_config(config, file_based, record_based)

    if args.dry_run:
        print("\n[DRY RUN] Config not written.")
        return

    with config_path.open("w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print("\nConfig updated.")


if __name__ == "__main__":
    main()
