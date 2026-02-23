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

# v8_ingest relative paths for file-based (Spatial Omics) and record-based
SPATIAL_OMICS_PATHS = ["SpatialOmics/Level_1", "SpatialOmics/Level_3", "SpatialOmics/Level_4"]
RECORD_BASED_V8_INGEST_PATHS = [
    "Clinical/Demographics",
    "Clinical/Diagnosis",
    "Clinical/Therapy",
    "Clinical/FollowUp",
    "Clinical/MolecularTest",
    "Clinical/Exposure",
    "Clinical/FamilyHistory",
    "Clinical/VitalStatus",
    "Biospecimen",
]


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


def discover_testing_v8_ingest(
    syn: synapseclient.Synapse, project_id: str
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    """
    Discover folder_id, fileview_id (and record_set_id for record-based) for each path
    in testing project v8_ingest.
    Returns:
        file_based: rel_path -> { synapse_id, fileview_id }
        record_based: rel_path -> { synapse_id, fileview_id?, record_set_id }
    """
    v8_ingest_id = get_child_by_name(syn, project_id, "v8_ingest")
    if not v8_ingest_id:
        return {}, {}

    task_record_sets = get_record_set_ids_from_curation_tasks(syn, project_id)
    # Map config schema name to task dataType (task names are often same as schema name)
    def data_type_for_path(rel_path: str) -> str:
        if rel_path == "Biospecimen":
            return "Biospecimen"
        return rel_path.split("/")[-1]  # Clinical/Demographics -> Demographics

    file_based: Dict[str, Dict[str, str]] = {}
    for rel_path in SPATIAL_OMICS_PATHS:
        folder_id = get_descendant_by_path(syn, v8_ingest_id, rel_path)
        if not folder_id:
            continue
        fv_id = find_fileview_under_folder(syn, folder_id)
        if not fv_id:
            fv_id = find_fileview_scoped_to_folder(syn, v8_ingest_id, folder_id)
        if not fv_id:
            fv_id = find_fileview_scoped_to_folder(syn, project_id, folder_id)
        file_based[rel_path] = {"synapse_id": folder_id, "fileview_id": fv_id or ""}

    record_based: Dict[str, Dict[str, str]] = {}
    for rel_path in RECORD_BASED_V8_INGEST_PATHS:
        folder_id = get_descendant_by_path(syn, v8_ingest_id, rel_path)
        if not folder_id:
            continue
        rs_id = find_record_set_under_folder(syn, folder_id)
        if not rs_id and task_record_sets:
            data_type = data_type_for_path(rel_path)
            rs_id = task_record_sets.get(data_type)
        fv_id = find_fileview_under_folder(syn, folder_id)
        if not fv_id:
            fv_id = find_fileview_scoped_to_folder(syn, v8_ingest_id, folder_id)
        if not fv_id:
            fv_id = find_fileview_scoped_to_folder(syn, project_id, folder_id)
        record_based[rel_path] = {
            "synapse_id": folder_id,
            "fileview_id": fv_id or "",
            "record_set_id": rs_id or "",
        }

    return file_based, record_based


def config_schema_name_for_spatial_path(rel_path: str) -> str:
    """SpatialOmics/Level_1 -> SpatialOmicsLevel1."""
    parts = rel_path.split("/")
    if len(parts) == 2 and parts[0] == "SpatialOmics":
        return f"SpatialOmicsLevel{parts[1].replace('Level_', '')}"
    return rel_path.replace("/", "_")


def config_schema_name_for_record_path(rel_path: str) -> str:
    """Clinical/Demographics -> Demographics; Biospecimen -> Biospecimen."""
    if rel_path == "Biospecimen":
        return "Biospecimen"
    return rel_path.split("/")[-1]


def add_testing_entries_to_config(config: Dict[str, Any], file_based: Dict, record_based: Dict) -> None:
    """Mutate config: add htan2-testing1 entries and record_set_id where applicable."""
    sb = config.setdefault("schema_bindings", {})
    fb = sb.setdefault("file_based", {})
    rb = sb.setdefault("record_based", {})

    for rel_path, ids in file_based.items():
        schema_name = config_schema_name_for_spatial_path(rel_path)
        if schema_name not in fb:
            continue
        projects = fb[schema_name].setdefault("projects", [])
        existing = next(
            (p for p in projects if p.get("name") == TESTING_PROJECT_NAME and p.get("subfolder") == rel_path),
            None,
        )
        if existing:
            existing["synapse_id"] = ids["synapse_id"]
            existing["fileview_id"] = ids.get("fileview_id") or ""
        else:
            entry = {
                "name": TESTING_PROJECT_NAME,
                "subfolder": rel_path,
                "synapse_id": ids["synapse_id"],
                "fileview_id": ids.get("fileview_id") or "",
            }
            projects.append(entry)

    for rel_path, ids in record_based.items():
        schema_name = config_schema_name_for_record_path(rel_path)
        if schema_name not in rb:
            continue
        projects = rb[schema_name].setdefault("projects", [])
        rs_id = (ids.get("record_set_id") or "").strip()
        fv_id = (ids.get("fileview_id") or "").strip()
        entry = {
            "name": TESTING_PROJECT_NAME,
            "subfolder": rel_path,
            "synapse_id": ids["synapse_id"],
        }
        if fv_id:
            entry["fileview_id"] = fv_id
        if rs_id:
            entry["record_set_id"] = rs_id
        existing = next(
            (p for p in projects if p.get("name") == TESTING_PROJECT_NAME and p.get("subfolder") == rel_path),
            None,
        )
        if existing:
            existing["synapse_id"] = ids["synapse_id"]
            if fv_id:
                existing["fileview_id"] = fv_id
            else:
                existing.pop("fileview_id", None)
            if rs_id:
                existing["record_set_id"] = rs_id
            else:
                existing.pop("record_set_id", None)
        else:
            projects.append(entry)

    # Remove blank record_set_id and fileview_id from record_based so we don't clutter the config
    for schema_name, schema_cfg in rb.items():
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

    print("Discovering htan2-testing1 v8_ingest folders, fileviews, and RecordSets...")
    file_based, record_based = discover_testing_v8_ingest(syn, args.project_id)

    print("\nFile-based (Spatial Omics):")
    for rel_path, ids in sorted(file_based.items()):
        print(f"  {rel_path}  folder={ids.get('synapse_id')}  fileview={ids.get('fileview_id') or 'n/a'}")

    print("\nRecord-based:")
    for rel_path, ids in sorted(record_based.items()):
        print(f"  {rel_path}  folder={ids.get('synapse_id')}  record_set={ids.get('record_set_id') or 'n/a'}  fileview={ids.get('fileview_id') or 'n/a'}")

    add_testing_entries_to_config(config, file_based, record_based)

    if args.dry_run:
        print("\n[DRY RUN] Config not written.")
        return

    with config_path.open("w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

    print("\nConfig updated.")


if __name__ == "__main__":
    main()
