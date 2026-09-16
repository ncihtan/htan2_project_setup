"""
Configuration constants for HTAN2 Synapse setup.

The module/assay layout is NOT hardcoded here anymore. It is loaded from the committed
artifact ``module_structure.yml``, which is generated from the data-model schemas +
``module_registry.yml`` by ``scripts/manage/generate_module_structure.py``. This module
reconstructs the historical constants (RECORD_BASED_MODULES, FILE_BASED_MODULES, ...) from
that artifact for backward compatibility, and exposes canonical accessors
(``iter_binding_targets``, ``schema_name_for_path``, ``schema_file_name``) so every script
shares one source of truth instead of re-deriving schema names independently.

Loading only reads the committed YAML artifact — it does NOT require the ``schemas/``
directory — so scripts that run outside the setup workflow keep working offline.
"""

from pathlib import Path

import yaml

# Team IDs
HTAN_DCC_ADMINS_TEAM_ID = "3497313"
HTAN_DCC_TEAM_ID = "3391844"
ACT_TEAM_ID = "464532"

_STRUCTURE_FILE = Path(__file__).parent / "module_structure.yml"

with open(_STRUCTURE_FILE) as _f:
    _STRUCTURE = yaml.safe_load(_f)

#: Data-model schema version the structure artifact was generated from (e.g. "v2.0.0").
SCHEMA_VERSION = _STRUCTURE.get("schema_version")
_MODULES = _STRUCTURE["modules"]


def _entry_path(module_name: str, umbrella, entry: dict) -> str:
    """Folder path for an entry, relative to the version folder (e.g. "Imaging/MultiplexMicroscopy/Level_2")."""
    base = f"{umbrella}/{module_name}" if umbrella else module_name
    if entry.get("bind_to_module"):
        return base
    return f"{base}/{entry['subfolder']}"


# ---------------------------------------------------------------------------
# Backward-compatible constants, reconstructed from module_structure.yml
# ---------------------------------------------------------------------------

RECORD_BASED_MODULES: dict = {}
FILE_BASED_MODULES: dict = {}
IMAGING_SUBFOLDERS: dict = {}
IMAGING_RECORD_BASED_SUBFOLDERS: dict = {}
# Record-based subfolders nested inside a file-based module (e.g. SpatialOmics/Panel,
# MassSpectrometryImaging/MolecularAssignment). Named for backward compatibility.
SPATIAL_RECORD_BASED_SUBFOLDERS: dict = {}

for _module_name, _module in _MODULES.items():
    _umbrella = _module.get("umbrella")
    _entries = _module["entries"]
    _file_entries = [e for e in _entries if e["kind"] == "file"]
    _record_entries = [e for e in _entries if e["kind"] == "record"]

    if _umbrella == "Imaging":
        FILE_BASED_MODULES.setdefault("Imaging", [])
        if _module_name not in FILE_BASED_MODULES["Imaging"]:
            FILE_BASED_MODULES["Imaging"].append(_module_name)
        IMAGING_SUBFOLDERS[_module_name] = [
            e["subfolder"] for e in _file_entries if not e.get("bind_to_module")
        ]
        if _record_entries:
            IMAGING_RECORD_BASED_SUBFOLDERS[_module_name] = [
                e["subfolder"] for e in _record_entries
            ]
    elif not _file_entries:
        # Pure record-based top-level module (Clinical, Biospecimen).
        # bind_to_module modules (Biospecimen) reconstruct to an empty subfolder list.
        RECORD_BASED_MODULES[_module_name] = [
            e["subfolder"] for e in _record_entries if not e.get("bind_to_module")
        ]
    else:
        # File-based top-level module; may carry record-based subfolders (Panel, MolecularAssignment).
        FILE_BASED_MODULES[_module_name] = [
            e["subfolder"] for e in _file_entries if not e.get("bind_to_module")
        ]
        if _record_entries:
            SPATIAL_RECORD_BASED_SUBFOLDERS[_module_name] = [
                e["subfolder"] for e in _record_entries
            ]


# ---------------------------------------------------------------------------
# Canonical accessors (single source of truth for schema names + file mapping)
# ---------------------------------------------------------------------------

def iter_binding_targets():
    """Yield one dict per schema-bound folder across all modules, in structure order.

    Each dict has: schema_name, kind ("file"|"record"), path (relative to the version
    folder, e.g. "WES/Level_1"), class (data-model file stem), bind_to_module (bool),
    upsert_keys (list; empty for file-based entries).
    """
    for module_name, module in _MODULES.items():
        umbrella = module.get("umbrella")
        for entry in module["entries"]:
            yield {
                "schema_name": entry["schema_name"],
                "kind": entry["kind"],
                "path": _entry_path(module_name, umbrella, entry),
                "class": entry["class"],
                "bind_to_module": bool(entry.get("bind_to_module", False)),
                "upsert_keys": list(entry.get("upsert_keys") or []),
            }


#: path (relative to version folder) -> canonical internal schema_name
SCHEMA_NAME_BY_PATH = {t["path"]: t["schema_name"] for t in iter_binding_targets()}
_CLASS_BY_SCHEMA_NAME = {t["schema_name"]: t["class"] for t in iter_binding_targets()}
_UPSERT_KEYS_BY_SCHEMA_NAME = {
    t["schema_name"]: t["upsert_keys"] for t in iter_binding_targets() if t["kind"] == "record"
}


def upsert_keys_for_schema(schema_name: str) -> list:
    """RecordSet primary-key column(s) for a record-based schema_name.

    Raises KeyError for unknown or file-based schema names. Callers must not fall back to
    guessing a key: an upsert key that does not identify a row makes Synapse overwrite
    distinct records, so failing to create the task is the safer outcome.
    """
    try:
        return list(_UPSERT_KEYS_BY_SCHEMA_NAME[schema_name])
    except KeyError:
        raise KeyError(
            f"no upsert_keys registered for record-based schema {schema_name!r}; "
            f"add it to htan2_synapse/module_registry.yml and regenerate module_structure.yml"
        ) from None


def schema_name_for_path(path: str):
    """Canonical internal schema_name for a folder path relative to the version folder.

    Accepts either a bare path ("WES/Level_1") or a version-prefixed one
    ("v9_ingest/WES/Level_1"); the leading version segment is stripped if present.
    """
    if path in SCHEMA_NAME_BY_PATH:
        return SCHEMA_NAME_BY_PATH[path]
    # Tolerate a leading vN_<type>/ prefix.
    stripped = path.split("/", 1)[1] if "/" in path else path
    return SCHEMA_NAME_BY_PATH.get(stripped)


def schema_file_name(schema_name: str, schema_version: str = "v1.0.0") -> str:
    """Data-model schema file name for an internal schema_name.

    Returns ``HTAN.<class>-<schema_version>-schema.json``. Falls back to using the
    schema_name itself as the class token when it is unknown (preserves prior behaviour).
    """
    cls = _CLASS_BY_SCHEMA_NAME.get(schema_name, schema_name)
    return f"HTAN.{cls}-{schema_version}-schema.json"
