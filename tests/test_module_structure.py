"""
Golden tests for the data-model-driven module structure.

These guard the two invariants that keep the refactor safe:
  * existing v8 bindings (schema_name + path) are still produced, unchanged, so nothing
    already created/bound in Synapse breaks;
  * the schema_name -> data-model-file mapping is byte-for-byte identical to the legacy
    hand-maintained map for every existing name.

Run: pytest tests/    (or: python tests/test_module_structure.py)
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import htan2_synapse.config as config  # noqa: E402

CONFIG_YML = REPO / "schema_binding_config.yml"
ARTIFACT = REPO / "htan2_synapse" / "module_structure.yml"
FIXTURES = Path(__file__).parent / "fixtures" / "schemas"
GENERATOR = REPO / "scripts" / "manage" / "generate_module_structure.py"


def _strip_version_prefix(subfolder: str) -> str:
    """'v8_ingest/WES/Level_1' -> 'WES/Level_1'."""
    parts = subfolder.split("/", 1)
    return parts[1] if len(parts) == 2 else subfolder


def _legacy_schema_file(schema_name: str, schema_version: str) -> str:
    """Verbatim copy of the pre-refactor bind_schemas_workflow.map_schema_name_to_file."""
    version_suffix = f"-{schema_version}"
    if schema_name == "DigitalPathology":
        return f"HTAN.DigitalPathologyData{version_suffix}-schema.json"
    elif schema_name == "Biospecimen":
        return f"HTAN.BiospecimenData{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel1":
        return f"HTAN.scRNALevel1{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel2":
        return f"HTAN.scRNALevel2{version_suffix}-schema.json"
    elif schema_name == "scRNA_seqLevel3_4":
        return f"HTAN.scRNALevel3and4{version_suffix}-schema.json"
    elif schema_name.startswith("SpatialOmics"):
        level_part = schema_name.replace("SpatialOmics", "Spatial")
        return f"HTAN.{level_part}{version_suffix}-schema.json"
    else:
        return f"HTAN.{schema_name}{version_suffix}-schema.json"


def _generated_targets():
    """(section, schema_name, path) set from the canonical accessor."""
    out = set()
    for t in config.iter_binding_targets():
        section = "file_based" if t["kind"] == "file" else "record_based"
        out.add((section, t["schema_name"], t["path"]))
    return out


def _config_targets():
    """(section, schema_name, path) set from committed schema_binding_config.yml (all versions)."""
    with open(CONFIG_YML) as f:
        cfg = yaml.safe_load(f)
    out = set()
    for section in ("file_based", "record_based"):
        for schema_name, sc in sb.get(section, {}).items():
            for proj in sc.get("projects", []):
                out.add((section, schema_name, _strip_version_prefix(proj["subfolder"])))
    return out


def test_a_existing_bindings_are_reproduced_exactly():
    """Every binding in schema_binding_config.yml is still produced with the same
    section, schema_name, and path — no renames, no moves."""
    generated = _generated_targets()
    existing = _config_targets()
    missing = existing - generated
    assert not missing, f"{len(missing)} existing binding(s) no longer produced: {sorted(missing)[:10]}"


def test_a_no_schema_name_path_collision():
    """No existing schema_name is remapped to a different path than the config records."""
    gen_by_name = {}
    for section, name, path in _generated_targets():
        gen_by_name.setdefault(name, set()).add(path)
    for section, name, path in _config_targets():
        assert path in gen_by_name.get(name, set()), (
            f"schema_name {name!r} maps to {sorted(gen_by_name.get(name, set()))} "
            f"but config uses path {path!r}"
        )


def test_c_schema_file_name_parity_with_legacy():
    """schema_file_name reproduces the legacy map for every existing schema_name,
    across both schema versions in play."""
    existing_names = {name for _, name, _ in _config_targets()}
    for version in ("v1.0.0", "v2.0.0"):
        for name in existing_names:
            assert config.schema_file_name(name, version) == _legacy_schema_file(name, version), name


def test_c_new_assays_resolve_to_expected_files():
    v = "v2.0.0"
    expected = {
        "scATAC_seqLevel1": "HTAN.scATACLevel1-v2.0.0-schema.json",
        "scATAC_seqLevel2": "HTAN.scATACLevel2-v2.0.0-schema.json",
        "scATAC_seqLevel3_4": "HTAN.scATACLevel3and4-v2.0.0-schema.json",
        "MassSpectrometryImagingLevel1": "HTAN.MassSpectrometryImagingLevel1-v2.0.0-schema.json",
        "MassSpectrometryImagingLevel4": "HTAN.MassSpectrometryImagingLevel4-v2.0.0-schema.json",
        "MolecularAssignment": "HTAN.MolecularAssignment-v2.0.0-schema.json",
    }
    for name, filename in expected.items():
        assert config.schema_file_name(name, v) == filename, name


def test_b_generator_reproduces_committed_artifact():
    """Generator run against the committed fixtures equals the committed artifact."""
    with tempfile.TemporaryDirectory() as td:
        out = Path(td) / "structure.yml"
        subprocess.run(
            [sys.executable, str(GENERATOR), "--schemas-dir", str(FIXTURES),
             "--schema-version", "v2.0.0", "--output", str(out)],
            check=True, capture_output=True,
        )
        assert yaml.safe_load(open(out)) == yaml.safe_load(open(ARTIFACT))


def test_b_generator_fails_loud_on_unregistered_schema():
    """An unregistered schema present in schemas/ aborts with exit 1 naming it."""
    with tempfile.TemporaryDirectory() as td:
        # copy fixtures + inject a bogus one
        for f in os.listdir(FIXTURES):
            (Path(td) / f).write_bytes((FIXTURES / f).read_bytes())
        (Path(td) / "HTAN.BogusAssay-v2.0.0-schema.json").write_text(
            json.dumps({"properties": {"FILENAME": {}}})
        )
        r = subprocess.run(
            [sys.executable, str(GENERATOR), "--schemas-dir", td,
             "--schema-version", "v2.0.0", "--output", str(Path(td) / "o.yml")],
            capture_output=True, text=True,
        )
        assert r.returncode == 1
        assert "BogusAssay" in r.stderr


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
