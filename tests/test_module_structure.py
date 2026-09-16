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

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import htan2_synapse.config as config  # noqa: E402

CONFIG_YML = REPO / "schema_binding_config.yml"
ARTIFACT = REPO / "htan2_synapse" / "module_structure.yml"
REGISTRY = REPO / "htan2_synapse" / "module_registry.yml"
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
    sb = cfg["schema_bindings"]
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


# Assays new in v2.0.0 — the legacy map predates them, so parity-with-legacy does not
# apply (e.g. scATAC's legacy else-branch produces the wrong file name). These are
# validated by test_c_new_assays_resolve_to_expected_files instead.
_NEW_IN_V2 = ("scATAC_seq", "MassSpectrometryImaging", "MolecularAssignment")


def test_c_schema_file_name_parity_with_legacy():
    """schema_file_name reproduces the legacy map for every *pre-existing* schema_name,
    across both schema versions in play."""
    existing_names = {
        name for _, name, _ in _config_targets()
        if not name.startswith(_NEW_IN_V2)
    }
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


# ---------------------------------------------------------------------------
# RecordSet upsert keys (primary keys)
#
# These previously came from a case-sensitive probe for 'HTAN_Participant_ID' that never
# matched the data model's 'HTAN_PARTICIPANT_ID', so every record-based task silently fell
# back to the schema's first required property — Demographics was keyed on ETHNIC_GROUP.
# ---------------------------------------------------------------------------

def _fixture_schema(class_token: str) -> dict:
    return json.loads((FIXTURES / f"HTAN.{class_token}-v2.0.0-schema.json").read_text())


def _run_generator(registry_path, schemas_dir, out_path):
    return subprocess.run(
        [sys.executable, str(GENERATOR), "--schemas-dir", str(schemas_dir),
         "--schema-version", "v2.0.0", "--registry", str(registry_path),
         "--output", str(out_path)],
        capture_output=True, text=True,
    )


def _registry_with(class_token, mutate):
    """Copy of the registry with `mutate` applied to the entry for `class_token`."""
    registry = yaml.safe_load(REGISTRY.read_text())
    for module in registry["modules"].values():
        for entry in module["entries"]:
            if entry["class"] == class_token:
                mutate(entry)
    return registry


def test_d_every_record_binding_has_upsert_keys_that_identify_a_row():
    """Every record-based binding resolves to keys that are all REQUIRED schema properties.

    Requiredness is the load-bearing part: a nullable key column means rows with a blank
    value collide, which is the same silent-overwrite failure as the wrong key.
    """
    record_names = {name for section, name, _ in _config_targets() if section == "record_based"}
    assert record_names, "no record-based bindings found in schema_binding_config.yml"

    class_by_name = {t["schema_name"]: t["class"] for t in config.iter_binding_targets()}
    for name in sorted(record_names):
        keys = config.upsert_keys_for_schema(name)
        assert keys, f"{name} has no upsert keys"
        schema = _fixture_schema(class_by_name[name])
        required = set(schema.get("required") or [])
        for key in keys:
            assert key in schema["properties"], f"{name}: key {key!r} is not a schema property"
            assert key in required, f"{name}: key {key!r} is optional, so it cannot identify a row"


def test_d_participant_scoped_tables_are_keyed_on_the_participant():
    """The regression that started this: clinical tables must key on the participant, not
    on whichever clinical attribute happened to sort first in `required`."""
    for name in ("Demographics", "Diagnosis", "Therapy", "FollowUp",
                 "MolecularTest", "Exposure", "FamilyHistory", "VitalStatus"):
        assert config.upsert_keys_for_schema(name)[0] == "HTAN_PARTICIPANT_ID", name


def test_d_clinical_tables_are_keyed_on_the_participant_alone():
    """Clinical keys are exactly what the data model declares — no extra columns.

    Guards a regression we shipped once: discriminator columns were added to Diagnosis,
    Therapy, FollowUp and MolecularTest on the assumption that one participant maps to many
    rows. The model says otherwise (`identifier: true` on ClinicalRecordAttributes
    .HTAN_PARTICIPANT_ID), and widening the key changes the grain of the table. If the data
    really is one-to-many, that is a data-model change, not a registry change.
    """
    for name in ("Demographics", "Diagnosis", "Therapy", "FollowUp",
                 "MolecularTest", "Exposure", "FamilyHistory", "VitalStatus"):
        assert config.upsert_keys_for_schema(name) == ["HTAN_PARTICIPANT_ID"], name


def test_d_child_recordsets_carry_a_discriminator():
    """The three per-row child RecordSets key on parent ID + a discriminator.

    Their parent ID is a foreign key repeated on every row of the set — the data model says
    so for ChannelMetadata ("All rows in a given ChannelMetadata RecordSet share the same
    HTAN_PANEL_ID") and MolecularAssignment ("Foreign key to the parent Level 3 OME-TIFF
    file ID (same value for all rows in a RecordSet)"), so it cannot identify a row alone.
    """
    expected = {
        "SpatialPanel": ["HTAN_PANEL_ID", "TARGET_NAME"],
        "ChannelMetadata": ["HTAN_PANEL_ID", "CHANNEL_ID"],
        "MolecularAssignment": ["HTAN_DATA_FILE_ID", "CHANNEL_INDEX"],
    }
    for name, keys in expected.items():
        assert config.upsert_keys_for_schema(name) == keys, name


def test_d_file_based_entries_have_no_upsert_keys():
    """File-based schemas have no RecordSet, so asking for keys is a programming error."""
    for name in ("BulkWESLevel1", "DigitalPathology", "MultiplexMicroscopyLevel2"):
        with pytest.raises(KeyError):
            config.upsert_keys_for_schema(name)


def test_d_generator_rejects_wrong_case_upsert_key():
    """The original bug shape: a key that differs only in case must abort, with a hint."""
    registry = _registry_with("Demographics",
                              lambda e: e.__setitem__("upsert_keys", ["HTAN_Participant_ID"]))
    with tempfile.TemporaryDirectory() as td:
        reg_path = Path(td) / "registry.yml"
        reg_path.write_text(yaml.safe_dump(registry))
        r = _run_generator(reg_path, FIXTURES, Path(td) / "o.yml")
    assert r.returncode == 1
    assert "HTAN_Participant_ID" in r.stderr
    assert "HTAN_PARTICIPANT_ID" in r.stderr  # the did-you-mean hint


def test_d_generator_rejects_optional_upsert_key():
    """ENSEMBL_ID is the tidier SpatialPanel discriminator but is optional — must abort."""
    registry = _registry_with(
        "SpatialPanel", lambda e: e.__setitem__("upsert_keys", ["HTAN_PANEL_ID", "ENSEMBL_ID"]))
    with tempfile.TemporaryDirectory() as td:
        reg_path = Path(td) / "registry.yml"
        reg_path.write_text(yaml.safe_dump(registry))
        r = _run_generator(reg_path, FIXTURES, Path(td) / "o.yml")
    assert r.returncode == 1
    assert "ENSEMBL_ID" in r.stderr and "optional" in r.stderr


def test_d_generator_rejects_record_entry_with_no_upsert_keys():
    registry = _registry_with("Demographics", lambda e: e.pop("upsert_keys", None))
    with tempfile.TemporaryDirectory() as td:
        reg_path = Path(td) / "registry.yml"
        reg_path.write_text(yaml.safe_dump(registry))
        r = _run_generator(reg_path, FIXTURES, Path(td) / "o.yml")
    assert r.returncode == 1
    assert "missing upsert_keys" in r.stderr


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
