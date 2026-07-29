"""Unit tests for the carry-forward column mapping (pure logic; no Synapse).

Run: pytest tests/test_carry_forward.py   (or: python tests/test_carry_forward.py)
"""

import importlib.util
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "carry_forward_recordsets",
    REPO / "scripts" / "manage" / "carry_forward_recordsets.py",
)
cf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cf)


def test_exact_match_copies_all_values():
    src = pd.DataFrame({"HTAN_Participant_ID": ["p1", "p2"], "AGE": [40, 51]})
    out, dropped, blanked = cf.map_columns(src, ["HTAN_Participant_ID", "AGE"])
    assert dropped == [] and blanked == []
    assert out["AGE"].tolist() == [40, 51]
    assert out["HTAN_Participant_ID"].tolist() == ["p1", "p2"]


def test_new_target_column_is_blanked():
    src = pd.DataFrame({"HTAN_Participant_ID": ["p1"], "AGE": [40]})
    out, dropped, blanked = cf.map_columns(src, ["HTAN_Participant_ID", "AGE", "NEW_V2_FIELD"])
    assert dropped == []
    assert blanked == ["NEW_V2_FIELD"]
    assert out["NEW_V2_FIELD"].isna().all()
    assert list(out.columns) == ["HTAN_Participant_ID", "AGE", "NEW_V2_FIELD"]


def test_source_only_column_is_dropped_and_reported():
    src = pd.DataFrame({"HTAN_Participant_ID": ["p1"], "REMOVED_V1_FIELD": ["x"], "AGE": [40]})
    out, dropped, blanked = cf.map_columns(src, ["HTAN_Participant_ID", "AGE"])
    assert dropped == ["REMOVED_V1_FIELD"]
    assert blanked == []
    assert "REMOVED_V1_FIELD" not in out.columns
    assert out["AGE"].tolist() == [40]


def test_overlap_values_preserved_amid_drift():
    src = pd.DataFrame({
        "HTAN_Participant_ID": ["p1", "p2"],
        "GONE": [1, 2],
        "AGE": [40, 51],
    })
    out, dropped, blanked = cf.map_columns(src, ["HTAN_Participant_ID", "AGE", "NEW"])
    assert dropped == ["GONE"]
    assert blanked == ["NEW"]
    # overlapping columns keep their exact values and row order
    assert out["HTAN_Participant_ID"].tolist() == ["p1", "p2"]
    assert out["AGE"].tolist() == [40, 51]


def test_target_column_order_is_authoritative():
    src = pd.DataFrame({"B": [1], "A": [2]})
    out, _, _ = cf.map_columns(src, ["A", "B", "C"])
    assert list(out.columns) == ["A", "B", "C"]


def test_empty_source_yields_zero_rows_with_target_columns():
    src = pd.DataFrame(columns=["HTAN_Participant_ID", "AGE"])
    out, dropped, blanked = cf.map_columns(src, ["HTAN_Participant_ID", "AGE", "NEW"])
    assert len(out) == 0
    assert list(out.columns) == ["HTAN_Participant_ID", "AGE", "NEW"]
    assert blanked == ["NEW"]


def test_index_record_entries_matches_prefix_and_key():
    config = {"schema_bindings": {"record_based": {
        "Demographics": {"projects": [
            {"name": "HTAN2_CRC", "subfolder": "v8_release/Clinical/Demographics",
             "synapse_id": "syn1", "fileview_id": "synRS_src"},
            {"name": "HTAN2_CRC", "subfolder": "v9_ingest/Clinical/Demographics",
             "synapse_id": "syn2", "fileview_id": "synRS_tgt"},
        ]},
    }, "file_based": {}}}
    src = cf.index_record_entries(config, "v8_release")
    tgt = cf.index_record_entries(config, "v9_ingest")
    assert src[("Demographics", "HTAN2_CRC")]["fileview_id"] == "synRS_src"
    assert tgt[("Demographics", "HTAN2_CRC")]["fileview_id"] == "synRS_tgt"
    assert ("Demographics", "HTAN2_CRC") not in cf.index_record_entries(config, "v9_release")


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-v"]))
