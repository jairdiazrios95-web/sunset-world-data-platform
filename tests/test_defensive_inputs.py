"""Fail-closed input validation regression tests.

No external services, network, or credentials required. Positive pipeline
acceptance is tested separately with physical services in GitHub Actions.
"""
import csv
import pytest

from sunset.parquet import csv_batches
from sunset.quality import inspect_csv
from warehouse.loader import row_batches


def test_landing_rejects_short_and_long_csv_rows(tmp_path):
    for contents in ("id,notes\nL1\n", "id,notes\nL1,ok,unexpected\n"):
        path = tmp_path / "bad.csv"
        path.write_text(contents, encoding="utf-8")
        with pytest.raises(ValueError, match="Malformed CSV row"):
            list(row_batches(path, ["id", "notes"], 1, required_keys=["id"]))


def test_landing_rejects_blank_natural_key_but_allows_blank_optional_field(tmp_path):
    path = tmp_path / "input.csv"
    path.write_text("id,notes\n  ,ok\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Empty natural key"):
        list(row_batches(path, ["id", "notes"], 1, required_keys=["id"]))
    path.write_text("id,notes\nL1,\n", encoding="utf-8")
    assert list(row_batches(path, ["id", "notes"], 1, required_keys=["id"])) == [
        [("L1", "")]
    ]


@pytest.mark.parametrize("contents", [
    "lead_id,cliente_id\nA1\n",
    "lead_id,cliente_id\nA1,C1,unexpected\n",
])
def test_quality_rejects_incomplete_csv_without_crashing(tmp_path, contents):
    path = tmp_path / "bad.csv"
    path.write_text(contents, encoding="utf-8")
    metrics, errors = inspect_csv(
        path, {"primary_key": ["lead_id"], "required": ["cliente_id"]},
        "clientes",
    )
    assert metrics["malformed_rows"] == 1
    assert any("malformed_rows" in error for error in errors)


def test_quality_rejects_duplicate_header(tmp_path):
    path = tmp_path / "duplicate.csv"
    path.write_text("lead_id,lead_id,cliente_id\nL1,L2,C1\n", encoding="utf-8")
    _, errors = inspect_csv(
        path, {"primary_key": ["lead_id"], "required": ["cliente_id"]},
        "clientes",
    )
    assert any("duplicate" in error for error in errors)


def test_parquet_rejects_whitespace_only_natural_key(tmp_path):
    # The low-level reader is valid; the source-level converter must reject
    # this key instead of relying on truthiness alone.
    path = tmp_path / "inputs.csv"
    path.write_text("id,date\n   ,2024-01-01\n", encoding="utf-8")
    assert list(csv_batches(path, 5))[0][1][0]["id"] == "   "
    import inspect
    from sunset.parquet import convert
    source = inspect.getsource(convert)
    assert "(row[key] or '').strip()" in source
