from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FULL_LOAD = ROOT / "consolidated_pipeline/3_fact_data_processing/1_full_load_fact.py"
INCREMENTAL = ROOT / "consolidated_pipeline/3_fact_data_processing/2_incremental_load_fact.py"
QUALITY_MODULE = ROOT / "consolidated_pipeline/common/order_quality.py"


def test_databricks_sources_are_valid_python():
    for path in (FULL_LOAD, INCREMENTAL, QUALITY_MODULE):
        compile(path.read_text(encoding="utf-8"), str(path), "exec")


def test_both_order_paths_persist_quarantine_and_metrics():
    for path in (FULL_LOAD, INCREMENTAL):
        source = path.read_text(encoding="utf-8")
        assert "%run /Workspace/Users/metti.mounu@gmail.com/consolidated_pipeline/common/order_quality" in source
        assert "split_valid_and_quarantined" in source
        assert "persist_quality_results" in source
        assert "quarantine_orders" in source
        assert "order_quality_metrics" in source
        assert '.otherwise("999999")' not in source


def test_quality_module_defines_stable_reason_codes():
    source = QUALITY_MODULE.read_text(encoding="utf-8")
    expected_codes = {
        "missing_order_id",
        "invalid_order_date",
        "invalid_customer_id",
        "invalid_product_id",
        "invalid_order_qty",
        "duplicate_record",
        "unknown_product_id",
    }
    assert all(code in source for code in expected_codes)

