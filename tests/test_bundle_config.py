from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
BUNDLE = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
JOBS_PATH = ROOT / "resources/jobs.yml"
JOBS = yaml.safe_load(JOBS_PATH.read_text(encoding="utf-8"))["resources"]["jobs"]
DEPLOY_WORKFLOW = (ROOT / ".github/workflows/deploy-databricks.yml").read_text(encoding="utf-8")


def _tasks(job_name):
    return {task["task_key"]: task for task in JOBS[job_name]["tasks"]}


def test_bundle_defines_isolated_dev_and_prod_targets():
    assert BUNDLE["targets"]["dev"]["default"] is True
    assert BUNDLE["targets"]["dev"]["variables"]["catalog"] == "fmcg_dev"
    assert BUNDLE["targets"]["dev"]["variables"]["source_root"] == "/Volumes/fmcg_dev/bronze/landing"
    assert BUNDLE["targets"]["dev"]["variables"]["incremental_schedule_status"] == "PAUSED"
    assert BUNDLE["targets"]["prod"]["variables"]["catalog"] == "fmcg"
    assert BUNDLE["targets"]["prod"]["variables"]["incremental_schedule_status"] == "UNPAUSED"


def test_bundle_contains_manual_initial_and_scheduled_incremental_jobs():
    assert set(JOBS) == {"fmcg_prepare_storage", "fmcg_initial_load", "fmcg_daily_incremental"}
    assert len(JOBS["fmcg_prepare_storage"]["tasks"]) == 1
    assert "schedule" not in JOBS["fmcg_initial_load"]
    schedule = JOBS["fmcg_daily_incremental"]["schedule"]
    assert schedule["quartz_cron_expression"] == "0 0 2 * * ?"
    assert schedule["timezone_id"] == "UTC"


def test_initial_load_dag_orders_dimensions_before_facts():
    tasks = _tasks("fmcg_initial_load")
    assert {item["task_key"] for item in tasks["load_pricing"]["depends_on"]} == {"load_products"}
    assert {item["task_key"] for item in tasks["load_historical_orders"]["depends_on"]} == {
        "create_date_dimension",
        "load_customers",
        "load_pricing",
    }


def test_every_notebook_task_points_to_a_versioned_source_file():
    for job in JOBS.values():
        for task in job["tasks"]:
            notebook = task["notebook_task"]
            resolved = (JOBS_PATH.parent / notebook["notebook_path"]).resolve()
            assert resolved.is_relative_to(ROOT)
            assert resolved.is_file(), resolved
            assert notebook["source"] == "WORKSPACE"


def test_all_data_tasks_receive_catalog_and_source_parameters():
    for job in JOBS.values():
        for task in job["tasks"]:
            parameters = task["notebook_task"].get("base_parameters", {})
            assert "catalog" in parameters
            if "setup" not in task["task_key"] and "date" not in task["task_key"]:
                assert parameters["source_root"] == "{{job.parameters.source_root}}"


def test_manual_deployment_uses_oidc_and_validates_before_deploying():
    assert "workflow_dispatch:" in DEPLOY_WORKFLOW
    assert "DATABRICKS_AUTH_TYPE: github-oidc" in DEPLOY_WORKFLOW
    assert "id-token: write" in DEPLOY_WORKFLOW
    assert DEPLOY_WORKFLOW.index("databricks bundle validate") < DEPLOY_WORKFLOW.index(
        "databricks bundle deploy"
    )

