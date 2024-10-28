# start_asset_specs
from dagster import AssetSpec

raw_customers_spec = AssetSpec(key=["raw_data", "raw_customers"])
export_customers_spec = AssetSpec(key="customers_csv", deps=["customers"])
# end_asset_specs

# start_dbt_assets
import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets


def dbt_project_path() -> Path:
    env_val = os.getenv("TUTORIAL_DBT_PROJECT_DIR")
    assert env_val, "TUTORIAL_DBT_PROJECT_DIR must be set"
    return Path(env_val)


@dbt_assets(
    manifest=dbt_project_path() / "target" / "manifest.json",
    project=DbtProject(dbt_project_path()),
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# end_dbt_assets

# start_task_mappings
from dagster_airlift.core import assets_with_task_mappings

mapped_assets = assets_with_task_mappings(
    dag_id="rebuild_customers_list",
    task_mappings={
        "load_raw_customers": [raw_customers_spec],
        "build_dbt_models": [dbt_project_assets],
        "export_customers": [export_customers_spec],
    },
)
# end_task_mappings


# start_build_defs
from dagster import Definitions
from dagster_airlift.core import AirflowInstance, BasicAuthBackend, build_defs_from_airflow_instance

defs = build_defs_from_airflow_instance(
    airflow_instance=AirflowInstance(
        auth_backend=BasicAuthBackend(
            webserver_url="http://localhost:8080",
            username="admin",
            password="admin",
        ),
        name="airflow_instance_one",
    ),
    defs=Definitions(
        assets=mapped_assets,
        resources={"dbt": DbtCliResource(project_dir=dbt_project_path())},
    ),
)
# end_build_defs
