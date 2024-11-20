from dagster_looker import (
    DagsterLookerApiTranslator,
    LookerResource,
    LookerStructureData,
    LookerStructureType,
    load_looker_asset_specs,
)

import dagster as dg
from dagster._core.definitions.asset_spec import replace_attributes

looker_resource = LookerResource(
    client_id=dg.EnvVar("LOOKERSDK_CLIENT_ID"),
    client_secret=dg.EnvVar("LOOKERSDK_CLIENT_SECRET"),
    base_url=dg.EnvVar("LOOKERSDK_HOST_URL"),
)


class CustomDagsterLookerApiTranslator(DagsterLookerApiTranslator):
    def get_asset_spec(self, looker_structure: LookerStructureData) -> dg.AssetSpec:
        # We create the default asset spec using super()
        default_spec = super().get_asset_spec(looker_structure)
        # We customize the team owner tag for all assets,
        # and we customize the asset key prefix only for dashboards.
        return replace_attributes(
            default_spec,
            key=default_spec.key.with_prefix("looker")
            if looker_structure.structure_type == LookerStructureType.DASHBOARD
            else default_spec.key,
            metadata={**default_spec.metadata, "custom": "metadata"},
            owners=["team:my_team"],
        )


looker_specs = load_looker_asset_specs(
    looker_resource, dagster_looker_translator=CustomDagsterLookerApiTranslator
)
defs = dg.Definitions(assets=[*looker_specs], resources={"looker": looker_resource})
