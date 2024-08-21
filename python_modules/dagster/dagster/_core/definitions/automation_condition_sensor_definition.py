import functools
from typing import Any, Mapping, Optional, cast

import dagster._check as check
from dagster._annotations import experimental
from dagster._core.asset_graph_view.asset_graph_view import AssetGraphView, TemporalContext
from dagster._core.definitions.asset_selection import CoercibleToAssetSelection
from dagster._core.definitions.data_time import CachingDataTimeResolver
from dagster._core.definitions.data_version import CachingStaleStatusResolver
from dagster._core.definitions.declarative_automation.automation_condition import (
    AutomationCondition,
)
from dagster._core.definitions.declarative_automation.automation_condition_evaluator import (
    AutomationConditionEvaluator,
)
from dagster._core.definitions.run_request import SensorResult
from dagster._time import get_current_datetime
from dagster._utils.caching_instance_queryer import CachingInstanceQueryer

from .asset_selection import AssetSelection
from .sensor_definition import (
    DefaultSensorStatus,
    SensorDefinition,
    SensorEvaluationContext,
    SensorType,
)
from .utils import check_valid_name, normalize_tags


def evaluate_automation_conditions(
    context: SensorEvaluationContext, sensor_def: "AutomationConditionSensorDefinition"
):
    from dagster._core.definitions.asset_daemon_context import build_run_requests
    from dagster._daemon.asset_daemon import (
        asset_daemon_cursor_from_instigator_serialized_cursor,
        asset_daemon_cursor_to_instigator_serialized_cursor,
    )

    asset_graph = check.not_none(context.repository_def).asset_graph

    instance_queryer = CachingInstanceQueryer(
        context.instance,
        asset_graph,
        evaluation_time=get_current_datetime(),
        logger=context.log,
    )

    asset_graph_view = AssetGraphView(
        stale_resolver=CachingStaleStatusResolver(
            instance=context.instance,
            asset_graph=asset_graph,
            instance_queryer=instance_queryer,
        ),
        temporal_context=TemporalContext(
            effective_dt=instance_queryer.evaluation_time,
            last_event_id=None,
        ),
    )

    data_time_resolver = CachingDataTimeResolver(
        asset_graph_view.get_inner_queryer_for_back_compat()
    )
    cursor = asset_daemon_cursor_from_instigator_serialized_cursor(
        context.cursor,
        asset_graph,
    )

    evaluator = AutomationConditionEvaluator(
        asset_graph=asset_graph,
        asset_keys=sensor_def.asset_selection.resolve(asset_graph),
        asset_graph_view=asset_graph_view,
        logger=context.log,
        data_time_resolver=data_time_resolver,
        cursor=cursor,
        respect_materialization_data_versions=True,
        auto_materialize_run_tags={},
        request_backfills=context.instance.da_request_backfills(),
        default_automation_condition=sensor_def.default_automation_condition,
    )
    results, to_request = evaluator.evaluate()
    new_cursor = cursor.with_updates(
        evaluation_id=cursor.evaluation_id,
        evaluation_timestamp=instance_queryer.evaluation_time.timestamp(),
        newly_observe_requested_asset_keys=[],  # skip for now, hopefully forever
        condition_cursors=[result.get_new_cursor() for result in results],
    )
    run_requests = build_run_requests(
        asset_partitions=to_request,
        asset_graph=asset_graph,
        # tick_id and sensor tags should get set in daemon
        run_tags=context.instance.auto_materialize_run_tags,
    )
    # only record evaluation results where something changed
    updated_evaluations = []
    for result in results:
        previous_cursor = cursor.get_previous_condition_cursor(result.asset_key)
        if (
            previous_cursor is None
            or previous_cursor.result_value_hash != result.value_hash
            or not result.true_slice.is_empty
        ):
            updated_evaluations.append(result.serializable_evaluation)

    return SensorResult(
        run_requests=run_requests,
        cursor=asset_daemon_cursor_to_instigator_serialized_cursor(new_cursor),
        automation_condition_evaluations=updated_evaluations,
    )


def not_supported(context) -> None:
    raise NotImplementedError(
        "Automation condition sensors cannot be evaluated like regular user-space sensors."
    )


@experimental
class AutomationConditionSensorDefinition(SensorDefinition):
    """Targets a set of assets and repeatedly evaluates all the AutomationConditions on all of
    those assets to determine which to request runs for.

    Args:
        name: The name of the sensor.
        asset_selection (Union[str, Sequence[str], Sequence[AssetKey], Sequence[Union[AssetsDefinition, SourceAsset]], AssetSelection]):
            The assets to evaluate AutomationConditions of and request runs for.
        run_tags: Optional[Mapping[str, Any]] = None,
        default_status (DefaultSensorStatus): Whether the sensor starts as running or not. The default
            status can be overridden from the Dagster UI or via the GraphQL API.
        minimum_interval_seconds (Optional[int]): The frequency at which to try to evaluate the
            sensor. The actual interval will be longer if the sensor evaluation takes longer than
            the provided interval.
        description (Optional[str]): A human-readable description of the sensor.
    """

    def __init__(
        self,
        name: str,
        *,
        asset_selection: CoercibleToAssetSelection,
        run_tags: Optional[Mapping[str, Any]] = None,
        default_status: DefaultSensorStatus = DefaultSensorStatus.STOPPED,
        minimum_interval_seconds: Optional[int] = None,
        description: Optional[str] = None,
        **kwargs,
    ):
        self._user_code = check.opt_bool_param(kwargs.get("user_code"), "user_code", default=False)
        self._default_automation_condition = check.opt_inst_param(
            kwargs.get("default_automation_condition"),
            "default_automation_condition",
            AutomationCondition,
        )
        if self._default_automation_condition:
            check.param_invariant(
                self._user_code,
                "default_automation_condition",
                "Default automation conditions unsupported for non-user-code sensors.",
            )
        self._run_tags = normalize_tags(run_tags).tags

        super().__init__(
            name=check_valid_name(name),
            job_name=None,
            evaluation_fn=functools.partial(evaluate_automation_conditions, sensor_def=self)
            if self._user_code
            else not_supported,
            minimum_interval_seconds=minimum_interval_seconds,
            description=description,
            job=None,
            jobs=None,
            default_status=default_status,
            required_resource_keys=None,
            asset_selection=asset_selection,
        )

    @property
    def run_tags(self) -> Mapping[str, str]:
        return self._run_tags

    @property
    def default_automation_condition(self) -> Optional[AutomationCondition]:
        return self._default_automation_condition

    @property
    def asset_selection(self) -> AssetSelection:
        return cast(AssetSelection, super().asset_selection)

    @property
    def sensor_type(self) -> SensorType:
        return SensorType.AUTOMATION if self._user_code else SensorType.AUTO_MATERIALIZE
