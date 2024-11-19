from functools import total_ordering
from heapq import heapify, heappop, heappush
from typing import AbstractSet, Callable, Iterable, List, NamedTuple, Optional, Sequence, Tuple

import dagster._check as check
from dagster._core.asset_graph_view.asset_graph_view import AssetGraphView
from dagster._core.asset_graph_view.serializable_entity_subset import (
    EntitySubsetValue,
    SerializableEntitySubset,
)
from dagster._core.definitions.asset_graph_subset import AssetGraphSubset
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.base_asset_graph import BaseAssetGraph
from dagster._core.definitions.time_window_partitions import get_time_partitions_def
from dagster._record import record


@record
class AssetGraphViewBfsFilterConditionResult:
    passed_subset_value: EntitySubsetValue
    excluded_subset_values_and_reasons: Sequence[Tuple[EntitySubsetValue, str]]


def bfs_filter_asset_graph_view(
    asset_graph_view: AssetGraphView,
    condition_fn: Callable[
        [AbstractSet[AssetKey], "EntitySubsetValue", "AssetGraphSubset"],
        AssetGraphViewBfsFilterConditionResult,
    ],
    initial_asset_subset: "AssetGraphSubset",
) -> Tuple[AssetGraphSubset, Sequence[Tuple[AssetGraphSubset, str]]]:
    """Returns asset partitions within the graph that satisfy supplied criteria.

    - Are >= initial_asset_partitions
    - Match the condition_fn
    - Any of their ancestors >= initial_asset_partitions match the condition_fn

    Also returns a list of tuples, where each tuple is a candidated_unit (list of
    AssetKeyPartitionKeys that must be materialized together - ie multi_asset) that do not
    satisfy the criteria and the reason they were filtered out.

    The condition_fn should return a tuple of a boolean indicating whether the asset partition meets
    the condition and a string explaining why it does not meet the condition, if applicable.

    Visits parents before children.

    When asset partitions are part of the same execution set (non-subsettable multi-asset),
    they're provided all at once to the condition_fn.
    """
    initial_subsets = list(
        initial_asset_subset.iterate_asset_subsets(asset_graph=asset_graph_view.asset_graph)
    )

    # invariant: we never consider an asset partition before considering its ancestors
    queue = ToposortedPriorityQueue(
        asset_graph_view.asset_graph, initial_subsets, include_full_execution_set=True
    )

    visited_graph_subset = AssetGraphSubset.from_serializable_entity_subsets(initial_subsets)

    result: AssetGraphSubset = AssetGraphSubset.empty()
    failed_reasons: List[Tuple[AssetGraphSubset, str]] = []

    asset_graph = asset_graph_view.asset_graph

    while len(queue) > 0:
        candidate_keys, candidate_subset_value = queue.dequeue()

        condition_result = condition_fn(candidate_keys, candidate_subset_value, result)
        subset_that_meets_condition = condition_result.passed_subset_value
        fail_reasons = condition_result.excluded_subset_values_and_reasons

        for fail_subset_value, fail_reason in fail_reasons:
            failed_asset_subset = AssetGraphSubset.from_serializable_entity_subsets(
                [
                    SerializableEntitySubset(key=asset_key, value=fail_subset_value)
                    for asset_key in candidate_keys
                ]
            )
            failed_reasons.append((failed_asset_subset, fail_reason))

        for candidate_key in candidate_keys:
            candidate_subset_value = SerializableEntitySubset(
                candidate_key, subset_that_meets_condition
            )
            if not candidate_subset_value.is_empty:
                result = result | AssetGraphSubset.from_serializable_entity_subsets(
                    [candidate_subset_value]
                )

                matching_entity_subset = check.not_none(
                    asset_graph_view.get_subset_from_serializable_subset(candidate_subset_value)
                )

                for child_key in asset_graph.get(candidate_key).child_keys:
                    child_subset = asset_graph_view.compute_child_subset(
                        child_key, matching_entity_subset
                    )
                    unvisited_child_subset = (
                        child_subset.convert_to_serializable_subset()
                        - visited_graph_subset.get_asset_subset(child_key, asset_graph)
                    )
                    if not unvisited_child_subset.is_empty:
                        queue.enqueue(unvisited_child_subset)
                        visited_graph_subset = (
                            visited_graph_subset
                            | AssetGraphSubset.from_serializable_entity_subsets(
                                [unvisited_child_subset]
                            )
                        )

    return result, failed_reasons


def sort_key_for_serializable_entity_subset(
    asset_graph: BaseAssetGraph, serializable_entity_subset: "SerializableEntitySubset"
) -> float:
    """Returns an integer sort key such that asset partition ranges are sorted in
    the order in which they should be materialized. For assets without a time
    window partition dimension, this is always 0.
    Assets with a time window partition dimension will be sorted from newest to oldest, unless they
    have a self-dependency, in which case they are sorted from oldest to newest.
    """
    partitions_def = asset_graph.get(serializable_entity_subset.key).partitions_def
    time_partitions_def = get_time_partitions_def(partitions_def)
    if time_partitions_def is None:
        return 0

    # A sort key such that time window partitions are sorted from oldest start time to newest start time
    start_timestamp = time_partitions_def.start_ts.timestamp

    if asset_graph.get(serializable_entity_subset.key).has_self_dependency:
        # sort self dependencies from oldest to newest, as older partitions must exist before
        # new ones can execute
        return start_timestamp
    else:
        # sort non-self dependencies from newest to oldest, as newer partitions are more relevant
        # than older ones
        return -1 * start_timestamp


class ToposortedPriorityQueue:
    """Queue that returns parents before their children."""

    @total_ordering
    class QueueItem(NamedTuple):
        level: int
        partition_sort_key: Optional[float]
        asset_keys: AbstractSet[AssetKey]
        entity_subset_value: "EntitySubsetValue"

        def __eq__(self, other: object) -> bool:
            if isinstance(other, ToposortedPriorityQueue.QueueItem):
                return (
                    self.level == other.level
                    and self.partition_sort_key == other.partition_sort_key
                )
            return False

        def __lt__(self, other: object) -> bool:
            if isinstance(other, ToposortedPriorityQueue.QueueItem):
                return self.level < other.level or (
                    self.level == other.level
                    and self.partition_sort_key is not None
                    and other.partition_sort_key is not None
                    and self.partition_sort_key < other.partition_sort_key
                )
            raise TypeError()

    def __init__(
        self,
        asset_graph: BaseAssetGraph,
        items: Iterable["SerializableEntitySubset"],
        include_full_execution_set: bool,
    ):
        self._asset_graph = asset_graph
        self._include_full_execution_set = include_full_execution_set

        self._toposort_level_by_asset_key = {
            asset_key: i
            for i, asset_keys in enumerate(asset_graph.toposorted_asset_keys_by_level)
            for asset_key in asset_keys
        }
        self._heap = [
            self._queue_item(serializable_entity_subset) for serializable_entity_subset in items
        ]
        heapify(self._heap)

    def enqueue(self, serializable_entity_subset: "SerializableEntitySubset") -> None:
        heappush(self._heap, self._queue_item(serializable_entity_subset))

    def dequeue(self) -> Tuple[AbstractSet[AssetKey], "EntitySubsetValue"]:
        # For multi-assets, will include all required multi-asset keys if
        # include_full_execution_set is set to True, or a list of size 1 with just the passed in
        # asset key if it was not. They will all have the same partition range.
        heap_value = heappop(self._heap)
        return heap_value.asset_keys, heap_value.entity_subset_value

    def _queue_item(
        self, serializable_entity_subset: "SerializableEntitySubset"
    ) -> "ToposortedPriorityQueue.QueueItem":
        asset_key = serializable_entity_subset.key

        if self._include_full_execution_set:
            execution_set_keys = self._asset_graph.get(asset_key).execution_set_asset_keys
        else:
            execution_set_keys = {asset_key}

        level = max(
            self._toposort_level_by_asset_key[asset_key] for asset_key in execution_set_keys
        )

        return ToposortedPriorityQueue.QueueItem(
            level,
            sort_key_for_serializable_entity_subset(self._asset_graph, serializable_entity_subset),
            execution_set_keys,
            serializable_entity_subset.value,
        )

    def __len__(self) -> int:
        return len(self._heap)
