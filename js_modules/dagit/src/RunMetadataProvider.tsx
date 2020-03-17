import * as React from "react";
import gql from "graphql-tag";

import { RunMetadataProviderMessageFragment } from "./types/RunMetadataProviderMessageFragment";
import { TempMetadataEntryFragment } from "./types/TempMetadataEntryFragment";

export enum IStepState {
  PREPARING = "preparing",
  RUNNING = "running",
  SUCCEEDED = "succeeded",
  SKIPPED = "skipped",
  FAILED = "failed"
}

export enum IExpectationResultStatus {
  PASSED = "Passed",
  FAILED = "Failed"
}

export enum IStepDisplayIconType {
  SUCCESS = "dot-success",
  FAILURE = "dot-failure",
  PENDING = "dot-pending",
  FILE = "file",
  LINK = "link",
  NONE = "none"
}

export enum IStepDisplayActionType {
  OPEN_IN_TAB = "open-in-tab",
  COPY = "copy",
  SHOW_IN_MODAL = "show-in-modal",
  NONE = "none"
}

interface IDisplayEventItem {
  text: string; // shown in gray on the left
  action: IStepDisplayActionType;
  actionText: string; // shown after `text`, optionally with a click action
  actionValue: string; // value passed to the click action
}

export interface IStepDisplayEvent {
  text: string;
  icon: IStepDisplayIconType;
  items: IDisplayEventItem[];
}

export interface IExpectationResult extends IStepDisplayEvent {
  status: IExpectationResultStatus;
}

export interface IMaterialization extends IStepDisplayEvent {}

export interface IMarker {
  key: string;
  start?: number;
  end?: number;
}

export interface IStepMetadata {
  // current state
  state: IStepState;

  // execution start and stop (user-code)
  start?: number;
  end?: number;

  // current state + prev state transition times
  transitions: {
    state: IStepState;
    time: number;
  }[];

  // accumulated metadata
  expectationResults: IExpectationResult[];
  materializations: IMaterialization[];
  markers: IMarker[];
}

export interface IRunMetadataDict {
  firstLogAt: number;
  mostRecentLogAt: number;
  startingProcessAt?: number;
  startedProcessAt?: number;
  startedPipelineAt?: number;
  exitedAt?: number;
  processId?: number;
  initFailed?: boolean;
  globalMarkers: IMarker[];
  steps: {
    [stepKey: string]: IStepMetadata;
  };
}

export const EMPTY_RUN_METADATA: IRunMetadataDict = {
  firstLogAt: 0,
  mostRecentLogAt: 0,
  globalMarkers: [],
  steps: {}
};

function itemsForMetadataEntries(
  metadataEntries: TempMetadataEntryFragment[]
): IDisplayEventItem[] {
  const items = [];
  for (const metadataEntry of metadataEntries) {
    switch (metadataEntry.__typename) {
      case "EventPathMetadataEntry":
        items.push({
          text: metadataEntry.label,
          actionText: "[Copy Path]",
          action: IStepDisplayActionType.COPY,
          actionValue: metadataEntry.path
        });
        break;
      case "EventJsonMetadataEntry":
        items.push({
          text: metadataEntry.label,
          actionText: "[Show Metadata]",
          action: IStepDisplayActionType.SHOW_IN_MODAL,
          // take JSON string, parse, and then pretty print
          actionValue: JSON.stringify(
            JSON.parse(metadataEntry.jsonString),
            null,
            2
          )
        });

        break;
      case "EventUrlMetadataEntry":
        items.push({
          text: metadataEntry.label,
          actionText: "[Open URL]",
          action: IStepDisplayActionType.OPEN_IN_TAB,
          actionValue: metadataEntry.url
        });

        break;
      case "EventTextMetadataEntry":
        items.push({
          text: metadataEntry.label,
          actionText: metadataEntry.text,
          action: IStepDisplayActionType.NONE,
          actionValue: ""
        });

        break;
      case "EventPythonArtifactMetadataEntry":
        items.push({
          text: metadataEntry.label,
          actionText: `${metadataEntry.module}:${metadataEntry.name} - ${metadataEntry.description}`,
          action: IStepDisplayActionType.NONE,
          actionValue: ""
        });

        break;
      case "EventMarkdownMetadataEntry":
        items.push({
          text: metadataEntry.label,
          actionText: "[Show Metadata]",
          action: IStepDisplayActionType.SHOW_IN_MODAL,
          actionValue: metadataEntry.mdStr
        });

        break;
    }
  }

  return items;
}

export function extractMetadataFromLogs(
  logs: RunMetadataProviderMessageFragment[]
): IRunMetadataDict {
  const metadata: IRunMetadataDict = {
    firstLogAt: 0,
    mostRecentLogAt: 0,
    globalMarkers: [],
    steps: {}
  };

  // Returns the most recent marker with the given `key` without an end time
  const upsertMarker = (set: IMarker[], key: string) => {
    let marker = set.find(f => f.key === key && !f.end);
    if (!marker) {
      marker = { key };
      set.unshift(marker);
    }
    return marker;
  };

  const upsertState = (
    step: IStepMetadata,
    time: number,
    state: IStepState
  ) => {
    step.transitions.push({ time, state });
    step.transitions = step.transitions.sort((a, b) => a.time - b.time);
    step.state = state;
  };

  logs.forEach(log => {
    const timestamp = Number.parseInt(log.timestamp, 10);

    metadata.firstLogAt = metadata.firstLogAt
      ? Math.min(metadata.firstLogAt, timestamp)
      : timestamp;
    metadata.mostRecentLogAt = Math.max(metadata.mostRecentLogAt, timestamp);

    if (log.__typename === "PipelineStartEvent") {
      metadata.startedPipelineAt = timestamp;
    }
    if (log.__typename === "PipelineInitFailureEvent") {
      metadata.initFailed = true;
      metadata.exitedAt = timestamp;
    }
    if (
      log.__typename === "PipelineFailureEvent" ||
      log.__typename === "PipelineSuccessEvent"
    ) {
      metadata.exitedAt = timestamp;
    }

    if (log.__typename === "EngineEvent" && !log.step) {
      if (log.markerStart) {
        upsertMarker(metadata.globalMarkers, log.markerStart).start = timestamp;
      }
      if (log.markerEnd) {
        upsertMarker(metadata.globalMarkers, log.markerEnd).end = timestamp;
      }
    }

    if (log.step) {
      const stepKey = log.step.key;
      const step =
        metadata.steps[stepKey] ||
        ({
          state: IStepState.PREPARING,
          transitions: [
            {
              state: IStepState.PREPARING,
              time: timestamp
            }
          ],
          start: undefined,
          end: undefined,
          markers: [],
          retries: [],
          expectationResults: [],
          materializations: []
        } as IStepMetadata);

      if (log.__typename === "ExecutionStepStartEvent") {
        upsertState(step, timestamp, IStepState.RUNNING);
        step.start = timestamp;
      } else if (log.__typename === "ExecutionStepSuccessEvent") {
        upsertState(step, timestamp, IStepState.SUCCEEDED);
        step.end = Math.max(timestamp, step.end || 0);
      } else if (log.__typename === "ExecutionStepSkippedEvent") {
        upsertState(step, timestamp, IStepState.SKIPPED);
      } else if (log.__typename === "ExecutionStepFailureEvent") {
        upsertState(step, timestamp, IStepState.FAILED);
        step.end = Math.max(timestamp, step.end || 0);
      } else if (log.__typename === "ExecutionStepUpForRetryEvent") {
        upsertState(step, timestamp, IStepState.PREPARING);
      } else if (log.__typename === "ExecutionStepRestartEvent") {
        upsertState(step, timestamp, IStepState.RUNNING);
      } else if (log.__typename === "StepMaterializationEvent") {
        step.materializations.push({
          icon: IStepDisplayIconType.LINK,
          text: log.materialization.label || "Materialization",
          items: itemsForMetadataEntries(log.materialization.metadataEntries)
        });
      } else if (log.__typename === "StepExpectationResultEvent") {
        step.expectationResults.push({
          status: log.expectationResult.success
            ? IExpectationResultStatus.PASSED
            : IExpectationResultStatus.FAILED,
          icon: log.expectationResult.success
            ? IStepDisplayIconType.SUCCESS
            : IStepDisplayIconType.FAILURE,
          text: log.expectationResult.label,
          items: itemsForMetadataEntries(log.expectationResult.metadataEntries)
        });
      } else if (log.__typename === "EngineEvent") {
        if (log.markerStart) {
          upsertMarker(step.markers, log.markerStart).start = timestamp;
        }
        if (log.markerEnd) {
          upsertMarker(step.markers, log.markerEnd).end = timestamp;
        }
      }

      metadata.steps[stepKey] = step;
    }
  });

  return metadata;
}

interface IRunMetadataProviderProps {
  logs: RunMetadataProviderMessageFragment[];
  children: (metadata: IRunMetadataDict) => React.ReactElement<any>;
}

export class RunMetadataProvider extends React.Component<
  IRunMetadataProviderProps
> {
  static fragments = {
    RunMetadataProviderMessageFragment: gql`
      fragment TempMetadataEntryFragment on EventMetadataEntry {
        label
        description
        ... on EventPathMetadataEntry {
          path
        }
        ... on EventJsonMetadataEntry {
          jsonString
        }
        ... on EventUrlMetadataEntry {
          url
        }
        ... on EventTextMetadataEntry {
          text
        }
        ... on EventMarkdownMetadataEntry {
          mdStr
        }
        ... on EventPythonArtifactMetadataEntry {
          module
          name
        }
      }

      fragment RunMetadataProviderMessageFragment on PipelineRunEvent {
        __typename
        ... on MessageEvent {
          message
          timestamp
          step {
            key
          }
        }
        ... on EngineEvent {
          markerStart
          markerEnd
        }
        ... on StepMaterializationEvent {
          step {
            key
          }
          materialization {
            label
            description
            metadataEntries {
              ...TempMetadataEntryFragment
            }
          }
        }
        ... on StepExpectationResultEvent {
          expectationResult {
            success
            label
            description
            metadataEntries {
              ...TempMetadataEntryFragment
            }
          }
        }
      }
    `
  };

  render() {
    return this.props.children(extractMetadataFromLogs(this.props.logs));
  }
}
