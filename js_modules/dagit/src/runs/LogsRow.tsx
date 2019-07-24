import * as React from "react";
import gql from "graphql-tag";
import { LogLevel } from "../types/globalTypes";
import { Tag, Colors } from "@blueprintjs/core";

import {
  LogsRowStructuredFragment,
  LogsRowStructuredFragment_ExecutionStepFailureEvent,
  LogsRowStructuredFragment_PipelineProcessStartedEvent,
  LogsRowStructuredFragment_PipelineProcessStartEvent,
  LogsRowStructuredFragment_PipelineInitFailureEvent
} from "./types/LogsRowStructuredFragment";
import { LogsRowUnstructuredFragment } from "./types/LogsRowUnstructuredFragment";
import {
  Row,
  StructuredContent,
  EventTypeColumn,
  SolidColumn,
  TimestampColumn
} from "./LogsRowComponents";
import { MetadataEntries, MetadataEntry } from "./MetadataEntry";
import { assertUnreachable } from "../Util";
import { MetadataEntryFragment } from "./types/MetadataEntryFragment";

export class Structured extends React.Component<{
  node: LogsRowStructuredFragment;
}> {
  static fragments = {
    LogsRowStructuredFragment: gql`
      fragment LogsRowStructuredFragment on PipelineRunEvent {
        __typename
        ... on MessageEvent {
          message
          timestamp
          level
          step {
            key
          }
        }
        ... on PipelineProcessStartedEvent {
          processId
        }
        ... on PipelineProcessStartEvent {
          pipelineName
          runId
        }
        ... on StepMaterializationEvent {
          step {
            key
          }
          materialization {
            label
            description
            metadataEntries {
              ...MetadataEntryFragment
            }
          }
        }
        ... on PipelineInitFailureEvent {
          error {
            stack
            message
          }
        }
        ... on ExecutionStepFailureEvent {
          message
          level
          step {
            key
          }
          error {
            stack
            message
          }
        }
        ... on ExecutionStepInputEvent {
          inputName
          typeCheck {
            label
            description
            success
            metadataEntries {
              ...MetadataEntryFragment
            }
          }
        }
        ... on ExecutionStepOutputEvent {
          outputName
          typeCheck {
            label
            description
            success
            metadataEntries {
              ...MetadataEntryFragment
            }
          }
        }
        ... on StepExpectationResultEvent {
          expectationResult {
            success
            label
            description
            metadataEntries {
              ...MetadataEntryFragment
            }
          }
        }
      }
      ${MetadataEntry.fragments.MetadataEntryFragment}
    `
  };

  renderStructuredContent() {
    const { node } = this.props;

    switch (node.__typename) {
      // Errors
      case "ExecutionStepFailureEvent":
      case "PipelineInitFailureEvent":
        return <FailureContent node={node} />;

      // Special Rendering
      case "PipelineProcessStartedEvent":
        return <PipelineProcessStartedContent node={node} />;
      case "PipelineProcessStartEvent":
        return <PipelineProcessStartContent node={node} />;

      // Default Behavior
      case "ExecutionStepStartEvent":
        return <DefaultContent message={node.message} eventType="Step Start" />;
      case "ExecutionStepSkippedEvent":
        return <DefaultContent message={node.message} eventType="Skipped" />;
      case "ExecutionStepSuccessEvent":
        return (
          <DefaultContent message={node.message} eventType="Step Finished" />
        );
      case "ExecutionStepInputEvent":
        return (
          <DefaultContent
            message={node.message}
            eventType="Input"
            eventIntent={node.typeCheck.success ? "success" : "warning"}
            metadataEntries={node.typeCheck.metadataEntries}
          />
        );
      case "ExecutionStepOutputEvent":
        return (
          <DefaultContent
            message={node.message}
            eventType="Output"
            eventIntent={node.typeCheck.success ? "success" : "warning"}
            metadataEntries={node.typeCheck.metadataEntries}
          />
        );
      case "StepExpectationResultEvent":
        return (
          <DefaultContent
            message={node.message}
            eventType="Expectation"
            eventIntent={node.expectationResult.success ? "success" : "warning"}
            metadataEntries={node.expectationResult.metadataEntries}
          />
        );
      case "StepMaterializationEvent":
        return (
          <DefaultContent
            message={node.message}
            eventType="Materialization"
            metadataEntries={node.materialization.metadataEntries}
          />
        );
      case "PipelineFailureEvent":
      case "PipelineProcessStartEvent":
      case "PipelineSuccessEvent":
      case "LogMessageEvent":
      case "PipelineStartEvent":
        return <DefaultContent message={node.message} />;

      default:
        // This allows us to check that the switch is exhaustive because the union type should
        // have been narrowed following each successive case to `never` at this point.
        return assertUnreachable(node);
    }
  }

  render() {
    const { node } = this.props;
    return (
      <Row level={LogLevel.INFO}>
        <SolidColumn stepKey={"step" in node && node.step && node.step.key} />
        <StructuredContent>{this.renderStructuredContent()}</StructuredContent>
        <TimestampColumn time={"timestamp" in node && node.timestamp} />
      </Row>
    );
  }
}

export class Unstructured extends React.Component<{
  node: LogsRowUnstructuredFragment;
}> {
  static fragments = {
    LogsRowUnstructuredFragment: gql`
      fragment LogsRowUnstructuredFragment on PipelineRunEvent {
        __typename
        ... on MessageEvent {
          message
          timestamp
          level
          step {
            key
          }
        }
      }
    `
  };

  render() {
    const { node } = this.props;
    return (
      <Row level={node.level}>
        <SolidColumn stepKey={node.step && node.step.key} />
        <EventTypeColumn>{node.level}</EventTypeColumn>
        <span style={{ flex: 1 }}>{node.message}</span>
        <TimestampColumn time={node.timestamp} />
      </Row>
    );
  }
}

// Structured Content Renderers

const DefaultContent: React.FunctionComponent<{
  message: string;
  eventType?: string;
  eventIntent?: "success" | "danger" | "warning";
  metadataEntries?: MetadataEntryFragment[];
}> = ({ message, eventType, eventIntent, metadataEntries }) => (
  <>
    <EventTypeColumn>
      {eventType && (
        <Tag minimal={true} intent={eventIntent}>
          {eventType}
        </Tag>
      )}
    </EventTypeColumn>
    <span style={{ flex: 1 }}>
      {message}
      <br />
      {metadataEntries && <MetadataEntries entries={metadataEntries} />}
    </span>
  </>
);

const PipelineProcessStartedContent: React.FunctionComponent<{
  node: LogsRowStructuredFragment_PipelineProcessStartedEvent;
}> = ({ node }) => (
  <>
    <EventTypeColumn>
      <Tag minimal={true}>Started</Tag>
    </EventTypeColumn>
    <span style={{ flex: 1 }}>
      {`${node.message} `}
      <div style={{ color: Colors.GRAY3 }}>{`PID: ${node.processId}`}</div>
    </span>
  </>
);

const PipelineProcessStartContent: React.FunctionComponent<{
  node: LogsRowStructuredFragment_PipelineProcessStartEvent;
}> = ({ node }) => (
  <>
    <EventTypeColumn>
      <Tag minimal={true}>Starting</Tag>
    </EventTypeColumn>
    <span style={{ flex: 1 }}>
      {`${node.message} `}
      <div style={{ color: Colors.GRAY3 }}>
        {`Pipeline Name: ${node.pipelineName}, Run ID: ${node.runId}`}
      </div>
    </span>
  </>
);

const FailureContent: React.FunctionComponent<{
  node:
    | LogsRowStructuredFragment_ExecutionStepFailureEvent
    | LogsRowStructuredFragment_PipelineInitFailureEvent;
}> = ({ node }) => (
  <>
    <EventTypeColumn>
      <Tag minimal={true} intent="danger">
        Failed
      </Tag>
    </EventTypeColumn>
    <span style={{ flex: 1, color: Colors.RED3 }}>
      {`${node.error.message}\n${node.error.stack}`}
    </span>
  </>
);
