import { Component, type ErrorInfo, type ReactNode } from "react";

import type { ParsedBlock } from "../../bridge/types";
import { useI18n } from "../../i18n/I18nProvider";
import { ActionBlock } from "./ActionBlock";
import { AttachmentsBlock } from "./AttachmentsBlock";
import { ConclusionBlock } from "./ConclusionBlock";
import { EvidenceBlock } from "./EvidenceBlock";
import { HypothesisBlock } from "./HypothesisBlock";
import { MarkdownBlock } from "./MarkdownBlock";
import { ReasoningBlock } from "./ReasoningBlock";
import { StatusBlock } from "./StatusBlock";
import { ToolGroupBlock } from "./ToolGroupBlock";
import { InvalidBlock, UnknownBlock } from "./UnknownBlock";
import { WorkBlock } from "./WorkBlock";
import "./blocks.css";

function BlockCrash({ rawKind }: { rawKind: string }) {
  const { t } = useI18n();
  return (
    <div className="block-card block-card--warning" role="alert">
      <p className="note note--danger">{t("run.partial")}</p>
      <span className="chip mono">{rawKind}</span>
    </div>
  );
}

class BlockBoundary extends Component<
  { children: ReactNode; rawKind: string },
  { failed: boolean }
> {
  override state = { failed: false };

  static getDerivedStateFromError(): { failed: boolean } {
    return { failed: true };
  }

  override componentDidCatch(error: Error, info: ErrorInfo): void {
    // A renderer bug must never take down the whole conversation.
    console.error("projection block renderer failed", error, info.componentStack);
  }

  override render(): ReactNode {
    if (this.state.failed) {
      return <BlockCrash rawKind={this.props.rawKind} />;
    }
    return this.props.children;
  }
}

export interface BlockRendererProps {
  block: ParsedBlock;
  runId: string;
  scope: string;
  offline: boolean;
  onActionRecorded?: (actionID: string, decision: "approve" | "reject") => void;
}

function renderBlock({
  block,
  runId,
  runVersion,
  scope,
  offline,
  onActionRecorded,
}: BlockRendererProps & { runVersion: number }): ReactNode {
  switch (block.kind) {
    case "status":
      return <StatusBlock data={block.data} />;
    case "reasoning":
      return <ReasoningBlock data={block.data} />;
    case "markdown":
      return <MarkdownBlock data={block.data} />;
    case "tool-group":
      return <ToolGroupBlock data={block.data} runId={runId} />;
    case "evidence":
      return <EvidenceBlock data={block.data} runId={runId} />;
    case "hypothesis":
      return <HypothesisBlock data={block.data} />;
    case "conclusion":
      return <ConclusionBlock data={block.data} />;
    case "work":
      return <WorkBlock data={block.data} runId={runId} />;
    case "action":
      return (
        <ActionBlock
          data={block.data}
          runId={runId}
          runVersion={runVersion}
          scope={scope}
          offline={offline}
          {...(onActionRecorded === undefined ? {} : { onRecorded: onActionRecorded })}
        />
      );
    case "attachments":
      return <AttachmentsBlock data={block.data} runId={runId} />;
    case "unknown":
      return <UnknownBlock rawKind={block.rawKind} />;
    case "invalid":
      return <InvalidBlock rawKind={block.rawKind} reason={block.reason} />;
    default: {
      const exhaustive: never = block;
      return exhaustive;
    }
  }
}

/**
 * Renders one validated projection block. Unknown kinds degrade to a
 * read-only card and a renderer failure is isolated to that block.
 */
export function BlockRenderer(props: BlockRendererProps & { runVersion: number }) {
  const rawKind = props.block.kind === "unknown" || props.block.kind === "invalid"
    ? props.block.rawKind
    : props.block.kind;
  return (
    <div className="block-renderer" data-block-id={props.block.id}>
      <BlockBoundary rawKind={rawKind}>
        {renderBlock(props)}
      </BlockBoundary>
    </div>
  );
}
