import type { MarkdownBlockData } from "../../bridge/types";
import { SafeMarkdown } from "../../components/ui/SafeMarkdown";
import "./blocks.css";

export function MarkdownBlock({ data }: { data: MarkdownBlockData }) {
  return (
    <div className="markdown-block block-card">
      <SafeMarkdown text={data.markdown} />
    </div>
  );
}
