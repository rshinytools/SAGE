import { useState } from "react";
import {
  User,
  Bot,
  ChevronDown,
  ChevronUp,
  Database,
  Clock,
  Gauge,
} from "lucide-react";
import { CodeBlock } from "./CodeBlock";
import { TypingIndicator } from "./TypingIndicator";
import type { ChatMessage } from "@/types/chat";

interface MessageBubbleProps {
  message: ChatMessage;
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const [showMetadata, setShowMetadata] = useState(false);
  const isUser = message.role === "user";
  const isStreaming = message.isStreaming && !message.content;

  // Simple markdown-like rendering for code blocks
  const renderContent = (content: string) => {
    const parts = content.split(/(```[\s\S]*?```)/g);
    return parts.map((part, index) => {
      if (part.startsWith("```") && part.endsWith("```")) {
        const match = part.match(/```(\w*)\n?([\s\S]*?)```/);
        if (match) {
          const [, language, code] = match;
          return <CodeBlock key={index} code={code.trim()} language={language || "sql"} />;
        }
      }
      // Convert inline code
      const inlineCodeParts = part.split(/(`[^`]+`)/g);
      return (
        <span key={index}>
          {inlineCodeParts.map((p, i) => {
            if (p.startsWith("`") && p.endsWith("`")) {
              return (
                <code
                  key={i}
                  className="bg-[var(--background-secondary)] px-1 py-0.5 rounded text-sm font-mono"
                >
                  {p.slice(1, -1)}
                </code>
              );
            }
            // Convert newlines to br
            return p.split("\n").map((line, li, arr) => (
              <span key={`${i}-${li}`}>
                {line}
                {li < arr.length - 1 && <br />}
              </span>
            ));
          })}
        </span>
      );
    });
  };

  const confidenceColor = (confidence?: number) => {
    if (!confidence) return "var(--muted)";
    if (confidence >= 90) return "var(--success)";
    if (confidence >= 70) return "var(--warning)";
    if (confidence >= 50) return "var(--warning)";
    return "var(--destructive)";
  };

  return (
    <div className={`flex gap-3 ${isUser ? "flex-row-reverse" : ""}`}>
      {/* Avatar */}
      <div
        className={`w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0 ${
          isUser
            ? "bg-[var(--primary)] text-white"
            : "bg-[var(--background-secondary)] text-[var(--primary)]"
        }`}
      >
        {isUser ? <User className="w-4 h-4" /> : <Bot className="w-4 h-4" />}
      </div>

      {/* Message Content */}
      <div className={`flex-1 max-w-[80%] ${isUser ? "text-right" : ""}`}>
        <div
          className={`inline-block p-3 rounded-lg ${
            isUser
              ? "bg-[var(--primary)] text-white rounded-tr-none"
              : "bg-[var(--background-secondary)] text-[var(--foreground)] rounded-tl-none"
          }`}
        >
          {isStreaming ? (
            <TypingIndicator />
          ) : (
            <div className="text-sm whitespace-pre-wrap text-left">
              {renderContent(message.content)}
            </div>
          )}
        </div>

        {/* Metadata Section */}
        {!isUser && message.metadata && !isStreaming && (
          <div className="mt-2">
            <button
              onClick={() => setShowMetadata(!showMetadata)}
              className="flex items-center gap-1 text-xs text-[var(--muted)] hover:text-[var(--foreground)] transition-colors"
            >
              {showMetadata ? (
                <ChevronUp className="w-3 h-3" />
              ) : (
                <ChevronDown className="w-3 h-3" />
              )}
              <span>Details</span>
              {message.metadata.confidence !== undefined && (
                <span
                  className="ml-2 px-1.5 py-0.5 rounded text-xs"
                  style={{
                    backgroundColor: `${confidenceColor(message.metadata.confidence)}20`,
                    color: confidenceColor(message.metadata.confidence),
                  }}
                >
                  {message.metadata.confidence}% confidence
                </span>
              )}
            </button>

            {showMetadata && (
              <div className="mt-2 p-3 bg-[var(--background-tertiary)] rounded border border-[var(--border)] text-left">
                <div className="grid grid-cols-2 gap-2 text-xs">
                  {message.metadata.model && (
                    <div className="flex items-center gap-1 text-[var(--muted)]">
                      <Bot className="w-3 h-3" />
                      <span>Model: {message.metadata.model}</span>
                    </div>
                  )}
                  {message.metadata.execution_time_ms !== undefined && (
                    <div className="flex items-center gap-1 text-[var(--muted)]">
                      <Clock className="w-3 h-3" />
                      <span>Time: {message.metadata.execution_time_ms}ms</span>
                    </div>
                  )}
                  {message.metadata.tokens !== undefined && (
                    <div className="flex items-center gap-1 text-[var(--muted)]">
                      <Gauge className="w-3 h-3" />
                      <span>Tokens: {message.metadata.tokens}</span>
                    </div>
                  )}
                </div>

                {message.metadata.sql_query && (
                  <div className="mt-2">
                    <div className="flex items-center gap-1 text-xs text-[var(--muted)] mb-1">
                      <Database className="w-3 h-3" />
                      <span>Generated SQL:</span>
                    </div>
                    <CodeBlock code={message.metadata.sql_query} language="sql" />
                  </div>
                )}

                {message.metadata.table_result && message.metadata.table_result.length > 0 && (
                  <div className="mt-2">
                    <div className="text-xs text-[var(--muted)] mb-1">
                      Query Results ({message.metadata.table_result.length} rows):
                    </div>
                    <div className="overflow-x-auto">
                      <table className="data-table text-xs">
                        <thead>
                          <tr>
                            {Object.keys(message.metadata.table_result[0]).map((key) => (
                              <th key={key}>{key}</th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {message.metadata.table_result.slice(0, 10).map((row, i) => (
                            <tr key={i}>
                              {Object.values(row).map((val, j) => (
                                <td key={j}>{String(val)}</td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                      {message.metadata.table_result.length > 10 && (
                        <div className="text-xs text-[var(--muted)] mt-1">
                          Showing 10 of {message.metadata.table_result.length} rows
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* Timestamp */}
        <div className={`text-xs text-[var(--muted)] mt-1 ${isUser ? "text-right" : ""}`}>
          {new Date(message.timestamp).toLocaleTimeString()}
        </div>
      </div>
    </div>
  );
}
