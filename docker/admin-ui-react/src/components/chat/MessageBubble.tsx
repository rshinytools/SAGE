import { useState } from "react";
import {
  User,
  Bot,
  ChevronDown,
  ChevronUp,
  Database,
  Clock,
  Gauge,
  FileCode,
  Download,
  AlertTriangle,
  CheckCircle,
  Info,
  BookOpen,
  Table,
} from "lucide-react";
import { CodeBlock } from "./CodeBlock";
import { TypingIndicator } from "./TypingIndicator";
import type { ChatMessage } from "@/types/chat";

interface MessageBubbleProps {
  message: ChatMessage;
}

export function MessageBubble({ message }: MessageBubbleProps) {
  const [showMetadata, setShowMetadata] = useState(false);
  const [showSQL, setShowSQL] = useState(false);
  const [showMethodology, setShowMethodology] = useState(false);
  const [showResults, setShowResults] = useState(true);
  const isUser = message.role === "user";
  const isStreaming = message.isStreaming && !message.content;

  // Check if this is a pipeline response
  const isPipelineResponse = message.metadata?.pipeline === true;

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

  const confidenceColor = (confidence?: number | string) => {
    // Handle both numeric confidence and level strings
    if (typeof confidence === "string") {
      switch (confidence.toLowerCase()) {
        case "high": return "#10b981"; // green
        case "medium": return "#f59e0b"; // yellow
        case "low": return "#f97316"; // orange
        case "very_low": return "#ef4444"; // red
        default: return "var(--muted)";
      }
    }
    if (typeof confidence === "number") {
      if (confidence >= 90) return "#10b981"; // green
      if (confidence >= 70) return "#f59e0b"; // yellow
      if (confidence >= 50) return "#f97316"; // orange
      return "#ef4444"; // red
    }
    return "var(--muted)";
  };

  const getConfidenceLabel = (confidence: any) => {
    if (typeof confidence === "object") {
      const score = confidence.score || 0;
      const level = confidence.level || "unknown";
      return { score, level, color: confidence.color || confidenceColor(level) };
    }
    if (typeof confidence === "number") {
      if (confidence >= 90) return { score: confidence, level: "high", color: "#10b981" };
      if (confidence >= 70) return { score: confidence, level: "medium", color: "#f59e0b" };
      if (confidence >= 50) return { score: confidence, level: "low", color: "#f97316" };
      return { score: confidence, level: "very_low", color: "#ef4444" };
    }
    return { score: 0, level: "unknown", color: "var(--muted)" };
  };

  const handleExportCSV = () => {
    const data = message.metadata?.table_result || message.metadata?.data;
    if (!data || data.length === 0) return;

    const headers = Object.keys(data[0]);
    const csv = [
      headers.join(","),
      ...data.map((row: any) => headers.map(h => JSON.stringify(row[h] ?? "")).join(","))
    ].join("\n");

    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `query-results-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
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
          <div className="mt-3 space-y-2">
            {/* Confidence Badge */}
            {(message.metadata.confidence !== undefined || isPipelineResponse) && (
              <div className="flex items-center gap-2 flex-wrap">
                {(() => {
                  const conf = getConfidenceLabel(message.metadata.confidence);
                  return (
                    <span
                      className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs font-medium"
                      style={{
                        backgroundColor: `${conf.color}20`,
                        color: conf.color,
                      }}
                    >
                      {conf.level === "high" && <CheckCircle className="w-3 h-3" />}
                      {conf.level === "medium" && <Info className="w-3 h-3" />}
                      {(conf.level === "low" || conf.level === "very_low") && <AlertTriangle className="w-3 h-3" />}
                      {conf.score}% {conf.level}
                    </span>
                  );
                })()}

                {message.metadata.execution_time_ms !== undefined && (
                  <span className="inline-flex items-center gap-1 px-2 py-1 rounded-full text-xs bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400">
                    <Clock className="w-3 h-3" />
                    {message.metadata.execution_time_ms.toFixed(0)}ms
                  </span>
                )}
              </div>
            )}

            {/* Action Buttons */}
            <div className="flex items-center gap-2 flex-wrap">
              {/* SQL Toggle */}
              {(message.metadata.sql || message.metadata.sql_query) && (
                <button
                  onClick={() => setShowSQL(!showSQL)}
                  className="inline-flex items-center gap-1 px-2 py-1 text-xs bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 hover:bg-blue-100 dark:hover:bg-blue-900/40 rounded transition-colors"
                >
                  <FileCode className="w-3 h-3" />
                  {showSQL ? "Hide SQL" : "Show SQL"}
                </button>
              )}

              {/* Methodology Toggle */}
              {message.metadata.methodology && (
                <button
                  onClick={() => setShowMethodology(!showMethodology)}
                  className="inline-flex items-center gap-1 px-2 py-1 text-xs bg-purple-50 dark:bg-purple-900/20 text-purple-600 dark:text-purple-400 hover:bg-purple-100 dark:hover:bg-purple-900/40 rounded transition-colors"
                >
                  <BookOpen className="w-3 h-3" />
                  {showMethodology ? "Hide Methodology" : "Show Methodology"}
                </button>
              )}

              {/* Results Toggle */}
              {(message.metadata.table_result?.length > 0 || message.metadata.data?.length > 0) && (
                <button
                  onClick={() => setShowResults(!showResults)}
                  className="inline-flex items-center gap-1 px-2 py-1 text-xs bg-green-50 dark:bg-green-900/20 text-green-600 dark:text-green-400 hover:bg-green-100 dark:hover:bg-green-900/40 rounded transition-colors"
                >
                  <Table className="w-3 h-3" />
                  {showResults ? "Hide Results" : "Show Results"}
                </button>
              )}

              {/* Export CSV */}
              {(message.metadata.table_result?.length > 0 || message.metadata.data?.length > 0) && (
                <button
                  onClick={handleExportCSV}
                  className="inline-flex items-center gap-1 px-2 py-1 text-xs bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors"
                >
                  <Download className="w-3 h-3" />
                  Export CSV
                </button>
              )}

              {/* Details Toggle */}
              <button
                onClick={() => setShowMetadata(!showMetadata)}
                className="inline-flex items-center gap-1 px-2 py-1 text-xs bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded transition-colors"
              >
                {showMetadata ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
                Details
              </button>
            </div>

            {/* SQL Display */}
            {showSQL && (message.metadata.sql || message.metadata.sql_query) && (
              <div className="p-3 bg-gray-50 dark:bg-gray-800/50 rounded border border-gray-200 dark:border-gray-700">
                <div className="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400 mb-2">
                  <Database className="w-3 h-3" />
                  <span>Generated SQL Query</span>
                </div>
                <CodeBlock code={message.metadata.sql || message.metadata.sql_query} language="sql" />
              </div>
            )}

            {/* Methodology Display */}
            {showMethodology && message.metadata.methodology && (
              <div className="p-3 bg-purple-50 dark:bg-purple-900/10 rounded border border-purple-200 dark:border-purple-800">
                <div className="flex items-center gap-1 text-xs text-purple-600 dark:text-purple-400 mb-2">
                  <BookOpen className="w-3 h-3" />
                  <span>Query Methodology</span>
                </div>
                <div className="text-xs space-y-1 text-gray-700 dark:text-gray-300">
                  {message.metadata.methodology.table_used && (
                    <div><strong>Table:</strong> {message.metadata.methodology.table_used}</div>
                  )}
                  {message.metadata.methodology.population_used && (
                    <div><strong>Population:</strong> {message.metadata.methodology.population_used}</div>
                  )}
                  {message.metadata.methodology.population_filter && (
                    <div><strong>Filter:</strong> <code className="bg-gray-100 dark:bg-gray-800 px-1 rounded">{message.metadata.methodology.population_filter}</code></div>
                  )}
                  {message.metadata.methodology.columns_used?.length > 0 && (
                    <div><strong>Columns:</strong> {message.metadata.methodology.columns_used.slice(0, 5).join(", ")}{message.metadata.methodology.columns_used.length > 5 ? "..." : ""}</div>
                  )}
                  {message.metadata.methodology.assumptions?.length > 0 && (
                    <div className="mt-2">
                      <strong>Assumptions:</strong>
                      <ul className="list-disc list-inside ml-2">
                        {message.metadata.methodology.assumptions.map((a: string, i: number) => (
                          <li key={i}>{a}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Results Table */}
            {showResults && (message.metadata.table_result?.length > 0 || message.metadata.data?.length > 0) && (
              <div className="p-3 bg-gray-50 dark:bg-gray-800/50 rounded border border-gray-200 dark:border-gray-700">
                {(() => {
                  const data = message.metadata.table_result || message.metadata.data;
                  return (
                    <>
                      <div className="flex items-center justify-between mb-2">
                        <span className="text-xs text-gray-500 dark:text-gray-400">
                          Query Results ({data.length} rows)
                        </span>
                      </div>
                      <div className="overflow-x-auto max-h-64 overflow-y-auto">
                        <table className="data-table text-xs w-full">
                          <thead className="sticky top-0 bg-gray-100 dark:bg-gray-800">
                            <tr>
                              {Object.keys(data[0]).map((key) => (
                                <th key={key} className="px-2 py-1 text-left font-medium text-gray-700 dark:text-gray-300 border-b border-gray-200 dark:border-gray-700">{key}</th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {data.slice(0, 20).map((row: any, i: number) => (
                              <tr key={i} className="hover:bg-gray-100 dark:hover:bg-gray-700/50">
                                {Object.values(row).map((val, j) => (
                                  <td key={j} className="px-2 py-1 border-b border-gray-100 dark:border-gray-800">{String(val)}</td>
                                ))}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                        {data.length > 20 && (
                          <div className="text-xs text-gray-500 dark:text-gray-400 mt-2 text-center">
                            Showing 20 of {data.length} rows. Export to see all.
                          </div>
                        )}
                      </div>
                    </>
                  );
                })()}
              </div>
            )}

            {/* Detailed Metadata (collapsed by default) */}
            {showMetadata && (
              <div className="p-3 bg-gray-50 dark:bg-gray-800/50 rounded border border-gray-200 dark:border-gray-700 text-left">
                <div className="grid grid-cols-2 gap-2 text-xs">
                  {message.metadata.model && (
                    <div className="flex items-center gap-1 text-gray-500 dark:text-gray-400">
                      <Bot className="w-3 h-3" />
                      <span>Model: {message.metadata.model}</span>
                    </div>
                  )}
                  {message.metadata.execution_time_ms !== undefined && (
                    <div className="flex items-center gap-1 text-gray-500 dark:text-gray-400">
                      <Clock className="w-3 h-3" />
                      <span>Time: {message.metadata.execution_time_ms.toFixed(0)}ms</span>
                    </div>
                  )}
                  {message.metadata.tokens !== undefined && (
                    <div className="flex items-center gap-1 text-gray-500 dark:text-gray-400">
                      <Gauge className="w-3 h-3" />
                      <span>Tokens: {message.metadata.tokens}</span>
                    </div>
                  )}
                  {isPipelineResponse && (
                    <div className="flex items-center gap-1 text-gray-500 dark:text-gray-400">
                      <CheckCircle className="w-3 h-3 text-green-500" />
                      <span>Pipeline: Active</span>
                    </div>
                  )}
                </div>

                {/* Warnings */}
                {message.metadata.warnings?.length > 0 && (
                  <div className="mt-2 p-2 bg-yellow-50 dark:bg-yellow-900/20 rounded border border-yellow-200 dark:border-yellow-800">
                    <div className="flex items-center gap-1 text-xs text-yellow-600 dark:text-yellow-400 mb-1">
                      <AlertTriangle className="w-3 h-3" />
                      <span>Warnings</span>
                    </div>
                    <ul className="text-xs text-yellow-700 dark:text-yellow-300 list-disc list-inside">
                      {message.metadata.warnings.map((w: string, i: number) => (
                        <li key={i}>{w}</li>
                      ))}
                    </ul>
                  </div>
                )}

                {/* Confidence Explanation */}
                {message.metadata.confidence?.explanation && (
                  <div className="mt-2 text-xs text-gray-600 dark:text-gray-400">
                    <strong>Confidence Explanation:</strong> {message.metadata.confidence.explanation}
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
