import { useState } from "react";
import { Check, Copy } from "lucide-react";

interface CodeBlockProps {
  code: string;
  language?: string;
}

export function CodeBlock({ code, language = "sql" }: CodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group my-2">
      <div className="flex items-center justify-between bg-[var(--background-secondary)] px-3 py-1 rounded-t border border-[var(--border)] border-b-0">
        <span className="text-xs text-[var(--muted)] uppercase">{language}</span>
        <button
          onClick={handleCopy}
          className="p-1 hover:bg-[var(--border)] rounded transition-colors"
          title="Copy code"
        >
          {copied ? (
            <Check className="w-4 h-4 text-[var(--success)]" />
          ) : (
            <Copy className="w-4 h-4 text-[var(--muted)]" />
          )}
        </button>
      </div>
      <pre className="bg-[var(--background-tertiary)] p-3 rounded-b border border-[var(--border)] overflow-x-auto">
        <code className="text-sm font-mono text-[var(--foreground)]">{code}</code>
      </pre>
    </div>
  );
}
