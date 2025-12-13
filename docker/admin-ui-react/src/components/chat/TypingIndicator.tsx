export function TypingIndicator() {
  return (
    <div className="flex items-center gap-1 px-3 py-2">
      <div className="flex items-center gap-1">
        <span
          className="w-2 h-2 bg-[var(--primary)] rounded-full animate-bounce"
          style={{ animationDelay: "0ms" }}
        />
        <span
          className="w-2 h-2 bg-[var(--primary)] rounded-full animate-bounce"
          style={{ animationDelay: "150ms" }}
        />
        <span
          className="w-2 h-2 bg-[var(--primary)] rounded-full animate-bounce"
          style={{ animationDelay: "300ms" }}
        />
      </div>
      <span className="text-sm text-[var(--muted)] ml-2">SAGE is thinking...</span>
    </div>
  );
}
