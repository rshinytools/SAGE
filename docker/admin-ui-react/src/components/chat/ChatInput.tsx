import { useState, useRef, useCallback } from "react";
import { Send, Paperclip, X, Square, Loader2, Mic } from "lucide-react";

interface ChatInputProps {
  onSend: (message: string, files?: File[]) => void;
  onCancel: () => void;
  isLoading: boolean;
  disabled?: boolean;
  initialMessage?: string;
}

export function ChatInput({ onSend, onCancel, isLoading, disabled, initialMessage }: ChatInputProps) {
  const [message, setMessage] = useState(initialMessage || "");
  const [files, setFiles] = useState<File[]>([]);
  const [isFocused, setIsFocused] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Update message when initialMessage changes
  if (initialMessage && message !== initialMessage) {
    setMessage(initialMessage);
  }

  const handleSubmit = useCallback(
    (e?: React.FormEvent) => {
      e?.preventDefault();
      if (message.trim() && !isLoading && !disabled) {
        onSend(message.trim(), files.length > 0 ? files : undefined);
        setMessage("");
        setFiles([]);
        // Reset textarea height
        if (textareaRef.current) {
          textareaRef.current.style.height = "auto";
        }
      }
    },
    [message, files, isLoading, disabled, onSend]
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit();
    }
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFiles = Array.from(e.target.files || []);
    setFiles((prev) => [...prev, ...selectedFiles]);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
  };

  const removeFile = (index: number) => {
    setFiles((prev) => prev.filter((_, i) => i !== index));
  };

  // Auto-resize textarea
  const handleInput = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setMessage(e.target.value);
    const textarea = textareaRef.current;
    if (textarea) {
      textarea.style.height = "auto";
      textarea.style.height = `${Math.min(textarea.scrollHeight, 200)}px`;
    }
  };

  return (
    <div className="border-t border-gray-200 dark:border-gray-800/50 bg-white dark:bg-gray-900 p-4">
      <div className="max-w-4xl mx-auto">
        {/* File previews */}
        {files.length > 0 && (
          <div className="flex flex-wrap gap-2 mb-3">
            {files.map((file, index) => (
              <div
                key={index}
                className="flex items-center gap-2 bg-gray-100 dark:bg-gray-800 px-3 py-1.5 rounded-lg text-sm border border-gray-200 dark:border-gray-700"
              >
                <Paperclip className="w-3 h-3 text-gray-500 dark:text-gray-400" />
                <span className="max-w-[150px] truncate text-gray-700 dark:text-gray-300">{file.name}</span>
                <button
                  onClick={() => removeFile(index)}
                  className="text-gray-400 dark:text-gray-500 hover:text-red-500 dark:hover:text-red-400 transition-colors"
                >
                  <X className="w-3 h-3" />
                </button>
              </div>
            ))}
          </div>
        )}

        <form onSubmit={handleSubmit}>
          <div className={`relative flex items-end gap-2 bg-gray-50 dark:bg-gray-800 rounded-2xl border transition-all duration-200 ${
            isFocused
              ? "border-blue-500/50 ring-2 ring-blue-500/20"
              : "border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600"
          }`}>
            {/* File upload button */}
            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              className="flex-shrink-0 p-3 text-gray-400 dark:text-gray-500 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
              title="Attach file"
              disabled={disabled}
            >
              <Paperclip className="w-5 h-5" />
            </button>
            <input
              ref={fileInputRef}
              type="file"
              multiple
              accept=".csv,.xlsx,.xls,.pdf,.txt,.json"
              onChange={handleFileSelect}
              className="hidden"
            />

            {/* Message input */}
            <textarea
              ref={textareaRef}
              value={message}
              onChange={handleInput}
              onKeyDown={handleKeyDown}
              onFocus={() => setIsFocused(true)}
              onBlur={() => setIsFocused(false)}
              placeholder="Ask a question about your clinical data..."
              className="flex-1 bg-transparent text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 resize-none min-h-[48px] max-h-[200px] py-3 pr-2 outline-none text-sm leading-relaxed"
              rows={1}
              disabled={disabled || isLoading}
            />

            {/* Voice button (placeholder) */}
            <button
              type="button"
              className="flex-shrink-0 p-3 text-gray-300 dark:text-gray-600 hover:text-gray-400 dark:hover:text-gray-400 transition-colors"
              title="Voice input (coming soon)"
              disabled
            >
              <Mic className="w-5 h-5" />
            </button>

            {/* Send/Cancel button */}
            {isLoading ? (
              <button
                type="button"
                onClick={onCancel}
                className="flex-shrink-0 m-2 w-10 h-10 flex items-center justify-center bg-red-500 hover:bg-red-400 text-white rounded-xl transition-all duration-200 shadow-lg shadow-red-500/20"
                title="Stop generating"
              >
                <Square className="w-4 h-4" />
              </button>
            ) : (
              <button
                type="submit"
                disabled={!message.trim() || disabled}
                className={`flex-shrink-0 m-2 w-10 h-10 flex items-center justify-center rounded-xl transition-all duration-200 ${
                  message.trim() && !disabled
                    ? "bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-400 hover:to-blue-500 text-white shadow-lg shadow-blue-500/20 hover:scale-105"
                    : "bg-gray-200 dark:bg-gray-700 text-gray-400 dark:text-gray-500 cursor-not-allowed"
                }`}
                title="Send message"
              >
                {disabled ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Send className="w-4 h-4" />
                )}
              </button>
            )}
          </div>
        </form>

        {/* Helper text */}
        <div className="flex items-center justify-center gap-4 mt-3 text-[11px] text-gray-400 dark:text-gray-600">
          <span>SAGE can make mistakes. Verify important data.</span>
        </div>
      </div>
    </div>
  );
}
