import { useEffect, useRef } from "react";
import { MessageBubble } from "./MessageBubble";
import { Sparkles, Database, Users, Activity, ArrowRight } from "lucide-react";
import type { ChatMessage } from "@/types/chat";

interface MessageListProps {
  messages: ChatMessage[];
  onSuggestionClick?: (suggestion: string) => void;
}

const suggestions = [
  {
    icon: Users,
    title: "Subject Count",
    query: "How many subjects are in the Safety Population?",
    color: "from-blue-500 to-cyan-500",
  },
  {
    icon: Activity,
    title: "Adverse Events",
    query: "Show me all adverse events with fever",
    color: "from-orange-500 to-red-500",
  },
  {
    icon: Database,
    title: "Medications",
    query: "Which subjects took Tylenol for headache?",
    color: "from-green-500 to-emerald-500",
  },
  {
    icon: Sparkles,
    title: "Treatment Arms",
    query: "Count subjects by treatment arm",
    color: "from-purple-500 to-pink-500",
  },
];

export function MessageList({ messages, onSuggestionClick }: MessageListProps) {
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  if (messages.length === 0) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center p-8 bg-gray-50 dark:bg-gray-900">
        {/* Logo and Title */}
        <div className="relative mb-8">
          <div className="absolute inset-0 bg-blue-500/20 blur-3xl rounded-full" />
          <div className="relative w-20 h-20 bg-gradient-to-br from-blue-500 to-blue-600 rounded-2xl flex items-center justify-center shadow-2xl shadow-blue-500/30">
            <Sparkles className="w-10 h-10 text-white" />
          </div>
        </div>

        <h2 className="text-2xl font-bold text-gray-900 dark:text-white mb-2">
          Welcome to SAGE
        </h2>
        <p className="text-gray-500 dark:text-gray-400 max-w-md text-center mb-8">
          Study Analytics Generative Engine — Ask questions about your clinical trial data in plain English
        </p>

        {/* Suggestion Cards */}
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 max-w-2xl w-full">
          {suggestions.map((suggestion, i) => (
            <button
              key={i}
              onClick={() => onSuggestionClick?.(suggestion.query)}
              className="group relative text-left p-4 bg-white dark:bg-gray-800/50 hover:bg-gray-50 dark:hover:bg-gray-800 border border-gray-200 dark:border-gray-700/50 hover:border-gray-300 dark:hover:border-gray-600 rounded-xl transition-all duration-300 hover:scale-[1.02] hover:shadow-lg shadow-sm"
            >
              <div className="flex items-start gap-3">
                <div className={`w-10 h-10 rounded-lg bg-gradient-to-br ${suggestion.color} flex items-center justify-center shadow-lg flex-shrink-0`}>
                  <suggestion.icon className="w-5 h-5 text-white" />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="text-xs font-medium text-gray-500 dark:text-gray-400 mb-1">
                    {suggestion.title}
                  </div>
                  <div className="text-sm text-gray-700 dark:text-gray-200 leading-snug">
                    {suggestion.query}
                  </div>
                </div>
                <ArrowRight className="w-4 h-4 text-gray-400 dark:text-gray-600 group-hover:text-gray-600 dark:group-hover:text-gray-400 group-hover:translate-x-1 transition-all flex-shrink-0 mt-1" />
              </div>
            </button>
          ))}
        </div>

        {/* Keyboard hint */}
        <div className="mt-8 flex items-center gap-2 text-gray-400 dark:text-gray-600 text-xs">
          <kbd className="px-2 py-1 bg-gray-100 dark:bg-gray-800 rounded border border-gray-200 dark:border-gray-700 font-mono text-gray-600 dark:text-gray-400">Enter</kbd>
          <span>to send</span>
          <span className="mx-2">•</span>
          <kbd className="px-2 py-1 bg-gray-100 dark:bg-gray-800 rounded border border-gray-200 dark:border-gray-700 font-mono text-gray-600 dark:text-gray-400">Shift + Enter</kbd>
          <span>for new line</span>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-y-auto bg-gray-50 dark:bg-gray-900">
      <div className="max-w-4xl mx-auto p-6 space-y-6">
        {messages.map((message) => (
          <MessageBubble key={message.id} message={message} />
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
