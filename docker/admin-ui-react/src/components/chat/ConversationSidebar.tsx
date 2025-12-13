import { useState } from "react";
import { Plus, MessageSquare, Trash2, Edit2, Check, X, PanelLeftClose, PanelLeft } from "lucide-react";
import type { Conversation } from "@/types/chat";

interface ConversationSidebarProps {
  conversations: Conversation[];
  currentId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
  onDelete: (id: string) => void;
  onRename: (id: string, title: string) => void;
  isCollapsed: boolean;
  onToggleCollapse: () => void;
}

export function ConversationSidebar({
  conversations,
  currentId,
  onSelect,
  onNew,
  onDelete,
  onRename,
  isCollapsed,
  onToggleCollapse,
}: ConversationSidebarProps) {
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editTitle, setEditTitle] = useState("");

  const startEditing = (conv: Conversation) => {
    setEditingId(conv.id);
    setEditTitle(conv.title);
  };

  const saveEdit = () => {
    if (editingId && editTitle.trim()) {
      onRename(editingId, editTitle.trim());
    }
    setEditingId(null);
    setEditTitle("");
  };

  const cancelEdit = () => {
    setEditingId(null);
    setEditTitle("");
  };

  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    const now = new Date();
    const diffDays = Math.floor((now.getTime() - date.getTime()) / (1000 * 60 * 60 * 24));

    if (diffDays === 0) return "Today";
    if (diffDays === 1) return "Yesterday";
    if (diffDays < 7) return `${diffDays}d ago`;
    return date.toLocaleDateString();
  };

  // Collapsed sidebar
  if (isCollapsed) {
    return (
      <div className="w-[52px] bg-gray-900/50 dark:bg-gray-950 border-r border-gray-200 dark:border-gray-800/50 flex flex-col h-full">
        {/* Toggle button */}
        <div className="p-2">
          <button
            onClick={onToggleCollapse}
            className="w-9 h-9 flex items-center justify-center text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-white/5 rounded-lg transition-all duration-200"
            title="Expand sidebar"
          >
            <PanelLeft className="w-4 h-4" />
          </button>
        </div>

        {/* New Chat Icon */}
        <div className="px-2 pb-2">
          <button
            onClick={onNew}
            className="w-9 h-9 flex items-center justify-center bg-gradient-to-br from-blue-500 to-blue-600 hover:from-blue-400 hover:to-blue-500 text-white rounded-lg transition-all duration-200 shadow-lg shadow-blue-500/20 hover:shadow-blue-500/30 hover:scale-105"
            title="New Chat"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        {/* Conversation icons */}
        <div className="flex-1 overflow-y-auto px-2 space-y-1">
          {conversations.map((conv) => (
            <button
              key={conv.id}
              onClick={() => onSelect(conv.id)}
              className={`w-9 h-9 flex items-center justify-center rounded-lg transition-all duration-200 ${
                currentId === conv.id
                  ? "bg-blue-500/20 text-blue-400 ring-1 ring-blue-500/30"
                  : "text-gray-500 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-100 dark:hover:bg-white/5"
              }`}
              title={conv.title}
            >
              <MessageSquare className="w-4 h-4" />
            </button>
          ))}
        </div>
      </div>
    );
  }

  // Expanded sidebar
  return (
    <div className="w-64 bg-gray-50 dark:bg-gray-900/50 border-r border-gray-200 dark:border-gray-800/50 flex flex-col h-full">
      {/* Header with collapse button */}
      <div className="p-3 flex items-center justify-between">
        <button
          onClick={onToggleCollapse}
          className="w-9 h-9 flex items-center justify-center text-gray-500 hover:text-gray-900 dark:text-gray-400 dark:hover:text-white hover:bg-gray-100 dark:hover:bg-white/5 rounded-lg transition-all duration-200"
          title="Collapse sidebar"
        >
          <PanelLeftClose className="w-4 h-4" />
        </button>
        <button
          onClick={onNew}
          className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-blue-500 to-blue-600 hover:from-blue-400 hover:to-blue-500 text-white text-sm font-medium rounded-lg transition-all duration-200 shadow-md shadow-blue-500/20 hover:shadow-blue-500/30"
        >
          <Plus className="w-4 h-4" />
          New Chat
        </button>
      </div>

      {/* Conversation List */}
      <div className="flex-1 overflow-y-auto px-2 pb-4">
        {conversations.length === 0 ? (
          <div className="p-4 text-center">
            <div className="w-12 h-12 mx-auto mb-3 rounded-full bg-gray-100 dark:bg-gray-800/50 flex items-center justify-center">
              <MessageSquare className="w-5 h-5 text-gray-400 dark:text-gray-600" />
            </div>
            <p className="text-gray-500 dark:text-gray-600 text-xs">No conversations yet</p>
            <p className="text-gray-400 dark:text-gray-700 text-[10px] mt-1">Start a new chat above</p>
          </div>
        ) : (
          <div className="space-y-1">
            {conversations.map((conv) => (
              <div
                key={conv.id}
                className={`group relative rounded-lg transition-all duration-200 ${
                  currentId === conv.id
                    ? "bg-blue-50 dark:bg-blue-500/10 ring-1 ring-blue-200 dark:ring-blue-500/20"
                    : "hover:bg-gray-100 dark:hover:bg-white/5"
                }`}
              >
                {editingId === conv.id ? (
                  <div className="p-2 flex items-center gap-2">
                    <input
                      type="text"
                      value={editTitle}
                      onChange={(e) => setEditTitle(e.target.value)}
                      className="flex-1 text-sm px-3 py-2 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-white border border-gray-300 dark:border-gray-700 focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20 outline-none transition-all"
                      autoFocus
                      onKeyDown={(e) => {
                        if (e.key === "Enter") saveEdit();
                        if (e.key === "Escape") cancelEdit();
                      }}
                    />
                    <button
                      onClick={saveEdit}
                      className="p-2 text-green-500 hover:bg-green-500/10 rounded-lg transition-colors"
                    >
                      <Check className="w-4 h-4" />
                    </button>
                    <button
                      onClick={cancelEdit}
                      className="p-2 text-red-500 hover:bg-red-500/10 rounded-lg transition-colors"
                    >
                      <X className="w-4 h-4" />
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={() => onSelect(conv.id)}
                    className="w-full text-left p-2.5 flex items-center gap-3"
                  >
                    <div className={`w-8 h-8 rounded-lg flex items-center justify-center flex-shrink-0 ${
                      currentId === conv.id
                        ? "bg-blue-100 dark:bg-blue-500/20 text-blue-500 dark:text-blue-400"
                        : "bg-gray-100 dark:bg-gray-800/50 text-gray-400 dark:text-gray-500"
                    }`}>
                      <MessageSquare className="w-4 h-4" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className={`text-sm font-medium truncate ${
                        currentId === conv.id
                          ? "text-blue-700 dark:text-white"
                          : "text-gray-700 dark:text-gray-300"
                      }`}>
                        {conv.title}
                      </div>
                      <div className="flex items-center gap-1.5 mt-0.5">
                        <span className={`text-[10px] ${
                          currentId === conv.id
                            ? "text-blue-500/70 dark:text-blue-400/70"
                            : "text-gray-400 dark:text-gray-600"
                        }`}>
                          {formatDate(conv.updated_at)}
                        </span>
                        <span className="text-gray-300 dark:text-gray-700 text-[10px]">•</span>
                        <span className={`text-[10px] ${
                          currentId === conv.id
                            ? "text-blue-500/70 dark:text-blue-400/70"
                            : "text-gray-400 dark:text-gray-600"
                        }`}>
                          {conv.message_count} msgs
                        </span>
                      </div>
                    </div>
                  </button>
                )}

                {/* Action buttons */}
                {editingId !== conv.id && (
                  <div className="absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        startEditing(conv);
                      }}
                      className="p-1.5 text-gray-400 hover:text-gray-700 dark:hover:text-white hover:bg-gray-200 dark:hover:bg-white/10 rounded-md transition-all"
                      title="Rename"
                    >
                      <Edit2 className="w-3 h-3" />
                    </button>
                    <button
                      onClick={(e) => {
                        e.stopPropagation();
                        if (confirm("Delete this conversation?")) {
                          onDelete(conv.id);
                        }
                      }}
                      className="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-500/10 rounded-md transition-all"
                      title="Delete"
                    >
                      <Trash2 className="w-3 h-3" />
                    </button>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
