import { useCallback, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { MessageList } from "./MessageList";
import { ChatInput } from "./ChatInput";
import { ConversationSidebar } from "./ConversationSidebar";
import { chatApi } from "@/api/chat";
import { useChatStore } from "@/stores/chatStore";
import { useToast } from "@/components/common";
import type { MessageMetadata } from "@/types/chat";

export function ChatContainer() {
  const queryClient = useQueryClient();
  const toast = useToast();
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [initialMessage, setInitialMessage] = useState("");

  const {
    currentConversationId,
    conversations,
    messages,
    isStreaming,
    setCurrentConversation,
    setConversations,
    removeConversation,
    updateConversation,
    setMessages,
    addMessage,
    startStreaming,
    appendStreamingContent,
    finishStreaming,
    cancelStreaming,
    clearAll,
  } = useChatStore();

  // Fetch conversations
  useQuery({
    queryKey: ["conversations"],
    queryFn: async () => {
      const data = await chatApi.getConversations();
      setConversations(data);
      return data;
    },
  });

  // Fetch messages when conversation changes
  useQuery({
    queryKey: ["conversation", currentConversationId],
    queryFn: async () => {
      if (!currentConversationId) return null;
      try {
        const data = await chatApi.getConversation(currentConversationId);
        setMessages(data.messages || []);
        return data;
      } catch {
        // Conversation doesn't exist (server restarted), clear it
        setCurrentConversation(null);
        setMessages([]);
        return null;
      }
    },
    enabled: !!currentConversationId,
  });

  // Delete conversation mutation
  const deleteConversationMutation = useMutation({
    mutationFn: chatApi.deleteConversation,
    onSuccess: (_, id) => {
      removeConversation(id);
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
      toast.success("Conversation deleted");
    },
    onError: () => {
      toast.error("Failed to delete conversation");
    },
  });

  // Clear all conversations mutation
  const clearAllMutation = useMutation({
    mutationFn: chatApi.deleteAllConversations,
    onSuccess: (result) => {
      clearAll();
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
      toast.success(
        "All conversations cleared",
        `${result.deleted_count} conversation${result.deleted_count === 1 ? '' : 's'} deleted`
      );
    },
    onError: () => {
      toast.error("Failed to clear conversations");
    },
  });

  // Rename conversation mutation
  const renameConversationMutation = useMutation({
    mutationFn: ({ id, title }: { id: string; title: string }) =>
      chatApi.updateConversationTitle(id, title),
    onSuccess: (data) => {
      updateConversation(data.id, { title: data.title });
      queryClient.invalidateQueries({ queryKey: ["conversations"] });
    },
  });

  // Send message
  const handleSend = useCallback(
    async (content: string, _files?: File[]) => {
      // Add user message
      const userMessage = {
        id: `user-${Date.now()}`,
        role: "user" as const,
        content,
        timestamp: new Date().toISOString(),
      };
      addMessage(userMessage);

      // Start streaming
      const controller = startStreaming();

      // Only send conversation_id if it exists in our conversations list
      // (prevents using stale IDs from localStorage after server restart)
      const validConversationId = currentConversationId &&
        conversations.some(c => c.id === currentConversationId)
        ? currentConversationId
        : undefined;

      // Send message with streaming
      chatApi.sendMessageStream(
        {
          message: content,
          conversation_id: validConversationId,
        },
        // On chunk
        (chunk) => {
          appendStreamingContent(chunk);
        },
        // On metadata
        (metadata) => {
          finishStreaming(metadata as MessageMetadata);
        },
        // On done
        (response) => {
          if (!currentConversationId) {
            setCurrentConversation(response.conversation_id);
            queryClient.invalidateQueries({ queryKey: ["conversations"] });
          }
        },
        // On error
        (error) => {
          console.error("Chat error:", error);
          finishStreaming();
          addMessage({
            id: `error-${Date.now()}`,
            role: "assistant",
            content: `Error: ${error}`,
            timestamp: new Date().toISOString(),
          });
        },
        controller.signal
      );
    },
    [
      currentConversationId,
      conversations,
      addMessage,
      startStreaming,
      appendStreamingContent,
      finishStreaming,
      setCurrentConversation,
      queryClient,
    ]
  );

  const handleNewChat = () => {
    setCurrentConversation(null);
    setMessages([]);
  };

  const handleSelectConversation = (id: string) => {
    setCurrentConversation(id);
  };

  const handleDeleteConversation = (id: string) => {
    deleteConversationMutation.mutate(id);
  };

  const handleRenameConversation = (id: string, title: string) => {
    renameConversationMutation.mutate({ id, title });
  };

  const handleClearAll = () => {
    clearAllMutation.mutate();
  };

  const handleToggleSidebar = () => {
    setIsSidebarCollapsed(!isSidebarCollapsed);
  };

  const handleSuggestionClick = (suggestion: string) => {
    setInitialMessage(suggestion);
  };

  // Clear initialMessage after ChatInput has consumed it
  const handleInitialMessageConsumed = useCallback(() => {
    setInitialMessage("");
  }, []);

  // Handle option click from clarification messages - send directly
  const handleOptionClick = useCallback(
    (option: string) => {
      handleSend(option);
    },
    [handleSend]
  );

  return (
    <div className="flex h-full">
      {/* Sidebar */}
      <ConversationSidebar
        conversations={conversations}
        currentId={currentConversationId}
        onSelect={handleSelectConversation}
        onNew={handleNewChat}
        onDelete={handleDeleteConversation}
        onRename={handleRenameConversation}
        onClearAll={handleClearAll}
        isCollapsed={isSidebarCollapsed}
        onToggleCollapse={handleToggleSidebar}
      />

      {/* Main Chat Area */}
      <div className="flex-1 flex flex-col min-w-0">
        <MessageList
          messages={messages}
          onSuggestionClick={handleSuggestionClick}
          onOptionClick={handleOptionClick}
        />
        <ChatInput
          onSend={handleSend}
          onCancel={cancelStreaming}
          isLoading={isStreaming}
          initialMessage={initialMessage}
          onInitialMessageConsumed={handleInitialMessageConsumed}
        />
      </div>
    </div>
  );
}
