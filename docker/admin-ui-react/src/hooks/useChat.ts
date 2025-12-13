import { useCallback, useRef, useState } from "react";
import { chatApi } from "@/api/chat";
import type { ChatMessage, MessageMetadata } from "@/types/chat";

interface UseChatOptions {
  conversationId?: string;
  onNewConversation?: (id: string) => void;
}

interface UseChatReturn {
  messages: ChatMessage[];
  isStreaming: boolean;
  error: string | null;
  sendMessage: (content: string, files?: File[]) => Promise<void>;
  cancelStream: () => void;
  clearMessages: () => void;
  loadConversation: (id: string) => Promise<void>;
}

export function useChat(options: UseChatOptions = {}): UseChatReturn {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const conversationIdRef = useRef<string | undefined>(options.conversationId);

  const loadConversation = useCallback(async (id: string) => {
    try {
      const history = await chatApi.getHistory(id);
      setMessages(history);
      conversationIdRef.current = id;
      setError(null);
    } catch (err) {
      setError("Failed to load conversation");
      console.error("Failed to load conversation:", err);
    }
  }, []);

  const sendMessage = useCallback(
    async (content: string, files?: File[]) => {
      if (!content.trim() || isStreaming) return;

      setError(null);

      // Create user message
      const userMessage: ChatMessage = {
        id: `temp-${Date.now()}`,
        role: "user",
        content: content.trim(),
        timestamp: new Date().toISOString(),
      };

      // Create placeholder assistant message
      const assistantMessage: ChatMessage = {
        id: `temp-assistant-${Date.now()}`,
        role: "assistant",
        content: "",
        timestamp: new Date().toISOString(),
        isStreaming: true,
      };

      setMessages((prev) => [...prev, userMessage, assistantMessage]);
      setIsStreaming(true);

      // Create abort controller
      abortControllerRef.current = new AbortController();

      // Handle file uploads if any
      const attachments: string[] = [];
      if (files && files.length > 0) {
        for (const file of files) {
          try {
            const result = await chatApi.uploadFile(file);
            attachments.push(result.file_id);
          } catch (err) {
            console.error("Failed to upload file:", err);
          }
        }
      }

      // Send streaming request
      chatApi.sendMessageStream(
        {
          message: content.trim(),
          conversation_id: conversationIdRef.current,
          attachments: attachments.length > 0 ? attachments : undefined,
        },
        // onChunk
        (chunk: string) => {
          setMessages((prev) => {
            const updated = [...prev];
            const lastMsg = updated[updated.length - 1];
            if (lastMsg && lastMsg.role === "assistant") {
              lastMsg.content += chunk;
            }
            return updated;
          });
        },
        // onMetadata
        (metadata: Record<string, unknown>) => {
          setMessages((prev) => {
            const updated = [...prev];
            const lastMsg = updated[updated.length - 1];
            if (lastMsg && lastMsg.role === "assistant") {
              lastMsg.metadata = metadata as MessageMetadata;
            }
            return updated;
          });
        },
        // onDone
        (response) => {
          setIsStreaming(false);
          conversationIdRef.current = response.conversation_id;

          // Update message with final ID
          setMessages((prev) => {
            const updated = [...prev];
            const lastMsg = updated[updated.length - 1];
            if (lastMsg && lastMsg.role === "assistant") {
              lastMsg.id = response.message_id;
              lastMsg.isStreaming = false;
            }
            return updated;
          });

          // Notify about new conversation
          if (options.onNewConversation && response.conversation_id) {
            options.onNewConversation(response.conversation_id);
          }
        },
        // onError
        (errorMsg: string) => {
          setIsStreaming(false);
          setError(errorMsg);

          // Update assistant message with error
          setMessages((prev) => {
            const updated = [...prev];
            const lastMsg = updated[updated.length - 1];
            if (lastMsg && lastMsg.role === "assistant") {
              lastMsg.content = `Error: ${errorMsg}`;
              lastMsg.isStreaming = false;
            }
            return updated;
          });
        },
        abortControllerRef.current.signal
      );
    },
    [isStreaming, options]
  );

  const cancelStream = useCallback(() => {
    if (abortControllerRef.current) {
      abortControllerRef.current.abort();
      setIsStreaming(false);
    }
  }, []);

  const clearMessages = useCallback(() => {
    setMessages([]);
    conversationIdRef.current = undefined;
    setError(null);
  }, []);

  return {
    messages,
    isStreaming,
    error,
    sendMessage,
    cancelStream,
    clearMessages,
    loadConversation,
  };
}
