import { apiClient } from "./client";
import type {
  Conversation,
  ChatMessage,
  SendMessageRequest,
  SendMessageResponse,
  UploadFileResponse,
} from "@/types/chat";

export const chatApi = {
  // Get all conversations for the current user
  getConversations: async (): Promise<Conversation[]> => {
    const response = await apiClient.get<Conversation[]>("/chat/conversations");
    return response.data;
  },

  // Get a specific conversation with messages
  getConversation: async (id: string): Promise<Conversation> => {
    const response = await apiClient.get<Conversation>(`/chat/conversations/${id}`);
    return response.data;
  },

  // Create a new conversation
  createConversation: async (title?: string): Promise<Conversation> => {
    const response = await apiClient.post<Conversation>("/chat/conversations", { title });
    return response.data;
  },

  // Delete a conversation
  deleteConversation: async (id: string): Promise<void> => {
    await apiClient.delete(`/chat/conversations/${id}`);
  },

  // Delete all conversations
  deleteAllConversations: async (): Promise<{ deleted_count: number }> => {
    const response = await apiClient.delete<{ success: boolean; deleted_count: number }>("/chat/conversations");
    return { deleted_count: response.data.deleted_count };
  },

  // Send a message (non-streaming, for fallback)
  sendMessage: async (request: SendMessageRequest): Promise<ChatMessage> => {
    const response = await apiClient.post<ChatMessage>("/chat/message", request);
    return response.data;
  },

  // Send a message with streaming response
  sendMessageStream: (
    request: SendMessageRequest,
    onChunk: (chunk: string) => void,
    onMetadata: (metadata: Record<string, unknown>) => void,
    onDone: (response: SendMessageResponse) => void,
    onError: (error: string) => void,
    signal?: AbortSignal
  ): void => {
    const token = localStorage.getItem("auth_token");
    const baseUrl = import.meta.env.VITE_API_URL || "/api/v1";

    fetch(`${baseUrl}/chat/message/stream`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": token ? `Bearer ${token}` : "",
      },
      body: JSON.stringify(request),
      signal,
    })
      .then(async (response) => {
        if (!response.ok) {
          const error = await response.text();
          onError(error || "Failed to send message");
          return;
        }

        const reader = response.body?.getReader();
        if (!reader) {
          onError("No response body");
          return;
        }

        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";

          for (const line of lines) {
            if (line.startsWith("data: ")) {
              const data = line.slice(6);
              if (data === "[DONE]") {
                continue;
              }
              try {
                const parsed = JSON.parse(data);
                if (parsed.type === "content") {
                  onChunk(parsed.content);
                } else if (parsed.type === "metadata") {
                  onMetadata(parsed.metadata);
                } else if (parsed.type === "done") {
                  onDone({
                    conversation_id: parsed.conversation_id,
                    message_id: parsed.message_id,
                  });
                } else if (parsed.type === "error") {
                  onError(parsed.error);
                }
              } catch {
                // Not JSON, treat as plain text content
                if (data.trim()) {
                  onChunk(data);
                }
              }
            }
          }
        }
      })
      .catch((error) => {
        if (error.name === "AbortError") {
          onError("Request cancelled");
        } else {
          onError(error.message || "Network error");
        }
      });
  },

  // Upload a file for context
  uploadFile: async (file: File): Promise<UploadFileResponse> => {
    const formData = new FormData();
    formData.append("file", file);
    const response = await apiClient.post<UploadFileResponse>("/chat/upload", formData, {
      headers: { "Content-Type": "multipart/form-data" },
    });
    return response.data;
  },

  // Get chat history for a conversation
  getHistory: async (conversationId: string): Promise<ChatMessage[]> => {
    const response = await apiClient.get<ChatMessage[]>(
      `/chat/conversations/${conversationId}/messages`
    );
    return response.data;
  },

  // Update conversation title
  updateConversationTitle: async (id: string, title: string): Promise<Conversation> => {
    const response = await apiClient.patch<Conversation>(`/chat/conversations/${id}`, { title });
    return response.data;
  },
};
