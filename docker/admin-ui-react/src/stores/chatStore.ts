import { create } from "zustand";
import { persist } from "zustand/middleware";
import type { ChatMessage, Conversation, MessageMetadata } from "@/types/chat";

interface ChatState {
  // Current conversation
  currentConversationId: string | null;
  conversations: Conversation[];
  messages: ChatMessage[];

  // UI state
  isStreaming: boolean;
  streamingContent: string;
  abortController: AbortController | null;

  // Actions
  setCurrentConversation: (id: string | null) => void;
  setConversations: (conversations: Conversation[]) => void;
  addConversation: (conversation: Conversation) => void;
  removeConversation: (id: string) => void;
  updateConversation: (id: string, updates: Partial<Conversation>) => void;

  setMessages: (messages: ChatMessage[]) => void;
  addMessage: (message: ChatMessage) => void;
  updateMessage: (id: string, updates: Partial<ChatMessage>) => void;

  // Streaming
  startStreaming: () => AbortController;
  appendStreamingContent: (content: string) => void;
  finishStreaming: (metadata?: MessageMetadata) => void;
  cancelStreaming: () => void;

  // Clear
  clearChat: () => void;
  clearAll: () => void;
}

export const useChatStore = create<ChatState>()(
  persist(
    (set, get) => ({
      currentConversationId: null,
      conversations: [],
      messages: [],
      isStreaming: false,
      streamingContent: "",
      abortController: null,

      setCurrentConversation: (id) => set({ currentConversationId: id }),

      setConversations: (conversations) => set({ conversations }),

      addConversation: (conversation) =>
        set((state) => ({
          conversations: [conversation, ...state.conversations],
        })),

      removeConversation: (id) =>
        set((state) => ({
          conversations: state.conversations.filter((c) => c.id !== id),
          currentConversationId:
            state.currentConversationId === id ? null : state.currentConversationId,
          messages: state.currentConversationId === id ? [] : state.messages,
        })),

      updateConversation: (id, updates) =>
        set((state) => ({
          conversations: state.conversations.map((c) =>
            c.id === id ? { ...c, ...updates } : c
          ),
        })),

      setMessages: (messages) => set({ messages }),

      addMessage: (message) =>
        set((state) => ({
          messages: [...state.messages, message],
        })),

      updateMessage: (id, updates) =>
        set((state) => ({
          messages: state.messages.map((m) =>
            m.id === id ? { ...m, ...updates } : m
          ),
        })),

      startStreaming: () => {
        const controller = new AbortController();
        const streamingMessageId = `streaming-${Date.now()}`;

        set((state) => ({
          isStreaming: true,
          streamingContent: "",
          abortController: controller,
          messages: [
            ...state.messages,
            {
              id: streamingMessageId,
              role: "assistant",
              content: "",
              timestamp: new Date().toISOString(),
              isStreaming: true,
            },
          ],
        }));

        return controller;
      },

      appendStreamingContent: (content) =>
        set((state) => {
          const newContent = state.streamingContent + content;
          return {
            streamingContent: newContent,
            messages: state.messages.map((m) =>
              m.isStreaming ? { ...m, content: newContent } : m
            ),
          };
        }),

      finishStreaming: (metadata) =>
        set((state) => ({
          isStreaming: false,
          streamingContent: "",
          abortController: null,
          messages: state.messages.map((m) =>
            m.isStreaming
              ? { ...m, isStreaming: false, metadata }
              : m
          ),
        })),

      cancelStreaming: () => {
        const { abortController } = get();
        if (abortController) {
          abortController.abort();
        }
        set((state) => ({
          isStreaming: false,
          streamingContent: "",
          abortController: null,
          messages: state.messages.filter((m) => !m.isStreaming),
        }));
      },

      clearChat: () =>
        set({
          messages: [],
          currentConversationId: null,
          isStreaming: false,
          streamingContent: "",
        }),

      clearAll: () =>
        set({
          currentConversationId: null,
          conversations: [],
          messages: [],
          isStreaming: false,
          streamingContent: "",
          abortController: null,
        }),
    }),
    {
      name: "sage-chat-storage",
      partialize: (state) => ({
        conversations: state.conversations.slice(0, 50), // Keep last 50 conversations
        currentConversationId: state.currentConversationId,
      }),
    }
  )
);
