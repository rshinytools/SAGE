// Chat Types for SAGE AI Assistant

export interface FileAttachment {
  id: string;
  name: string;
  type: string;
  size: number;
  url?: string;
}

export interface MessageMetadata {
  model?: string;
  tokens?: number;
  sql_query?: string;
  table_result?: Record<string, unknown>[];
  confidence?: number;
  execution_time_ms?: number;
}

export interface ChatMessage {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: string;
  attachments?: FileAttachment[];
  metadata?: MessageMetadata;
  isStreaming?: boolean;
}

export interface Conversation {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
  message_count: number;
  messages?: ChatMessage[];
}

export interface SendMessageRequest {
  message: string;
  conversation_id?: string;
  attachments?: string[]; // File IDs
}

export interface SendMessageResponse {
  conversation_id: string;
  message_id: string;
}

export interface ChatStreamEvent {
  type: "content" | "metadata" | "done" | "error";
  content?: string;
  metadata?: MessageMetadata;
  error?: string;
}

export interface UploadFileResponse {
  file_id: string;
  filename: string;
  size: number;
}
