// Chat Types for SAGE AI Assistant

export interface FileAttachment {
  id: string;
  name: string;
  type: string;
  size: number;
  url?: string;
}

export interface ConfidenceScore {
  score: number;
  level: string;
  explanation?: string;
  components?: Record<string, unknown>;
}

export interface Methodology {
  query?: string;
  table_used?: string;
  population_used?: string;
  population_filter?: string;
  columns_used?: string[];
  entities_resolved?: Array<{ original: string; resolved: string; confidence: number }>;
  sql_executed?: string;
  confidence_score?: number;
  confidence_level?: string;
  assumptions?: string[];
  timestamp?: string;
}

export interface MessageMetadata {
  model?: string;
  tokens?: number;
  sql_query?: string;
  sql?: string;
  table_result?: Record<string, unknown>[];
  data?: Record<string, unknown>[];
  confidence?: number | ConfidenceScore;
  execution_time_ms?: number;
  methodology?: Methodology;
  pipeline?: boolean;
  pipeline_used?: boolean;
  success?: boolean;
  warnings?: string[];
  row_count?: number;
  // Clarification message properties
  clarification_needed?: boolean;
  response_type?: string;
  questions_count?: number;
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
