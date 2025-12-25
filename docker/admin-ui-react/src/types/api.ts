// Authentication Types
export interface LoginRequest {
  username: string;
  password: string;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
  user: User;
}

export interface User {
  username: string;
  email?: string;
  role: "admin" | "user" | "viewer";
  permissions: string[];
}

// System Types
export interface HealthStatus {
  status: "healthy" | "degraded" | "unhealthy";
  version: string;
  uptime: number;
  services: ServiceHealth[];
}

export interface ServiceHealth {
  name: string;
  status: "healthy" | "unhealthy";
  latency?: number;
  message?: string;
}

export interface SystemStats {
  cpu_percent: number;
  memory_percent: number;
  disk_percent: number;
  active_sessions: number;
}

// Data Management Types
export interface DataTable {
  name: string;
  rows: number;
  columns: number;
  size_bytes: number;
  created_at: string;
  modified_at: string;
}

export interface TableColumn {
  name: string;
  dtype: string;
  nullable: boolean;
  unique_values?: number;
  sample_values?: string[];
}

export interface TablePreview {
  columns: string[];
  data: Record<string, unknown>[];
  total_rows: number;
}

// Metadata Types
export type ApprovalStatus = "pending" | "approved" | "rejected";

export interface ApprovalInfo {
  status: ApprovalStatus;
  reviewed_by?: string;
  reviewed_at?: string;
  comment?: string;
}

export interface MetadataDomain {
  name: string;
  description?: string;
  variables_count: number;
  approved_count?: number;
  pending_count?: number;
  datasets_count: number;
  last_updated: string;
  status?: ApprovalStatus;
  approval?: ApprovalInfo;
}

export interface MetadataVariable {
  name: string;
  label: string;
  type: string;
  length?: number;
  format?: string;
  domain: string;
  dataset: string;
  // Extended fields for full variable info
  codelist?: string;
  codelist_values?: Array<{ code: string; decode: string }>;
  origin?: string;
  role?: string;
  core?: string;
  description?: string;
  derivation?: string;
  plain_english?: string;
  source?: string;
  predecessor?: string;
  comment?: string;
  status?: ApprovalStatus;
  approval?: ApprovalInfo;
}

export interface MetadataCodelist {
  name: string;
  label: string;
  data_type: string;
  values: Array<{ code: string; decode: string }>;
  status?: ApprovalStatus;
  approval?: ApprovalInfo;
}

export interface MetadataIssue {
  id: string;
  severity: "error" | "warning" | "info";
  domain: string;
  variable?: string;
  message: string;
  rule: string;
  suggestion?: string;
}

// Dictionary Types
export interface DictionaryEntry {
  term: string;
  definition: string;
  synonyms: string[];
  category: string;
  source: string;
  created_at: string;
  updated_at: string;
}

// User Management Types
export interface UserAccount {
  id: string;
  username: string;
  email: string;
  role: "admin" | "user" | "viewer";
  permissions: string[];
  created_at: string;
  last_login?: string;
  is_active: boolean;
}

export interface CreateUserRequest {
  username: string;
  email: string;
  password: string;
  role: "admin" | "user" | "viewer";
  permissions?: string[];
}

export interface UpdateUserRequest {
  email?: string;
  role?: "admin" | "user" | "viewer";
  permissions?: string[];
  is_active?: boolean;
}

// Audit Log Types
export interface AuditLogEntry {
  id: number;
  timestamp: string;
  user_id: string;
  username: string;
  user?: string;  // Legacy alias for username
  action: string;
  resource_type?: string;
  resource?: string;  // Legacy alias for resource_type
  resource_id?: string;
  status: "success" | "failure" | "error";
  ip_address?: string;
  user_agent?: string;
  request_method?: string;
  request_path?: string;
  duration_ms?: number;
  error_message?: string;
  checksum?: string;
  details?: Record<string, unknown>;
}

export interface AuditLogFilter {
  userId?: string;
  username?: string;
  user?: string;  // Legacy
  action?: string;
  resourceType?: string;
  resource?: string;  // Legacy
  status?: string;
  startDate?: string;
  endDate?: string;
  start_date?: string;  // API format
  end_date?: string;    // API format
  searchText?: string;
}

// Project Tracker Types
export interface TrackerPhase {
  id: number;
  name: string;
  description?: string;
  order_num: number;
  tasks: TrackerTask[];
  progress: number;
}

export interface TrackerTask {
  id: number;
  phase_id: number;
  name: string;
  description?: string;
  status: "pending" | "in_progress" | "completed" | "blocked";
  priority: "low" | "medium" | "high" | "critical";
  assigned_to?: string;
  due_date?: string;
  completed_at?: string;
  notes?: string;
  order_num: number;
}

export interface CreateTaskRequest {
  phase_id: number;
  name: string;
  description?: string;
  priority?: "low" | "medium" | "high" | "critical";
  assigned_to?: string;
  due_date?: string;
}

export interface UpdateTaskRequest {
  name?: string;
  description?: string;
  status?: "pending" | "in_progress" | "completed" | "blocked";
  priority?: "low" | "medium" | "high" | "critical";
  assigned_to?: string;
  due_date?: string;
  notes?: string;
}

// Settings Types
export interface SystemSettings {
  site_name: string;
  site_description?: string;
  maintenance_mode: boolean;
  allow_registration: boolean;
  session_timeout_minutes: number;
  max_upload_size_mb: number;
  default_theme: "light" | "dark" | "system";
}

export interface LLMSettings {
  provider: "ollama" | "openai" | "anthropic";
  model: string;
  base_url?: string;
  api_key?: string;
  temperature: number;
  max_tokens: number;
}

// API Response Types
export interface ApiResponse<T> {
  data: T;
  message?: string;
}

export interface PaginatedResponse<T> {
  items: T[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface ApiError {
  detail: string;
  status_code: number;
}
