import apiClient from "./client";
import type { DataTable, TableColumn, TablePreview, PaginatedResponse } from "@/types/api";

// API response wrapper type - backend wraps all responses
interface ApiResponse<T> {
  success: boolean;
  data: T;
  meta?: { timestamp: string; count?: number };
}

// ============================================
// Data Factory Types
// ============================================

export interface FileInfo {
  filename: string;
  table_name: string;
  size: number;
  modified: string;
  type: string;
  status: string;
  record_id: string | null;
  row_count: number | null;
  column_count: number | null;
}

export interface UploadResult {
  filename: string;
  table_name: string;
  size: number;
  file_hash: string;
  uploaded_at: string;
  record_id: string;
  warning?: string;
  blocked?: boolean;
  block_reason?: string;
  schema_diff?: {
    has_changes: boolean;
    severity: string;
    added_columns: string[];
    removed_columns: string[];
    type_changes: Array<{ column: string; old: string; new: string }>;
  };
}

export interface ProcessingResult {
  job_id: string;
  status: string;
  total: number;
  completed: number;
  failed: number;
  results: Array<{
    filename: string;
    table_name?: string;
    status: string;
    rows?: number;
    columns?: number;
    schema_version?: number;
    error?: string;
    reason?: string;
  }>;
}

export interface SchemaVersion {
  version: number;
  schema_hash: string;
  column_count: number;
  source_file: string;
  created_at: string;
  is_current: boolean;
  columns: Array<{
    name: string;
    dtype: string;
    nullable: boolean;
  }>;
}

export interface FileRecord {
  id: string;
  filename: string;
  table_name: string;
  file_format: string;
  file_size: number;
  file_hash: string;
  schema_hash: string | null;
  status: string;
  uploaded_at: string;
  processed_at: string | null;
  row_count: number | null;
  column_count: number | null;
  schema_version: number | null;
  error_message: string | null;
  processing_steps: Array<{
    step_name: string;
    status: string;
    started_at: string | null;
    completed_at: string | null;
    message: string | null;
    error: string | null;
  }>;
  metadata: Record<string, unknown>;
}

export interface DataFactoryStatus {
  modules_available: boolean;
  database_exists: boolean;
  raw_directory: string;
  database_path: string;
  import_error?: string;
  file_statistics?: {
    total_files: number;
    unique_tables: number;
    total_rows_processed: number;
    total_size_bytes: number;
    total_size_mb: number;
    recent_uploads_24h: number;
    status_counts: Record<string, number>;
    format_counts: Record<string, number>;
  };
  table_summary?: Array<{
    table_name: string;
    total_uploads: number;
    completed: number;
    failed: number;
    latest_rows: number;
    latest_columns: number;
    last_upload: string;
  }>;
  schema_tables?: string[];
}

// SSE Progress event types
export interface ProgressEvent {
  step: string;
  message: string;
  progress: number;
  details?: {
    rows?: number;
    columns?: number;
  };
}

export interface SchemaChangeEvent {
  table: string;
  severity: string;
  added_columns: string[];
  removed_columns: string[];
  changes: Array<{
    column: string;
    type: string;
    old: string;
    new: string;
  }>;
}

export interface ProcessingCompleteEvent {
  table: string;
  rows: number;
  columns: number;
  schema_version: number;
  progress: number;
}

// ============================================
// Data API Client
// ============================================

export const dataApi = {
  // ============================================
  // Status & Statistics
  // ============================================

  getStatus: async (): Promise<DataFactoryStatus> => {
    const response = await apiClient.get<ApiResponse<DataFactoryStatus>>("/data/status");
    return response.data.data;
  },

  // ============================================
  // File Operations
  // ============================================

  getFiles: async (): Promise<FileInfo[]> => {
    const response = await apiClient.get<ApiResponse<FileInfo[]>>("/data/files");
    return response.data.data;
  },

  uploadFile: async (
    file: File,
    options?: {
      processImmediately?: boolean;
      blockOnBreaking?: boolean;
      onProgress?: (progress: number) => void;
    }
  ): Promise<UploadResult> => {
    const formData = new FormData();
    formData.append("file", file);

    const params = new URLSearchParams();
    if (options?.processImmediately) {
      params.append("process_immediately", "true");
    }
    if (options?.blockOnBreaking !== undefined) {
      params.append("block_on_breaking", options.blockOnBreaking.toString());
    }

    const response = await apiClient.post<ApiResponse<UploadResult>>(
      `/data/files/upload?${params.toString()}`,
      formData,
      {
        headers: {
          "Content-Type": "multipart/form-data",
        },
        onUploadProgress: (progressEvent) => {
          if (options?.onProgress && progressEvent.total) {
            const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
            options.onProgress(progress);
          }
        },
      }
    );
    return response.data.data;
  },

  deleteFile: async (filename: string): Promise<void> => {
    await apiClient.delete(`/data/files/${filename}`);
  },

  // ============================================
  // Processing
  // ============================================

  processFiles: async (
    files?: string[],
    blockOnBreaking: boolean = true
  ): Promise<ProcessingResult> => {
    const response = await apiClient.post<ApiResponse<ProcessingResult>>("/data/process", {
      files,
      block_on_breaking: blockOnBreaking,
    });
    return response.data.data;
  },

  processFileWithStream: (
    filename: string,
    options?: {
      blockOnBreaking?: boolean;
      onProgress?: (event: ProgressEvent) => void;
      onSchemaChange?: (event: SchemaChangeEvent) => void;
      onComplete?: (event: ProcessingCompleteEvent) => void;
      onError?: (error: { message: string; step: string }) => void;
      onBlocked?: (reason: { reason: string; severity: string }) => void;
    }
  ): EventSource => {
    const params = new URLSearchParams();
    if (options?.blockOnBreaking !== undefined) {
      params.append("block_on_breaking", options.blockOnBreaking.toString());
    }

    // EventSource doesn't support custom headers, so pass token as query param
    const token = localStorage.getItem("auth_token");
    if (token) {
      params.append("token", token);
    }

    const eventSource = new EventSource(
      `${apiClient.defaults.baseURL}/data/process/stream/${filename}?${params.toString()}`
    );

    eventSource.addEventListener("progress", (event) => {
      if (options?.onProgress) {
        options.onProgress(JSON.parse(event.data));
      }
    });

    eventSource.addEventListener("schema_change", (event) => {
      if (options?.onSchemaChange) {
        options.onSchemaChange(JSON.parse(event.data));
      }
    });

    eventSource.addEventListener("complete", (event) => {
      if (options?.onComplete) {
        options.onComplete(JSON.parse(event.data));
      }
      eventSource.close();
    });

    eventSource.addEventListener("error", (event) => {
      if (options?.onError && event instanceof MessageEvent) {
        options.onError(JSON.parse(event.data));
      }
      eventSource.close();
    });

    eventSource.addEventListener("blocked", (event) => {
      if (options?.onBlocked) {
        options.onBlocked(JSON.parse(event.data));
      }
      eventSource.close();
    });

    return eventSource;
  },

  // ============================================
  // Schema Operations
  // ============================================

  getSchemaVersions: async (tableName: string): Promise<SchemaVersion[]> => {
    const response = await apiClient.get<ApiResponse<{ table: string; versions: SchemaVersion[] }>>(
      `/data/schema/versions/${tableName}`
    );
    return response.data.data.versions;
  },

  compareSchema: async (
    tableName: string
  ): Promise<{
    table: string;
    has_previous: boolean;
    current_version?: number;
    total_versions?: number;
    current_schema?: {
      columns: Array<{ name: string; dtype: string; nullable: boolean }>;
      column_count: number;
      schema_hash: string;
    };
    message?: string;
  }> => {
    const response = await apiClient.post<
      ApiResponse<{
        table: string;
        has_previous: boolean;
        current_version?: number;
        total_versions?: number;
        current_schema?: {
          columns: Array<{ name: string; dtype: string; nullable: boolean }>;
          column_count: number;
          schema_hash: string;
        };
        message?: string;
      }>
    >("/data/schema/compare", { table_name: tableName });
    return response.data.data;
  },

  rollbackSchema: async (
    tableName: string,
    targetVersion: number
  ): Promise<{
    table: string;
    previous_version: number | null;
    current_version: number;
    message: string;
  }> => {
    const response = await apiClient.post<
      ApiResponse<{
        table: string;
        previous_version: number | null;
        current_version: number;
        message: string;
      }>
    >("/data/schema/rollback", { table_name: tableName, target_version: targetVersion });
    return response.data.data;
  },

  // ============================================
  // File History
  // ============================================

  getFileHistory: async (options?: {
    table?: string;
    status?: string;
    limit?: number;
  }): Promise<FileRecord[]> => {
    const params = new URLSearchParams();
    if (options?.table) params.append("table", options.table);
    if (options?.status) params.append("status", options.status);
    if (options?.limit) params.append("limit", options.limit.toString());

    const response = await apiClient.get<ApiResponse<FileRecord[]>>(
      `/data/history?${params.toString()}`
    );
    return response.data.data;
  },

  getFileRecord: async (recordId: string): Promise<FileRecord> => {
    const response = await apiClient.get<ApiResponse<FileRecord>>(`/data/history/${recordId}`);
    return response.data.data;
  },

  // ============================================
  // Table Operations (existing)
  // ============================================

  getTables: async (): Promise<DataTable[]> => {
    interface TableInfo {
      name: string;
      rows: number;
      columns: number;
      schema_version: number | null;
      loaded_at: string;
    }
    const response = await apiClient.get<ApiResponse<TableInfo[]>>("/data/tables");
    return response.data.data.map((t) => ({
      name: t.name,
      rows: t.rows,
      columns: t.columns,
      size_bytes: 0,
      created_at: t.loaded_at,
      modified_at: t.loaded_at,
    }));
  },

  getTable: async (name: string): Promise<DataTable & { schema_info?: Record<string, unknown> }> => {
    interface TableDetailInfo {
      name: string;
      columns: unknown[];
      row_count: number;
      sample: unknown[];
      schema_info?: {
        version: number;
        schema_hash: string;
        source_file: string;
        created_at: string;
      };
    }
    const response = await apiClient.get<ApiResponse<TableDetailInfo>>(`/data/tables/${name}`);
    const data = response.data.data;
    return {
      name: data.name,
      rows: data.row_count,
      columns: data.columns.length,
      size_bytes: 0,
      created_at: new Date().toISOString(),
      modified_at: new Date().toISOString(),
      schema_info: data.schema_info,
    };
  },

  getTableColumns: async (name: string): Promise<TableColumn[]> => {
    interface SchemaResponse {
      name: string;
      columns: Array<{
        name: string;
        type: string;
        nullable?: boolean;
      }>;
    }
    const response = await apiClient.get<ApiResponse<SchemaResponse>>(`/data/tables/${name}/schema`);
    return response.data.data.columns.map((col) => ({
      name: col.name,
      dtype: col.type,
      nullable: col.nullable ?? true,
    }));
  },

  getTablePreview: async (
    name: string,
    page: number = 1,
    pageSize: number = 50
  ): Promise<PaginatedResponse<Record<string, unknown>>> => {
    interface PreviewResponse {
      columns: string[];
      data: unknown[][];
      total: number;
    }
    const response = await apiClient.get<ApiResponse<PreviewResponse>>(
      `/data/tables/${name}/preview`,
      {
        params: { limit: pageSize, offset: (page - 1) * pageSize },
      }
    );
    const data = response.data.data;

    // Transform array data to objects using columns
    const items = data.data.map((row) => {
      const obj: Record<string, unknown> = {};
      data.columns.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    return {
      items,
      total: data.total,
      page,
      page_size: pageSize,
      total_pages: Math.ceil(data.total / pageSize),
    };
  },

  deleteTable: async (name: string, deleteHistory: boolean = false): Promise<void> => {
    await apiClient.delete(`/data/tables/${name}?delete_history=${deleteHistory}`);
  },

  exportTable: async (name: string, format: "csv" | "excel" | "parquet"): Promise<Blob> => {
    if (format === "parquet") {
      // Use dedicated Parquet export endpoint
      const response = await apiClient.post(
        `/data/tables/${name}/export/parquet`,
        {},
        { responseType: "blob" }
      );
      return response.data;
    }

    // For CSV/Excel, generate client-side from query
    const response = await apiClient.post<ApiResponse<{ columns: string[]; data: unknown[][] }>>(
      "/data/query",
      { sql: `SELECT * FROM "${name}"` }
    );
    const data = response.data.data;
    const headers = data.columns.join(",");
    const rows = data.data.map((row) =>
      row.map((val) => JSON.stringify(val ?? "")).join(",")
    );
    const csv = [headers, ...rows].join("\n");
    return new Blob([csv], { type: "text/csv" });
  },

  runQuery: async (query: string): Promise<TablePreview> => {
    interface QueryResponse {
      columns: string[];
      data: unknown[][];
      row_count: number;
      execution_time: number;
    }
    const response = await apiClient.post<ApiResponse<QueryResponse>>("/data/query", { sql: query });
    const data = response.data.data;

    // Transform array data to objects
    const items = data.data.map((row) => {
      const obj: Record<string, unknown> = {};
      data.columns.forEach((col, i) => {
        obj[col] = row[i];
      });
      return obj;
    });

    return {
      columns: data.columns,
      data: items,
      total_rows: data.row_count,
    };
  },

  // ============================================
  // Validation
  // ============================================

  getValidationReport: async (
    tableName: string
  ): Promise<{
    table: string;
    quality_score: number;
    row_count: number;
    column_count?: number;
    issues: Array<{
      type: string;
      column?: string;
      message: string;
    }>;
  }> => {
    const response = await apiClient.get<
      ApiResponse<{
        table: string;
        quality_score: number;
        row_count: number;
        column_count?: number;
        issues: Array<{
          type: string;
          column?: string;
          message: string;
        }>;
      }>
    >(`/data/validation/${tableName}`);
    return response.data.data;
  },

  runValidation: async (
    tables?: string[]
  ): Promise<{
    results: Array<{
      table: string;
      quality_score: number;
      row_count?: number;
      issues: Array<{
        type: string;
        column?: string;
        message: string;
      }>;
    }>;
  }> => {
    const response = await apiClient.post<
      ApiResponse<{
        results: Array<{
          table: string;
          quality_score: number;
          row_count?: number;
          issues: Array<{
            type: string;
            column?: string;
            message: string;
          }>;
        }>;
      }>
    >("/data/validate", null, { params: { tables } });
    return response.data.data;
  },
};

export default dataApi;
