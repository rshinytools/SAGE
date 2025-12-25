import apiClient from "./client";
import type { AuditLogEntry, AuditLogFilter } from "@/types/api";

// Extended types for audit
export interface AuditLogDetail extends AuditLogEntry {
  query_details?: QueryAuditDetails;
  signatures?: ElectronicSignature[];
}

export interface QueryAuditDetails {
  original_question?: string;
  sanitized_question?: string;
  intent_classification?: string;
  matched_entities?: Array<{ original: string; matched: string; confidence: number }>;
  generated_sql?: string;
  llm_prompt?: string;
  llm_response?: string;
  llm_model?: string;
  llm_tokens_used?: number;
  confidence_score?: number;
  confidence_breakdown?: Record<string, number>;
  execution_time_ms?: number;
  result_row_count?: number;
  tables_accessed?: string[];
  columns_used?: string[];
}

export interface ElectronicSignature {
  id?: number;
  audit_log_id: number;
  signer_user_id: string;
  signer_username: string;
  signature_meaning: string;
  signature_timestamp: string;
  signature_hash?: string;
}

export interface IntegrityCheckResult {
  log_id: number;
  integrity_valid: boolean;
  stored_checksum: string;
  computed_checksum: string;
  verified_at: string;
  discrepancy_details?: string;
}

export interface AuditStatistics {
  total_events: number;
  by_action: Record<string, number>;
  by_status: Record<string, number>;
  by_user: Record<string, number>;
  by_resource_type: Record<string, number>;
  average_query_confidence?: number;
  average_duration_ms?: number;
  date_range?: { start?: string; end?: string };
}

export interface AuditLogsListResponse {
  logs: AuditLogEntry[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface AuditUser {
  user_id: string;
  username: string;
}

export const auditApi = {
  /**
   * Get paginated audit logs with filters
   */
  getLogs: async (
    filter?: AuditLogFilter,
    page: number = 1,
    pageSize: number = 50
  ): Promise<AuditLogsListResponse> => {
    const response = await apiClient.get<AuditLogsListResponse>("/audit/logs", {
      params: {
        user_id: filter?.userId,
        username: filter?.username,
        action: filter?.action,
        resource_type: filter?.resourceType,
        status: filter?.status,
        start_date: filter?.startDate,
        end_date: filter?.endDate,
        search_text: filter?.searchText,
        page,
        page_size: pageSize,
      },
    });
    return response.data;
  },

  /**
   * Get a single audit log with full details
   */
  getLog: async (id: number): Promise<AuditLogDetail> => {
    const response = await apiClient.get<AuditLogDetail>(`/audit/logs/${id}`);
    return response.data;
  },

  /**
   * Get query details for an audit log
   */
  getQueryDetails: async (logId: number): Promise<QueryAuditDetails> => {
    const response = await apiClient.get<QueryAuditDetails>(`/audit/logs/${logId}/query-details`);
    return response.data;
  },

  /**
   * Get audit statistics
   */
  getStatistics: async (
    startDate?: string,
    endDate?: string
  ): Promise<AuditStatistics> => {
    const response = await apiClient.get<AuditStatistics>("/audit/statistics", {
      params: { start_date: startDate, end_date: endDate },
    });
    return response.data;
  },

  /**
   * Get available action types
   */
  getActions: async (): Promise<string[]> => {
    const response = await apiClient.get<string[]>("/audit/actions");
    return response.data;
  },

  /**
   * Get users who have audit entries
   */
  getUsers: async (): Promise<AuditUser[]> => {
    const response = await apiClient.get<AuditUser[]>("/audit/users");
    return response.data;
  },

  /**
   * Get available resource types
   */
  getResourceTypes: async (): Promise<string[]> => {
    const response = await apiClient.get<string[]>("/audit/resource-types");
    return response.data;
  },

  /**
   * Verify audit log integrity
   */
  verifyIntegrity: async (logId: number): Promise<IntegrityCheckResult> => {
    const response = await apiClient.get<IntegrityCheckResult>(`/audit/logs/${logId}/verify`);
    return response.data;
  },

  /**
   * Add electronic signature to audit log
   */
  addSignature: async (logId: number, meaning: string): Promise<ElectronicSignature> => {
    const response = await apiClient.post<ElectronicSignature>(`/audit/logs/${logId}/signature`, {
      meaning,
    });
    return response.data;
  },

  /**
   * Export logs to Excel
   */
  exportExcel: async (filter?: AuditLogFilter): Promise<void> => {
    const response = await apiClient.get("/audit/export/excel", {
      params: {
        user_id: filter?.userId,
        action: filter?.action,
        status: filter?.status,
        start_date: filter?.startDate,
        end_date: filter?.endDate,
      },
      responseType: "blob",
    });

    // Create download link
    const blob = new Blob([response.data], {
      type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `audit_logs_${new Date().toISOString().split("T")[0]}.xlsx`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  },

  /**
   * Export logs to PDF
   */
  exportPdf: async (filter?: AuditLogFilter): Promise<void> => {
    const response = await apiClient.get("/audit/export/pdf", {
      params: {
        user_id: filter?.userId,
        action: filter?.action,
        status: filter?.status,
        start_date: filter?.startDate,
        end_date: filter?.endDate,
      },
      responseType: "blob",
    });

    // Create download link
    const blob = new Blob([response.data], { type: "application/pdf" });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `audit_report_${new Date().toISOString().split("T")[0]}.pdf`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  },

  /**
   * Export logs to CSV
   */
  exportCsv: async (filter?: AuditLogFilter): Promise<void> => {
    const response = await apiClient.get("/audit/export/csv", {
      params: {
        user_id: filter?.userId,
        action: filter?.action,
        status: filter?.status,
        start_date: filter?.startDate,
        end_date: filter?.endDate,
      },
      responseType: "blob",
    });

    // Create download link
    const blob = new Blob([response.data], { type: "text/csv" });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `audit_logs_${new Date().toISOString().split("T")[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  },

  /**
   * Export logs to JSON
   */
  exportJson: async (filter?: AuditLogFilter): Promise<void> => {
    const response = await apiClient.get("/audit/export/json", {
      params: {
        user_id: filter?.userId,
        action: filter?.action,
        status: filter?.status,
        start_date: filter?.startDate,
        end_date: filter?.endDate,
      },
      responseType: "blob",
    });

    // Create download link
    const blob = new Blob([response.data], { type: "application/json" });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `audit_logs_${new Date().toISOString().split("T")[0]}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  },

  // Legacy methods for backwards compatibility
  exportLogs: async (filter?: AuditLogFilter, format: "csv" | "json" = "csv"): Promise<void> => {
    if (format === "json") {
      return auditApi.exportJson(filter);
    }
    return auditApi.exportCsv(filter);
  },

  getResources: async (): Promise<string[]> => {
    return auditApi.getResourceTypes();
  },
};
