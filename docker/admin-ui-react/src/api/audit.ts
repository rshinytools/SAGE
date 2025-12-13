import apiClient from "./client";
import type { AuditLogEntry, AuditLogFilter, PaginatedResponse } from "@/types/api";

export const auditApi = {
  getLogs: async (
    filter?: AuditLogFilter,
    page: number = 1,
    pageSize: number = 50
  ): Promise<PaginatedResponse<AuditLogEntry>> => {
    const response = await apiClient.get<PaginatedResponse<AuditLogEntry>>("/audit/logs", {
      params: { ...filter, page, page_size: pageSize },
    });
    return response.data;
  },

  getLog: async (id: string): Promise<AuditLogEntry> => {
    const response = await apiClient.get<AuditLogEntry>(`/audit/logs/${id}`);
    return response.data;
  },

  exportLogs: async (filter?: AuditLogFilter, format: "csv" | "json" = "csv"): Promise<Blob> => {
    const response = await apiClient.get("/audit/logs/export", {
      params: { ...filter, format },
      responseType: "blob",
    });
    return response.data;
  },

  getActions: async (): Promise<string[]> => {
    const response = await apiClient.get<string[]>("/audit/actions");
    return response.data;
  },

  getResources: async (): Promise<string[]> => {
    const response = await apiClient.get<string[]>("/audit/resources");
    return response.data;
  },

  getStatistics: async (
    startDate?: string,
    endDate?: string
  ): Promise<{
    total_entries: number;
    by_action: Record<string, number>;
    by_user: Record<string, number>;
    by_status: { success: number; failure: number };
  }> => {
    const response = await apiClient.get<{
      total_entries: number;
      by_action: Record<string, number>;
      by_user: Record<string, number>;
      by_status: { success: number; failure: number };
    }>("/audit/statistics", {
      params: { start_date: startDate, end_date: endDate },
    });
    return response.data;
  },
};
