import apiClient from "./client";
import type {
  MetadataDomain,
  MetadataVariable,
  MetadataIssue,
  MetadataCodelist,
  PaginatedResponse,
  ApprovalStatus
} from "@/types/api";

// API response wrapper type - backend wraps all responses
interface ApiResponse<T> {
  success: boolean;
  data: T;
  meta?: { timestamp: string; count?: number };
}

export const metadataApi = {
  getDomains: async (): Promise<MetadataDomain[]> => {
    interface DomainInfo {
      name: string;
      label?: string;
      variable_count: number;
      approved_count: number;
      pending_count: number;
      status: ApprovalStatus;
    }
    const response = await apiClient.get<ApiResponse<DomainInfo[]>>("/metadata/domains");
    return (response.data.data || []).map((d) => ({
      name: d.name,
      description: d.label,
      variables_count: d.variable_count,
      approved_count: d.approved_count || 0,
      pending_count: d.pending_count || 0,
      datasets_count: 0,
      last_updated: new Date().toISOString(),
      status: d.status,
    }));
  },

  getDomain: async (name: string): Promise<MetadataDomain> => {
    const response = await apiClient.get<ApiResponse<Record<string, unknown>>>(`/metadata/domains/${name}`);
    const data = response.data.data;
    return {
      name: (data.name as string) || name,
      description: data.label as string,
      variables_count: (data.variable_count as number) || 0,
      datasets_count: 0,
      last_updated: new Date().toISOString(),
    };
  },

  getVariables: async (
    domain?: string,
    page: number = 1,
    pageSize: number = 50
  ): Promise<PaginatedResponse<MetadataVariable>> => {
    // Backend requires domain parameter - get variables for specific domain
    if (!domain) {
      // If no domain specified, return empty result
      return {
        items: [],
        total: 0,
        page,
        page_size: pageSize,
        total_pages: 0,
      };
    }

    interface VariableInfo {
      name: string;
      label: string;
      data_type: string;
      length?: number;
      format?: string;
      codelist?: string;
      origin?: string;
      derivation?: string;
      plain_english?: string;
      status?: ApprovalStatus;  // API returns status directly, not in approval object
      approval?: { status: ApprovalStatus };  // Keep for backwards compat
    }
    const response = await apiClient.get<ApiResponse<VariableInfo[]>>(
      `/metadata/domains/${domain}/variables`
    );
    const vars = (response.data.data || []).map((v) => ({
      name: v.name,
      label: v.label,
      type: v.data_type,
      length: v.length,
      format: v.format,
      codelist: v.codelist,
      origin: v.origin,
      derivation: v.derivation,
      plain_english: v.plain_english,
      domain,
      dataset: "",
      // API returns status directly at top level, not nested in approval object
      status: v.status || v.approval?.status || "pending",
    }));

    // Paginate client-side
    const start = (page - 1) * pageSize;
    const paginatedVars = vars.slice(start, start + pageSize);

    return {
      items: paginatedVars,
      total: vars.length,
      page,
      page_size: pageSize,
      total_pages: Math.ceil(vars.length / pageSize),
    };
  },

  getVariable: async (domain: string, name: string): Promise<MetadataVariable> => {
    const response = await apiClient.get<ApiResponse<Record<string, unknown>>>(
      `/metadata/domains/${domain}/variables/${name}`
    );
    const data = response.data.data;
    const approval = data.approval as { status: ApprovalStatus; reviewed_by?: string; reviewed_at?: string; comment?: string } | undefined;
    return {
      name: (data.name as string) || name,
      label: (data.label as string) || "",
      type: (data.data_type as string) || "string",
      length: data.length as number,
      format: data.format as string,
      codelist: data.codelist as string,
      codelist_values: data.codelist_values as Array<{ code: string; decode: string }>,
      origin: data.origin as string,
      role: data.role as string,
      core: data.core as string,
      description: data.description as string,
      derivation: data.derivation as string,
      plain_english: data.plain_english as string,
      source: data.source as string,
      predecessor: data.predecessor as string,
      comment: data.comment as string,
      domain,
      dataset: "",
      status: approval?.status || "pending",
      approval: approval,
    };
  },

  runAudit: async (_domain?: string): Promise<{ job_id: string }> => {
    // Backend doesn't have audit endpoint - return mock job_id
    console.warn("runAudit: Backend endpoint not implemented");
    return { job_id: `audit-${Date.now()}` };
  },

  getAuditResults: async (_jobId: string): Promise<MetadataIssue[]> => {
    // Backend doesn't have audit results endpoint - return empty
    console.warn("getAuditResults: Backend endpoint not implemented");
    return [];
  },

  getIssues: async (
    _severity?: "error" | "warning" | "info",
    _domain?: string
  ): Promise<MetadataIssue[]> => {
    // Backend doesn't have issues endpoint - use pending items
    try {
      interface PendingResponse {
        domains: unknown[];
        variables: unknown[];
        codelists: unknown[];
      }
      const response = await apiClient.get<ApiResponse<PendingResponse>>("/metadata/pending");
      const pending = response.data.data;
      const issues: MetadataIssue[] = [];

      // Convert pending items to issues format
      if (pending.variables && Array.isArray(pending.variables)) {
        pending.variables.forEach((v: unknown, idx: number) => {
          const variable = v as Record<string, unknown>;
          issues.push({
            id: `issue-${idx}`,
            severity: "warning",
            domain: (variable.domain as string) || "",
            variable: variable.name as string,
            message: "Variable pending approval",
            rule: "approval_required",
          });
        });
      }
      return issues;
    } catch {
      return [];
    }
  },

  uploadSpecification: async (file: File): Promise<{ domains: number; variables: number }> => {
    const formData = new FormData();
    formData.append("file", file);

    // Backend uses /import endpoint
    interface ImportResponse {
      domains_imported: number;
      variables_imported: number;
      codelists_imported: number;
    }
    const response = await apiClient.post<ApiResponse<ImportResponse>>(
      "/metadata/import",
      formData,
      {
        headers: {
          "Content-Type": "multipart/form-data",
        },
        timeout: 300000, // 5 minutes for large Excel files
      }
    );
    const data = response.data.data;
    return {
      domains: data.domains_imported,
      variables: data.variables_imported,
    };
  },

  // ============================================
  // Version Control
  // ============================================

  getVersions: async (limit: number = 50): Promise<Array<{
    version_id: string;
    version_number: number;
    content_hash: string;
    created_at: string;
    created_by: string;
    comment?: string;
    parent_version?: string;
  }>> => {
    const response = await apiClient.get<ApiResponse<Array<{
      version_id: string;
      version_number: number;
      content_hash: string;
      created_at: string;
      created_by: string;
      comment?: string;
      parent_version?: string;
    }>>>("/metadata/versions", { params: { limit } });
    return response.data.data || [];
  },

  getVersion: async (versionId: string): Promise<{
    version: {
      version_id: string;
      version_number: number;
      content_hash: string;
      created_at: string;
      created_by: string;
      comment?: string;
    };
    content: Record<string, unknown>;
  }> => {
    const response = await apiClient.get<ApiResponse<{
      version: {
        version_id: string;
        version_number: number;
        content_hash: string;
        created_at: string;
        created_by: string;
        comment?: string;
      };
      content: Record<string, unknown>;
    }>>(`/metadata/versions/${versionId}`);
    return response.data.data;
  },

  rollbackVersion: async (versionId: string): Promise<{ message: string }> => {
    const response = await apiClient.post<ApiResponse<{ message: string }>>(
      `/metadata/versions/${versionId}/rollback`
    );
    return response.data.data;
  },

  getChangeHistory: async (entityType?: string, entityId?: string, limit: number = 100): Promise<Array<{
    entity_type: string;
    entity_id: string;
    change_type: string;
    field_name?: string;
    old_value?: string;
    new_value?: string;
    user: string;
    timestamp: string;
    comment?: string;
  }>> => {
    const response = await apiClient.get<ApiResponse<Array<{
      entity_type: string;
      entity_id: string;
      change_type: string;
      field_name?: string;
      old_value?: string;
      new_value?: string;
      user: string;
      timestamp: string;
      comment?: string;
    }>>>("/metadata/history", {
      params: { entity_type: entityType, entity_id: entityId, limit },
    });
    return response.data.data || [];
  },

  compareVersions: async (
    version1: string,
    version2: string
  ): Promise<{
    added: MetadataVariable[];
    removed: MetadataVariable[];
    modified: MetadataVariable[];
  }> => {
    // Backend uses /versions/diff with v1 and v2 params
    try {
      const response = await apiClient.get<ApiResponse<Record<string, unknown>>>("/metadata/versions/diff", {
        params: { v1: version1, v2: version2 },
      });
      const data = response.data.data;
      return {
        added: (data.added as MetadataVariable[]) || [],
        removed: (data.removed as MetadataVariable[]) || [],
        modified: (data.modified as MetadataVariable[]) || [],
      };
    } catch {
      return { added: [], removed: [], modified: [] };
    }
  },

  // ============================================
  // Domain Management
  // ============================================

  deleteDomain: async (name: string): Promise<{ message: string }> => {
    const response = await apiClient.delete<ApiResponse<{ message: string }>>(
      `/metadata/domains/${name}`
    );
    return response.data.data;
  },

  // ============================================
  // Approval Methods
  // ============================================

  approveDomain: async (name: string, comment?: string): Promise<void> => {
    await apiClient.post(`/metadata/domains/${name}/approve`, null, {
      params: { comment },
    });
  },

  rejectDomain: async (name: string, comment: string): Promise<void> => {
    await apiClient.post(`/metadata/domains/${name}/reject`, null, {
      params: { comment },
    });
  },

  approveVariable: async (domain: string, name: string, comment?: string): Promise<void> => {
    await apiClient.post(`/metadata/domains/${domain}/variables/${name}/approve`, null, {
      params: { comment },
    });
  },

  rejectVariable: async (domain: string, name: string, comment: string): Promise<void> => {
    await apiClient.post(`/metadata/domains/${domain}/variables/${name}/reject`, null, {
      params: { comment },
    });
  },

  approveCodelist: async (name: string, comment?: string): Promise<void> => {
    await apiClient.post(`/metadata/codelists/${name}/approve`, null, {
      params: { comment },
    });
  },

  // Bulk approve all variables in a domain (single API call, single version entry)
  bulkApproveVariables: async (
    domain: string,
    comment?: string,
    _onProgress?: (current: number, total: number, variableName: string) => void
  ): Promise<{ approved: number; total: number }> => {
    // Use the new bulk-approve endpoint that creates only ONE version entry
    const response = await apiClient.post<ApiResponse<{ approved: number; total: number; message: string }>>(
      `/metadata/domains/${domain}/bulk-approve`,
      null,
      { params: { comment: comment || "Bulk approved" } }
    );
    return {
      approved: response.data.data.approved,
      total: response.data.data.total,
    };
  },

  // Get statistics
  getStats: async (): Promise<{
    total_domains: number;
    total_variables: number;
    total_codelists: number;
    approved_domains: number;
    approved_variables: number;
    approved_codelists: number;
    pending_domains: number;
    pending_variables: number;
    pending_codelists: number;
  }> => {
    const response = await apiClient.get<ApiResponse<Record<string, number>>>("/metadata/stats");
    const data = response.data.data;
    return {
      total_domains: data.total_domains || 0,
      total_variables: data.total_variables || 0,
      total_codelists: data.total_codelists || 0,
      approved_domains: data.approved_domains || 0,
      approved_variables: data.approved_variables || 0,
      approved_codelists: data.approved_codelists || 0,
      pending_domains: data.pending_domains || 0,
      pending_variables: data.pending_variables || 0,
      pending_codelists: data.pending_codelists || 0,
    };
  },

  // Get codelists
  getCodelists: async (): Promise<MetadataCodelist[]> => {
    interface CodelistInfo {
      name: string;
      label: string;
      data_type: string;
      values: Array<{ code: string; decode: string }>;
      approval?: { status: ApprovalStatus };
    }
    const response = await apiClient.get<ApiResponse<CodelistInfo[]>>("/metadata/codelists");
    return (response.data.data || []).map((c) => ({
      name: c.name,
      label: c.label,
      data_type: c.data_type,
      values: c.values,
      status: c.approval?.status || "pending",
    }));
  },

  // ============================================
  // CDISC Library
  // ============================================

  getCDISCStats: async (): Promise<{
    initialized: boolean;
    total_domains: number;
    total_variables: number;
    domains_by_standard: Record<string, number>;
    variables_by_standard: Record<string, number>;
  }> => {
    const response = await apiClient.get<ApiResponse<{
      initialized: boolean;
      total_domains: number;
      total_variables: number;
      domains_by_standard: Record<string, number>;
      variables_by_standard: Record<string, number>;
    }>>("/metadata/cdisc/stats");
    return response.data.data;
  },

  getCDISCDomains: async (standard?: string): Promise<Array<{
    standard: string;
    version: string;
    name: string;
    label: string;
    domain_class: string;
    structure: string;
  }>> => {
    const params = standard ? { standard } : {};
    const response = await apiClient.get<ApiResponse<Array<{
      standard: string;
      version: string;
      name: string;
      label: string;
      domain_class: string;
      structure: string;
    }>>>("/metadata/cdisc/domains", { params });
    return response.data.data || [];
  },

  getCDISCVariables: async (domain: string, standard?: string): Promise<Array<{
    standard: string;
    version: string;
    domain: string;
    name: string;
    label: string;
    data_type: string;
    core: string;
    role: string;
    codelist: string;
    description: string;
  }>> => {
    const params = standard ? { standard } : {};
    const response = await apiClient.get<ApiResponse<Array<{
      standard: string;
      version: string;
      domain: string;
      name: string;
      label: string;
      data_type: string;
      core: string;
      role: string;
      codelist: string;
      description: string;
    }>>>(`/metadata/cdisc/domains/${domain}/variables`, { params });
    return response.data.data || [];
  },

  searchCDISCVariables: async (query: string, limit: number = 50): Promise<Array<{
    standard: string;
    version: string;
    domain: string;
    name: string;
    label: string;
    data_type: string;
    core: string;
  }>> => {
    const response = await apiClient.get<ApiResponse<Array<{
      standard: string;
      version: string;
      domain: string;
      name: string;
      label: string;
      data_type: string;
      core: string;
    }>>>("/metadata/cdisc/search", { params: { q: query, limit } });
    return response.data.data || [];
  },

  // ============================================
  // Auto-Approval
  // ============================================

  runAutoApproval: async (dryRun: boolean = false): Promise<{
    message: string;
    dry_run: boolean;
    total_variables: number;
    auto_approved: number;
    quick_review: number;
    manual_review: number;
    auto_approved_pct: number;
    processing_time_seconds: number;
  }> => {
    const response = await apiClient.post<ApiResponse<{
      message: string;
      dry_run: boolean;
      total_variables: number;
      auto_approved: number;
      quick_review: number;
      manual_review: number;
      auto_approved_pct: number;
      processing_time_seconds: number;
    }>>("/metadata/auto-approve", null, { params: { dry_run: dryRun } });
    return response.data.data;
  },

  // ============================================
  // Audit Variables
  // ============================================

  getPendingCount: async (): Promise<number> => {
    const response = await apiClient.get<ApiResponse<{ pending: number }>>("/metadata/audit/pending-count");
    return response.data.data.pending;
  },

  // Returns the audit stream URL for SSE connection
  getAuditStreamUrl: (): string => {
    return "/api/v1/metadata/audit/stream";
  },

  // ============================================
  // Export
  // ============================================

  exportMetadata: async (approvedOnly: boolean = true): Promise<void> => {
    // Get the auth token
    const token = localStorage.getItem("auth_token");

    // Construct the URL with query params
    const baseUrl = import.meta.env.VITE_API_URL || "http://localhost:8002/api/v1";
    const url = `${baseUrl}/metadata/export?approved_only=${approvedOnly}`;

    // Fetch the file
    const response = await fetch(url, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    if (!response.ok) {
      throw new Error("Failed to export metadata");
    }

    // Get the blob and create download link
    const blob = await response.blob();
    const downloadUrl = window.URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = downloadUrl;
    link.download = `golden_metadata_${approvedOnly ? "approved" : "all"}_${new Date().toISOString().split("T")[0]}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(downloadUrl);
  },
};
