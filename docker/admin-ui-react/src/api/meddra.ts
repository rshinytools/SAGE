import apiClient from "./client";

// MedDRA Library Types

export interface MedDRAVersion {
  version: string;
  language: string;
  loaded_at: string;
  total_terms: number;
  soc_count: number;
  hlgt_count: number;
  hlt_count: number;
  pt_count: number;
  llt_count: number;
  file_path: string;
}

export interface MedDRAStatus {
  available: boolean;
  current_version: MedDRAVersion | null;
  loading_in_progress: boolean;
}

export interface MedDRATerm {
  code: string;
  name: string;
  level: "SOC" | "HLGT" | "HLT" | "PT" | "LLT";
  parent_code?: string;
  parent_name?: string;
}

export interface MedDRAHierarchy {
  llt?: MedDRATerm;
  pt: MedDRATerm;
  hlt: MedDRATerm;
  hlgt: MedDRATerm;
  soc: MedDRATerm;
}

export interface MedDRASearchResult {
  term: MedDRATerm;
  match_score: number;
  hierarchy: MedDRAHierarchy;
}

export interface MedDRASearchResponse {
  query: string;
  level_filter: string | null;
  count: number;
  results: MedDRASearchResult[];
}

export interface MedDRALookupResponse {
  found: boolean;
  query: string;
  exact_match: MedDRATerm | null;
  hierarchy: MedDRAHierarchy | null;
  related_terms: MedDRATerm[];
  message: string;
}

export interface MedDRAUploadResponse {
  success: boolean;
  message: string;
  version?: MedDRAVersion;
}

export interface MedDRAPreviewResponse {
  filename: string;
  rows: number;
  columns: string[];
  column_mappings: Record<string, string | null>;
  missing_required: string[];
  can_load: boolean;
  sample_data: Record<string, unknown>[];
  message: string;
}

export interface MedDRAStatistics {
  version: string;
  term_counts: {
    soc: number;
    hlgt: number;
    hlt: number;
    pt: number;
    llt: number;
    total: number;
  };
  top_socs: { name: string; pt_count: number }[];
}

export const meddraApi = {
  // Get MedDRA status
  getStatus: async (): Promise<MedDRAStatus> => {
    const response = await apiClient.get<MedDRAStatus>("/meddra/status");
    return response.data;
  },

  // Preview MedDRA SAS7BDAT file structure
  previewFile: async (file: File): Promise<MedDRAPreviewResponse> => {
    const formData = new FormData();
    formData.append("file", file);

    const response = await apiClient.post<MedDRAPreviewResponse>("/meddra/preview", formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
    });
    return response.data;
  },

  // Upload MedDRA SAS7BDAT file
  uploadFile: async (file: File, onProgress?: (progress: number) => void): Promise<MedDRAUploadResponse> => {
    const formData = new FormData();
    formData.append("file", file);

    const response = await apiClient.post<MedDRAUploadResponse>("/meddra/upload", formData, {
      headers: {
        "Content-Type": "multipart/form-data",
      },
      onUploadProgress: (progressEvent) => {
        if (progressEvent.total && onProgress) {
          const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
          onProgress(progress);
        }
      },
    });
    return response.data;
  },

  // Delete current MedDRA version
  deleteVersion: async (): Promise<{ success: boolean; message: string }> => {
    const response = await apiClient.delete("/meddra/version");
    return response.data;
  },

  // Search MedDRA terms
  search: async (
    query: string,
    level?: "SOC" | "HLGT" | "HLT" | "PT" | "LLT",
    limit: number = 20
  ): Promise<MedDRASearchResponse> => {
    const response = await apiClient.get<MedDRASearchResponse>("/meddra/search", {
      params: { query, level, limit },
    });
    return response.data;
  },

  // Lookup a term with full hierarchy
  lookup: async (term: string): Promise<MedDRALookupResponse> => {
    const response = await apiClient.get<MedDRALookupResponse>("/meddra/lookup", {
      params: { term },
    });
    return response.data;
  },

  // Get term by code
  getTermByCode: async (code: string): Promise<{ term: MedDRATerm; hierarchy: MedDRAHierarchy }> => {
    const response = await apiClient.get(`/meddra/term/${code}`);
    return response.data;
  },

  // Get all children of a term
  getChildren: async (code: string): Promise<{ parent: MedDRATerm; children: MedDRATerm[] }> => {
    const response = await apiClient.get(`/meddra/term/${code}/children`);
    return response.data;
  },

  // Get statistics
  getStatistics: async (): Promise<MedDRAStatistics> => {
    const response = await apiClient.get<MedDRAStatistics>("/meddra/statistics");
    return response.data;
  },

  // Browse by SOC
  getBySoc: async (socCode?: string): Promise<{ socs: MedDRATerm[]; children?: MedDRATerm[] }> => {
    const url = socCode ? `/meddra/browse/soc/${socCode}` : "/meddra/browse/soc";
    const response = await apiClient.get(url);
    return response.data;
  },

  // Get PTs by SOC (simplified hierarchy)
  getPtsBySoc: async (socCode: string): Promise<{ soc: MedDRATerm; pt_count: number; pts: MedDRATerm[] }> => {
    const response = await apiClient.get(`/meddra/browse/soc/${socCode}/pts`);
    return response.data;
  },

  // Get LLTs by PT
  getLltsByPt: async (ptCode: string): Promise<{ pt: MedDRATerm; llt_count: number; llts: MedDRATerm[] }> => {
    const response = await apiClient.get(`/meddra/browse/pt/${ptCode}/llts`);
    return response.data;
  },
};
