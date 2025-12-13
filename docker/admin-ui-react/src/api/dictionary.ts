import apiClient from "./client";

// Types for Factory 3 Dictionary API (Fuzzy Matching Only)
// Note: Semantic/embedding search removed - clinical data requires controlled vocabulary (MedDRA)

export interface DictionaryStatus {
  available: boolean;
  last_build: string | null;
  build_duration_seconds: number | null;
  build_in_progress: boolean;
  build_progress: number;  // 0-100 percentage
  build_step: string;  // Current step description
  fuzzy_index: {
    entries: number;
    path: string;
    loaded?: boolean;
  } | null;
  schema_map: {
    tables: number;
    path: string;
  } | null;
}

export interface SearchRequest {
  query: string;
  threshold?: number;
  limit?: number;
  table_filter?: string;
  column_filter?: string;
}

export interface SearchResult {
  value: string;
  score: number;
  table: string;
  column: string;
  match_type: string;
  id: string;
}

export interface SearchResponse {
  query: string;
  threshold: number;
  count: number;
  results: SearchResult[];
}

export interface BuildRequest {
  rebuild?: boolean;
  tables?: string[];
}

export interface BuildResponse {
  status: string;
  message: string;
  options: BuildRequest;
}

export interface ColumnValues {
  table: string;
  column: string;
  total: number;
  offset: number;
  limit: number;
  values: string[];
}

export interface TableInfo {
  columns: string[];
  value_count: number;
}

export interface TablesResponse {
  count: number;
  tables: Record<string, TableInfo>;
}

export interface SchemaMap {
  columns: Record<string, {
    name: string;
    tables: string[];
    type: string;
    is_key: boolean;
    description: string;
    codelist: string | null;
    unique_values_count: number;
    sample_values: string[];
  }>;
  tables: Record<string, {
    name: string;
    columns: string[];
    row_count: number;
    description: string;
    domain_type: string;
    key_columns: string[];
  }>;
  generated_at: string;
  version: string;
}

export interface DictionaryStatistics {
  fuzzy: {
    total_entries: number;
    unique_values: number;
    tables: number;
    columns: number;
    entries_by_column: Record<string, number>;
  } | null;
  schema_map: {
    tables: number;
    columns: number;
    generated_at: string;
  } | null;
}

export interface CorrectionResponse {
  query: string;
  count: number;
  suggestions: {
    original: string;
    suggestion: string;
    score: number;
    table: string;
    column: string;
  }[];
}

// Clarification response for term lookup
export interface LookupResponse {
  found: boolean;
  query: string;
  message: string;
  suggestions: SearchResult[];
}

export const dictionaryApi = {
  // Status and Build
  getStatus: async (): Promise<DictionaryStatus> => {
    const response = await apiClient.get<DictionaryStatus>("/dictionary/status");
    return response.data;
  },

  triggerBuild: async (options: BuildRequest = {}): Promise<BuildResponse> => {
    const response = await apiClient.post<BuildResponse>("/dictionary/build", options);
    return response.data;
  },

  // Search (Fuzzy matching only)
  search: async (request: SearchRequest): Promise<SearchResponse> => {
    const response = await apiClient.post<SearchResponse>("/dictionary/search", request);
    return response.data;
  },

  // Term lookup with clarification support
  lookup: async (term: string, threshold: number = 85.0, limit: number = 10): Promise<LookupResponse> => {
    const response = await apiClient.post<LookupResponse>("/dictionary/lookup", {
      term,
      threshold,
      limit,
    });
    return response.data;
  },

  // Spell correction
  correctSpelling: async (query: string, threshold: number = 60.0, limit: number = 5): Promise<CorrectionResponse> => {
    const response = await apiClient.post<CorrectionResponse>("/dictionary/correct", {
      query,
      threshold,
      limit,
    });
    return response.data;
  },

  // Values
  getColumnValues: async (table: string, column: string, limit: number = 100, offset: number = 0): Promise<ColumnValues> => {
    const response = await apiClient.get<ColumnValues>(`/dictionary/values/${table}/${column}`, {
      params: { limit, offset },
    });
    return response.data;
  },

  // Tables
  getTables: async (): Promise<TablesResponse> => {
    const response = await apiClient.get<TablesResponse>("/dictionary/tables");
    return response.data;
  },

  // Schema Map
  getSchemaMap: async (): Promise<SchemaMap> => {
    const response = await apiClient.get<SchemaMap>("/dictionary/schema-map");
    return response.data;
  },

  // Statistics
  getStatistics: async (): Promise<DictionaryStatistics> => {
    const response = await apiClient.get<DictionaryStatistics>("/dictionary/statistics");
    return response.data;
  },
};
