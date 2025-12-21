import apiClient from "./client";

// ============================================================================
// Types
// ============================================================================

export interface DocumentSection {
  heading: string;
  content: string;
  level: number;
}

export interface DocumentSummary {
  id: string;
  title: string;
  category: string;
  path: string;
  summary?: string;
}

export interface DocumentDetail {
  id: string;
  title: string;
  category: string;
  path: string;
  summary: string;
  sections: DocumentSection[];
  keywords: string[];
  word_count: number;
}

export interface SearchResult {
  doc_id: string;
  title: string;
  category: string;
  path: string;
  relevance_score: number;
  matched_keywords: string[];
  summary: string;
  sections: DocumentSection[];
}

export interface CategoryInfo {
  name: string;
  document_count: number;
  documents: DocumentSummary[];
}

export interface CategoriesResponse {
  categories: CategoryInfo[];
  total_documents: number;
}

export interface SearchResponse {
  query: string;
  results: SearchResult[];
  total_results: number;
}

export interface AskResponse {
  query: string;
  is_meta_query: boolean;
  answer: string;
  sources: DocumentSummary[];
}

export interface DocsStats {
  total_documents: number;
  total_categories: number;
  total_keywords: number;
  generated_at: string;
}

export interface RawMarkdownResponse {
  path: string;
  content: string;
}

// ============================================================================
// API Client
// ============================================================================

export const docsApi = {
  /**
   * Get documentation statistics
   */
  getStats: async (): Promise<DocsStats> => {
    const response = await apiClient.get<DocsStats>("/docs/stats");
    return response.data;
  },

  /**
   * List all documentation categories
   */
  getCategories: async (): Promise<CategoriesResponse> => {
    const response = await apiClient.get<CategoriesResponse>("/docs/categories");
    return response.data;
  },

  /**
   * List all documents, optionally filtered by category
   */
  getDocuments: async (category?: string): Promise<DocumentSummary[]> => {
    const response = await apiClient.get<DocumentSummary[]>("/docs/documents", {
      params: category ? { category } : undefined,
    });
    return response.data;
  },

  /**
   * Get a specific document by ID
   */
  getDocument: async (docId: string): Promise<DocumentDetail> => {
    const response = await apiClient.get<DocumentDetail>(`/docs/documents/${docId}`);
    return response.data;
  },

  /**
   * Search documentation
   */
  search: async (query: string, limit: number = 10): Promise<SearchResponse> => {
    const response = await apiClient.get<SearchResponse>("/docs/search", {
      params: { q: query, limit },
    });
    return response.data;
  },

  /**
   * Ask a question about SAGE
   */
  ask: async (query: string): Promise<AskResponse> => {
    const response = await apiClient.get<AskResponse>("/docs/ask", {
      params: { q: query },
    });
    return response.data;
  },

  /**
   * Get raw markdown content
   */
  getRawContent: async (docPath: string): Promise<RawMarkdownResponse> => {
    const response = await apiClient.get<RawMarkdownResponse>(`/docs/content/${docPath}`);
    return response.data;
  },
};

export default docsApi;
