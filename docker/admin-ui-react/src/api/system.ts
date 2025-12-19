import apiClient from "./client";
import type { HealthStatus, SystemStats, SystemSettings, LLMSettings } from "@/types/api";

// API response wrapper type - backend wraps all responses
interface ApiResponse<T> {
  success: boolean;
  data: T;
  meta?: { timestamp: string };
}

export const systemApi = {
  getHealth: async (): Promise<HealthStatus> => {
    interface HealthResponse {
      status: string;
      timestamp: string;
    }
    const response = await apiClient.get<ApiResponse<HealthResponse>>("/system/health");
    // Transform to expected format
    return {
      status: response.data.data.status as "healthy" | "degraded" | "unhealthy",
      version: "1.0.0",
      uptime: 0,
      services: [],
    };
  },

  getStats: async (): Promise<SystemStats> => {
    // Backend uses /system/info instead of /stats
    interface InfoResponse {
      version: string;
      uptime: string;
      platform: string;
      disk_usage: { percent_used: number };
      memory_usage: { percent_used?: number };
    }
    const response = await apiClient.get<ApiResponse<InfoResponse>>("/system/info");
    // Transform to expected format
    return {
      cpu_percent: 0,
      memory_percent: response.data.data.memory_usage?.percent_used || 0,
      disk_percent: response.data.data.disk_usage?.percent_used || 0,
      active_sessions: 0,
    };
  },

  getSettings: async (): Promise<SystemSettings> => {
    // Backend uses /system/config instead of /settings
    const response = await apiClient.get<ApiResponse<Record<string, unknown>>>("/system/config");
    const config = response.data.data;
    return {
      site_name: "SAGE",
      site_description: "Study Analytics Generative Engine",
      maintenance_mode: false,
      allow_registration: false,
      session_timeout_minutes: (config.access_token_expire_minutes as number) || 60,
      max_upload_size_mb: 100,
      default_theme: "system",
    };
  },

  updateSettings: async (settings: Partial<SystemSettings>): Promise<SystemSettings> => {
    // Backend uses PUT /system/config with key/value
    if (settings.session_timeout_minutes !== undefined) {
      await apiClient.put("/system/config", null, {
        params: { key: "ACCESS_TOKEN_EXPIRE_MINUTES", value: String(settings.session_timeout_minutes) }
      });
    }
    // Return current settings
    return systemApi.getSettings();
  },

  getLLMSettings: async (): Promise<LLMSettings> => {
    // Get from config endpoint
    const response = await apiClient.get<ApiResponse<Record<string, unknown>>>("/system/config");
    const config = response.data.data;
    return {
      provider: "ollama",
      model: (config.llm_model as string) || "llama3.1:70b",
      base_url: "http://ollama:11434",
      temperature: 0.7,
      max_tokens: 4096,
    };
  },

  updateLLMSettings: async (settings: Partial<LLMSettings>): Promise<LLMSettings> => {
    // Update LLM model via config endpoint
    if (settings.model) {
      await apiClient.put("/system/config", null, {
        params: { key: "LLM_MODEL", value: settings.model }
      });
    }
    return systemApi.getLLMSettings();
  },

  // Cache Management
  getCacheStats: async (): Promise<CacheStats> => {
    const response = await apiClient.get<ApiResponse<CacheStats>>("/system/cache");
    return response.data.data;
  },

  clearCache: async (): Promise<{ message: string; entries_cleared: number }> => {
    const response = await apiClient.post<ApiResponse<{ message: string; entries_cleared: number }>>("/system/cache/clear");
    return response.data.data;
  },

  getCacheEntries: async (limit: number = 50): Promise<CacheEntry[]> => {
    const response = await apiClient.get<ApiResponse<CacheEntry[]>>("/system/cache/entries", {
      params: { limit }
    });
    return response.data.data;
  },

  // LLM Provider Management
  getLLMConfig: async (): Promise<LLMConfigResponse> => {
    const response = await apiClient.get<ApiResponse<LLMConfigResponse>>("/system/llm");
    return response.data.data;
  },

  getLLMProviders: async (): Promise<LLMProvidersResponse> => {
    const response = await apiClient.get<ApiResponse<LLMProvidersResponse>>("/system/llm/providers");
    return response.data.data;
  },

  setLLMProvider: async (provider: string, model?: string, apiKey?: string): Promise<SetProviderResponse> => {
    const params: Record<string, string> = { provider };
    if (model) params.model = model;
    if (apiKey) params.api_key = apiKey;

    const response = await apiClient.post<ApiResponse<SetProviderResponse>>("/system/llm/provider", null, { params });
    return response.data.data;
  },

  testLLMConnection: async (): Promise<LLMTestResponse> => {
    const response = await apiClient.post<ApiResponse<LLMTestResponse>>("/system/llm/test");
    return response.data.data;
  },

  getLLMAuditLog: async (limit: number = 100): Promise<LLMAuditResponse> => {
    const response = await apiClient.get<ApiResponse<LLMAuditResponse>>("/system/llm/audit", {
      params: { limit }
    });
    return response.data.data;
  },
};

// Cache types
export interface CacheStats {
  size: number;
  max_size: number;
  hits: number;
  misses: number;
  evictions: number;
  expirations: number;
  data_invalidations: number;
  hit_rate: number;
  hit_rate_str: string;
  data_version: string | null;
  db_path: string | null;
}

export interface CacheEntry {
  key: string;
  created_at: number;
  ttl_seconds: number;
  hit_count: number;
  is_expired: boolean;
  age_seconds: number;
}

// LLM Provider types
export interface LLMConfigResponse {
  provider: string;
  model: string;
  available_providers: Record<string, boolean>;
  settings: {
    temperature: number;
    max_tokens: number;
    timeout: number;
    safety_audit_enabled: boolean;
    block_pii: boolean;
  };
  claude: {
    model: string;
    api_key_configured: boolean;
  };
}

export interface LLMProviderInfo {
  id: string;
  name: string;
  description: string;
  available: boolean;
  is_local: boolean;
  requires_api_key: boolean;
  api_key_configured?: boolean;
  models: string[];
}

export interface LLMProvidersResponse {
  current_provider: string;
  providers: LLMProviderInfo[];
}

export interface SetProviderResponse {
  provider: string;
  model: string;
  available: boolean;
  message?: string;
  warning?: string;
}

export interface LLMTestResponse {
  provider: string;
  model: string;
  status: string;
  response_time_ms?: number;
  response_preview?: string;
  message: string;
  error?: string;
}

export interface LLMAuditEntry {
  timestamp: string;
  provider: string;
  model: string;
  prompt_hash: string;
  prompt_length: number;
  contains_schema_only: boolean;
  potential_pii_detected: boolean;
  pii_patterns_found: string[];
  blocked: boolean;
  block_reason?: string;
  prompt_preview?: string;
}

export interface LLMAuditResponse {
  entries: LLMAuditEntry[];
  total_found: number;
  audit_enabled: boolean;
  message?: string;
}
