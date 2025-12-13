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

  testLLMConnection: async (): Promise<{ success: boolean; message: string }> => {
    // Try to reach Ollama through health detailed
    try {
      await apiClient.get<ApiResponse<{ services: Record<string, string> }>>("/system/health/detailed");
      return { success: true, message: "LLM service is reachable" };
    } catch {
      return { success: false, message: "LLM service is not reachable" };
    }
  },
};
