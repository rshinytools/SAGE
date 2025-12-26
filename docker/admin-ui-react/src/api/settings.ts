import apiClient from "./client";

// Types
export interface SettingValue {
  key: string;
  value: unknown;
  value_type: "string" | "number" | "boolean" | "enum" | "array" | "password";
  category: string;
  label: string;
  description?: string;
  is_sensitive: boolean;
  options?: string[];
  min_value?: number;
  max_value?: number;
  updated_at?: string;
  updated_by?: string;
}

export interface SettingCategory {
  id: string;
  name: string;
  description: string;
  icon: string;
  order: number;
  settings: SettingValue[];
}

export interface SettingsResponse {
  categories: SettingCategory[];
}

export interface SettingUpdateResponse {
  success: boolean;
  message: string;
  category: string;
  key: string;
  value: unknown;
}

export interface ResetResponse {
  success: boolean;
  message: string;
  category?: string;
}

export interface ImportResponse {
  success: boolean;
  imported: number;
  skipped: number;
  errors: number;
}

export interface AuditEntry {
  setting_category: string;
  setting_key: string;
  old_value: unknown;
  new_value: unknown;
  changed_by?: string;
  changed_at: string;
}

export interface ExportData {
  version: string;
  exported_at: string;
  exported_by: string;
  settings: Record<string, Record<string, unknown>>;
}

// API Functions
export const settingsApi = {
  /**
   * Get all settings grouped by category
   */
  getAll: async (): Promise<SettingsResponse> => {
    const response = await apiClient.get<SettingsResponse>("/settings");
    return response.data;
  },

  /**
   * Get settings for a specific category
   */
  getCategory: async (categoryId: string): Promise<SettingCategory> => {
    const response = await apiClient.get<SettingCategory>(`/settings/${categoryId}`);
    return response.data;
  },

  /**
   * Get a single setting value
   */
  getSetting: async (category: string, key: string): Promise<unknown> => {
    const response = await apiClient.get<{ success: boolean; data: { value: unknown } }>(
      `/settings/${category}/${key}`
    );
    return response.data.data.value;
  },

  /**
   * Update a setting value
   */
  updateSetting: async (
    category: string,
    key: string,
    value: unknown
  ): Promise<SettingUpdateResponse> => {
    const response = await apiClient.put<SettingUpdateResponse>(
      `/settings/${category}/${key}`,
      { value }
    );
    return response.data;
  },

  /**
   * Reset all settings to defaults
   */
  resetAll: async (): Promise<ResetResponse> => {
    const response = await apiClient.post<ResetResponse>("/settings/reset");
    return response.data;
  },

  /**
   * Reset a category to defaults
   */
  resetCategory: async (category: string): Promise<ResetResponse> => {
    const response = await apiClient.post<ResetResponse>(`/settings/${category}/reset`);
    return response.data;
  },

  /**
   * Export all settings as JSON
   */
  exportSettings: async (): Promise<ExportData> => {
    const response = await apiClient.get<{ success: boolean; data: ExportData }>(
      "/settings/export/json"
    );
    return response.data.data;
  },

  /**
   * Import settings from JSON
   */
  importSettings: async (
    settings: Record<string, Record<string, unknown>>,
    overwrite: boolean = false
  ): Promise<ImportResponse> => {
    const response = await apiClient.post<ImportResponse>("/settings/import", {
      settings,
      overwrite,
    });
    return response.data;
  },

  /**
   * Get setting change history
   */
  getAuditHistory: async (
    category?: string,
    key?: string,
    limit: number = 100
  ): Promise<AuditEntry[]> => {
    const params = new URLSearchParams();
    if (category) params.append("category", category);
    if (key) params.append("key", key);
    params.append("limit", String(limit));

    const response = await apiClient.get<{ success: boolean; data: AuditEntry[] }>(
      `/settings/audit/history?${params.toString()}`
    );
    return response.data.data;
  },
};

export default settingsApi;
