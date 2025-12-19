import React, { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Save, TestTube, Settings, Bot, Shield, Palette, Database, Trash2, RefreshCw } from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { useTheme } from "@/hooks/useTheme";
import { systemApi } from "@/api/system";
import type { SystemSettings } from "@/types/api";

type Tab = "general" | "llm" | "security" | "appearance" | "cache";

export function SettingsPage() {
  const [activeTab, setActiveTab] = useState<Tab>("general");
  const queryClient = useQueryClient();

  const { data: settings, isLoading: settingsLoading } = useQuery({
    queryKey: ["systemSettings"],
    queryFn: systemApi.getSettings,
  });

  const updateSettingsMutation = useMutation({
    mutationFn: systemApi.updateSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["systemSettings"] });
    },
  });

  const tabs = [
    { id: "general", label: "General", icon: Settings },
    { id: "llm", label: "LLM Settings", icon: Bot },
    { id: "cache", label: "Cache", icon: Database },
    { id: "security", label: "Security", icon: Shield },
    { id: "appearance", label: "Appearance", icon: Palette },
  ] as const;

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div>
        <h1 className="text-2xl font-bold text-[var(--foreground)]">Settings</h1>
        <p className="text-[var(--foreground-muted)]">
          Configure system preferences and integrations
        </p>
      </div>

      {/* Tabs */}
      <div className="tabs-list">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`tab-trigger ${activeTab === tab.id ? "active" : ""}`}
            onClick={() => setActiveTab(tab.id)}
          >
            <tab.icon className="w-4 h-4 mr-2 inline" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {activeTab === "general" && (
        <GeneralSettings
          settings={settings}
          isLoading={settingsLoading}
          onSave={(data) => updateSettingsMutation.mutate(data)}
          isSaving={updateSettingsMutation.isPending}
        />
      )}

      {activeTab === "llm" && <LLMSettingsPanel />}

      {activeTab === "cache" && <CacheSettings />}

      {activeTab === "security" && <SecuritySettings />}

      {activeTab === "appearance" && <AppearanceSettings />}
    </div>
  );
}

interface GeneralSettingsProps {
  settings?: SystemSettings;
  isLoading: boolean;
  onSave: (data: Partial<SystemSettings>) => void;
  isSaving: boolean;
}

function GeneralSettings({
  settings,
  isLoading,
  onSave,
  isSaving,
}: GeneralSettingsProps) {
  const [formData, setFormData] = useState<Partial<SystemSettings>>({
    site_name: settings?.site_name || "SAGE",
    site_description: settings?.site_description || "",
    maintenance_mode: settings?.maintenance_mode || false,
    allow_registration: settings?.allow_registration || false,
    session_timeout_minutes: settings?.session_timeout_minutes || 60,
    max_upload_size_mb: settings?.max_upload_size_mb || 100,
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSave(formData);
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
      </div>
    );
  }

  return (
    <WPBox title="General Settings">
      <form onSubmit={handleSubmit} className="space-y-4 max-w-xl">
        <div>
          <label className="block text-sm font-medium mb-1">Site Name</label>
          <input
            type="text"
            value={formData.site_name}
            onChange={(e) => setFormData({ ...formData, site_name: e.target.value })}
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Site Description</label>
          <textarea
            value={formData.site_description}
            onChange={(e) =>
              setFormData({ ...formData, site_description: e.target.value })
            }
            rows={2}
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">
            Session Timeout (minutes)
          </label>
          <input
            type="number"
            value={formData.session_timeout_minutes}
            onChange={(e) =>
              setFormData({
                ...formData,
                session_timeout_minutes: Number(e.target.value),
              })
            }
            min={5}
            max={1440}
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">
            Max Upload Size (MB)
          </label>
          <input
            type="number"
            value={formData.max_upload_size_mb}
            onChange={(e) =>
              setFormData({
                ...formData,
                max_upload_size_mb: Number(e.target.value),
              })
            }
            min={1}
            max={1000}
          />
        </div>
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="maintenance"
            checked={formData.maintenance_mode}
            onChange={(e) =>
              setFormData({ ...formData, maintenance_mode: e.target.checked })
            }
          />
          <label htmlFor="maintenance" className="text-sm">
            Enable Maintenance Mode
          </label>
        </div>
        <div className="flex items-center gap-2">
          <input
            type="checkbox"
            id="registration"
            checked={formData.allow_registration}
            onChange={(e) =>
              setFormData({ ...formData, allow_registration: e.target.checked })
            }
          />
          <label htmlFor="registration" className="text-sm">
            Allow User Registration
          </label>
        </div>
        <button type="submit" className="btn btn-primary btn-md" disabled={isSaving}>
          <Save className="w-4 h-4" />
          {isSaving ? "Saving..." : "Save Changes"}
        </button>
      </form>
    </WPBox>
  );
}

// New LLM Settings Panel using provider system
function LLMSettingsPanel() {
  const queryClient = useQueryClient();
  const [selectedProvider, setSelectedProvider] = useState<string>("");
  const [selectedModel, setSelectedModel] = useState<string>("");
  const [apiKey, setApiKey] = useState<string>("");
  const [showApiKey, setShowApiKey] = useState(false);

  // Fetch current config
  const { data: llmConfig, isLoading: configLoading } = useQuery({
    queryKey: ["llmConfig"],
    queryFn: systemApi.getLLMConfig,
  });

  // Fetch available providers
  const { data: providersData, isLoading: providersLoading } = useQuery({
    queryKey: ["llmProviders"],
    queryFn: systemApi.getLLMProviders,
  });

  // Set provider mutation
  const setProviderMutation = useMutation({
    mutationFn: ({ provider, model, apiKey }: { provider: string; model?: string; apiKey?: string }) =>
      systemApi.setLLMProvider(provider, model, apiKey),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["llmConfig"] });
      queryClient.invalidateQueries({ queryKey: ["llmProviders"] });
    },
  });

  // Test connection mutation
  const testMutation = useMutation({
    mutationFn: systemApi.testLLMConnection,
  });

  // Initialize selected values when data loads
  React.useEffect(() => {
    if (providersData && !selectedProvider) {
      setSelectedProvider(providersData.current_provider);
    }
    if (llmConfig && !selectedModel) {
      setSelectedModel(llmConfig.model);
    }
  }, [providersData, llmConfig, selectedProvider, selectedModel]);

  const isLoading = configLoading || providersLoading;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
      </div>
    );
  }

  const providers = providersData?.providers || [];
  const currentProvider = providers.find(p => p.id === selectedProvider);

  const handleApplyChanges = () => {
    setProviderMutation.mutate({
      provider: selectedProvider,
      model: selectedModel || undefined,
      apiKey: apiKey || undefined,
    });
  };

  return (
    <div className="space-y-4">
      <WPBox title="LLM Provider">
        <div className="space-y-6">
          {/* Provider Selection */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {providers.map((provider) => (
              <div
                key={provider.id}
                className={`p-4 rounded-lg border-2 cursor-pointer transition-all ${
                  selectedProvider === provider.id
                    ? "border-[var(--primary)] bg-[rgba(0,112,243,0.1)]"
                    : "border-[var(--border)] hover:border-[var(--primary-muted)]"
                } ${!provider.available && provider.id !== "mock" ? "opacity-60" : ""}`}
                onClick={() => {
                  setSelectedProvider(provider.id);
                  if (provider.models.length > 0) {
                    setSelectedModel(provider.models[0]);
                  }
                }}
              >
                <div className="flex items-center justify-between mb-2">
                  <span className="font-medium">{provider.name}</span>
                  {provider.available ? (
                    <span className="px-2 py-0.5 text-xs rounded bg-[var(--success)] text-white">Available</span>
                  ) : provider.requires_api_key && !provider.api_key_configured ? (
                    <span className="px-2 py-0.5 text-xs rounded bg-[var(--warning)] text-black">Needs API Key</span>
                  ) : (
                    <span className="px-2 py-0.5 text-xs rounded bg-[var(--muted)] text-white">Unavailable</span>
                  )}
                </div>
                <p className="text-sm text-[var(--foreground-muted)]">{provider.description}</p>
                {provider.is_local && (
                  <div className="mt-2 flex items-center text-xs text-[var(--success)]">
                    <Shield className="w-3 h-3 mr-1" />
                    Data stays local
                  </div>
                )}
              </div>
            ))}
          </div>

          {/* Model Selection */}
          {currentProvider && (
            <div>
              <label className="block text-sm font-medium mb-2">Model</label>
              <select
                value={selectedModel}
                onChange={(e) => setSelectedModel(e.target.value)}
                className="w-full max-w-md"
              >
                {currentProvider.models.map((model) => (
                  <option key={model} value={model}>{model}</option>
                ))}
              </select>
            </div>
          )}

          {/* API Key Input (for Claude) */}
          {selectedProvider === "claude" && (
            <div>
              <label className="block text-sm font-medium mb-2">
                Anthropic API Key
                {llmConfig?.claude.api_key_configured && (
                  <span className="ml-2 text-xs text-[var(--success)]">(configured)</span>
                )}
              </label>
              <div className="flex gap-2 max-w-md">
                <input
                  type={showApiKey ? "text" : "password"}
                  value={apiKey}
                  onChange={(e) => setApiKey(e.target.value)}
                  placeholder={llmConfig?.claude.api_key_configured ? "Leave empty to keep current" : "sk-ant-..."}
                  className="flex-1"
                />
                <button
                  type="button"
                  className="btn btn-secondary btn-sm"
                  onClick={() => setShowApiKey(!showApiKey)}
                >
                  {showApiKey ? "Hide" : "Show"}
                </button>
              </div>
              <p className="text-xs text-[var(--muted)] mt-1">
                Get your API key from <a href="https://console.anthropic.com/" target="_blank" rel="noopener noreferrer" className="text-[var(--primary)]">console.anthropic.com</a>
              </p>
            </div>
          )}

          {/* Test Result */}
          {testMutation.data && (
            <div
              className={`p-3 rounded ${
                testMutation.data.status === "connected"
                  ? "bg-[rgba(0,163,42,0.1)] border border-[var(--success)]"
                  : "bg-[rgba(214,54,56,0.1)] border border-[var(--destructive)]"
              }`}
            >
              <div className="font-medium">{testMutation.data.message}</div>
              {testMutation.data.response_time_ms && (
                <div className="text-sm text-[var(--foreground-muted)]">
                  Response time: {testMutation.data.response_time_ms}ms
                </div>
              )}
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex items-center gap-3 pt-2">
            <button
              className="btn btn-primary btn-md"
              onClick={handleApplyChanges}
              disabled={setProviderMutation.isPending}
            >
              <Save className="w-4 h-4" />
              {setProviderMutation.isPending ? "Applying..." : "Apply Changes"}
            </button>
            <button
              className="btn btn-secondary btn-md"
              onClick={() => testMutation.mutate()}
              disabled={testMutation.isPending}
            >
              <TestTube className="w-4 h-4" />
              {testMutation.isPending ? "Testing..." : "Test Connection"}
            </button>
          </div>
        </div>
      </WPBox>

      {/* Safety Settings */}
      <WPBox title="Data Safety">
        <div className="space-y-4">
          <div className="p-4 rounded-lg bg-[rgba(0,163,42,0.1)] border border-[var(--success)]">
            <h4 className="font-medium text-[var(--success)] mb-2 flex items-center">
              <Shield className="w-4 h-4 mr-2" />
              Safety Layer Active
            </h4>
            <ul className="text-sm text-[var(--foreground-muted)] space-y-1">
              <li>• Only schema metadata is sent to the LLM (table/column names)</li>
              <li>• Actual patient data never leaves your infrastructure</li>
              <li>• PII patterns are detected and blocked automatically</li>
              <li>• All external API calls are logged for compliance</li>
            </ul>
          </div>

          {llmConfig?.settings && (
            <div className="grid grid-cols-2 gap-4">
              <div className="p-3 rounded bg-[var(--surface)] border border-[var(--border)]">
                <div className="text-sm font-medium">Safety Audit</div>
                <div className={llmConfig.settings.safety_audit_enabled ? "text-[var(--success)]" : "text-[var(--muted)]"}>
                  {llmConfig.settings.safety_audit_enabled ? "Enabled" : "Disabled"}
                </div>
              </div>
              <div className="p-3 rounded bg-[var(--surface)] border border-[var(--border)]">
                <div className="text-sm font-medium">PII Blocking</div>
                <div className={llmConfig.settings.block_pii ? "text-[var(--success)]" : "text-[var(--muted)]"}>
                  {llmConfig.settings.block_pii ? "Enabled" : "Disabled"}
                </div>
              </div>
            </div>
          )}
        </div>
      </WPBox>

      {/* Current Settings */}
      {llmConfig && (
        <WPBox title="Current Configuration">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
            <div>
              <div className="text-[var(--muted)]">Active Provider</div>
              <div className="font-medium">{llmConfig.provider}</div>
            </div>
            <div>
              <div className="text-[var(--muted)]">Model</div>
              <div className="font-medium">{llmConfig.model}</div>
            </div>
            <div>
              <div className="text-[var(--muted)]">Temperature</div>
              <div className="font-medium">{llmConfig.settings.temperature}</div>
            </div>
            <div>
              <div className="text-[var(--muted)]">Timeout</div>
              <div className="font-medium">{llmConfig.settings.timeout}s</div>
            </div>
          </div>
        </WPBox>
      )}
    </div>
  );
}

function CacheSettings() {
  const queryClient = useQueryClient();
  const [showConfirm, setShowConfirm] = useState(false);

  const { data: cacheStats, isLoading, refetch } = useQuery({
    queryKey: ["cacheStats"],
    queryFn: systemApi.getCacheStats,
    refetchInterval: 30000, // Refresh every 30 seconds
  });

  const clearCacheMutation = useMutation({
    mutationFn: systemApi.clearCache,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["cacheStats"] });
      setShowConfirm(false);
    },
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
      </div>
    );
  }

  const stats = cacheStats || {
    size: 0,
    max_size: 1000,
    hits: 0,
    misses: 0,
    hit_rate: 0,
    hit_rate_str: "0.0%",
    data_version: null,
    evictions: 0,
    expirations: 0,
    data_invalidations: 0,
  };

  const totalRequests = stats.hits + stats.misses;

  return (
    <div className="space-y-4">
      <WPBox title="Query Cache">
        <div className="space-y-6">
          {/* Cache Statistics Grid */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div className="p-4 rounded-lg bg-[var(--surface-2)] border border-[var(--border)]">
              <div className="text-2xl font-bold text-[var(--primary)]">{stats.size}</div>
              <div className="text-sm text-[var(--foreground-muted)]">Cached Queries</div>
              <div className="text-xs text-[var(--muted)] mt-1">Max: {stats.max_size}</div>
            </div>
            <div className="p-4 rounded-lg bg-[var(--surface-2)] border border-[var(--border)]">
              <div className="text-2xl font-bold text-[var(--success)]">{stats.hit_rate_str}</div>
              <div className="text-sm text-[var(--foreground-muted)]">Hit Rate</div>
              <div className="text-xs text-[var(--muted)] mt-1">{stats.hits} hits / {totalRequests} total</div>
            </div>
            <div className="p-4 rounded-lg bg-[var(--surface-2)] border border-[var(--border)]">
              <div className="text-2xl font-bold text-[var(--foreground)]">{stats.hits}</div>
              <div className="text-sm text-[var(--foreground-muted)]">Cache Hits</div>
              <div className="text-xs text-[var(--muted)] mt-1">Served from cache</div>
            </div>
            <div className="p-4 rounded-lg bg-[var(--surface-2)] border border-[var(--border)]">
              <div className="text-2xl font-bold text-[var(--warning)]">{stats.misses}</div>
              <div className="text-sm text-[var(--foreground-muted)]">Cache Misses</div>
              <div className="text-xs text-[var(--muted)] mt-1">Required LLM call</div>
            </div>
          </div>

          {/* Additional Stats */}
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <div className="p-3 rounded bg-[var(--surface)] border border-[var(--border)]">
              <div className="text-sm font-medium">Evictions</div>
              <div className="text-lg">{stats.evictions}</div>
            </div>
            <div className="p-3 rounded bg-[var(--surface)] border border-[var(--border)]">
              <div className="text-sm font-medium">Expirations</div>
              <div className="text-lg">{stats.expirations}</div>
            </div>
            <div className="p-3 rounded bg-[var(--surface)] border border-[var(--border)]">
              <div className="text-sm font-medium">Data Invalidations</div>
              <div className="text-lg">{stats.data_invalidations}</div>
            </div>
          </div>

          {/* Data Version */}
          {stats.data_version && (
            <div className="p-4 rounded-lg bg-[var(--surface)] border border-[var(--border)]">
              <div className="flex items-center gap-2 mb-2">
                <Database className="w-4 h-4 text-[var(--primary)]" />
                <span className="font-medium">Data Version Tracking</span>
              </div>
              <div className="text-sm text-[var(--foreground-muted)]">
                Current Version: <code className="px-2 py-0.5 rounded bg-[var(--surface-2)] text-[var(--primary)]">{stats.data_version}</code>
              </div>
              <p className="text-xs text-[var(--muted)] mt-2">
                Cache automatically invalidates when data changes (new files loaded, tables updated)
              </p>
            </div>
          )}

          {/* Actions */}
          <div className="flex items-center gap-3 pt-2">
            <button
              className="btn btn-secondary btn-md"
              onClick={() => refetch()}
            >
              <RefreshCw className="w-4 h-4" />
              Refresh Stats
            </button>

            {!showConfirm ? (
              <button
                className="btn btn-destructive btn-md"
                onClick={() => setShowConfirm(true)}
                disabled={stats.size === 0}
              >
                <Trash2 className="w-4 h-4" />
                Clear Cache
              </button>
            ) : (
              <div className="flex items-center gap-2">
                <span className="text-sm text-[var(--foreground-muted)]">Clear all {stats.size} entries?</span>
                <button
                  className="btn btn-destructive btn-sm"
                  onClick={() => clearCacheMutation.mutate()}
                  disabled={clearCacheMutation.isPending}
                >
                  {clearCacheMutation.isPending ? "Clearing..." : "Yes, Clear"}
                </button>
                <button
                  className="btn btn-secondary btn-sm"
                  onClick={() => setShowConfirm(false)}
                >
                  Cancel
                </button>
              </div>
            )}
          </div>

          {/* Info */}
          <div className="p-4 rounded-lg bg-[rgba(0,112,243,0.1)] border border-[var(--primary)]">
            <h4 className="font-medium text-[var(--primary)] mb-2">About Query Cache</h4>
            <ul className="text-sm text-[var(--foreground-muted)] space-y-1">
              <li>• Caches query results to avoid repeated LLM calls</li>
              <li>• Entries expire after 1 hour (TTL)</li>
              <li>• Cache automatically clears when clinical data changes</li>
              <li>• Similar queries (case/punctuation differences) share cache entries</li>
            </ul>
          </div>
        </div>
      </WPBox>
    </div>
  );
}

function SecuritySettings() {
  return (
    <WPBox title="Security Settings">
      <div className="space-y-4 max-w-xl">
        <div>
          <label className="block text-sm font-medium mb-1">
            Password Policy
          </label>
          <div className="space-y-2">
            <label className="flex items-center gap-2">
              <input type="checkbox" defaultChecked />
              <span className="text-sm">Require minimum 8 characters</span>
            </label>
            <label className="flex items-center gap-2">
              <input type="checkbox" defaultChecked />
              <span className="text-sm">Require uppercase and lowercase</span>
            </label>
            <label className="flex items-center gap-2">
              <input type="checkbox" defaultChecked />
              <span className="text-sm">Require numbers</span>
            </label>
            <label className="flex items-center gap-2">
              <input type="checkbox" />
              <span className="text-sm">Require special characters</span>
            </label>
          </div>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">
            Login Attempts
          </label>
          <input
            type="number"
            defaultValue={5}
            min={3}
            max={10}
            className="w-32"
          />
          <p className="text-xs text-[var(--muted)] mt-1">
            Maximum failed login attempts before lockout
          </p>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">
            Lockout Duration (minutes)
          </label>
          <input
            type="number"
            defaultValue={15}
            min={5}
            max={60}
            className="w-32"
          />
        </div>
        <button className="btn-primary">
          <Save className="w-4 h-4 mr-2 inline" />
          Save Changes
        </button>
      </div>
    </WPBox>
  );
}

function AppearanceSettings() {
  const { theme, setTheme } = useTheme();

  return (
    <WPBox title="Appearance">
      <div className="space-y-4 max-w-xl">
        <div>
          <label className="block text-sm font-medium mb-1">Theme</label>
          <select
            value={theme}
            onChange={(e) => setTheme(e.target.value as "light" | "dark" | "system")}
          >
            <option value="system">System Default</option>
            <option value="light">Light</option>
            <option value="dark">Dark</option>
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium mb-3">Preview</label>
          <div className="grid grid-cols-2 gap-4">
            <div className="p-4 bg-white border border-gray-200 rounded">
              <div className="h-4 w-24 bg-gray-800 rounded mb-2"></div>
              <div className="h-2 w-full bg-gray-200 rounded mb-1"></div>
              <div className="h-2 w-3/4 bg-gray-200 rounded"></div>
              <p className="text-xs text-gray-500 mt-2">Light Theme</p>
            </div>
            <div className="p-4 bg-gray-800 border border-gray-700 rounded">
              <div className="h-4 w-24 bg-white rounded mb-2"></div>
              <div className="h-2 w-full bg-gray-600 rounded mb-1"></div>
              <div className="h-2 w-3/4 bg-gray-600 rounded"></div>
              <p className="text-xs text-gray-400 mt-2">Dark Theme</p>
            </div>
          </div>
        </div>
      </div>
    </WPBox>
  );
}
