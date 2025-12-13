import { useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { Save, TestTube, Settings, Bot, Shield, Palette } from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { useTheme } from "@/hooks/useTheme";
import { systemApi } from "@/api/system";
import type { SystemSettings, LLMSettings } from "@/types/api";

type Tab = "general" | "llm" | "security" | "appearance";

export function SettingsPage() {
  const [activeTab, setActiveTab] = useState<Tab>("general");
  const queryClient = useQueryClient();

  const { data: settings, isLoading: settingsLoading } = useQuery({
    queryKey: ["systemSettings"],
    queryFn: systemApi.getSettings,
  });

  const { data: llmSettings, isLoading: llmLoading } = useQuery({
    queryKey: ["llmSettings"],
    queryFn: systemApi.getLLMSettings,
  });

  const updateSettingsMutation = useMutation({
    mutationFn: systemApi.updateSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["systemSettings"] });
    },
  });

  const updateLLMMutation = useMutation({
    mutationFn: systemApi.updateLLMSettings,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["llmSettings"] });
    },
  });

  const testLLMMutation = useMutation({
    mutationFn: systemApi.testLLMConnection,
  });

  const tabs = [
    { id: "general", label: "General", icon: Settings },
    { id: "llm", label: "LLM Settings", icon: Bot },
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

      {activeTab === "llm" && (
        <LLMSettingsPanel
          settings={llmSettings}
          isLoading={llmLoading}
          onSave={(data) => updateLLMMutation.mutate(data)}
          onTest={() => testLLMMutation.mutate()}
          isSaving={updateLLMMutation.isPending}
          isTesting={testLLMMutation.isPending}
          testResult={testLLMMutation.data}
        />
      )}

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

interface LLMSettingsPanelProps {
  settings?: LLMSettings;
  isLoading: boolean;
  onSave: (data: Partial<LLMSettings>) => void;
  onTest: () => void;
  isSaving: boolean;
  isTesting: boolean;
  testResult?: { success: boolean; message: string };
}

function LLMSettingsPanel({
  settings,
  isLoading,
  onSave,
  onTest,
  isSaving,
  isTesting,
  testResult,
}: LLMSettingsPanelProps) {
  const [formData, setFormData] = useState<Partial<LLMSettings>>({
    provider: settings?.provider || "ollama",
    model: settings?.model || "llama3.2",
    base_url: settings?.base_url || "http://ollama:11434",
    api_key: settings?.api_key || "",
    temperature: settings?.temperature || 0.7,
    max_tokens: settings?.max_tokens || 2048,
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
    <WPBox title="LLM Configuration">
      <form onSubmit={handleSubmit} className="space-y-4 max-w-xl">
        <div>
          <label className="block text-sm font-medium mb-1">Provider</label>
          <select
            value={formData.provider}
            onChange={(e) =>
              setFormData({
                ...formData,
                provider: e.target.value as LLMSettings["provider"],
              })
            }
          >
            <option value="ollama">Ollama (Local)</option>
            <option value="openai">OpenAI</option>
            <option value="anthropic">Anthropic</option>
          </select>
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Model</label>
          <input
            type="text"
            value={formData.model}
            onChange={(e) => setFormData({ ...formData, model: e.target.value })}
            placeholder="e.g., llama3.2, gpt-4, claude-3-opus"
          />
        </div>
        {formData.provider === "ollama" && (
          <div>
            <label className="block text-sm font-medium mb-1">Base URL</label>
            <input
              type="text"
              value={formData.base_url}
              onChange={(e) => setFormData({ ...formData, base_url: e.target.value })}
              placeholder="http://ollama:11434"
            />
          </div>
        )}
        {(formData.provider === "openai" || formData.provider === "anthropic") && (
          <div>
            <label className="block text-sm font-medium mb-1">API Key</label>
            <input
              type="password"
              value={formData.api_key}
              onChange={(e) => setFormData({ ...formData, api_key: e.target.value })}
              placeholder="sk-..."
            />
          </div>
        )}
        <div>
          <label className="block text-sm font-medium mb-1">
            Temperature ({formData.temperature})
          </label>
          <input
            type="range"
            min="0"
            max="1"
            step="0.1"
            value={formData.temperature}
            onChange={(e) =>
              setFormData({ ...formData, temperature: Number(e.target.value) })
            }
            className="w-full"
          />
        </div>
        <div>
          <label className="block text-sm font-medium mb-1">Max Tokens</label>
          <input
            type="number"
            value={formData.max_tokens}
            onChange={(e) =>
              setFormData({ ...formData, max_tokens: Number(e.target.value) })
            }
            min={100}
            max={32000}
          />
        </div>

        {testResult && (
          <div
            className={`p-3 rounded ${
              testResult.success
                ? "bg-[rgba(0,163,42,0.1)] border border-[var(--success)] text-[var(--success)]"
                : "bg-[rgba(214,54,56,0.1)] border border-[var(--destructive)] text-[var(--destructive)]"
            }`}
          >
            {testResult.message}
          </div>
        )}

        <div className="flex items-center gap-2">
          <button type="submit" className="btn-primary" disabled={isSaving}>
            <Save className="w-4 h-4 mr-2 inline" />
            {isSaving ? "Saving..." : "Save Changes"}
          </button>
          <button
            type="button"
            className="btn-secondary"
            onClick={onTest}
            disabled={isTesting}
          >
            <TestTube className="w-4 h-4 mr-2 inline" />
            {isTesting ? "Testing..." : "Test Connection"}
          </button>
        </div>
      </form>
    </WPBox>
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
