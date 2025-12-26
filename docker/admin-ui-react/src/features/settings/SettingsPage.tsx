import React, { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Settings,
  Shield,
  Brain,
  Database,
  FileSpreadsheet,
  BookOpen,
  ScrollText,
  Gauge,
  Save,
  RotateCcw,
  Download,
  Upload,
  Check,
  AlertCircle,
  Eye,
  EyeOff,
  History,
} from "lucide-react";
import { settingsApi, type SettingValue } from "@/api/settings";
import { ConfirmDialog } from "@/components/common/ConfirmDialog";
import { useToast } from "@/components/common/Toast";
import { useThemeStore } from "@/stores/themeStore";
import { useDateFormat } from "@/hooks/useDateFormat";

// Icon mapping for categories
const categoryIcons: Record<string, React.ElementType> = {
  Settings: Settings,
  Shield: Shield,
  Brain: Brain,
  Database: Database,
  FileSpreadsheet: FileSpreadsheet,
  BookOpen: BookOpen,
  ScrollText: ScrollText,
  Gauge: Gauge,
};

export function SettingsPage() {
  const [activeCategory, setActiveCategory] = useState<string>("");
  const [pendingChanges, setPendingChanges] = useState<Record<string, Record<string, unknown>>>({});
  const [showResetDialog, setShowResetDialog] = useState(false);
  const [showResetCategoryDialog, setShowResetCategoryDialog] = useState(false);
  const [showExportDialog, setShowExportDialog] = useState(false);
  const [showImportDialog, setShowImportDialog] = useState(false);
  const [showHistoryPanel, setShowHistoryPanel] = useState(false);
  const queryClient = useQueryClient();
  const toast = useToast();

  // Fetch all settings
  const { data: settingsData, isLoading, error } = useQuery({
    queryKey: ["settings"],
    queryFn: settingsApi.getAll,
  });

  // Update setting mutation
  const updateMutation = useMutation({
    mutationFn: ({ category, key, value }: { category: string; key: string; value: unknown }) =>
      settingsApi.updateSetting(category, key, value),
    onSuccess: (_, variables) => {
      // Remove from pending changes
      setPendingChanges((prev) => {
        const updated = { ...prev };
        if (updated[variables.category]) {
          delete updated[variables.category][variables.key];
          if (Object.keys(updated[variables.category]).length === 0) {
            delete updated[variables.category];
          }
        }
        return updated;
      });
      queryClient.invalidateQueries({ queryKey: ["settings"] });
      toast.success("Setting updated successfully");
    },
    onError: (error: Error) => {
      toast.error("Failed to update setting", error.message);
    },
  });

  // Reset all mutation
  const resetAllMutation = useMutation({
    mutationFn: settingsApi.resetAll,
    onSuccess: () => {
      setPendingChanges({});
      queryClient.invalidateQueries({ queryKey: ["settings"] });
      setShowResetDialog(false);
      toast.success("All settings reset to defaults");
    },
  });

  // Reset category mutation
  const resetCategoryMutation = useMutation({
    mutationFn: (category: string) => settingsApi.resetCategory(category),
    onSuccess: () => {
      setPendingChanges((prev) => {
        const updated = { ...prev };
        delete updated[activeCategory];
        return updated;
      });
      queryClient.invalidateQueries({ queryKey: ["settings"] });
      setShowResetCategoryDialog(false);
      toast.success(`${activeCategory} settings reset to defaults`);
    },
  });

  // Set initial active category
  useEffect(() => {
    if (settingsData?.categories && settingsData.categories.length > 0 && !activeCategory) {
      setActiveCategory(settingsData.categories[0].id);
    }
  }, [settingsData, activeCategory]);

  const handleValueChange = (category: string, key: string, value: unknown) => {
    setPendingChanges((prev) => ({
      ...prev,
      [category]: {
        ...(prev[category] || {}),
        [key]: value,
      },
    }));
  };

  const handleSave = (category: string, key: string) => {
    const value = pendingChanges[category]?.[key];
    if (value !== undefined) {
      updateMutation.mutate({ category, key, value });
    }
  };

  const handleSaveAll = () => {
    Object.entries(pendingChanges).forEach(([category, settings]) => {
      Object.entries(settings).forEach(([key, value]) => {
        updateMutation.mutate({ category, key, value });
      });
    });
  };

  const handleExport = async () => {
    try {
      const exportData = await settingsApi.exportSettings();
      const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: "application/json" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `sage-settings-${new Date().toISOString().split("T")[0]}.json`;
      a.click();
      URL.revokeObjectURL(url);
      setShowExportDialog(false);
      toast.success("Settings exported successfully");
    } catch {
      toast.error("Failed to export settings");
    }
  };

  const hasPendingChanges = Object.keys(pendingChanges).length > 0;
  const currentCategory = settingsData?.categories.find((c) => c.id === activeCategory);

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-16">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-6 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
        <div className="flex items-center gap-2 text-red-600 dark:text-red-400">
          <AlertCircle className="w-5 h-5" />
          <span>Failed to load settings. Please try again.</span>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">Settings</h1>
          <p className="text-gray-500 dark:text-gray-400">
            Configure platform settings and preferences
          </p>
        </div>
        <div className="flex items-center gap-2">
          {hasPendingChanges && (
            <button
              onClick={handleSaveAll}
              disabled={updateMutation.isPending}
              className="btn btn-primary btn-md"
            >
              <Save className="w-4 h-4" />
              Save All Changes
            </button>
          )}
          <button
            onClick={() => setShowHistoryPanel(!showHistoryPanel)}
            className="btn btn-secondary btn-md"
          >
            <History className="w-4 h-4" />
            History
          </button>
          <button onClick={() => setShowExportDialog(true)} className="btn btn-secondary btn-md">
            <Download className="w-4 h-4" />
            Export
          </button>
          <button onClick={() => setShowImportDialog(true)} className="btn btn-secondary btn-md">
            <Upload className="w-4 h-4" />
            Import
          </button>
          <button onClick={() => setShowResetDialog(true)} className="btn btn-destructive btn-md">
            <RotateCcw className="w-4 h-4" />
            Reset All
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex gap-6">
        {/* Category Navigation */}
        <div className="w-64 flex-shrink-0">
          <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
            <div className="p-4 border-b border-gray-200 dark:border-gray-800">
              <h3 className="font-medium text-gray-900 dark:text-white">Categories</h3>
            </div>
            <nav className="p-2">
              {settingsData?.categories.map((category) => {
                const Icon = categoryIcons[category.icon] || Settings;
                const hasChanges = !!pendingChanges[category.id];
                return (
                  <button
                    key={category.id}
                    onClick={() => setActiveCategory(category.id)}
                    className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-left transition-colors ${
                      activeCategory === category.id
                        ? "bg-blue-50 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400"
                        : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800"
                    }`}
                  >
                    <Icon className="w-5 h-5" />
                    <span className="flex-1 text-sm font-medium">{category.name}</span>
                    {hasChanges && (
                      <span className="w-2 h-2 rounded-full bg-amber-500" title="Unsaved changes" />
                    )}
                  </button>
                );
              })}
            </nav>
          </div>
        </div>

        {/* Settings Form */}
        <div className="flex-1">
          {currentCategory && (
            <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800">
              <div className="p-6 border-b border-gray-200 dark:border-gray-800">
                <div className="flex items-center justify-between">
                  <div>
                    <h2 className="text-lg font-semibold text-gray-900 dark:text-white">
                      {currentCategory.name}
                    </h2>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                      {currentCategory.description}
                    </p>
                  </div>
                  <button
                    onClick={() => setShowResetCategoryDialog(true)}
                    className="btn btn-secondary btn-sm"
                  >
                    <RotateCcw className="w-4 h-4" />
                    Reset Category
                  </button>
                </div>
              </div>

              <div className="p-6 space-y-6">
                {currentCategory.settings.map((setting) => (
                  <SettingField
                    key={setting.key}
                    setting={setting}
                    pendingValue={pendingChanges[currentCategory.id]?.[setting.key]}
                    onChange={(value) => handleValueChange(currentCategory.id, setting.key, value)}
                    onSave={() => handleSave(currentCategory.id, setting.key)}
                    isSaving={updateMutation.isPending}
                  />
                ))}
              </div>
            </div>
          )}
        </div>

        {/* History Panel */}
        {showHistoryPanel && (
          <div className="w-80 flex-shrink-0">
            <HistoryPanel onClose={() => setShowHistoryPanel(false)} />
          </div>
        )}
      </div>

      {/* Dialogs */}
      <ConfirmDialog
        open={showResetDialog}
        onOpenChange={setShowResetDialog}
        onConfirm={() => resetAllMutation.mutate()}
        title="Reset All Settings"
        description="This will reset ALL settings across all categories to their default values. This action cannot be undone."
        confirmLabel="Reset All"
        variant="danger"
      />

      <ConfirmDialog
        open={showResetCategoryDialog}
        onOpenChange={setShowResetCategoryDialog}
        onConfirm={() => resetCategoryMutation.mutate(activeCategory)}
        title={`Reset ${currentCategory?.name}`}
        description={`This will reset all settings in "${currentCategory?.name}" to their default values.`}
        confirmLabel="Reset Category"
        variant="danger"
      />

      <ConfirmDialog
        open={showExportDialog}
        onOpenChange={setShowExportDialog}
        onConfirm={handleExport}
        title="Export Settings"
        description="Export all settings as a JSON file. Sensitive values like API keys will not be included."
        confirmLabel="Export"
        variant="default"
      />

      {showImportDialog && (
        <ImportDialog onClose={() => setShowImportDialog(false)} />
      )}
    </div>
  );
}

// Individual Setting Field Component
interface SettingFieldProps {
  setting: SettingValue;
  pendingValue?: unknown;
  onChange: (value: unknown) => void;
  onSave: () => void;
  isSaving: boolean;
}

function SettingField({ setting, pendingValue, onChange, onSave, isSaving }: SettingFieldProps) {
  const [showPassword, setShowPassword] = useState(false);
  const currentValue = pendingValue ?? setting.value;
  const hasChanged = pendingValue !== undefined;
  const setTheme = useThemeStore((state) => state.setTheme);
  const { formatDateTime } = useDateFormat();

  // Handle special settings that need immediate effects
  const handleChange = (value: unknown) => {
    onChange(value);

    // Apply theme change immediately
    if (setting.key === "default_theme" && typeof value === "string") {
      setTheme(value as "light" | "dark" | "system");
    }
  };

  const renderInput = () => {
    switch (setting.value_type) {
      case "string":
        return (
          <input
            type="text"
            value={currentValue as string}
            onChange={(e) => handleChange(e.target.value)}
            className="input w-full max-w-md"
          />
        );

      case "password":
        return (
          <div className="flex gap-2 max-w-md">
            <input
              type={showPassword ? "text" : "password"}
              value={currentValue as string}
              onChange={(e) => handleChange(e.target.value)}
              placeholder={setting.is_sensitive ? "Leave empty to keep current" : ""}
              className="input flex-1"
            />
            <button
              type="button"
              onClick={() => setShowPassword(!showPassword)}
              className="btn btn-secondary btn-sm"
            >
              {showPassword ? <EyeOff className="w-4 h-4" /> : <Eye className="w-4 h-4" />}
            </button>
          </div>
        );

      case "number":
        return (
          <input
            type="number"
            value={currentValue as number}
            onChange={(e) => handleChange(Number(e.target.value))}
            min={setting.min_value}
            max={setting.max_value}
            step={setting.max_value && setting.max_value <= 1 ? 0.1 : 1}
            className="input w-32"
          />
        );

      case "boolean":
        return (
          <label className="relative inline-flex items-center cursor-pointer">
            <input
              type="checkbox"
              checked={currentValue as boolean}
              onChange={(e) => handleChange(e.target.checked)}
              className="sr-only peer"
            />
            <div className="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 dark:peer-focus:ring-blue-800 rounded-full peer dark:bg-gray-700 peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all dark:border-gray-600 peer-checked:bg-blue-600"></div>
            <span className="ml-3 text-sm text-gray-600 dark:text-gray-400">
              {currentValue ? "Enabled" : "Disabled"}
            </span>
          </label>
        );

      case "enum":
        // For settings with many options, use a select dropdown
        if (setting.options && setting.options.length > 4) {
          return (
            <select
              value={currentValue as string}
              onChange={(e) => handleChange(e.target.value)}
              className="input w-full max-w-md"
            >
              {setting.options.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          );
        }
        // For few options, use button group
        return (
          <div className="flex flex-wrap gap-2">
            {setting.options?.map((option) => (
              <button
                key={option}
                onClick={() => handleChange(option)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${
                  currentValue === option
                    ? "bg-blue-600 text-white"
                    : "bg-gray-100 dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-700"
                }`}
              >
                {option}
              </button>
            ))}
          </div>
        );

      case "array":
        const arrayValue = Array.isArray(currentValue) ? currentValue : [];
        return (
          <div className="space-y-2 max-w-md">
            <div className="flex flex-wrap gap-2">
              {arrayValue.map((item: string, index: number) => (
                <span
                  key={index}
                  className="inline-flex items-center gap-1 px-2 py-1 bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300 rounded text-sm"
                >
                  {item}
                  <button
                    onClick={() => handleChange(arrayValue.filter((_, i) => i !== index))}
                    className="hover:text-blue-600"
                  >
                    &times;
                  </button>
                </span>
              ))}
            </div>
            <input
              type="text"
              placeholder="Type and press Enter to add"
              className="input w-full"
              onKeyDown={(e) => {
                if (e.key === "Enter" && (e.target as HTMLInputElement).value) {
                  e.preventDefault();
                  handleChange([...arrayValue, (e.target as HTMLInputElement).value]);
                  (e.target as HTMLInputElement).value = "";
                }
              }}
            />
          </div>
        );

      default:
        return <span className="text-gray-500">Unsupported type: {setting.value_type}</span>;
    }
  };

  return (
    <div className="pb-6 border-b border-gray-100 dark:border-gray-800 last:border-0 last:pb-0">
      <div className="flex items-start justify-between mb-2">
        <div>
          <label className="block text-sm font-medium text-gray-900 dark:text-white">
            {setting.label}
          </label>
          {setting.description && (
            <p className="text-sm text-gray-500 dark:text-gray-400">{setting.description}</p>
          )}
        </div>
        {hasChanged && (
          <button
            onClick={onSave}
            disabled={isSaving}
            className="btn btn-primary btn-sm"
          >
            {isSaving ? (
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white" />
            ) : (
              <Check className="w-4 h-4" />
            )}
            Save
          </button>
        )}
      </div>
      <div className="mt-2">{renderInput()}</div>
      {setting.min_value !== undefined && setting.max_value !== undefined && (
        <p className="text-xs text-gray-400 mt-1">
          Range: {setting.min_value} - {setting.max_value}
        </p>
      )}
      {setting.updated_at && (
        <p className="text-xs text-gray-400 mt-1">
          Last updated: {formatDateTime(setting.updated_at)}
          {setting.updated_by && ` by ${setting.updated_by}`}
        </p>
      )}
    </div>
  );
}

// History Panel Component
function HistoryPanel({ onClose }: { onClose: () => void }) {
  const { data: history, isLoading } = useQuery({
    queryKey: ["settingsHistory"],
    queryFn: () => settingsApi.getAuditHistory(undefined, undefined, 50),
  });
  const { formatDateTime } = useDateFormat();

  return (
    <div className="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-800 overflow-hidden">
      <div className="p-4 border-b border-gray-200 dark:border-gray-800 flex items-center justify-between">
        <h3 className="font-medium text-gray-900 dark:text-white">Change History</h3>
        <button onClick={onClose} className="text-gray-400 hover:text-gray-600">&times;</button>
      </div>
      <div className="max-h-[600px] overflow-y-auto">
        {isLoading ? (
          <div className="p-4 text-center text-gray-500">Loading...</div>
        ) : history && history.length > 0 ? (
          <div className="divide-y divide-gray-100 dark:divide-gray-800">
            {history.map((entry, index) => (
              <div key={index} className="p-3">
                <div className="text-sm font-medium text-gray-900 dark:text-white">
                  {entry.setting_category}/{entry.setting_key}
                </div>
                <div className="text-xs text-gray-500 mt-1">
                  {entry.changed_by && <span>{entry.changed_by} - </span>}
                  {formatDateTime(entry.changed_at)}
                </div>
                <div className="text-xs mt-1">
                  <span className="text-red-500 line-through">
                    {JSON.stringify(entry.old_value)}
                  </span>
                  {" → "}
                  <span className="text-green-500">{JSON.stringify(entry.new_value)}</span>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="p-4 text-center text-gray-500">No changes recorded</div>
        )}
      </div>
    </div>
  );
}

// Import Dialog Component
function ImportDialog({ onClose }: { onClose: () => void }) {
  const [file, setFile] = useState<File | null>(null);
  const [overwrite, setOverwrite] = useState(false);
  const [importing, setImporting] = useState(false);
  const queryClient = useQueryClient();
  const toast = useToast();

  const handleImport = async () => {
    if (!file) return;

    setImporting(true);
    try {
      const text = await file.text();
      const data = JSON.parse(text);
      const result = await settingsApi.importSettings(data.settings || data, overwrite);

      queryClient.invalidateQueries({ queryKey: ["settings"] });
      if (result.errors > 0) {
        toast.warning(`Imported ${result.imported} settings`, `Skipped: ${result.skipped}, Errors: ${result.errors}`);
      } else {
        toast.success(`Imported ${result.imported} settings`, `Skipped: ${result.skipped}`);
      }
      onClose();
    } catch {
      toast.error("Failed to import settings", "Invalid file format");
    } finally {
      setImporting(false);
    }
  };

  return (
    <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
      <div className="bg-white dark:bg-gray-900 rounded-xl shadow-lg max-w-md w-full mx-4">
        <div className="p-6 border-b border-gray-200 dark:border-gray-800">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white">Import Settings</h3>
        </div>
        <div className="p-6 space-y-4">
          <div>
            <label className="block text-sm font-medium mb-2">Select JSON File</label>
            <input
              type="file"
              accept=".json"
              onChange={(e) => setFile(e.target.files?.[0] || null)}
              className="w-full"
            />
          </div>
          <label className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={overwrite}
              onChange={(e) => setOverwrite(e.target.checked)}
            />
            <span className="text-sm">Overwrite existing values</span>
          </label>
        </div>
        <div className="p-6 border-t border-gray-200 dark:border-gray-800 flex justify-end gap-3">
          <button onClick={onClose} className="btn btn-secondary btn-md">
            Cancel
          </button>
          <button
            onClick={handleImport}
            disabled={!file || importing}
            className="btn btn-primary btn-md"
          >
            {importing ? "Importing..." : "Import"}
          </button>
        </div>
      </div>
    </div>
  );
}
