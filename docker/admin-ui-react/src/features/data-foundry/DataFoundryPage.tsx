import { useState, useCallback, useRef } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Upload,
  Download,
  Trash2,
  Eye,
  Table,
  FileSpreadsheet,
  Database,
  History,
  GitBranch,
  AlertTriangle,
  CheckCircle2,
  Clock,
  XCircle,
  RefreshCw,
  Play,
  FileText,
  HardDrive,
  Layers,
  ArrowUpDown,
  Plus,
  Minus,
  Activity,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { dataApi } from "@/api/data";
import type {
  FileInfo,
  FileRecord,
  ProgressEvent,
  SchemaChangeEvent,
  ProcessingCompleteEvent,
} from "@/api/data";
import { formatBytes, formatDateTime } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";

type Tab = "files" | "tables" | "upload" | "history" | "schema" | "status";

interface ProcessingState {
  isProcessing: boolean;
  filename: string | null;
  progress: number;
  step: string;
  message: string;
  schemaChanges: SchemaChangeEvent | null;
  error: string | null;
  completed: ProcessingCompleteEvent | null;
}

export function DataFoundryPage() {
  const [activeTab, setActiveTab] = useState<Tab>("files");
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [dragActive, setDragActive] = useState(false);
  const [previewTable, setPreviewTable] = useState<string | null>(null);
  const [previewData, setPreviewData] = useState<{ columns: string[]; data: Record<string, unknown>[] } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [deleteTableName, setDeleteTableName] = useState<string | null>(null);
  const [deleteWithHistory, setDeleteWithHistory] = useState(false);
  const [compareVersions, setCompareVersions] = useState<{ v1: number; v2: number } | null>(null);
  const [exportTableName, setExportTableName] = useState<string | null>(null);
  const [exportFormat, setExportFormat] = useState<"csv" | "parquet">("csv");
  const [exporting, setExporting] = useState(false);
  const [rollbackTarget, setRollbackTarget] = useState<{ table: string; version: number } | null>(null);
  const [processingState, setProcessingState] = useState<ProcessingState>({
    isProcessing: false,
    filename: null,
    progress: 0,
    step: "",
    message: "",
    schemaChanges: null,
    error: null,
    completed: null,
  });
  const fileInputRef = useRef<HTMLInputElement>(null);
  const queryClient = useQueryClient();

  // Queries
  const { data: status, isLoading: statusLoading } = useQuery({
    queryKey: ["dataFactoryStatus"],
    queryFn: dataApi.getStatus,
    refetchInterval: 30000,
  });

  const { data: files, isLoading: filesLoading, refetch: refetchFiles } = useQuery({
    queryKey: ["dataFiles"],
    queryFn: dataApi.getFiles,
  });

  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ["tables"],
    queryFn: dataApi.getTables,
  });

  const { data: fileHistory, isLoading: historyLoading } = useQuery({
    queryKey: ["fileHistory"],
    queryFn: () => dataApi.getFileHistory({ limit: 100 }),
  });

  const { data: schemaVersions } = useQuery({
    queryKey: ["schemaVersions", selectedTable],
    queryFn: () => dataApi.getSchemaVersions(selectedTable!),
    enabled: !!selectedTable && activeTab === "schema",
  });

  // Mutations
  const uploadMutation = useMutation({
    mutationFn: (file: File) =>
      dataApi.uploadFile(file, {
        onProgress: setUploadProgress,
        blockOnBreaking: true,
      }),
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ["dataFiles"] });
      setUploadProgress(0);

      if (result.schema_diff?.has_changes) {
        // Show schema change notification
        setProcessingState((prev) => ({
          ...prev,
          schemaChanges: {
            table: result.table_name,
            severity: result.schema_diff!.severity,
            added_columns: result.schema_diff!.added_columns,
            removed_columns: result.schema_diff!.removed_columns,
            changes: result.schema_diff!.type_changes.map((tc) => ({
              column: tc.column,
              type: "type_changed",
              old: tc.old,
              new: tc.new,
            })),
          },
        }));
      }
    },
  });

  const processMutation = useMutation({
    mutationFn: (files?: string[]) => dataApi.processFiles(files, true),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tables"] });
      queryClient.invalidateQueries({ queryKey: ["dataFiles"] });
      queryClient.invalidateQueries({ queryKey: ["fileHistory"] });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: dataApi.deleteFile,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["dataFiles"] });
    },
  });

  const deleteTableMutation = useMutation({
    mutationFn: ({ name, deleteHistory }: { name: string; deleteHistory: boolean }) =>
      dataApi.deleteTable(name, deleteHistory),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["tables"] });
      queryClient.invalidateQueries({ queryKey: ["schemaVersions"] });
      queryClient.invalidateQueries({ queryKey: ["dataFactoryStatus"] });
      setDeleteTableName(null);
      setDeleteWithHistory(false);
    },
  });

  const rollbackMutation = useMutation({
    mutationFn: ({ table, version }: { table: string; version: number }) =>
      dataApi.rollbackSchema(table, version),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["schemaVersions"] });
      setRollbackTarget(null);
    },
  });

  // Process file with SSE streaming
  const processFileWithStream = useCallback((filename: string) => {
    setProcessingState({
      isProcessing: true,
      filename,
      progress: 0,
      step: "starting",
      message: "Starting processing...",
      schemaChanges: null,
      error: null,
      completed: null,
    });

    const eventSource = dataApi.processFileWithStream(filename, {
      blockOnBreaking: true,
      onProgress: (event: ProgressEvent) => {
        setProcessingState((prev) => ({
          ...prev,
          progress: event.progress,
          step: event.step,
          message: event.message,
        }));
      },
      onSchemaChange: (event: SchemaChangeEvent) => {
        setProcessingState((prev) => ({
          ...prev,
          schemaChanges: event,
        }));
      },
      onComplete: (event: ProcessingCompleteEvent) => {
        setProcessingState((prev) => ({
          ...prev,
          isProcessing: false,
          progress: 100,
          completed: event,
        }));
        queryClient.invalidateQueries({ queryKey: ["tables"] });
        queryClient.invalidateQueries({ queryKey: ["dataFiles"] });
        queryClient.invalidateQueries({ queryKey: ["fileHistory"] });
      },
      onError: (error) => {
        setProcessingState((prev) => ({
          ...prev,
          isProcessing: false,
          error: error.message,
        }));
      },
      onBlocked: (reason) => {
        setProcessingState((prev) => ({
          ...prev,
          isProcessing: false,
          error: `Blocked: ${reason.reason} (${reason.severity})`,
        }));
      },
    });

    return eventSource;
  }, [queryClient]);

  // Drag and drop handlers
  const handleDrag = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setDragActive(false);

      const files = Array.from(e.dataTransfer.files);
      if (files.length > 0) {
        uploadMutation.mutate(files[0]);
      }
    },
    [uploadMutation]
  );

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      uploadMutation.mutate(file);
    }
  };

  // Preview table data
  const handlePreview = async (tableName: string) => {
    setPreviewTable(tableName);
    setPreviewLoading(true);
    try {
      const result = await dataApi.getTablePreview(tableName, 1, 50);
      setPreviewData({
        columns: Object.keys(result.items[0] || {}),
        data: result.items,
      });
    } catch (error) {
      console.error("Preview error:", error);
      setPreviewData(null);
    } finally {
      setPreviewLoading(false);
    }
  };

  // Export table - show format selector modal
  const handleExport = (tableName: string) => {
    setExportTableName(tableName);
    setExportFormat("csv"); // default to CSV
  };

  // Actually export the table with selected format
  const executeExport = async () => {
    if (!exportTableName) return;
    setExporting(true);
    try {
      const blob = await dataApi.exportTable(exportTableName, exportFormat);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${exportTableName}.${exportFormat}`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      setExportTableName(null);
    } catch (error) {
      console.error("Export error:", error);
      alert("Export failed: " + (error as Error).message);
    } finally {
      setExporting(false);
    }
  };

  // Column definitions
  const fileColumns: ColumnDef<FileInfo>[] = [
    {
      accessorKey: "filename",
      header: "File",
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <FileText className="w-4 h-4 text-[var(--muted)]" />
          <span className="font-medium">{row.original.filename}</span>
        </div>
      ),
    },
    {
      accessorKey: "table_name",
      header: "Table",
      cell: ({ row }) => (
        <span className="font-mono text-sm bg-[var(--muted)]/10 px-2 py-0.5 rounded">
          {row.original.table_name}
        </span>
      ),
    },
    {
      accessorKey: "type",
      header: "Format",
      cell: ({ row }) => (
        <span className="uppercase text-xs font-medium text-[var(--muted)]">
          {row.original.type}
        </span>
      ),
    },
    {
      accessorKey: "size",
      header: "Size",
      cell: ({ row }) => formatBytes(row.original.size),
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => {
        const status = row.original.status;
        const statusConfig: Record<string, { icon: typeof CheckCircle2; color: string }> = {
          completed: { icon: CheckCircle2, color: "text-green-500" },
          pending: { icon: Clock, color: "text-yellow-500" },
          failed: { icon: XCircle, color: "text-red-500" },
          processing: { icon: RefreshCw, color: "text-blue-500" },
        };
        const config = statusConfig[status] || statusConfig.pending;
        const Icon = config.icon;
        return (
          <div className={`flex items-center gap-1.5 ${config.color}`}>
            <Icon className={`w-4 h-4 ${status === "processing" ? "animate-spin" : ""}`} />
            <span className="capitalize text-sm">{status}</span>
          </div>
        );
      },
    },
    {
      accessorKey: "row_count",
      header: "Rows",
      cell: ({ row }) =>
        row.original.row_count?.toLocaleString() || "-",
    },
    {
      id: "actions",
      header: "Actions",
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          {row.original.status === "pending" && (
            <button
              className="p-1.5 hover:bg-[var(--primary)]/10 rounded text-[var(--primary)]"
              onClick={() => processFileWithStream(row.original.filename)}
              title="Process"
            >
              <Play className="w-4 h-4" />
            </button>
          )}
          <button
            className="p-1.5 hover:bg-red-500/10 rounded text-red-500"
            onClick={() => {
              if (confirm(`Delete file "${row.original.filename}"?`)) {
                deleteMutation.mutate(row.original.filename);
              }
            }}
            title="Delete"
          >
            <Trash2 className="w-4 h-4" />
          </button>
        </div>
      ),
    },
  ];

  const historyColumns: ColumnDef<FileRecord>[] = [
    {
      accessorKey: "filename",
      header: "File",
    },
    {
      accessorKey: "table_name",
      header: "Table",
      cell: ({ row }) => (
        <span className="font-mono text-sm">{row.original.table_name}</span>
      ),
    },
    {
      accessorKey: "file_format",
      header: "Format",
      cell: ({ row }) => (
        <span className="uppercase text-xs">{row.original.file_format}</span>
      ),
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => {
        const status = row.original.status;
        const colors: Record<string, string> = {
          completed: "bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400",
          failed: "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-400",
          pending: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-400",
          archived: "bg-gray-100 text-gray-800 dark:bg-gray-900/30 dark:text-gray-400",
        };
        return (
          <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${colors[status] || colors.pending}`}>
            {status}
          </span>
        );
      },
    },
    {
      accessorKey: "row_count",
      header: "Rows",
      cell: ({ row }) => row.original.row_count?.toLocaleString() || "-",
    },
    {
      accessorKey: "schema_version",
      header: "Schema Ver.",
      cell: ({ row }) => row.original.schema_version ? `v${row.original.schema_version}` : "-",
    },
    {
      accessorKey: "uploaded_at",
      header: "Uploaded",
      cell: ({ row }) => formatDateTime(row.original.uploaded_at),
    },
  ];

  const tabs = [
    { id: "files" as const, label: "Source Files", icon: FileSpreadsheet },
    { id: "tables" as const, label: "Tables", icon: Table },
    { id: "upload" as const, label: "Upload", icon: Upload },
    { id: "history" as const, label: "History", icon: History },
    { id: "schema" as const, label: "Schema", icon: GitBranch },
    { id: "status" as const, label: "Status", icon: Activity },
  ];

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Data Foundry
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Factory 1: Transform source files to DuckDB tables
          </p>
        </div>
        <div className="flex items-center gap-3">
          {status?.file_statistics && (
            <div className="flex items-center gap-4 text-sm">
              <div className="flex items-center gap-1.5">
                <HardDrive className="w-4 h-4 text-[var(--muted)]" />
                <span>{status.file_statistics.total_files} files</span>
              </div>
              <div className="flex items-center gap-1.5">
                <Layers className="w-4 h-4 text-[var(--muted)]" />
                <span>{status.file_statistics.unique_tables} tables</span>
              </div>
              <div className="flex items-center gap-1.5">
                <Database className="w-4 h-4 text-[var(--muted)]" />
                <span>{status.file_statistics.total_rows_processed.toLocaleString()} rows</span>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Processing Progress Modal */}
      {processingState.isProcessing && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-[var(--card)] rounded-lg shadow-xl p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold mb-4">
              Processing {processingState.filename}
            </h3>
            <div className="space-y-4">
              <div>
                <div className="flex justify-between text-sm mb-2">
                  <span className="text-[var(--muted)]">{processingState.message}</span>
                  <span className="font-medium">{processingState.progress}%</span>
                </div>
                <div className="h-2 bg-[var(--muted)]/20 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-[var(--primary)] rounded-full transition-all duration-300"
                    style={{ width: `${processingState.progress}%` }}
                  />
                </div>
              </div>
              {processingState.schemaChanges && (
                <div className="p-3 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg">
                  <div className="flex items-center gap-2 text-yellow-700 dark:text-yellow-400 mb-2">
                    <AlertTriangle className="w-4 h-4" />
                    <span className="font-medium">Schema Changes Detected</span>
                  </div>
                  <div className="text-sm space-y-1">
                    {processingState.schemaChanges.added_columns.length > 0 && (
                      <div className="flex items-center gap-2 text-green-600">
                        <Plus className="w-3 h-3" />
                        Added: {processingState.schemaChanges.added_columns.join(", ")}
                      </div>
                    )}
                    {processingState.schemaChanges.removed_columns.length > 0 && (
                      <div className="flex items-center gap-2 text-red-600">
                        <Minus className="w-3 h-3" />
                        Removed: {processingState.schemaChanges.removed_columns.join(", ")}
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Processing Complete/Error Toast */}
      {processingState.completed && !processingState.isProcessing && (
        <div className="bg-green-100 dark:bg-green-900/30 border border-green-500 rounded-lg p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <CheckCircle2 className="w-5 h-5 text-green-500" />
            <div>
              <p className="font-medium text-green-700 dark:text-green-400">
                Processing Complete
              </p>
              <p className="text-sm text-green-600 dark:text-green-500">
                Table {processingState.completed.table}: {processingState.completed.rows.toLocaleString()} rows,{" "}
                {processingState.completed.columns} columns (Schema v{processingState.completed.schema_version})
              </p>
            </div>
          </div>
          <button
            onClick={() => setProcessingState((prev) => ({ ...prev, completed: null }))}
            className="text-green-500 hover:text-green-700"
          >
            <XCircle className="w-5 h-5" />
          </button>
        </div>
      )}

      {processingState.error && !processingState.isProcessing && (
        <div className="bg-red-100 dark:bg-red-900/30 border border-red-500 rounded-lg p-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <XCircle className="w-5 h-5 text-red-500" />
            <div>
              <p className="font-medium text-red-700 dark:text-red-400">Processing Failed</p>
              <p className="text-sm text-red-600 dark:text-red-500">{processingState.error}</p>
            </div>
          </div>
          <button
            onClick={() => setProcessingState((prev) => ({ ...prev, error: null }))}
            className="text-red-500 hover:text-red-700"
          >
            <XCircle className="w-5 h-5" />
          </button>
        </div>
      )}

      {/* Preview Modal */}
      {previewTable && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-[var(--card)] rounded-lg shadow-xl w-full max-w-5xl max-h-[80vh] flex flex-col">
            <div className="flex items-center justify-between p-4 border-b border-[var(--border)]">
              <h3 className="text-lg font-semibold">
                Preview: {previewTable}
              </h3>
              <button
                onClick={() => {
                  setPreviewTable(null);
                  setPreviewData(null);
                }}
                className="p-1.5 hover:bg-[var(--muted)]/10 rounded"
              >
                <XCircle className="w-5 h-5" />
              </button>
            </div>
            <div className="flex-1 overflow-auto p-4">
              {previewLoading ? (
                <div className="flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
                </div>
              ) : previewData && previewData.data.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="data-table text-sm">
                    <thead>
                      <tr>
                        {previewData.columns.map((col) => (
                          <th key={col} className="whitespace-nowrap">{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {previewData.data.map((row, idx) => (
                        <tr key={idx}>
                          {previewData.columns.map((col) => (
                            <td key={col} className="whitespace-nowrap max-w-xs truncate" title={String(row[col] ?? "")}>
                              {String(row[col] ?? "")}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-center text-[var(--muted)] py-8">No data available</p>
              )}
            </div>
            <div className="p-4 border-t border-[var(--border)] text-sm text-[var(--muted)]">
              Showing up to 50 rows
            </div>
          </div>
        </div>
      )}

      {/* Delete Table Confirmation Modal */}
      {deleteTableName && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
          <div className="bg-[var(--card)] rounded-lg shadow-xl w-full max-w-md p-6">
            <div className="flex items-center gap-3 mb-4">
              <div className="p-2 bg-red-100 dark:bg-red-900/30 rounded-full">
                <AlertTriangle className="w-6 h-6 text-red-500" />
              </div>
              <h3 className="text-lg font-semibold">Delete Table</h3>
            </div>
            <p className="text-[var(--muted)] mb-4">
              Are you sure you want to delete the table <span className="font-mono font-bold text-[var(--foreground)]">{deleteTableName}</span>?
              This action cannot be undone.
            </p>
            <div className="flex items-center gap-2 mb-6 p-3 bg-[var(--muted)]/10 rounded-lg">
              <input
                type="checkbox"
                id="deleteHistory"
                checked={deleteWithHistory}
                onChange={(e) => setDeleteWithHistory(e.target.checked)}
                className="w-4 h-4 rounded border-[var(--border)]"
              />
              <label htmlFor="deleteHistory" className="text-sm">
                Also delete schema version history
              </label>
            </div>
            <div className="flex justify-end gap-3">
              <button
                className="btn btn-secondary"
                onClick={() => {
                  setDeleteTableName(null);
                  setDeleteWithHistory(false);
                }}
              >
                Cancel
              </button>
              <button
                className="btn bg-red-500 hover:bg-red-600 text-white"
                onClick={() => deleteTableMutation.mutate({ name: deleteTableName, deleteHistory: deleteWithHistory })}
                disabled={deleteTableMutation.isPending}
              >
                {deleteTableMutation.isPending ? (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4 mr-2" />
                    Delete Table
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Export Table Modal */}
      {exportTableName && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-[var(--card)] border border-[var(--border)] rounded-lg p-6 max-w-md w-full mx-4 shadow-xl">
            <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <Download className="w-5 h-5" />
              Export Table: {exportTableName}
            </h3>
            <p className="text-sm text-[var(--muted)] mb-4">
              Choose a format to export the table data.
            </p>
            <div className="space-y-3 mb-6">
              <label className="flex items-center gap-3 p-3 border border-[var(--border)] rounded-lg cursor-pointer hover:bg-[var(--muted)]/10 transition-colors">
                <input
                  type="radio"
                  name="exportFormat"
                  checked={exportFormat === "csv"}
                  onChange={() => setExportFormat("csv")}
                  className="w-4 h-4"
                />
                <div className="flex-1">
                  <div className="font-medium">CSV</div>
                  <div className="text-xs text-[var(--muted)]">Comma-separated values, opens in Excel</div>
                </div>
              </label>
              <label className="flex items-center gap-3 p-3 border border-[var(--border)] rounded-lg cursor-pointer hover:bg-[var(--muted)]/10 transition-colors">
                <input
                  type="radio"
                  name="exportFormat"
                  checked={exportFormat === "parquet"}
                  onChange={() => setExportFormat("parquet")}
                  className="w-4 h-4"
                />
                <div className="flex-1">
                  <div className="font-medium">Parquet</div>
                  <div className="text-xs text-[var(--muted)]">Columnar format, optimized for data analysis</div>
                </div>
              </label>
            </div>
            <div className="flex justify-end gap-3">
              <button
                className="btn btn-secondary"
                onClick={() => setExportTableName(null)}
                disabled={exporting}
              >
                Cancel
              </button>
              <button
                className="btn btn-primary flex items-center"
                onClick={executeExport}
                disabled={exporting}
              >
                {exporting ? (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    Exporting...
                  </>
                ) : (
                  <>
                    <Download className="w-4 h-4 mr-2" />
                    Export
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Schema Rollback Modal */}
      {rollbackTarget && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-[var(--card)] border border-[var(--border)] rounded-lg p-6 max-w-md w-full mx-4 shadow-xl">
            <h3 className="text-lg font-semibold mb-4 flex items-center gap-2">
              <History className="w-5 h-5" />
              Rollback Schema
            </h3>
            <p className="text-sm mb-4">
              Are you sure you want to rollback <strong>{rollbackTarget.table}</strong> to{" "}
              <strong>version {rollbackTarget.version}</strong>?
            </p>
            <p className="text-sm text-[var(--muted)] mb-4">
              This will mark version {rollbackTarget.version} as the current schema version.
              The actual table data will not be modified - only the schema tracking metadata will change.
            </p>
            <div className="flex justify-end gap-3">
              <button
                className="btn btn-secondary"
                onClick={() => setRollbackTarget(null)}
                disabled={rollbackMutation.isPending}
              >
                Cancel
              </button>
              <button
                className="btn btn-primary flex items-center"
                onClick={() => rollbackMutation.mutate({
                  table: rollbackTarget.table,
                  version: rollbackTarget.version
                })}
                disabled={rollbackMutation.isPending}
              >
                {rollbackMutation.isPending ? (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    Rolling back...
                  </>
                ) : (
                  <>
                    <History className="w-4 h-4 mr-2" />
                    Rollback to v{rollbackTarget.version}
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

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
      {activeTab === "files" && (
        <WPBox
          title="Source Files"
          headerAction={
            <div className="flex items-center gap-2">
              <button
                className="btn btn-secondary btn-sm"
                onClick={() => refetchFiles()}
              >
                <RefreshCw className="w-4 h-4 mr-1" />
                Refresh
              </button>
              <button
                className="btn btn-primary btn-sm"
                onClick={() => processMutation.mutate(undefined)}
                disabled={processMutation.isPending}
              >
                <Play className="w-4 h-4 mr-1" />
                Process All
              </button>
            </div>
          }
        >
          {filesLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
            </div>
          ) : files && files.length > 0 ? (
            <DataTable
              columns={fileColumns}
              data={files}
              searchColumn="filename"
              searchPlaceholder="Search files..."
            />
          ) : (
            <div className="text-center py-12 text-[var(--muted)]">
              <FileSpreadsheet className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>No source files found</p>
              <p className="text-sm mt-1">Upload files to get started</p>
            </div>
          )}
        </WPBox>
      )}

      {activeTab === "tables" && (
        <WPBox title="DuckDB Tables">
          {tablesLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
            </div>
          ) : tables && tables.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Table Name</th>
                    <th>Rows</th>
                    <th>Columns</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {tables.map((table) => (
                    <tr key={table.name}>
                      <td className="font-mono font-medium">{table.name}</td>
                      <td>{table.rows.toLocaleString()}</td>
                      <td>{table.columns}</td>
                      <td>
                        <div className="flex items-center gap-2">
                          <button
                            className="p-1.5 hover:bg-[var(--primary)]/10 rounded text-[var(--primary)]"
                            onClick={() => {
                              setSelectedTable(table.name);
                              setActiveTab("schema");
                            }}
                            title="View Schema"
                          >
                            <GitBranch className="w-4 h-4" />
                          </button>
                          <button
                            className="p-1.5 hover:bg-[var(--primary)]/10 rounded text-[var(--primary)]"
                            onClick={() => handlePreview(table.name)}
                            title="Preview"
                          >
                            <Eye className="w-4 h-4" />
                          </button>
                          <button
                            className="p-1.5 hover:bg-[var(--primary)]/10 rounded text-[var(--primary)]"
                            onClick={() => handleExport(table.name)}
                            title="Export"
                          >
                            <Download className="w-4 h-4" />
                          </button>
                          <button
                            className="p-1.5 hover:bg-red-500/10 rounded text-red-500"
                            onClick={() => setDeleteTableName(table.name)}
                            title="Delete Table"
                          >
                            <Trash2 className="w-4 h-4" />
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center py-12 text-[var(--muted)]">
              <Database className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>No tables loaded</p>
              <p className="text-sm mt-1">Process source files to create tables</p>
            </div>
          )}
        </WPBox>
      )}

      {activeTab === "upload" && (
        <WPBox title="Upload Data Files">
          <div
            className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
              dragActive
                ? "border-[var(--primary)] bg-[var(--primary)]/5"
                : "border-[var(--border)]"
            }`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
          >
            <FileSpreadsheet
              className={`w-16 h-16 mx-auto mb-4 ${
                dragActive ? "text-[var(--primary)]" : "text-[var(--muted)]"
              }`}
            />
            <p className="text-lg font-medium text-[var(--foreground)] mb-2">
              {dragActive ? "Drop file here" : "Drag & drop your data file"}
            </p>
            <p className="text-sm text-[var(--muted)] mb-4">
              Supports SAS7BDAT, Parquet, CSV, and XPT formats
            </p>
            <input
              ref={fileInputRef}
              type="file"
              className="hidden"
              accept=".sas7bdat,.parquet,.csv,.xpt"
              onChange={handleFileSelect}
            />
            <button
              className="btn btn-primary btn-md"
              onClick={() => fileInputRef.current?.click()}
              disabled={uploadMutation.isPending}
            >
              {uploadMutation.isPending ? (
                <>
                  <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                  Uploading... {uploadProgress}%
                </>
              ) : (
                <>
                  <Upload className="w-4 h-4 mr-2" />
                  Choose File
                </>
              )}
            </button>
          </div>

          {uploadProgress > 0 && uploadProgress < 100 && (
            <div className="mt-4">
              <div className="flex justify-between text-sm mb-2">
                <span>Uploading...</span>
                <span>{uploadProgress}%</span>
              </div>
              <div className="h-2 bg-[var(--muted)]/20 rounded-full overflow-hidden">
                <div
                  className="h-full bg-[var(--primary)] rounded-full transition-all"
                  style={{ width: `${uploadProgress}%` }}
                />
              </div>
            </div>
          )}

          {uploadMutation.isSuccess && (
            <div className="mt-4 p-4 bg-green-100 dark:bg-green-900/30 border border-green-500 rounded-lg">
              <div className="flex items-center gap-2 text-green-700 dark:text-green-400">
                <CheckCircle2 className="w-5 h-5" />
                <span className="font-medium">File uploaded successfully!</span>
              </div>
              <p className="text-sm text-green-600 dark:text-green-500 mt-1">
                Go to Source Files tab to process the file.
              </p>
            </div>
          )}

          {uploadMutation.isError && (
            <div className="mt-4 p-4 bg-red-100 dark:bg-red-900/30 border border-red-500 rounded-lg">
              <div className="flex items-center gap-2 text-red-700 dark:text-red-400">
                <XCircle className="w-5 h-5" />
                <span className="font-medium">Upload failed</span>
              </div>
              <p className="text-sm text-red-600 dark:text-red-500 mt-1">
                {(uploadMutation.error as Error).message}
              </p>
            </div>
          )}

          {/* Format Info */}
          <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-4">
            {[
              { ext: "SAS7BDAT", desc: "SAS datasets" },
              { ext: "Parquet", desc: "Columnar format" },
              { ext: "CSV", desc: "Comma-separated" },
              { ext: "XPT", desc: "SAS transport" },
            ].map((fmt) => (
              <div
                key={fmt.ext}
                className="p-3 bg-[var(--muted)]/10 rounded-lg text-center"
              >
                <p className="font-mono font-medium">.{fmt.ext.toLowerCase()}</p>
                <p className="text-xs text-[var(--muted)]">{fmt.desc}</p>
              </div>
            ))}
          </div>
        </WPBox>
      )}

      {activeTab === "history" && (
        <WPBox title="Processing History">
          {historyLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
            </div>
          ) : fileHistory && fileHistory.length > 0 ? (
            <DataTable
              columns={historyColumns}
              data={fileHistory}
              searchColumn="filename"
              searchPlaceholder="Search history..."
            />
          ) : (
            <div className="text-center py-12 text-[var(--muted)]">
              <History className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>No processing history</p>
            </div>
          )}
        </WPBox>
      )}

      {activeTab === "schema" && (
        <WPBox title="Schema Version History">
          <div className="space-y-4">
            {/* Table selector */}
            <div className="flex items-center gap-4">
              <label className="text-sm font-medium">Select Table:</label>
              <select
                className="input w-64"
                value={selectedTable || ""}
                onChange={(e) => {
                  setSelectedTable(e.target.value || null);
                  setCompareVersions(null);
                }}
              >
                <option value="">-- Select a table --</option>
                {tables?.map((t) => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
              </select>
            </div>

            {selectedTable && schemaVersions && schemaVersions.length > 0 ? (
              <div className="space-y-4">
                {/* Compare versions selector */}
                {schemaVersions.length > 1 && (
                  <div className="p-4 bg-[var(--muted)]/10 rounded-lg">
                    <h4 className="font-medium mb-3 flex items-center gap-2">
                      <ArrowUpDown className="w-4 h-4" />
                      Compare Versions
                    </h4>
                    <div className="flex items-center gap-4 flex-wrap">
                      <div className="flex items-center gap-2">
                        <label className="text-sm">From:</label>
                        <select
                          className="input w-24"
                          value={compareVersions?.v1 ?? ""}
                          onChange={(e) => setCompareVersions(prev => ({
                            v1: parseInt(e.target.value),
                            v2: prev?.v2 ?? schemaVersions[0].version
                          }))}
                        >
                          <option value="">--</option>
                          {schemaVersions.map(v => (
                            <option key={v.version} value={v.version}>v{v.version}</option>
                          ))}
                        </select>
                      </div>
                      <div className="flex items-center gap-2">
                        <label className="text-sm">To:</label>
                        <select
                          className="input w-24"
                          value={compareVersions?.v2 ?? ""}
                          onChange={(e) => setCompareVersions(prev => ({
                            v1: prev?.v1 ?? schemaVersions[schemaVersions.length - 1].version,
                            v2: parseInt(e.target.value)
                          }))}
                        >
                          <option value="">--</option>
                          {schemaVersions.map(v => (
                            <option key={v.version} value={v.version}>v{v.version}</option>
                          ))}
                        </select>
                      </div>
                      {compareVersions && (
                        <button
                          className="btn btn-secondary btn-sm"
                          onClick={() => setCompareVersions(null)}
                        >
                          Clear
                        </button>
                      )}
                    </div>

                    {/* Comparison Result */}
                    {compareVersions && compareVersions.v1 && compareVersions.v2 && (() => {
                      const v1Data = schemaVersions.find(v => v.version === compareVersions.v1);
                      const v2Data = schemaVersions.find(v => v.version === compareVersions.v2);
                      if (!v1Data?.columns || !v2Data?.columns) return null;

                      const v1Cols = new Map(v1Data.columns.map(c => [c.name, c]));
                      const v2Cols = new Map(v2Data.columns.map(c => [c.name, c]));
                      const allCols = new Set([...v1Cols.keys(), ...v2Cols.keys()]);

                      const added = [...allCols].filter(c => !v1Cols.has(c) && v2Cols.has(c));
                      const removed = [...allCols].filter(c => v1Cols.has(c) && !v2Cols.has(c));
                      const changed = [...allCols].filter(c => {
                        const c1 = v1Cols.get(c);
                        const c2 = v2Cols.get(c);
                        return c1 && c2 && c1.dtype !== c2.dtype;
                      });

                      return (
                        <div className="mt-4 space-y-3">
                          <div className="flex items-center gap-4 text-sm">
                            <span className="text-green-500 flex items-center gap-1">
                              <Plus className="w-4 h-4" /> {added.length} added
                            </span>
                            <span className="text-red-500 flex items-center gap-1">
                              <Minus className="w-4 h-4" /> {removed.length} removed
                            </span>
                            <span className="text-yellow-500 flex items-center gap-1">
                              <ArrowUpDown className="w-4 h-4" /> {changed.length} changed
                            </span>
                          </div>

                          {(added.length > 0 || removed.length > 0 || changed.length > 0) && (
                            <div className="max-h-48 overflow-auto border border-[var(--border)] rounded">
                              <table className="w-full text-xs">
                                <thead className="bg-[var(--muted)]/20 sticky top-0">
                                  <tr>
                                    <th className="p-2 text-left">Column</th>
                                    <th className="p-2 text-left">v{compareVersions.v1}</th>
                                    <th className="p-2 text-left">v{compareVersions.v2}</th>
                                    <th className="p-2 text-left">Change</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {added.map(col => (
                                    <tr key={col} className="border-t border-[var(--border)] bg-green-500/10">
                                      <td className="p-2 font-mono">{col}</td>
                                      <td className="p-2 text-[var(--muted)]">—</td>
                                      <td className="p-2">{v2Cols.get(col)?.dtype}</td>
                                      <td className="p-2 text-green-500 font-medium">Added</td>
                                    </tr>
                                  ))}
                                  {removed.map(col => (
                                    <tr key={col} className="border-t border-[var(--border)] bg-red-500/10">
                                      <td className="p-2 font-mono">{col}</td>
                                      <td className="p-2">{v1Cols.get(col)?.dtype}</td>
                                      <td className="p-2 text-[var(--muted)]">—</td>
                                      <td className="p-2 text-red-500 font-medium">Removed</td>
                                    </tr>
                                  ))}
                                  {changed.map(col => (
                                    <tr key={col} className="border-t border-[var(--border)] bg-yellow-500/10">
                                      <td className="p-2 font-mono">{col}</td>
                                      <td className="p-2">{v1Cols.get(col)?.dtype}</td>
                                      <td className="p-2">{v2Cols.get(col)?.dtype}</td>
                                      <td className="p-2 text-yellow-500 font-medium">Type Changed</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          )}

                          {added.length === 0 && removed.length === 0 && changed.length === 0 && (
                            <p className="text-sm text-[var(--muted)]">No schema changes between these versions.</p>
                          )}
                        </div>
                      );
                    })()}
                  </div>
                )}

                {/* Version cards */}
                {schemaVersions.map((version) => (
                  <div
                    key={version.version}
                    className={`p-4 rounded-lg border ${
                      version.is_current
                        ? "border-[var(--primary)] bg-[var(--primary)]/5"
                        : "border-[var(--border)]"
                    }`}
                  >
                    <div className="flex items-center justify-between mb-3">
                      <div className="flex items-center gap-3">
                        <span className="font-mono font-bold text-lg">
                          v{version.version}
                        </span>
                        {version.is_current && (
                          <span className="px-2 py-0.5 bg-[var(--primary)] text-white text-xs rounded-full">
                            Current
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-3">
                        <span className="text-sm text-[var(--muted)]">
                          {formatDateTime(version.created_at)}
                        </span>
                        {!version.is_current && (
                          <button
                            className="btn btn-secondary btn-sm flex items-center gap-1"
                            onClick={() => setRollbackTarget({ table: selectedTable!, version: version.version })}
                            title="Rollback to this version"
                          >
                            <History className="w-3 h-3" />
                            Rollback
                          </button>
                        )}
                      </div>
                    </div>
                    <div className="grid grid-cols-3 gap-4 text-sm">
                      <div>
                        <span className="text-[var(--muted)]">Columns:</span>{" "}
                        <span className="font-medium">{version.column_count}</span>
                      </div>
                      <div>
                        <span className="text-[var(--muted)]">Source:</span>{" "}
                        <span className="font-mono text-xs">{version.source_file}</span>
                      </div>
                      <div>
                        <span className="text-[var(--muted)]">Hash:</span>{" "}
                        <span className="font-mono text-xs">{version.schema_hash}</span>
                      </div>
                    </div>
                    {version.columns && version.columns.length > 0 && (
                      <details className="mt-3">
                        <summary className="cursor-pointer text-sm text-[var(--primary)] hover:underline">
                          View columns ({version.columns.length})
                        </summary>
                        <div className="mt-2 max-h-48 overflow-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="text-left text-[var(--muted)]">
                                <th className="p-1">Column</th>
                                <th className="p-1">Type</th>
                                <th className="p-1">Nullable</th>
                              </tr>
                            </thead>
                            <tbody>
                              {version.columns.map((col) => (
                                <tr key={col.name} className="border-t border-[var(--border)]">
                                  <td className="p-1 font-mono">{col.name}</td>
                                  <td className="p-1">{col.dtype}</td>
                                  <td className="p-1">{col.nullable ? "Yes" : "No"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </details>
                    )}
                  </div>
                ))}
              </div>
            ) : selectedTable ? (
              <div className="text-center py-8 text-[var(--muted)]">
                <GitBranch className="w-12 h-12 mx-auto mb-4 opacity-50" />
                <p>No schema versions found for {selectedTable}</p>
              </div>
            ) : (
              <div className="text-center py-8 text-[var(--muted)]">
                <ArrowUpDown className="w-12 h-12 mx-auto mb-4 opacity-50" />
                <p>Select a table to view schema history</p>
              </div>
            )}
          </div>
        </WPBox>
      )}

      {activeTab === "status" && (
        <div className="grid gap-5 md:grid-cols-2">
          <WPBox title="System Status">
            {statusLoading ? (
              <div className="flex items-center justify-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]" />
              </div>
            ) : status ? (
              <div className="space-y-4">
                <div className="flex items-center gap-3">
                  <div
                    className={`w-3 h-3 rounded-full ${
                      status.modules_available ? "bg-green-500" : "bg-red-500"
                    }`}
                  />
                  <span>
                    Core Modules:{" "}
                    {status.modules_available ? "Available" : "Unavailable"}
                  </span>
                </div>
                <div className="flex items-center gap-3">
                  <div
                    className={`w-3 h-3 rounded-full ${
                      status.database_exists ? "bg-green-500" : "bg-yellow-500"
                    }`}
                  />
                  <span>
                    Database:{" "}
                    {status.database_exists ? "Ready" : "Not Created"}
                  </span>
                </div>
                <div className="text-sm text-[var(--muted)] space-y-1">
                  <p>Raw Directory: {status.raw_directory}</p>
                  <p>Database Path: {status.database_path}</p>
                </div>
                {status.import_error && (
                  <div className="p-3 bg-red-100 dark:bg-red-900/30 rounded text-red-600 text-sm">
                    {status.import_error}
                  </div>
                )}
              </div>
            ) : null}
          </WPBox>

          <WPBox title="Statistics">
            {status?.file_statistics ? (
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-[var(--muted)]/10 rounded-lg">
                  <p className="text-2xl font-bold">{status.file_statistics.total_files}</p>
                  <p className="text-sm text-[var(--muted)]">Total Files</p>
                </div>
                <div className="p-3 bg-[var(--muted)]/10 rounded-lg">
                  <p className="text-2xl font-bold">{status.file_statistics.unique_tables}</p>
                  <p className="text-sm text-[var(--muted)]">Unique Tables</p>
                </div>
                <div className="p-3 bg-[var(--muted)]/10 rounded-lg">
                  <p className="text-2xl font-bold">
                    {status.file_statistics.total_rows_processed.toLocaleString()}
                  </p>
                  <p className="text-sm text-[var(--muted)]">Rows Processed</p>
                </div>
                <div className="p-3 bg-[var(--muted)]/10 rounded-lg">
                  <p className="text-2xl font-bold">{status.file_statistics.total_size_mb} MB</p>
                  <p className="text-sm text-[var(--muted)]">Total Size</p>
                </div>
              </div>
            ) : (
              <p className="text-[var(--muted)]">No statistics available</p>
            )}
          </WPBox>

          {status?.file_statistics && (
            <>
              <WPBox title="Status Breakdown">
                <div className="space-y-3">
                  {Object.entries(status.file_statistics.status_counts).map(([key, value]) => (
                    <div key={key} className="flex items-center justify-between">
                      <span className="capitalize">{key}</span>
                      <span className="font-mono font-medium">{value}</span>
                    </div>
                  ))}
                </div>
              </WPBox>

              <WPBox title="Format Breakdown">
                <div className="space-y-3">
                  {Object.entries(status.file_statistics.format_counts).map(([key, value]) => (
                    <div key={key} className="flex items-center justify-between">
                      <span className="uppercase text-sm">{key}</span>
                      <span className="font-mono font-medium">{value}</span>
                    </div>
                  ))}
                </div>
              </WPBox>
            </>
          )}
        </div>
      )}
    </div>
  );
}
