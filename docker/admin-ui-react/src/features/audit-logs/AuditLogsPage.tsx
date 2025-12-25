import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import {
  Download,
  Filter,
  ScrollText,
  CheckCircle,
  XCircle,
  FileSpreadsheet,
  FileText,
  AlertTriangle,
  Clock,
  Users,
  ChevronDown,
  Eye,
  X,
  MessageSquare,
  Code,
  Database,
  Gauge,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { useToast } from "@/components/common/Toast";
import { auditApi, type QueryAuditDetails } from "@/api/audit";
import { formatDateTime } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import type { AuditLogEntry, AuditLogFilter } from "@/types/api";

export function AuditLogsPage() {
  const [filter, setFilter] = useState<AuditLogFilter>({});
  const [showFilters, setShowFilters] = useState(false);
  const [showExportMenu, setShowExportMenu] = useState(false);
  const [page, setPage] = useState(1);
  const [selectedLog, setSelectedLog] = useState<AuditLogEntry | null>(null);
  const [queryDetails, setQueryDetails] = useState<QueryAuditDetails | null>(null);
  const [showDetailsModal, setShowDetailsModal] = useState(false);
  const pageSize = 50;
  const toast = useToast();

  const { data: logsResponse, isLoading } = useQuery({
    queryKey: ["auditLogs", filter, page, pageSize],
    queryFn: () => auditApi.getLogs(filter, page, pageSize),
  });

  const { data: actions } = useQuery({
    queryKey: ["auditActions"],
    queryFn: auditApi.getActions,
  });

  const { data: resourceTypes } = useQuery({
    queryKey: ["auditResourceTypes"],
    queryFn: auditApi.getResourceTypes,
  });

  const { data: statistics } = useQuery({
    queryKey: ["auditStatistics", filter.startDate, filter.endDate],
    queryFn: () => auditApi.getStatistics(filter.startDate, filter.endDate),
  });

  const exportExcelMutation = useMutation({
    mutationFn: () => auditApi.exportExcel(filter),
    onSuccess: () => {
      toast.success("Excel export started", "The file will download shortly");
      setShowExportMenu(false);
    },
    onError: (error: Error) => {
      toast.error("Export failed", error.message);
    },
  });

  const exportPdfMutation = useMutation({
    mutationFn: () => auditApi.exportPdf(filter),
    onSuccess: () => {
      toast.success("PDF export started", "The file will download shortly");
      setShowExportMenu(false);
    },
    onError: (error: Error) => {
      toast.error("Export failed", error.message);
    },
  });

  const exportCsvMutation = useMutation({
    mutationFn: () => auditApi.exportCsv(filter),
    onSuccess: () => {
      toast.success("CSV export started", "The file will download shortly");
      setShowExportMenu(false);
    },
    onError: (error: Error) => {
      toast.error("Export failed", error.message);
    },
  });

  // Handle viewing log details
  const handleViewDetails = async (log: AuditLogEntry) => {
    setSelectedLog(log);
    setQueryDetails(null);
    setShowDetailsModal(true);

    // If it's a QUERY action, fetch the query details
    if (log.action === "QUERY" && log.id) {
      try {
        const details = await auditApi.getQueryDetails(log.id);
        setQueryDetails(details);
      } catch (error) {
        console.error("Failed to fetch query details:", error);
      }
    }
  };

  const columns: ColumnDef<AuditLogEntry>[] = [
    {
      accessorKey: "timestamp",
      header: "Timestamp",
      cell: ({ row }) => (
        <span className="text-sm font-mono">
          {formatDateTime(row.original.timestamp)}
        </span>
      ),
    },
    {
      accessorKey: "username",
      header: "User",
      cell: ({ row }) => (
        <span className="font-medium">
          {row.original.username || row.original.user || "-"}
        </span>
      ),
    },
    {
      accessorKey: "action",
      header: "Action",
      cell: ({ row }) => {
        const action = row.original.action;
        let variant: "primary" | "success" | "warning" | "destructive" = "primary";
        if (action?.includes("LOGIN")) variant = "success";
        if (action?.includes("FAILED") || action?.includes("ERROR")) variant = "destructive";
        if (action?.includes("UPLOAD") || action?.includes("DATA")) variant = "warning";
        if (action === "QUERY") variant = "primary";
        return <StatusBadge variant={variant}>{action}</StatusBadge>;
      },
    },
    {
      accessorKey: "resource_type",
      header: "Resource",
      cell: ({ row }) => row.original.resource_type || "-",
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => (
        <StatusBadge
          variant={row.original.status === "success" ? "success" : "destructive"}
        >
          <span className="flex items-center gap-1">
            {row.original.status === "success" ? (
              <CheckCircle className="w-3 h-3" />
            ) : (
              <XCircle className="w-3 h-3" />
            )}
            {row.original.status}
          </span>
        </StatusBadge>
      ),
    },
    {
      accessorKey: "duration_ms",
      header: "Duration",
      cell: ({ row }) =>
        row.original.duration_ms ? `${row.original.duration_ms}ms` : "-",
    },
    {
      accessorKey: "ip_address",
      header: "IP Address",
      cell: ({ row }) => (
        <span className="font-mono text-sm">
          {row.original.ip_address || "-"}
        </span>
      ),
    },
    {
      id: "actions",
      header: "",
      cell: ({ row }) => (
        <button
          className="p-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded"
          onClick={() => handleViewDetails(row.original)}
          title="View Details"
        >
          <Eye className="w-4 h-4 text-gray-500" />
        </button>
      ),
    },
  ];

  const clearFilters = () => {
    setFilter({});
    setPage(1);
  };

  // Extract logs array from response
  const logs = logsResponse?.logs || [];
  const totalLogs = logsResponse?.total || 0;
  const totalPages = logsResponse?.total_pages || 1;

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Audit Logs
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Track system activity and user actions for 21 CFR Part 11 compliance
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            className="btn btn-secondary btn-md"
            onClick={() => setShowFilters(!showFilters)}
          >
            <Filter className="w-4 h-4" />
            Filters
          </button>

          {/* Export Dropdown */}
          <div className="relative">
            <button
              className="btn btn-secondary btn-md"
              onClick={() => setShowExportMenu(!showExportMenu)}
            >
              <Download className="w-4 h-4" />
              Export
              <ChevronDown className="w-4 h-4" />
            </button>

            {showExportMenu && (
              <div className="absolute right-0 mt-2 w-48 bg-white dark:bg-gray-800 rounded-lg shadow-lg border border-gray-200 dark:border-gray-700 py-1 z-10">
                <button
                  className="w-full px-4 py-2 text-left text-sm hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2"
                  onClick={() => exportExcelMutation.mutate()}
                  disabled={exportExcelMutation.isPending}
                >
                  <FileSpreadsheet className="w-4 h-4 text-green-600" />
                  Export to Excel
                </button>
                <button
                  className="w-full px-4 py-2 text-left text-sm hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2"
                  onClick={() => exportPdfMutation.mutate()}
                  disabled={exportPdfMutation.isPending}
                >
                  <FileText className="w-4 h-4 text-red-600" />
                  Export to PDF
                </button>
                <button
                  className="w-full px-4 py-2 text-left text-sm hover:bg-gray-100 dark:hover:bg-gray-700 flex items-center gap-2"
                  onClick={() => exportCsvMutation.mutate()}
                  disabled={exportCsvMutation.isPending}
                >
                  <FileText className="w-4 h-4 text-blue-600" />
                  Export to CSV
                </button>
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Statistics */}
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4">
        <div className="wp-box p-4 flex items-center gap-3">
          <ScrollText className="w-8 h-8 text-[var(--primary)]" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.total_events || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Total Events</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <CheckCircle className="w-8 h-8 text-[var(--success)]" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.by_status?.success || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Successful</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <XCircle className="w-8 h-8 text-[var(--destructive)]" />
          <div>
            <div className="text-2xl font-bold">
              {(statistics?.by_status?.failure || 0) +
                (statistics?.by_status?.error || 0)}
            </div>
            <div className="text-sm text-[var(--muted)]">Failed</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <Users className="w-8 h-8 text-blue-500" />
          <div>
            <div className="text-2xl font-bold">
              {Object.keys(statistics?.by_user || {}).length}
            </div>
            <div className="text-sm text-[var(--muted)]">Active Users</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <AlertTriangle className="w-8 h-8 text-yellow-500" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.by_action?.QUERY || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Queries</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <Clock className="w-8 h-8 text-purple-500" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.average_duration_ms
                ? `${Math.round(statistics.average_duration_ms)}ms`
                : "-"}
            </div>
            <div className="text-sm text-[var(--muted)]">Avg Duration</div>
          </div>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <WPBox title="Filters">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">User</label>
              <input
                type="text"
                placeholder="Filter by username"
                value={filter.username || ""}
                onChange={(e) =>
                  setFilter({ ...filter, username: e.target.value || undefined })
                }
                className="input"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Action</label>
              <select
                value={filter.action || ""}
                onChange={(e) =>
                  setFilter({ ...filter, action: e.target.value || undefined })
                }
                className="select"
              >
                <option value="">All Actions</option>
                {actions?.map((action) => (
                  <option key={action} value={action}>
                    {action}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Resource Type</label>
              <select
                value={filter.resourceType || ""}
                onChange={(e) =>
                  setFilter({ ...filter, resourceType: e.target.value || undefined })
                }
                className="select"
              >
                <option value="">All Resources</option>
                {resourceTypes?.map((resource) => (
                  <option key={resource} value={resource}>
                    {resource}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Status</label>
              <select
                value={filter.status || ""}
                onChange={(e) =>
                  setFilter({
                    ...filter,
                    status: e.target.value || undefined,
                  })
                }
                className="select"
              >
                <option value="">All Statuses</option>
                <option value="success">Success</option>
                <option value="failure">Failure</option>
                <option value="error">Error</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Start Date</label>
              <input
                type="date"
                value={filter.startDate || ""}
                onChange={(e) =>
                  setFilter({ ...filter, startDate: e.target.value || undefined })
                }
                className="input"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">End Date</label>
              <input
                type="date"
                value={filter.endDate || ""}
                onChange={(e) =>
                  setFilter({ ...filter, endDate: e.target.value || undefined })
                }
                className="input"
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Search</label>
              <input
                type="text"
                placeholder="Search in paths, errors..."
                value={filter.searchText || ""}
                onChange={(e) =>
                  setFilter({ ...filter, searchText: e.target.value || undefined })
                }
                className="input"
              />
            </div>
            <div className="flex items-end">
              <button className="btn btn-secondary btn-md" onClick={clearFilters}>
                Clear Filters
              </button>
            </div>
          </div>
        </WPBox>
      )}

      {/* Logs Table */}
      <WPBox
        title={`Audit Log Entries (${totalLogs.toLocaleString()} total)`}
      >
        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
          </div>
        ) : logs.length > 0 ? (
          <>
            <DataTable columns={columns} data={logs} pageSize={logs.length || 50} />

            {/* Pagination */}
            {totalPages > 1 && (
              <div className="flex items-center justify-between mt-4 pt-4 border-t border-gray-200 dark:border-gray-700">
                <div className="text-sm text-[var(--muted)]">
                  Showing {(page - 1) * pageSize + 1} to{" "}
                  {Math.min(page * pageSize, totalLogs)} of {totalLogs} entries
                </div>
                <div className="flex items-center gap-2">
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => setPage(page - 1)}
                    disabled={page <= 1}
                  >
                    Previous
                  </button>
                  <span className="px-3 py-1 text-sm">
                    Page {page} of {totalPages}
                  </span>
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => setPage(page + 1)}
                    disabled={page >= totalPages}
                  >
                    Next
                  </button>
                </div>
              </div>
            )}
          </>
        ) : (
          <div className="text-center py-8 text-[var(--muted)]">
            <ScrollText className="w-12 h-12 mx-auto mb-4" />
            <p>No audit log entries found</p>
            {Object.keys(filter).length > 0 && (
              <button
                className="mt-4 btn btn-secondary btn-sm"
                onClick={clearFilters}
              >
                Clear filters
              </button>
            )}
          </div>
        )}
      </WPBox>

      {/* Details Modal */}
      {showDetailsModal && selectedLog && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-4xl w-full max-h-[90vh] overflow-hidden">
            {/* Modal Header */}
            <div className="flex items-center justify-between p-4 border-b border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold">
                Audit Log Details
                {selectedLog.action === "QUERY" && (
                  <span className="ml-2 text-sm font-normal text-gray-500">
                    - Query Traceability
                  </span>
                )}
              </h2>
              <button
                onClick={() => setShowDetailsModal(false)}
                className="p-1 hover:bg-gray-100 dark:hover:bg-gray-700 rounded"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Modal Content */}
            <div className="p-4 overflow-y-auto max-h-[calc(90vh-120px)]">
              {/* Basic Info */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                <div>
                  <div className="text-xs text-gray-500 uppercase">Timestamp</div>
                  <div className="font-mono text-sm">{formatDateTime(selectedLog.timestamp)}</div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">User</div>
                  <div className="font-medium">{selectedLog.username || selectedLog.user || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">Action</div>
                  <div>{selectedLog.action}</div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">Status</div>
                  <StatusBadge variant={selectedLog.status === "success" ? "success" : "destructive"}>
                    {selectedLog.status}
                  </StatusBadge>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">IP Address</div>
                  <div className="font-mono text-sm">{selectedLog.ip_address || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">Duration</div>
                  <div>{selectedLog.duration_ms ? `${selectedLog.duration_ms}ms` : "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">Resource</div>
                  <div>{selectedLog.resource_type || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-gray-500 uppercase">Resource ID</div>
                  <div className="font-mono text-sm truncate">{selectedLog.resource_id || "-"}</div>
                </div>
              </div>

              {/* Query Details Section (for QUERY actions) */}
              {selectedLog.action === "QUERY" && (
                <div className="space-y-4">
                  <h3 className="text-md font-semibold border-b pb-2 flex items-center gap-2">
                    <MessageSquare className="w-4 h-4" />
                    Query Traceability
                  </h3>

                  {queryDetails ? (
                    <div className="space-y-4">
                      {/* User Question */}
                      <div className="bg-blue-50 dark:bg-blue-900/20 p-4 rounded-lg">
                        <div className="text-xs text-blue-600 dark:text-blue-400 uppercase mb-1 flex items-center gap-1">
                          <MessageSquare className="w-3 h-3" />
                          User Question
                        </div>
                        <div className="text-sm">{queryDetails.original_question || "-"}</div>
                        {queryDetails.sanitized_question && queryDetails.sanitized_question !== queryDetails.original_question && (
                          <div className="mt-2 text-xs text-gray-500">
                            Sanitized: {queryDetails.sanitized_question}
                          </div>
                        )}
                      </div>

                      {/* Intent & Confidence */}
                      <div className="grid grid-cols-2 gap-4">
                        <div className="bg-gray-50 dark:bg-gray-700/50 p-4 rounded-lg">
                          <div className="text-xs text-gray-500 uppercase mb-1">Intent Classification</div>
                          <div className="font-medium">{queryDetails.intent_classification || "-"}</div>
                        </div>
                        <div className="bg-gray-50 dark:bg-gray-700/50 p-4 rounded-lg">
                          <div className="text-xs text-gray-500 uppercase mb-1 flex items-center gap-1">
                            <Gauge className="w-3 h-3" />
                            Confidence Score
                          </div>
                          <div className="font-medium">
                            {queryDetails.confidence_score
                              ? `${queryDetails.confidence_score > 1
                                  ? queryDetails.confidence_score.toFixed(1)
                                  : (queryDetails.confidence_score * 100).toFixed(1)}%`
                              : "-"}
                          </div>
                        </div>
                      </div>

                      {/* Generated SQL */}
                      {queryDetails.generated_sql && (
                        <div className="bg-gray-900 text-green-400 p-4 rounded-lg">
                          <div className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                            <Database className="w-3 h-3" />
                            Generated SQL
                          </div>
                          <pre className="text-sm overflow-x-auto whitespace-pre-wrap font-mono">
                            {queryDetails.generated_sql}
                          </pre>
                        </div>
                      )}

                      {/* LLM Prompt */}
                      {queryDetails.llm_prompt && (
                        <div className="bg-purple-50 dark:bg-purple-900/20 p-4 rounded-lg">
                          <div className="text-xs text-purple-600 dark:text-purple-400 uppercase mb-2 flex items-center gap-1">
                            <Code className="w-3 h-3" />
                            LLM Prompt Sent
                          </div>
                          <pre className="text-xs overflow-x-auto whitespace-pre-wrap max-h-48 overflow-y-auto bg-white dark:bg-gray-800 p-2 rounded">
                            {queryDetails.llm_prompt}
                          </pre>
                        </div>
                      )}

                      {/* LLM Response */}
                      {queryDetails.llm_response && (
                        <div className="bg-green-50 dark:bg-green-900/20 p-4 rounded-lg">
                          <div className="text-xs text-green-600 dark:text-green-400 uppercase mb-2">
                            LLM Response
                          </div>
                          <pre className="text-xs overflow-x-auto whitespace-pre-wrap max-h-48 overflow-y-auto bg-white dark:bg-gray-800 p-2 rounded">
                            {queryDetails.llm_response}
                          </pre>
                        </div>
                      )}

                      {/* Execution Stats */}
                      <div className="grid grid-cols-3 gap-4">
                        <div className="bg-gray-50 dark:bg-gray-700/50 p-3 rounded-lg text-center">
                          <div className="text-xs text-gray-500 uppercase">LLM Model</div>
                          <div className="font-medium text-sm">{queryDetails.llm_model || "-"}</div>
                        </div>
                        <div className="bg-gray-50 dark:bg-gray-700/50 p-3 rounded-lg text-center">
                          <div className="text-xs text-gray-500 uppercase">Tokens Used</div>
                          <div className="font-medium text-sm">{queryDetails.llm_tokens_used || "-"}</div>
                        </div>
                        <div className="bg-gray-50 dark:bg-gray-700/50 p-3 rounded-lg text-center">
                          <div className="text-xs text-gray-500 uppercase">Result Rows</div>
                          <div className="font-medium text-sm">{queryDetails.result_row_count ?? "-"}</div>
                        </div>
                      </div>

                      {/* Tables Accessed */}
                      {queryDetails.tables_accessed && queryDetails.tables_accessed.length > 0 && (
                        <div>
                          <div className="text-xs text-gray-500 uppercase mb-1">Tables Accessed</div>
                          <div className="flex gap-2 flex-wrap">
                            {queryDetails.tables_accessed.map((table, i) => (
                              <span key={i} className="px-2 py-1 bg-gray-100 dark:bg-gray-700 rounded text-sm">
                                {table}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="text-center py-8 text-gray-500">
                      <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-gray-400 mx-auto mb-2"></div>
                      Loading query details...
                    </div>
                  )}
                </div>
              )}

              {/* Error Message */}
              {selectedLog.error_message && (
                <div className="mt-4 bg-red-50 dark:bg-red-900/20 p-4 rounded-lg">
                  <div className="text-xs text-red-600 dark:text-red-400 uppercase mb-1">Error Message</div>
                  <div className="text-sm text-red-700 dark:text-red-300">{selectedLog.error_message}</div>
                </div>
              )}

              {/* Checksum for compliance */}
              {selectedLog.checksum && (
                <div className="mt-4 text-xs text-gray-400">
                  <span className="uppercase">Integrity Checksum:</span>{" "}
                  <span className="font-mono">{selectedLog.checksum}</span>
                </div>
              )}
            </div>

            {/* Modal Footer */}
            <div className="flex justify-end p-4 border-t border-gray-200 dark:border-gray-700">
              <button
                onClick={() => setShowDetailsModal(false)}
                className="btn btn-secondary btn-md"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
