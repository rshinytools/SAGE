import { useState, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  FileSearch,
  AlertTriangle,
  AlertCircle,
  Info,
  Upload,
  GitCompare,
  List,
  CheckCircle2,
  XCircle,
  Clock,
  ChevronRight,
  X,
  Check,
  Database,
  Sparkles,
  FileSpreadsheet,
  Loader2,
  Trash2,
  Download,
  History,
  RotateCcw,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { ConfirmModal } from "@/components/common/ConfirmModal";
import { useToast } from "@/components/common/Toast";
import { metadataApi } from "@/api/metadata";
import type { ColumnDef } from "@tanstack/react-table";
import type { MetadataDomain, MetadataVariable, MetadataIssue, MetadataCodelist, ApprovalStatus, PaginatedResponse } from "@/types/api";

type Tab = "domains" | "variables" | "codelists" | "issues" | "upload" | "compare" | "history";

// Status badge component for approval status
function ApprovalBadge({ status }: { status?: ApprovalStatus | "partial" }) {
  if (!status || status === "pending") {
    return (
      <StatusBadge variant="warning">
        <Clock className="w-3 h-3 mr-1" />
        Pending
      </StatusBadge>
    );
  }
  if (status === "approved") {
    return (
      <StatusBadge variant="success">
        <CheckCircle2 className="w-3 h-3 mr-1" />
        Approved
      </StatusBadge>
    );
  }
  if (status === "partial") {
    return (
      <StatusBadge variant="default">
        <Clock className="w-3 h-3 mr-1" />
        Partial
      </StatusBadge>
    );
  }
  return (
    <StatusBadge variant="destructive">
      <XCircle className="w-3 h-3 mr-1" />
      Rejected
    </StatusBadge>
  );
}

export function MetadataAuditorPage() {
  const toast = useToast();
  const [activeTab, setActiveTab] = useState<Tab>("domains");
  const [selectedDomain, setSelectedDomain] = useState<string | null>(null);
  const [selectedVariable, setSelectedVariable] = useState<MetadataVariable | null>(null);
  const [showDetailPanel, setShowDetailPanel] = useState(false);
  const [approvalComment, setApprovalComment] = useState("");
  const [uploadStep, setUploadStep] = useState<'idle' | 'uploading' | 'parsing' | 'merging' | 'complete' | 'error'>('idle');
  const [uploadProgress, setUploadProgress] = useState<string>('');
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [variablesPageIndex, setVariablesPageIndex] = useState(0);

  // Confirmation modal state
  const [confirmModal, setConfirmModal] = useState<{
    isOpen: boolean;
    title: string;
    message: string;
    onConfirm: () => void;
    variant?: "danger" | "warning" | "info";
    confirmText?: string;
  }>({
    isOpen: false,
    title: "",
    message: "",
    onConfirm: () => {},
  });

  // Bulk approve progress state
  const [bulkApproveProgress, setBulkApproveProgress] = useState<{
    current: number;
    total: number;
    message?: string;
  } | undefined>(undefined);
  const [isBulkApproving, setIsBulkApproving] = useState(false);

  // Export state
  const [isExporting, setIsExporting] = useState(false);
  const [showExportOptions, setShowExportOptions] = useState(false);

  // Audit modal state
  const [showAuditModal, setShowAuditModal] = useState(false);
  const [showAuditConfig, setShowAuditConfig] = useState(false);  // Show config before starting
  const [useLLM, setUseLLM] = useState(false);  // Default: skip LLM (fast mode)
  const [auditStep, setAuditStep] = useState<1 | 2 | 3>(1);
  const [auditProgress, setAuditProgress] = useState({ current: 0, total: 0, message: '' });
  const [auditStartTime, setAuditStartTime] = useState<number | null>(null);
  const [auditStats, setAuditStats] = useState({
    cdisc_approved: 0,
    llm_approved: 0,
    quick_review: 0,
    manual_review: 0,
    needs_llm: 0
  });
  const [auditComplete, setAuditComplete] = useState(false);
  const [auditError, setAuditError] = useState<string | null>(null);

  // Calculate time remaining
  const getTimeRemaining = () => {
    if (!auditStartTime || auditProgress.total === 0 || auditProgress.current === 0) return null;
    const elapsed = (Date.now() - auditStartTime) / 1000;
    const rate = auditProgress.current / elapsed;
    const remaining = (auditProgress.total - auditProgress.current) / rate;
    if (remaining < 60) return `${Math.ceil(remaining)}s remaining`;
    if (remaining < 3600) return `${Math.ceil(remaining / 60)}m remaining`;
    return `${Math.ceil(remaining / 3600)}h remaining`;
  };

  const queryClient = useQueryClient();

  // Queries
  const { data: domains, isLoading: domainsLoading } = useQuery({
    queryKey: ["domains"],
    queryFn: metadataApi.getDomains,
  });

  const { data: variables, isLoading: variablesLoading } = useQuery({
    queryKey: ["variables", selectedDomain],
    queryFn: () => metadataApi.getVariables(selectedDomain || undefined),
    enabled: !!selectedDomain || activeTab === "variables",
  });

  const { data: codelists, isLoading: codelistsLoading } = useQuery({
    queryKey: ["codelists"],
    queryFn: metadataApi.getCodelists,
    enabled: activeTab === "codelists",
  });

  const { data: stats } = useQuery({
    queryKey: ["metadata-stats"],
    queryFn: metadataApi.getStats,
  });

  const { data: issues, isLoading: issuesLoading } = useQuery({
    queryKey: ["issues"],
    queryFn: () => metadataApi.getIssues(),
  });

  const { data: versions, isLoading: versionsLoading } = useQuery({
    queryKey: ["versions"],
    queryFn: () => metadataApi.getVersions(50),
    enabled: activeTab === "history" || activeTab === "compare",
  });

  // Mutations
  const uploadMutation = useMutation({
    mutationFn: metadataApi.uploadSpecification,
    onMutate: () => {
      setUploadStep('uploading');
      setUploadProgress('Uploading file to server...');
    },
    onSuccess: (data) => {
      setUploadStep('complete');
      setUploadProgress(`Successfully imported ${data.domains} domains and ${data.variables} variables!`);
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      queryClient.invalidateQueries({ queryKey: ["variables"] });
      queryClient.invalidateQueries({ queryKey: ["codelists"] });
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
      setSelectedFile(null);
    },
    onError: (error: Error) => {
      setUploadStep('error');
      setUploadProgress(error.message || 'Upload failed. Please check the file format.');
    },
  });

  const approveVariableMutation = useMutation({
    mutationFn: ({ domain, name, comment }: { domain: string; name: string; comment?: string }) =>
      metadataApi.approveVariable(domain, name, comment),
    onSuccess: (_data, variables) => {
      // Update the variable status in-place to preserve pagination
      queryClient.setQueryData(
        ["variables", variables.domain],
        (oldData: PaginatedResponse<MetadataVariable> | undefined) => {
          if (!oldData) return oldData;
          return {
            ...oldData,
            items: oldData.items.map((v) =>
              v.name === variables.name ? { ...v, status: "approved" as const } : v
            ),
          };
        }
      );
      // Also update the selected variable if it's the one being approved
      if (selectedVariable?.name === variables.name) {
        setSelectedVariable({ ...selectedVariable, status: "approved" });
      }
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      setApprovalComment("");
    },
  });

  const rejectVariableMutation = useMutation({
    mutationFn: ({ domain, name, comment }: { domain: string; name: string; comment: string }) =>
      metadataApi.rejectVariable(domain, name, comment),
    onSuccess: (_data, variables) => {
      // Update the variable status in-place to preserve pagination
      queryClient.setQueryData(
        ["variables", variables.domain],
        (oldData: PaginatedResponse<MetadataVariable> | undefined) => {
          if (!oldData) return oldData;
          return {
            ...oldData,
            items: oldData.items.map((v) =>
              v.name === variables.name ? { ...v, status: "rejected" as const } : v
            ),
          };
        }
      );
      // Also update the selected variable if it's the one being rejected
      if (selectedVariable?.name === variables.name) {
        setSelectedVariable({ ...selectedVariable, status: "rejected" });
      }
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      setApprovalComment("");
    },
  });

  // Custom bulk approve function with progress
  const handleBulkApprove = async (domain: string) => {
    setIsBulkApproving(true);

    try {
      const result = await metadataApi.bulkApproveVariables(domain, "Bulk approved");

      // Close the modal
      setConfirmModal(prev => ({ ...prev, isOpen: false }));

      // Refresh data
      queryClient.invalidateQueries({ queryKey: ["variables"] });
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      queryClient.invalidateQueries({ queryKey: ["versions"] });

      toast.success("Variables Approved", `Successfully approved ${result.approved} variables in ${domain}.`);
    } catch (error) {
      toast.error("Approval Failed", error instanceof Error ? error.message : "Failed to approve variables.");
      setConfirmModal(prev => ({ ...prev, isOpen: false }));
    } finally {
      setIsBulkApproving(false);
    }
  };

  // Export handler
  const handleExport = async (approvedOnly: boolean) => {
    setIsExporting(true);
    setShowExportOptions(false);
    try {
      await metadataApi.exportMetadata(approvedOnly);
      toast.success("Export Complete", `Metadata exported successfully.`);
    } catch (error) {
      toast.error("Export Failed", error instanceof Error ? error.message : "Failed to export metadata.");
    } finally {
      setIsExporting(false);
    }
  };

  const approveCodelistMutation = useMutation({
    mutationFn: ({ name, comment }: { name: string; comment?: string }) =>
      metadataApi.approveCodelist(name, comment),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["codelists"] });
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
    },
  });

  const autoApprovalMutation = useMutation({
    mutationFn: (dryRun: boolean) => metadataApi.runAutoApproval(dryRun),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      queryClient.invalidateQueries({ queryKey: ["variables"] });
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
    },
  });

  const deleteDomainMutation = useMutation({
    mutationFn: (name: string) => metadataApi.deleteDomain(name),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      queryClient.invalidateQueries({ queryKey: ["variables"] });
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
      setDomainToDelete(null);
    },
  });

  const rollbackMutation = useMutation({
    mutationFn: (versionId: string) => metadataApi.rollbackVersion(versionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["domains"] });
      queryClient.invalidateQueries({ queryKey: ["variables"] });
      queryClient.invalidateQueries({ queryKey: ["codelists"] });
      queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
      queryClient.invalidateQueries({ queryKey: ["versions"] });
      setVersionToRestore(null);
      toast.success("Rollback Complete", "Successfully restored to the selected version.");
    },
    onError: (error: Error) => {
      toast.error("Rollback Failed", error.message || "Failed to restore version.");
    },
  });

  // State for delete confirmation
  const [domainToDelete, setDomainToDelete] = useState<string | null>(null);
  const [versionToRestore, setVersionToRestore] = useState<{ id: string; number: number } | null>(null);

  // Compare tab state
  const [compareVersion1, setCompareVersion1] = useState<string | null>(null);
  const [compareVersion2, setCompareVersion2] = useState<string | null>(null);
  const [isComparing, setIsComparing] = useState(false);
  const [compareResult, setCompareResult] = useState<{
    added: Record<string, unknown>[];
    removed: Record<string, unknown>[];
    modified: Record<string, unknown>[];
  } | null>(null);

  // Compare versions handler
  const handleCompareVersions = async () => {
    if (!compareVersion1 || !compareVersion2) return;
    setIsComparing(true);
    setCompareResult(null);
    try {
      const result = await metadataApi.compareVersions(compareVersion1, compareVersion2);
      setCompareResult({
        added: result.added as unknown as Record<string, unknown>[],
        removed: result.removed as unknown as Record<string, unknown>[],
        modified: result.modified as unknown as Record<string, unknown>[],
      });
    } catch (error) {
      toast.error("Compare Failed", error instanceof Error ? error.message : "Failed to compare versions.");
    } finally {
      setIsComparing(false);
    }
  };

  // Column definitions
  const domainColumns: ColumnDef<MetadataDomain>[] = [
    {
      accessorKey: "name",
      header: "Domain",
      cell: ({ row }) => (
        <button
          className="text-[var(--primary)] hover:underline font-medium flex items-center gap-1"
          onClick={() => {
            setSelectedDomain(row.original.name);
            setVariablesPageIndex(0);  // Reset to first page when changing domain
            setActiveTab("variables");
          }}
        >
          {row.original.name}
          <ChevronRight className="w-4 h-4" />
        </button>
      ),
    },
    {
      accessorKey: "description",
      header: "Description",
    },
    {
      accessorKey: "variables_count",
      header: "Variables",
    },
    {
      id: "approved_count",
      header: "Approved",
      cell: ({ row }) => (
        <span className="text-[var(--success)] font-medium">
          {row.original.approved_count || 0}
        </span>
      ),
    },
    {
      id: "pending_count",
      header: "Pending",
      cell: ({ row }) => (
        <span className={`font-medium ${(row.original.pending_count || 0) > 0 ? 'text-[var(--warning)]' : 'text-[var(--muted)]'}`}>
          {row.original.pending_count || 0}
        </span>
      ),
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => <ApprovalBadge status={row.original.status} />,
    },
    {
      id: "actions",
      header: "Actions",
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <button
            className="btn btn-sm btn-outline"
            onClick={() => {
              setBulkApproveProgress(undefined);
              setConfirmModal({
                isOpen: true,
                title: "Approve All Variables",
                message: `Are you sure you want to approve all pending variables in ${row.original.name}? This action cannot be undone.`,
                confirmText: "Approve All",
                variant: "info",
                onConfirm: () => {
                  handleBulkApprove(row.original.name);
                },
              });
            }}
            disabled={isBulkApproving}
          >
            <Check className="w-3 h-3 mr-1" />
            Approve All
          </button>
          <button
            className="btn btn-sm btn-outline text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20"
            onClick={() => setDomainToDelete(row.original.name)}
            title="Delete domain"
          >
            <Trash2 className="w-3 h-3" />
          </button>
        </div>
      ),
    },
  ];

  const variableColumns: ColumnDef<MetadataVariable>[] = [
    {
      accessorKey: "name",
      header: "Variable",
      cell: ({ row }) => (
        <button
          className="text-[var(--primary)] hover:underline font-medium"
          onClick={() => {
            setSelectedVariable(row.original);
            setShowDetailPanel(true);
          }}
        >
          {row.original.name}
        </button>
      ),
    },
    {
      accessorKey: "label",
      header: "Label",
    },
    {
      accessorKey: "type",
      header: "Type",
    },
    {
      accessorKey: "origin",
      header: "Origin",
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => <ApprovalBadge status={row.original.status} />,
    },
    {
      id: "actions",
      header: "Actions",
      cell: ({ row }) => (
        <div className="flex gap-2">
          <button
            className="p-1 text-[var(--success)] hover:bg-[var(--success)]/10 rounded"
            onClick={() => approveVariableMutation.mutate({
              domain: row.original.domain,
              name: row.original.name,
            })}
            title="Approve"
            disabled={row.original.status === "approved"}
          >
            <Check className="w-4 h-4" />
          </button>
          <button
            className="p-1 text-[var(--destructive)] hover:bg-[var(--destructive)]/10 rounded"
            onClick={() => {
              const comment = prompt("Rejection reason:");
              if (comment) {
                rejectVariableMutation.mutate({
                  domain: row.original.domain,
                  name: row.original.name,
                  comment,
                });
              }
            }}
            title="Reject"
            disabled={row.original.status === "rejected"}
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      ),
    },
  ];

  const codelistColumns: ColumnDef<MetadataCodelist>[] = [
    {
      accessorKey: "name",
      header: "Codelist",
    },
    {
      accessorKey: "label",
      header: "Label",
    },
    {
      accessorKey: "data_type",
      header: "Type",
    },
    {
      id: "values_count",
      header: "Values",
      cell: ({ row }) => row.original.values?.length || 0,
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => <ApprovalBadge status={row.original.status} />,
    },
    {
      id: "actions",
      header: "Actions",
      cell: ({ row }) => (
        <button
          className="p-1 text-[var(--success)] hover:bg-[var(--success)]/10 rounded"
          onClick={() => approveCodelistMutation.mutate({ name: row.original.name })}
          title="Approve"
          disabled={row.original.status === "approved"}
        >
          <Check className="w-4 h-4" />
        </button>
      ),
    },
  ];

  const issueColumns: ColumnDef<MetadataIssue>[] = [
    {
      accessorKey: "severity",
      header: "Severity",
      cell: ({ row }) => {
        const severity = row.original.severity;
        return (
          <StatusBadge
            variant={
              severity === "error"
                ? "destructive"
                : severity === "warning"
                ? "warning"
                : "primary"
            }
          >
            <span className="flex items-center gap-1">
              {severity === "error" && <AlertCircle className="w-3 h-3" />}
              {severity === "warning" && <AlertTriangle className="w-3 h-3" />}
              {severity === "info" && <Info className="w-3 h-3" />}
              {severity}
            </span>
          </StatusBadge>
        );
      },
    },
    {
      accessorKey: "domain",
      header: "Domain",
    },
    {
      accessorKey: "variable",
      header: "Variable",
    },
    {
      accessorKey: "message",
      header: "Message",
    },
    {
      accessorKey: "rule",
      header: "Rule",
    },
  ];

  const handleFileSelect = (file: File) => {
    setSelectedFile(file);
    setUploadStep('idle');
    setUploadProgress('');
  };

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      handleFileSelect(file);
    }
  };

  const handleDrag = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  }, []);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      const file = e.dataTransfer.files[0];
      if (file.name.endsWith('.xlsx') || file.name.endsWith('.xls')) {
        handleFileSelect(file);
      } else {
        setUploadStep('error');
        setUploadProgress('Invalid file type. Please upload an Excel file (.xlsx or .xls)');
      }
    }
  }, []);

  const startUpload = () => {
    if (selectedFile) {
      uploadMutation.mutate(selectedFile);
    }
  };

  const resetUpload = () => {
    setSelectedFile(null);
    setUploadStep('idle');
    setUploadProgress('');
    uploadMutation.reset();
  };

  // Open audit config modal
  const openAuditConfig = useCallback(() => {
    setShowAuditConfig(true);
  }, []);

  // Start audit process with SSE
  const startAudit = useCallback(async (skipLLM: boolean = true) => {
    setShowAuditConfig(false);
    setShowAuditModal(true);
    setAuditStep(1);
    setAuditProgress({ current: 0, total: 0, message: 'Connecting...' });
    setAuditStartTime(Date.now());
    setAuditStats({ cdisc_approved: 0, llm_approved: 0, quick_review: 0, manual_review: 0, needs_llm: 0 });
    setAuditComplete(false);
    setAuditError(null);

    try {
      // Get auth token from localStorage (same key as authStore.ts)
      const token = localStorage.getItem('auth_token');

      if (!token) {
        setAuditError('Not authenticated. Please log in again.');
        return;
      }

      // Use fetch with streaming (EventSource doesn't support auth headers)
      // Pass skip_llm parameter to backend
      const response = await fetch(`/api/v1/metadata/audit/stream?skip_llm=${skipLLM}`, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Accept': 'text/event-stream',
        },
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      const reader = response.body?.getReader();
      const decoder = new TextDecoder();

      if (!reader) {
        throw new Error('No response body');
      }

      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const data = JSON.parse(line.slice(6));

              if (data.type === 'progress') {
                setAuditStep(data.data.step);
                setAuditProgress({
                  current: data.data.current,
                  total: data.data.total,
                  message: data.data.message
                });
                setAuditStats({
                  cdisc_approved: data.data.cdisc_approved || 0,
                  llm_approved: data.data.llm_approved || 0,
                  quick_review: data.data.quick_review || 0,
                  manual_review: data.data.manual_review || 0,
                  needs_llm: data.data.needs_llm || 0
                });
              } else if (data.type === 'complete') {
                setAuditStep(3);
                setAuditComplete(true);
                setAuditStats({
                  cdisc_approved: data.data.cdisc_approved || 0,
                  llm_approved: data.data.llm_approved || 0,
                  quick_review: data.data.quick_review || 0,
                  manual_review: data.data.manual_review || 0,
                  needs_llm: 0
                });
                setAuditProgress({
                  current: data.data.total_variables,
                  total: data.data.total_variables,
                  message: `Audit complete! ${data.data.approval_rate}% auto-approved`
                });
                // Refresh data - wrap in try-catch to handle token expiry gracefully
                try {
                  await queryClient.invalidateQueries({ queryKey: ["domains"] });
                  await queryClient.invalidateQueries({ queryKey: ["variables"] });
                  await queryClient.invalidateQueries({ queryKey: ["metadata-stats"] });
                } catch (refreshError) {
                  console.warn('Failed to refresh data after audit - you may need to refresh the page:', refreshError);
                  // Don't fail the audit completion, just show a message
                  setAuditProgress(prev => ({
                    ...prev,
                    message: `${prev.message} - Refresh page to see updated data.`
                  }));
                }
              } else if (data.type === 'error') {
                setAuditError(data.data.message);
              }
            } catch (e) {
              console.error('Failed to parse SSE data:', e);
            }
          }
        }
      }
    } catch (error) {
      setAuditError(error instanceof Error ? error.message : 'Unknown error');
    }
  }, [queryClient]);

  const closeAuditModal = () => {
    setShowAuditModal(false);
    setAuditStep(1);
    setAuditComplete(false);
    setAuditError(null);
  };

  const tabs = [
    { id: "domains", label: "Domains", icon: List },
    { id: "variables", label: "Variables", icon: FileSearch },
    { id: "codelists", label: "Codelists", icon: Database },
    { id: "issues", label: "Issues", icon: AlertTriangle },
    { id: "upload", label: "Upload Spec", icon: Upload },
    { id: "compare", label: "Compare", icon: GitCompare },
    { id: "history", label: "History", icon: History },
  ] as const;

  const issueCounts = {
    errors: issues?.filter((i) => i.severity === "error").length || 0,
    warnings: issues?.filter((i) => i.severity === "warning").length || 0,
    info: issues?.filter((i) => i.severity === "info").length || 0,
  };

  return (
    <div className="space-y-5" onClick={() => showExportOptions && setShowExportOptions(false)}>
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Metadata Auditor
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Audit and approve CDISC metadata specifications
          </p>
        </div>
        <div className="flex gap-2">
          {/* Export Button with Dropdown */}
          <div className="relative" onClick={(e) => e.stopPropagation()}>
            <button
              className="btn btn-outline btn-md"
              onClick={() => setShowExportOptions(!showExportOptions)}
              disabled={isExporting || (stats?.total_variables || 0) === 0}
            >
              {isExporting ? (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Download className="w-4 h-4 mr-2" />
              )}
              {isExporting ? "Exporting..." : "Export"}
            </button>
            {showExportOptions && (
              <div className="absolute right-0 mt-2 w-56 bg-[var(--card)] rounded-lg shadow-xl border border-[var(--border)] z-50">
                <div className="p-1">
                  <button
                    className="w-full text-left px-4 py-2 text-sm hover:bg-[var(--muted)]/20 rounded-md flex items-center gap-2"
                    onClick={() => handleExport(true)}
                  >
                    <CheckCircle2 className="w-4 h-4 text-green-500" />
                    <div>
                      <div className="font-medium">Approved Only</div>
                      <div className="text-xs text-[var(--muted)]">{stats?.approved_variables || 0} variables</div>
                    </div>
                  </button>
                  <button
                    className="w-full text-left px-4 py-2 text-sm hover:bg-[var(--muted)]/20 rounded-md flex items-center gap-2"
                    onClick={() => handleExport(false)}
                  >
                    <Database className="w-4 h-4 text-blue-500" />
                    <div>
                      <div className="font-medium">All Metadata</div>
                      <div className="text-xs text-[var(--muted)]">{stats?.total_variables || 0} variables</div>
                    </div>
                  </button>
                </div>
              </div>
            )}
          </div>

          <button
            className="btn btn-primary btn-md"
            onClick={openAuditConfig}
            disabled={(stats?.pending_variables || 0) === 0}
          >
            <Sparkles className="w-4 h-4 mr-2" />
            Audit Variables
            {(stats?.pending_variables || 0) > 0 && (
              <span className="ml-2 px-2 py-0.5 rounded-full bg-white/20 text-xs">
                {stats?.pending_variables} pending
              </span>
            )}
          </button>
        </div>
      </div>

      {/* Auto-Approval Results */}
      {autoApprovalMutation.isSuccess && (
        <div className="wp-box p-4 border-l-4 border-[var(--success)]">
          <div className="flex items-start gap-3">
            <CheckCircle2 className="w-5 h-5 text-[var(--success)] mt-0.5" />
            <div className="flex-1">
              <div className="font-medium">{autoApprovalMutation.data.message}</div>
              <div className="text-sm text-[var(--muted)] mt-1 grid grid-cols-4 gap-4">
                <div>
                  <span className="font-bold text-[var(--success)]">{autoApprovalMutation.data.auto_approved}</span>
                  {" "}auto-approved ({autoApprovalMutation.data.auto_approved_pct}%)
                </div>
                <div>
                  <span className="font-bold text-[var(--warning)]">{autoApprovalMutation.data.quick_review}</span>
                  {" "}quick review
                </div>
                <div>
                  <span className="font-bold text-[var(--destructive)]">{autoApprovalMutation.data.manual_review}</span>
                  {" "}manual review
                </div>
                <div>
                  <span className="text-[var(--muted)]">{autoApprovalMutation.data.processing_time_seconds}s</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Stats Overview */}
      <div className="grid grid-cols-4 gap-4">
        <div className="wp-box p-4">
          <div className="text-sm text-[var(--muted)] mb-1">Total Domains</div>
          <div className="text-2xl font-bold">{stats?.total_domains || domains?.length || 0}</div>
          <div className="text-xs text-[var(--success)] mt-1">
            {stats?.approved_domains || 0} approved
          </div>
        </div>
        <div className="wp-box p-4">
          <div className="text-sm text-[var(--muted)] mb-1">Total Variables</div>
          <div className="text-2xl font-bold">{stats?.total_variables || 0}</div>
          <div className="text-xs text-[var(--success)] mt-1">
            {stats?.approved_variables || 0} approved
          </div>
        </div>
        <div className="wp-box p-4">
          <div className="text-sm text-[var(--muted)] mb-1">Total Codelists</div>
          <div className="text-2xl font-bold">{stats?.total_codelists || 0}</div>
          <div className="text-xs text-[var(--success)] mt-1">
            {stats?.approved_codelists || 0} approved
          </div>
        </div>
        <div className="wp-box p-4">
          <div className="text-sm text-[var(--muted)] mb-1">Pending Approval</div>
          <div className="text-2xl font-bold text-[var(--warning)]">
            {stats?.pending_variables || 0}
          </div>
          <div className="text-xs text-[var(--muted)] mt-1">
            variables awaiting review
          </div>
        </div>
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
      {activeTab === "domains" && (
        <WPBox title="CDISC Domains">
          {domainsLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
            </div>
          ) : (
            <DataTable
              columns={domainColumns}
              data={domains || []}
              searchColumn="name"
              searchPlaceholder="Search domains..."
            />
          )}
        </WPBox>
      )}

      {activeTab === "variables" && (
        <WPBox
          title={selectedDomain ? `Variables: ${selectedDomain}` : "Variables"}
          headerAction={
            <div className="flex gap-2">
              {selectedDomain && (
                <>
                  <button
                    className="btn btn-sm btn-outline"
                    onClick={() => {
                      setSelectedDomain(null);
                      setVariablesPageIndex(0);
                      setActiveTab("domains");
                    }}
                  >
                    Back to Domains
                  </button>
                  <button
                    className="btn btn-sm btn-primary"
                    onClick={() => {
                      setBulkApproveProgress(undefined);
                      setConfirmModal({
                        isOpen: true,
                        title: "Approve All Variables",
                        message: `Are you sure you want to approve all pending variables in ${selectedDomain}? This action cannot be undone.`,
                        confirmText: "Approve All",
                        variant: "info",
                        onConfirm: () => {
                          handleBulkApprove(selectedDomain!);
                        },
                      });
                    }}
                    disabled={isBulkApproving}
                  >
                    <Check className="w-3 h-3 mr-1" />
                    {isBulkApproving ? "Approving..." : "Approve All"}
                  </button>
                </>
              )}
            </div>
          }
        >
          {!selectedDomain ? (
            <div className="text-center py-8 text-[var(--muted)]">
              <FileSearch className="w-12 h-12 mx-auto mb-4" />
              <p>Select a domain from the Domains tab to view its variables</p>
            </div>
          ) : variablesLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
            </div>
          ) : (
            <DataTable
              columns={variableColumns}
              data={variables?.items || []}
              searchColumn="name"
              searchPlaceholder="Search variables..."
              pageIndex={variablesPageIndex}
              onPageChange={setVariablesPageIndex}
            />
          )}
        </WPBox>
      )}

      {activeTab === "codelists" && (
        <WPBox title="Codelists">
          {codelistsLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
            </div>
          ) : (
            <DataTable
              columns={codelistColumns}
              data={codelists || []}
              searchColumn="name"
              searchPlaceholder="Search codelists..."
            />
          )}
        </WPBox>
      )}

      {activeTab === "issues" && (
        <WPBox title="Audit Issues">
          <div className="grid grid-cols-3 gap-4 mb-4">
            <div className="flex items-center gap-3 p-3 bg-[var(--destructive)]/10 rounded">
              <AlertCircle className="w-6 h-6 text-[var(--destructive)]" />
              <div>
                <div className="font-bold">{issueCounts.errors}</div>
                <div className="text-sm text-[var(--muted)]">Errors</div>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 bg-[var(--warning)]/10 rounded">
              <AlertTriangle className="w-6 h-6 text-[var(--warning)]" />
              <div>
                <div className="font-bold">{issueCounts.warnings}</div>
                <div className="text-sm text-[var(--muted)]">Warnings</div>
              </div>
            </div>
            <div className="flex items-center gap-3 p-3 bg-[var(--primary)]/10 rounded">
              <Info className="w-6 h-6 text-[var(--primary)]" />
              <div>
                <div className="font-bold">{issueCounts.info}</div>
                <div className="text-sm text-[var(--muted)]">Info</div>
              </div>
            </div>
          </div>
          {issuesLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
            </div>
          ) : (
            <DataTable
              columns={issueColumns}
              data={issues || []}
              searchColumn="message"
              searchPlaceholder="Search issues..."
            />
          )}
        </WPBox>
      )}

      {activeTab === "upload" && (
        <WPBox title="Upload Specification">
          <div className="space-y-6">
            {/* Instructions */}
            <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-4">
              <div className="flex items-start gap-3">
                <Info className="w-5 h-5 text-blue-500 mt-0.5 flex-shrink-0" />
                <div className="text-sm">
                  <p className="font-medium text-blue-800 dark:text-blue-300 mb-1">Upload Instructions</p>
                  <ul className="text-blue-700 dark:text-blue-400 space-y-1">
                    <li>Upload SDTM or ADaM specification Excel files (.xlsx, .xls)</li>
                    <li>You can upload one spec at a time - they will be merged</li>
                    <li>Recommended order: SDTM spec first, then ADaM spec</li>
                    <li>After upload, review the Domains tab to verify parsing</li>
                  </ul>
                </div>
              </div>
            </div>

            {/* Drop Zone */}
            <div
              className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
                dragActive
                  ? 'border-[var(--primary)] bg-[var(--primary)]/5'
                  : selectedFile
                    ? 'border-green-400 bg-green-50 dark:bg-green-900/20'
                    : 'border-[var(--border)] hover:border-[var(--primary)]/50'
              }`}
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
            >
              {selectedFile ? (
                <div className="space-y-4">
                  <FileSpreadsheet className="w-16 h-16 mx-auto text-green-500" />
                  <div>
                    <p className="text-lg font-medium text-[var(--foreground)]">{selectedFile.name}</p>
                    <p className="text-sm text-[var(--muted)]">
                      {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
                    </p>
                  </div>
                  <div className="flex justify-center gap-3">
                    <button
                      className="btn btn-outline btn-md"
                      onClick={resetUpload}
                      disabled={uploadMutation.isPending}
                    >
                      <X className="w-4 h-4 mr-2" />
                      Remove
                    </button>
                    <button
                      className="btn btn-primary btn-md"
                      onClick={startUpload}
                      disabled={uploadMutation.isPending}
                    >
                      {uploadMutation.isPending ? (
                        <>
                          <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                          Processing...
                        </>
                      ) : (
                        <>
                          <Upload className="w-4 h-4 mr-2" />
                          Upload & Process
                        </>
                      )}
                    </button>
                  </div>
                </div>
              ) : (
                <>
                  <Upload className={`w-12 h-12 mx-auto mb-4 ${dragActive ? 'text-[var(--primary)]' : 'text-[var(--muted)]'}`} />
                  <p className="text-[var(--foreground)] mb-2">
                    {dragActive ? 'Drop your file here' : 'Drag & drop your Excel specification'}
                  </p>
                  <p className="text-sm text-[var(--muted)] mb-4">
                    or click to browse
                  </p>
                  <label className="btn btn-primary btn-md cursor-pointer inline-flex">
                    <input
                      type="file"
                      className="hidden"
                      accept=".xlsx,.xls"
                      onChange={handleFileUpload}
                    />
                    Choose File
                  </label>
                </>
              )}
            </div>

            {/* Progress Steps */}
            {(uploadMutation.isPending || uploadStep === 'complete' || uploadStep === 'error') && (
              <div className="border border-[var(--border)] rounded-lg p-4">
                <h4 className="font-medium mb-4">Processing Steps</h4>
                <div className="space-y-3">
                  {/* Step 1: Upload */}
                  <div className="flex items-center gap-3">
                    {uploadStep === 'uploading' ? (
                      <Loader2 className="w-5 h-5 text-[var(--primary)] animate-spin" />
                    ) : uploadStep === 'complete' || uploadStep === 'parsing' || uploadStep === 'merging' ? (
                      <CheckCircle2 className="w-5 h-5 text-green-500" />
                    ) : uploadStep === 'error' ? (
                      <XCircle className="w-5 h-5 text-red-500" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                    )}
                    <span className={uploadStep !== 'idle' ? 'text-[var(--foreground)]' : 'text-[var(--muted)]'}>
                      Uploading file to server
                    </span>
                  </div>

                  {/* Step 2: Parse */}
                  <div className="flex items-center gap-3">
                    {uploadStep === 'parsing' ? (
                      <Loader2 className="w-5 h-5 text-[var(--primary)] animate-spin" />
                    ) : uploadStep === 'complete' || uploadStep === 'merging' ? (
                      <CheckCircle2 className="w-5 h-5 text-green-500" />
                    ) : uploadStep === 'error' && uploadMutation.isPending ? (
                      <XCircle className="w-5 h-5 text-red-500" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                    )}
                    <span className={['parsing', 'merging', 'complete'].includes(uploadStep) ? 'text-[var(--foreground)]' : 'text-[var(--muted)]'}>
                      Parsing Excel specification (detecting domains, variables, codelists)
                    </span>
                  </div>

                  {/* Step 3: Merge */}
                  <div className="flex items-center gap-3">
                    {uploadStep === 'merging' ? (
                      <Loader2 className="w-5 h-5 text-[var(--primary)] animate-spin" />
                    ) : uploadStep === 'complete' ? (
                      <CheckCircle2 className="w-5 h-5 text-green-500" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                    )}
                    <span className={['merging', 'complete'].includes(uploadStep) ? 'text-[var(--foreground)]' : 'text-[var(--muted)]'}>
                      Merging into metadata store & saving
                    </span>
                  </div>

                  {/* Step 4: Complete */}
                  <div className="flex items-center gap-3">
                    {uploadStep === 'complete' ? (
                      <CheckCircle2 className="w-5 h-5 text-green-500" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                    )}
                    <span className={uploadStep === 'complete' ? 'text-[var(--foreground)]' : 'text-[var(--muted)]'}>
                      Complete
                    </span>
                  </div>
                </div>
              </div>
            )}

            {/* Success Message */}
            {uploadStep === 'complete' && (
              <div className="p-4 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 rounded-lg">
                <div className="flex items-center gap-3">
                  <CheckCircle2 className="w-6 h-6 text-green-500 flex-shrink-0" />
                  <div>
                    <p className="font-medium text-green-800 dark:text-green-300">Upload Successful!</p>
                    <p className="text-sm text-green-700 dark:text-green-400">{uploadProgress}</p>
                  </div>
                </div>
                <div className="mt-4 flex gap-3">
                  <button
                    className="btn btn-outline btn-sm"
                    onClick={resetUpload}
                  >
                    Upload Another File
                  </button>
                  <button
                    className="btn btn-primary btn-sm"
                    onClick={() => setActiveTab('domains')}
                  >
                    View Domains
                  </button>
                </div>
              </div>
            )}

            {/* Error Message */}
            {uploadStep === 'error' && (
              <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg">
                <div className="flex items-center gap-3">
                  <AlertCircle className="w-6 h-6 text-red-500 flex-shrink-0" />
                  <div>
                    <p className="font-medium text-red-800 dark:text-red-300">Upload Failed</p>
                    <p className="text-sm text-red-700 dark:text-red-400">{uploadProgress}</p>
                  </div>
                </div>
                <div className="mt-4">
                  <button
                    className="btn btn-outline btn-sm"
                    onClick={resetUpload}
                  >
                    Try Again
                  </button>
                </div>
              </div>
            )}
          </div>
        </WPBox>
      )}

      {activeTab === "compare" && (
        <WPBox title="Version Comparison">
          {versionsLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
            </div>
          ) : versions && versions.length >= 2 ? (
            <div className="space-y-4">
              <p className="text-sm text-[var(--muted)]">
                Select two versions to compare their differences. The comparison will show added, removed, and modified variables between the versions.
              </p>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium mb-2">From Version (Older)</label>
                  <select
                    className="w-full p-2 border border-[var(--border)] rounded bg-[var(--background)] text-[var(--foreground)]"
                    value={compareVersion1 || ""}
                    onChange={(e) => setCompareVersion1(e.target.value || null)}
                  >
                    <option value="">Select a version...</option>
                    {versions.map((v) => (
                      <option key={v.version_id} value={v.version_id}>
                        v{v.version_number} - {v.comment || "No description"} ({new Date(v.created_at).toLocaleDateString()})
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium mb-2">To Version (Newer)</label>
                  <select
                    className="w-full p-2 border border-[var(--border)] rounded bg-[var(--background)] text-[var(--foreground)]"
                    value={compareVersion2 || ""}
                    onChange={(e) => setCompareVersion2(e.target.value || null)}
                  >
                    <option value="">Select a version...</option>
                    {versions.map((v) => (
                      <option key={v.version_id} value={v.version_id}>
                        v{v.version_number} - {v.comment || "No description"} ({new Date(v.created_at).toLocaleDateString()})
                      </option>
                    ))}
                  </select>
                </div>
              </div>
              <button
                className="btn btn-primary"
                onClick={handleCompareVersions}
                disabled={!compareVersion1 || !compareVersion2 || isComparing}
              >
                {isComparing ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Comparing...
                  </>
                ) : (
                  <>
                    <GitCompare className="w-4 h-4 mr-2" />
                    Compare Versions
                  </>
                )}
              </button>
              {compareResult && (
                <div className="space-y-4 mt-6">
                  <div className="grid grid-cols-3 gap-4">
                    <div className="p-3 bg-green-50 dark:bg-green-900/20 rounded-lg border border-green-200 dark:border-green-800">
                      <div className="text-2xl font-bold text-green-600">{compareResult.added.length}</div>
                      <div className="text-sm text-green-700 dark:text-green-400">Added</div>
                    </div>
                    <div className="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg border border-red-200 dark:border-red-800">
                      <div className="text-2xl font-bold text-red-600">{compareResult.removed.length}</div>
                      <div className="text-sm text-red-700 dark:text-red-400">Removed</div>
                    </div>
                    <div className="p-3 bg-amber-50 dark:bg-amber-900/20 rounded-lg border border-amber-200 dark:border-amber-800">
                      <div className="text-2xl font-bold text-amber-600">{compareResult.modified.length}</div>
                      <div className="text-sm text-amber-700 dark:text-amber-400">Modified</div>
                    </div>
                  </div>
                  {compareResult.added.length > 0 && (
                    <div>
                      <h4 className="font-medium text-green-600 mb-2">Added Variables</h4>
                      <div className="space-y-1">
                        {compareResult.added.slice(0, 10).map((item: Record<string, unknown>, i: number) => {
                          const domain = String(item.domain || "");
                          const name = String(item.name || "");
                          return (
                            <div key={i} className="p-2 bg-green-50 dark:bg-green-900/10 rounded text-sm">
                              <span className="font-medium">{domain || name}</span>
                              {name && domain && <span>.{name}</span>}
                            </div>
                          );
                        })}
                        {compareResult.added.length > 10 && (
                          <div className="text-sm text-[var(--muted)]">
                            ... and {compareResult.added.length - 10} more
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {compareResult.removed.length > 0 && (
                    <div>
                      <h4 className="font-medium text-red-600 mb-2">Removed Variables</h4>
                      <div className="space-y-1">
                        {compareResult.removed.slice(0, 10).map((item: Record<string, unknown>, i: number) => {
                          const domain = String(item.domain || "");
                          const name = String(item.name || "");
                          return (
                            <div key={i} className="p-2 bg-red-50 dark:bg-red-900/10 rounded text-sm">
                              <span className="font-medium">{domain || name}</span>
                              {name && domain && <span>.{name}</span>}
                            </div>
                          );
                        })}
                        {compareResult.removed.length > 10 && (
                          <div className="text-sm text-[var(--muted)]">
                            ... and {compareResult.removed.length - 10} more
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {compareResult.modified.length > 0 && (
                    <div>
                      <h4 className="font-medium text-amber-600 mb-2">Modified Variables</h4>
                      <div className="space-y-1">
                        {compareResult.modified.slice(0, 10).map((item: Record<string, unknown>, i: number) => {
                          const domain = String(item.domain || "");
                          const name = String(item.name || "");
                          return (
                            <div key={i} className="p-2 bg-amber-50 dark:bg-amber-900/10 rounded text-sm">
                              <span className="font-medium">{domain || name}</span>
                              {name && domain && <span>.{name}</span>}
                            </div>
                          );
                        })}
                        {compareResult.modified.length > 10 && (
                          <div className="text-sm text-[var(--muted)]">
                            ... and {compareResult.modified.length - 10} more
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {compareResult.added.length === 0 && compareResult.removed.length === 0 && compareResult.modified.length === 0 && (
                    <div className="text-center py-4 text-[var(--muted)]">
                      <CheckCircle2 className="w-8 h-8 mx-auto mb-2 text-green-500" />
                      <p>No differences found between these versions</p>
                    </div>
                  )}
                </div>
              )}
            </div>
          ) : (
            <div className="text-center py-8 text-[var(--muted)]">
              <GitCompare className="w-12 h-12 mx-auto mb-4" />
              <p>Compare different versions of metadata specifications</p>
              <p className="text-sm mt-2">
                {versions && versions.length === 1
                  ? "Only one version exists. Import more specifications to enable comparison."
                  : "No versions available. Import specifications first."}
              </p>
            </div>
          )}
        </WPBox>
      )}

      {activeTab === "history" && (
        <WPBox title="Version History">
          {versionsLoading ? (
            <div className="flex items-center justify-center py-8">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
            </div>
          ) : versions && versions.length > 0 ? (
            <div className="space-y-3">
              {versions.map((version, index) => (
                <div
                  key={version.version_id}
                  className={`p-4 rounded-lg border ${
                    index === 0
                      ? "border-[var(--primary)] bg-[var(--primary)]/5"
                      : "border-[var(--border)]"
                  }`}
                >
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-3">
                      <div
                        className={`p-2 rounded-full ${
                          index === 0
                            ? "bg-[var(--primary)]/10"
                            : "bg-[var(--muted)]/10"
                        }`}
                      >
                        <History
                          className={`w-5 h-5 ${
                            index === 0
                              ? "text-[var(--primary)]"
                              : "text-[var(--muted)]"
                          }`}
                        />
                      </div>
                      <div>
                        <div className="flex items-center gap-2">
                          <span className="font-medium">
                            Version {version.version_number}
                          </span>
                          {index === 0 && (
                            <span className="px-2 py-0.5 text-xs font-medium bg-[var(--primary)] text-white rounded-full">
                              Current
                            </span>
                          )}
                        </div>
                        <p className="text-sm text-[var(--muted)]">
                          {version.comment || "No description"}
                        </p>
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="text-sm text-[var(--foreground)]">
                        {version.created_by}
                      </div>
                      <div className="text-xs text-[var(--muted)]">
                        {new Date(version.created_at).toLocaleString()}
                      </div>
                    </div>
                  </div>
                  {index > 0 && (
                    <div className="mt-3 pt-3 border-t border-[var(--border)]">
                      <button
                        className="btn btn-sm btn-outline"
                        onClick={() =>
                          setVersionToRestore({
                            id: version.version_id,
                            number: version.version_number,
                          })
                        }
                        disabled={rollbackMutation.isPending}
                      >
                        <RotateCcw className="w-3 h-3 mr-1" />
                        Restore This Version
                      </button>
                    </div>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-8 text-[var(--muted)]">
              <History className="w-12 h-12 mx-auto mb-4" />
              <p>No version history available</p>
              <p className="text-sm mt-2">
                Versions are created when you import specifications or make changes
              </p>
            </div>
          )}
        </WPBox>
      )}

      {/* Variable Detail Panel */}
      {showDetailPanel && selectedVariable && (
        <div className="fixed inset-0 bg-black/50 z-50 flex justify-end">
          <div className="w-[600px] bg-[var(--background)] h-full overflow-y-auto shadow-xl">
            <div className="sticky top-0 bg-[var(--background)] border-b border-[var(--border)] p-4 flex justify-between items-center">
              <div>
                <h2 className="text-lg font-bold">{selectedVariable.domain}.{selectedVariable.name}</h2>
                <p className="text-sm text-[var(--muted)]">{selectedVariable.label}</p>
              </div>
              <button
                onClick={() => {
                  setShowDetailPanel(false);
                  setSelectedVariable(null);
                }}
                className="p-2 hover:bg-[var(--muted)]/10 rounded"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            <div className="p-4 space-y-4">
              {/* Status */}
              <div className="flex items-center gap-2">
                <span className="text-sm font-medium">Status:</span>
                <ApprovalBadge status={selectedVariable.status} />
              </div>

              {/* Variable Info */}
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <div className="text-xs text-[var(--muted)]">Type</div>
                  <div className="font-medium">{selectedVariable.type}</div>
                </div>
                <div>
                  <div className="text-xs text-[var(--muted)]">Length</div>
                  <div className="font-medium">{selectedVariable.length || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-[var(--muted)]">Format</div>
                  <div className="font-medium">{selectedVariable.format || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-[var(--muted)]">Origin</div>
                  <div className="font-medium">{selectedVariable.origin || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-[var(--muted)]">Core</div>
                  <div className="font-medium">{selectedVariable.core || "-"}</div>
                </div>
                <div>
                  <div className="text-xs text-[var(--muted)]">Codelist</div>
                  <div className="font-medium">{selectedVariable.codelist || "-"}</div>
                </div>
              </div>

              {/* Derivation */}
              {selectedVariable.derivation && (
                <div>
                  <div className="text-xs text-[var(--muted)] mb-1">Derivation</div>
                  <div className="p-3 bg-[var(--muted)]/10 rounded text-sm font-mono whitespace-pre-wrap">
                    {selectedVariable.derivation}
                  </div>
                </div>
              )}

              {/* Plain English Description */}
              {selectedVariable.plain_english && (
                <div>
                  <div className="text-xs text-[var(--muted)] mb-1">Plain English</div>
                  <div className="p-3 bg-[var(--primary)]/10 rounded text-sm">
                    {selectedVariable.plain_english}
                  </div>
                </div>
              )}

              {/* Comment */}
              {selectedVariable.comment && (
                <div>
                  <div className="text-xs text-[var(--muted)] mb-1">Comment</div>
                  <div className="text-sm">{selectedVariable.comment}</div>
                </div>
              )}

              {/* Approval Actions */}
              <div className="border-t border-[var(--border)] pt-4 mt-4">
                <div className="text-sm font-medium mb-2">Approval Actions</div>
                <div className="space-y-3">
                  <textarea
                    className="w-full p-2 border border-[var(--border)] rounded bg-[var(--background)] text-[var(--foreground)]"
                    placeholder="Add a comment (optional for approval, required for rejection)"
                    rows={2}
                    value={approvalComment}
                    onChange={(e) => setApprovalComment(e.target.value)}
                  />
                  <div className="flex gap-2">
                    <button
                      className="flex-1 btn btn-success"
                      onClick={() => {
                        approveVariableMutation.mutate({
                          domain: selectedVariable.domain,
                          name: selectedVariable.name,
                          comment: approvalComment || undefined,
                        });
                        setShowDetailPanel(false);
                        setSelectedVariable(null);
                      }}
                      disabled={approveVariableMutation.isPending || selectedVariable.status === "approved"}
                    >
                      <Check className="w-4 h-4 mr-1" />
                      Approve
                    </button>
                    <button
                      className="flex-1 btn btn-destructive"
                      onClick={() => {
                        if (!approvalComment) {
                          alert("Please provide a rejection reason");
                          return;
                        }
                        rejectVariableMutation.mutate({
                          domain: selectedVariable.domain,
                          name: selectedVariable.name,
                          comment: approvalComment,
                        });
                        setShowDetailPanel(false);
                        setSelectedVariable(null);
                      }}
                      disabled={rejectVariableMutation.isPending || selectedVariable.status === "rejected"}
                    >
                      <X className="w-4 h-4 mr-1" />
                      Reject
                    </button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Delete Domain Confirmation Modal */}
      {domainToDelete && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center">
          <div className="bg-[var(--background)] rounded-lg shadow-xl p-6 max-w-md w-full mx-4">
            <div className="flex items-center gap-3 mb-4">
              <div className="p-2 bg-red-100 dark:bg-red-900/30 rounded-full">
                <Trash2 className="w-6 h-6 text-red-500" />
              </div>
              <h3 className="text-lg font-bold">Delete Domain</h3>
            </div>
            <p className="text-[var(--muted)] mb-6">
              Are you sure you want to delete <strong>{domainToDelete}</strong> and all its variables?
              This action cannot be undone.
            </p>
            <div className="flex gap-3 justify-end">
              <button
                className="btn btn-outline"
                onClick={() => setDomainToDelete(null)}
                disabled={deleteDomainMutation.isPending}
              >
                Cancel
              </button>
              <button
                className="btn bg-red-500 hover:bg-red-600 text-white"
                onClick={() => deleteDomainMutation.mutate(domainToDelete)}
                disabled={deleteDomainMutation.isPending}
              >
                {deleteDomainMutation.isPending ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Deleting...
                  </>
                ) : (
                  <>
                    <Trash2 className="w-4 h-4 mr-2" />
                    Delete
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Audit Config Modal */}
      {showAuditConfig && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-[var(--background)] rounded-xl shadow-2xl w-full max-w-md overflow-hidden">
            {/* Header */}
            <div className="flex items-center gap-3 p-6 pb-4">
              <div className="p-2.5 bg-[var(--primary)]/10 rounded-full">
                <Sparkles className="w-6 h-6 text-[var(--primary)]" />
              </div>
              <div>
                <h3 className="text-lg font-bold">Audit Configuration</h3>
                <p className="text-sm text-[var(--muted)]">{stats?.pending_variables || 0} variables pending</p>
              </div>
            </div>

            {/* Content */}
            <div className="px-6 space-y-4">
              {/* Description */}
              <div className="p-4 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg">
                <p className="text-sm text-blue-700 dark:text-blue-300">
                  <strong>Step 1:</strong> CDISC Library Check - Variables matching CDISC standards will be auto-approved immediately.
                </p>
                <p className="text-sm text-blue-700 dark:text-blue-300 mt-2">
                  <strong>Step 2:</strong> LLM Analysis - Non-CDISC variables will be analyzed by DeepSeek-R1 for custom variable validation.
                </p>
              </div>

              {/* LLM Option */}
              <div className="p-4 border border-[var(--border)] rounded-lg">
                <label className="flex items-start gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={useLLM}
                    onChange={(e) => setUseLLM(e.target.checked)}
                    className="mt-1 w-4 h-4 rounded border-[var(--border)] text-[var(--primary)] focus:ring-[var(--primary)]"
                  />
                  <div className="flex-1">
                    <span className="font-medium">Enable LLM analysis for non-CDISC variables</span>
                    <p className="text-sm text-[var(--muted)] mt-1">
                      If disabled, non-CDISC variables will be marked for manual review only.
                    </p>
                  </div>
                </label>
              </div>

              {/* Warning when LLM enabled */}
              {useLLM && (
                <div className="p-4 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded-lg">
                  <div className="flex items-start gap-2">
                    <AlertTriangle className="w-5 h-5 text-amber-500 mt-0.5 flex-shrink-0" />
                    <div className="text-sm text-amber-700 dark:text-amber-300">
                      <p className="font-medium">LLM processing can take a long time</p>
                      <p className="mt-1">
                        DeepSeek-R1 analysis may take 30-60 seconds per batch of 10 variables.
                        For {stats?.pending_variables || 0} pending variables, this could take several hours.
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Actions - with proper spacing and elegant buttons */}
            <div className="flex gap-3 justify-end p-6 pt-6 mt-2 border-t border-[var(--border)]">
              <button
                className="px-5 py-2.5 rounded-lg border border-[var(--border)] text-[var(--foreground)] hover:bg-[var(--muted)]/10 transition-colors font-medium"
                onClick={() => setShowAuditConfig(false)}
              >
                Cancel
              </button>
              <button
                className="px-5 py-2.5 rounded-lg bg-[var(--primary)] text-white hover:bg-[var(--primary)]/90 transition-colors font-medium flex items-center gap-2 shadow-sm"
                onClick={() => startAudit(!useLLM)}
              >
                <Sparkles className="w-4 h-4" />
                Start Audit
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Audit Variables Modal */}
      {showAuditModal && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center">
          <div className="bg-[var(--background)] rounded-lg shadow-xl p-6 max-w-2xl w-full mx-4">
            {/* Header */}
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="p-2 bg-[var(--primary)]/10 rounded-full">
                  <Sparkles className="w-6 h-6 text-[var(--primary)]" />
                </div>
                <h3 className="text-lg font-bold">Variable Audit</h3>
              </div>
              {/* Always show close button when complete or error */}
              {(auditComplete || auditError) && (
                <button
                  onClick={closeAuditModal}
                  className="p-2 hover:bg-[var(--muted)]/20 rounded-full transition-colors"
                  title="Close"
                >
                  <X className="w-5 h-5" />
                </button>
              )}
            </div>

            {/* Error State */}
            {auditError && (
              <div className="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg mb-6">
                <div className="flex items-center gap-3">
                  <AlertCircle className="w-5 h-5 text-red-500" />
                  <div>
                    <p className="font-medium text-red-800 dark:text-red-300">Audit Failed</p>
                    <p className="text-sm text-red-700 dark:text-red-400">{auditError}</p>
                  </div>
                </div>
              </div>
            )}

            {/* Progress Steps */}
            <div className="space-y-4 mb-6">
              {/* Step 1: CDISC Check */}
              <div className={`p-4 rounded-lg border ${auditStep >= 1 ? 'border-[var(--primary)] bg-[var(--primary)]/5' : 'border-[var(--border)]'}`}>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    {auditStep > 1 ? (
                      <CheckCircle2 className="w-5 h-5 text-[var(--success)]" />
                    ) : auditStep === 1 ? (
                      <Loader2 className="w-5 h-5 text-[var(--primary)] animate-spin" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                    )}
                    <span className="font-medium">Step 1: CDISC Library Check</span>
                  </div>
                  {auditStep === 1 && auditProgress.total > 0 && (
                    <span className="text-sm text-[var(--muted)]">
                      {auditProgress.current} / {auditProgress.total}
                    </span>
                  )}
                </div>
                {auditStep === 1 && (
                  <>
                    <div className="w-full bg-[var(--muted)]/20 rounded-full h-2 mb-2">
                      <div
                        className="bg-[var(--primary)] h-2 rounded-full transition-all duration-300"
                        style={{ width: auditProgress.total > 0 ? `${(auditProgress.current / auditProgress.total) * 100}%` : '0%' }}
                      />
                    </div>
                    <p className="text-sm text-[var(--muted)]">{auditProgress.message}</p>
                    {getTimeRemaining() && (
                      <p className="text-sm text-[var(--primary)] font-medium mt-1">
                        ⏱ {getTimeRemaining()}
                      </p>
                    )}
                  </>
                )}
                {auditStep > 1 && (
                  <p className="text-sm text-[var(--success)] ml-7">
                    {auditStats.cdisc_approved} variables auto-approved
                  </p>
                )}
              </div>

              {/* Step 2: LLM Analysis */}
              <div className={`p-4 rounded-lg border ${auditStep >= 2 ? 'border-[var(--primary)] bg-[var(--primary)]/5' : 'border-[var(--border)]'}`}>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    {auditStep > 2 ? (
                      <CheckCircle2 className="w-5 h-5 text-[var(--success)]" />
                    ) : auditStep === 2 ? (
                      <Loader2 className="w-5 h-5 text-[var(--primary)] animate-spin" />
                    ) : (
                      <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                    )}
                    <span className="font-medium">Step 2: LLM Analysis</span>
                  </div>
                  {auditStep === 2 && auditProgress.total > 0 && (
                    <span className="text-sm text-[var(--muted)]">
                      {auditProgress.current} / {auditProgress.total}
                    </span>
                  )}
                </div>
                {auditStep === 2 && (
                  <>
                    <div className="w-full bg-[var(--muted)]/20 rounded-full h-2 mb-2">
                      <div
                        className="bg-[var(--primary)] h-2 rounded-full transition-all duration-300"
                        style={{ width: auditProgress.total > 0 ? `${(auditProgress.current / auditProgress.total) * 100}%` : '0%' }}
                      />
                    </div>
                    <p className="text-sm text-[var(--muted)]">{auditProgress.message}</p>
                    {getTimeRemaining() && (
                      <p className="text-sm text-[var(--primary)] font-medium mt-1">
                        ⏱ {getTimeRemaining()}
                      </p>
                    )}
                  </>
                )}
                {auditStep === 1 && auditStats.needs_llm > 0 && (
                  <p className="text-sm text-[var(--muted)] ml-7">
                    {auditStats.needs_llm} variables pending LLM analysis
                  </p>
                )}
                {auditStep > 2 && (
                  <p className="text-sm text-[var(--success)] ml-7">
                    {auditStats.llm_approved} additional variables approved by LLM
                  </p>
                )}
              </div>

              {/* Step 3: Complete */}
              <div className={`p-4 rounded-lg border ${auditStep >= 3 ? 'border-[var(--success)] bg-[var(--success)]/5' : 'border-[var(--border)]'}`}>
                <div className="flex items-center gap-2">
                  {auditComplete ? (
                    <CheckCircle2 className="w-5 h-5 text-[var(--success)]" />
                  ) : (
                    <div className="w-5 h-5 rounded-full border-2 border-[var(--muted)]" />
                  )}
                  <span className="font-medium">Step 3: Complete</span>
                </div>
              </div>
            </div>

            {/* Summary Stats */}
            {(auditStep > 1 || auditComplete) && (
              <div className="grid grid-cols-4 gap-3 mb-6">
                <div className="p-3 bg-[var(--success)]/10 rounded-lg text-center">
                  <div className="text-2xl font-bold text-[var(--success)]">
                    {auditStats.cdisc_approved}
                  </div>
                  <div className="text-xs text-[var(--muted)]">CDISC Approved</div>
                </div>
                <div className="p-3 bg-[var(--primary)]/10 rounded-lg text-center">
                  <div className="text-2xl font-bold text-[var(--primary)]">
                    {auditStats.llm_approved}
                  </div>
                  <div className="text-xs text-[var(--muted)]">LLM Approved</div>
                </div>
                <div className="p-3 bg-[var(--warning)]/10 rounded-lg text-center">
                  <div className="text-2xl font-bold text-[var(--warning)]">
                    {auditStats.quick_review}
                  </div>
                  <div className="text-xs text-[var(--muted)]">Quick Review</div>
                </div>
                <div className="p-3 bg-[var(--destructive)]/10 rounded-lg text-center">
                  <div className="text-2xl font-bold text-[var(--destructive)]">
                    {auditStats.manual_review}
                  </div>
                  <div className="text-xs text-[var(--muted)]">Manual Review</div>
                </div>
              </div>
            )}

            {/* Actions */}
            <div className="flex justify-end gap-3 pt-2 border-t border-[var(--border)]">
              {auditComplete ? (
                <button
                  className="btn btn-primary px-6"
                  onClick={closeAuditModal}
                >
                  <CheckCircle2 className="w-4 h-4 mr-2" />
                  Done
                </button>
              ) : auditError ? (
                <button
                  className="btn btn-primary px-6"
                  onClick={closeAuditModal}
                >
                  Close
                </button>
              ) : (
                <div className="text-sm text-[var(--muted)] flex items-center gap-2 py-2">
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Audit in progress... Please wait
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Confirmation Modal */}
      <ConfirmModal
        isOpen={confirmModal.isOpen}
        onClose={() => {
          setConfirmModal(prev => ({ ...prev, isOpen: false }));
          setBulkApproveProgress(undefined);
        }}
        onConfirm={confirmModal.onConfirm}
        title={confirmModal.title}
        message={confirmModal.message}
        confirmText={confirmModal.confirmText}
        variant={confirmModal.variant}
        isLoading={isBulkApproving}
        progress={bulkApproveProgress}
      />

      {/* Restore Version Confirmation Modal */}
      {versionToRestore && (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
          <div className="bg-[var(--background)] rounded-lg shadow-xl p-6 max-w-md w-full">
            <div className="flex items-center gap-3 mb-4">
              <div className="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-full flex-shrink-0">
                <RotateCcw className="w-6 h-6 text-amber-500" />
              </div>
              <h3 className="text-lg font-bold">Restore Version</h3>
            </div>
            <p className="text-[var(--muted)] mb-6">
              Are you sure you want to restore to <strong>Version {versionToRestore.number}</strong>?
              This will revert all metadata (domains, variables, codelists) to that version's state.
              A new version will be created so you can undo this if needed.
            </p>
            <div className="flex flex-wrap gap-3 justify-end">
              <button
                className="btn btn-outline px-4 py-2"
                onClick={() => setVersionToRestore(null)}
                disabled={rollbackMutation.isPending}
              >
                Cancel
              </button>
              <button
                className="btn bg-amber-500 hover:bg-amber-600 text-white px-4 py-2 flex items-center"
                onClick={() => rollbackMutation.mutate(versionToRestore.id)}
                disabled={rollbackMutation.isPending}
              >
                {rollbackMutation.isPending ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Restoring...
                  </>
                ) : (
                  <>
                    <RotateCcw className="w-4 h-4 mr-2" />
                    Restore
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
