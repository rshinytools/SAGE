import { useState, useCallback } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Search,
  RefreshCw,
  Upload,
  Database,
  CheckCircle,
  XCircle,
  Clock,
  Trash2,
  ChevronRight,
  ChevronDown,
  FileUp,
  AlertCircle,
  Info,
  Layers,
  List,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { meddraApi } from "@/api/meddra";
import type { MedDRATerm, MedDRASearchResult, MedDRAHierarchy, MedDRAPreviewResponse } from "@/api/meddra";

type TabType = "status" | "search" | "browse" | "upload";

export function MedDRALibraryPage() {
  const [activeTab, setActiveTab] = useState<TabType>("status");
  const queryClient = useQueryClient();

  const { data: status, isLoading: statusLoading, refetch: refetchStatus } = useQuery({
    queryKey: ["meddraStatus"],
    queryFn: meddraApi.getStatus,
    refetchInterval: (query) => query.state.data?.loading_in_progress ? 2000 : false,
  });

  const tabs = [
    { id: "status" as TabType, label: "Status", icon: Database },
    { id: "search" as TabType, label: "Search", icon: Search },
    { id: "browse" as TabType, label: "Browse", icon: Layers },
    { id: "upload" as TabType, label: "Upload", icon: Upload },
  ];

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            MedDRA Library
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Medical Dictionary for Regulatory Activities - Controlled Terminology
          </p>
        </div>
        <div className="flex items-center gap-2">
          {status?.loading_in_progress && (
            <span className="flex items-center gap-2 text-sm text-[var(--warning)]">
              <RefreshCw className="w-4 h-4 animate-spin" />
              Loading MedDRA...
            </span>
          )}
          <button
            className="btn btn-secondary btn-sm"
            onClick={() => refetchStatus()}
          >
            <RefreshCw className="w-4 h-4" />
            Refresh
          </button>
        </div>
      </div>

      {/* Info Banner */}
      <div className="p-4 bg-[var(--primary)]/10 border border-[var(--primary)]/20 rounded-lg">
        <div className="flex items-start gap-3">
          <Info className="w-5 h-5 text-[var(--primary)] mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-[var(--foreground)]">MedDRA Hierarchy</p>
            <p className="text-[var(--muted)]">
              SOC (System Organ Class) → HLGT (High Level Group Term) → HLT (High Level Term) →
              PT (Preferred Term) → LLT (Lowest Level Term)
            </p>
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="flex border-b border-[var(--border)]">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`flex items-center gap-2 px-4 py-3 border-b-2 -mb-px transition-colors ${
              activeTab === tab.id
                ? "border-[var(--primary)] text-[var(--primary)]"
                : "border-transparent text-[var(--muted)] hover:text-[var(--foreground)]"
            }`}
            onClick={() => setActiveTab(tab.id)}
          >
            <tab.icon className="w-4 h-4" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      <div>
        {activeTab === "status" && <StatusTab status={status} loading={statusLoading} onRefresh={refetchStatus} />}
        {activeTab === "search" && <SearchTab available={status?.available || false} />}
        {activeTab === "browse" && <BrowseTab available={status?.available || false} />}
        {activeTab === "upload" && <UploadTab onSuccess={() => { refetchStatus(); queryClient.invalidateQueries({ queryKey: ["meddra"] }); }} />}
      </div>
    </div>
  );
}

// ============================================================================
// Status Tab
// ============================================================================

function StatusTab({ status, loading, onRefresh }: { status: any; loading: boolean; onRefresh: () => void }) {
  const queryClient = useQueryClient();

  const deleteMutation = useMutation({
    mutationFn: meddraApi.deleteVersion,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["meddra"] });
      onRefresh();
    },
  });

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="w-8 h-8 animate-spin text-[var(--primary)]" />
      </div>
    );
  }

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return "Never";
    return new Date(dateStr).toLocaleString();
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
      {/* Status Card */}
      <WPBox title="MedDRA Status">
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-[var(--muted)]">Available</span>
            {status?.available ? (
              <span className="flex items-center gap-1 text-[var(--success)]">
                <CheckCircle className="w-4 h-4" /> Yes
              </span>
            ) : (
              <span className="flex items-center gap-1 text-[var(--destructive)]">
                <XCircle className="w-4 h-4" /> No
              </span>
            )}
          </div>

          {status?.current_version ? (
            <>
              <div className="flex items-center justify-between">
                <span className="text-[var(--muted)]">Version</span>
                <span className="font-mono">{status.current_version.version}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-[var(--muted)]">Language</span>
                <span>{status.current_version.language || "English"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-[var(--muted)]">Loaded</span>
                <span className="flex items-center gap-1">
                  <Clock className="w-4 h-4" />
                  {formatDate(status.current_version.loaded_at)}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-[var(--muted)]">Total Terms</span>
                <span className="font-mono text-lg">
                  {status.current_version.total_terms?.toLocaleString() || 0}
                </span>
              </div>
            </>
          ) : (
            <div className="text-center py-4 text-[var(--muted)]">
              <Database className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p>No MedDRA version loaded</p>
              <p className="text-xs mt-1">Go to Upload tab to load MedDRA dictionary</p>
            </div>
          )}
        </div>
      </WPBox>

      {/* Term Counts */}
      {status?.current_version && (
        <WPBox title="Term Counts by Level">
          <div className="space-y-3">
            <div className="flex items-center justify-between p-3 bg-[var(--background-alt)] rounded">
              <span className="font-medium">SOC (System Organ Class)</span>
              <span className="font-mono">{status.current_version.soc_count?.toLocaleString() || 0}</span>
            </div>
            <div className="flex items-center justify-between p-3 bg-[var(--background-alt)] rounded">
              <span className="font-medium">HLGT (High Level Group Term)</span>
              <span className="font-mono">{status.current_version.hlgt_count?.toLocaleString() || 0}</span>
            </div>
            <div className="flex items-center justify-between p-3 bg-[var(--background-alt)] rounded">
              <span className="font-medium">HLT (High Level Term)</span>
              <span className="font-mono">{status.current_version.hlt_count?.toLocaleString() || 0}</span>
            </div>
            <div className="flex items-center justify-between p-3 bg-[var(--background-alt)] rounded">
              <span className="font-medium">PT (Preferred Term)</span>
              <span className="font-mono">{status.current_version.pt_count?.toLocaleString() || 0}</span>
            </div>
            <div className="flex items-center justify-between p-3 bg-[var(--background-alt)] rounded">
              <span className="font-medium">LLT (Lowest Level Term)</span>
              <span className="font-mono">{status.current_version.llt_count?.toLocaleString() || 0}</span>
            </div>
          </div>

          {status.current_version && (
            <div className="mt-4 pt-4 border-t border-[var(--border)]">
              <button
                className="btn btn-destructive btn-sm w-full"
                onClick={() => {
                  if (confirm("Are you sure you want to delete the current MedDRA version?")) {
                    deleteMutation.mutate();
                  }
                }}
                disabled={deleteMutation.isPending}
              >
                {deleteMutation.isPending ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <Trash2 className="w-4 h-4" />
                )}
                Delete Current Version
              </button>
            </div>
          )}
        </WPBox>
      )}
    </div>
  );
}

// ============================================================================
// Search Tab
// ============================================================================

function SearchTab({ available }: { available: boolean }) {
  const [query, setQuery] = useState("");
  const [levelFilter, setLevelFilter] = useState<string>("");
  const [results, setResults] = useState<MedDRASearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedTerm, setSelectedTerm] = useState<MedDRASearchResult | null>(null);

  const handleSearch = async () => {
    if (!query.trim()) return;

    setIsSearching(true);
    setError(null);
    setSelectedTerm(null);

    try {
      const response = await meddraApi.search(
        query.trim(),
        levelFilter as any || undefined,
        30
      );
      setResults(response.results);
    } catch (err: any) {
      setError(err.response?.data?.detail || "Search failed");
      setResults([]);
    } finally {
      setIsSearching(false);
    }
  };

  if (!available) {
    return (
      <WPBox>
        <div className="text-center py-8 text-[var(--muted)]">
          <AlertCircle className="w-12 h-12 mx-auto mb-4 opacity-50" />
          <p>MedDRA dictionary not loaded</p>
          <p className="text-sm mt-2">Go to Upload tab to load a MedDRA dictionary first</p>
        </div>
      </WPBox>
    );
  }

  return (
    <div className="space-y-5">
      {/* Search Form */}
      <WPBox title="Search MedDRA Terms">
        <div className="space-y-4">
          <div className="flex gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[var(--muted)]" />
                <input
                  type="text"
                  placeholder='Search terms (e.g., "headache", "MI", "cardiac")'
                  className="pl-10 w-full"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleSearch()}
                />
              </div>
            </div>
            <select
              value={levelFilter}
              onChange={(e) => setLevelFilter(e.target.value)}
              className="w-40"
            >
              <option value="">All Levels</option>
              <option value="SOC">SOC</option>
              <option value="HLGT">HLGT</option>
              <option value="HLT">HLT</option>
              <option value="PT">PT</option>
              <option value="LLT">LLT</option>
            </select>
            <button
              className="btn btn-primary btn-md"
              onClick={handleSearch}
              disabled={isSearching || !query.trim()}
            >
              {isSearching ? (
                <RefreshCw className="w-4 h-4 animate-spin" />
              ) : (
                <Search className="w-4 h-4" />
              )}
              Search
            </button>
          </div>

          {error && (
            <div className="p-3 bg-[var(--destructive)]/10 border border-[var(--destructive)]/20 rounded text-[var(--destructive)] text-sm">
              {error}
            </div>
          )}
        </div>
      </WPBox>

      {/* Results and Hierarchy */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        {/* Results List */}
        {results.length > 0 && (
          <WPBox title={`Results (${results.length})`}>
            <div className="space-y-2 max-h-[500px] overflow-y-auto">
              {results.map((result, idx) => (
                <button
                  key={`${result.term.code}-${idx}`}
                  className={`w-full text-left p-3 rounded transition-colors ${
                    selectedTerm?.term.code === result.term.code
                      ? "bg-[var(--primary)]/20 border border-[var(--primary)]"
                      : "bg-[var(--background-alt)] hover:bg-[var(--background-alt)]/80"
                  }`}
                  onClick={() => setSelectedTerm(result)}
                >
                  <div className="flex items-center justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium">{result.term.name}</span>
                        <span className={`text-xs px-2 py-0.5 rounded ${getLevelColor(result.term.level)}`}>
                          {result.term.level}
                        </span>
                      </div>
                      <div className="text-sm text-[var(--muted)] font-mono">
                        {result.term.code}
                      </div>
                    </div>
                    <ChevronRight className="w-4 h-4 text-[var(--muted)]" />
                  </div>
                </button>
              ))}
            </div>
          </WPBox>
        )}

        {/* Hierarchy View */}
        {selectedTerm && (
          <WPBox title="MedDRA Hierarchy">
            <HierarchyView hierarchy={selectedTerm.hierarchy} selectedCode={selectedTerm.term.code} />
          </WPBox>
        )}
      </div>

      {query && results.length === 0 && !isSearching && !error && (
        <WPBox>
          <div className="text-center py-8 text-[var(--muted)]">
            <Search className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p>No MedDRA terms found for "{query}"</p>
          </div>
        </WPBox>
      )}
    </div>
  );
}

// ============================================================================
// Browse Tab - Simplified Hierarchy: SOC → PT → LLT
// ============================================================================

function BrowseTab({ available }: { available: boolean }) {
  const [expandedSocs, setExpandedSocs] = useState<Set<string>>(new Set());
  const [expandedPts, setExpandedPts] = useState<Set<string>>(new Set());
  const [searchFilter, setSearchFilter] = useState("");

  const { data: socsData, isLoading } = useQuery({
    queryKey: ["meddra", "socs"],
    queryFn: () => meddraApi.getBySoc(),
    enabled: available,
  });

  if (!available) {
    return (
      <WPBox>
        <div className="text-center py-8 text-[var(--muted)]">
          <AlertCircle className="w-12 h-12 mx-auto mb-4 opacity-50" />
          <p>MedDRA dictionary not loaded</p>
          <p className="text-sm mt-2">Go to Upload tab to load a MedDRA dictionary first</p>
        </div>
      </WPBox>
    );
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="w-8 h-8 animate-spin text-[var(--primary)]" />
      </div>
    );
  }

  const toggleSoc = (code: string) => {
    const newExpanded = new Set(expandedSocs);
    if (newExpanded.has(code)) {
      newExpanded.delete(code);
    } else {
      newExpanded.add(code);
    }
    setExpandedSocs(newExpanded);
  };

  const togglePt = (code: string) => {
    const newExpanded = new Set(expandedPts);
    if (newExpanded.has(code)) {
      newExpanded.delete(code);
    } else {
      newExpanded.add(code);
    }
    setExpandedPts(newExpanded);
  };

  // Filter SOCs by search
  const filteredSocs = socsData?.socs?.filter(soc =>
    searchFilter === "" || soc.name.toLowerCase().includes(searchFilter.toLowerCase())
  ) || [];

  return (
    <div className="space-y-4">
      {/* Header with search */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="font-medium text-[var(--foreground)]">MedDRA Hierarchy Browser</h3>
          <p className="text-sm text-[var(--muted)]">SOC → PT → LLT (simplified view)</p>
        </div>
        <div className="relative w-64">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[var(--muted)]" />
          <input
            type="text"
            placeholder="Filter SOCs..."
            className="pl-10 w-full text-sm"
            value={searchFilter}
            onChange={(e) => setSearchFilter(e.target.value)}
          />
        </div>
      </div>

      {/* Tree View */}
      <div className="border border-[var(--border)] rounded-lg overflow-hidden">
        <div className="max-h-[600px] overflow-y-auto">
          {filteredSocs.map((soc) => (
            <SocTreeNode
              key={soc.code}
              soc={soc}
              isExpanded={expandedSocs.has(soc.code)}
              onToggle={() => toggleSoc(soc.code)}
              expandedPts={expandedPts}
              onTogglePt={togglePt}
            />
          ))}
          {filteredSocs.length === 0 && (
            <div className="text-center py-8 text-[var(--muted)]">
              <List className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>No SOCs found</p>
            </div>
          )}
        </div>
      </div>

      {/* Legend */}
      <div className="flex items-center gap-4 text-xs text-[var(--muted)]">
        <div className="flex items-center gap-1">
          <span className={`px-2 py-0.5 rounded ${getLevelColor("SOC")}`}>SOC</span>
          <span>System Organ Class</span>
        </div>
        <div className="flex items-center gap-1">
          <span className={`px-2 py-0.5 rounded ${getLevelColor("PT")}`}>PT</span>
          <span>Preferred Term</span>
        </div>
        <div className="flex items-center gap-1">
          <span className={`px-2 py-0.5 rounded ${getLevelColor("LLT")}`}>LLT</span>
          <span>Lowest Level Term</span>
        </div>
      </div>
    </div>
  );
}

// SOC Node Component
function SocTreeNode({
  soc,
  isExpanded,
  onToggle,
  expandedPts,
  onTogglePt
}: {
  soc: MedDRATerm;
  isExpanded: boolean;
  onToggle: () => void;
  expandedPts: Set<string>;
  onTogglePt: (code: string) => void;
}) {
  const { data: ptsData, isLoading } = useQuery({
    queryKey: ["meddra", "soc", soc.code, "pts"],
    queryFn: () => meddraApi.getPtsBySoc(soc.code),
    enabled: isExpanded,
  });

  return (
    <div className="border-b border-[var(--border)] last:border-b-0">
      {/* SOC Header */}
      <button
        className="w-full flex items-center gap-2 p-3 hover:bg-[var(--background-alt)] text-left transition-colors"
        onClick={onToggle}
      >
        <div className="w-5 h-5 flex items-center justify-center">
          {isExpanded ? (
            <ChevronDown className="w-4 h-4 text-[var(--primary)]" />
          ) : (
            <ChevronRight className="w-4 h-4" />
          )}
        </div>
        <span className={`text-xs px-2 py-0.5 rounded font-medium ${getLevelColor("SOC")}`}>SOC</span>
        <span className="font-medium flex-1">{soc.name}</span>
        {ptsData && (
          <span className="text-xs text-[var(--muted)] bg-[var(--background-alt)] px-2 py-0.5 rounded">
            {ptsData.pt_count} PTs
          </span>
        )}
        <span className="text-xs text-[var(--muted)] font-mono">{soc.code}</span>
      </button>

      {/* PTs List */}
      {isExpanded && (
        <div className="bg-[var(--background-alt)]/50">
          {isLoading ? (
            <div className="flex items-center gap-2 px-10 py-3 text-sm text-[var(--muted)]">
              <RefreshCw className="w-4 h-4 animate-spin" />
              Loading Preferred Terms...
            </div>
          ) : (
            ptsData?.pts?.map((pt) => (
              <PtTreeNode
                key={pt.code}
                pt={pt}
                isExpanded={expandedPts.has(pt.code)}
                onToggle={() => onTogglePt(pt.code)}
              />
            ))
          )}
          {ptsData?.pts?.length === 0 && (
            <div className="px-10 py-3 text-sm text-[var(--muted)]">
              No Preferred Terms found
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// PT Node Component
function PtTreeNode({
  pt,
  isExpanded,
  onToggle
}: {
  pt: MedDRATerm;
  isExpanded: boolean;
  onToggle: () => void;
}) {
  const { data: lltsData, isLoading } = useQuery({
    queryKey: ["meddra", "pt", pt.code, "llts"],
    queryFn: () => meddraApi.getLltsByPt(pt.code),
    enabled: isExpanded,
  });

  const hasLlts = lltsData?.llt_count && lltsData.llt_count > 0;

  return (
    <div>
      {/* PT Header */}
      <button
        className="w-full flex items-center gap-2 px-6 py-2 hover:bg-[var(--background-alt)] text-left transition-colors border-l-2 border-transparent hover:border-[var(--primary)]"
        onClick={onToggle}
      >
        <div className="w-5 h-5 flex items-center justify-center ml-4">
          {isExpanded ? (
            <ChevronDown className="w-3 h-3 text-[var(--success)]" />
          ) : (
            <ChevronRight className="w-3 h-3 text-[var(--muted)]" />
          )}
        </div>
        <span className={`text-xs px-2 py-0.5 rounded ${getLevelColor("PT")}`}>PT</span>
        <span className="text-sm flex-1">{pt.name}</span>
        {lltsData && hasLlts && (
          <span className="text-xs text-[var(--muted)]">
            {lltsData.llt_count} LLTs
          </span>
        )}
        <span className="text-xs text-[var(--muted)] font-mono">{pt.code}</span>
      </button>

      {/* LLTs List */}
      {isExpanded && (
        <div className="ml-16 border-l border-[var(--border)]">
          {isLoading ? (
            <div className="flex items-center gap-2 px-4 py-2 text-xs text-[var(--muted)]">
              <RefreshCw className="w-3 h-3 animate-spin" />
              Loading...
            </div>
          ) : (
            lltsData?.llts?.map((llt) => (
              <div
                key={llt.code}
                className="flex items-center gap-2 px-4 py-1.5 hover:bg-[var(--background)] text-xs"
              >
                <span className={`px-1.5 py-0.5 rounded ${getLevelColor("LLT")}`}>LLT</span>
                <span className="flex-1 text-[var(--muted)]">{llt.name}</span>
                <span className="text-[var(--muted)] font-mono">{llt.code}</span>
              </div>
            ))
          )}
          {lltsData?.llts?.length === 0 && (
            <div className="px-4 py-2 text-xs text-[var(--muted)] italic">
              No LLTs (PT is the lowest level)
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ============================================================================
// Upload Tab
// ============================================================================

function UploadTab({ onSuccess }: { onSuccess: () => void }) {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [dragActive, setDragActive] = useState(false);
  const [previewData, setPreviewData] = useState<MedDRAPreviewResponse | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const uploadMutation = useMutation({
    mutationFn: (file: File) => meddraApi.uploadFile(file, setUploadProgress),
    onSuccess: () => {
      setSelectedFile(null);
      setUploadProgress(0);
      setPreviewData(null);
      onSuccess();
    },
  });

  // Preview the file when selected
  const handlePreview = async (file: File) => {
    setPreviewLoading(true);
    setPreviewError(null);
    setPreviewData(null);
    try {
      const preview = await meddraApi.previewFile(file);
      setPreviewData(preview);
    } catch (err: any) {
      setPreviewError(err.response?.data?.detail || "Failed to preview file");
    } finally {
      setPreviewLoading(false);
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
      if (file.name.endsWith(".sas7bdat")) {
        setSelectedFile(file);
        handlePreview(file);
      }
    }
  }, []);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files[0]) {
      const file = e.target.files[0];
      setSelectedFile(file);
      handlePreview(file);
    }
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
      {/* Upload Area */}
      <WPBox title="Upload MedDRA Dictionary">
        <div className="space-y-4">
          <div
            className={`border-2 border-dashed rounded-lg p-8 text-center transition-colors ${
              dragActive
                ? "border-[var(--primary)] bg-[var(--primary)]/10"
                : "border-[var(--border)] hover:border-[var(--primary)]/50"
            }`}
            onDragEnter={handleDrag}
            onDragLeave={handleDrag}
            onDragOver={handleDrag}
            onDrop={handleDrop}
          >
            <input
              type="file"
              accept=".sas7bdat"
              onChange={handleFileSelect}
              className="hidden"
              id="meddra-file-input"
            />
            <label
              htmlFor="meddra-file-input"
              className="cursor-pointer flex flex-col items-center"
            >
              <FileUp className="w-12 h-12 text-[var(--muted)] mb-4" />
              <p className="text-[var(--foreground)] font-medium">
                Drop MedDRA SAS7BDAT file here
              </p>
              <p className="text-sm text-[var(--muted)] mt-1">
                or click to browse
              </p>
            </label>
          </div>

          {selectedFile && (
            <div className="p-4 bg-[var(--background-alt)] rounded-lg">
              <div className="flex items-center justify-between">
                <div>
                  <p className="font-medium">{selectedFile.name}</p>
                  <p className="text-sm text-[var(--muted)]">
                    {(selectedFile.size / 1024 / 1024).toFixed(2)} MB
                  </p>
                </div>
                <button
                  className="text-[var(--destructive)] hover:text-[var(--destructive)]/80"
                  onClick={() => { setSelectedFile(null); setPreviewData(null); setPreviewError(null); }}
                >
                  <XCircle className="w-5 h-5" />
                </button>
              </div>

              {/* Preview Loading */}
              {previewLoading && (
                <div className="mt-3 flex items-center gap-2 text-sm text-[var(--muted)]">
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  Analyzing file structure...
                </div>
              )}

              {/* Preview Error */}
              {previewError && (
                <div className="mt-3 p-3 bg-[var(--destructive)]/10 border border-[var(--destructive)]/20 rounded text-[var(--destructive)] text-sm">
                  {previewError}
                </div>
              )}

              {/* Preview Results */}
              {previewData && (
                <div className="mt-3 space-y-3">
                  <div className="flex items-center gap-2">
                    {previewData.can_load ? (
                      <CheckCircle className="w-5 h-5 text-[var(--success)]" />
                    ) : (
                      <AlertCircle className="w-5 h-5 text-[var(--destructive)]" />
                    )}
                    <span className={previewData.can_load ? "text-[var(--success)]" : "text-[var(--destructive)]"}>
                      {previewData.message}
                    </span>
                  </div>

                  <div className="text-sm">
                    <p className="text-[var(--muted)]">Rows: <span className="font-mono">{previewData.rows.toLocaleString()}</span></p>
                    <p className="text-[var(--muted)]">Columns found: <span className="font-mono">{previewData.columns.length}</span></p>
                  </div>

                  {/* Column Mappings */}
                  <div className="text-sm">
                    <p className="font-medium mb-2">Column Mappings:</p>
                    <div className="grid grid-cols-2 gap-2">
                      {Object.entries(previewData.column_mappings).map(([expected, found]) => (
                        <div key={expected} className="flex items-center gap-2">
                          {found ? (
                            <CheckCircle className="w-3 h-3 text-[var(--success)]" />
                          ) : (
                            <XCircle className="w-3 h-3 text-[var(--destructive)]" />
                          )}
                          <span className="font-mono text-xs">{expected}</span>
                          {found && <span className="text-[var(--muted)] text-xs">← {found}</span>}
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* All Columns in File */}
                  <details className="text-sm">
                    <summary className="cursor-pointer text-[var(--muted)] hover:text-[var(--foreground)]">
                      View all columns in file ({previewData.columns.length})
                    </summary>
                    <div className="mt-2 p-2 bg-[var(--background)] rounded font-mono text-xs max-h-32 overflow-y-auto">
                      {previewData.columns.join(", ")}
                    </div>
                  </details>
                </div>
              )}

              {uploadMutation.isPending && (
                <div className="mt-3">
                  <div className="h-2 bg-[var(--border)] rounded-full overflow-hidden">
                    <div
                      className="h-full bg-[var(--primary)] transition-all"
                      style={{ width: `${uploadProgress}%` }}
                    />
                  </div>
                  <p className="text-sm text-[var(--muted)] mt-1 text-center">
                    Uploading... {uploadProgress}%
                  </p>
                </div>
              )}
            </div>
          )}

          <button
            className="btn btn-primary btn-md w-full"
            onClick={() => selectedFile && uploadMutation.mutate(selectedFile)}
            disabled={!selectedFile || uploadMutation.isPending}
          >
            {uploadMutation.isPending ? (
              <>
                <RefreshCw className="w-4 h-4 animate-spin" />
                Processing...
              </>
            ) : (
              <>
                <Upload className="w-4 h-4" />
                Upload and Load MedDRA
              </>
            )}
          </button>

          {uploadMutation.error && (
            <div className="p-3 bg-[var(--destructive)]/10 border border-[var(--destructive)]/20 rounded text-[var(--destructive)] text-sm">
              {(uploadMutation.error as any)?.response?.data?.detail || "Upload failed"}
            </div>
          )}

          {uploadMutation.isSuccess && (
            <div className="p-3 bg-[var(--success)]/10 border border-[var(--success)]/20 rounded text-[var(--success)] text-sm">
              MedDRA dictionary loaded successfully!
            </div>
          )}
        </div>
      </WPBox>

      {/* Instructions */}
      <WPBox title="Instructions">
        <div className="space-y-4 text-sm">
          <div>
            <h4 className="font-medium mb-2">Supported Format</h4>
            <p className="text-[var(--muted)]">
              Upload a MedDRA dictionary in SAS7BDAT format. The file should contain
              the full hierarchy (mdhier) with all term levels.
            </p>
          </div>

          <div>
            <h4 className="font-medium mb-2">Expected Structure</h4>
            <ul className="list-disc list-inside text-[var(--muted)] space-y-1">
              <li>SOC_CODE, SOC_NAME - System Organ Class</li>
              <li>HLGT_CODE, HLGT_NAME - High Level Group Term</li>
              <li>HLT_CODE, HLT_NAME - High Level Term</li>
              <li>PT_CODE, PT_NAME - Preferred Term</li>
              <li>LLT_CODE, LLT_NAME - Lowest Level Term</li>
            </ul>
          </div>

          <div>
            <h4 className="font-medium mb-2">Version Management</h4>
            <p className="text-[var(--muted)]">
              Only one MedDRA version can be loaded at a time. Uploading a new file
              will replace the existing dictionary.
            </p>
          </div>

          <div className="p-3 bg-[var(--warning)]/10 border border-[var(--warning)]/20 rounded">
            <p className="text-[var(--warning)]">
              <strong>Note:</strong> Ensure your MedDRA version matches the version
              used to code your clinical data for accurate term matching.
            </p>
          </div>
        </div>
      </WPBox>
    </div>
  );
}

// ============================================================================
// Helper Components
// ============================================================================

function HierarchyView({ hierarchy, selectedCode }: { hierarchy: MedDRAHierarchy; selectedCode: string }) {
  const levels = [
    { key: "soc", label: "SOC", term: hierarchy.soc },
    { key: "hlgt", label: "HLGT", term: hierarchy.hlgt },
    { key: "hlt", label: "HLT", term: hierarchy.hlt },
    { key: "pt", label: "PT", term: hierarchy.pt },
    { key: "llt", label: "LLT", term: hierarchy.llt },
  ].filter((l) => l.term);

  return (
    <div className="space-y-2">
      {levels.map((level, idx) => (
        <div key={level.key} className="flex items-start gap-2">
          <div className="flex flex-col items-center">
            <div
              className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-medium ${
                level.term?.code === selectedCode
                  ? "bg-[var(--primary)] text-white"
                  : "bg-[var(--background-alt)]"
              }`}
            >
              {idx + 1}
            </div>
            {idx < levels.length - 1 && (
              <div className="w-px h-8 bg-[var(--border)]" />
            )}
          </div>
          <div className={`flex-1 p-3 rounded ${
            level.term?.code === selectedCode
              ? "bg-[var(--primary)]/10 border border-[var(--primary)]"
              : "bg-[var(--background-alt)]"
          }`}>
            <div className="flex items-center gap-2">
              <span className={`text-xs px-2 py-0.5 rounded ${getLevelColor(level.label as any)}`}>
                {level.label}
              </span>
              <span className="font-medium">{level.term?.name}</span>
            </div>
            <div className="text-sm text-[var(--muted)] font-mono mt-1">
              {level.term?.code}
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function getLevelColor(level: string): string {
  switch (level) {
    case "SOC":
      return "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300";
    case "HLGT":
      return "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300";
    case "HLT":
      return "bg-cyan-100 text-cyan-700 dark:bg-cyan-900/30 dark:text-cyan-300";
    case "PT":
      return "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300";
    case "LLT":
      return "bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-300";
    default:
      return "bg-gray-100 text-gray-700 dark:bg-gray-900/30 dark:text-gray-300";
  }
}
