import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import {
  Search,
  RefreshCw,
  Database,
  BookOpen,
  Zap,
  Table,
  CheckCircle,
  XCircle,
  Clock,
  Play,
  ChevronDown,
  ChevronRight,
  FileText,
  Layers,
  AlertCircle,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { dictionaryApi } from "@/api/dictionary";
import type { SearchResult, DictionaryStatus } from "@/api/dictionary";

type TabType = "status" | "search" | "values" | "schema" | "rebuild";

export function DictionaryManagerPage() {
  const [activeTab, setActiveTab] = useState<TabType>("status");

  const { data: status, isLoading: statusLoading, refetch: refetchStatus } = useQuery({
    queryKey: ["dictionaryStatus"],
    queryFn: dictionaryApi.getStatus,
    refetchInterval: (query) => query.state.data?.build_in_progress ? 2000 : false,
  });

  const tabs = [
    { id: "status" as TabType, label: "Status", icon: Database },
    { id: "search" as TabType, label: "Search", icon: Search },
    { id: "values" as TabType, label: "Values", icon: BookOpen },
    { id: "schema" as TabType, label: "Schema Map", icon: Layers },
    { id: "rebuild" as TabType, label: "Rebuild", icon: RefreshCw },
  ];

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Dictionary Manager
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Factory 3: Fuzzy matching indexes for clinical data values
          </p>
        </div>
        <div className="flex items-center gap-2">
          {status?.build_in_progress && (
            <span className="flex items-center gap-2 text-sm text-[var(--warning)]">
              <RefreshCw className="w-4 h-4 animate-spin" />
              Building... {status.build_progress}%
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
          <AlertCircle className="w-5 h-5 text-[var(--primary)] mt-0.5" />
          <div className="text-sm">
            <p className="font-medium text-[var(--foreground)]">Clinical Data Integrity</p>
            <p className="text-[var(--muted)]">
              This dictionary uses fuzzy matching for typo correction only. Synonym matching
              is handled via MedDRA hierarchy (see MedDRA Library) to ensure controlled vocabulary compliance.
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
        {activeTab === "status" && <StatusTab status={status} loading={statusLoading} />}
        {activeTab === "search" && <SearchTab />}
        {activeTab === "values" && <ValuesTab />}
        {activeTab === "schema" && <SchemaTab />}
        {activeTab === "rebuild" && <RebuildTab status={status} onSuccess={() => refetchStatus()} />}
      </div>
    </div>
  );
}

// ============================================================================
// Status Tab
// ============================================================================

function StatusTab({ status, loading }: { status?: DictionaryStatus; loading: boolean }) {
  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="w-8 h-8 animate-spin text-[var(--primary)]" />
      </div>
    );
  }

  if (!status) {
    return (
      <WPBox>
        <div className="text-center py-8 text-[var(--muted)]">
          <XCircle className="w-12 h-12 mx-auto mb-4" />
          <p>Unable to load dictionary status</p>
        </div>
      </WPBox>
    );
  }

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return "Never";
    return new Date(dateStr).toLocaleString();
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
      {/* Overall Status */}
      <WPBox title="Dictionary Status">
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <span className="text-[var(--muted)]">Available</span>
            {status.available ? (
              <span className="flex items-center gap-1 text-[var(--success)]">
                <CheckCircle className="w-4 h-4" /> Yes
              </span>
            ) : (
              <span className="flex items-center gap-1 text-[var(--destructive)]">
                <XCircle className="w-4 h-4" /> No
              </span>
            )}
          </div>
          <div className="flex items-center justify-between">
            <span className="text-[var(--muted)]">Last Build</span>
            <span className="flex items-center gap-1">
              <Clock className="w-4 h-4" />
              {formatDate(status.last_build)}
            </span>
          </div>
          {status.build_duration_seconds && (
            <div className="flex items-center justify-between">
              <span className="text-[var(--muted)]">Build Duration</span>
              <span>{status.build_duration_seconds.toFixed(1)}s</span>
            </div>
          )}
          <div className="flex items-center justify-between">
            <span className="text-[var(--muted)]">Build Status</span>
            {status.build_in_progress ? (
              <span className="flex items-center gap-1 text-[var(--warning)]">
                <RefreshCw className="w-4 h-4 animate-spin" /> In Progress
              </span>
            ) : (
              <span className="text-[var(--success)]">Ready</span>
            )}
          </div>
        </div>
      </WPBox>

      {/* Fuzzy Index */}
      <WPBox title="Fuzzy Index">
        {status.fuzzy_index ? (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-[var(--muted)]">Status</span>
              {status.fuzzy_index.loaded ? (
                <span className="flex items-center gap-1 text-[var(--success)]">
                  <CheckCircle className="w-4 h-4" /> Loaded
                </span>
              ) : (
                <span className="text-[var(--muted)]">Available</span>
              )}
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[var(--muted)]">Index Entries</span>
              <span className="font-mono text-lg">{status.fuzzy_index.entries?.toLocaleString() || 0}</span>
            </div>
            <div className="text-xs text-[var(--muted)] truncate">
              {status.fuzzy_index.path}
            </div>
          </div>
        ) : (
          <div className="text-center py-4 text-[var(--muted)]">
            <Zap className="w-8 h-8 mx-auto mb-2 opacity-50" />
            <p>Not built yet</p>
            <p className="text-xs mt-1">Go to Rebuild tab to build index</p>
          </div>
        )}
      </WPBox>

      {/* Schema Map */}
      <WPBox title="Schema Map">
        {status.schema_map ? (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <span className="text-[var(--muted)]">Tables</span>
              <span className="font-mono text-lg">{status.schema_map.tables}</span>
            </div>
            <div className="text-xs text-[var(--muted)] truncate">
              {status.schema_map.path}
            </div>
          </div>
        ) : (
          <div className="text-center py-4 text-[var(--muted)]">
            <Table className="w-8 h-8 mx-auto mb-2 opacity-50" />
            <p>Not built yet</p>
            <p className="text-xs mt-1">Go to Rebuild tab to build index</p>
          </div>
        )}
      </WPBox>
    </div>
  );
}

// ============================================================================
// Search Tab
// ============================================================================

function SearchTab() {
  const [query, setQuery] = useState("");
  const [threshold, setThreshold] = useState(70);
  const [results, setResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async () => {
    if (!query.trim()) return;

    setIsSearching(true);
    setError(null);

    try {
      const response = await dictionaryApi.search({
        query: query.trim(),
        threshold,
        limit: 20,
      });
      setResults(response.results);
    } catch (err: any) {
      setError(err.response?.data?.detail || "Search failed");
      setResults([]);
    } finally {
      setIsSearching(false);
    }
  };

  const getScoreColor = (score: number) => {
    if (score >= 90) return "text-[var(--success)]";
    if (score >= 70) return "text-[var(--warning)]";
    return "text-[var(--muted)]";
  };

  return (
    <div className="space-y-5">
      {/* Search Form */}
      <WPBox title="Search Clinical Values (Fuzzy Matching)">
        <div className="space-y-4">
          <p className="text-sm text-[var(--muted)]">
            Search for clinical values with typo correction. This helps identify the correct
            terms when users make spelling mistakes (e.g., "Tyleonl" finds "TYLENOL").
          </p>
          <div className="flex gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[var(--muted)]" />
                <input
                  type="text"
                  placeholder='Try "Tyleonl" or "HEADAHCE" or "hypertention"...'
                  className="pl-10 w-full"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleSearch()}
                />
              </div>
            </div>
            <div className="flex items-center gap-2">
              <label className="text-sm text-[var(--muted)]">Min Score:</label>
              <input
                type="number"
                value={threshold}
                onChange={(e) => setThreshold(Number(e.target.value))}
                className="w-20"
                min={0}
                max={100}
              />
            </div>
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

      {/* Results */}
      {results.length > 0 && (
        <WPBox title={`Results (${results.length})`}>
          <div className="space-y-2">
            {results.map((result, idx) => (
              <div
                key={`${result.id}-${idx}`}
                className="flex items-center justify-between p-3 bg-[var(--background-alt)] rounded"
              >
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="font-medium">{result.value}</span>
                    <span className="text-xs px-2 py-0.5 rounded bg-[var(--primary)]/10 text-[var(--primary)]">
                      {result.match_type}
                    </span>
                  </div>
                  <div className="text-sm text-[var(--muted)]">
                    {result.table}.{result.column}
                  </div>
                </div>
                <div className={`font-mono text-lg ${getScoreColor(result.score)}`}>
                  {result.score.toFixed(1)}%
                </div>
              </div>
            ))}
          </div>
        </WPBox>
      )}

      {query && results.length === 0 && !isSearching && !error && (
        <WPBox>
          <div className="text-center py-8 text-[var(--muted)]">
            <Search className="w-12 h-12 mx-auto mb-4 opacity-50" />
            <p>No matches found for "{query}"</p>
            <p className="text-sm mt-2">Try lowering the minimum score threshold</p>
          </div>
        </WPBox>
      )}
    </div>
  );
}

// ============================================================================
// Values Tab
// ============================================================================

function ValuesTab() {
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [selectedColumn, setSelectedColumn] = useState<string | null>(null);

  const { data: tables, isLoading: tablesLoading } = useQuery({
    queryKey: ["dictionaryTables"],
    queryFn: dictionaryApi.getTables,
  });

  const { data: values, isLoading: valuesLoading } = useQuery({
    queryKey: ["dictionaryValues", selectedTable, selectedColumn],
    queryFn: () => dictionaryApi.getColumnValues(selectedTable!, selectedColumn!),
    enabled: !!selectedTable && !!selectedColumn,
  });

  if (tablesLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="w-8 h-8 animate-spin text-[var(--primary)]" />
      </div>
    );
  }

  if (!tables || tables.count === 0) {
    return (
      <WPBox>
        <div className="text-center py-8 text-[var(--muted)]">
          <BookOpen className="w-12 h-12 mx-auto mb-4 opacity-50" />
          <p>No indexed tables found. Run dictionary build first.</p>
        </div>
      </WPBox>
    );
  }

  return (
    <div className="grid grid-cols-4 gap-5">
      {/* Table/Column List */}
      <div className="col-span-1">
        <WPBox title="Tables">
          <div className="space-y-1 max-h-[600px] overflow-y-auto">
            {Object.entries(tables.tables).map(([tableName, tableInfo]) => (
              <div key={tableName}>
                <button
                  className={`w-full flex items-center justify-between px-3 py-2 rounded text-left hover:bg-[var(--background-alt)] ${
                    selectedTable === tableName ? "bg-[var(--primary)]/10 text-[var(--primary)]" : ""
                  }`}
                  onClick={() => {
                    setSelectedTable(selectedTable === tableName ? null : tableName);
                    setSelectedColumn(null);
                  }}
                >
                  <span className="flex items-center gap-2">
                    {selectedTable === tableName ? (
                      <ChevronDown className="w-4 h-4" />
                    ) : (
                      <ChevronRight className="w-4 h-4" />
                    )}
                    {tableName}
                  </span>
                  <span className="text-xs text-[var(--muted)]">
                    {tableInfo.value_count}
                  </span>
                </button>
                {selectedTable === tableName && (
                  <div className="ml-6 space-y-1 mt-1">
                    {tableInfo.columns.map((col) => (
                      <button
                        key={col}
                        className={`w-full px-3 py-1.5 rounded text-left text-sm hover:bg-[var(--background-alt)] ${
                          selectedColumn === col ? "bg-[var(--primary)]/10 text-[var(--primary)]" : "text-[var(--muted)]"
                        }`}
                        onClick={() => setSelectedColumn(col)}
                      >
                        {col}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </WPBox>
      </div>

      {/* Values List */}
      <div className="col-span-3">
        {selectedTable && selectedColumn ? (
          <WPBox title={`${selectedTable}.${selectedColumn}`}>
            {valuesLoading ? (
              <div className="flex items-center justify-center py-8">
                <RefreshCw className="w-6 h-6 animate-spin text-[var(--primary)]" />
              </div>
            ) : values?.values?.length ? (
              <div>
                <div className="text-sm text-[var(--muted)] mb-3">
                  Showing {values.values.length} of {values.total} values
                </div>
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-2">
                  {values.values.map((val, idx) => (
                    <div
                      key={idx}
                      className="px-3 py-2 bg-[var(--background-alt)] rounded text-sm truncate"
                      title={val}
                    >
                      {val}
                    </div>
                  ))}
                </div>
              </div>
            ) : (
              <div className="text-center py-8 text-[var(--muted)]">
                <FileText className="w-8 h-8 mx-auto mb-2 opacity-50" />
                <p>No values found</p>
              </div>
            )}
          </WPBox>
        ) : (
          <WPBox>
            <div className="text-center py-12 text-[var(--muted)]">
              <BookOpen className="w-12 h-12 mx-auto mb-4 opacity-50" />
              <p>Select a table and column to view values</p>
            </div>
          </WPBox>
        )}
      </div>
    </div>
  );
}

// ============================================================================
// Schema Tab
// ============================================================================

function SchemaTab() {
  const [searchTerm, setSearchTerm] = useState("");

  const { data: schemaMap, isLoading } = useQuery({
    queryKey: ["schemaMap"],
    queryFn: dictionaryApi.getSchemaMap,
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <RefreshCw className="w-8 h-8 animate-spin text-[var(--primary)]" />
      </div>
    );
  }

  if (!schemaMap) {
    return (
      <WPBox>
        <div className="text-center py-8 text-[var(--muted)]">
          <Layers className="w-12 h-12 mx-auto mb-4 opacity-50" />
          <p>Schema map not found. Run dictionary build first.</p>
        </div>
      </WPBox>
    );
  }

  const filteredColumns = Object.entries(schemaMap.columns).filter(([name]) =>
    name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-5">
      {/* Search */}
      <WPBox>
        <div className="relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-[var(--muted)]" />
          <input
            type="text"
            placeholder="Search columns..."
            className="pl-10 w-full"
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
          />
        </div>
      </WPBox>

      {/* Info */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <WPBox className="text-center">
          <div className="text-3xl font-bold text-[var(--primary)]">
            {Object.keys(schemaMap.tables).length}
          </div>
          <div className="text-sm text-[var(--muted)]">Tables</div>
        </WPBox>
        <WPBox className="text-center">
          <div className="text-3xl font-bold text-[var(--primary)]">
            {Object.keys(schemaMap.columns).length}
          </div>
          <div className="text-sm text-[var(--muted)]">Columns</div>
        </WPBox>
        <WPBox className="text-center">
          <div className="text-sm text-[var(--muted)]">Generated</div>
          <div className="text-sm">
            {schemaMap.generated_at ? new Date(schemaMap.generated_at).toLocaleString() : "Unknown"}
          </div>
        </WPBox>
      </div>

      {/* Columns Table */}
      <WPBox title={`Columns (${filteredColumns.length})`}>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3">Column</th>
                <th className="text-left py-2 px-3">Tables</th>
                <th className="text-left py-2 px-3">Type</th>
                <th className="text-left py-2 px-3">Key</th>
                <th className="text-left py-2 px-3">Description</th>
                <th className="text-right py-2 px-3">Unique Values</th>
              </tr>
            </thead>
            <tbody>
              {filteredColumns.slice(0, 50).map(([name, col]) => (
                <tr key={name} className="border-b border-[var(--border)]/50 hover:bg-[var(--background-alt)]">
                  <td className="py-2 px-3 font-medium">{name}</td>
                  <td className="py-2 px-3">
                    <div className="flex flex-wrap gap-1">
                      {col.tables.slice(0, 3).map((t) => (
                        <span key={t} className="text-xs px-2 py-0.5 rounded bg-[var(--primary)]/10">
                          {t}
                        </span>
                      ))}
                      {col.tables.length > 3 && (
                        <span className="text-xs text-[var(--muted)]">+{col.tables.length - 3}</span>
                      )}
                    </div>
                  </td>
                  <td className="py-2 px-3 font-mono text-xs">{col.type}</td>
                  <td className="py-2 px-3">
                    {col.is_key && <span className="text-[var(--success)]">Yes</span>}
                  </td>
                  <td className="py-2 px-3 text-[var(--muted)] max-w-xs truncate">
                    {col.description || "-"}
                  </td>
                  <td className="py-2 px-3 text-right font-mono">
                    {col.unique_values_count.toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {filteredColumns.length > 50 && (
            <div className="text-center py-3 text-sm text-[var(--muted)]">
              Showing 50 of {filteredColumns.length} columns
            </div>
          )}
        </div>
      </WPBox>
    </div>
  );
}

// ============================================================================
// Rebuild Tab
// ============================================================================

function RebuildTab({ status, onSuccess }: { status?: DictionaryStatus; onSuccess: () => void }) {
  const [options, setOptions] = useState({
    rebuild: false,
  });

  const buildMutation = useMutation({
    mutationFn: dictionaryApi.triggerBuild,
    onSuccess: () => {
      onSuccess();
    },
  });

  const isBuilding = buildMutation.isPending || status?.build_in_progress;
  const progress = status?.build_progress || 0;
  const step = status?.build_step || "";

  // Helper to get step status
  const getStepStatus = (stepMin: number, stepMax: number) => {
    if (!isBuilding) return "pending";
    if (progress >= stepMax) return "complete";
    if (progress >= stepMin) return "active";
    return "pending";
  };

  const stepStyles = {
    pending: "bg-[var(--background-alt)] text-[var(--muted)]",
    active: "bg-[var(--primary)] text-white animate-pulse",
    complete: "bg-[var(--success)] text-white",
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-5">
      {/* Build Options & Progress */}
      <WPBox title="Build Dictionary">
        <div className="space-y-4">
          {/* Progress Bar (shown during build) */}
          {isBuilding && (
            <div className="space-y-2">
              <div className="flex justify-between items-center text-sm">
                <span className="text-[var(--foreground)]">Building...</span>
                <span className="font-mono text-[var(--primary)]">{progress}%</span>
              </div>
              <div className="w-full h-3 bg-[var(--background-alt)] rounded-full overflow-hidden">
                <div
                  className="h-full bg-[var(--primary)] rounded-full transition-all duration-300 ease-out"
                  style={{ width: `${progress}%` }}
                />
              </div>
              <div className="text-sm text-[var(--muted)] truncate">
                {step}
              </div>
            </div>
          )}

          {/* Options (shown when not building) */}
          {!isBuilding && (
            <label className="flex items-center gap-3 cursor-pointer">
              <input
                type="checkbox"
                checked={options.rebuild}
                onChange={(e) => setOptions({ ...options, rebuild: e.target.checked })}
                className="w-4 h-4"
              />
              <div>
                <div className="font-medium">Full Rebuild</div>
                <div className="text-sm text-[var(--muted)]">
                  Clear existing indexes and rebuild from scratch
                </div>
              </div>
            </label>
          )}

          <div className="pt-4 border-t border-[var(--border)]">
            <button
              className="btn btn-primary btn-md w-full"
              onClick={() => buildMutation.mutate(options)}
              disabled={isBuilding}
            >
              {isBuilding ? (
                <>
                  <RefreshCw className="w-4 h-4 animate-spin" />
                  Building... {progress}%
                </>
              ) : (
                <>
                  <Play className="w-4 h-4" />
                  Start Build
                </>
              )}
            </button>
          </div>

          {buildMutation.error && (
            <div className="p-3 bg-[var(--destructive)]/10 border border-[var(--destructive)]/20 rounded text-[var(--destructive)] text-sm">
              {(buildMutation.error as any)?.response?.data?.detail || "Build failed"}
            </div>
          )}

          {/* Last Build Info */}
          {!isBuilding && status?.last_build && (
            <div className="pt-4 border-t border-[var(--border)] text-sm text-[var(--muted)]">
              <div className="flex justify-between">
                <span>Last build:</span>
                <span>{new Date(status.last_build).toLocaleString()}</span>
              </div>
              {status.build_duration_seconds && (
                <div className="flex justify-between mt-1">
                  <span>Duration:</span>
                  <span>{status.build_duration_seconds.toFixed(2)}s</span>
                </div>
              )}
            </div>
          )}
        </div>
      </WPBox>

      {/* Build Process Steps */}
      <WPBox title="Build Process">
        <div className="space-y-4">
          <div className="space-y-3">
            {/* Step 1: Scan Values (0-40%) */}
            <div className="flex items-start gap-3">
              <div className={`w-6 h-6 rounded-full flex items-center justify-center text-sm ${stepStyles[getStepStatus(0, 40)]}`}>
                {getStepStatus(0, 40) === "complete" ? <CheckCircle className="w-4 h-4" /> : "1"}
              </div>
              <div>
                <div className="font-medium">Scan Values</div>
                <div className="text-sm text-[var(--muted)]">
                  Extract unique values from DuckDB tables (AE, CM, LB, VS, etc.)
                </div>
              </div>
            </div>

            {/* Step 2: Build Fuzzy Index (40-70%) */}
            <div className="flex items-start gap-3">
              <div className={`w-6 h-6 rounded-full flex items-center justify-center text-sm ${stepStyles[getStepStatus(40, 70)]}`}>
                {getStepStatus(40, 70) === "complete" ? <CheckCircle className="w-4 h-4" /> : "2"}
              </div>
              <div>
                <div className="font-medium">Build Fuzzy Index</div>
                <div className="text-sm text-[var(--muted)]">
                  Create RapidFuzz index for typo correction and partial matching
                </div>
              </div>
            </div>

            {/* Step 3: Build Schema Map (70-100%) */}
            <div className="flex items-start gap-3">
              <div className={`w-6 h-6 rounded-full flex items-center justify-center text-sm ${stepStyles[getStepStatus(70, 100)]}`}>
                {getStepStatus(70, 100) === "complete" ? <CheckCircle className="w-4 h-4" /> : "3"}
              </div>
              <div>
                <div className="font-medium">Build Schema Map</div>
                <div className="text-sm text-[var(--muted)]">
                  Generate column to table lookups for SQL generation
                </div>
              </div>
            </div>
          </div>

          <div className="pt-4 border-t border-[var(--border)] text-sm text-[var(--muted)]">
            <p>
              <strong>Prerequisites:</strong> Factory 1 must be complete (clinical.duckdb must exist).
            </p>
            <p className="mt-2">
              <strong>Note:</strong> Synonym/term mapping is handled via MedDRA Library,
              not AI-based semantic matching, to ensure controlled vocabulary compliance.
            </p>
          </div>
        </div>
      </WPBox>
    </div>
  );
}
