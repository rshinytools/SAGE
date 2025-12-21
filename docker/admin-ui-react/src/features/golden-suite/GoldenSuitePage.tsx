import { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import {
  Play,
  Download,
  Trash2,
  CheckCircle,
  XCircle,
  RefreshCw,
  FlaskConical,
  AlertCircle,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { goldenSuiteApi, type TestCategory, type TestRunSummary, type TestResult } from "@/api/goldenSuite";
import type { ColumnDef } from "@tanstack/react-table";

export function GoldenSuitePage() {
  const queryClient = useQueryClient();
  const [selectedCategories, setSelectedCategories] = useState<string[]>([]);
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [viewingRun, setViewingRun] = useState<TestRunSummary | null>(null);
  const [downloadCategory, setDownloadCategory] = useState<string>("");

  // Fetch categories
  const { data: categoriesData, isLoading: loadingCategories } = useQuery({
    queryKey: ["goldenCategories"],
    queryFn: goldenSuiteApi.getCategories,
  });

  // Fetch runs
  const { data: runsData, isLoading: loadingRuns } = useQuery({
    queryKey: ["goldenRuns"],
    queryFn: goldenSuiteApi.getRuns,
    refetchInterval: activeRunId ? 2000 : false, // Poll when a run is active
  });

  // Fetch active run details
  const { data: activeRun } = useQuery({
    queryKey: ["goldenRun", activeRunId],
    queryFn: () => (activeRunId ? goldenSuiteApi.getRun(activeRunId) : null),
    enabled: !!activeRunId,
    refetchInterval: activeRunId ? 2000 : false,
  });

  // Start run mutation
  const startRunMutation = useMutation({
    mutationFn: goldenSuiteApi.startRun,
    onSuccess: (data) => {
      setActiveRunId(data.run_id);
      queryClient.invalidateQueries({ queryKey: ["goldenRuns"] });
    },
  });

  // Delete run mutation
  const deleteRunMutation = useMutation({
    mutationFn: goldenSuiteApi.deleteRun,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["goldenRuns"] });
      if (viewingRun) setViewingRun(null);
    },
  });

  // Update activeRunId when run completes
  useEffect(() => {
    if (activeRun?.status === "completed" || activeRun?.status === "failed") {
      setActiveRunId(null);
      queryClient.invalidateQueries({ queryKey: ["goldenRuns"] });
    }
  }, [activeRun?.status, queryClient]);

  // Handle category toggle
  const toggleCategory = (category: string) => {
    setSelectedCategories((prev) =>
      prev.includes(category)
        ? prev.filter((c) => c !== category)
        : [...prev, category]
    );
  };

  // Start test run
  const handleStartRun = () => {
    startRunMutation.mutate({
      categories: selectedCategories.length > 0 ? selectedCategories : undefined,
      include_flows: true,
    });
  };

  // Download results
  const handleDownload = async (runId: string, format: "json" | "csv" | "html") => {
    try {
      const blob = await goldenSuiteApi.downloadRun(runId, format, downloadCategory || undefined);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `golden_test_${runId}.${format}`;
      a.click();
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error("Download failed:", error);
    }
  };

  // View run details
  const handleViewRun = async (runId: string) => {
    const run = await goldenSuiteApi.getRun(runId);
    setViewingRun(run);
  };

  // Results table columns
  const resultColumns: ColumnDef<TestResult>[] = [
    {
      accessorKey: "question_id",
      header: "ID",
      size: 60,
    },
    {
      accessorKey: "question",
      header: "Question",
      cell: ({ row }) => (
        <span title={row.original.question}>
          {row.original.question.slice(0, 60)}
          {row.original.question.length > 60 ? "..." : ""}
        </span>
      ),
    },
    {
      accessorKey: "category",
      header: "Category",
      cell: ({ row }) => (
        <StatusBadge variant="info">{row.original.category}</StatusBadge>
      ),
    },
    {
      accessorKey: "expected",
      header: "Expected",
      size: 100,
      cell: ({ row }) => (
        <span className="font-mono text-sm">
          {typeof row.original.expected === "object"
            ? "Distribution"
            : row.original.expected}
        </span>
      ),
    },
    {
      accessorKey: "actual",
      header: "Actual",
      size: 100,
      cell: ({ row }) => (
        <span className="font-mono text-sm">
          {row.original.actual ?? "N/A"}
        </span>
      ),
    },
    {
      accessorKey: "confidence",
      header: "Confidence",
      size: 100,
      cell: ({ row }) => {
        const score = row.original.confidence?.score;
        if (!score) return "-";
        return (
          <span
            className={`font-medium ${
              score >= 90
                ? "text-green-600"
                : score >= 70
                ? "text-yellow-600"
                : "text-red-600"
            }`}
          >
            {score}%
          </span>
        );
      },
    },
    {
      accessorKey: "match",
      header: "Result",
      size: 80,
      cell: ({ row }) => {
        const match = row.original.match;
        if (match === null) {
          return (
            <StatusBadge variant="warning">
              <AlertCircle className="w-3 h-3 mr-1" />
              Manual
            </StatusBadge>
          );
        }
        return match ? (
          <StatusBadge variant="success">
            <CheckCircle className="w-3 h-3 mr-1" />
            Pass
          </StatusBadge>
        ) : (
          <StatusBadge variant="destructive">
            <XCircle className="w-3 h-3 mr-1" />
            Fail
          </StatusBadge>
        );
      },
    },
  ];

  // Runs table columns
  const runColumns: ColumnDef<TestRunSummary>[] = [
    {
      accessorKey: "run_id",
      header: "Run ID",
      size: 100,
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => {
        const status = row.original.status;
        const variant =
          status === "completed"
            ? "success"
            : status === "running"
            ? "primary"
            : status === "failed"
            ? "destructive"
            : "default";
        return (
          <StatusBadge variant={variant}>
            {status === "running" && (
              <RefreshCw className="w-3 h-3 mr-1 animate-spin" />
            )}
            {status}
          </StatusBadge>
        );
      },
    },
    {
      accessorKey: "completed_questions",
      header: "Progress",
      cell: ({ row }) => (
        <span>
          {row.original.completed_questions}/{row.original.total_questions}
        </span>
      ),
    },
    {
      accessorKey: "accuracy",
      header: "Accuracy",
      cell: ({ row }) => {
        const acc = row.original.accuracy;
        return (
          <span
            className={`font-bold ${
              acc >= 80 ? "text-green-600" : acc >= 60 ? "text-yellow-600" : "text-red-600"
            }`}
          >
            {acc.toFixed(1)}%
          </span>
        );
      },
    },
    {
      accessorKey: "started_at",
      header: "Started",
      cell: ({ row }) => new Date(row.original.started_at).toLocaleString(),
    },
    {
      id: "actions",
      header: "Actions",
      cell: ({ row }) => (
        <div className="flex gap-2">
          <button
            className="btn btn-xs btn-secondary"
            onClick={() => handleViewRun(row.original.run_id)}
            disabled={row.original.status === "running"}
          >
            View
          </button>
          <button
            className="btn btn-xs btn-secondary"
            onClick={() => handleDownload(row.original.run_id, "html")}
            disabled={row.original.status !== "completed"}
          >
            <Download className="w-3 h-3" />
          </button>
          <button
            className="btn btn-xs btn-destructive"
            onClick={() => deleteRunMutation.mutate(row.original.run_id)}
          >
            <Trash2 className="w-3 h-3" />
          </button>
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Golden Test Suite
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Validate SAGE accuracy with standardized test questions
          </p>
        </div>
      </div>

      {/* Active Run Progress */}
      {activeRun && activeRun.status === "running" && (
        <WPBox title="Running Test Suite">
          <div className="space-y-4">
            <div className="flex items-center gap-4">
              <RefreshCw className="w-6 h-6 text-[var(--primary)] animate-spin" />
              <div className="flex-1">
                <div className="flex justify-between mb-1">
                  <span className="font-medium">
                    Progress: {activeRun.completed_questions} / {activeRun.total_questions}
                  </span>
                  <span>{Math.round((activeRun.completed_questions / activeRun.total_questions) * 100)}%</span>
                </div>
                <div className="w-full h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                  <div
                    className="h-full bg-[var(--primary)] transition-all duration-300"
                    style={{
                      width: `${(activeRun.completed_questions / activeRun.total_questions) * 100}%`,
                    }}
                  />
                </div>
              </div>
            </div>
            <div className="grid grid-cols-4 gap-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-green-600">{activeRun.matches}</div>
                <div className="text-sm text-[var(--muted)]">Passed</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-red-600">{activeRun.mismatches}</div>
                <div className="text-sm text-[var(--muted)]">Failed</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-yellow-600">{activeRun.manual_check}</div>
                <div className="text-sm text-[var(--muted)]">Manual</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold">{activeRun.accuracy.toFixed(1)}%</div>
                <div className="text-sm text-[var(--muted)]">Accuracy</div>
              </div>
            </div>
          </div>
        </WPBox>
      )}

      {/* Category Selection */}
      <WPBox title="Test Categories">
        {loadingCategories ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              {categoriesData?.categories.map((cat: TestCategory) => (
                <label
                  key={cat.name}
                  className={`flex items-center gap-3 p-3 rounded-lg border cursor-pointer transition-colors ${
                    selectedCategories.includes(cat.name)
                      ? "border-[var(--primary)] bg-[var(--primary)]/10"
                      : "border-gray-200 dark:border-gray-700 hover:border-gray-300 dark:hover:border-gray-600"
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={selectedCategories.includes(cat.name)}
                    onChange={() => toggleCategory(cat.name)}
                    className="w-4 h-4"
                  />
                  <div className="flex-1">
                    <div className="font-medium">{cat.name}</div>
                    <div className="text-sm text-[var(--muted)]">{cat.count} questions</div>
                  </div>
                </label>
              ))}
            </div>
            <div className="flex items-center justify-between pt-4 border-t border-gray-200 dark:border-gray-700">
              <div className="text-sm text-[var(--muted)]">
                {selectedCategories.length === 0
                  ? `All ${categoriesData?.total_questions || 0} questions will be tested`
                  : `${categoriesData?.categories
                      .filter((c: TestCategory) => selectedCategories.includes(c.name))
                      .reduce((sum: number, c: TestCategory) => sum + c.count, 0)} questions selected`}
              </div>
              <button
                className="btn btn-primary btn-md"
                onClick={handleStartRun}
                disabled={startRunMutation.isPending || !!activeRunId}
              >
                {startRunMutation.isPending ? (
                  <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                ) : (
                  <Play className="w-4 h-4 mr-2" />
                )}
                {activeRunId ? "Test Running..." : "Start Test Run"}
              </button>
            </div>
          </div>
        )}
      </WPBox>

      {/* Viewing Run Results */}
      {viewingRun && (
        <WPBox
          title={`Results: ${viewingRun.run_id}`}
          headerAction={
            <button
              className="btn btn-secondary btn-sm"
              onClick={() => setViewingRun(null)}
            >
              Close
            </button>
          }
        >
          {/* Summary */}
          <div className="grid grid-cols-5 gap-4 mb-6">
            <div className="text-center p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
              <div className="text-2xl font-bold">{viewingRun.total_questions}</div>
              <div className="text-sm text-[var(--muted)]">Total</div>
            </div>
            <div className="text-center p-4 bg-green-50 dark:bg-green-900/20 rounded-lg">
              <div className="text-2xl font-bold text-green-600">{viewingRun.matches}</div>
              <div className="text-sm text-[var(--muted)]">Passed</div>
            </div>
            <div className="text-center p-4 bg-red-50 dark:bg-red-900/20 rounded-lg">
              <div className="text-2xl font-bold text-red-600">{viewingRun.mismatches}</div>
              <div className="text-sm text-[var(--muted)]">Failed</div>
            </div>
            <div className="text-center p-4 bg-yellow-50 dark:bg-yellow-900/20 rounded-lg">
              <div className="text-2xl font-bold text-yellow-600">{viewingRun.manual_check}</div>
              <div className="text-sm text-[var(--muted)]">Manual</div>
            </div>
            <div className="text-center p-4 bg-blue-50 dark:bg-blue-900/20 rounded-lg">
              <div
                className={`text-2xl font-bold ${
                  viewingRun.accuracy >= 80
                    ? "text-green-600"
                    : viewingRun.accuracy >= 60
                    ? "text-yellow-600"
                    : "text-red-600"
                }`}
              >
                {viewingRun.accuracy.toFixed(1)}%
              </div>
              <div className="text-sm text-[var(--muted)]">Accuracy</div>
            </div>
          </div>

          {/* By Category */}
          {Object.keys(viewingRun.by_category).length > 0 && (
            <div className="mb-6">
              <h4 className="font-medium mb-3">By Category</h4>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                {Object.entries(viewingRun.by_category).map(([cat, stats]) => (
                  <div
                    key={cat}
                    className="p-3 border border-gray-200 dark:border-gray-700 rounded-lg"
                  >
                    <div className="font-medium text-sm">{cat}</div>
                    <div className="flex items-center justify-between mt-1">
                      <span className="text-xs text-[var(--muted)]">
                        {stats.match}/{stats.total}
                      </span>
                      <span
                        className={`font-bold ${
                          stats.accuracy >= 80
                            ? "text-green-600"
                            : stats.accuracy >= 60
                            ? "text-yellow-600"
                            : "text-red-600"
                        }`}
                      >
                        {stats.accuracy.toFixed(0)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Download Options */}
          <div className="flex items-center gap-4 mb-4 p-4 bg-gray-50 dark:bg-gray-800 rounded-lg">
            <span className="text-sm font-medium">Download:</span>
            <select
              className="text-sm border rounded px-2 py-1"
              value={downloadCategory}
              onChange={(e) => setDownloadCategory(e.target.value)}
            >
              <option value="">All Categories</option>
              {Object.keys(viewingRun.by_category).map((cat) => (
                <option key={cat} value={cat}>
                  {cat}
                </option>
              ))}
            </select>
            <button
              className="btn btn-secondary btn-sm"
              onClick={() => handleDownload(viewingRun.run_id, "csv")}
            >
              <Download className="w-3 h-3 mr-1" />
              CSV
            </button>
            <button
              className="btn btn-secondary btn-sm"
              onClick={() => handleDownload(viewingRun.run_id, "html")}
            >
              <Download className="w-3 h-3 mr-1" />
              HTML Report
            </button>
            <button
              className="btn btn-secondary btn-sm"
              onClick={() => handleDownload(viewingRun.run_id, "json")}
            >
              <Download className="w-3 h-3 mr-1" />
              JSON
            </button>
          </div>

          {/* Results Table */}
          {viewingRun.results && viewingRun.results.length > 0 && (
            <DataTable
              columns={resultColumns}
              data={
                downloadCategory
                  ? viewingRun.results.filter((r) => r.category === downloadCategory)
                  : viewingRun.results
              }
              pageSize={20}
            />
          )}
        </WPBox>
      )}

      {/* Previous Runs */}
      <WPBox title="Test Run History">
        {loadingRuns ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
          </div>
        ) : runsData?.runs?.length ? (
          <DataTable columns={runColumns} data={runsData.runs} pageSize={10} />
        ) : (
          <div className="text-center py-8 text-[var(--muted)]">
            <FlaskConical className="w-12 h-12 mx-auto mb-4" />
            <p>No test runs yet</p>
            <p className="text-sm">Start a test run to validate SAGE accuracy</p>
          </div>
        )}
      </WPBox>
    </div>
  );
}
