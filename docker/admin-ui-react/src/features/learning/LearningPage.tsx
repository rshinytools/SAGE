import { useState, useEffect } from "react";
import {
  Brain,
  CheckCircle,
  XCircle,
  Trash2,
  RefreshCw,
  Search,
  ChevronDown,
  ChevronUp,
  AlertCircle,
  ThumbsUp,
  ThumbsDown,
  Edit3,
  Clock,
  Database,
  TrendingUp,
  Award,
} from "lucide-react";
import { feedbackApi, type LearningExample, type PendingReview, type FeedbackStats, type ExampleStats } from "@/api/feedback";
import { CodeBlock } from "@/components/chat/CodeBlock";

type TabType = "pending" | "examples" | "stats";

export function LearningPage() {
  const [activeTab, setActiveTab] = useState<TabType>("pending");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Pending reviews state
  const [pendingReviews, setPendingReviews] = useState<PendingReview[]>([]);
  const [expandedReview, setExpandedReview] = useState<string | null>(null);

  // Examples state
  const [examples, setExamples] = useState<LearningExample[]>([]);
  const [verifiedOnly, setVerifiedOnly] = useState(false);
  const [categoryFilter] = useState<string>("");
  const [searchQuery, setSearchQuery] = useState("");
  const [expandedExample, setExpandedExample] = useState<string | null>(null);

  // Stats state
  const [feedbackStats, setFeedbackStats] = useState<FeedbackStats | null>(null);
  const [exampleStats, setExampleStats] = useState<ExampleStats | null>(null);

  // Load data based on active tab
  useEffect(() => {
    loadData();
  }, [activeTab, verifiedOnly, categoryFilter]);

  const loadData = async () => {
    setLoading(true);
    setError(null);

    try {
      if (activeTab === "pending") {
        const data = await feedbackApi.getPendingReviews();
        setPendingReviews(data);
      } else if (activeTab === "examples") {
        const data = await feedbackApi.getExamples(verifiedOnly, categoryFilter || undefined);
        setExamples(data);
      } else if (activeTab === "stats") {
        const [feedback, example] = await Promise.all([
          feedbackApi.getStats(),
          feedbackApi.getExampleStats(),
        ]);
        setFeedbackStats(feedback);
        setExampleStats(example);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  };

  const handleApproveFeedback = async (id: string) => {
    try {
      await feedbackApi.approveFeedback(id);
      setPendingReviews(prev => prev.filter(r => r.id !== id));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to approve feedback");
    }
  };

  const handleRejectFeedback = async (id: string) => {
    try {
      await feedbackApi.rejectFeedback(id);
      setPendingReviews(prev => prev.filter(r => r.id !== id));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to reject feedback");
    }
  };

  const handleVerifyExample = async (id: string) => {
    try {
      await feedbackApi.verifyExample(id);
      setExamples(prev =>
        prev.map(e => (e.id === id ? { ...e, verified: true } : e))
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to verify example");
    }
  };

  const handleDeleteExample = async (id: string) => {
    if (!confirm("Are you sure you want to delete this example?")) return;
    try {
      await feedbackApi.deleteExample(id);
      setExamples(prev => prev.filter(e => e.id !== id));
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete example");
    }
  };

  const filteredExamples = examples.filter(e =>
    searchQuery
      ? e.question.toLowerCase().includes(searchQuery.toLowerCase()) ||
        e.sql.toLowerCase().includes(searchQuery.toLowerCase())
      : true
  );

  const getFeedbackTypeIcon = (type: string) => {
    switch (type.toUpperCase()) {
      case "CONFIRM":
        return <ThumbsUp className="w-4 h-4 text-green-500" />;
      case "REJECT":
        return <ThumbsDown className="w-4 h-4 text-red-500" />;
      case "CORRECT":
        return <Edit3 className="w-4 h-4 text-blue-500" />;
      default:
        return <AlertCircle className="w-4 h-4 text-gray-500" />;
    }
  };

  return (
    <div className="p-6 max-w-7xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-purple-100 dark:bg-purple-900/30 rounded-lg flex items-center justify-center">
            <Brain className="w-5 h-5 text-purple-600 dark:text-purple-400" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">
              AI Learning System
            </h1>
            <p className="text-sm text-gray-500 dark:text-gray-400">
              Review feedback and manage learning examples
            </p>
          </div>
        </div>
        <button
          onClick={loadData}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 dark:bg-gray-800 hover:bg-gray-200 dark:hover:bg-gray-700 rounded-lg transition-colors"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? "animate-spin" : ""}`} />
          Refresh
        </button>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 mb-6 border-b border-gray-200 dark:border-gray-700">
        <button
          onClick={() => setActiveTab("pending")}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "pending"
              ? "border-purple-500 text-purple-600 dark:text-purple-400"
              : "border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"
          }`}
        >
          Pending Reviews
          {pendingReviews.length > 0 && (
            <span className="ml-2 px-2 py-0.5 bg-purple-100 dark:bg-purple-900/30 text-purple-600 dark:text-purple-400 rounded-full text-xs">
              {pendingReviews.length}
            </span>
          )}
        </button>
        <button
          onClick={() => setActiveTab("examples")}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "examples"
              ? "border-purple-500 text-purple-600 dark:text-purple-400"
              : "border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"
          }`}
        >
          Learning Examples
        </button>
        <button
          onClick={() => setActiveTab("stats")}
          className={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${
            activeTab === "stats"
              ? "border-purple-500 text-purple-600 dark:text-purple-400"
              : "border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300"
          }`}
        >
          Statistics
        </button>
      </div>

      {/* Error Display */}
      {error && (
        <div className="mb-4 p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-lg flex items-center gap-2 text-red-600 dark:text-red-400">
          <AlertCircle className="w-5 h-5" />
          {error}
        </div>
      )}

      {/* Tab Content */}
      {activeTab === "pending" && (
        <div className="space-y-4">
          {loading ? (
            <div className="text-center py-12 text-gray-500">Loading...</div>
          ) : pendingReviews.length === 0 ? (
            <div className="text-center py-12">
              <CheckCircle className="w-12 h-12 mx-auto text-green-500 mb-4" />
              <p className="text-gray-500 dark:text-gray-400">
                No pending reviews
              </p>
            </div>
          ) : (
            pendingReviews.map((review) => (
              <div
                key={review.id}
                className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden"
              >
                <div
                  className="p-4 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50"
                  onClick={() =>
                    setExpandedReview(
                      expandedReview === review.id ? null : review.id
                    )
                  }
                >
                  <div className="flex items-start justify-between">
                    <div className="flex items-start gap-3">
                      {getFeedbackTypeIcon(review.feedback_type)}
                      <div>
                        <p className="font-medium text-gray-900 dark:text-gray-100">
                          {review.question}
                        </p>
                        <p className="text-sm text-gray-500 dark:text-gray-400">
                          {review.feedback_type} by {review.submitted_by}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-gray-400">
                        {new Date(review.submitted_at).toLocaleDateString()}
                      </span>
                      {expandedReview === review.id ? (
                        <ChevronUp className="w-4 h-4" />
                      ) : (
                        <ChevronDown className="w-4 h-4" />
                      )}
                    </div>
                  </div>
                </div>

                {expandedReview === review.id && (
                  <div className="px-4 pb-4 border-t border-gray-100 dark:border-gray-700">
                    <div className="mt-4 space-y-4">
                      <div>
                        <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                          Original SQL
                        </label>
                        <CodeBlock code={review.generated_sql} language="sql" />
                      </div>

                      {review.corrected_sql && (
                        <div>
                          <label className="text-xs font-medium text-green-600 dark:text-green-400">
                            Corrected SQL
                          </label>
                          <CodeBlock code={review.corrected_sql} language="sql" />
                        </div>
                      )}

                      {review.correction_notes && (
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                            Notes
                          </label>
                          <p className="text-sm text-gray-700 dark:text-gray-300 mt-1">
                            {review.correction_notes}
                          </p>
                        </div>
                      )}

                      {review.issue_description && (
                        <div>
                          <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                            Issue Description
                          </label>
                          <p className="text-sm text-gray-700 dark:text-gray-300 mt-1">
                            {review.issue_description}
                          </p>
                        </div>
                      )}

                      <div className="flex gap-2 pt-2">
                        <button
                          onClick={() => handleApproveFeedback(review.id)}
                          className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg transition-colors"
                        >
                          <CheckCircle className="w-4 h-4" />
                          Approve & Create Example
                        </button>
                        <button
                          onClick={() => handleRejectFeedback(review.id)}
                          className="flex items-center gap-2 px-4 py-2 bg-gray-100 dark:bg-gray-700 hover:bg-gray-200 dark:hover:bg-gray-600 text-gray-700 dark:text-gray-300 rounded-lg transition-colors"
                        >
                          <XCircle className="w-4 h-4" />
                          Reject
                        </button>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      )}

      {activeTab === "examples" && (
        <div className="space-y-4">
          {/* Filters */}
          <div className="flex gap-4 flex-wrap">
            <div className="flex-1 min-w-[200px]">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search examples..."
                  className="w-full pl-10 pr-4 py-2 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-purple-500"
                />
              </div>
            </div>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={verifiedOnly}
                onChange={(e) => setVerifiedOnly(e.target.checked)}
                className="rounded border-gray-300 text-purple-600 focus:ring-purple-500"
              />
              <span className="text-sm text-gray-600 dark:text-gray-400">
                Verified only
              </span>
            </label>
          </div>

          {/* Examples List */}
          {loading ? (
            <div className="text-center py-12 text-gray-500">Loading...</div>
          ) : filteredExamples.length === 0 ? (
            <div className="text-center py-12">
              <Database className="w-12 h-12 mx-auto text-gray-300 dark:text-gray-600 mb-4" />
              <p className="text-gray-500 dark:text-gray-400">
                No examples found
              </p>
            </div>
          ) : (
            <div className="space-y-3">
              {filteredExamples.map((example) => (
                <div
                  key={example.id}
                  className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden"
                >
                  <div
                    className="p-4 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-700/50"
                    onClick={() =>
                      setExpandedExample(
                        expandedExample === example.id ? null : example.id
                      )
                    }
                  >
                    <div className="flex items-start justify-between">
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-1">
                          {example.verified ? (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400 rounded-full text-xs">
                              <CheckCircle className="w-3 h-3" />
                              Verified
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-yellow-100 dark:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400 rounded-full text-xs">
                              <Clock className="w-3 h-3" />
                              Pending
                            </span>
                          )}
                          <span className="text-xs text-gray-400">
                            {example.category}
                          </span>
                        </div>
                        <p className="font-medium text-gray-900 dark:text-gray-100">
                          {example.question}
                        </p>
                        <div className="flex items-center gap-4 mt-2 text-xs text-gray-500 dark:text-gray-400">
                          <span>Confidence: {example.confidence.toFixed(0)}%</span>
                          <span>Used: {example.usage_count} times</span>
                          <span>Success: {(example.success_rate * 100).toFixed(0)}%</span>
                        </div>
                      </div>
                      {expandedExample === example.id ? (
                        <ChevronUp className="w-4 h-4" />
                      ) : (
                        <ChevronDown className="w-4 h-4" />
                      )}
                    </div>
                  </div>

                  {expandedExample === example.id && (
                    <div className="px-4 pb-4 border-t border-gray-100 dark:border-gray-700">
                      <div className="mt-4">
                        <label className="text-xs font-medium text-gray-500 dark:text-gray-400">
                          SQL Query
                        </label>
                        <CodeBlock code={example.sql} language="sql" />
                      </div>

                      <div className="flex gap-2 mt-4">
                        {!example.verified && (
                          <button
                            onClick={() => handleVerifyExample(example.id)}
                            className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg transition-colors"
                          >
                            <CheckCircle className="w-4 h-4" />
                            Verify
                          </button>
                        )}
                        <button
                          onClick={() => handleDeleteExample(example.id)}
                          className="flex items-center gap-2 px-4 py-2 bg-red-100 dark:bg-red-900/20 hover:bg-red-200 dark:hover:bg-red-900/40 text-red-600 dark:text-red-400 rounded-lg transition-colors"
                        >
                          <Trash2 className="w-4 h-4" />
                          Delete
                        </button>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {activeTab === "stats" && (
        <div className="space-y-6">
          {loading ? (
            <div className="text-center py-12 text-gray-500">Loading...</div>
          ) : (
            <>
              {/* Feedback Stats */}
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
                  Feedback Statistics
                </h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-gray-500 dark:text-gray-400 mb-1">
                      <TrendingUp className="w-4 h-4" />
                      <span className="text-sm">Total Feedback</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {feedbackStats?.total_feedback || 0}
                    </p>
                  </div>
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-green-500 mb-1">
                      <ThumbsUp className="w-4 h-4" />
                      <span className="text-sm">Confirmations</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {feedbackStats?.confirmations || 0}
                    </p>
                  </div>
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-blue-500 mb-1">
                      <Edit3 className="w-4 h-4" />
                      <span className="text-sm">Corrections</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {feedbackStats?.corrections || 0}
                    </p>
                  </div>
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-red-500 mb-1">
                      <ThumbsDown className="w-4 h-4" />
                      <span className="text-sm">Rejections</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {feedbackStats?.rejections || 0}
                    </p>
                  </div>
                </div>
              </div>

              {/* Example Stats */}
              <div>
                <h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100 mb-4">
                  Learning Examples
                </h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-gray-500 dark:text-gray-400 mb-1">
                      <Database className="w-4 h-4" />
                      <span className="text-sm">Total Examples</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {exampleStats?.total_examples || 0}
                    </p>
                  </div>
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-green-500 mb-1">
                      <Award className="w-4 h-4" />
                      <span className="text-sm">Verified</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {exampleStats?.verified_examples || 0}
                    </p>
                  </div>
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-yellow-500 mb-1">
                      <Clock className="w-4 h-4" />
                      <span className="text-sm">Pending</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {exampleStats?.unverified_examples || 0}
                    </p>
                  </div>
                  <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
                    <div className="flex items-center gap-2 text-purple-500 mb-1">
                      <TrendingUp className="w-4 h-4" />
                      <span className="text-sm">Avg Confidence</span>
                    </div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-gray-100">
                      {exampleStats?.average_confidence?.toFixed(0) || 0}%
                    </p>
                  </div>
                </div>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
