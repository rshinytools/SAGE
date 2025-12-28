import { useState } from "react";
import { ThumbsUp, ThumbsDown, Edit3, Flag, Check, Loader2 } from "lucide-react";
import { feedbackApi } from "@/api/feedback";

interface FeedbackButtonsProps {
  messageId: string;
  question: string;
  sql?: string;
  onFeedbackSubmitted?: (type: string, success: boolean) => void;
}

type FeedbackState = "idle" | "submitting" | "confirmed" | "rejected" | "corrected";

export function FeedbackButtons({
  messageId,
  question,
  sql,
  onFeedbackSubmitted,
}: FeedbackButtonsProps) {
  const [state, setState] = useState<FeedbackState>("idle");
  const [showCorrectionModal, setShowCorrectionModal] = useState(false);
  const [showRejectModal, setShowRejectModal] = useState(false);
  const [correctedSql, setCorrectedSql] = useState(sql || "");
  const [correctionNotes, setCorrectionNotes] = useState("");
  const [rejectReason, setRejectReason] = useState("");
  const [error, setError] = useState<string | null>(null);

  const handleConfirm = async () => {
    if (!sql) return;
    setState("submitting");
    setError(null);

    try {
      await feedbackApi.confirmResponse(messageId, question, sql);
      setState("confirmed");
      onFeedbackSubmitted?.("confirm", true);
    } catch (err) {
      setError("Failed to submit feedback");
      setState("idle");
      onFeedbackSubmitted?.("confirm", false);
    }
  };

  const handleReject = async () => {
    if (!sql) return;
    setState("submitting");
    setError(null);

    try {
      await feedbackApi.rejectResponse(messageId, question, sql, rejectReason);
      setState("rejected");
      setShowRejectModal(false);
      onFeedbackSubmitted?.("reject", true);
    } catch (err) {
      setError("Failed to submit feedback");
      setState("idle");
      onFeedbackSubmitted?.("reject", false);
    }
  };

  const handleCorrect = async () => {
    if (!sql) return;
    setState("submitting");
    setError(null);

    try {
      await feedbackApi.correctResponse(
        messageId,
        question,
        sql,
        correctedSql,
        correctionNotes
      );
      setState("corrected");
      setShowCorrectionModal(false);
      onFeedbackSubmitted?.("correct", true);
    } catch (err) {
      setError("Failed to submit correction");
      setState("idle");
      onFeedbackSubmitted?.("correct", false);
    }
  };

  // Don't show feedback buttons if no SQL was generated
  if (!sql) return null;

  // Show success state
  if (state === "confirmed") {
    return (
      <div className="flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
        <Check className="w-3 h-3" />
        <span>Thanks for confirming!</span>
      </div>
    );
  }

  if (state === "rejected") {
    return (
      <div className="flex items-center gap-1 text-xs text-orange-600 dark:text-orange-400">
        <Flag className="w-3 h-3" />
        <span>Feedback recorded</span>
      </div>
    );
  }

  if (state === "corrected") {
    return (
      <div className="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400">
        <Edit3 className="w-3 h-3" />
        <span>Correction submitted</span>
      </div>
    );
  }

  return (
    <>
      <div className="flex items-center gap-1">
        {/* Confirm Button */}
        <button
          onClick={handleConfirm}
          disabled={state === "submitting"}
          className="inline-flex items-center gap-1 px-2 py-1 text-xs text-gray-500 dark:text-gray-400 hover:text-green-600 dark:hover:text-green-400 hover:bg-green-50 dark:hover:bg-green-900/20 rounded transition-colors disabled:opacity-50"
          title="This response is correct"
        >
          {state === "submitting" ? (
            <Loader2 className="w-3 h-3 animate-spin" />
          ) : (
            <ThumbsUp className="w-3 h-3" />
          )}
        </button>

        {/* Reject Button */}
        <button
          onClick={() => setShowRejectModal(true)}
          disabled={state === "submitting"}
          className="inline-flex items-center gap-1 px-2 py-1 text-xs text-gray-500 dark:text-gray-400 hover:text-red-600 dark:hover:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded transition-colors disabled:opacity-50"
          title="This response is incorrect"
        >
          <ThumbsDown className="w-3 h-3" />
        </button>

        {/* Correct Button */}
        <button
          onClick={() => setShowCorrectionModal(true)}
          disabled={state === "submitting"}
          className="inline-flex items-center gap-1 px-2 py-1 text-xs text-gray-500 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400 hover:bg-blue-50 dark:hover:bg-blue-900/20 rounded transition-colors disabled:opacity-50"
          title="Provide correct SQL"
        >
          <Edit3 className="w-3 h-3" />
          <span>Correct</span>
        </button>

        {error && (
          <span className="text-xs text-red-500 ml-2">{error}</span>
        )}
      </div>

      {/* Rejection Modal */}
      {showRejectModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-md w-full mx-4 p-6">
            <h3 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">
              Report Incorrect Response
            </h3>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
              Help us improve by describing what was wrong with this response.
            </p>
            <textarea
              value={rejectReason}
              onChange={(e) => setRejectReason(e.target.value)}
              placeholder="What was incorrect? (optional)"
              className="w-full h-24 p-3 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <div className="flex justify-end gap-2 mt-4">
              <button
                onClick={() => setShowRejectModal(false)}
                className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleReject}
                disabled={state === "submitting"}
                className="px-4 py-2 text-sm bg-red-600 hover:bg-red-700 text-white rounded-lg transition-colors disabled:opacity-50 flex items-center gap-2"
              >
                {state === "submitting" && <Loader2 className="w-4 h-4 animate-spin" />}
                Submit
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Correction Modal */}
      {showCorrectionModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow-xl max-w-2xl w-full mx-4 p-6">
            <h3 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">
              Provide Correct SQL
            </h3>
            <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
              Your correction will help improve future responses.
            </p>

            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Correct SQL Query
              </label>
              <textarea
                value={correctedSql}
                onChange={(e) => setCorrectedSql(e.target.value)}
                placeholder="Enter the correct SQL query..."
                className="w-full h-32 p-3 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 font-mono text-sm resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Notes (optional)
              </label>
              <textarea
                value={correctionNotes}
                onChange={(e) => setCorrectionNotes(e.target.value)}
                placeholder="Explain what was wrong and why this is correct..."
                className="w-full h-20 p-3 border border-gray-200 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 text-sm resize-none focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
            </div>

            <div className="flex justify-end gap-2">
              <button
                onClick={() => setShowCorrectionModal(false)}
                className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleCorrect}
                disabled={state === "submitting" || !correctedSql.trim()}
                className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors disabled:opacity-50 flex items-center gap-2"
              >
                {state === "submitting" && <Loader2 className="w-4 h-4 animate-spin" />}
                Submit Correction
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
