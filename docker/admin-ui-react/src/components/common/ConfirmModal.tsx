import { AlertTriangle, X, CheckCircle2 } from "lucide-react";

interface ConfirmModalProps {
  isOpen: boolean;
  onClose: () => void;
  onConfirm: () => void;
  title: string;
  message: string;
  confirmText?: string;
  cancelText?: string;
  variant?: "danger" | "warning" | "info";
  isLoading?: boolean;
  progress?: {
    current: number;
    total: number;
    message?: string;
  };
}

export function ConfirmModal({
  isOpen,
  onClose,
  onConfirm,
  title,
  message,
  confirmText = "Confirm",
  cancelText = "Cancel",
  variant = "warning",
  isLoading = false,
  progress,
}: ConfirmModalProps) {
  if (!isOpen) return null;

  const variantStyles = {
    danger: {
      icon: "text-red-500",
      button: "bg-red-600 hover:bg-red-700 text-white",
      iconBg: "bg-red-100 dark:bg-red-900/30",
      progressBar: "bg-red-500",
    },
    warning: {
      icon: "text-yellow-500",
      button: "bg-yellow-600 hover:bg-yellow-700 text-white",
      iconBg: "bg-yellow-100 dark:bg-yellow-900/30",
      progressBar: "bg-yellow-500",
    },
    info: {
      icon: "text-blue-500",
      button: "bg-blue-600 hover:bg-blue-700 text-white",
      iconBg: "bg-blue-100 dark:bg-blue-900/30",
      progressBar: "bg-blue-500",
    },
  };

  const styles = variantStyles[variant];
  const isProcessing = isLoading && progress && progress.total > 0;
  const isComplete = progress && progress.current === progress.total && progress.total > 0;
  const progressPercent = progress && progress.total > 0 ? (progress.current / progress.total) * 100 : 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50 backdrop-blur-sm"
        onClick={isProcessing ? undefined : onClose}
      />

      {/* Modal */}
      <div className="relative bg-[var(--card)] rounded-lg shadow-xl max-w-md w-full mx-4 animate-in zoom-in-95 fade-in duration-200">
        {/* Close button */}
        {!isProcessing && (
          <button
            onClick={onClose}
            className="absolute top-4 right-4 p-1 text-[var(--muted)] hover:text-[var(--foreground)] transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        )}

        <div className="p-6">
          {/* Icon and Title */}
          <div className="flex items-start gap-4">
            <div className={`p-3 rounded-full ${isComplete ? "bg-green-100 dark:bg-green-900/30" : styles.iconBg}`}>
              {isComplete ? (
                <CheckCircle2 className="w-6 h-6 text-green-500" />
              ) : (
                <AlertTriangle className={`w-6 h-6 ${styles.icon}`} />
              )}
            </div>
            <div className="flex-1">
              <h3 className="text-lg font-semibold text-[var(--foreground)]">
                {isComplete ? "Complete!" : title}
              </h3>
              <p className="mt-2 text-sm text-[var(--muted)]">
                {isComplete ? `Successfully approved ${progress?.current} variables.` : message}
              </p>
            </div>
          </div>

          {/* Progress Bar */}
          {isProcessing && !isComplete && (
            <div className="mt-6">
              <div className="flex justify-between text-sm mb-2">
                <span className="text-[var(--muted)]">
                  {progress?.message || "Processing..."}
                </span>
                <span className="text-[var(--foreground)] font-medium">
                  {progress?.current} / {progress?.total}
                </span>
              </div>
              <div className="w-full bg-[var(--muted)]/20 rounded-full h-2.5 overflow-hidden">
                <div
                  className={`h-full rounded-full transition-all duration-300 ${styles.progressBar}`}
                  style={{ width: `${progressPercent}%` }}
                />
              </div>
              <p className="mt-2 text-xs text-[var(--muted)] text-center">
                Please wait, do not close this window...
              </p>
            </div>
          )}

          {/* Buttons */}
          <div className="mt-6 flex justify-end gap-3">
            {isComplete ? (
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm font-medium text-white bg-green-600 hover:bg-green-700 rounded-lg transition-colors"
              >
                Done
              </button>
            ) : isProcessing ? null : (
              <>
                <button
                  onClick={onClose}
                  disabled={isLoading}
                  className="px-4 py-2 text-sm font-medium text-[var(--foreground)] bg-[var(--muted)]/20 hover:bg-[var(--muted)]/30 rounded-lg transition-colors disabled:opacity-50"
                >
                  {cancelText}
                </button>
                <button
                  onClick={onConfirm}
                  disabled={isLoading}
                  className={`px-4 py-2 text-sm font-medium rounded-lg transition-colors disabled:opacity-50 flex items-center gap-2 ${styles.button}`}
                >
                  {isLoading && (
                    <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                  )}
                  {confirmText}
                </button>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
