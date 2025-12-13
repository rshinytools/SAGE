import { cn } from "@/lib/utils";

interface HealthCircleProps {
  percentage: number;
  size?: number;
  strokeWidth?: number;
  label?: string;
  status?: "healthy" | "degraded" | "unhealthy";
}

export function HealthCircle({
  percentage,
  size = 80,
  strokeWidth = 8,
  label,
  status,
}: HealthCircleProps) {
  const radius = (size - strokeWidth) / 2;
  const circumference = radius * 2 * Math.PI;
  const offset = circumference - (percentage / 100) * circumference;

  const getColor = () => {
    if (status) {
      switch (status) {
        case "healthy":
          return "var(--success)";
        case "degraded":
          return "var(--warning)";
        case "unhealthy":
          return "var(--destructive)";
      }
    }
    if (percentage >= 80) return "var(--success)";
    if (percentage >= 50) return "var(--warning)";
    return "var(--destructive)";
  };

  return (
    <div className="health-circle">
      <div className="relative" style={{ width: size, height: size }}>
        <svg width={size} height={size} className="transform -rotate-90">
          {/* Background circle */}
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke="var(--border)"
            strokeWidth={strokeWidth}
          />
          {/* Progress circle */}
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke={getColor()}
            strokeWidth={strokeWidth}
            strokeDasharray={circumference}
            strokeDashoffset={offset}
            strokeLinecap="round"
            className="transition-all duration-500"
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-lg font-bold text-[var(--foreground)]">
            {Math.round(percentage)}%
          </span>
        </div>
      </div>
      {label && (
        <div>
          <div
            className={cn(
              "text-sm font-medium capitalize",
              status === "healthy" && "text-[var(--success)]",
              status === "degraded" && "text-[var(--warning)]",
              status === "unhealthy" && "text-[var(--destructive)]"
            )}
          >
            {status || "Progress"}
          </div>
          <div className="text-xs text-[var(--foreground-muted)]">{label}</div>
        </div>
      )}
    </div>
  );
}
