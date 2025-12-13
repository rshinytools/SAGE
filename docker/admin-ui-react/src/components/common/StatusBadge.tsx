import { cn } from "@/lib/utils";

type BadgeVariant = "primary" | "success" | "warning" | "destructive" | "default" | "info";

interface StatusBadgeProps {
  children: React.ReactNode;
  variant?: BadgeVariant;
  className?: string;
  size?: "sm" | "md";
}

const variantClasses: Record<BadgeVariant, string> = {
  primary: "badge-primary",
  success: "badge-success",
  warning: "badge-warning",
  destructive: "badge-error",
  info: "badge-primary",
  default: "badge-neutral",
};

export function StatusBadge({
  children,
  variant = "default",
  className,
  size = "md",
}: StatusBadgeProps) {
  return (
    <span
      className={cn(
        "badge",
        variantClasses[variant],
        size === "sm" && "text-[10px] px-2 py-0.5",
        className
      )}
    >
      {children}
    </span>
  );
}

// Convenience components for common status values
export function StatusPending() {
  return <StatusBadge variant="warning">Pending</StatusBadge>;
}

export function StatusInProgress() {
  return <StatusBadge variant="primary">In Progress</StatusBadge>;
}

export function StatusCompleted() {
  return <StatusBadge variant="success">Completed</StatusBadge>;
}

export function StatusBlocked() {
  return <StatusBadge variant="destructive">Blocked</StatusBadge>;
}

export function StatusHealthy() {
  return <StatusBadge variant="success">Healthy</StatusBadge>;
}

export function StatusUnhealthy() {
  return <StatusBadge variant="destructive">Unhealthy</StatusBadge>;
}

export function StatusDegraded() {
  return <StatusBadge variant="warning">Degraded</StatusBadge>;
}
