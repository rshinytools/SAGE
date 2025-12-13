import { cn } from "@/lib/utils";

interface GlanceItemProps {
  icon: React.ReactNode;
  label: string;
  value: string | number;
  color?: "primary" | "success" | "warning" | "destructive";
  onClick?: () => void;
}

const colorMap = {
  primary: "bg-[var(--primary)]",
  success: "bg-[var(--success)]",
  warning: "bg-[var(--warning)]",
  destructive: "bg-[var(--destructive)]",
};

export function GlanceItem({
  icon,
  label,
  value,
  color = "primary",
  onClick,
}: GlanceItemProps) {
  const Wrapper = onClick ? "button" : "div";

  return (
    <Wrapper
      className={cn("glance-item", onClick && "cursor-pointer hover:opacity-80")}
      onClick={onClick}
    >
      <div className={cn("glance-icon", colorMap[color])}>{icon}</div>
      <div>
        <div className="text-2xl font-bold text-[var(--foreground)]">{value}</div>
        <div className="text-sm text-[var(--foreground-muted)]">{label}</div>
      </div>
    </Wrapper>
  );
}
