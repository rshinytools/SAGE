import { cn } from "@/lib/utils";

interface WPBoxProps {
  title?: string;
  description?: string;
  children: React.ReactNode;
  className?: string;
  headerAction?: React.ReactNode;
  footer?: React.ReactNode;
  noPadding?: boolean;
}

export function WPBox({
  title,
  description,
  children,
  className,
  headerAction,
  footer,
  noPadding = false,
}: WPBoxProps) {
  return (
    <div className={cn("card", className)}>
      {title && (
        <div className="card-header flex items-center justify-between">
          <div>
            <h3>{title}</h3>
            {description && <p>{description}</p>}
          </div>
          {headerAction}
        </div>
      )}
      <div className={cn(noPadding ? "p-0" : "card-body")}>{children}</div>
      {footer && (
        <div className="px-6 py-4 border-t border-gray-100 dark:border-gray-800">
          {footer}
        </div>
      )}
    </div>
  );
}

// Alias for TailAdmin-style naming
export const ComponentCard = WPBox;
