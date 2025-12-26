import { useQuery } from "@tanstack/react-query";
import { settingsApi } from "@/api/settings";
import { format as dateFnsFormat } from "date-fns";

// Map settings format to date-fns format
const FORMAT_MAP: Record<string, string> = {
  "YYYY-MM-DD": "yyyy-MM-dd",
  "MM/DD/YYYY": "MM/dd/yyyy",
  "DD/MM/YYYY": "dd/MM/yyyy",
  "DD-MMM-YYYY": "dd-MMM-yyyy",
};

/**
 * Hook to get date formatting functions based on platform settings.
 * Falls back to ISO format if settings aren't loaded.
 */
export function useDateFormat() {
  const { data: settings } = useQuery({
    queryKey: ["settings"],
    queryFn: settingsApi.getAll,
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: false,
  });

  // Get the date format from settings, default to YYYY-MM-DD
  const dateFormatSetting = settings?.categories
    ?.find((c) => c.id === "general")
    ?.settings?.find((s) => s.key === "date_format")?.value as string | undefined;

  const dateFormat = FORMAT_MAP[dateFormatSetting || "YYYY-MM-DD"] || "yyyy-MM-dd";

  /**
   * Format a date according to platform settings
   */
  const formatDate = (date: Date | string | null | undefined): string => {
    if (!date) return "-";
    try {
      const d = typeof date === "string" ? new Date(date) : date;
      if (isNaN(d.getTime())) return "-";
      return dateFnsFormat(d, dateFormat);
    } catch {
      return "-";
    }
  };

  /**
   * Format a date with time according to platform settings
   */
  const formatDateTime = (date: Date | string | null | undefined): string => {
    if (!date) return "-";
    try {
      const d = typeof date === "string" ? new Date(date) : date;
      if (isNaN(d.getTime())) return "-";
      return dateFnsFormat(d, `${dateFormat} HH:mm:ss`);
    } catch {
      return "-";
    }
  };

  /**
   * Format a relative time (e.g., "2 hours ago")
   */
  const formatRelative = (date: Date | string | null | undefined): string => {
    if (!date) return "-";
    try {
      const d = typeof date === "string" ? new Date(date) : date;
      if (isNaN(d.getTime())) return "-";

      const now = new Date();
      const diffMs = now.getTime() - d.getTime();
      const diffSecs = Math.floor(diffMs / 1000);
      const diffMins = Math.floor(diffSecs / 60);
      const diffHours = Math.floor(diffMins / 60);
      const diffDays = Math.floor(diffHours / 24);

      if (diffSecs < 60) return "just now";
      if (diffMins < 60) return `${diffMins} min ago`;
      if (diffHours < 24) return `${diffHours} hour${diffHours > 1 ? "s" : ""} ago`;
      if (diffDays < 7) return `${diffDays} day${diffDays > 1 ? "s" : ""} ago`;

      return formatDate(d);
    } catch {
      return "-";
    }
  };

  return {
    dateFormat: dateFormatSetting || "YYYY-MM-DD",
    formatDate,
    formatDateTime,
    formatRelative,
  };
}
