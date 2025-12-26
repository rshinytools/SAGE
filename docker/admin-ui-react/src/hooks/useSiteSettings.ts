import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { settingsApi } from "@/api/settings";

interface SiteSettings {
  siteName: string;
  siteDescription: string;
  isLoading: boolean;
}

/**
 * Hook to fetch and apply site-wide settings like site name and description.
 * Automatically updates the document title when settings are loaded.
 */
export function useSiteSettings(): SiteSettings {
  const { data: settings, isLoading } = useQuery({
    queryKey: ["settings"],
    queryFn: settingsApi.getAll,
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: false,
  });

  // Extract site name and description from settings
  const generalSettings = settings?.categories?.find((c) => c.id === "general")?.settings;
  const siteName = (generalSettings?.find((s) => s.key === "site_name")?.value as string) || "SAGE";
  const siteDescription = (generalSettings?.find((s) => s.key === "site_description")?.value as string) || "Study Analytics Generative Engine";

  // Update document title when site name changes
  useEffect(() => {
    if (!isLoading && siteName) {
      document.title = siteName;
    }
  }, [siteName, isLoading]);

  return {
    siteName,
    siteDescription,
    isLoading,
  };
}

/**
 * Set document title with optional page suffix
 */
export function usePageTitle(pageTitle?: string) {
  const { siteName, isLoading } = useSiteSettings();

  useEffect(() => {
    if (!isLoading) {
      document.title = pageTitle ? `${pageTitle} | ${siteName}` : siteName;
    }
  }, [pageTitle, siteName, isLoading]);
}
