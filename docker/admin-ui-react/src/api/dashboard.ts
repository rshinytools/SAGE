import apiClient from "./client";
import type { DashboardStats, RecentQuery, QueryTrend } from "@/types/api";

interface DashboardStatsResponse {
  success: boolean;
  data: DashboardStats;
  meta: {
    timestamp: string;
  };
}

interface RecentQueriesResponse {
  success: boolean;
  data: RecentQuery[];
  meta: {
    timestamp: string;
  };
}

interface QueryTrendsResponse {
  success: boolean;
  data: QueryTrend[];
  meta: {
    timestamp: string;
  };
}

export const dashboardApi = {
  getStats: async (): Promise<DashboardStats> => {
    const response = await apiClient.get<DashboardStatsResponse>("/dashboard/stats");
    return response.data.data;
  },

  getRecentQueries: async (limit: number = 10): Promise<RecentQuery[]> => {
    const response = await apiClient.get<RecentQueriesResponse>("/dashboard/queries/recent", {
      params: { limit },
    });
    return response.data.data;
  },

  getQueryTrends: async (hours: number = 24): Promise<QueryTrend[]> => {
    const response = await apiClient.get<QueryTrendsResponse>("/dashboard/queries/trends", {
      params: { hours },
    });
    return response.data.data;
  },
};
