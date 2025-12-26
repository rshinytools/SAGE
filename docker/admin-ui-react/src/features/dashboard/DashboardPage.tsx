import { useQuery } from "@tanstack/react-query";
import {
  Database,
  FileSearch,
  Users,
  MessageSquare,
  TrendingUp,
  Cpu,
  HardDrive,
  MemoryStick,
  Zap,
  Clock,
  CheckCircle,
  XCircle,
  AlertCircle,
  RefreshCw,
  Bot,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { StatusBadge } from "@/components/common/StatusBadge";
import { dashboardApi } from "@/api/dashboard";
import { cn } from "@/lib/utils";
import type { RecentQuery, DashboardServiceHealth } from "@/types/api";

// Stat Card Component
interface StatCardProps {
  title: string;
  value: number | string;
  subtitle?: string;
  icon: React.ReactNode;
  iconBg: string;
}

function StatCard({ title, value, subtitle, icon, iconBg }: StatCardProps) {
  return (
    <div className="stat-card">
      <div className="flex items-start justify-between">
        <div>
          <p className="stat-card-label">{title}</p>
          <h3 className="stat-card-value mt-2">{value}</h3>
          {subtitle && (
            <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
              {subtitle}
            </p>
          )}
        </div>
        <div className={cn("stat-card-icon", iconBg)}>{icon}</div>
      </div>
    </div>
  );
}

// Progress Bar Component
function ProgressBar({
  label,
  value,
  subtitle,
  colorClass = "bg-[var(--color-brand-500)]",
}: {
  label: string;
  value: number;
  subtitle?: string;
  colorClass?: string;
}) {
  return (
    <div>
      <div className="flex justify-between text-sm mb-2">
        <span className="text-gray-700 dark:text-gray-300">{label}</span>
        <div className="text-right">
          <span className="text-gray-500 dark:text-gray-400">{Math.round(value)}%</span>
          {subtitle && (
            <span className="text-xs text-gray-400 dark:text-gray-500 ml-2">{subtitle}</span>
          )}
        </div>
      </div>
      <div className="progress-bar">
        <div
          className={cn("progress-bar-fill", colorClass)}
          style={{ width: `${Math.min(value, 100)}%` }}
        />
      </div>
    </div>
  );
}

// Confidence Badge Component
function ConfidenceBadge({ confidence }: { confidence: number | null }) {
  if (confidence === null) {
    return (
      <span className="px-2 py-1 text-xs font-medium rounded-full bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400">
        N/A
      </span>
    );
  }

  if (confidence >= 90) {
    return (
      <span className="px-2 py-1 text-xs font-medium rounded-full bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
        {confidence}%
      </span>
    );
  } else if (confidence >= 70) {
    return (
      <span className="px-2 py-1 text-xs font-medium rounded-full bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400">
        {confidence}%
      </span>
    );
  } else {
    return (
      <span className="px-2 py-1 text-xs font-medium rounded-full bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400">
        {confidence}%
      </span>
    );
  }
}

// Service Status Icon
function ServiceStatusIcon({ status }: { status: DashboardServiceHealth["status"] }) {
  if (status === "healthy") {
    return <CheckCircle className="w-4 h-4 text-green-500" />;
  } else if (status === "unhealthy") {
    return <XCircle className="w-4 h-4 text-red-500" />;
  } else {
    return <AlertCircle className="w-4 h-4 text-gray-400" />;
  }
}

export function DashboardPage() {
  const { data: stats, isLoading, refetch, isFetching } = useQuery({
    queryKey: ["dashboardStats"],
    queryFn: dashboardApi.getStats,
    refetchInterval: 30000,
    staleTime: 10000,
  });

  const { data: recentQueries } = useQuery({
    queryKey: ["recentQueries"],
    queryFn: () => dashboardApi.getRecentQueries(8),
    refetchInterval: 30000,
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center min-h-[400px]">
        <span className="spinner spinner-lg" />
      </div>
    );
  }

  const queryStats = stats?.queries;
  const userStats = stats?.users;
  const dataStats = stats?.data;
  const metadataStats = stats?.metadata;
  const cacheStats = stats?.cache;
  const llmStats = stats?.llm;
  const services = stats?.services || [];
  const resources = stats?.resources;

  return (
    <div className="space-y-6">
      {/* Page Header with Refresh */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">
            Dashboard
          </h1>
          <p className="text-gray-500 dark:text-gray-400 mt-1">
            Real-time platform analytics and health monitoring
          </p>
        </div>
        <button
          onClick={() => refetch()}
          disabled={isFetching}
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors disabled:opacity-50"
        >
          <RefreshCw className={cn("w-4 h-4", isFetching && "animate-spin")} />
          Refresh
        </button>
      </div>

      {/* Key Metrics Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
        <StatCard
          title="Queries Today"
          value={queryStats?.today || 0}
          subtitle={`${queryStats?.total || 0} total`}
          icon={<MessageSquare className="w-6 h-6 text-[var(--color-brand-500)]" />}
          iconBg="bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10"
        />
        <StatCard
          title="Avg Confidence"
          value={queryStats?.avg_confidence ? `${Math.round(queryStats.avg_confidence)}%` : "N/A"}
          subtitle={queryStats?.avg_execution_time_ms ? `${Math.round(queryStats.avg_execution_time_ms)}ms avg` : undefined}
          icon={<TrendingUp className="w-6 h-6 text-[var(--color-success-500)]" />}
          iconBg="bg-[var(--color-success-50)] dark:bg-[var(--color-success-500)]/10"
        />
        <StatCard
          title="Active Users (24h)"
          value={userStats?.active_24h || 0}
          subtitle={`${userStats?.total || 0} total users`}
          icon={<Users className="w-6 h-6 text-[var(--color-info-500)]" />}
          iconBg="bg-[var(--color-info-50)] dark:bg-[var(--color-info-500)]/10"
        />
        <StatCard
          title="Data Tables"
          value={dataStats?.total_tables || 0}
          subtitle={`${dataStats?.total_rows?.toLocaleString() || 0} rows`}
          icon={<Database className="w-6 h-6 text-[var(--color-warning-500)]" />}
          iconBg="bg-[var(--color-warning-50)] dark:bg-[var(--color-warning-500)]/10"
        />
        <StatCard
          title="Metadata Pending"
          value={metadataStats?.pending || 0}
          subtitle={`${metadataStats?.approved || 0} approved`}
          icon={<FileSearch className="w-6 h-6 text-purple-500" />}
          iconBg="bg-purple-50 dark:bg-purple-500/10"
        />
      </div>

      {/* Query Analytics Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-6">
        {/* Recent Queries */}
        <WPBox title="Recent Queries" description="Latest AI chat interactions">
          <div className="space-y-3 max-h-[360px] overflow-y-auto">
            {recentQueries && recentQueries.length > 0 ? (
              recentQueries.map((query: RecentQuery) => (
                <div
                  key={query.id}
                  className="flex items-start gap-3 p-3 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800"
                >
                  <div className="flex-shrink-0 mt-1">
                    <MessageSquare className="w-4 h-4 text-gray-400" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-gray-900 dark:text-white truncate">
                      {query.question}
                    </p>
                    <div className="flex items-center gap-2 mt-1">
                      <span className="text-xs text-gray-500 dark:text-gray-400">
                        {query.username}
                      </span>
                      <span className="text-xs text-gray-400">•</span>
                      <span className="text-xs text-gray-500 dark:text-gray-400">
                        {query.relative}
                      </span>
                      {query.execution_time_ms && (
                        <>
                          <span className="text-xs text-gray-400">•</span>
                          <span className="text-xs text-gray-500 dark:text-gray-400">
                            {Math.round(query.execution_time_ms)}ms
                          </span>
                        </>
                      )}
                    </div>
                  </div>
                  <ConfidenceBadge confidence={query.confidence} />
                </div>
              ))
            ) : (
              <p className="text-gray-500 dark:text-gray-400 text-center py-8">
                No queries yet
              </p>
            )}
          </div>
        </WPBox>

        {/* Confidence Distribution */}
        <WPBox title="Query Confidence" description="Response quality distribution">
          <div className="space-y-6">
            {/* Distribution Bars */}
            <div className="space-y-4">
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <CheckCircle className="w-4 h-4 text-green-500" />
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                      High Confidence (90%+)
                    </span>
                  </div>
                  <span className="text-sm font-bold text-gray-900 dark:text-white">
                    {queryStats?.confidence_distribution?.high || 0}
                  </span>
                </div>
                <div className="progress-bar h-2">
                  <div
                    className="progress-bar-fill bg-green-500"
                    style={{
                      width: `${
                        queryStats?.total
                          ? ((queryStats.confidence_distribution?.high || 0) / queryStats.total) * 100
                          : 0
                      }%`,
                    }}
                  />
                </div>
              </div>

              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <AlertCircle className="w-4 h-4 text-yellow-500" />
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                      Medium Confidence (70-89%)
                    </span>
                  </div>
                  <span className="text-sm font-bold text-gray-900 dark:text-white">
                    {queryStats?.confidence_distribution?.medium || 0}
                  </span>
                </div>
                <div className="progress-bar h-2">
                  <div
                    className="progress-bar-fill bg-yellow-500"
                    style={{
                      width: `${
                        queryStats?.total
                          ? ((queryStats.confidence_distribution?.medium || 0) / queryStats.total) * 100
                          : 0
                      }%`,
                    }}
                  />
                </div>
              </div>

              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <XCircle className="w-4 h-4 text-red-500" />
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                      Low Confidence (&lt;70%)
                    </span>
                  </div>
                  <span className="text-sm font-bold text-gray-900 dark:text-white">
                    {queryStats?.confidence_distribution?.low || 0}
                  </span>
                </div>
                <div className="progress-bar h-2">
                  <div
                    className="progress-bar-fill bg-red-500"
                    style={{
                      width: `${
                        queryStats?.total
                          ? ((queryStats.confidence_distribution?.low || 0) / queryStats.total) * 100
                          : 0
                      }%`,
                    }}
                  />
                </div>
              </div>
            </div>

            {/* Top Queried Tables */}
            {queryStats?.top_tables && queryStats.top_tables.length > 0 && (
              <div className="pt-4 border-t border-gray-200 dark:border-gray-700">
                <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">
                  Most Queried Tables
                </h4>
                <div className="flex flex-wrap gap-2">
                  {queryStats.top_tables.slice(0, 5).map((table) => (
                    <span
                      key={table.table}
                      className="px-2 py-1 text-xs font-medium rounded-full bg-[var(--color-brand-50)] text-[var(--color-brand-600)] dark:bg-[var(--color-brand-500)]/10 dark:text-[var(--color-brand-400)]"
                    >
                      {table.table} ({table.count})
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </WPBox>
      </div>

      {/* Service Health and LLM Status */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 md:gap-6">
        {/* Service Health */}
        <WPBox title="Service Health" description="Backend service status" className="lg:col-span-2">
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            {services.map((service) => (
              <div
                key={service.name}
                className="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800"
              >
                <div className="flex items-center gap-3">
                  <ServiceStatusIcon status={service.status} />
                  <div>
                    <p className="text-sm font-medium text-gray-900 dark:text-white">
                      {service.name}
                    </p>
                    {service.latency_ms !== null && (
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        {Math.round(service.latency_ms)}ms
                      </p>
                    )}
                  </div>
                </div>
                <StatusBadge
                  variant={service.status === "healthy" ? "success" : service.status === "unhealthy" ? "destructive" : "default"}
                >
                  {service.status}
                </StatusBadge>
              </div>
            ))}
          </div>
        </WPBox>

        {/* LLM Provider Status */}
        <WPBox title="LLM Provider" description="AI model status">
          <div className="space-y-4">
            <div className="flex items-center gap-3 p-4 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className={cn(
                "w-12 h-12 rounded-xl flex items-center justify-center",
                llmStats?.status === "available"
                  ? "bg-green-50 dark:bg-green-500/10"
                  : "bg-red-50 dark:bg-red-500/10"
              )}>
                <Bot className={cn(
                  "w-6 h-6",
                  llmStats?.status === "available" ? "text-green-500" : "text-red-500"
                )} />
              </div>
              <div className="flex-1">
                <p className="font-medium text-gray-900 dark:text-white">
                  {llmStats?.provider || "Unknown"}
                </p>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  {llmStats?.model || "No model"}
                </p>
              </div>
              <StatusBadge variant={llmStats?.status === "available" ? "success" : "destructive"}>
                {llmStats?.status || "unknown"}
              </StatusBadge>
            </div>

            {/* Cache Stats */}
            <div className="p-4 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="flex items-center gap-2 mb-3">
                <Zap className="w-4 h-4 text-[var(--color-brand-500)]" />
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  Query Cache
                </span>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <p className="text-2xl font-bold text-gray-900 dark:text-white">
                    {cacheStats?.hit_rate ? `${Math.round(cacheStats.hit_rate)}%` : "0%"}
                  </p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">Hit Rate</p>
                </div>
                <div>
                  <p className="text-2xl font-bold text-gray-900 dark:text-white">
                    {cacheStats?.total_entries || 0}
                  </p>
                  <p className="text-xs text-gray-500 dark:text-gray-400">Entries</p>
                </div>
              </div>
            </div>
          </div>
        </WPBox>
      </div>

      {/* Data Tables and System Resources */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-6">
        {/* Data Tables */}
        <WPBox title="Data Tables" description="Loaded clinical datasets">
          <div className="space-y-3 max-h-[320px] overflow-y-auto">
            {dataStats?.tables && dataStats.tables.length > 0 ? (
              dataStats.tables.slice(0, 8).map((table) => (
                <div
                  key={table.name}
                  className="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800"
                >
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-lg bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10 flex items-center justify-center">
                      <Database className="w-5 h-5 text-[var(--color-brand-500)]" />
                    </div>
                    <div>
                      <p className="font-medium text-gray-900 dark:text-white">
                        {table.name}
                      </p>
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        {table.rows.toLocaleString()} rows • {table.columns} cols
                      </p>
                    </div>
                  </div>
                  {table.size_kb > 0 && (
                    <span className="text-xs text-gray-400 dark:text-gray-500">
                      {table.size_kb >= 1024
                        ? `${(table.size_kb / 1024).toFixed(1)} MB`
                        : `${table.size_kb} KB`}
                    </span>
                  )}
                </div>
              ))
            ) : (
              <p className="text-gray-500 dark:text-gray-400 text-center py-8">
                No data tables loaded
              </p>
            )}
          </div>
        </WPBox>

        {/* System Resources */}
        <WPBox title="System Resources" description="Server utilization">
          <div className="space-y-5">
            <div className="flex items-center gap-4 p-4 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="w-12 h-12 rounded-xl bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10 flex items-center justify-center">
                <Cpu className="w-6 h-6 text-[var(--color-brand-500)]" />
              </div>
              <div className="flex-1">
                <ProgressBar
                  label="CPU Usage"
                  value={resources?.cpu_percent || 0}
                  colorClass="bg-[var(--color-brand-500)]"
                />
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="w-12 h-12 rounded-xl bg-[var(--color-success-50)] dark:bg-[var(--color-success-500)]/10 flex items-center justify-center">
                <MemoryStick className="w-6 h-6 text-[var(--color-success-500)]" />
              </div>
              <div className="flex-1">
                <ProgressBar
                  label="Memory Usage"
                  value={resources?.memory_percent || 0}
                  subtitle={resources?.memory_used_gb ? `${resources.memory_used_gb.toFixed(1)}/${resources.memory_total_gb?.toFixed(1)} GB` : undefined}
                  colorClass="bg-[var(--color-success-500)]"
                />
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="w-12 h-12 rounded-xl bg-[var(--color-warning-50)] dark:bg-[var(--color-warning-500)]/10 flex items-center justify-center">
                <HardDrive className="w-6 h-6 text-[var(--color-warning-500)]" />
              </div>
              <div className="flex-1">
                <ProgressBar
                  label="Disk Usage"
                  value={resources?.disk_percent || 0}
                  subtitle={resources?.disk_used_gb ? `${resources.disk_used_gb.toFixed(1)}/${resources.disk_total_gb?.toFixed(1)} GB` : undefined}
                  colorClass="bg-[var(--color-warning-500)]"
                />
              </div>
            </div>
          </div>
        </WPBox>
      </div>

      {/* User Activity Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 md:gap-6">
        {/* Recent Logins */}
        <WPBox title="Recent Logins" description="User activity" className="lg:col-span-2">
          <div className="space-y-3">
            {userStats?.recent_logins && userStats.recent_logins.length > 0 ? (
              userStats.recent_logins.map((login, idx) => (
                <div
                  key={`${login.username}-${idx}`}
                  className="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800"
                >
                  <div className="flex items-center gap-3">
                    <div className="w-10 h-10 rounded-full bg-[var(--color-brand-100)] dark:bg-[var(--color-brand-500)]/20 flex items-center justify-center">
                      <span className="text-sm font-medium text-[var(--color-brand-600)] dark:text-[var(--color-brand-400)]">
                        {login.username.charAt(0).toUpperCase()}
                      </span>
                    </div>
                    <div>
                      <p className="font-medium text-gray-900 dark:text-white">
                        {login.username}
                      </p>
                      <p className="text-xs text-gray-500 dark:text-gray-400">
                        Logged in
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
                    <Clock className="w-4 h-4" />
                    {login.relative}
                  </div>
                </div>
              ))
            ) : (
              <p className="text-gray-500 dark:text-gray-400 text-center py-8">
                No recent logins
              </p>
            )}
          </div>
        </WPBox>

        {/* User Access Levels */}
        <WPBox title="Access Levels" description="User distribution">
          <div className="space-y-4">
            <div className="p-4 rounded-lg bg-purple-50 dark:bg-purple-500/10 border border-purple-100 dark:border-purple-500/20">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  Full Admin
                </span>
                <span className="text-2xl font-bold text-gray-900 dark:text-white">
                  {userStats?.by_access_level?.admin || 0}
                </span>
              </div>
            </div>
            <div className="p-4 rounded-lg bg-blue-50 dark:bg-blue-500/10 border border-blue-100 dark:border-blue-500/20">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  User Admin
                </span>
                <span className="text-2xl font-bold text-gray-900 dark:text-white">
                  {userStats?.by_access_level?.user_admin || 0}
                </span>
              </div>
            </div>
            <div className="p-4 rounded-lg bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="flex items-center justify-between">
                <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                  Chat Only
                </span>
                <span className="text-2xl font-bold text-gray-900 dark:text-white">
                  {userStats?.by_access_level?.chat_only || 0}
                </span>
              </div>
            </div>
          </div>
        </WPBox>
      </div>
    </div>
  );
}
