import { useQuery } from "@tanstack/react-query";
import {
  Database,
  FileSearch,
  Users,
  ClipboardList,
  AlertTriangle,
  CheckCircle,
  Clock,
  TrendingUp,
  TrendingDown,
  Server,
  Cpu,
  HardDrive,
  MemoryStick,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { StatusBadge } from "@/components/common/StatusBadge";
import { systemApi } from "@/api/system";
import { trackerApi } from "@/api/tracker";
import { dataApi } from "@/api/data";
import { formatDateTime, cn } from "@/lib/utils";

// Stat Card Component
interface StatCardProps {
  title: string;
  value: number | string;
  icon: React.ReactNode;
  iconBg: string;
  trend?: {
    value: number;
    isPositive: boolean;
  };
}

function StatCard({ title, value, icon, iconBg, trend }: StatCardProps) {
  return (
    <div className="stat-card">
      <div className="flex items-start justify-between">
        <div>
          <p className="stat-card-label">{title}</p>
          <h3 className="stat-card-value mt-2">{value}</h3>
          {trend && (
            <div className="flex items-center gap-1 mt-2">
              {trend.isPositive ? (
                <TrendingUp className="w-4 h-4 text-[var(--color-success-500)]" />
              ) : (
                <TrendingDown className="w-4 h-4 text-[var(--color-error-500)]" />
              )}
              <span
                className={cn(
                  "text-sm font-medium",
                  trend.isPositive
                    ? "text-[var(--color-success-500)]"
                    : "text-[var(--color-error-500)]"
                )}
              >
                {trend.value}%
              </span>
            </div>
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
  colorClass = "bg-[var(--color-brand-500)]",
}: {
  label: string;
  value: number;
  colorClass?: string;
}) {
  return (
    <div>
      <div className="flex justify-between text-sm mb-2">
        <span className="text-gray-700 dark:text-gray-300">{label}</span>
        <span className="text-gray-500 dark:text-gray-400">{value}%</span>
      </div>
      <div className="progress-bar">
        <div
          className={cn("progress-bar-fill", colorClass)}
          style={{ width: `${value}%` }}
        />
      </div>
    </div>
  );
}

export function DashboardPage() {
  const { data: health, isLoading: healthLoading } = useQuery({
    queryKey: ["health"],
    queryFn: systemApi.getHealth,
    refetchInterval: 30000,
  });

  const { data: stats } = useQuery({
    queryKey: ["stats"],
    queryFn: systemApi.getStats,
    refetchInterval: 60000,
  });

  const { data: trackerProgress } = useQuery({
    queryKey: ["trackerProgress"],
    queryFn: trackerApi.getProgress,
  });

  const { data: tables } = useQuery({
    queryKey: ["tables"],
    queryFn: dataApi.getTables,
  });

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-gray-900 dark:text-white">
          Dashboard
        </h1>
        <p className="text-gray-500 dark:text-gray-400 mt-1">
          Welcome to SAGE Admin Panel
        </p>
      </div>

      {/* Stat Cards Row */}
      <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-4 gap-4 md:gap-6">
        <StatCard
          title="Data Tables"
          value={tables?.length || 0}
          icon={<Database className="w-6 h-6 text-[var(--color-brand-500)]" />}
          iconBg="bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10"
        />
        <StatCard
          title="Metadata Issues"
          value={0}
          icon={
            <FileSearch className="w-6 h-6 text-[var(--color-warning-500)]" />
          }
          iconBg="bg-[var(--color-warning-50)] dark:bg-[var(--color-warning-500)]/10"
        />
        <StatCard
          title="Active Sessions"
          value={stats?.active_sessions || 0}
          icon={<Users className="w-6 h-6 text-[var(--color-success-500)]" />}
          iconBg="bg-[var(--color-success-50)] dark:bg-[var(--color-success-500)]/10"
        />
        <StatCard
          title="Total Tasks"
          value={trackerProgress?.total_tasks || 0}
          icon={
            <ClipboardList className="w-6 h-6 text-[var(--color-info-500)]" />
          }
          iconBg="bg-[var(--color-info-50)] dark:bg-[var(--color-info-500)]/10"
        />
      </div>

      {/* Service Status and Project Progress */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-6">
        {/* Service Status */}
        <WPBox title="Service Status" description="Real-time service health">
          {healthLoading ? (
            <div className="flex items-center justify-center py-8">
              <span className="spinner spinner-lg" />
            </div>
          ) : (
            <div className="space-y-4">
              {health?.services?.map((service) => (
                <div
                  key={service.name}
                  className="flex items-center justify-between p-4 rounded-xl bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800"
                >
                  <div className="flex items-center gap-3">
                    <div
                      className={cn(
                        "w-10 h-10 rounded-lg flex items-center justify-center",
                        service.status === "healthy"
                          ? "bg-[var(--color-success-50)] dark:bg-[var(--color-success-500)]/10"
                          : "bg-[var(--color-error-50)] dark:bg-[var(--color-error-500)]/10"
                      )}
                    >
                      <Server
                        className={cn(
                          "w-5 h-5",
                          service.status === "healthy"
                            ? "text-[var(--color-success-500)]"
                            : "text-[var(--color-error-500)]"
                        )}
                      />
                    </div>
                    <div>
                      <p className="font-medium text-gray-900 dark:text-white">
                        {service.name}
                      </p>
                      {service.latency && (
                        <p className="text-sm text-gray-500 dark:text-gray-400">
                          {service.latency}ms latency
                        </p>
                      )}
                    </div>
                  </div>
                  <StatusBadge
                    variant={
                      service.status === "healthy" ? "success" : "destructive"
                    }
                  >
                    {service.status}
                  </StatusBadge>
                </div>
              )) || (
                <p className="text-gray-500 dark:text-gray-400 text-center py-8">
                  No services configured
                </p>
              )}
            </div>
          )}
        </WPBox>

        {/* Project Progress */}
        <WPBox
          title="Project Progress"
          description="Task completion overview"
        >
          <div className="space-y-6">
            {/* Overall Progress */}
            <div>
              <div className="flex items-center justify-between mb-4">
                <span className="text-lg font-semibold text-gray-900 dark:text-white">
                  {trackerProgress?.overall_progress || 0}%
                </span>
                <span className="text-sm text-gray-500 dark:text-gray-400">
                  {trackerProgress?.completed_tasks || 0} of{" "}
                  {trackerProgress?.total_tasks || 0} tasks
                </span>
              </div>
              <div className="progress-bar h-3">
                <div
                  className="progress-bar-fill bg-gradient-to-r from-[var(--color-brand-500)] to-[var(--color-brand-400)]"
                  style={{ width: `${trackerProgress?.overall_progress || 0}%` }}
                />
              </div>
            </div>

            {/* Task Breakdown */}
            <div className="grid grid-cols-2 gap-4">
              <div className="p-4 rounded-xl bg-[var(--color-success-50)] dark:bg-[var(--color-success-500)]/10 border border-[var(--color-success-100)] dark:border-[var(--color-success-500)]/20">
                <div className="flex items-center gap-3">
                  <CheckCircle className="w-5 h-5 text-[var(--color-success-500)]" />
                  <div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-white">
                      {trackerProgress?.completed_tasks || 0}
                    </p>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                      Completed
                    </p>
                  </div>
                </div>
              </div>
              <div className="p-4 rounded-xl bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10 border border-[var(--color-brand-100)] dark:border-[var(--color-brand-500)]/20">
                <div className="flex items-center gap-3">
                  <Clock className="w-5 h-5 text-[var(--color-brand-500)]" />
                  <div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-white">
                      {trackerProgress?.in_progress_tasks || 0}
                    </p>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                      In Progress
                    </p>
                  </div>
                </div>
              </div>
              <div className="p-4 rounded-xl bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
                <div className="flex items-center gap-3">
                  <Clock className="w-5 h-5 text-gray-400" />
                  <div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-white">
                      {(trackerProgress?.total_tasks || 0) -
                        (trackerProgress?.completed_tasks || 0) -
                        (trackerProgress?.in_progress_tasks || 0) -
                        (trackerProgress?.blocked_tasks || 0)}
                    </p>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                      Pending
                    </p>
                  </div>
                </div>
              </div>
              <div className="p-4 rounded-xl bg-[var(--color-error-50)] dark:bg-[var(--color-error-500)]/10 border border-[var(--color-error-100)] dark:border-[var(--color-error-500)]/20">
                <div className="flex items-center gap-3">
                  <AlertTriangle className="w-5 h-5 text-[var(--color-error-500)]" />
                  <div>
                    <p className="text-2xl font-bold text-gray-900 dark:text-white">
                      {trackerProgress?.blocked_tasks || 0}
                    </p>
                    <p className="text-sm text-gray-600 dark:text-gray-400">
                      Blocked
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </WPBox>
      </div>

      {/* Recent Data Tables and System Resources */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 md:gap-6">
        {/* Recent Data Tables */}
        <WPBox title="Recent Data Tables" description="Latest loaded datasets">
          <div className="space-y-3">
            {tables?.slice(0, 5).map((table) => (
              <div
                key={table.name}
                className="flex items-center justify-between p-4 rounded-xl bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800 hover:border-[var(--color-brand-200)] dark:hover:border-[var(--color-brand-800)] transition-colors cursor-pointer"
              >
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10 flex items-center justify-center">
                    <Database className="w-5 h-5 text-[var(--color-brand-500)]" />
                  </div>
                  <div>
                    <p className="font-medium text-gray-900 dark:text-white">
                      {table.name}
                    </p>
                    <p className="text-sm text-gray-500 dark:text-gray-400">
                      {table.rows.toLocaleString()} rows · {table.columns} cols
                    </p>
                  </div>
                </div>
                <span className="text-xs text-gray-400 dark:text-gray-500">
                  {formatDateTime(table.modified_at)}
                </span>
              </div>
            )) || (
              <p className="text-gray-500 dark:text-gray-400 text-center py-8">
                No data tables loaded
              </p>
            )}
          </div>
        </WPBox>

        {/* System Resources */}
        <WPBox
          title="System Resources"
          description="Server resource utilization"
        >
          <div className="space-y-6">
            <div className="flex items-center gap-4 p-4 rounded-xl bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="w-12 h-12 rounded-xl bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-500)]/10 flex items-center justify-center">
                <Cpu className="w-6 h-6 text-[var(--color-brand-500)]" />
              </div>
              <div className="flex-1">
                <ProgressBar
                  label="CPU Usage"
                  value={stats?.cpu_percent || 0}
                  colorClass="bg-[var(--color-brand-500)]"
                />
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-xl bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="w-12 h-12 rounded-xl bg-[var(--color-success-50)] dark:bg-[var(--color-success-500)]/10 flex items-center justify-center">
                <MemoryStick className="w-6 h-6 text-[var(--color-success-500)]" />
              </div>
              <div className="flex-1">
                <ProgressBar
                  label="Memory Usage"
                  value={stats?.memory_percent || 0}
                  colorClass="bg-[var(--color-success-500)]"
                />
              </div>
            </div>

            <div className="flex items-center gap-4 p-4 rounded-xl bg-gray-50 dark:bg-white/[0.02] border border-gray-100 dark:border-gray-800">
              <div className="w-12 h-12 rounded-xl bg-[var(--color-warning-50)] dark:bg-[var(--color-warning-500)]/10 flex items-center justify-center">
                <HardDrive className="w-6 h-6 text-[var(--color-warning-500)]" />
              </div>
              <div className="flex-1">
                <ProgressBar
                  label="Disk Usage"
                  value={stats?.disk_percent || 0}
                  colorClass="bg-[var(--color-warning-500)]"
                />
              </div>
            </div>
          </div>
        </WPBox>
      </div>
    </div>
  );
}
