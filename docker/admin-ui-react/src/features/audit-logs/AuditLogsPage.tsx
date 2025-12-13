import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Download, Filter, ScrollText, CheckCircle, XCircle } from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { auditApi } from "@/api/audit";
import { formatDateTime } from "@/lib/utils";
import type { ColumnDef } from "@tanstack/react-table";
import type { AuditLogEntry, AuditLogFilter } from "@/types/api";

export function AuditLogsPage() {
  const [filter, setFilter] = useState<AuditLogFilter>({});
  const [showFilters, setShowFilters] = useState(false);

  const { data: logs, isLoading } = useQuery({
    queryKey: ["auditLogs", filter],
    queryFn: () => auditApi.getLogs(filter),
  });

  const { data: actions } = useQuery({
    queryKey: ["auditActions"],
    queryFn: auditApi.getActions,
  });

  const { data: resources } = useQuery({
    queryKey: ["auditResources"],
    queryFn: auditApi.getResources,
  });

  const { data: statistics } = useQuery({
    queryKey: ["auditStatistics", filter.start_date, filter.end_date],
    queryFn: () => auditApi.getStatistics(filter.start_date, filter.end_date),
  });

  const columns: ColumnDef<AuditLogEntry>[] = [
    {
      accessorKey: "timestamp",
      header: "Timestamp",
      cell: ({ row }) => formatDateTime(row.original.timestamp),
    },
    {
      accessorKey: "user",
      header: "User",
      cell: ({ row }) => (
        <span className="font-medium">{row.original.user}</span>
      ),
    },
    {
      accessorKey: "action",
      header: "Action",
      cell: ({ row }) => (
        <StatusBadge variant="primary">{row.original.action}</StatusBadge>
      ),
    },
    {
      accessorKey: "resource",
      header: "Resource",
    },
    {
      accessorKey: "resource_id",
      header: "Resource ID",
      cell: ({ row }) => row.original.resource_id || "-",
    },
    {
      accessorKey: "status",
      header: "Status",
      cell: ({ row }) => (
        <StatusBadge
          variant={row.original.status === "success" ? "success" : "destructive"}
        >
          <span className="flex items-center gap-1">
            {row.original.status === "success" ? (
              <CheckCircle className="w-3 h-3" />
            ) : (
              <XCircle className="w-3 h-3" />
            )}
            {row.original.status}
          </span>
        </StatusBadge>
      ),
    },
    {
      accessorKey: "ip_address",
      header: "IP Address",
      cell: ({ row }) => row.original.ip_address || "-",
    },
  ];

  const handleExport = async (format: "csv" | "json") => {
    const blob = await auditApi.exportLogs(filter, format);
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `audit-logs.${format}`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const clearFilters = () => {
    setFilter({});
  };

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            Audit Logs
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Track system activity and user actions
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button
            className="btn btn-secondary btn-md"
            onClick={() => setShowFilters(!showFilters)}
          >
            <Filter className="w-4 h-4" />
            Filters
          </button>
          <button className="btn btn-secondary btn-md" onClick={() => handleExport("csv")}>
            <Download className="w-4 h-4" />
            Export
          </button>
        </div>
      </div>

      {/* Statistics */}
      <div className="grid grid-cols-4 gap-4">
        <div className="wp-box p-4 flex items-center gap-3">
          <ScrollText className="w-8 h-8 text-[var(--primary)]" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.total_entries || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Total Entries</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <CheckCircle className="w-8 h-8 text-[var(--success)]" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.by_status?.success || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Successful</div>
          </div>
        </div>
        <div className="wp-box p-4 flex items-center gap-3">
          <XCircle className="w-8 h-8 text-[var(--destructive)]" />
          <div>
            <div className="text-2xl font-bold">
              {statistics?.by_status?.failure || 0}
            </div>
            <div className="text-sm text-[var(--muted)]">Failed</div>
          </div>
        </div>
        <div className="wp-box p-4">
          <div className="text-lg font-bold">
            {Object.keys(statistics?.by_user || {}).length}
          </div>
          <div className="text-sm text-[var(--muted)]">Active Users</div>
        </div>
      </div>

      {/* Filters */}
      {showFilters && (
        <WPBox title="Filters">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <label className="block text-sm font-medium mb-1">User</label>
              <input
                type="text"
                placeholder="Filter by user"
                value={filter.user || ""}
                onChange={(e) =>
                  setFilter({ ...filter, user: e.target.value || undefined })
                }
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Action</label>
              <select
                value={filter.action || ""}
                onChange={(e) =>
                  setFilter({ ...filter, action: e.target.value || undefined })
                }
              >
                <option value="">All Actions</option>
                {actions?.map((action) => (
                  <option key={action} value={action}>
                    {action}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Resource</label>
              <select
                value={filter.resource || ""}
                onChange={(e) =>
                  setFilter({ ...filter, resource: e.target.value || undefined })
                }
              >
                <option value="">All Resources</option>
                {resources?.map((resource) => (
                  <option key={resource} value={resource}>
                    {resource}
                  </option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Status</label>
              <select
                value={filter.status || ""}
                onChange={(e) =>
                  setFilter({
                    ...filter,
                    status: (e.target.value as "success" | "failure") || undefined,
                  })
                }
              >
                <option value="">All Statuses</option>
                <option value="success">Success</option>
                <option value="failure">Failure</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">Start Date</label>
              <input
                type="date"
                value={filter.start_date || ""}
                onChange={(e) =>
                  setFilter({ ...filter, start_date: e.target.value || undefined })
                }
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-1">End Date</label>
              <input
                type="date"
                value={filter.end_date || ""}
                onChange={(e) =>
                  setFilter({ ...filter, end_date: e.target.value || undefined })
                }
              />
            </div>
            <div className="col-span-2 flex items-end">
              <button className="btn btn-secondary btn-md" onClick={clearFilters}>
                Clear Filters
              </button>
            </div>
          </div>
        </WPBox>
      )}

      {/* Logs Table */}
      <WPBox title="Audit Log Entries">
        {isLoading ? (
          <div className="flex items-center justify-center py-8">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
          </div>
        ) : logs?.items?.length ? (
          <DataTable columns={columns} data={logs.items} pageSize={20} />
        ) : (
          <div className="text-center py-8 text-[var(--muted)]">
            <ScrollText className="w-12 h-12 mx-auto mb-4" />
            <p>No audit log entries found</p>
          </div>
        )}
      </WPBox>
    </div>
  );
}
