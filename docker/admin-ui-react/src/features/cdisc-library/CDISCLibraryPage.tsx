import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  BookOpen,
  Search,
  Database,
  FileText,
  ChevronRight,
  Info,
  Tag,
  Layers,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { DataTable } from "@/components/common/DataTable";
import { StatusBadge } from "@/components/common/StatusBadge";
import { metadataApi } from "@/api/metadata";
import type { ColumnDef } from "@tanstack/react-table";

interface CDISCDomain {
  standard: string;
  version: string;
  name: string;
  label: string;
  domain_class: string;
  structure: string;
}

interface CDISCVariable {
  standard: string;
  version: string;
  domain: string;
  name: string;
  label: string;
  data_type: string;
  core: string;
  role: string;
  codelist: string;
  description: string;
}

// Search results have fewer fields
interface CDISCSearchResult {
  standard: string;
  version: string;
  domain: string;
  name: string;
  label: string;
  data_type: string;
  core: string;
}

type Tab = "overview" | "sdtm" | "adam" | "search";

export function CDISCLibraryPage() {
  const [activeTab, setActiveTab] = useState<Tab>("overview");
  const [selectedDomain, setSelectedDomain] = useState<string | null>(null);
  const [selectedStandard, setSelectedStandard] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  // Queries
  const { data: stats, isLoading: statsLoading } = useQuery({
    queryKey: ["cdisc-stats"],
    queryFn: metadataApi.getCDISCStats,
  });

  const { data: domains, isLoading: domainsLoading } = useQuery({
    queryKey: ["cdisc-domains", selectedStandard],
    queryFn: () => metadataApi.getCDISCDomains(selectedStandard || undefined),
    enabled: activeTab !== "overview" || !!selectedStandard,
  });

  const { data: variables, isLoading: variablesLoading } = useQuery({
    queryKey: ["cdisc-variables", selectedDomain, selectedStandard],
    queryFn: () => metadataApi.getCDISCVariables(selectedDomain!, selectedStandard || undefined),
    enabled: !!selectedDomain,
  });

  const { data: searchResults, isLoading: searchLoading } = useQuery({
    queryKey: ["cdisc-search", searchQuery],
    queryFn: () => metadataApi.searchCDISCVariables(searchQuery),
    enabled: searchQuery.length >= 2,
  });

  // Column definitions
  const domainColumns: ColumnDef<CDISCDomain>[] = [
    {
      accessorKey: "name",
      header: "Domain",
      cell: ({ row }) => (
        <button
          className="text-[var(--primary)] hover:underline font-medium flex items-center gap-1"
          onClick={() => {
            setSelectedDomain(row.original.name);
            setSelectedStandard(row.original.standard);
          }}
        >
          {row.original.name}
          <ChevronRight className="w-4 h-4" />
        </button>
      ),
    },
    {
      accessorKey: "label",
      header: "Label",
    },
    {
      accessorKey: "domain_class",
      header: "Class",
      cell: ({ row }) => (
        <StatusBadge variant="default">
          {row.original.domain_class}
        </StatusBadge>
      ),
    },
    {
      accessorKey: "standard",
      header: "Standard",
      cell: ({ row }) => (
        <StatusBadge variant={row.original.standard === "SDTM" ? "primary" : "success"}>
          {row.original.standard} {row.original.version}
        </StatusBadge>
      ),
    },
  ];

  const variableColumns: ColumnDef<CDISCVariable>[] = [
    {
      accessorKey: "name",
      header: "Variable",
      cell: ({ row }) => (
        <span className="font-mono font-medium">{row.original.name}</span>
      ),
    },
    {
      accessorKey: "label",
      header: "Label",
    },
    {
      accessorKey: "data_type",
      header: "Type",
      cell: ({ row }) => (
        <span className="text-[var(--muted)]">{row.original.data_type || "-"}</span>
      ),
    },
    {
      accessorKey: "core",
      header: "Core",
      cell: ({ row }) => {
        const core = row.original.core;
        if (!core) return "-";
        const variant = core === "Req" ? "destructive" : core === "Exp" ? "warning" : "default";
        return <StatusBadge variant={variant}>{core}</StatusBadge>;
      },
    },
    {
      accessorKey: "role",
      header: "Role",
      cell: ({ row }) => (
        <span className="text-[var(--muted)]">{row.original.role || "-"}</span>
      ),
    },
    {
      accessorKey: "codelist",
      header: "Codelist",
      cell: ({ row }) => (
        <span className="text-[var(--muted)] text-sm">{row.original.codelist || "-"}</span>
      ),
    },
  ];

  const searchColumns: ColumnDef<CDISCSearchResult>[] = [
    {
      accessorKey: "domain",
      header: "Domain",
      cell: ({ row }) => (
        <button
          className="text-[var(--primary)] hover:underline font-medium"
          onClick={() => {
            setSelectedDomain(row.original.domain);
            setSelectedStandard(row.original.standard);
            setActiveTab(row.original.standard === "SDTM" ? "sdtm" : "adam");
          }}
        >
          {row.original.domain}
        </button>
      ),
    },
    {
      accessorKey: "name",
      header: "Variable",
      cell: ({ row }) => (
        <span className="font-mono font-medium">{row.original.name}</span>
      ),
    },
    {
      accessorKey: "label",
      header: "Label",
    },
    {
      accessorKey: "standard",
      header: "Standard",
      cell: ({ row }) => (
        <StatusBadge variant={row.original.standard === "SDTM" ? "primary" : "success"}>
          {row.original.standard}
        </StatusBadge>
      ),
    },
    {
      accessorKey: "core",
      header: "Core",
      cell: ({ row }) => {
        const core = row.original.core;
        if (!core) return "-";
        const variant = core === "Req" ? "destructive" : core === "Exp" ? "warning" : "default";
        return <StatusBadge variant={variant}>{core}</StatusBadge>;
      },
    },
  ];

  const tabs = [
    { id: "overview", label: "Overview", icon: BookOpen },
    { id: "sdtm", label: "SDTM IG", icon: Database },
    { id: "adam", label: "ADaM IG", icon: Layers },
    { id: "search", label: "Search", icon: Search },
  ] as const;

  // Filter domains by standard for SDTM/ADaM tabs
  const filteredDomains = domains?.filter(d => {
    if (activeTab === "sdtm") return d.standard === "SDTM";
    if (activeTab === "adam") return d.standard === "ADaM";
    return true;
  }) || [];

  return (
    <div className="space-y-5">
      {/* Page Header */}
      <div className="flex justify-between items-start">
        <div>
          <h1 className="text-2xl font-bold text-[var(--foreground)]">
            CDISC Standards Library
          </h1>
          <p className="text-[var(--foreground-muted)]">
            Browse SDTM IG 3.4 and ADaM IG 1.3 standard definitions
          </p>
        </div>
      </div>

      {/* Stats Overview */}
      {!statsLoading && stats && (
        <div className="grid grid-cols-4 gap-4">
          <div className="wp-box p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-[var(--primary)]/10 rounded">
                <Database className="w-5 h-5 text-[var(--primary)]" />
              </div>
              <div>
                <div className="text-2xl font-bold">{stats.total_domains}</div>
                <div className="text-sm text-[var(--muted)]">Total Domains</div>
              </div>
            </div>
          </div>
          <div className="wp-box p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-[var(--success)]/10 rounded">
                <FileText className="w-5 h-5 text-[var(--success)]" />
              </div>
              <div>
                <div className="text-2xl font-bold">{stats.total_variables}</div>
                <div className="text-sm text-[var(--muted)]">Total Variables</div>
              </div>
            </div>
          </div>
          <div className="wp-box p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-500/10 rounded">
                <Tag className="w-5 h-5 text-blue-500" />
              </div>
              <div>
                <div className="text-2xl font-bold">{stats.domains_by_standard?.SDTM || 0}</div>
                <div className="text-sm text-[var(--muted)]">SDTM Domains</div>
              </div>
            </div>
          </div>
          <div className="wp-box p-4">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-purple-500/10 rounded">
                <Layers className="w-5 h-5 text-purple-500" />
              </div>
              <div>
                <div className="text-2xl font-bold">{stats.domains_by_standard?.ADaM || 0}</div>
                <div className="text-sm text-[var(--muted)]">ADaM Datasets</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="tabs-list">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`tab-trigger ${activeTab === tab.id ? "active" : ""}`}
            onClick={() => {
              setActiveTab(tab.id);
              setSelectedDomain(null);
              if (tab.id === "sdtm") setSelectedStandard("SDTM");
              else if (tab.id === "adam") setSelectedStandard("ADaM");
              else setSelectedStandard(null);
            }}
          >
            <tab.icon className="w-4 h-4 mr-2 inline" />
            {tab.label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {activeTab === "overview" && (
        <div className="grid grid-cols-2 gap-6">
          <WPBox title="SDTM Implementation Guide v3.4">
            <div className="space-y-4">
              <p className="text-[var(--muted)]">
                Study Data Tabulation Model Implementation Guide for Human Clinical Trials
              </p>
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-[var(--muted)]/10 rounded">
                  <div className="text-xl font-bold">{stats?.domains_by_standard?.SDTM || 0}</div>
                  <div className="text-sm text-[var(--muted)]">Domains</div>
                </div>
                <div className="p-3 bg-[var(--muted)]/10 rounded">
                  <div className="text-xl font-bold">{stats?.variables_by_standard?.SDTM || 0}</div>
                  <div className="text-sm text-[var(--muted)]">Variables</div>
                </div>
              </div>
              <div className="text-sm text-[var(--muted)]">
                <strong>Domain Classes:</strong> Interventions, Events, Findings,
                Special-Purpose, Trial Design, Relationship
              </div>
              <button
                className="btn btn-primary btn-sm"
                onClick={() => {
                  setActiveTab("sdtm");
                  setSelectedStandard("SDTM");
                }}
              >
                Browse SDTM Domains
                <ChevronRight className="w-4 h-4 ml-1" />
              </button>
            </div>
          </WPBox>

          <WPBox title="ADaM Implementation Guide v1.3">
            <div className="space-y-4">
              <p className="text-[var(--muted)]">
                Analysis Data Model Implementation Guide for standard analysis datasets
              </p>
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-[var(--muted)]/10 rounded">
                  <div className="text-xl font-bold">{stats?.domains_by_standard?.ADaM || 0}</div>
                  <div className="text-sm text-[var(--muted)]">Datasets</div>
                </div>
                <div className="p-3 bg-[var(--muted)]/10 rounded">
                  <div className="text-xl font-bold">{stats?.variables_by_standard?.ADaM || 0}</div>
                  <div className="text-sm text-[var(--muted)]">Variables</div>
                </div>
              </div>
              <div className="text-sm text-[var(--muted)]">
                <strong>Data Structures:</strong> ADSL (Subject Level),
                BDS (Basic Data Structure), OCCDS (Occurrence)
              </div>
              <button
                className="btn btn-primary btn-sm"
                onClick={() => {
                  setActiveTab("adam");
                  setSelectedStandard("ADaM");
                }}
              >
                Browse ADaM Datasets
                <ChevronRight className="w-4 h-4 ml-1" />
              </button>
            </div>
          </WPBox>

          <WPBox title="About CDISC Standards" className="col-span-2">
            <div className="flex gap-4">
              <Info className="w-6 h-6 text-[var(--primary)] flex-shrink-0 mt-1" />
              <div className="text-sm text-[var(--muted)] space-y-2">
                <p>
                  The CDISC Standards Library contains standard domain and variable
                  definitions from the SDTM and ADaM Implementation Guides. This library
                  is used by the auto-approval engine to automatically approve variables
                  that match CDISC standards.
                </p>
                <p>
                  <strong>Core Designations:</strong>
                </p>
                <ul className="list-disc list-inside space-y-1">
                  <li><StatusBadge variant="destructive">Req</StatusBadge> - Required: Must be included if applicable</li>
                  <li><StatusBadge variant="warning">Exp</StatusBadge> - Expected: Should be included if applicable</li>
                  <li><StatusBadge variant="default">Perm</StatusBadge> - Permissible: May be included if applicable</li>
                </ul>
              </div>
            </div>
          </WPBox>
        </div>
      )}

      {(activeTab === "sdtm" || activeTab === "adam") && (
        <>
          {selectedDomain ? (
            <WPBox
              title={`${selectedDomain} Variables`}
              headerAction={
                <button
                  className="btn btn-sm btn-outline"
                  onClick={() => setSelectedDomain(null)}
                >
                  Back to Domains
                </button>
              }
            >
              {variablesLoading ? (
                <div className="flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
                </div>
              ) : (
                <DataTable
                  columns={variableColumns}
                  data={variables || []}
                  searchColumn="name"
                  searchPlaceholder="Search variables..."
                />
              )}
            </WPBox>
          ) : (
            <WPBox title={activeTab === "sdtm" ? "SDTM Domains" : "ADaM Datasets"}>
              {domainsLoading ? (
                <div className="flex items-center justify-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
                </div>
              ) : (
                <DataTable
                  columns={domainColumns}
                  data={filteredDomains}
                  searchColumn="name"
                  searchPlaceholder="Search domains..."
                />
              )}
            </WPBox>
          )}
        </>
      )}

      {activeTab === "search" && (
        <WPBox title="Search CDISC Variables">
          <div className="space-y-4">
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-[var(--muted)]" />
                <input
                  type="text"
                  className="w-full pl-10 pr-4 py-2 border border-[var(--border)] rounded bg-[var(--background)] text-[var(--foreground)]"
                  placeholder="Search by variable name or label..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                />
              </div>
            </div>

            {searchQuery.length < 2 ? (
              <div className="text-center py-8 text-[var(--muted)]">
                <Search className="w-12 h-12 mx-auto mb-4" />
                <p>Enter at least 2 characters to search</p>
              </div>
            ) : searchLoading ? (
              <div className="flex items-center justify-center py-8">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-[var(--primary)]"></div>
              </div>
            ) : searchResults && searchResults.length > 0 ? (
              <DataTable
                columns={searchColumns}
                data={searchResults}
                searchColumn="name"
                searchPlaceholder=""
              />
            ) : (
              <div className="text-center py-8 text-[var(--muted)]">
                <p>No variables found matching "{searchQuery}"</p>
              </div>
            )}
          </div>
        </WPBox>
      )}
    </div>
  );
}
