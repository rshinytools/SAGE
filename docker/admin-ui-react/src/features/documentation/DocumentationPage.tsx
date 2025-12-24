import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Search,
  Book,
  FileText,
  ChevronRight,
  Sparkles,
  Send,
  ArrowLeft,
  ExternalLink,
  Folder,
  Hash,
  Clock,
  X,
  Loader2,
  BookOpen,
  Shield,
  Database,
  Cog,
  Users,
  CheckCircle,
  AlertCircle,
} from "lucide-react";
import { WPBox } from "@/components/layout/WPBox";
import { docsApi, type CategoryInfo, type DocumentDetail, type SearchResult, type AskResponse } from "@/api/docs";
import { cn } from "@/lib/utils";
import { MarkdownRenderer } from "@/components/MarkdownRenderer";

// ============================================================================
// Category Icon Mapping
// ============================================================================

const categoryIcons: Record<string, React.ReactNode> = {
  "Getting Started": <BookOpen className="w-5 h-5" />,
  "Architecture": <Cog className="w-5 h-5" />,
  "Factories": <Database className="w-5 h-5" />,
  "Factory 1 - Data": <Database className="w-5 h-5" />,
  "Factory 2 - Metadata": <FileText className="w-5 h-5" />,
  "Factory 3 - Dictionary": <Book className="w-5 h-5" />,
  "Factory 3.5 - MedDRA": <Shield className="w-5 h-5" />,
  "Factory 4 - Engine": <Cog className="w-5 h-5" />,
  "User Guide": <Users className="w-5 h-5" />,
  "Admin Guide": <Shield className="w-5 h-5" />,
  "Compliance": <CheckCircle className="w-5 h-5" />,
  "overview": <Book className="w-5 h-5" />,
};

const categoryColors: Record<string, string> = {
  "Getting Started": "from-green-500 to-emerald-600",
  "Architecture": "from-purple-500 to-violet-600",
  "Factories": "from-blue-500 to-indigo-600",
  "Factory 1 - Data": "from-blue-500 to-indigo-600",
  "Factory 2 - Metadata": "from-cyan-500 to-teal-600",
  "Factory 3 - Dictionary": "from-orange-500 to-amber-600",
  "Factory 3.5 - MedDRA": "from-pink-500 to-rose-600",
  "Factory 4 - Engine": "from-indigo-500 to-purple-600",
  "User Guide": "from-teal-500 to-cyan-600",
  "Admin Guide": "from-red-500 to-rose-600",
  "Compliance": "from-emerald-500 to-green-600",
  "overview": "from-gray-500 to-slate-600",
};

// ============================================================================
// Component: Search Bar
// ============================================================================

interface SearchBarProps {
  value: string;
  onChange: (value: string) => void;
  onSearch: () => void;
  placeholder?: string;
  isLoading?: boolean;
}

function SearchBar({ value, onChange, onSearch, placeholder, isLoading }: SearchBarProps) {
  return (
    <div className="relative">
      <div className="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none">
        <Search className="h-5 w-5 text-gray-400" />
      </div>
      <input
        type="text"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && onSearch()}
        placeholder={placeholder || "Search documentation..."}
        className="block w-full pl-11 pr-12 py-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-xl text-gray-900 dark:text-white placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-[var(--color-brand-500)] focus:border-transparent transition-all"
      />
      {value && (
        <button
          onClick={() => onChange("")}
          className="absolute inset-y-0 right-12 flex items-center pr-2 text-gray-400 hover:text-gray-600"
        >
          <X className="h-4 w-4" />
        </button>
      )}
      <button
        onClick={onSearch}
        disabled={!value.trim() || isLoading}
        className="absolute inset-y-0 right-0 px-4 flex items-center text-[var(--color-brand-500)] hover:text-[var(--color-brand-600)] disabled:opacity-50 disabled:cursor-not-allowed"
      >
        {isLoading ? (
          <Loader2 className="h-5 w-5 animate-spin" />
        ) : (
          <Send className="h-5 w-5" />
        )}
      </button>
    </div>
  );
}

// ============================================================================
// Component: Category Card
// ============================================================================

interface CategoryCardProps {
  category: CategoryInfo;
  onClick: () => void;
}

function CategoryCard({ category, onClick }: CategoryCardProps) {
  const icon = categoryIcons[category.name] || <Folder className="w-5 h-5" />;
  const gradient = categoryColors[category.name] || "from-gray-500 to-slate-600";

  return (
    <button
      onClick={onClick}
      className="group relative overflow-hidden rounded-xl bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 p-6 text-left transition-all hover:shadow-lg hover:shadow-gray-200/50 dark:hover:shadow-gray-900/50 hover:border-[var(--color-brand-300)] dark:hover:border-[var(--color-brand-700)] hover:-translate-y-0.5"
    >
      <div className="flex items-start gap-4">
        <div className={cn("flex-shrink-0 p-3 rounded-xl bg-gradient-to-br text-white", gradient)}>
          {icon}
        </div>
        <div className="flex-1 min-w-0">
          <h3 className="font-semibold text-gray-900 dark:text-white group-hover:text-[var(--color-brand-600)] dark:group-hover:text-[var(--color-brand-400)] transition-colors">
            {category.name}
          </h3>
          <p className="mt-1 text-sm text-gray-500 dark:text-gray-400">
            {category.document_count} {category.document_count === 1 ? "document" : "documents"}
          </p>
        </div>
        <ChevronRight className="flex-shrink-0 w-5 h-5 text-gray-400 group-hover:text-[var(--color-brand-500)] group-hover:translate-x-1 transition-all" />
      </div>
      <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-white/10 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none" />
    </button>
  );
}

// ============================================================================
// Component: Document List Item
// ============================================================================

interface DocumentListItemProps {
  doc: { id: string; title: string; path: string; summary?: string };
  onClick: () => void;
}

function DocumentListItem({ doc, onClick }: DocumentListItemProps) {
  return (
    <button
      onClick={onClick}
      className="w-full p-4 text-left rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 hover:border-[var(--color-brand-300)] dark:hover:border-[var(--color-brand-700)] hover:shadow-md transition-all group"
    >
      <div className="flex items-start gap-3">
        <FileText className="flex-shrink-0 w-5 h-5 mt-0.5 text-gray-400 group-hover:text-[var(--color-brand-500)]" />
        <div className="flex-1 min-w-0">
          <h4 className="font-medium text-gray-900 dark:text-white group-hover:text-[var(--color-brand-600)] dark:group-hover:text-[var(--color-brand-400)]">
            {doc.title}
          </h4>
          {doc.summary && (
            <p className="mt-1 text-sm text-gray-500 dark:text-gray-400 line-clamp-2">
              {doc.summary}
            </p>
          )}
        </div>
        <ChevronRight className="flex-shrink-0 w-4 h-4 text-gray-400 group-hover:text-[var(--color-brand-500)] group-hover:translate-x-1 transition-all" />
      </div>
    </button>
  );
}

// ============================================================================
// Component: Search Result Item
// ============================================================================

interface SearchResultItemProps {
  result: SearchResult;
  onClick: () => void;
}

function SearchResultItem({ result, onClick }: SearchResultItemProps) {
  return (
    <button
      onClick={onClick}
      className="w-full p-5 text-left rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 hover:border-[var(--color-brand-300)] dark:hover:border-[var(--color-brand-700)] hover:shadow-lg transition-all group"
    >
      <div className="flex items-start gap-4">
        <div className="flex-shrink-0 p-2 rounded-lg bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-900)]/20 text-[var(--color-brand-600)] dark:text-[var(--color-brand-400)]">
          <FileText className="w-5 h-5" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <h4 className="font-semibold text-gray-900 dark:text-white group-hover:text-[var(--color-brand-600)] dark:group-hover:text-[var(--color-brand-400)]">
              {result.title}
            </h4>
            <span className="px-2 py-0.5 text-xs font-medium rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">
              {result.category}
            </span>
          </div>
          <p className="mt-2 text-sm text-gray-600 dark:text-gray-300 line-clamp-2">
            {result.summary}
          </p>
          {result.matched_keywords.length > 0 && (
            <div className="mt-3 flex flex-wrap gap-1.5">
              {result.matched_keywords.slice(0, 5).map((keyword) => (
                <span
                  key={keyword}
                  className="inline-flex items-center gap-1 px-2 py-0.5 text-xs rounded-md bg-[var(--color-brand-50)] dark:bg-[var(--color-brand-900)]/20 text-[var(--color-brand-700)] dark:text-[var(--color-brand-300)]"
                >
                  <Hash className="w-3 h-3" />
                  {keyword}
                </span>
              ))}
            </div>
          )}
        </div>
        <div className="flex-shrink-0 text-right">
          <div className="text-sm font-medium text-[var(--color-brand-600)] dark:text-[var(--color-brand-400)]">
            {Math.round(result.relevance_score * 10)}% match
          </div>
        </div>
      </div>
    </button>
  );
}

// ============================================================================
// Component: Ask Response Card
// ============================================================================

interface AskResponseCardProps {
  response: AskResponse;
  onSourceClick: (docId: string) => void;
}

function AskResponseCard({ response, onSourceClick }: AskResponseCardProps) {
  return (
    <div className="rounded-xl border border-[var(--color-brand-200)] dark:border-[var(--color-brand-800)] bg-gradient-to-br from-[var(--color-brand-50)] to-white dark:from-[var(--color-brand-900)]/20 dark:to-gray-800 p-6">
      <div className="flex items-start gap-3 mb-4">
        <div className="flex-shrink-0 p-2 rounded-lg bg-[var(--color-brand-500)] text-white">
          <Sparkles className="w-5 h-5" />
        </div>
        <div>
          <h3 className="font-semibold text-gray-900 dark:text-white">Answer</h3>
          <p className="text-sm text-gray-500 dark:text-gray-400">Based on SAGE documentation</p>
        </div>
      </div>
      <MarkdownRenderer
        content={response.answer}
        className="prose prose-sm dark:prose-invert max-w-none"
      />
      {response.sources.length > 0 && (
        <div className="mt-6 pt-4 border-t border-gray-200 dark:border-gray-700">
          <h4 className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-3">Sources</h4>
          <div className="flex flex-wrap gap-2">
            {response.sources.map((source) => (
              <button
                key={source.id}
                onClick={() => onSourceClick(source.path.replace(".md", ""))}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-lg bg-white dark:bg-gray-700 border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:border-[var(--color-brand-300)] dark:hover:border-[var(--color-brand-600)] hover:text-[var(--color-brand-600)] dark:hover:text-[var(--color-brand-400)] transition-colors"
              >
                <ExternalLink className="w-3.5 h-3.5" />
                {source.title}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ============================================================================
// Component: Document Viewer
// ============================================================================

interface DocumentViewerProps {
  document: DocumentDetail;
  onBack: () => void;
}

function DocumentViewer({ document, onBack }: DocumentViewerProps) {
  const { data: rawContent } = useQuery({
    queryKey: ["doc-content", document.path],
    queryFn: () => docsApi.getRawContent(document.path),
    staleTime: 5 * 60 * 1000,
  });

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center gap-4">
        <button
          onClick={onBack}
          className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
        >
          <ArrowLeft className="w-5 h-5 text-gray-600 dark:text-gray-400" />
        </button>
        <div>
          <div className="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
            <span>{document.category}</span>
            <ChevronRight className="w-4 h-4" />
          </div>
          <h1 className="text-2xl font-bold text-gray-900 dark:text-white">
            {document.title}
          </h1>
        </div>
      </div>

      {/* Metadata */}
      <div className="flex flex-wrap gap-4 text-sm text-gray-500 dark:text-gray-400">
        <div className="flex items-center gap-1.5">
          <Clock className="w-4 h-4" />
          <span>{document.word_count} words</span>
        </div>
        <div className="flex items-center gap-1.5">
          <FileText className="w-4 h-4" />
          <span>{document.sections.length} sections</span>
        </div>
      </div>

      {/* Keywords */}
      {document.keywords.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {document.keywords.slice(0, 10).map((keyword) => (
            <span
              key={keyword}
              className="px-2.5 py-1 text-xs rounded-full bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400"
            >
              {keyword}
            </span>
          ))}
        </div>
      )}

      {/* Content */}
      <WPBox>
        <div className="prose prose-lg dark:prose-invert max-w-none prose-headings:font-semibold prose-h2:text-xl prose-h3:text-lg prose-a:text-[var(--color-brand-600)] dark:prose-a:text-[var(--color-brand-400)] prose-pre:bg-gray-100 dark:prose-pre:bg-gray-800 prose-code:text-[var(--color-brand-600)] dark:prose-code:text-[var(--color-brand-400)]">
          {rawContent ? (
            <MarkdownRenderer content={rawContent.content} />
          ) : (
            <>
              {/* Summary with markdown rendering */}
              <MarkdownRenderer content={document.summary} className="lead" />
              {/* Sections with markdown rendering */}
              {document.sections.map((section, index) => (
                <div key={index}>
                  {section.level === 2 ? (
                    <h2>{section.heading}</h2>
                  ) : (
                    <h3>{section.heading}</h3>
                  )}
                  <MarkdownRenderer content={section.content} />
                </div>
              ))}
            </>
          )}
        </div>
      </WPBox>
    </div>
  );
}

// ============================================================================
// Main Component: Documentation Page
// ============================================================================

type ViewMode = "home" | "category" | "document" | "search" | "ask";

export function DocumentationPage() {
  const [viewMode, setViewMode] = useState<ViewMode>("home");
  const [searchQuery, setSearchQuery] = useState("");
  const [askQuery, setAskQuery] = useState("");
  const [selectedCategory, setSelectedCategory] = useState<CategoryInfo | null>(null);
  const [selectedDocId, setSelectedDocId] = useState<string | null>(null);

  // Queries
  const { data: categories, isLoading: categoriesLoading } = useQuery({
    queryKey: ["doc-categories"],
    queryFn: docsApi.getCategories,
    staleTime: 5 * 60 * 1000,
  });

  const { data: stats } = useQuery({
    queryKey: ["doc-stats"],
    queryFn: docsApi.getStats,
    staleTime: 5 * 60 * 1000,
  });

  const { data: searchResults, isLoading: searchLoading, refetch: doSearch } = useQuery({
    queryKey: ["doc-search", searchQuery],
    queryFn: () => docsApi.search(searchQuery),
    enabled: false,
  });

  const { data: askResponse, isLoading: askLoading, refetch: doAsk } = useQuery({
    queryKey: ["doc-ask", askQuery],
    queryFn: () => docsApi.ask(askQuery),
    enabled: false,
  });

  const { data: selectedDocument } = useQuery({
    queryKey: ["doc-detail", selectedDocId],
    queryFn: () => docsApi.getDocument(selectedDocId!),
    enabled: !!selectedDocId,
  });

  // Handlers
  const handleSearch = () => {
    if (searchQuery.trim()) {
      setViewMode("search");
      doSearch();
    }
  };

  const handleAsk = () => {
    if (askQuery.trim()) {
      setViewMode("ask");
      doAsk();
    }
  };

  const handleCategoryClick = (category: CategoryInfo) => {
    setSelectedCategory(category);
    setViewMode("category");
  };

  const handleDocumentClick = (docId: string) => {
    setSelectedDocId(docId);
    setViewMode("document");
  };

  const handleBack = () => {
    if (viewMode === "document" && selectedCategory) {
      setViewMode("category");
      setSelectedDocId(null);
    } else {
      setViewMode("home");
      setSelectedCategory(null);
      setSelectedDocId(null);
    }
  };

  // Render Document View
  if (viewMode === "document" && selectedDocument) {
    return (
      <div className="space-y-6">
        <DocumentViewer document={selectedDocument} onBack={handleBack} />
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* Hero Section */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-br from-[var(--color-brand-500)] via-[var(--color-brand-600)] to-indigo-700 p-8 md:p-12">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxwYXRoIGQ9Ik0zNiAxOGMzLjMxNCAwIDYgMi42ODYgNiA2cy0yLjY4NiA2LTYgNi02LTIuNjg2LTYtNiAyLjY4Ni02IDYtNiIgc3Ryb2tlPSJyZ2JhKDI1NSwyNTUsMjU1LDAuMSkiIHN0cm9rZS13aWR0aD0iMiIvPjwvZz48L3N2Zz4=')] opacity-30" />
        <div className="relative z-10 max-w-3xl">
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 rounded-lg bg-white/20">
              <Book className="w-6 h-6 text-white" />
            </div>
            <h1 className="text-3xl font-bold text-white">Documentation</h1>
          </div>
          <p className="text-lg text-white/80 mb-8">
            Explore SAGE documentation, learn about features, and get answers to your questions.
          </p>

          {/* Search / Ask Toggle */}
          <div className="bg-white/10 backdrop-blur-sm rounded-xl p-4 space-y-4">
            <div className="flex gap-2">
              <button
                onClick={() => {
                  setViewMode("home");
                  setSearchQuery("");
                }}
                className={cn(
                  "px-4 py-2 rounded-lg text-sm font-medium transition-colors",
                  viewMode !== "ask"
                    ? "bg-white text-[var(--color-brand-600)]"
                    : "text-white/80 hover:text-white hover:bg-white/10"
                )}
              >
                <Search className="w-4 h-4 inline mr-2" />
                Search Docs
              </button>
              <button
                onClick={() => setViewMode("ask")}
                className={cn(
                  "px-4 py-2 rounded-lg text-sm font-medium transition-colors",
                  viewMode === "ask"
                    ? "bg-white text-[var(--color-brand-600)]"
                    : "text-white/80 hover:text-white hover:bg-white/10"
                )}
              >
                <Sparkles className="w-4 h-4 inline mr-2" />
                Ask the System
              </button>
            </div>

            {viewMode === "ask" ? (
              <SearchBar
                value={askQuery}
                onChange={setAskQuery}
                onSearch={handleAsk}
                placeholder="Ask a question about SAGE... (e.g., 'How does SQL injection protection work?')"
                isLoading={askLoading}
              />
            ) : (
              <SearchBar
                value={searchQuery}
                onChange={setSearchQuery}
                onSearch={handleSearch}
                placeholder="Search documentation..."
                isLoading={searchLoading}
              />
            )}
          </div>
        </div>

        {/* Stats */}
        {stats && (
          <div className="relative z-10 mt-8 flex flex-wrap gap-6">
            <div className="text-white/80">
              <span className="text-2xl font-bold text-white">{stats.total_documents}</span>
              <span className="ml-2">Documents</span>
            </div>
            <div className="text-white/80">
              <span className="text-2xl font-bold text-white">{stats.total_categories}</span>
              <span className="ml-2">Categories</span>
            </div>
            <div className="text-white/80">
              <span className="text-2xl font-bold text-white">{stats.total_keywords}</span>
              <span className="ml-2">Keywords</span>
            </div>
          </div>
        )}
      </div>

      {/* Ask Response */}
      {viewMode === "ask" && askResponse && (
        <AskResponseCard response={askResponse} onSourceClick={handleDocumentClick} />
      )}

      {/* Search Results */}
      {viewMode === "search" && searchResults && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              Search Results
            </h2>
            <button
              onClick={() => {
                setViewMode("home");
                setSearchQuery("");
              }}
              className="text-sm text-[var(--color-brand-600)] dark:text-[var(--color-brand-400)] hover:underline"
            >
              Clear search
            </button>
          </div>
          {searchResults.results.length === 0 ? (
            <WPBox>
              <div className="text-center py-12">
                <AlertCircle className="w-12 h-12 mx-auto text-gray-400 mb-4" />
                <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-2">
                  No results found
                </h3>
                <p className="text-gray-500 dark:text-gray-400">
                  Try different keywords or browse categories below.
                </p>
              </div>
            </WPBox>
          ) : (
            <div className="space-y-3">
              {searchResults.results.map((result) => (
                <SearchResultItem
                  key={result.doc_id}
                  result={result}
                  onClick={() => handleDocumentClick(result.doc_id)}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Category View */}
      {viewMode === "category" && selectedCategory && (
        <div className="space-y-4">
          <div className="flex items-center gap-4">
            <button
              onClick={handleBack}
              className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
            >
              <ArrowLeft className="w-5 h-5 text-gray-600 dark:text-gray-400" />
            </button>
            <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
              {selectedCategory.name}
            </h2>
            <span className="text-sm text-gray-500 dark:text-gray-400">
              {selectedCategory.document_count} documents
            </span>
          </div>
          <div className="grid gap-3">
            {selectedCategory.documents.map((doc) => (
              <DocumentListItem
                key={doc.id}
                doc={doc}
                onClick={() => handleDocumentClick(doc.id)}
              />
            ))}
          </div>
        </div>
      )}

      {/* Categories Grid (Home View) */}
      {(viewMode === "home" || viewMode === "ask") && (
        <div className="space-y-4">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
            Browse by Category
          </h2>
          {categoriesLoading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 animate-spin text-[var(--color-brand-500)]" />
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {categories?.categories.map((category) => (
                <CategoryCard
                  key={category.name}
                  category={category}
                  onClick={() => handleCategoryClick(category)}
                />
              ))}
            </div>
          )}
        </div>
      )}

      {/* Quick Links */}
      {(viewMode === "home" || viewMode === "ask") && (
        <div className="space-y-4">
          <h2 className="text-xl font-semibold text-gray-900 dark:text-white">
            Popular Topics
          </h2>
          <div className="flex flex-wrap gap-2">
            {[
              "Getting Started",
              "Security",
              "Confidence Scores",
              "SQL Generation",
              "Fuzzy Matching",
              "Audit Trail",
              "GAMP 5",
              "MedDRA",
            ].map((topic) => (
              <button
                key={topic}
                onClick={() => {
                  setSearchQuery(topic);
                  handleSearch();
                }}
                className="px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-gray-700 dark:text-gray-300 hover:border-[var(--color-brand-300)] dark:hover:border-[var(--color-brand-700)] hover:text-[var(--color-brand-600)] dark:hover:text-[var(--color-brand-400)] transition-colors"
              >
                {topic}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default DocumentationPage;
