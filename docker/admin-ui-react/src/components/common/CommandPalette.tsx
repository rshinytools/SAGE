import { useState, useEffect, useCallback, useRef } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import {
  Search,
  MessageSquare,
  LayoutDashboard,
  Database,
  FileSpreadsheet,
  BookOpen,
  Users,
  FileText,
  FolderKanban,
  Settings,
  Sun,
  Moon,
  LogOut,
  Command,
} from "lucide-react";
import { useTheme } from "@/hooks/useTheme";
import { useAuth } from "@/hooks/useAuth";

interface CommandItem {
  id: string;
  label: string;
  description?: string;
  icon: React.ReactNode;
  action: () => void;
  keywords?: string[];
  category: "navigation" | "action" | "settings";
}

export function CommandPalette() {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const navigate = useNavigate();
  const location = useLocation();
  const { effectiveTheme, toggleTheme } = useTheme();
  const { logout, hasPermission } = useAuth();

  const isAdmin = hasPermission("admin_access") || hasPermission("manage_users");

  // Define all commands
  const commands: CommandItem[] = [
    // Navigation
    {
      id: "chat",
      label: "Chat",
      description: "AI chat assistant",
      icon: <MessageSquare className="w-4 h-4" />,
      action: () => navigate("/chat"),
      keywords: ["ai", "assistant", "query", "ask"],
      category: "navigation",
    },
    ...(isAdmin
      ? [
          {
            id: "dashboard",
            label: "Dashboard",
            description: "Overview and metrics",
            icon: <LayoutDashboard className="w-4 h-4" />,
            action: () => navigate("/dashboard"),
            keywords: ["home", "overview", "metrics"],
            category: "navigation" as const,
          },
          {
            id: "data",
            label: "Data Management",
            description: "Upload and manage data files",
            icon: <Database className="w-4 h-4" />,
            action: () => navigate("/data"),
            keywords: ["upload", "sas", "files", "parquet"],
            category: "navigation" as const,
          },
          {
            id: "metadata",
            label: "Metadata Auditor",
            description: "Review and approve metadata",
            icon: <FileSpreadsheet className="w-4 h-4" />,
            action: () => navigate("/metadata"),
            keywords: ["variables", "review", "approve"],
            category: "navigation" as const,
          },
          {
            id: "dictionary",
            label: "Dictionary Manager",
            description: "Fuzzy matching and synonyms",
            icon: <BookOpen className="w-4 h-4" />,
            action: () => navigate("/dictionary"),
            keywords: ["fuzzy", "synonyms", "matching"],
            category: "navigation" as const,
          },
          {
            id: "users",
            label: "User Management",
            description: "Manage users and roles",
            icon: <Users className="w-4 h-4" />,
            action: () => navigate("/users"),
            keywords: ["admin", "roles", "permissions"],
            category: "navigation" as const,
          },
          {
            id: "audit",
            label: "Audit Logs",
            description: "View query history",
            icon: <FileText className="w-4 h-4" />,
            action: () => navigate("/audit"),
            keywords: ["logs", "history", "queries"],
            category: "navigation" as const,
          },
          {
            id: "tracker",
            label: "Project Tracker",
            description: "Track implementation progress",
            icon: <FolderKanban className="w-4 h-4" />,
            action: () => navigate("/tracker"),
            keywords: ["tasks", "progress", "phases"],
            category: "navigation" as const,
          },
          {
            id: "settings",
            label: "Settings",
            description: "System configuration",
            icon: <Settings className="w-4 h-4" />,
            action: () => navigate("/settings"),
            keywords: ["config", "configuration", "system"],
            category: "navigation" as const,
          },
        ]
      : []),
    // Actions
    {
      id: "toggle-theme",
      label: effectiveTheme === "dark" ? "Switch to Light Mode" : "Switch to Dark Mode",
      description: "Toggle dark/light theme",
      icon: effectiveTheme === "dark" ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />,
      action: () => {
        toggleTheme();
        setIsOpen(false);
      },
      keywords: ["theme", "dark", "light", "mode"],
      category: "settings",
    },
    {
      id: "logout",
      label: "Sign Out",
      description: "Log out of your account",
      icon: <LogOut className="w-4 h-4" />,
      action: () => {
        logout();
        setIsOpen(false);
      },
      keywords: ["logout", "exit", "sign out"],
      category: "action",
    },
  ];

  // Filter commands based on search
  const filteredCommands = commands.filter((cmd) => {
    if (!search) return true;
    const searchLower = search.toLowerCase();
    return (
      cmd.label.toLowerCase().includes(searchLower) ||
      cmd.description?.toLowerCase().includes(searchLower) ||
      cmd.keywords?.some((k) => k.toLowerCase().includes(searchLower))
    );
  });

  // Group commands by category
  const groupedCommands = {
    navigation: filteredCommands.filter((c) => c.category === "navigation"),
    action: filteredCommands.filter((c) => c.category === "action"),
    settings: filteredCommands.filter((c) => c.category === "settings"),
  };

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Open palette with Cmd+K or Ctrl+K
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setIsOpen((prev) => !prev);
        setSearch("");
        setSelectedIndex(0);
      }

      // Close with Escape
      if (e.key === "Escape" && isOpen) {
        setIsOpen(false);
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [isOpen]);

  // Focus input when opened
  useEffect(() => {
    if (isOpen) {
      inputRef.current?.focus();
    }
  }, [isOpen]);

  // Handle keyboard navigation within palette
  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setSelectedIndex((prev) => Math.min(prev + 1, filteredCommands.length - 1));
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        setSelectedIndex((prev) => Math.max(prev - 1, 0));
      } else if (e.key === "Enter") {
        e.preventDefault();
        if (filteredCommands[selectedIndex]) {
          filteredCommands[selectedIndex].action();
          setIsOpen(false);
        }
      }
    },
    [filteredCommands, selectedIndex]
  );

  // Reset selected index when search changes
  useEffect(() => {
    setSelectedIndex(0);
  }, [search]);

  // Close on route change
  useEffect(() => {
    setIsOpen(false);
  }, [location.pathname]);

  if (!isOpen) return null;

  return (
    <>
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50"
        onClick={() => setIsOpen(false)}
      />

      {/* Palette */}
      <div className="fixed top-[20%] left-1/2 -translate-x-1/2 w-full max-w-xl z-50">
        <div className="bg-white dark:bg-gray-900 rounded-xl shadow-2xl border border-gray-200 dark:border-gray-800 overflow-hidden">
          {/* Search Input */}
          <div className="flex items-center gap-3 px-4 py-3 border-b border-gray-200 dark:border-gray-800">
            <Search className="w-5 h-5 text-gray-400" />
            <input
              ref={inputRef}
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Search commands..."
              className="flex-1 bg-transparent text-gray-900 dark:text-white placeholder-gray-400 outline-none text-sm"
            />
            <kbd className="hidden sm:inline-flex items-center gap-1 px-2 py-1 text-xs font-medium text-gray-400 bg-gray-100 dark:bg-gray-800 rounded">
              <Command className="w-3 h-3" />K
            </kbd>
          </div>

          {/* Results */}
          <div className="max-h-80 overflow-y-auto py-2">
            {filteredCommands.length === 0 ? (
              <div className="px-4 py-8 text-center text-gray-500 text-sm">
                No commands found
              </div>
            ) : (
              <>
                {groupedCommands.navigation.length > 0 && (
                  <div>
                    <div className="px-4 py-1.5 text-xs font-medium text-gray-400 uppercase">
                      Navigation
                    </div>
                    {groupedCommands.navigation.map((cmd) => {
                      const globalIndex = filteredCommands.indexOf(cmd);
                      return (
                        <button
                          key={cmd.id}
                          onClick={() => {
                            cmd.action();
                            setIsOpen(false);
                          }}
                          className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                            globalIndex === selectedIndex
                              ? "bg-blue-50 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400"
                              : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800"
                          }`}
                        >
                          <span className="text-gray-400">{cmd.icon}</span>
                          <div className="flex-1 min-w-0">
                            <div className="text-sm font-medium">{cmd.label}</div>
                            {cmd.description && (
                              <div className="text-xs text-gray-500 truncate">
                                {cmd.description}
                              </div>
                            )}
                          </div>
                        </button>
                      );
                    })}
                  </div>
                )}

                {groupedCommands.settings.length > 0 && (
                  <div>
                    <div className="px-4 py-1.5 text-xs font-medium text-gray-400 uppercase mt-2">
                      Settings
                    </div>
                    {groupedCommands.settings.map((cmd) => {
                      const globalIndex = filteredCommands.indexOf(cmd);
                      return (
                        <button
                          key={cmd.id}
                          onClick={() => {
                            cmd.action();
                            setIsOpen(false);
                          }}
                          className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                            globalIndex === selectedIndex
                              ? "bg-blue-50 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400"
                              : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800"
                          }`}
                        >
                          <span className="text-gray-400">{cmd.icon}</span>
                          <div className="flex-1 min-w-0">
                            <div className="text-sm font-medium">{cmd.label}</div>
                            {cmd.description && (
                              <div className="text-xs text-gray-500 truncate">
                                {cmd.description}
                              </div>
                            )}
                          </div>
                        </button>
                      );
                    })}
                  </div>
                )}

                {groupedCommands.action.length > 0 && (
                  <div>
                    <div className="px-4 py-1.5 text-xs font-medium text-gray-400 uppercase mt-2">
                      Actions
                    </div>
                    {groupedCommands.action.map((cmd) => {
                      const globalIndex = filteredCommands.indexOf(cmd);
                      return (
                        <button
                          key={cmd.id}
                          onClick={() => {
                            cmd.action();
                            setIsOpen(false);
                          }}
                          className={`w-full flex items-center gap-3 px-4 py-2.5 text-left transition-colors ${
                            globalIndex === selectedIndex
                              ? "bg-blue-50 dark:bg-blue-500/10 text-blue-600 dark:text-blue-400"
                              : "text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800"
                          }`}
                        >
                          <span className="text-gray-400">{cmd.icon}</span>
                          <div className="flex-1 min-w-0">
                            <div className="text-sm font-medium">{cmd.label}</div>
                            {cmd.description && (
                              <div className="text-xs text-gray-500 truncate">
                                {cmd.description}
                              </div>
                            )}
                          </div>
                        </button>
                      );
                    })}
                  </div>
                )}
              </>
            )}
          </div>

          {/* Footer */}
          <div className="px-4 py-2 border-t border-gray-200 dark:border-gray-800 flex items-center gap-4 text-xs text-gray-400">
            <span className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 bg-gray-100 dark:bg-gray-800 rounded">↑↓</kbd>
              navigate
            </span>
            <span className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 bg-gray-100 dark:bg-gray-800 rounded">↵</kbd>
              select
            </span>
            <span className="flex items-center gap-1">
              <kbd className="px-1.5 py-0.5 bg-gray-100 dark:bg-gray-800 rounded">esc</kbd>
              close
            </span>
          </div>
        </div>
      </div>
    </>
  );
}
