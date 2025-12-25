import { useEffect, useRef } from "react";
import { Outlet, Link } from "react-router-dom";
import { useAuth } from "@/hooks/useAuth";
import { useTheme } from "@/hooks/useTheme";
import { Sun, Moon, LogOut, LayoutDashboard, Users, ScrollText } from "lucide-react";

export function ChatLayout() {
  const { user, logout, hasPermission } = useAuth();
  const { effectiveTheme, toggleTheme } = useTheme();
  const inputRef = useRef<HTMLInputElement>(null);

  // Check permissions for navigation
  const isFullAdmin = hasPermission("*");
  const isUserAdmin = hasPermission("user_admin");

  // Cmd+K to focus search
  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "k") {
        event.preventDefault();
        inputRef.current?.focus();
      }
    };

    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, []);

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900">
      {/* Header */}
      <header className="sticky top-0 flex items-center justify-between h-16 px-4 md:px-6 bg-white border-b border-gray-200 dark:border-gray-800 dark:bg-gray-900 z-50">
        {/* Logo */}
        <Link to="/chat" className="flex items-center gap-3">
          <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-[var(--color-brand-500)] to-[var(--color-brand-600)] flex items-center justify-center">
            <span className="text-white font-bold text-base">S</span>
          </div>
          <div>
            <h1 className="text-lg font-semibold text-gray-900 dark:text-white leading-tight">
              SAGE Chat
            </h1>
            <p className="text-xs text-gray-500 dark:text-gray-400 hidden sm:block">
              AI Assistant
            </p>
          </div>
        </Link>

        {/* Right side controls */}
        <div className="flex items-center gap-3">
          {/* Navigation Links based on permissions */}
          {isFullAdmin ? (
            // Full Admin sees Admin Panel link
            <Link
              to="/dashboard"
              className="hidden sm:flex items-center gap-2 px-4 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
            >
              <LayoutDashboard className="w-4 h-4" />
              <span>Admin Panel</span>
            </Link>
          ) : isUserAdmin ? (
            // User Admin sees Users and Audit links
            <div className="hidden sm:flex items-center gap-1">
              <Link
                to="/users"
                className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
              >
                <Users className="w-4 h-4" />
                <span>Users</span>
              </Link>
              <Link
                to="/audit"
                className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded-lg transition-colors"
              >
                <ScrollText className="w-4 h-4" />
                <span>Audit</span>
              </Link>
            </div>
          ) : null}

          {/* Theme Toggle */}
          <button
            onClick={toggleTheme}
            className="flex items-center justify-center w-10 h-10 text-gray-500 transition-colors bg-white border border-gray-200 rounded-full hover:text-gray-900 hover:bg-gray-100 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-white"
          >
            {effectiveTheme === "dark" ? (
              <Sun className="w-5 h-5" />
            ) : (
              <Moon className="w-5 h-5" />
            )}
          </button>

          {/* User Menu */}
          <div className="flex items-center gap-3 pl-3 border-l border-gray-200 dark:border-gray-800">
            <div className="hidden sm:block text-right">
              <p className="text-sm font-medium text-gray-900 dark:text-white">
                {user?.username || "Guest"}
              </p>
              <p className="text-xs text-gray-500 dark:text-gray-400 capitalize">
                {user?.role || "user"}
              </p>
            </div>
            <div className="avatar avatar-md">
              <span className="avatar-initials">
                {user?.username?.charAt(0).toUpperCase() || "U"}
              </span>
            </div>
            <button
              onClick={() => logout()}
              className="flex items-center justify-center w-10 h-10 text-gray-500 hover:text-red-500 dark:text-gray-400 dark:hover:text-red-400 transition-colors"
              title="Sign Out"
            >
              <LogOut className="w-5 h-5" />
            </button>
          </div>
        </div>
      </header>

      {/* Main Content - Full Height Chat */}
      <main className="h-[calc(100vh-4rem)]">
        <Outlet />
      </main>
    </div>
  );
}
