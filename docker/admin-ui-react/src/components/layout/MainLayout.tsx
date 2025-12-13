import { useState, useEffect, useRef } from "react";
import { Outlet, Link } from "react-router-dom";
import { Sidebar } from "./Sidebar";
import { useTheme } from "@/hooks/useTheme";
import { Menu, X, Search, Sun, Moon, Bell } from "lucide-react";
import { cn } from "@/lib/utils";

// Backdrop component for mobile
function Backdrop({
  isOpen,
  onClick,
}: {
  isOpen: boolean;
  onClick: () => void;
}) {
  return (
    <div
      className={cn(
        "fixed inset-0 z-40 bg-gray-900/50 xl:hidden transition-opacity duration-300",
        isOpen ? "opacity-100" : "opacity-0 pointer-events-none"
      )}
      onClick={onClick}
    />
  );
}

// Header component
function AppHeader({
  isMobileOpen,
  onToggleSidebar,
  onToggleMobileSidebar,
}: {
  isMobileOpen: boolean;
  onToggleSidebar: () => void;
  onToggleMobileSidebar: () => void;
}) {
  const { effectiveTheme, toggleTheme } = useTheme();
  const inputRef = useRef<HTMLInputElement>(null);

  const handleToggle = () => {
    if (window.innerWidth >= 1280) {
      onToggleSidebar();
    } else {
      onToggleMobileSidebar();
    }
  };

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
    <header className="sticky top-0 flex w-full bg-white border-gray-200 z-[99998] dark:border-gray-800 dark:bg-gray-900 xl:border-b">
      <div className="flex flex-col items-center justify-between grow xl:flex-row xl:px-6">
        <div className="flex items-center justify-between w-full gap-2 px-3 py-3 border-b border-gray-200 dark:border-gray-800 sm:gap-4 xl:justify-normal xl:border-b-0 xl:px-0 lg:py-4">
          {/* Sidebar Toggle Button */}
          <button
            className={cn(
              "items-center justify-center w-10 h-10 text-gray-500 border-gray-200 rounded-lg z-[99999] dark:border-gray-800 flex dark:text-gray-400 lg:h-11 lg:w-11 xl:border",
              isMobileOpen && "bg-gray-100 dark:bg-white/[0.03]"
            )}
            onClick={handleToggle}
            aria-label="Toggle Sidebar"
          >
            {isMobileOpen ? (
              <X className="w-6 h-6" />
            ) : (
              <Menu className="w-5 h-5" />
            )}
          </button>

          {/* Mobile Logo */}
          <Link to="/" className="xl:hidden flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-[var(--color-brand-500)] to-[var(--color-brand-600)] flex items-center justify-center">
              <span className="text-white font-bold text-sm">S</span>
            </div>
            <span className="font-semibold text-gray-900 dark:text-white">SAGE</span>
          </Link>

          {/* Spacer for mobile */}
          <div className="xl:hidden w-10" />

          {/* Desktop Search */}
          <div className="hidden xl:block">
            <div className="relative">
              <span className="absolute left-4 top-1/2 -translate-y-1/2 pointer-events-none">
                <Search className="w-5 h-5 text-gray-400 dark:text-gray-500" />
              </span>
              <input
                ref={inputRef}
                type="text"
                placeholder="Search or type command..."
                className="h-11 w-full rounded-lg border border-gray-200 bg-transparent py-2.5 pl-12 pr-14 text-sm text-gray-800 shadow-[var(--shadow-theme-xs)] placeholder:text-gray-400 focus:border-[var(--color-brand-300)] focus:outline-none focus:ring-[3px] focus:ring-[var(--color-brand-500)]/10 dark:border-gray-800 dark:bg-gray-900 dark:text-white/90 dark:placeholder:text-white/30 dark:focus:border-[var(--color-brand-800)] xl:w-[430px]"
              />
              <button className="absolute right-2.5 top-1/2 -translate-y-1/2 inline-flex items-center gap-0.5 rounded-lg border border-gray-200 bg-gray-50 px-[7px] py-[4.5px] text-xs tracking-tight text-gray-500 dark:border-gray-800 dark:bg-white/[0.03] dark:text-gray-400">
                <span>⌘</span>
                <span>K</span>
              </button>
            </div>
          </div>
        </div>

        {/* Right side controls */}
        <div className="hidden xl:flex items-center gap-3">
          {/* Theme Toggle */}
          <button
            onClick={toggleTheme}
            className="relative flex items-center justify-center text-gray-500 transition-colors bg-white border border-gray-200 rounded-full hover:text-gray-900 h-11 w-11 hover:bg-gray-100 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-white"
          >
            {effectiveTheme === "dark" ? (
              <Sun className="w-5 h-5" />
            ) : (
              <Moon className="w-5 h-5" />
            )}
          </button>

          {/* Notifications */}
          <button className="relative flex items-center justify-center text-gray-500 transition-colors bg-white border border-gray-200 rounded-full hover:text-gray-900 h-11 w-11 hover:bg-gray-100 dark:border-gray-800 dark:bg-gray-900 dark:text-gray-400 dark:hover:bg-gray-800 dark:hover:text-white">
            <Bell className="w-5 h-5" />
            <span className="absolute top-2 right-2 w-2 h-2 bg-[var(--color-error-500)] rounded-full" />
          </button>
        </div>
      </div>
    </header>
  );
}

export function MainLayout() {
  const [isExpanded, setIsExpanded] = useState(true);
  const [isMobileOpen, setIsMobileOpen] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const [isMobile, setIsMobile] = useState(false);

  // Handle window resize
  useEffect(() => {
    const handleResize = () => {
      const mobile = window.innerWidth < 1280;
      setIsMobile(mobile);
      if (!mobile) {
        setIsMobileOpen(false);
      }
    };

    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const toggleSidebar = () => setIsExpanded((prev) => !prev);
  const toggleMobileSidebar = () => setIsMobileOpen((prev) => !prev);

  const effectiveExpanded = isMobile ? false : isExpanded;

  return (
    <div className="min-h-screen xl:flex">
      <Sidebar
        isExpanded={effectiveExpanded}
        isMobileOpen={isMobileOpen}
        isHovered={isHovered}
        onHoverChange={setIsHovered}
        onMobileClose={() => setIsMobileOpen(false)}
      />
      <Backdrop isOpen={isMobileOpen} onClick={() => setIsMobileOpen(false)} />
      <div
        className={cn(
          "flex-1 transition-all duration-300 ease-in-out",
          effectiveExpanded || isHovered ? "xl:ml-[290px]" : "xl:ml-[90px]",
          isMobileOpen ? "ml-0" : ""
        )}
      >
        <AppHeader
          isMobileOpen={isMobileOpen}
          onToggleSidebar={toggleSidebar}
          onToggleMobileSidebar={toggleMobileSidebar}
        />
        <main className="p-4 mx-auto max-w-[1536px] md:p-6">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
