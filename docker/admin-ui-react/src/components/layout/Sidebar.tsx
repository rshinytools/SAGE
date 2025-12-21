import { useCallback, useEffect, useRef, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import {
  LayoutDashboard,
  MessageSquare,
  Factory,
  FileSearch,
  BookOpen,
  Library,
  Users,
  ScrollText,
  ClipboardList,
  Settings,
  LogOut,
  Sun,
  Moon,
  ChevronDown,
  MoreHorizontal,
  Pill,
  FlaskConical,
} from "lucide-react";
import { useAuth } from "@/hooks/useAuth";
import { useTheme } from "@/hooks/useTheme";
import { cn } from "@/lib/utils";

type NavItem = {
  path?: string;
  label: string;
  icon: React.ElementType;
  permission?: string;
  subItems?: { path: string; label: string }[];
};

const menuItems: NavItem[] = [
  { path: "/dashboard", label: "Dashboard", icon: LayoutDashboard },
  { path: "/chat", label: "AI Chat", icon: MessageSquare },
  { path: "/data-foundry", label: "Data Foundry", icon: Factory },
  { path: "/metadata", label: "Metadata Auditor", icon: FileSearch },
  { path: "/cdisc-library", label: "CDISC Library", icon: Library },
  { path: "/dictionary", label: "Dictionary", icon: BookOpen },
  { path: "/meddra", label: "MedDRA Library", icon: Pill },
  { path: "/golden-suite", label: "Golden Test Suite", icon: FlaskConical },
  { path: "/users", label: "Users", icon: Users, permission: "manage_users" },
  { path: "/audit", label: "Audit Logs", icon: ScrollText, permission: "view_audit" },
  { path: "/tracker", label: "Project Tracker", icon: ClipboardList },
  { path: "/settings", label: "Settings", icon: Settings, permission: "manage_settings" },
];

interface SidebarProps {
  isExpanded: boolean;
  isMobileOpen: boolean;
  isHovered: boolean;
  onHoverChange: (hovered: boolean) => void;
  onMobileClose: () => void;
}

export function Sidebar({
  isExpanded,
  isMobileOpen,
  isHovered,
  onHoverChange,
  onMobileClose,
}: SidebarProps) {
  const { user, logout, hasPermission } = useAuth();
  const { effectiveTheme, toggleTheme } = useTheme();
  const location = useLocation();

  const [openSubmenu, setOpenSubmenu] = useState<number | null>(null);
  const [subMenuHeight, setSubMenuHeight] = useState<Record<number, number>>({});
  const subMenuRefs = useRef<Record<number, HTMLDivElement | null>>({});

  const isActive = useCallback(
    (path: string) => location.pathname === path,
    [location.pathname]
  );

  // Close mobile sidebar on route change
  useEffect(() => {
    if (isMobileOpen) {
      onMobileClose();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.pathname]);

  // Update submenu heights
  useEffect(() => {
    if (openSubmenu !== null && subMenuRefs.current[openSubmenu]) {
      setSubMenuHeight((prev) => ({
        ...prev,
        [openSubmenu]: subMenuRefs.current[openSubmenu]?.scrollHeight || 0,
      }));
    }
  }, [openSubmenu]);

  const filteredMenuItems = menuItems.filter(
    (item) => !item.permission || hasPermission(item.permission)
  );

  const handleSubmenuToggle = (index: number) => {
    setOpenSubmenu((prev) => (prev === index ? null : index));
  };

  const showLabels = isExpanded || isHovered || isMobileOpen;

  return (
    <aside
      className={cn(
        "fixed flex flex-col top-0 left-0 h-screen transition-all duration-300 ease-in-out z-50",
        "bg-white dark:bg-gray-900 border-r border-gray-200 dark:border-gray-800",
        isExpanded || isMobileOpen
          ? "w-[290px]"
          : isHovered
          ? "w-[290px]"
          : "w-[90px]",
        isMobileOpen ? "translate-x-0" : "-translate-x-full",
        "xl:translate-x-0"
      )}
      onMouseEnter={() => !isExpanded && onHoverChange(true)}
      onMouseLeave={() => onHoverChange(false)}
    >
      {/* Logo */}
      <div
        className={cn(
          "py-8 px-5 flex",
          !showLabels ? "xl:justify-center" : "justify-start"
        )}
      >
        <NavLink to="/" className="flex items-center gap-3">
          {showLabels ? (
            <>
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-[var(--color-brand-500)] to-[var(--color-brand-600)] flex items-center justify-center">
                <span className="text-white font-bold text-lg">S</span>
              </div>
              <div>
                <h1 className="text-lg font-semibold text-gray-900 dark:text-white">
                  SAGE
                </h1>
                <p className="text-xs text-gray-500 dark:text-gray-400">
                  Study Analytics
                </p>
              </div>
            </>
          ) : (
            <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-[var(--color-brand-500)] to-[var(--color-brand-600)] flex items-center justify-center">
              <span className="text-white font-bold text-lg">S</span>
            </div>
          )}
        </NavLink>
      </div>

      {/* Navigation */}
      <div className="flex-1 flex flex-col overflow-y-auto duration-300 ease-linear no-scrollbar px-5">
        <nav className="mb-6">
          <div className="flex flex-col gap-4">
            {/* Menu Section */}
            <div>
              <h2
                className={cn(
                  "mb-4 text-xs uppercase flex leading-5 text-gray-400 font-medium tracking-wider",
                  !showLabels ? "xl:justify-center" : "justify-start"
                )}
              >
                {showLabels ? (
                  "Menu"
                ) : (
                  <MoreHorizontal className="w-6 h-6" />
                )}
              </h2>
              <ul className="flex flex-col gap-1">
                {filteredMenuItems.map((nav, index) => (
                  <li key={nav.label}>
                    {nav.subItems ? (
                      <>
                        <button
                          onClick={() => handleSubmenuToggle(index)}
                          className={cn(
                            "menu-item group w-full cursor-pointer",
                            openSubmenu === index
                              ? "menu-item-active"
                              : "menu-item-inactive",
                            !showLabels && "xl:justify-center"
                          )}
                        >
                          <span
                            className={cn(
                              "menu-item-icon-size",
                              openSubmenu === index
                                ? "menu-item-icon-active"
                                : "menu-item-icon-inactive"
                            )}
                          >
                            <nav.icon className="w-6 h-6" />
                          </span>
                          {showLabels && (
                            <>
                              <span className="menu-item-text">{nav.label}</span>
                              <ChevronDown
                                className={cn(
                                  "ml-auto w-5 h-5 transition-transform duration-200",
                                  openSubmenu === index && "rotate-180 text-white"
                                )}
                              />
                            </>
                          )}
                        </button>
                        {nav.subItems && showLabels && (
                          <div
                            ref={(el) => {
                              subMenuRefs.current[index] = el;
                            }}
                            className="overflow-hidden transition-all duration-300"
                            style={{
                              height:
                                openSubmenu === index
                                  ? `${subMenuHeight[index]}px`
                                  : "0px",
                            }}
                          >
                            <ul className="mt-2 space-y-1 ml-9">
                              {nav.subItems.map((subItem) => (
                                <li key={subItem.path}>
                                  <NavLink
                                    to={subItem.path}
                                    className={cn(
                                      "menu-dropdown-item",
                                      isActive(subItem.path)
                                        ? "menu-dropdown-item-active"
                                        : "menu-dropdown-item-inactive"
                                    )}
                                  >
                                    {subItem.label}
                                  </NavLink>
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </>
                    ) : (
                      nav.path && (
                        <NavLink
                          to={nav.path}
                          className={({ isActive: active }) =>
                            cn(
                              "menu-item group",
                              active ? "menu-item-active" : "menu-item-inactive",
                              !showLabels && "xl:justify-center"
                            )
                          }
                        >
                          {({ isActive: active }) => (
                            <>
                              <span
                                className={cn(
                                  "menu-item-icon-size",
                                  active
                                    ? "menu-item-icon-active"
                                    : "menu-item-icon-inactive"
                                )}
                              >
                                <nav.icon className="w-6 h-6" />
                              </span>
                              {showLabels && (
                                <span className="menu-item-text">{nav.label}</span>
                              )}
                            </>
                          )}
                        </NavLink>
                      )
                    )}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </nav>

        {/* Footer Widget */}
        {showLabels && (
          <div className="mt-auto mb-6">
            {/* Theme Toggle */}
            <button
              onClick={toggleTheme}
              className="menu-item menu-item-inactive w-full justify-start"
            >
              <span className="menu-item-icon-size menu-item-icon-inactive">
                {effectiveTheme === "dark" ? (
                  <Sun className="w-6 h-6" />
                ) : (
                  <Moon className="w-6 h-6" />
                )}
              </span>
              <span className="menu-item-text">
                {effectiveTheme === "dark" ? "Light Mode" : "Dark Mode"}
              </span>
            </button>

            {/* User Info */}
            <div className="mt-4 p-4 rounded-xl bg-gray-50 dark:bg-white/[0.03] border border-gray-100 dark:border-gray-800">
              <div className="flex items-center gap-3">
                <div className="avatar avatar-md">
                  <span className="avatar-initials">
                    {user?.username?.charAt(0).toUpperCase() || "U"}
                  </span>
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900 dark:text-white truncate">
                    {user?.username || "Guest"}
                  </p>
                  <p className="text-xs text-gray-500 dark:text-gray-400 capitalize">
                    {user?.role || "user"}
                  </p>
                </div>
              </div>
              <button
                onClick={() => logout()}
                className="mt-3 w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium text-red-600 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-500/10 rounded-lg transition-colors"
              >
                <LogOut className="w-4 h-4" />
                <span>Sign Out</span>
              </button>
            </div>
          </div>
        )}
      </div>
    </aside>
  );
}

// Simple sidebar for backward compatibility
export function SimpleSidebar() {
  const [isExpanded] = useState(true);
  const [isMobileOpen, setIsMobileOpen] = useState(false);
  const [isHovered, setIsHovered] = useState(false);

  return (
    <Sidebar
      isExpanded={isExpanded}
      isMobileOpen={isMobileOpen}
      isHovered={isHovered}
      onHoverChange={setIsHovered}
      onMobileClose={() => setIsMobileOpen(false)}
    />
  );
}
