import { Navigate, useLocation } from "react-router-dom";
import { useAuthStore } from "@/stores/authStore";

interface ProtectedRouteProps {
  children: React.ReactNode;
  requiredPermission?: string;
}

export function ProtectedRoute({ children, requiredPermission }: ProtectedRouteProps) {
  const location = useLocation();
  const { isAuthenticated, isLoading, hasPermission, isAdmin } = useAuthStore();

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center bg-gray-50 dark:bg-gray-900">
        <span className="spinner spinner-lg" />
      </div>
    );
  }

  if (!isAuthenticated) {
    return <Navigate to="/login" state={{ from: location }} replace />;
  }

  // Special handling for admin_access - requires admin role
  if (requiredPermission === "admin_access" && !isAdmin()) {
    // Non-admin users trying to access admin pages get redirected to chat
    return <Navigate to="/chat" replace />;
  }

  // Regular permission check
  if (requiredPermission && requiredPermission !== "admin_access" && !hasPermission(requiredPermission)) {
    return <Navigate to="/dashboard" replace />;
  }

  return <>{children}</>;
}
