import { Navigate, useLocation } from "react-router-dom";
import { useAuthStore } from "@/stores/authStore";

interface ProtectedRouteProps {
  children: React.ReactNode;
  requiredPermission?: string;
}

export function ProtectedRoute({ children, requiredPermission }: ProtectedRouteProps) {
  const location = useLocation();
  const { isAuthenticated, isLoading, hasPermission } = useAuthStore();

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

  // Check if user has the required permission
  // hasPermission will return true if user has "*" (superuser) or the specific permission
  if (requiredPermission && !hasPermission(requiredPermission)) {
    // Users without required permission get redirected to chat
    return <Navigate to="/chat" replace />;
  }

  return <>{children}</>;
}
