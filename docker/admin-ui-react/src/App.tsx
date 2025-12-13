import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { MainLayout } from "@/components/layout/MainLayout";
import { ChatLayout } from "@/components/layout/ChatLayout";
import { ProtectedRoute } from "@/components/common/ProtectedRoute";
import { CommandPalette } from "@/components/common/CommandPalette";
import { ErrorBoundary } from "@/components/common/ErrorBoundary";
import { ToastProvider } from "@/components/common/Toast";

import { LoginPage } from "@/features/auth/LoginPage";
import { DashboardPage } from "@/features/dashboard/DashboardPage";
import { DataFoundryPage } from "@/features/data-foundry/DataFoundryPage";
import { MetadataAuditorPage } from "@/features/metadata-auditor/MetadataAuditorPage";
import { DictionaryManagerPage } from "@/features/dictionary-manager/DictionaryManagerPage";
import { MedDRALibraryPage } from "@/features/meddra-library/MedDRALibraryPage";
import { UserManagementPage } from "@/features/user-management/UserManagementPage";
import { AuditLogsPage } from "@/features/audit-logs/AuditLogsPage";
import { ProjectTrackerPage } from "@/features/project-tracker/ProjectTrackerPage";
import { SettingsPage } from "@/features/settings/SettingsPage";
import { ChatPage } from "@/features/chat/ChatPage";
import { CDISCLibraryPage } from "@/features/cdisc-library/CDISCLibraryPage";
import { useAuthStore } from "@/stores/authStore";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 30000, // 30 seconds
    },
  },
});

// Component to handle root redirect based on user role
function RootRedirect() {
  const { isAdmin, isAuthenticated } = useAuthStore();

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  // Admin users go to dashboard, regular users go to chat
  return <Navigate to={isAdmin() ? "/dashboard" : "/chat"} replace />;
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <ToastProvider>
        <ErrorBoundary>
          <CommandPalette />
          <Routes>
          {/* Public Routes */}
          <Route path="/login" element={<LoginPage />} />

          {/* Chat Route - Available to ALL authenticated users */}
          <Route
            element={
              <ProtectedRoute>
                <ChatLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/chat" element={<ChatPage />} />
          </Route>

          {/* Admin Routes - Only for admin users */}
          <Route
            element={
              <ProtectedRoute requiredPermission="admin_access">
                <MainLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/data-foundry" element={<DataFoundryPage />} />
            <Route path="/metadata" element={<MetadataAuditorPage />} />
            <Route path="/cdisc-library" element={<CDISCLibraryPage />} />
            <Route path="/dictionary" element={<DictionaryManagerPage />} />
            <Route path="/meddra" element={<MedDRALibraryPage />} />
            <Route
              path="/users"
              element={
                <ProtectedRoute requiredPermission="manage_users">
                  <UserManagementPage />
                </ProtectedRoute>
              }
            />
            <Route
              path="/audit"
              element={
                <ProtectedRoute requiredPermission="view_audit">
                  <AuditLogsPage />
                </ProtectedRoute>
              }
            />
            <Route path="/tracker" element={<ProjectTrackerPage />} />
            <Route
              path="/settings"
              element={
                <ProtectedRoute requiredPermission="manage_settings">
                  <SettingsPage />
                </ProtectedRoute>
              }
            />
          </Route>

          {/* Root redirect based on role */}
          <Route path="/" element={<RootRedirect />} />
          <Route path="*" element={<RootRedirect />} />
        </Routes>
        </ErrorBoundary>
        </ToastProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
