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
import { SettingsPage } from "@/features/settings/SettingsPage";
import { ChatPage } from "@/features/chat/ChatPage";
import { CDISCLibraryPage } from "@/features/cdisc-library/CDISCLibraryPage";
import { GoldenSuitePage } from "@/features/golden-suite/GoldenSuitePage";
import { DocumentationPage } from "@/features/documentation/DocumentationPage";
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

// Component to handle root redirect based on user permissions
function RootRedirect() {
  const { hasPermission, isAuthenticated } = useAuthStore();

  if (!isAuthenticated) {
    return <Navigate to="/login" replace />;
  }

  // Admins go to dashboard, user_admin goes to users page, others go to chat
  if (hasPermission("*")) {
    return <Navigate to="/dashboard" replace />;
  } else if (hasPermission("user_admin")) {
    return <Navigate to="/users" replace />;
  } else {
    return <Navigate to="/chat" replace />;
  }
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

          {/* User Admin Routes - For user_admin permission (User Management + Audit) */}
          <Route
            element={
              <ProtectedRoute requiredPermission="user_admin">
                <MainLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/users" element={<UserManagementPage />} />
            <Route path="/audit" element={<AuditLogsPage />} />
          </Route>

          {/* Full Admin Routes - Only for users with * permission */}
          <Route
            element={
              <ProtectedRoute requiredPermission="*">
                <MainLayout />
              </ProtectedRoute>
            }
          >
            <Route path="/dashboard" element={<DashboardPage />} />
            <Route path="/documentation" element={<DocumentationPage />} />
            <Route path="/data-foundry" element={<DataFoundryPage />} />
            <Route path="/metadata" element={<MetadataAuditorPage />} />
            <Route path="/cdisc-library" element={<CDISCLibraryPage />} />
            <Route path="/dictionary" element={<DictionaryManagerPage />} />
            <Route path="/meddra" element={<MedDRALibraryPage />} />
            <Route path="/golden-suite" element={<GoldenSuitePage />} />
            <Route path="/settings" element={<SettingsPage />} />
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
