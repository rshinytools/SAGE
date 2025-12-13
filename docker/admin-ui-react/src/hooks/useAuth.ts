import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAuthStore } from "@/stores/authStore";
import { authApi } from "@/api/auth";
import type { LoginRequest } from "@/types/api";

export function useAuth() {
  const queryClient = useQueryClient();
  const { user, token, isAuthenticated, login, logout, hasPermission, isAdmin } = useAuthStore();

  const loginMutation = useMutation({
    mutationFn: (credentials: LoginRequest) => authApi.login(credentials),
    onSuccess: (data) => {
      login(data.access_token, data.user);
      queryClient.invalidateQueries({ queryKey: ["currentUser"] });
    },
  });

  const logoutMutation = useMutation({
    mutationFn: authApi.logout,
    onSuccess: () => {
      logout();
      queryClient.clear();
    },
    onError: () => {
      // Even if server logout fails, clear local state
      logout();
      queryClient.clear();
    },
  });

  const { data: currentUser, isLoading: isLoadingUser } = useQuery({
    queryKey: ["currentUser"],
    queryFn: authApi.getCurrentUser,
    enabled: isAuthenticated && !!token,
    retry: false,
    staleTime: 5 * 60 * 1000, // 5 minutes
  });

  const changePasswordMutation = useMutation({
    mutationFn: ({
      currentPassword,
      newPassword,
    }: {
      currentPassword: string;
      newPassword: string;
    }) => authApi.changePassword(currentPassword, newPassword),
  });

  return {
    user: currentUser || user,
    token,
    isAuthenticated,
    isLoading: loginMutation.isPending || isLoadingUser,
    error: loginMutation.error,
    login: loginMutation.mutate,
    logout: logoutMutation.mutate,
    changePassword: changePasswordMutation.mutate,
    hasPermission,
    isAdmin,
  };
}
