import { create } from "zustand";
import { persist } from "zustand/middleware";

interface User {
  username: string;
  email?: string;
  role: "admin" | "user" | "viewer";
  permissions: string[];
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (token: string, user: User) => void;
  logout: () => void;
  setLoading: (loading: boolean) => void;
  hasPermission: (permission: string) => boolean;
  isAdmin: () => boolean;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      user: null,
      token: null,
      isAuthenticated: false,
      isLoading: true,

      login: (token: string, user: User) => {
        // Also save token separately for chat API streaming
        localStorage.setItem("auth_token", token);
        set({
          token,
          user,
          isAuthenticated: true,
          isLoading: false,
        });
      },

      logout: () => {
        // Clear the separate token storage
        localStorage.removeItem("auth_token");
        set({
          token: null,
          user: null,
          isAuthenticated: false,
          isLoading: false,
        });
      },

      setLoading: (loading: boolean) => {
        set({ isLoading: loading });
      },

      hasPermission: (permission: string) => {
        const { user } = get();
        if (!user) return false;
        if (user.role === "admin") return true;
        return user.permissions.includes(permission);
      },

      isAdmin: () => {
        const { user } = get();
        return user?.role === "admin";
      },
    }),
    {
      name: "sage-auth",
      partialize: (state) => ({
        token: state.token,
        user: state.user,
        isAuthenticated: state.isAuthenticated,
      }),
      onRehydrateStorage: () => (state) => {
        if (state) {
          state.setLoading(false);
        }
      },
    }
  )
);
