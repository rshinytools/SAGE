import apiClient from "./client";
import type { LoginRequest, LoginResponse, User } from "@/types/api";

// API response wrapper type
interface ApiLoginResponse {
  success: boolean;
  data: {
    access_token: string;
    token_type: string;
    expires_in: number;
    refresh_token: string;
  };
  meta: {
    timestamp: string;
  };
}

// User info response from /auth/me
interface MeResponse {
  success: boolean;
  data: {
    id?: string;
    username: string;
    email?: string;
    roles: string[];
    permissions: string[];
    last_login: string | null;
    must_change_password?: boolean;
  };
  meta: {
    timestamp: string;
  };
}

export const authApi = {
  login: async (credentials: LoginRequest): Promise<LoginResponse> => {
    const formData = new URLSearchParams();
    formData.append("username", credentials.username);
    formData.append("password", credentials.password);

    const response = await apiClient.post<ApiLoginResponse>("/auth/token", formData, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
      },
    });

    // Store token temporarily to fetch user data
    const token = response.data.data.access_token;

    // Fetch actual user data from /auth/me
    const meResponse = await apiClient.get<MeResponse>("/auth/me", {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    });

    const userData = meResponse.data.data;
    const user: User = {
      username: userData.username,
      email: userData.email,
      role: userData.roles.includes("admin") ? "admin" :
            userData.roles.includes("user") ? "user" : "viewer",
      permissions: userData.permissions || [],
    };

    return {
      access_token: token,
      token_type: response.data.data.token_type,
      user,
    };
  },

  getCurrentUser: async (): Promise<User> => {
    const response = await apiClient.get<MeResponse>("/auth/me");

    const userData = response.data.data;
    // Transform API response to User type with actual permissions
    return {
      username: userData.username,
      email: userData.email,
      role: userData.roles.includes("admin") ? "admin" :
            userData.roles.includes("user") ? "user" : "viewer",
      permissions: userData.permissions || [],
    };
  },

  logout: async (): Promise<void> => {
    await apiClient.post("/auth/logout");
  },

  refreshToken: async (): Promise<{ access_token: string }> => {
    // Backend wraps response in {success, data, meta}
    interface RefreshResponse {
      success: boolean;
      data: {
        access_token: string;
        token_type: string;
        expires_in: number;
      };
    }
    const response = await apiClient.post<RefreshResponse>("/auth/refresh");
    return { access_token: response.data.data.access_token };
  },

  changePassword: async (currentPassword: string, newPassword: string): Promise<void> => {
    // Backend uses PUT /auth/password with old_password and new_password query params
    await apiClient.put("/auth/password", null, {
      params: {
        old_password: currentPassword,
        new_password: newPassword,
      },
    });
  },
};
