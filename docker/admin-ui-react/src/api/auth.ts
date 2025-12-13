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

    // Transform API response to expected format
    // Create user object from credentials (username) since API doesn't return full user
    const user: User = {
      username: credentials.username,
      role: "admin", // Default role, should be fetched from /auth/me
      permissions: ["*"], // Admin has all permissions
    };

    return {
      access_token: response.data.data.access_token,
      token_type: response.data.data.token_type,
      user,
    };
  },

  getCurrentUser: async (): Promise<User> => {
    interface MeResponse {
      success: boolean;
      data: {
        username: string;
        roles: string[];
        last_login: string;
      };
    }
    const response = await apiClient.get<MeResponse>("/auth/me");

    // Transform API response to User type
    return {
      username: response.data.data.username,
      role: response.data.data.roles.includes("admin") ? "admin" : "user",
      permissions: response.data.data.roles.includes("admin") ? ["*"] : [],
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
