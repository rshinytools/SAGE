import apiClient from "./client";
import type {
  UserAccount,
  CreateUserRequest,
  UpdateUserRequest,
  PaginatedResponse,
} from "@/types/api";

export const usersApi = {
  getUsers: async (
    page: number = 1,
    pageSize: number = 50
  ): Promise<PaginatedResponse<UserAccount>> => {
    const response = await apiClient.get<PaginatedResponse<UserAccount>>("/users", {
      params: { page, page_size: pageSize },
    });
    return response.data;
  },

  getUser: async (id: string): Promise<UserAccount> => {
    const response = await apiClient.get<UserAccount>(`/users/${id}`);
    return response.data;
  },

  createUser: async (data: CreateUserRequest): Promise<UserAccount> => {
    const response = await apiClient.post<UserAccount>("/users", data);
    return response.data;
  },

  updateUser: async (id: string, data: UpdateUserRequest): Promise<UserAccount> => {
    const response = await apiClient.put<UserAccount>(`/users/${id}`, data);
    return response.data;
  },

  deleteUser: async (id: string): Promise<void> => {
    await apiClient.delete(`/users/${id}`);
  },

  resetPassword: async (id: string): Promise<{ temporary_password: string }> => {
    const response = await apiClient.post<{ temporary_password: string }>(
      `/users/${id}/reset-password`
    );
    return response.data;
  },

  toggleUserStatus: async (id: string, isActive: boolean): Promise<UserAccount> => {
    const response = await apiClient.patch<UserAccount>(`/users/${id}/status`, {
      is_active: isActive,
    });
    return response.data;
  },

  getPermissions: async (): Promise<string[]> => {
    const response = await apiClient.get<string[]>("/users/permissions");
    return response.data;
  },
};
