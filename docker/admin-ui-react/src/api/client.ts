import axios, { type AxiosError, type InternalAxiosRequestConfig } from "axios";
import { useAuthStore } from "@/stores/authStore";

const API_BASE_URL = import.meta.env.VITE_API_URL || "/api/v1";

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 30000,
});

// Request interceptor - add JWT token
apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const token = useAuthStore.getState().token;
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error: AxiosError) => {
    return Promise.reject(error);
  }
);

// Response interceptor - handle auth errors
apiClient.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    // Don't redirect on 401 for login endpoint - let the login page handle the error
    const isLoginEndpoint = error.config?.url?.includes("/auth/token");

    // Check if we're already on the login page to prevent redirect loops
    const isOnLoginPage = window.location.pathname === "/login";

    // Extract error details
    const apiError = error.response?.data as { detail?: string | { code?: string; message?: string } } | undefined;
    const errorCode = typeof apiError?.detail === "object" ? apiError.detail.code : undefined;

    // Handle 503 Maintenance Mode - log out user and redirect to login
    if (error.response?.status === 503 && errorCode === "MAINTENANCE_MODE" && !isOnLoginPage) {
      const hadToken = useAuthStore.getState().token;
      if (hadToken) {
        useAuthStore.getState().logout();
        // Add a flag to show maintenance message on login page
        sessionStorage.setItem("maintenance_mode", "true");
        window.location.href = "/login";
      }
    }

    if (error.response?.status === 401 && !isLoginEndpoint && !isOnLoginPage) {
      // Token expired or invalid - try to refresh or logout
      // Only logout if there was actually a token (not just an unauthenticated request)
      const hadToken = useAuthStore.getState().token;
      if (hadToken) {
        useAuthStore.getState().logout();
        window.location.href = "/login";
      }
    }

    // Extract error message from API response
    let errorMessage = "An error occurred";

    if (typeof apiError?.detail === "string") {
      errorMessage = apiError.detail;
    } else if (apiError?.detail?.message) {
      errorMessage = apiError.detail.message;
    } else if (error.response?.status === 401) {
      errorMessage = "Invalid username or password";
    } else if (error.response?.status === 403) {
      errorMessage = "Access denied";
    } else if (error.response?.status === 503) {
      errorMessage = "Service temporarily unavailable";
    } else if (error.response?.status === 500) {
      errorMessage = "Server error. Please try again later.";
    } else if (error.code === "ECONNABORTED") {
      errorMessage = "Request timed out. Please try again.";
    } else if (!error.response) {
      errorMessage = "Unable to connect to server. Please check your connection.";
    }

    // Create a new error with the extracted message
    const enhancedError = new Error(errorMessage);
    (enhancedError as Error & { originalError: AxiosError }).originalError = error;

    return Promise.reject(enhancedError);
  }
);

export default apiClient;
