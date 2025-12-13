import apiClient from "./client";
import type {
  TrackerPhase,
  TrackerTask,
  CreateTaskRequest,
  UpdateTaskRequest,
} from "@/types/api";

// API response wrapper type - backend wraps all responses
interface ApiResponse<T> {
  success: boolean;
  data: T;
  meta?: { timestamp: string };
}

export const trackerApi = {
  getPhases: async (): Promise<TrackerPhase[]> => {
    const response = await apiClient.get<ApiResponse<TrackerPhase[]>>("/tracker/phases");
    return response.data.data || [];
  },

  getPhase: async (id: number): Promise<TrackerPhase> => {
    const response = await apiClient.get<ApiResponse<TrackerPhase>>(`/tracker/phases/${id}`);
    return response.data.data;
  },

  createPhase: async (name: string, description?: string): Promise<TrackerPhase> => {
    // Backend doesn't have create phase - return mock for now
    // This would need backend implementation
    console.warn("createPhase: Backend endpoint not implemented");
    return {
      id: Date.now(),
      name,
      description,
      order_num: 0,
      tasks: [],
      progress: 0,
    };
  },

  updatePhase: async (
    id: number,
    data: { name?: string; description?: string }
  ): Promise<TrackerPhase> => {
    const response = await apiClient.put<ApiResponse<TrackerPhase>>(`/tracker/phases/${id}`, data);
    return response.data.data;
  },

  deletePhase: async (_id: number): Promise<void> => {
    // Backend doesn't have delete phase - log warning
    console.warn("deletePhase: Backend endpoint not implemented");
    await Promise.resolve();
  },

  getTasks: async (phaseId?: number): Promise<TrackerTask[]> => {
    const response = await apiClient.get<ApiResponse<TrackerTask[]>>("/tracker/tasks", {
      params: phaseId ? { phase_id: phaseId } : undefined,
    });
    return response.data.data || [];
  },

  getTask: async (id: number): Promise<TrackerTask> => {
    const response = await apiClient.get<ApiResponse<TrackerTask>>(`/tracker/tasks/${id}`);
    return response.data.data;
  },

  createTask: async (data: CreateTaskRequest): Promise<TrackerTask> => {
    // Backend doesn't have create task endpoint - return mock
    console.warn("createTask: Backend endpoint not implemented");
    return {
      id: Date.now(),
      phase_id: data.phase_id,
      name: data.name,
      description: data.description,
      status: "pending",
      priority: data.priority || "medium",
      assigned_to: data.assigned_to,
      due_date: data.due_date,
      order_num: 0,
    };
  },

  updateTask: async (id: number, data: UpdateTaskRequest): Promise<TrackerTask> => {
    const response = await apiClient.put<ApiResponse<TrackerTask>>(`/tracker/tasks/${id}`, data);
    return response.data.data;
  },

  deleteTask: async (_id: number): Promise<void> => {
    // Backend doesn't have delete task - log warning
    console.warn("deleteTask: Backend endpoint not implemented");
    await Promise.resolve();
  },

  updateTaskStatus: async (
    id: number,
    status: "pending" | "in_progress" | "completed" | "blocked"
  ): Promise<TrackerTask> => {
    // Use existing update endpoint with status
    const response = await apiClient.put<ApiResponse<TrackerTask>>(`/tracker/tasks/${id}`, {
      status,
    });
    return response.data.data;
  },

  reorderTasks: async (_phaseId: number, _taskIds: number[]): Promise<void> => {
    // Backend doesn't have reorder - log warning
    console.warn("reorderTasks: Backend endpoint not implemented");
    await Promise.resolve();
  },

  getProgress: async (): Promise<{
    total_tasks: number;
    completed_tasks: number;
    in_progress_tasks: number;
    blocked_tasks: number;
    overall_progress: number;
  }> => {
    // Backend uses /summary endpoint instead of /progress
    interface SummaryResponse {
      total_progress: number;
      phases_total: number;
      phases_complete: number;
      tasks_total: number;
      tasks_complete: number;
    }
    const response = await apiClient.get<ApiResponse<SummaryResponse>>("/tracker/summary");
    const data = response.data.data;
    return {
      total_tasks: data.tasks_total || 0,
      completed_tasks: data.tasks_complete || 0,
      in_progress_tasks: 0, // Not provided by backend
      blocked_tasks: 0, // Not provided by backend
      overall_progress: data.total_progress || 0,
    };
  },
};
