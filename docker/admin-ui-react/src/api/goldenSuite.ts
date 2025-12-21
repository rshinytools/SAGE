import apiClient from "./client";

// Types
export interface TestCategory {
  name: string;
  count: number;
}

export interface TestQuestion {
  id: number;
  question: string;
  category: string;
  sql_template: string;
  answer_type: string;
  notes?: string;
  expected_answer?: number | string | Record<string, number>;
  flow_id?: string;
  turn?: number;
}

export interface TestResult {
  question_id: number;
  question: string;
  category: string;
  expected: number | string | Record<string, number> | null;
  actual: number | null;
  match: boolean | null;
  answer_text: string;
  sql_executed: string;
  confidence: { score?: number; level?: string };
  execution_time_ms: number;
  error: string;
  flow_id?: string;
  turn?: number;
}

export interface TestRunSummary {
  run_id: string;
  status: "pending" | "running" | "completed" | "failed";
  started_at: string;
  completed_at?: string;
  total_questions: number;
  completed_questions: number;
  matches: number;
  mismatches: number;
  manual_check: number;
  accuracy: number;
  categories_requested?: string[];
  by_category: Record<string, {
    total: number;
    match: number;
    mismatch: number;
    manual: number;
    accuracy: number;
  }>;
  results?: TestResult[];
}

export interface RunRequest {
  categories?: string[];
  question_ids?: number[];
  include_flows?: boolean;
}

// API functions
export const goldenSuiteApi = {
  getCategories: async (): Promise<{ categories: TestCategory[]; total_questions: number }> => {
    const response = await apiClient.get("/golden-suite/categories");
    return response.data.data;
  },

  getQuestions: async (
    category?: string,
    includeExpected: boolean = false
  ): Promise<{ questions: TestQuestion[]; count: number }> => {
    const response = await apiClient.get("/golden-suite/questions", {
      params: { category, include_expected: includeExpected },
    });
    return response.data.data;
  },

  getQuestion: async (id: number): Promise<TestQuestion> => {
    const response = await apiClient.get(`/golden-suite/questions/${id}`);
    return response.data.data;
  },

  startRun: async (request: RunRequest): Promise<{ run_id: string; status: string; total_questions: number; message: string }> => {
    const response = await apiClient.post("/golden-suite/run", request);
    return response.data.data;
  },

  getRuns: async (): Promise<{ runs: TestRunSummary[] }> => {
    const response = await apiClient.get("/golden-suite/runs");
    return response.data.data;
  },

  getRun: async (runId: string): Promise<TestRunSummary> => {
    const response = await apiClient.get(`/golden-suite/runs/${runId}`);
    return response.data.data;
  },

  downloadRun: async (runId: string, format: "json" | "csv" | "html", category?: string): Promise<Blob> => {
    const response = await apiClient.get(`/golden-suite/runs/${runId}/download`, {
      params: { format, category },
      responseType: "blob",
    });
    return response.data;
  },

  deleteRun: async (runId: string): Promise<void> => {
    await apiClient.delete(`/golden-suite/runs/${runId}`);
  },
};
