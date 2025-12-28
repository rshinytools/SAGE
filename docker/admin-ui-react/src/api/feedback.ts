import { apiClient } from "./client";

// Types
export interface FeedbackSubmission {
  query_id: string;
  question: string;
  generated_sql: string;
  feedback_type: "CONFIRM" | "CORRECT" | "REJECT" | "REPORT";
  corrected_sql?: string;
  correction_notes?: string;
  issue_description?: string;
  rating?: number;
}

export interface FeedbackResponse {
  success: boolean;
  message: string;
  feedback_id?: string;
  example_created: boolean;
  example_id?: string;
}

export interface FeedbackStats {
  total_feedback: number;
  confirmations: number;
  corrections: number;
  rejections: number;
  reports: number;
  pending_reviews: number;
  examples_created: number;
  average_rating?: number;
}

export interface PendingReview {
  id: string;
  query_id: string;
  question: string;
  generated_sql: string;
  feedback_type: string;
  corrected_sql?: string;
  correction_notes?: string;
  issue_description?: string;
  submitted_by: string;
  submitted_at: string;
  status: string;
}

export interface LearningExample {
  id: string;
  question: string;
  sql: string;
  category: string;
  confidence: number;
  verified: boolean;
  usage_count: number;
  success_rate: number;
  created_at: string;
}

export interface ExampleStats {
  total_examples: number;
  verified_examples: number;
  unverified_examples: number;
  categories: Record<string, number>;
  average_confidence: number;
}

export const feedbackApi = {
  // Submit feedback for a query response
  submitFeedback: async (feedback: FeedbackSubmission): Promise<FeedbackResponse> => {
    const response = await apiClient.post<FeedbackResponse>("/feedback/submit", feedback);
    return response.data;
  },

  // Quick confirm - response was correct
  confirmResponse: async (
    queryId: string,
    question: string,
    sql: string
  ): Promise<FeedbackResponse> => {
    return feedbackApi.submitFeedback({
      query_id: queryId,
      question,
      generated_sql: sql,
      feedback_type: "CONFIRM",
    });
  },

  // Quick reject - response was wrong
  rejectResponse: async (
    queryId: string,
    question: string,
    sql: string,
    reason?: string
  ): Promise<FeedbackResponse> => {
    return feedbackApi.submitFeedback({
      query_id: queryId,
      question,
      generated_sql: sql,
      feedback_type: "REJECT",
      issue_description: reason,
    });
  },

  // Correct response - provide correct SQL
  correctResponse: async (
    queryId: string,
    question: string,
    originalSql: string,
    correctedSql: string,
    notes?: string
  ): Promise<FeedbackResponse> => {
    return feedbackApi.submitFeedback({
      query_id: queryId,
      question,
      generated_sql: originalSql,
      feedback_type: "CORRECT",
      corrected_sql: correctedSql,
      correction_notes: notes,
    });
  },

  // Get feedback statistics
  getStats: async (): Promise<FeedbackStats> => {
    const response = await apiClient.get<FeedbackStats>("/feedback/stats");
    return response.data;
  },

  // Get pending reviews (admin only)
  getPendingReviews: async (limit = 20, offset = 0): Promise<PendingReview[]> => {
    const response = await apiClient.get<PendingReview[]>(
      `/feedback/pending?limit=${limit}&offset=${offset}`
    );
    return response.data;
  },

  // Approve pending feedback (admin only)
  approveFeedback: async (feedbackId: string): Promise<{ success: boolean; message: string }> => {
    const response = await apiClient.post<{ success: boolean; message: string }>(
      `/feedback/pending/${feedbackId}/approve`
    );
    return response.data;
  },

  // Reject pending feedback (admin only)
  rejectFeedback: async (
    feedbackId: string,
    reason?: string
  ): Promise<{ success: boolean; message: string }> => {
    const response = await apiClient.post<{ success: boolean; message: string }>(
      `/feedback/pending/${feedbackId}/reject`,
      { reason }
    );
    return response.data;
  },

  // Get learning examples (admin only)
  getExamples: async (
    verifiedOnly = false,
    category?: string,
    limit = 50,
    offset = 0
  ): Promise<LearningExample[]> => {
    const params = new URLSearchParams();
    params.append("limit", limit.toString());
    params.append("offset", offset.toString());
    if (verifiedOnly) params.append("verified_only", "true");
    if (category) params.append("category", category);

    const response = await apiClient.get<LearningExample[]>(`/feedback/examples?${params}`);
    return response.data;
  },

  // Verify a learning example (admin only)
  verifyExample: async (exampleId: string): Promise<{ success: boolean; message: string }> => {
    const response = await apiClient.post<{ success: boolean; message: string }>(
      `/feedback/examples/${exampleId}/verify`
    );
    return response.data;
  },

  // Delete a learning example (admin only)
  deleteExample: async (exampleId: string): Promise<{ success: boolean; message: string }> => {
    const response = await apiClient.delete<{ success: boolean; message: string }>(
      `/feedback/examples/${exampleId}`
    );
    return response.data;
  },

  // Get example statistics (admin only)
  getExampleStats: async (): Promise<ExampleStats> => {
    const response = await apiClient.get<ExampleStats>("/feedback/examples/stats");
    return response.data;
  },
};
