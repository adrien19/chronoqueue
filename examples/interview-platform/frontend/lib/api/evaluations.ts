import { apiClient } from "./client";
import { interviewsApi } from "./interviews";
import type {
    Evaluation,
    CreateEvaluationRequest,
    ApiResponse,
    PaginatedResponse,
    Interview,
} from "@/types";

// Enhanced evaluation with interview details
export interface EvaluationWithInterview extends Evaluation {
    interview?: Interview;
    candidateName?: string;
    position?: string;
    interviewDate?: string;
}

export const evaluationsApi = {
    // List evaluations with optional status filter
    list: async (
        status?: string,
        page: number = 1,
        pageSize: number = 50
    ): Promise<PaginatedResponse<Evaluation>> => {
        const params = new URLSearchParams();
        if (status) params.append('status', status);
        params.append('page', page.toString());
        params.append('pageSize', pageSize.toString());

        const response = await apiClient.get(`/api/evaluations?${params}`);
        return response.data;
    },

    // Get all evaluations for an interview
    getByInterview: async (
        interviewId: string
    ): Promise<ApiResponse<Evaluation[]>> => {
        const response = await apiClient.get(
            `/api/interviews/${interviewId}/evaluations`
        );
        return response.data;
    },

    // Get evaluation by ID
    getById: async (id: string): Promise<ApiResponse<Evaluation>> => {
        const response = await apiClient.get(`/api/evaluations/${id}`);
        return response.data;
    },

    // Create new evaluation
    create: async (
        data: CreateEvaluationRequest
    ): Promise<ApiResponse<Evaluation>> => {
        const response = await apiClient.post("/api/evaluations", data);
        return response.data;
    },

    // Update evaluation
    update: async (
        id: string,
        data: Partial<CreateEvaluationRequest>
    ): Promise<ApiResponse<Evaluation>> => {
        const response = await apiClient.put(`/api/evaluations/${id}`, data);
        return response.data;
    },

    // Get pending evaluations for current user
    getPending: async (): Promise<ApiResponse<Evaluation[]>> => {
        const response = await apiClient.get("/api/evaluations/pending");
        return response.data;
    },

    // Get evaluation stats for dashboard
    getStats: async () => {
        const [pendingRes, completedRes] = await Promise.all([
            evaluationsApi.list('pending', 1, 100),
            evaluationsApi.list('completed', 1, 100),
        ]);

        let pending = pendingRes.data || [];
        let completed = completedRes.data || [];

        // Enrich evaluations with interview data
        const enrichEvaluation = async (evaluation: Evaluation): Promise<EvaluationWithInterview> => {
            try {
                const interviewRes = await interviewsApi.getById(evaluation.interviewId);
                const interview = interviewRes.data;
                if (!interview) {
                    return evaluation;
                }
                return {
                    ...evaluation,
                    interview,
                    candidateName: interview.candidateName,
                    position: interview.position,
                    interviewDate: interview.scheduledAt,
                };
            } catch (error) {
                // If interview fetch fails, return evaluation without enrichment
                return evaluation;
            }
        };

        // Enrich all evaluations with interview data
        const enrichedPending = await Promise.all(
            pending.map(enrichEvaluation)
        );
        const enrichedCompleted = await Promise.all(
            completed.map(enrichEvaluation)
        );

        // Calculate average time from creation to update (approximation)
        let totalTimeHours = 0;
        let completedCount = 0;

        for (const evaluation of enrichedCompleted) {
            if (evaluation.updatedAt && evaluation.createdAt) {
                const updatedTime = new Date(evaluation.updatedAt).getTime();
                const createdTime = new Date(evaluation.createdAt).getTime();
                const diffHours = (updatedTime - createdTime) / (1000 * 60 * 60);
                totalTimeHours += diffHours;
                completedCount++;
            }
        }

        const avgTimeHours = completedCount > 0 ? totalTimeHours / completedCount : 0;

        return {
            pending: enrichedPending,
            completed: enrichedCompleted,
            pendingCount: pendingRes.pagination?.totalItems || enrichedPending.length,
            completedCount: completedRes.pagination?.totalItems || enrichedCompleted.length,
            avgTimeHours: Math.round(avgTimeHours * 10) / 10, // Round to 1 decimal
        };
    },
};
