import { apiClient } from "./client";
import type {
    Interview,
    CreateInterviewRequest,
    ApiResponse,
    PaginatedResponse,
} from "@/types";

export const interviewsApi = {
    // Get all interviews with optional filtering
    getAll: async (params?: {
        status?: string;
        page?: number;
        pageSize?: number;
    }): Promise<PaginatedResponse<Interview>> => {
        const response = await apiClient.get("/api/interviews", { params });
        return response.data;
    },

    // Get interview by ID
    getById: async (id: string): Promise<ApiResponse<Interview>> => {
        const response = await apiClient.get(`/api/interviews/${id}`);
        return response.data;
    },

    // Create new interview
    create: async (
        data: CreateInterviewRequest
    ): Promise<ApiResponse<Interview>> => {
        const response = await apiClient.post("/api/interviews", data);
        return response.data;
    },

    // Update interview
    update: async (
        id: string,
        data: Partial<CreateInterviewRequest>
    ): Promise<ApiResponse<Interview>> => {
        const response = await apiClient.put(`/api/interviews/${id}`, data);
        return response.data;
    },

    // Cancel interview
    cancel: async (id: string): Promise<ApiResponse<Interview>> => {
        const response = await apiClient.post(`/api/interviews/${id}/cancel`);
        return response.data;
    },

    // Complete interview
    complete: async (id: string): Promise<ApiResponse<Interview>> => {
        const response = await apiClient.post(`/api/interviews/${id}/complete`);
        return response.data;
    },
};
