/**
 * API Client - Type-safe API service
 */

import axios, { AxiosInstance, AxiosError } from "axios";
import {
    Interview,
    CreateInterviewRequest,
    Evaluation,
    CreateEvaluationRequest,
    Report,
    GenerateReportRequest,
    DashboardStats,
    QueueStats,
    QueueMessage,
    APIResponse,
    PaginatedResponse,
} from "../types/api";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001";

export class APIClient {
    private client: AxiosInstance;

    constructor() {
        console.log("[API Client] Initializing with URL:", API_URL);
        
        this.client = axios.create({
            baseURL: API_URL,
            withCredentials: true,
            headers: {
                "Content-Type": "application/json",
            },
        });

        // Response interceptor for error handling
        this.client.interceptors.response.use(
            (response) => response,
            (error: AxiosError) => {
                if (error.response) {
                    console.error("[API Client] Server error:", {
                        status: error.response.status,
                        data: error.response.data,
                        url: error.config?.url,
                        method: error.config?.method,
                    });
                } else if (error.request) {
                    console.error("[API Client] Network error:", {
                        baseURL: error.config?.baseURL,
                        url: error.config?.url,
                        fullURL: `${error.config?.baseURL}${error.config?.url}`,
                        method: error.config?.method,
                        message: error.message,
                        code: error.code,
                    });
                } else {
                    console.error("[API Client] Request setup error:", error.message);
                }
                return Promise.reject(error);
            }
        );
    }

    // ========================================================================
    // Dashboard
    // ========================================================================

    async getDashboardStats(): Promise<DashboardStats> {
        const response = await this.client.get<APIResponse<DashboardStats>>("/api/dashboard/stats");
        return response.data.data || {
            totalInterviews: 0,
            pendingEvaluations: 0,
            completedReports: 0,
            averageEvaluationTime: 0,
            interviewsByStatus: {},
            evaluationsByStatus: {},
            recentActivity: [],
        };
    }

    async getDashboardActivity() {
        const response = await this.client.get<APIResponse>("/api/dashboard/activity");
        return response.data.data || [];
    }

    // ========================================================================
    // Interviews
    // ========================================================================

    async listInterviews(params?: { status?: string; page?: number; pageSize?: number }): Promise<PaginatedResponse<Interview>> {
        const response = await this.client.get<PaginatedResponse<Interview>>("/api/interviews", { params });
        return response.data || {
            success: true,
            data: [],
            pagination: { page: 1, pageSize: 20, totalItems: 0, totalPages: 0 },
        };
    }

    async getInterview(id: string): Promise<Interview | null> {
        try {
            const response = await this.client.get<APIResponse<Interview>>(`/api/interviews/${id}`);
            return response.data.data || null;
        } catch (error) {
            console.error("[API Client] Failed to get interview:", error);
            return null;
        }
    }

    async createInterview(data: CreateInterviewRequest): Promise<Interview> {
        const response = await this.client.post<APIResponse<Interview>>("/api/interviews", data);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to create interview");
        }
        return response.data.data;
    }

    async updateInterview(id: string, updates: Partial<Interview>): Promise<Interview> {
        const response = await this.client.put<APIResponse<Interview>>(`/api/interviews/${id}`, updates);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to update interview");
        }
        return response.data.data;
    }

    async cancelInterview(id: string): Promise<Interview> {
        const response = await this.client.post<APIResponse<Interview>>(`/api/interviews/${id}/cancel`);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to cancel interview");
        }
        return response.data.data;
    }

    async startInterview(id: string): Promise<Interview> {
        const response = await this.client.post<APIResponse<Interview>>(`/api/interviews/${id}/start`);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to start interview");
        }
        return response.data.data;
    }

    async completeInterview(id: string): Promise<Interview> {
        const response = await this.client.post<APIResponse<Interview>>(`/api/interviews/${id}/complete`);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to complete interview");
        }
        return response.data.data;
    }

    async getInterviewEvaluations(id: string): Promise<Evaluation[]> {
        try {
            const response = await this.client.get<APIResponse<Evaluation[]>>(`/api/interviews/${id}/evaluations`);
            return response.data.data || [];
        } catch (error) {
            console.error("[API Client] Failed to get interview evaluations:", error);
            return [];
        }
    }

    async getInterviewReport(id: string): Promise<Report | null> {
        try {
            const response = await this.client.get<APIResponse<Report>>(`/api/interviews/${id}/report`);
            return response.data.data || null;
        } catch (error) {
            console.error("[API Client] Failed to get interview report:", error);
            return null;
        }
    }

    // ========================================================================
    // Evaluations
    // ========================================================================

    async listEvaluations(params?: { status?: string; page?: number; pageSize?: number }): Promise<PaginatedResponse<Evaluation>> {
        const response = await this.client.get<PaginatedResponse<Evaluation>>("/api/evaluations", { params });
        return response.data || {
            success: true,
            data: [],
            pagination: { page: 1, pageSize: 20, totalItems: 0, totalPages: 0 },
        };
    }

    async getEvaluation(id: string): Promise<Evaluation | null> {
        try {
            const response = await this.client.get<APIResponse<Evaluation>>(`/api/evaluations/${id}`);
            return response.data.data || null;
        } catch (error) {
            console.error("[API Client] Failed to get evaluation:", error);
            return null;
        }
    }

    async getPendingEvaluations(): Promise<Evaluation[]> {
        try {
            const response = await this.client.get<APIResponse<Evaluation[]>>("/api/evaluations/pending");
            return response.data.data || [];
        } catch (error) {
            console.error("[API Client] Failed to get pending evaluations:", error);
            return [];
        }
    }

    async createEvaluation(data: CreateEvaluationRequest): Promise<Evaluation> {
        const response = await this.client.post<APIResponse<Evaluation>>("/api/evaluations", data);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to create evaluation");
        }
        return response.data.data;
    }

    async updateEvaluation(id: string, updates: Partial<Evaluation>): Promise<Evaluation> {
        const response = await this.client.put<APIResponse<Evaluation>>(`/api/evaluations/${id}`, updates);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to update evaluation");
        }
        return response.data.data;
    }

    // ========================================================================
    // Reports
    // ========================================================================

    async listReports(params?: { status?: string; page?: number; pageSize?: number }): Promise<PaginatedResponse<Report>> {
        const response = await this.client.get<PaginatedResponse<Report>>("/api/reports", { params });
        return response.data || {
            success: true,
            data: [],
            pagination: { page: 1, pageSize: 20, totalItems: 0, totalPages: 0 },
        };
    }

    async getReport(id: string): Promise<Report | null> {
        try {
            const response = await this.client.get<APIResponse<Report>>(`/api/reports/${id}`);
            return response.data.data || null;
        } catch (error) {
            console.error("[API Client] Failed to get report:", error);
            return null;
        }
    }

    async generateReport(data: GenerateReportRequest): Promise<Report> {
        const response = await this.client.post<APIResponse<Report>>("/api/reports/generate", data);
        if (!response.data.success || !response.data.data) {
            throw new Error(response.data.error || "Failed to generate report");
        }
        return response.data.data;
    }

    async sendReport(id: string): Promise<void> {
        const response = await this.client.post<APIResponse>(`/api/reports/${id}/send`);
        if (!response.data.success) {
            throw new Error(response.data.error || "Failed to send report");
        }
    }

    async downloadReportPDF(id: string): Promise<Blob> {
        const response = await this.client.get(`/api/reports/${id}/pdf`, {
            responseType: "blob",
        });
        return response.data;
    }

    // ========================================================================
    // Queue Monitoring
    // ========================================================================

    async listQueues(): Promise<QueueStats[]> {
        try {
            const response = await this.client.get<APIResponse<QueueStats[]>>("/api/queues");
            return response.data.data || [];
        } catch (error) {
            console.error("[API Client] Failed to list queues:", error);
            return [];
        }
    }

    async getQueueStats(name: string): Promise<QueueStats | null> {
        try {
            const response = await this.client.get<APIResponse<QueueStats>>(`/api/queues/${name}/stats`);
            return response.data.data || null;
        } catch (error) {
            console.error("[API Client] Failed to get queue stats:", error);
            return null;
        }
    }

    async getRecentMessages(): Promise<QueueMessage[]> {
        try {
            const response = await this.client.get<APIResponse<QueueMessage[]>>("/api/queues/messages/recent");
            return response.data.data || [];
        } catch (error) {
            console.error("[API Client] Failed to get recent messages:", error);
            return [];
        }
    }
}

// Export singleton instance
export const apiClient = new APIClient();
