import { apiClient } from "./client";
import type {
    Report,
    GenerateReportRequest,
    ApiResponse,
} from "@/types";

export const reportsApi = {
    // Get all reports
    getAll: async (params?: {
        status?: string;
        page?: number;
        pageSize?: number;
    }): Promise<ApiResponse<Report[]>> => {
        const response = await apiClient.get("/api/reports", { params });
        return response.data;
    },

    // Get report by ID
    getById: async (id: string): Promise<ApiResponse<Report>> => {
        const response = await apiClient.get(`/api/reports/${id}`);
        return response.data;
    },

    // Get report by interview ID
    getByInterview: async (interviewId: string): Promise<ApiResponse<Report>> => {
        const response = await apiClient.get(`/api/interviews/${interviewId}/report`);
        return response.data;
    },

    // Generate report for interview
    generate: async (
        data: GenerateReportRequest
    ): Promise<ApiResponse<Report>> => {
        const response = await apiClient.post("/api/reports/generate", data);
        return response.data;
    },

    // Send report via email
    send: async (reportId: string): Promise<ApiResponse<void>> => {
        const response = await apiClient.post(`/api/reports/${reportId}/send`);
        return response.data;
    },

    // Download report as PDF
    downloadPdf: async (reportId: string): Promise<Blob> => {
        const response = await apiClient.get(`/api/reports/${reportId}/pdf`, {
            responseType: "blob",
        });
        return response.data;
    },
};
