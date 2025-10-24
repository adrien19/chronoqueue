import { apiClient } from "./client";
import type { DashboardStats, ApiResponse } from "@/types";

export const dashboardApi = {
    // Get dashboard statistics
    getStats: async (): Promise<ApiResponse<DashboardStats>> => {
        const response = await apiClient.get("/api/dashboard/stats");
        return response.data;
    },

    // Get recent activity
    getActivity: async (limit?: number): Promise<ApiResponse<any[]>> => {
        const response = await apiClient.get("/api/dashboard/activity", {
            params: { limit },
        });
        return response.data;
    },
};
