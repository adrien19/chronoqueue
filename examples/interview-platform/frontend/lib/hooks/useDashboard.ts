import { useQuery } from "@tanstack/react-query";
import { dashboardApi } from "@/lib/api";

// Query Keys
export const dashboardKeys = {
    all: ["dashboard"] as const,
    stats: () => [...dashboardKeys.all, "stats"] as const,
    activity: (limit?: number) => [...dashboardKeys.all, "activity", limit] as const,
};

// Get dashboard statistics
export const useDashboardStats = () => {
    return useQuery({
        queryKey: dashboardKeys.stats(),
        queryFn: () => dashboardApi.getStats(),
        refetchInterval: 30000, // Refetch every 30 seconds
    });
};

// Get recent activity
export const useDashboardActivity = (limit?: number) => {
    return useQuery({
        queryKey: dashboardKeys.activity(limit),
        queryFn: () => dashboardApi.getActivity(limit),
        refetchInterval: 30000, // Refetch every 30 seconds
    });
};
