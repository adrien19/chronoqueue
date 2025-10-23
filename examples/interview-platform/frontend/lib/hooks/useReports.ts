import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { reportsApi } from "@/lib/api";
import type { GenerateReportRequest } from "@/types";
import { toast } from "sonner";

// Query Keys
export const reportKeys = {
    all: ["reports"] as const,
    lists: () => [...reportKeys.all, "list"] as const,
    list: (filters?: any) => [...reportKeys.lists(), filters] as const,
    details: () => [...reportKeys.all, "detail"] as const,
    detail: (id: string) => [...reportKeys.details(), id] as const,
    byInterview: (interviewId: string) =>
        [...reportKeys.all, "interview", interviewId] as const,
    stats: () => [...reportKeys.all, "stats"] as const,
};

// Get all reports
export const useReports = (params?: {
    status?: string;
    page?: number;
    pageSize?: number;
}) => {
    return useQuery({
        queryKey: reportKeys.list(params),
        queryFn: () => reportsApi.getAll(params),
    });
};

// Get report by ID
export const useReport = (id: string) => {
    return useQuery({
        queryKey: reportKeys.detail(id),
        queryFn: () => reportsApi.getById(id),
        enabled: !!id,
    });
};

// Get report by interview ID
export const useReportByInterview = (interviewId: string) => {
    return useQuery({
        queryKey: reportKeys.byInterview(interviewId),
        queryFn: () => reportsApi.getByInterview(interviewId),
        enabled: !!interviewId,
    });
};

// Get reports stats (all reports for stats calculation)
export const useReportsStats = () => {
    return useQuery({
        queryKey: reportKeys.stats(),
        queryFn: async () => {
            const allReports = await reportsApi.getAll({ pageSize: 1000 });
            const reports = allReports.data || [];

            return {
                all: reports,
                total: reports.length,
                ready: reports.filter((r) => r.status === "ready"),
                pending: reports.filter((r) => r.status === "pending"),
                sent: reports.filter((r) => r.status === "sent"),
            };
        },
        refetchInterval: 30000, // Refetch every 30 seconds
    });
};

// Generate report mutation
export const useGenerateReport = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: (data: GenerateReportRequest) => reportsApi.generate(data),
        onSuccess: (_, variables) => {
            queryClient.invalidateQueries({
                queryKey: reportKeys.byInterview(variables.interviewId),
            });
            queryClient.invalidateQueries({ queryKey: reportKeys.lists() });
            queryClient.invalidateQueries({ queryKey: reportKeys.stats() });
            toast.success("Report generated successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to generate report");
        },
    });
};

// Send report mutation
export const useSendReport = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: (reportId: string) => reportsApi.send(reportId),
        onSuccess: (_, reportId) => {
            queryClient.invalidateQueries({ queryKey: reportKeys.detail(reportId) });
            queryClient.invalidateQueries({ queryKey: reportKeys.lists() });
            queryClient.invalidateQueries({ queryKey: reportKeys.stats() });
            toast.success("Report sent successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to send report");
        },
    });
};

// Download report as PDF
export const useDownloadReport = () => {
    return useMutation({
        mutationFn: async (reportId: string) => {
            const blob = await reportsApi.downloadPdf(reportId);
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement("a");
            link.href = url;
            link.download = `report-${reportId}.pdf`;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            window.URL.revokeObjectURL(url);
        },
        onSuccess: () => {
            toast.success("Report downloaded successfully!");
        },
        onError: (error: any) => {
            toast.error("Failed to download report");
        },
    });
};
