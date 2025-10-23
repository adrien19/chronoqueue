import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { interviewsApi } from "@/lib/api";
import type { CreateInterviewRequest } from "@/types";
import { toast } from "sonner";

// Query Keys
export const interviewKeys = {
    all: ["interviews"] as const,
    lists: () => [...interviewKeys.all, "list"] as const,
    list: (filters?: any) => [...interviewKeys.lists(), filters] as const,
    details: () => [...interviewKeys.all, "detail"] as const,
    detail: (id: string) => [...interviewKeys.details(), id] as const,
};

// Get all interviews
export const useInterviews = (params?: {
    status?: string;
    page?: number;
    pageSize?: number;
}) => {
    return useQuery({
        queryKey: interviewKeys.list(params),
        queryFn: () => interviewsApi.getAll(params),
    });
};

// Get interview by ID
export const useInterview = (id: string) => {
    return useQuery({
        queryKey: interviewKeys.detail(id),
        queryFn: () => interviewsApi.getById(id),
        enabled: !!id,
    });
};

// Create interview mutation
export const useCreateInterview = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: (data: CreateInterviewRequest) => interviewsApi.create(data),
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: interviewKeys.lists() });
            toast.success("Interview scheduled successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to schedule interview");
        },
    });
};

// Update interview mutation
export const useUpdateInterview = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: ({
            id,
            data,
        }: {
            id: string;
            data: Partial<CreateInterviewRequest>;
        }) => interviewsApi.update(id, data),
        onSuccess: (_, variables) => {
            queryClient.invalidateQueries({ queryKey: interviewKeys.detail(variables.id) });
            queryClient.invalidateQueries({ queryKey: interviewKeys.lists() });
            toast.success("Interview updated successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to update interview");
        },
    });
};

// Cancel interview mutation
export const useCancelInterview = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: (id: string) => interviewsApi.cancel(id),
        onSuccess: (_, id) => {
            queryClient.invalidateQueries({ queryKey: interviewKeys.detail(id) });
            queryClient.invalidateQueries({ queryKey: interviewKeys.lists() });
            toast.success("Interview cancelled successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to cancel interview");
        },
    });
};

// Complete interview mutation
export const useCompleteInterview = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: (id: string) => interviewsApi.complete(id),
        onSuccess: (_, id) => {
            queryClient.invalidateQueries({ queryKey: interviewKeys.detail(id) });
            queryClient.invalidateQueries({ queryKey: interviewKeys.lists() });
            toast.success("Interview marked as completed!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to complete interview");
        },
    });
};
