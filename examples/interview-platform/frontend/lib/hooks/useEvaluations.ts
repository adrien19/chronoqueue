import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { evaluationsApi } from "@/lib/api";
import type { CreateEvaluationRequest } from "@/types";
import { toast } from "sonner";

// Query Keys
export const evaluationKeys = {
    all: ["evaluations"] as const,
    lists: () => [...evaluationKeys.all, "list"] as const,
    list: (filters?: any) => [...evaluationKeys.lists(), filters] as const,
    details: () => [...evaluationKeys.all, "detail"] as const,
    detail: (id: string) => [...evaluationKeys.details(), id] as const,
    byInterview: (interviewId: string) =>
        [...evaluationKeys.all, "interview", interviewId] as const,
    pending: () => [...evaluationKeys.all, "pending"] as const,
    stats: () => [...evaluationKeys.all, "stats"] as const,
};

// List evaluations with optional status filter
export const useEvaluations = (status?: string, page: number = 1, pageSize: number = 50) => {
    return useQuery({
        queryKey: evaluationKeys.list({ status, page, pageSize }),
        queryFn: () => evaluationsApi.list(status, page, pageSize),
    });
};

// Get evaluation stats for dashboard
export const useEvaluationStats = () => {
    return useQuery({
        queryKey: evaluationKeys.stats(),
        queryFn: () => evaluationsApi.getStats(),
        refetchInterval: 30000, // Refetch every 30 seconds
    });
};

// Get evaluations by interview ID
export const useEvaluationsByInterview = (interviewId: string) => {
    return useQuery({
        queryKey: evaluationKeys.byInterview(interviewId),
        queryFn: () => evaluationsApi.getByInterview(interviewId),
        enabled: !!interviewId,
    });
};

// Get evaluation by ID
export const useEvaluation = (id: string) => {
    return useQuery({
        queryKey: evaluationKeys.detail(id),
        queryFn: () => evaluationsApi.getById(id),
        enabled: !!id,
    });
};

// Get pending evaluations
export const usePendingEvaluations = () => {
    return useQuery({
        queryKey: evaluationKeys.pending(),
        queryFn: () => evaluationsApi.getPending(),
    });
};

// Create evaluation mutation
export const useCreateEvaluation = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: (data: CreateEvaluationRequest) => evaluationsApi.create(data),
        onSuccess: (_, variables) => {
            queryClient.invalidateQueries({
                queryKey: evaluationKeys.byInterview(variables.interviewId),
            });
            queryClient.invalidateQueries({ queryKey: evaluationKeys.pending() });
            toast.success("Evaluation submitted successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to submit evaluation");
        },
    });
};

// Update evaluation mutation
export const useUpdateEvaluation = () => {
    const queryClient = useQueryClient();

    return useMutation({
        mutationFn: ({
            id,
            data,
        }: {
            id: string;
            data: Partial<CreateEvaluationRequest>;
        }) => evaluationsApi.update(id, data),
        onSuccess: (response, variables) => {
            queryClient.invalidateQueries({ queryKey: evaluationKeys.detail(variables.id) });
            if (response.data) {
                queryClient.invalidateQueries({
                    queryKey: evaluationKeys.byInterview(response.data.interviewId),
                });
            }
            toast.success("Evaluation updated successfully!");
        },
        onError: (error: any) => {
            toast.error(error.response?.data?.message || "Failed to update evaluation");
        },
    });
};
