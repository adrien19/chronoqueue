/**
 * Typed API Service for Interview Platform
 */

import { apiClient } from "./client"

// ============================================================================
// Types
// ============================================================================

export interface Interview {
    id: string
    candidateName: string
    candidateEmail: string
    position: string
    scheduledAt: string
    duration: number
    status: string
    interviewerIds: string[]
    tags?: string[]
    createdAt: string
    updatedAt: string
}

export interface Evaluation {
    id: string
    interview_id: string
    evaluator_name: string
    evaluator_email: string
    technical_score: number
    communication_score: number
    problem_solving_score: number
    cultural_fit_score: number
    overall_score: number
    strengths: string[]
    weaknesses: string[]
    recommendation: string
    comments?: string
    status: string
    submitted_at?: string
    created_at: string
    updated_at: string
}

export interface Report {
    id: number
    interview_id: string
    summary: string
    technical_assessment: string
    strengths: string
    weaknesses: string
    recommendation: string
    confidence_level: string
    status: string
    generated_at?: string
    sent_at?: string
    created_at: string
    updated_at: string
}

export interface DashboardStats {
    totalInterviews: number;
    pendingEvaluations: number;
    completedReports: number;
    completedEvaluations?: number;
    averageEvaluationTime: number; // in hours
    interviewsByStatus: Record<string, number>;
    evaluationsByStatus: Record<string, number>;
    recentActivity: ActivityItem[];
}

export interface ActivityItem {
    id: string;
    type: "interview" | "evaluation" | "report";
    action: string;
    timestamp: string;
    user?: string;
}

export interface QueueStats {
    queue_name: string
    total_messages: number
    pending_messages: number
    processing_messages: number
    failed_messages: number
}

export interface APIResponse<T = any> {
    success: boolean
    data?: T
    error?: string
}

export interface PaginationParams {
    page?: number
    limit?: number
    status?: string
}

export interface CreateInterviewRequest {
    candidateName: string
    candidateEmail: string
    position: string
    scheduledAt: string
    duration: number
    interviewerIds: string[]
    tags?: string[]
}

export interface UpdateInterviewRequest {
    candidate_name?: string
    candidate_email?: string
    position?: string
    scheduled_date?: string
    duration?: number
    interviewers?: string[]
    notes?: string
    tags?: string[]
    status?: string
}

export interface CreateEvaluationRequest {
    interview_id: string
    evaluator_name: string
    evaluator_email: string
    technical_score: number
    communication_score: number
    problem_solving_score: number
    cultural_fit_score: number
    strengths: string[]
    weaknesses: string[]
    recommendation: string
    comments?: string
}

export interface GenerateReportRequest {
    interview_id: string
}

// ============================================================================
// API Service Functions
// ============================================================================

// Dashboard API
export const getDashboardStats = async (): Promise<DashboardStats> => {
    const response = await apiClient.get<APIResponse<DashboardStats>>("/api/dashboard/stats")
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch dashboard stats")
    }
    return response.data.data!
}

export const getDashboardActivity = async () => {
    const response = await apiClient.get("/api/dashboard/activity")
    return response.data
}

// Interviews API
export const listInterviews = async (params?: PaginationParams): Promise<Interview[]> => {
    const response = await apiClient.get<APIResponse<Interview[]>>("/api/interviews", { params })
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch interviews")
    }
    return response.data.data!
}

export const getInterview = async (id: string): Promise<Interview> => {
    const response = await apiClient.get<APIResponse<Interview>>(`/api/interviews/${id}`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch interview")
    }
    return response.data.data!
}

export const createInterview = async (data: CreateInterviewRequest): Promise<Interview> => {
    const response = await apiClient.post<APIResponse<Interview>>("/api/interviews", data)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to create interview")
    }
    return response.data.data!
}

export const updateInterview = async (
    id: string,
    data: UpdateInterviewRequest
): Promise<Interview> => {
    const response = await apiClient.put<APIResponse<Interview>>(`/api/interviews/${id}`, data)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to update interview")
    }
    return response.data.data!
}

export const cancelInterview = async (id: string): Promise<Interview> => {
    const response = await apiClient.post<APIResponse<Interview>>(`/api/interviews/${id}/cancel`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to cancel interview")
    }
    return response.data.data!
}

export const startInterview = async (id: string): Promise<Interview> => {
    const response = await apiClient.post<APIResponse<Interview>>(`/api/interviews/${id}/start`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to start interview")
    }
    return response.data.data!
}

export const completeInterview = async (id: string): Promise<Interview> => {
    const response = await apiClient.post<APIResponse<Interview>>(`/api/interviews/${id}/complete`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to complete interview")
    }
    return response.data.data!
}

export const getInterviewEvaluations = async (id: string): Promise<Evaluation[]> => {
    const response = await apiClient.get<APIResponse<Evaluation[]>>(
        `/api/interviews/${id}/evaluations`
    )
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch evaluations")
    }
    return response.data.data!
}

export const getInterviewReport = async (id: string): Promise<Report> => {
    const response = await apiClient.get<APIResponse<Report>>(`/api/interviews/${id}/report`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch report")
    }
    return response.data.data!
}

// Evaluations API
export const listEvaluations = async (params?: PaginationParams): Promise<Evaluation[]> => {
    const response = await apiClient.get<APIResponse<Evaluation[]>>("/api/evaluations", { params })
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch evaluations")
    }
    return response.data.data!
}

export const getEvaluation = async (id: number): Promise<Evaluation> => {
    const response = await apiClient.get<APIResponse<Evaluation>>(`/api/evaluations/${id}`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch evaluation")
    }
    return response.data.data!
}

export const createEvaluation = async (data: CreateEvaluationRequest): Promise<Evaluation> => {
    const response = await apiClient.post<APIResponse<Evaluation>>("/api/evaluations", data)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to create evaluation")
    }
    return response.data.data!
}

export const getPendingEvaluations = async (): Promise<Evaluation[]> => {
    const response = await apiClient.get<APIResponse<Evaluation[]>>("/api/evaluations/pending")
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch pending evaluations")
    }
    return response.data.data!
}

// Reports API
export const listReports = async (params?: PaginationParams): Promise<Report[]> => {
    const response = await apiClient.get<APIResponse<Report[]>>("/api/reports", { params })
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch reports")
    }
    return response.data.data!
}

export const getReport = async (id: number): Promise<Report> => {
    const response = await apiClient.get<APIResponse<Report>>(`/api/reports/${id}`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch report")
    }
    return response.data.data!
}

export const generateReport = async (data: GenerateReportRequest): Promise<Report> => {
    const response = await apiClient.post<APIResponse<Report>>("/api/reports/generate", data)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to generate report")
    }
    return response.data.data!
}

export const sendReport = async (id: number): Promise<Report> => {
    const response = await apiClient.post<APIResponse<Report>>(`/api/reports/${id}/send`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to send report")
    }
    return response.data.data!
}

export const downloadReportPDF = async (id: number): Promise<Blob> => {
    const response = await apiClient.get(`/api/reports/${id}/pdf`, {
        responseType: "blob",
    })
    return response.data
}

// Queues API
export const listQueues = async (): Promise<QueueStats[]> => {
    const response = await apiClient.get<APIResponse<QueueStats[]>>("/api/queues")
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch queues")
    }
    return response.data.data!
}

export const getQueueStats = async (name: string): Promise<QueueStats> => {
    const response = await apiClient.get<APIResponse<QueueStats>>(`/api/queues/${name}/stats`)
    if (!response.data.success) {
        throw new Error(response.data.error || "Failed to fetch queue stats")
    }
    return response.data.data!
}

export const getRecentMessages = async () => {
    const response = await apiClient.get("/api/queues/messages/recent")
    return response.data
}
