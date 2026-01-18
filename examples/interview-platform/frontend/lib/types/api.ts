/**
 * API Types - Matches backend models exactly
 */

// ============================================================================
// Interview Types
// ============================================================================

export type InterviewStatus = "pending" | "scheduled" | "in_progress" | "completed" | "cancelled";

export interface Interview {
    id: string;
    candidateName: string;
    candidateEmail: string;
    position: string;
    scheduledAt: string;
    duration: number;
    status: InterviewStatus;
    interviewerIds: string[];
    tags: string[];
    createdAt: string;
    updatedAt: string;
}

export interface CreateInterviewRequest {
    candidateName: string;
    candidateEmail: string;
    position: string;
    scheduledAt: string;
    duration: number;
    interviewerIds: string[];
    tags?: string[];
}

// ============================================================================
// Evaluation Types
// ============================================================================

export type EvaluationStatus = "pending" | "in_review" | "completed" | "failed";
export type Recommendation = "strong_hire" | "hire" | "maybe" | "no_hire";

export interface Evaluation {
    id: string;
    interviewId: string;
    evaluatorId: string;
    evaluatorName: string;
    evaluatorEmail: string;
    technicalScore: number;
    communicationScore: number;
    problemSolvingScore: number;
    culturalFitScore: number;
    overallScore: number;
    strengths: string[];
    weaknesses: string[];
    recommendation: Recommendation;
    comments: string;
    status: EvaluationStatus;
    createdAt: string;
    updatedAt: string;
}

export interface CreateEvaluationRequest {
    interviewId: string;
    evaluatorName: string;
    evaluatorEmail: string;
    technicalScore: number;
    communicationScore: number;
    problemSolvingScore: number;
    culturalFitScore: number;
    strengths: string[];
    weaknesses: string[];
    recommendation: Recommendation;
    comments: string;
}

// ============================================================================
// Report Types
// ============================================================================

export type ReportStatus = "pending" | "ready" | "sent";

export interface Report {
    id: string;
    interviewId: string;
    candidateName: string;
    position: string;
    interviewDate: string;
    evaluationCount: number;
    averageTechnical: number;
    averageCommunication: number;
    averageCultureFit: number;
    averageOverall: number;
    finalRecommendation: string;
    status: ReportStatus;
    generatedAt?: string;
    sentAt?: string;
    createdAt: string;
    updatedAt: string;
}

export interface GenerateReportRequest {
    interviewId: string;
    includeNotes: boolean;
}

// ============================================================================
// Dashboard Types
// ============================================================================

export interface DashboardStats {
    totalInterviews: number;
    pendingEvaluations: number;
    completedReports: number;
    averageEvaluationTime: number;
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

// ============================================================================
// Queue Types
// ============================================================================

export interface QueueStats {
    name: string;
    description: string;
    messagesInQueue: number;
    messagesProcessing: number;
    messagesCompleted: number;
    messagesFailed: number;
    averageProcessingTime: number;
    status: string;
    lastProcessed: string;
}

export interface QueueMessage {
    id: string;
    queueName: string;
    type: string;
    subject: string;
    priority: number;
    status: string;
    createdAt: string;
    processedAt?: string;
}

// ============================================================================
// API Response Types
// ============================================================================

export interface APIResponse<T = any> {
    success: boolean;
    data?: T;
    error?: string;
    message?: string;
}

export interface PaginatedResponse<T = any> {
    success: boolean;
    data: T[];
    pagination: Pagination;
}

export interface Pagination {
    page: number;
    pageSize: number;
    totalItems: number;
    totalPages: number;
}
