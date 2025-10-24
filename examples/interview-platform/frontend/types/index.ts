// ============================================================================
// Core Types for Interview Evaluation Platform
// ============================================================================

export enum InterviewStatus {
    PENDING = "pending",
    SCHEDULED = "scheduled",
    IN_PROGRESS = "in_progress",
    COMPLETED = "completed",
    CANCELLED = "cancelled",
}

export enum EvaluationStatus {
    PENDING = "pending",
    IN_REVIEW = "in_review",
    COMPLETED = "completed",
    FAILED = "failed",
}

export enum Priority {
    LOW = 1,
    NORMAL = 2,
    HIGH = 3,
    URGENT = 4,
}

// ============================================================================
// Interview Types
// ============================================================================

export interface Interview {
    id: string;
    candidateId: string;
    candidateName: string;
    candidateEmail: string;
    position: string;
    scheduledAt: string;
    duration: number; // minutes
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

export interface Evaluation {
    id: string;
    interviewId: string;
    evaluatorId: string;
    evaluatorName: string;
    technicalScore: number;
    communicationScore: number;
    cultureFitScore: number;
    overallScore: number;
    strengths: string[];
    weaknesses: string[];
    recommendation: "strong_hire" | "hire" | "maybe" | "no_hire";
    notes: string;
    status: EvaluationStatus;
    createdAt: string;
    updatedAt: string;
}

export interface CreateEvaluationRequest {
    interviewId: string;
    technicalScore: number;
    communicationScore: number;
    cultureFitScore: number;
    strengths: string[];
    weaknesses: string[];
    recommendation: "strong_hire" | "hire" | "maybe" | "no_hire";
    notes: string;
}

// ============================================================================
// Report Types
// ============================================================================

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
    status: "pending" | "ready" | "sent";
    generatedAt?: string;
    sentAt?: string;
    createdAt: string;
    updatedAt: string;
}

export interface GenerateReportRequest {
    interviewId: string;
    includeNotes?: boolean;
}

// ============================================================================
// Notification Types
// ============================================================================

export interface Notification {
    id: string;
    userId: string;
    type: "interview_scheduled" | "evaluation_pending" | "report_ready" | "reminder";
    title: string;
    message: string;
    priority: Priority;
    read: boolean;
    actionUrl?: string;
    createdAt: string;
}

// ============================================================================
// ChronoQueue Message Types
// ============================================================================

export interface QueueMessage<T = unknown> {
    id: string;
    queueName: string;
    payload: T;
    priority: Priority;
    scheduledFor?: string;
    createdAt: string;
    status: "pending" | "processing" | "completed" | "failed";
    retryCount: number;
    maxRetries: number;
}

// ============================================================================
// Dashboard Statistics
// ============================================================================

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

// ============================================================================
// API Response Types
// ============================================================================

export interface ApiResponse<T> {
    success: boolean;
    data?: T;
    error?: string;
    message?: string;
}

export interface PaginatedResponse<T> {
    success: boolean;
    data: T[];
    pagination: {
        page: number;
        pageSize: number;
        totalItems: number;
        totalPages: number;
    };
}

// ============================================================================
// Form Types
// ============================================================================

export interface InterviewFormData {
    candidateName: string;
    candidateEmail: string;
    position: string;
    scheduledAt: Date;
    duration: number;
    interviewerIds: string[];
    tags: string[];
}

export interface EvaluationFormData {
    technicalScore: number;
    communicationScore: number;
    cultureFitScore: number;
    strengths: string;
    weaknesses: string;
    recommendation: "strong_hire" | "hire" | "maybe" | "no_hire";
    notes: string;
}
