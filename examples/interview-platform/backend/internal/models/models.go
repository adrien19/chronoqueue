package models

import "time"

type InterviewStatus string

const (
	StatusPending    InterviewStatus = "pending"
	StatusScheduled  InterviewStatus = "scheduled"
	StatusInProgress InterviewStatus = "in_progress"
	StatusCompleted  InterviewStatus = "completed"
	StatusCancelled  InterviewStatus = "cancelled"
)

type Interview struct {
	ID             string          `json:"id"`
	CandidateName  string          `json:"candidateName"`
	CandidateEmail string          `json:"candidateEmail"`
	Position       string          `json:"position"`
	ScheduledAt    time.Time       `json:"scheduledAt"`
	Duration       int             `json:"duration"` // minutes
	Status         InterviewStatus `json:"status"`
	InterviewerIDs []string        `json:"interviewerIds"`
	Tags           []string        `json:"tags"`
	CreatedAt      time.Time       `json:"createdAt"`
	UpdatedAt      time.Time       `json:"updatedAt"`
}

type CreateInterviewRequest struct {
	CandidateName  string    `json:"candidateName"`
	CandidateEmail string    `json:"candidateEmail"`
	Position       string    `json:"position"`
	ScheduledAt    time.Time `json:"scheduledAt"`
	Duration       int       `json:"duration"`
	InterviewerIDs []string  `json:"interviewerIds"`
	Tags           []string  `json:"tags"`
}

type EvaluationStatus string

const (
	EvalStatusPending   EvaluationStatus = "pending"
	EvalStatusInReview  EvaluationStatus = "in_review"
	EvalStatusCompleted EvaluationStatus = "completed"
	EvalStatusFailed    EvaluationStatus = "failed"
)

type Recommendation string

const (
	RecommendationStrongHire Recommendation = "strong_hire"
	RecommendationHire       Recommendation = "hire"
	RecommendationMaybe      Recommendation = "maybe"
	RecommendationNoHire     Recommendation = "no_hire"
)

type Evaluation struct {
	ID                  string           `json:"id"`
	InterviewID         string           `json:"interviewId"`
	EvaluatorID         string           `json:"evaluatorId"`
	EvaluatorName       string           `json:"evaluatorName"`
	EvaluatorEmail      string           `json:"evaluatorEmail"`
	TechnicalScore      int              `json:"technicalScore"`
	CommunicationScore  int              `json:"communicationScore"`
	ProblemSolvingScore int              `json:"problemSolvingScore"`
	CultureFitScore     int              `json:"culturalFitScore"`
	OverallScore        float64          `json:"overallScore"`
	Strengths           []string         `json:"strengths"`
	Weaknesses          []string         `json:"weaknesses"`
	Recommendation      Recommendation   `json:"recommendation"`
	Comments            string           `json:"comments"`
	Status              EvaluationStatus `json:"status"`
	CreatedAt           time.Time        `json:"createdAt"`
	UpdatedAt           time.Time        `json:"updatedAt"`
}

type CreateEvaluationRequest struct {
	InterviewID         string         `json:"interviewId"`
	EvaluatorName       string         `json:"evaluatorName"`
	EvaluatorEmail      string         `json:"evaluatorEmail"`
	TechnicalScore      int            `json:"technicalScore"`
	CommunicationScore  int            `json:"communicationScore"`
	ProblemSolvingScore int            `json:"problemSolvingScore"`
	CultureFitScore     int            `json:"culturalFitScore"`
	Strengths           []string       `json:"strengths"`
	Weaknesses          []string       `json:"weaknesses"`
	Recommendation      Recommendation `json:"recommendation"`
	Comments            string         `json:"comments"`
}

type ReportStatus string

const (
	ReportStatusPending ReportStatus = "pending"
	ReportStatusReady   ReportStatus = "ready"
	ReportStatusSent    ReportStatus = "sent"
)

type Report struct {
	ID                   string       `json:"id"`
	InterviewID          string       `json:"interviewId"`
	CandidateName        string       `json:"candidateName"`
	Position             string       `json:"position"`
	InterviewDate        time.Time    `json:"interviewDate"`
	EvaluationCount      int          `json:"evaluationCount"`
	AverageTechnical     float64      `json:"averageTechnical"`
	AverageCommunication float64      `json:"averageCommunication"`
	AverageCultureFit    float64      `json:"averageCultureFit"`
	AverageOverall       float64      `json:"averageOverall"`
	FinalRecommendation  string       `json:"finalRecommendation"`
	Status               ReportStatus `json:"status"`
	GeneratedAt          *time.Time   `json:"generatedAt,omitempty"`
	SentAt               *time.Time   `json:"sentAt,omitempty"`
	CreatedAt            time.Time    `json:"createdAt"`
	UpdatedAt            time.Time    `json:"updatedAt"`
}

type GenerateReportRequest struct {
	InterviewID  string `json:"interviewId"`
	IncludeNotes bool   `json:"includeNotes"`
}

type DashboardStats struct {
	TotalInterviews       int            `json:"totalInterviews"`
	PendingEvaluations    int            `json:"pendingEvaluations"`
	CompletedReports      int            `json:"completedReports"`
	AverageEvaluationTime float64        `json:"averageEvaluationTime"` // hours
	InterviewsByStatus    map[string]int `json:"interviewsByStatus"`
	EvaluationsByStatus   map[string]int `json:"evaluationsByStatus"`
	RecentActivity        []ActivityItem `json:"recentActivity"`
}

type ActivityItem struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"` // interview, evaluation, report
	Action    string    `json:"action"`
	Timestamp time.Time `json:"timestamp"`
	User      string    `json:"user,omitempty"`
}

type QueueStats struct {
	Name                  string    `json:"name"`
	Description           string    `json:"description"`
	MessagesInQueue       int       `json:"messagesInQueue"`
	MessagesProcessing    int       `json:"messagesProcessing"`
	MessagesCompleted     int       `json:"messagesCompleted"`
	MessagesFailed        int       `json:"messagesFailed"`
	AverageProcessingTime float64   `json:"averageProcessingTime"` // seconds
	Status                string    `json:"status"`
	LastProcessed         time.Time `json:"lastProcessed"`
}

type QueueMessage struct {
	ID          string     `json:"id"`
	QueueName   string     `json:"queueName"`
	Type        string     `json:"type"`
	Subject     string     `json:"subject"`
	Priority    int        `json:"priority"`
	Status      string     `json:"status"`
	CreatedAt   time.Time  `json:"createdAt"`
	ProcessedAt *time.Time `json:"processedAt,omitempty"`
}

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

type PaginatedResponse struct {
	Success    bool        `json:"success"`
	Data       interface{} `json:"data"`
	Pagination Pagination  `json:"pagination"`
}

type Pagination struct {
	Page       int `json:"page"`
	PageSize   int `json:"pageSize"`
	TotalItems int `json:"totalItems"`
	TotalPages int `json:"totalPages"`
}
