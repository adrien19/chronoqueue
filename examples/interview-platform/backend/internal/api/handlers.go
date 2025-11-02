package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/adrien19/chronoqueue/client"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/db"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/sse"
)

// Handlers contains all HTTP handlers
type Handlers struct {
	db          *db.Database
	queue       *client.ChronoQueueClient
	broadcaster *sse.Broadcaster
}

// NewHandlers creates a new handlers instance
func NewHandlers(database *db.Database, queueClient *client.ChronoQueueClient, broadcaster *sse.Broadcaster) *Handlers {
	return &Handlers{
		db:          database,
		queue:       queueClient,
		broadcaster: broadcaster,
	}
}

// Helper functions

func respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("ERROR: Failed to encode JSON response: %v, data: %+v", err, data)
	}
}

func respondError(w http.ResponseWriter, status int, message string) {
	respondJSON(w, status, models.APIResponse{
		Success: false,
		Error:   message,
	})
}

func respondSuccess(w http.ResponseWriter, data interface{}) {
	respondJSON(w, http.StatusOK, models.APIResponse{
		Success: true,
		Data:    data,
	})
}

// GetDashboardStats returns dashboard statistics
func (h *Handlers) GetDashboardStats(w http.ResponseWriter, r *http.Request) {
	// Get counts using ListInterviews
	_, totalInterviews, _ := h.db.ListInterviews("all", 1, 0)
	_, scheduledCount, _ := h.db.ListInterviews(string(models.StatusScheduled), 1, 0)
	_, completedCount, _ := h.db.ListInterviews(string(models.StatusCompleted), 1, 0)
	_, cancelledCount, _ := h.db.ListInterviews(string(models.StatusCancelled), 1, 0)

	_, pendingEvals, _ := h.db.ListEvaluations(string(models.EvalStatusPending), 1, 0)
	_, completedEvals, _ := h.db.ListEvaluations(string(models.EvalStatusCompleted), 1, 0)

	_, completedReportsCount, _ := h.db.ListReports(string(models.ReportStatusReady), 1, 0)

	stats := models.DashboardStats{
		TotalInterviews:       totalInterviews,
		PendingEvaluations:    pendingEvals,
		CompletedReports:      completedReportsCount,
		AverageEvaluationTime: 24.0, // TODO: Calculate actual average
		InterviewsByStatus: map[string]int{
			"scheduled": scheduledCount,
			"completed": completedCount,
			"cancelled": cancelledCount,
		},
		EvaluationsByStatus: map[string]int{
			"pending":   pendingEvals,
			"completed": completedEvals,
		},
		RecentActivity: []models.ActivityItem{},
	}

	respondSuccess(w, stats)
}

// GetDashboardActivity returns recent activity
func (h *Handlers) GetDashboardActivity(w http.ResponseWriter, r *http.Request) {
	// TODO: Implement actual activity tracking
	activity := []models.ActivityItem{}
	respondSuccess(w, activity)
}

// SSEHandler handles Server-Sent Events connections
func (h *Handlers) SSEHandler(w http.ResponseWriter, r *http.Request) {
	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Generate client ID
	clientID := fmt.Sprintf("client-%d", time.Now().UnixNano())

	// Register client
	client := h.broadcaster.AddClient(clientID)
	defer h.broadcaster.RemoveClient(clientID)

	// Send initial connection message
	fmt.Fprintf(w, "event: connected\ndata: {\"clientId\":\"%s\",\"timestamp\":\"%s\"}\n\n", clientID, time.Now().Format(time.RFC3339))
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// Keep connection alive and send events
	for {
		select {
		case <-r.Context().Done():
			// Client disconnected
			log.Printf("[SSE] Client %s disconnected via context", clientID)
			return
		case event, ok := <-client.Channel:
			if !ok {
				// Channel closed
				return
			}
			// Send event to client
			eventData := sse.FormatSSE(event)
			fmt.Fprint(w, eventData)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		case <-time.After(30 * time.Second):
			// Send keepalive ping
			fmt.Fprintf(w, ": keepalive\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}

// ListInterviews returns a list of interviews
func (h *Handlers) ListInterviews(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "all"
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	offset := (page - 1) * pageSize

	interviews, total, err := h.db.ListInterviews(status, pageSize, offset)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	totalPages := (total + pageSize - 1) / pageSize

	respondJSON(w, http.StatusOK, models.PaginatedResponse{
		Success: true,
		Data:    interviews,
		Pagination: models.Pagination{
			Page:       page,
			PageSize:   pageSize,
			TotalItems: total,
			TotalPages: totalPages,
		},
	})
}

// GetInterview returns a single interview
func (h *Handlers) GetInterview(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	interview, err := h.db.GetInterview(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Interview not found")
		return
	}

	respondSuccess(w, interview)
}

// CreateInterview creates a new interview
func (h *Handlers) CreateInterview(w http.ResponseWriter, r *http.Request) {
	var req models.CreateInterviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	interview := &models.Interview{
		CandidateName:  req.CandidateName,
		CandidateEmail: req.CandidateEmail,
		Position:       req.Position,
		ScheduledAt:    req.ScheduledAt,
		Duration:       req.Duration,
		InterviewerIDs: req.InterviewerIDs,
		Tags:           req.Tags,
	}

	if err := h.db.CreateInterview(interview); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to create interview")
		return
	}

	// Broadcast SSE event immediately after successful creation
	h.broadcaster.Broadcast(sse.EventInterviewCreated, map[string]interface{}{
		"id":            interview.ID,
		"candidateName": interview.CandidateName,
		"position":      interview.Position,
		"scheduledAt":   interview.ScheduledAt.Format(time.RFC3339),
		"status":        interview.Status,
	})

	// Post message to ChronoQueue for scheduling notifications
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create payload as a map (convert time.Time to string for structpb compatibility)
	payloadMap := map[string]interface{}{
		"interview_id":    interview.ID,
		"candidate_name":  interview.CandidateName,
		"candidate_email": interview.CandidateEmail,
		"scheduled_at":    interview.ScheduledAt.Format(time.RFC3339),
		"action":          "schedule_interview",
	}

	// Convert to structpb.Struct
	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err != nil {
		log.Printf("Failed to create payload struct: %v", err)
	} else if h.queue == nil {
		log.Printf("ERROR: ChronoQueue client is nil!")
	} else {
		log.Printf("Posting message to interview-scheduler queue for interview ID %s", interview.ID)
		messageID := fmt.Sprintf("interview-%s-%d", interview.ID, time.Now().Unix())
		resp, err := h.queue.PostMessage(ctx, "interview-scheduler", messageID, client.MessageOptions{
			Payload: client.Payload{
				Data:        payloadStruct,
				ContentType: "application/json",
			},
			MaxAttempts:   3,
			LeaseDuration: "5s",
		})

		if err != nil || !resp.Success {
			log.Printf("Failed to post message to interview-scheduler queue: %v", err)
		} else {
			log.Printf("Successfully posted message %s to interview-scheduler queue", messageID)
		}
	}

	respondSuccess(w, interview)
}

// UpdateInterview updates an existing interview
func (h *Handlers) UpdateInterview(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var updates map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if err := h.db.UpdateInterview(id, updates); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to update interview")
		return
	}

	interview, _ := h.db.GetInterview(id)

	// Broadcast update event
	h.broadcaster.Broadcast(sse.EventInterviewUpdated, map[string]interface{}{
		"id":            interview.ID,
		"candidateName": interview.CandidateName,
		"status":        interview.Status,
		"scheduledAt":   interview.ScheduledAt.Format(time.RFC3339),
	})

	respondSuccess(w, interview)
}

// CancelInterview cancels an interview
func (h *Handlers) CancelInterview(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.db.UpdateInterviewStatus(id, models.StatusCancelled); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to cancel interview")
		return
	}

	interview, err := h.db.GetInterview(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Interview not found")
		return
	}

	// Broadcast SSE event
	h.broadcaster.Broadcast(sse.EventInterviewUpdated, map[string]interface{}{
		"id":            interview.ID,
		"candidateName": interview.CandidateName,
		"status":        interview.Status,
		"action":        "cancelled",
	})

	respondSuccess(w, interview)
}

// StartInterview marks an interview as in progress
func (h *Handlers) StartInterview(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.db.UpdateInterviewStatus(id, models.StatusInProgress); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to start interview")
		return
	}

	interview, err := h.db.GetInterview(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Interview not found")
		return
	}

	// Broadcast SSE event
	h.broadcaster.Broadcast(sse.EventInterviewUpdated, map[string]interface{}{
		"id":            interview.ID,
		"candidateName": interview.CandidateName,
		"status":        interview.Status,
		"action":        "started",
	})

	respondSuccess(w, interview)
}

// CompleteInterview marks an interview as completed and triggers evaluation requests
func (h *Handlers) CompleteInterview(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := h.db.UpdateInterviewStatus(id, models.StatusCompleted); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to complete interview")
		return
	}

	interview, err := h.db.GetInterview(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Interview not found")
		return
	}

	// Broadcast SSE event
	h.broadcaster.Broadcast(sse.EventInterviewUpdated, map[string]interface{}{
		"id":            interview.ID,
		"candidateName": interview.CandidateName,
		"status":        interview.Status,
		"action":        "completed",
	})

	// Send notification messages to request evaluations from interviewers
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, interviewerEmail := range interview.InterviewerIDs {
		payloadMap := map[string]interface{}{
			"interview_id":      interview.ID,
			"candidate_name":    interview.CandidateName,
			"position":          interview.Position,
			"interviewer_email": interviewerEmail,
			"notification_type": "evaluation_request",
			"subject":           fmt.Sprintf("Evaluation Required: %s - %s", interview.CandidateName, interview.Position),
		}

		payload, err := structpb.NewStruct(payloadMap)
		if err != nil {
			log.Printf("Failed to create payload for evaluation request: %v", err)
			continue
		}

		messageID := fmt.Sprintf("eval-request-%s-%s", interview.ID, interviewerEmail)
		_, err = h.queue.PostMessage(ctx, "notification-sender", messageID, client.MessageOptions{
			Payload: client.Payload{
				Data:        payload,
				ContentType: "application/json",
			},
			MaxAttempts:   3,
			LeaseDuration: "5s",
		})

		if err != nil {
			log.Printf("Failed to send evaluation request notification for interviewer %s: %v", interviewerEmail, err)
		} else {
			log.Printf("Sent evaluation request notification to %s for interview %s", interviewerEmail, interview.ID)
		}
	}

	respondSuccess(w, interview)
}

// GetInterviewEvaluations returns evaluations for an interview
func (h *Handlers) GetInterviewEvaluations(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	evaluations, err := h.db.GetEvaluationsByInterview(id)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to get evaluations")
		return
	}

	respondSuccess(w, evaluations)
}

// GetInterviewReport returns the report for an interview
func (h *Handlers) GetInterviewReport(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	report, err := h.db.GetReportByInterview(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Report not found")
		return
	}

	respondSuccess(w, report)
}

// Evaluation handlers

func (h *Handlers) ListEvaluations(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}
	offset := (page - 1) * pageSize

	evaluations, total, err := h.db.ListEvaluations(status, pageSize, offset)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	totalPages := (total + pageSize - 1) / pageSize
	respondJSON(w, http.StatusOK, models.PaginatedResponse{
		Success: true,
		Data:    evaluations,
		Pagination: models.Pagination{
			Page:       page,
			PageSize:   pageSize,
			TotalItems: total,
			TotalPages: totalPages,
		},
	})
}

func (h *Handlers) CreateEvaluation(w http.ResponseWriter, r *http.Request) {
	var req models.CreateEvaluationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	evaluation := &models.Evaluation{
		InterviewID:         req.InterviewID,
		EvaluatorName:       req.EvaluatorName,
		EvaluatorEmail:      req.EvaluatorEmail,
		TechnicalScore:      req.TechnicalScore,
		CommunicationScore:  req.CommunicationScore,
		ProblemSolvingScore: req.ProblemSolvingScore,
		CultureFitScore:     req.CultureFitScore,
		Strengths:           req.Strengths,
		Weaknesses:          req.Weaknesses,
		Recommendation:      req.Recommendation,
		Comments:            req.Comments,
		Status:              models.EvalStatusPending,
	}

	if err := h.db.CreateEvaluation(evaluation); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to create evaluation")
		return
	}

	// Broadcast SSE event immediately after successful creation
	h.broadcaster.Broadcast(sse.EventEvaluationCreated, map[string]interface{}{
		"id":             evaluation.ID,
		"interviewId":    evaluation.InterviewID,
		"overallScore":   evaluation.OverallScore,
		"recommendation": evaluation.Recommendation,
		"status":         evaluation.Status,
	})

	// Post message to ChronoQueue for evaluation processing
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	payloadMap := map[string]interface{}{
		"evaluation_id": evaluation.ID,
		"interview_id":  evaluation.InterviewID,
		"overall_score": evaluation.OverallScore,
		"action":        "process_evaluation",
	}

	payloadStruct, err := structpb.NewStruct(payloadMap)
	if err == nil {
		messageID := fmt.Sprintf("evaluation-%s-%d", evaluation.ID, time.Now().Unix())
		_, _ = h.queue.PostMessage(ctx, "evaluation-processor", messageID, client.MessageOptions{
			Payload: client.Payload{
				Data:        payloadStruct,
				ContentType: "application/json",
			},
			MaxAttempts:   3,
			LeaseDuration: "5s",
		})
	}

	respondSuccess(w, evaluation)
}

func (h *Handlers) GetPendingEvaluations(w http.ResponseWriter, r *http.Request) {
	evaluations, _, err := h.db.ListEvaluations(string(models.EvalStatusPending), 100, 0)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to get pending evaluations")
		return
	}
	respondSuccess(w, evaluations)
}

func (h *Handlers) GetEvaluation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	evaluation, err := h.db.GetEvaluation(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Evaluation not found")
		return
	}
	respondSuccess(w, evaluation)
}

func (h *Handlers) UpdateEvaluation(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var updates map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// check if evaluation exists
	_, err := h.db.GetEvaluation(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Evaluation not found")
		return
	}

	// Update status if provided
	if status, ok := updates["status"].(string); ok {
		if err := h.db.UpdateEvaluationStatus(id, models.EvaluationStatus(status)); err != nil {
			respondError(w, http.StatusInternalServerError, "Failed to update evaluation")
			return
		}
	}

	evaluation, err := h.db.GetEvaluation(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Evaluation not found")
		return
	}
	respondSuccess(w, evaluation)
}

// ListReports returns all reports with optional filtering
func (h *Handlers) ListReports(w http.ResponseWriter, r *http.Request) {
	status := r.URL.Query().Get("status")
	if status == "" {
		status = "all"
	}

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	offset := (page - 1) * pageSize

	reports, total, err := h.db.ListReports(status, pageSize, offset)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	totalPages := (total + pageSize - 1) / pageSize

	respondJSON(w, http.StatusOK, models.PaginatedResponse{
		Success: true,
		Data:    reports,
		Pagination: models.Pagination{
			Page:       page,
			PageSize:   pageSize,
			TotalItems: total,
			TotalPages: totalPages,
		},
	})
}

// GenerateReport creates a report generation request and queues it for processing
func (h *Handlers) GenerateReport(w http.ResponseWriter, r *http.Request) {
	var req models.GenerateReportRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Validate interview exists
	interview, err := h.db.GetInterview(req.InterviewID)
	if err != nil {
		respondError(w, http.StatusNotFound, "Interview not found")
		return
	}

	// Check if report already exists for this interview
	existingReport, _ := h.db.GetReportByInterview(req.InterviewID)
	if existingReport != nil {
		respondError(w, http.StatusConflict, "Report already exists for this interview")
		return
	}

	// Get all evaluations for this interview to calculate averages
	evaluations, err := h.db.GetEvaluationsByInterview(req.InterviewID)
	if err != nil || len(evaluations) == 0 {
		respondError(w, http.StatusBadRequest, "No evaluations found for this interview")
		return
	}

	// Calculate averages
	var totalTechnical, totalCommunication, totalCultureFit, totalOverall float64
	for _, eval := range evaluations {
		totalTechnical += float64(eval.TechnicalScore)
		totalCommunication += float64(eval.CommunicationScore)
		totalCultureFit += float64(eval.CultureFitScore)
		totalOverall += float64(eval.OverallScore)
	}
	count := float64(len(evaluations))

	// Determine final recommendation based on average overall score
	avgOverall := totalOverall / count
	var finalRec string
	switch {
	case avgOverall >= 9:
		finalRec = string(models.RecommendationStrongHire)
	case avgOverall >= 7:
		finalRec = string(models.RecommendationHire)
	case avgOverall >= 5:
		finalRec = string(models.RecommendationMaybe)
	default:
		finalRec = string(models.RecommendationNoHire)
	}

	// Create report in database
	report := &models.Report{
		InterviewID:          req.InterviewID,
		CandidateName:        interview.CandidateName,
		Position:             interview.Position,
		InterviewDate:        interview.ScheduledAt,
		EvaluationCount:      len(evaluations),
		AverageTechnical:     totalTechnical / count,
		AverageCommunication: totalCommunication / count,
		AverageCultureFit:    totalCultureFit / count,
		AverageOverall:       avgOverall,
		FinalRecommendation:  finalRec,
		Status:               models.ReportStatusPending,
	}

	if err := h.db.CreateReport(report); err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to create report")
		return
	}

	// Broadcast SSE event immediately after successful creation
	h.broadcaster.Broadcast(sse.EventReportGenerated, map[string]interface{}{
		"id":                  report.ID,
		"interviewId":         report.InterviewID,
		"candidateName":       report.CandidateName,
		"position":            report.Position,
		"finalRecommendation": report.FinalRecommendation,
		"averageOverall":      report.AverageOverall,
		"status":              report.Status,
	})

	// Post message to report-generator queue for PDF generation
	ctx := context.Background()
	messageID := fmt.Sprintf("report-%s-%d", report.ID, time.Now().Unix())

	payloadMap := map[string]interface{}{
		"report_id":      report.ID,
		"interview_id":   req.InterviewID,
		"include_notes":  req.IncludeNotes,
		"candidate_name": interview.CandidateName,
		"position":       interview.Position,
	}
	payloadStruct, _ := structpb.NewStruct(payloadMap)

	_, err = h.queue.PostMessage(ctx, "report-generator", messageID, client.MessageOptions{
		Payload: client.Payload{
			Data:        payloadStruct,
			ContentType: "application/json",
		},
		MaxAttempts:   3,
		LeaseDuration: "5s",
	})
	if err != nil {
		log.Printf("Failed to queue report generation: %v, failed report ID: %s", err, report.ID)
		// Report is created but queue failed - don't fail request
		// Worker can pick it up later or admin can retry
	}

	respondSuccess(w, report)
}

// GetReport retrieves a report by ID
func (h *Handlers) GetReport(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		respondError(w, http.StatusBadRequest, "Report ID is required")
		return
	}

	report, err := h.db.GetReport(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Report not found")
		return
	}

	respondSuccess(w, report)
}

// SendReport queues a report to be sent via email
func (h *Handlers) SendReport(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		respondError(w, http.StatusBadRequest, "Report ID is required")
		return
	}

	report, err := h.db.GetReport(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Report not found")
		return
	}

	// Check if report is ready to be sent
	if report.Status != models.ReportStatusReady {
		respondError(w, http.StatusBadRequest, "Report is not ready to be sent")
		return
	}

	// Post message to notification-sender queue
	ctx := context.Background()
	messageID := fmt.Sprintf("send-report-%s-%d", report.ID, time.Now().Unix())

	payloadMap := map[string]interface{}{
		"report_id":      report.ID,
		"interview_id":   report.InterviewID,
		"candidate_name": report.CandidateName,
		"position":       report.Position,
		"recipient":      "hiring-manager@company.com", // TODO: Get from request or config
		"subject":        fmt.Sprintf("Interview Report for %s - %s", report.CandidateName, report.Position),
	}
	payloadStruct, _ := structpb.NewStruct(payloadMap)

	_, err = h.queue.PostMessage(ctx, "notification-sender", messageID, client.MessageOptions{
		Payload: client.Payload{
			Data:        payloadStruct,
			ContentType: "application/json",
		},
		MaxAttempts:   3,
		LeaseDuration: "5s",
	})
	if err != nil {
		respondError(w, http.StatusInternalServerError, "Failed to queue report sending")
		return
	}

	respondSuccess(w, map[string]string{
		"message": "Report queued for sending",
		"status":  "pending",
	})
}

// DownloadReportPDF serves the PDF file for a report
func (h *Handlers) DownloadReportPDF(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		respondError(w, http.StatusBadRequest, "Report ID is required")
		return
	}

	report, err := h.db.GetReport(id)
	if err != nil {
		respondError(w, http.StatusNotFound, "Report not found")
		return
	}

	// Check if report PDF is ready
	if report.Status == models.ReportStatusPending {
		respondError(w, http.StatusNotFound, "Report PDF is not ready yet")
		return
	}

	// TODO: In a real implementation, this would fetch the PDF from storage
	// For now, return a placeholder response
	w.Header().Set("Content-Type", "application/json")
	respondSuccess(w, map[string]string{
		"message":   "PDF download endpoint - implementation pending",
		"report_id": id,
		"status":    string(report.Status),
		"note":      "Worker service will generate PDF and store it in cloud storage",
	})
}

func (h *Handlers) ListQueues(w http.ResponseWriter, r *http.Request) {
	// Define our known queues
	queueNames := []string{
		"interview-scheduler",
		"evaluation-processor",
		"report-generator",
		"notification-sender",
	}

	queueDescriptions := map[string]string{
		"interview-scheduler":  "Handles interview scheduling and notifications",
		"evaluation-processor": "Processes evaluation submissions and calculations",
		"report-generator":     "Generates consolidated interview reports",
		"notification-sender":  "Sends email and SMS notifications",
	}

	var queueStats []models.QueueStats

	// Fetch stats for each queue
	for _, queueName := range queueNames {
		stats := models.QueueStats{
			Name:                  queueName,
			Description:           queueDescriptions[queueName],
			MessagesInQueue:       0,
			MessagesProcessing:    0,
			MessagesCompleted:     0,
			MessagesFailed:        0,
			AverageProcessingTime: 0,
			Status:                "active",
			LastProcessed:         time.Now(),
		}

		// Fetch real stats from ChronoQueue using persistent client
		ctx := r.Context()
		if h.queue != nil {
			stateResp, err := h.queue.GetQueueState(ctx, queueName)
			if err != nil {
				log.Printf("Error getting queue state for %s: %v", queueName, err)
			} else if stateResp != nil {
				stateCounts := stateResp.GetStateCounts()

				stats.MessagesInQueue = int(stateCounts["PENDING"])
				stats.MessagesProcessing = int(stateCounts["RUNNING"])
				stats.MessagesCompleted = int(stateCounts["COMPLETED"])
				stats.MessagesFailed = int(stateCounts["ERRORED"])
			}
		} else {
			log.Printf("Queue client is nil, cannot fetch real stats for %s", queueName)
		}

		queueStats = append(queueStats, stats)
	}

	log.Printf("Returning queue stats for %d queues", len(queueStats))
	respondSuccess(w, queueStats)
}

func (h *Handlers) GetQueueStats(w http.ResponseWriter, r *http.Request) {
	queueName := chi.URLParam(r, "queueName")

	if queueName == "" {
		respondError(w, http.StatusBadRequest, "queue name is required")
		return
	}

	queueDescriptions := map[string]string{
		"interview-scheduler":  "Handles interview scheduling and notifications",
		"evaluation-processor": "Processes evaluation submissions and calculations",
		"report-generator":     "Generates consolidated interview reports",
		"notification-sender":  "Sends email and SMS notifications",
	}

	stats := models.QueueStats{
		Name:                  queueName,
		Description:           queueDescriptions[queueName],
		MessagesInQueue:       0,
		MessagesProcessing:    0,
		MessagesCompleted:     0,
		MessagesFailed:        0,
		AverageProcessingTime: 0,
		Status:                "active",
		LastProcessed:         time.Now(),
	}

	// Fetch real stats from ChronoQueue using persistent client
	ctx := r.Context()
	if h.queue != nil {
		stateResp, err := h.queue.GetQueueState(ctx, queueName)
		if err != nil {
			log.Printf("Error getting queue state for %s: %v", queueName, err)
		} else if stateResp != nil {
			stateCounts := stateResp.GetStateCounts()

			stats.MessagesInQueue = int(stateCounts["PENDING"])
			stats.MessagesProcessing = int(stateCounts["RUNNING"])
			stats.MessagesCompleted = int(stateCounts["COMPLETED"])
			stats.MessagesFailed = int(stateCounts["ERRORED"])
		}
	} else {
		log.Printf("Queue client is nil, cannot fetch real stats for %s", queueName)
	}

	respondSuccess(w, stats)
}

func (h *Handlers) GetRecentMessages(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	queueNames := []string{
		"interview-scheduler",
		"evaluation-processor",
		"report-generator",
		"notification-sender",
	}

	var allMessages []models.QueueMessage

	// Use the persistent client from handlers (best practice for production)
	if h.queue == nil {
		log.Printf("Queue client is nil, cannot fetch recent messages")
		respondSuccess(w, []models.QueueMessage{})
		return
	}

	timeRange := client.TimeRangeOption{
		Min: time.Now().Add(-24 * time.Hour).Unix(),
		Max: 0,
	}

	log.Printf("Querying queues for recent messages: %v", timeRange)

	// Collect messages from all queues using PeekQueueMessages
	for _, queueName := range queueNames {
		// Peek last 10 messages from each queue within 24 hours
		// timeRange := client.TimeRangeOption{
		// 	Min: 0,
		// 	Max: 0,
		// }

		peekResp, err := h.queue.PeekQueueMessages(ctx, queueName, 10, timeRange)
		if err != nil {
			log.Printf("Error peeking messages for %s: %v", queueName, err)
			continue
		}

		if peekResp == nil {
			log.Printf("PeekQueueMessages returned nil response for %s", queueName)
			continue
		}

		messages := peekResp.GetMessages()
		log.Printf("Successfully peeked %d messages from queue %s", len(messages), queueName)

		// Convert proto messages to our model
		for _, msg := range messages {
			if msg == nil {
				log.Printf("Encountered nil message in queue %s", queueName)
				continue
			}

			metadata := msg.GetMetadata()
			if metadata == nil {
				log.Printf("Message metadata is nil for message %s", msg.GetMessageId())
				continue
			}

			// Use current time as approximation for creation time
			// The actual creation time is not reliably available from peek response
			createdAt := time.Now().Add(-time.Duration(len(allMessages)) * time.Minute)

			queueMsg := models.QueueMessage{
				ID:        msg.GetMessageId(),
				QueueName: queueName,
				Type:      "", // Will be extracted from payload if available
				Subject:   msg.GetMessageId(),
				Priority:  int(metadata.GetPriority()),
				Status:    mapMessageState(int32(metadata.GetState())),
				CreatedAt: createdAt,
			}

			// Try to get type from payload
			if payload := metadata.GetPayload(); payload != nil {
				if data := payload.GetData(); data != nil {
					fields := data.AsMap()
					if action, ok := fields["action"].(string); ok {
						queueMsg.Type = action
					} else if notificationType, ok := fields["notification_type"].(string); ok {
						queueMsg.Type = notificationType
					}
				}
			}

			// Set processed time if completed (use approximation based on creation time)
			if metadata.GetState() == 3 { // COMPLETED
				processedTime := createdAt.Add(30 * time.Second)
				queueMsg.ProcessedAt = &processedTime
			}

			allMessages = append(allMessages, queueMsg)
		}
	}

	// Sort messages by creation time (newest first) and limit to 20	// Sort messages by creation time (newest first) and limit to 20
	if len(allMessages) > 1 {
		for i := 0; i < len(allMessages)-1; i++ {
			for j := i + 1; j < len(allMessages); j++ {
				if allMessages[i].CreatedAt.Before(allMessages[j].CreatedAt) {
					allMessages[i], allMessages[j] = allMessages[j], allMessages[i]
				}
			}
		}
	}

	// Limit to 20 most recent
	if len(allMessages) > 20 {
		allMessages = allMessages[:20]
	}

	log.Printf("Returning %d recent messages across all queues: \n%v\n", len(allMessages), allMessages)
	log.Printf("DEBUG: About to call respondSuccess with %d messages", len(allMessages))
	log.Printf("DEBUG: First message (if exists): %+v", func() interface{} {
		if len(allMessages) > 0 {
			return allMessages[0]
		}
		return "no messages"
	}())
	respondSuccess(w, allMessages)
}

// Helper function to map proto message state to string
func mapMessageState(state int32) string {
	switch state {
	case 0:
		return "invisible"
	case 1:
		return "pending"
	case 2:
		return "processing"
	case 3:
		return "completed"
	case 4:
		return "canceled"
	case 5:
		return "failed"
	default:
		return "unknown"
	}
}
