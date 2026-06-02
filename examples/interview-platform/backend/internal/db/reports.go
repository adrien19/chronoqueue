package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
)

// CreateReport creates a new report
func (d *Database) CreateReport(report *models.Report) error {
	report.ID = uuid.New().String()
	report.CreatedAt = time.Now()
	report.UpdatedAt = time.Now()

	var generatedAt, sentAt interface{}
	if report.GeneratedAt != nil {
		generatedAt = formatTime(*report.GeneratedAt)
	}
	if report.SentAt != nil {
		sentAt = formatTime(*report.SentAt)
	}

	query := `
		INSERT INTO reports (id, interview_id, candidate_name, position, interview_date,
			evaluation_count, average_technical, average_communication, average_culture_fit,
			average_overall, final_recommendation, status, generated_at, sent_at, 
			created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := d.db.Exec(
		query,
		report.ID,
		report.InterviewID,
		report.CandidateName,
		report.Position,
		formatTime(report.InterviewDate),
		report.EvaluationCount,
		report.AverageTechnical,
		report.AverageCommunication,
		report.AverageCultureFit,
		report.AverageOverall,
		report.FinalRecommendation,
		report.Status,
		generatedAt,
		sentAt,
		formatTime(report.CreatedAt),
		formatTime(report.UpdatedAt),
	)

	return err
}

// GetReport retrieves a report by ID
func (d *Database) GetReport(id string) (*models.Report, error) {
	query := `
		SELECT id, interview_id, candidate_name, position, interview_date, evaluation_count,
			average_technical, average_communication, average_culture_fit, average_overall,
			final_recommendation, status, generated_at, sent_at, created_at, updated_at
		FROM reports
		WHERE id = ?
	`

	var report models.Report
	var interviewDate, createdAt, updatedAt string
	var generatedAt, sentAt sql.NullString

	err := d.db.QueryRow(query, id).Scan(
		&report.ID,
		&report.InterviewID,
		&report.CandidateName,
		&report.Position,
		&interviewDate,
		&report.EvaluationCount,
		&report.AverageTechnical,
		&report.AverageCommunication,
		&report.AverageCultureFit,
		&report.AverageOverall,
		&report.FinalRecommendation,
		&report.Status,
		&generatedAt,
		&sentAt,
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("report not found")
	}
	if err != nil {
		return nil, err
	}

	report.InterviewDate, _ = parseTime(interviewDate)
	report.CreatedAt, _ = parseTime(createdAt)
	report.UpdatedAt, _ = parseTime(updatedAt)

	if generatedAt.Valid {
		t, _ := parseTime(generatedAt.String)
		report.GeneratedAt = &t
	}
	if sentAt.Valid {
		t, _ := parseTime(sentAt.String)
		report.SentAt = &t
	}

	return &report, nil
}

// GetReportByInterview retrieves a report by interview ID
func (d *Database) GetReportByInterview(interviewID string) (*models.Report, error) {
	query := `
		SELECT id, interview_id, candidate_name, position, interview_date, evaluation_count,
			average_technical, average_communication, average_culture_fit, average_overall,
			final_recommendation, status, generated_at, sent_at, created_at, updated_at
		FROM reports
		WHERE interview_id = ?
		ORDER BY created_at DESC
		LIMIT 1
	`

	var report models.Report
	var interviewDate, createdAt, updatedAt string
	var generatedAt, sentAt sql.NullString

	err := d.db.QueryRow(query, interviewID).Scan(
		&report.ID,
		&report.InterviewID,
		&report.CandidateName,
		&report.Position,
		&interviewDate,
		&report.EvaluationCount,
		&report.AverageTechnical,
		&report.AverageCommunication,
		&report.AverageCultureFit,
		&report.AverageOverall,
		&report.FinalRecommendation,
		&report.Status,
		&generatedAt,
		&sentAt,
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("report not found")
	}
	if err != nil {
		return nil, err
	}

	report.InterviewDate, _ = parseTime(interviewDate)
	report.CreatedAt, _ = parseTime(createdAt)
	report.UpdatedAt, _ = parseTime(updatedAt)

	if generatedAt.Valid {
		t, _ := parseTime(generatedAt.String)
		report.GeneratedAt = &t
	}
	if sentAt.Valid {
		t, _ := parseTime(sentAt.String)
		report.SentAt = &t
	}

	return &report, nil
}

// ListReports retrieves all reports with optional filtering
func (d *Database) ListReports(status string, limit, offset int) ([]*models.Report, int, error) {
	var reports []*models.Report
	var query string
	var args []interface{}

	if status != "" && status != "all" {
		query = `
			SELECT id, interview_id, candidate_name, position, interview_date, evaluation_count,
				average_technical, average_communication, average_culture_fit, average_overall,
				final_recommendation, status, generated_at, sent_at, created_at, updated_at
			FROM reports
			WHERE status = ?
			ORDER BY created_at DESC
			LIMIT ? OFFSET ?
		`
		args = []interface{}{status, limit, offset}
	} else {
		query = `
			SELECT id, interview_id, candidate_name, position, interview_date, evaluation_count,
				average_technical, average_communication, average_culture_fit, average_overall,
				final_recommendation, status, generated_at, sent_at, created_at, updated_at
			FROM reports
			ORDER BY created_at DESC
			LIMIT ? OFFSET ?
		`
		args = []interface{}{limit, offset}
	}

	rows, err := d.db.Query(query, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var report models.Report
		var interviewDate, createdAt, updatedAt string
		var generatedAt, sentAt sql.NullString

		err := rows.Scan(
			&report.ID,
			&report.InterviewID,
			&report.CandidateName,
			&report.Position,
			&interviewDate,
			&report.EvaluationCount,
			&report.AverageTechnical,
			&report.AverageCommunication,
			&report.AverageCultureFit,
			&report.AverageOverall,
			&report.FinalRecommendation,
			&report.Status,
			&generatedAt,
			&sentAt,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, 0, err
		}

		report.InterviewDate, _ = parseTime(interviewDate)
		report.CreatedAt, _ = parseTime(createdAt)
		report.UpdatedAt, _ = parseTime(updatedAt)

		if generatedAt.Valid {
			t, _ := parseTime(generatedAt.String)
			report.GeneratedAt = &t
		}
		if sentAt.Valid {
			t, _ := parseTime(sentAt.String)
			report.SentAt = &t
		}

		reports = append(reports, &report)
	}

	// Get total count
	var countQuery string
	var countArgs []interface{}
	if status != "" && status != "all" {
		countQuery = "SELECT COUNT(*) FROM reports WHERE status = ?"
		countArgs = []interface{}{status}
	} else {
		countQuery = "SELECT COUNT(*) FROM reports"
	}

	var total int
	err = d.db.QueryRow(countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	return reports, total, nil
}

// UpdateReportStatus updates the status of a report
func (d *Database) UpdateReportStatus(id string, status models.ReportStatus) error {
	query := `UPDATE reports SET status = ?, updated_at = ? WHERE id = ?`
	_, err := d.db.Exec(query, status, formatTime(time.Now()), id)
	return err
}

// MarkReportAsSent marks a report as sent
func (d *Database) MarkReportAsSent(id string) error {
	now := time.Now()
	query := `UPDATE reports SET status = ?, sent_at = ?, updated_at = ? WHERE id = ?`
	_, err := d.db.Exec(query, models.ReportStatusSent, formatTime(now), formatTime(now), id)
	return err
}
