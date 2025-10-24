package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
)

// CreateInterview creates a new interview
func (d *Database) CreateInterview(interview *models.Interview) error {
	interview.ID = uuid.New().String()
	interview.CreatedAt = time.Now()
	interview.UpdatedAt = time.Now()
	interview.Status = models.StatusPending

	interviewerIDsJSON, _ := json.Marshal(interview.InterviewerIDs)
	tagsJSON, _ := json.Marshal(interview.Tags)

	query := `
		INSERT INTO interviews (id, candidate_name, candidate_email, position, scheduled_at, 
			duration, status, interviewer_ids, tags, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := d.db.Exec(query,
		interview.ID,
		interview.CandidateName,
		interview.CandidateEmail,
		interview.Position,
		formatTime(interview.ScheduledAt),
		interview.Duration,
		interview.Status,
		string(interviewerIDsJSON),
		string(tagsJSON),
		formatTime(interview.CreatedAt),
		formatTime(interview.UpdatedAt),
	)

	return err
}

// GetInterview retrieves an interview by ID
func (d *Database) GetInterview(id string) (*models.Interview, error) {
	query := `
		SELECT id, candidate_name, candidate_email, position, scheduled_at, duration, 
			status, interviewer_ids, tags, created_at, updated_at
		FROM interviews
		WHERE id = ?
	`

	var interview models.Interview
	var scheduledAt, createdAt, updatedAt string
	var interviewerIDsJSON, tagsJSON string

	err := d.db.QueryRow(query, id).Scan(
		&interview.ID,
		&interview.CandidateName,
		&interview.CandidateEmail,
		&interview.Position,
		&scheduledAt,
		&interview.Duration,
		&interview.Status,
		&interviewerIDsJSON,
		&tagsJSON,
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("interview not found")
	}
	if err != nil {
		return nil, err
	}

	interview.ScheduledAt, _ = parseTime(scheduledAt)
	interview.CreatedAt, _ = parseTime(createdAt)
	interview.UpdatedAt, _ = parseTime(updatedAt)
	json.Unmarshal([]byte(interviewerIDsJSON), &interview.InterviewerIDs)
	json.Unmarshal([]byte(tagsJSON), &interview.Tags)

	return &interview, nil
}

// ListInterviews retrieves all interviews with optional filtering
func (d *Database) ListInterviews(status string, limit, offset int) ([]*models.Interview, int, error) {
	var interviews []*models.Interview
	var query string
	var args []interface{}

	if status != "" && status != "all" {
		query = `
			SELECT id, candidate_name, candidate_email, position, scheduled_at, duration, 
				status, interviewer_ids, tags, created_at, updated_at
			FROM interviews
			WHERE status = ?
			ORDER BY scheduled_at DESC
			LIMIT ? OFFSET ?
		`
		args = []interface{}{status, limit, offset}
	} else {
		query = `
			SELECT id, candidate_name, candidate_email, position, scheduled_at, duration, 
				status, interviewer_ids, tags, created_at, updated_at
			FROM interviews
			ORDER BY scheduled_at DESC
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
		var interview models.Interview
		var scheduledAt, createdAt, updatedAt string
		var interviewerIDsJSON, tagsJSON string

		err := rows.Scan(
			&interview.ID,
			&interview.CandidateName,
			&interview.CandidateEmail,
			&interview.Position,
			&scheduledAt,
			&interview.Duration,
			&interview.Status,
			&interviewerIDsJSON,
			&tagsJSON,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, 0, err
		}

		interview.ScheduledAt, _ = parseTime(scheduledAt)
		interview.CreatedAt, _ = parseTime(createdAt)
		interview.UpdatedAt, _ = parseTime(updatedAt)
		json.Unmarshal([]byte(interviewerIDsJSON), &interview.InterviewerIDs)
		json.Unmarshal([]byte(tagsJSON), &interview.Tags)

		interviews = append(interviews, &interview)
	}

	// Get total count
	var countQuery string
	var countArgs []interface{}
	if status != "" && status != "all" {
		countQuery = "SELECT COUNT(*) FROM interviews WHERE status = ?"
		countArgs = []interface{}{status}
	} else {
		countQuery = "SELECT COUNT(*) FROM interviews"
	}

	var total int
	err = d.db.QueryRow(countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	return interviews, total, nil
}

// UpdateInterview updates an existing interview
func (d *Database) UpdateInterview(id string, updates map[string]interface{}) error {
	updates["updated_at"] = formatTime(time.Now())

	// Build dynamic update query
	query := "UPDATE interviews SET "
	var args []interface{}
	i := 0

	for key, value := range updates {
		if i > 0 {
			query += ", "
		}
		query += fmt.Sprintf("%s = ?", key)
		args = append(args, value)
		i++
	}

	query += " WHERE id = ?"
	args = append(args, id)

	_, err := d.db.Exec(query, args...)
	return err
}

// UpdateInterviewStatus updates the status of an interview
func (d *Database) UpdateInterviewStatus(id string, status models.InterviewStatus) error {
	query := `UPDATE interviews SET status = ?, updated_at = ? WHERE id = ?`
	_, err := d.db.Exec(query, status, formatTime(time.Now()), id)
	return err
}

// GetInterviewsByStatus counts interviews by status
func (d *Database) GetInterviewsByStatus() (map[string]int, error) {
	query := `SELECT status, COUNT(*) FROM interviews GROUP BY status`

	rows, err := d.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int)
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		result[status] = count
	}

	return result, nil
}
