package db

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/adrien19/chronoqueue/examples/interview-platform/backend/internal/models"
)

// CreateEvaluation creates a new evaluation
func (d *Database) CreateEvaluation(eval *models.Evaluation) error {
	eval.ID = uuid.New().String()
	eval.CreatedAt = time.Now()
	eval.UpdatedAt = time.Now()
	eval.Status = models.EvalStatusPending

	// Calculate overall score as average of all scores
	eval.OverallScore = float64(eval.TechnicalScore+eval.CommunicationScore+eval.ProblemSolvingScore+eval.CultureFitScore) / 4.0

	strengthsJSON, _ := json.Marshal(eval.Strengths)
	weaknessesJSON, _ := json.Marshal(eval.Weaknesses)

	query := `
		INSERT INTO evaluations (id, interview_id, evaluator_id, evaluator_name, evaluator_email,
			technical_score, communication_score, problem_solving_score, cultural_fit_score, overall_score,
			strengths, weaknesses, recommendation, comments, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err := d.db.Exec(
		query,
		eval.ID,
		eval.InterviewID,
		eval.EvaluatorID,
		eval.EvaluatorName,
		eval.EvaluatorEmail,
		eval.TechnicalScore,
		eval.CommunicationScore,
		eval.ProblemSolvingScore,
		eval.CultureFitScore,
		eval.OverallScore,
		string(strengthsJSON),
		string(weaknessesJSON),
		eval.Recommendation,
		eval.Comments,
		eval.Status,
		formatTime(eval.CreatedAt),
		formatTime(eval.UpdatedAt),
	)

	return err
}

// GetEvaluation retrieves an evaluation by ID
func (d *Database) GetEvaluation(id string) (*models.Evaluation, error) {
	query := `
		SELECT id, interview_id, evaluator_id, evaluator_name, evaluator_email,
			technical_score, communication_score, problem_solving_score, cultural_fit_score, overall_score,
			strengths, weaknesses, recommendation, comments, status, created_at, updated_at
		FROM evaluations
		WHERE id = ?
	`

	var eval models.Evaluation
	var createdAt, updatedAt string
	var strengthsJSON, weaknessesJSON string

	err := d.db.QueryRow(query, id).Scan(
		&eval.ID,
		&eval.InterviewID,
		&eval.EvaluatorID,
		&eval.EvaluatorName,
		&eval.EvaluatorEmail,
		&eval.TechnicalScore,
		&eval.CommunicationScore,
		&eval.ProblemSolvingScore,
		&eval.CultureFitScore,
		&eval.OverallScore,
		&strengthsJSON,
		&weaknessesJSON,
		&eval.Recommendation,
		&eval.Comments,
		&eval.Status,
		&createdAt,
		&updatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("evaluation not found")
	}
	if err != nil {
		return nil, err
	}

	eval.CreatedAt, _ = parseTime(createdAt)
	eval.UpdatedAt, _ = parseTime(updatedAt)
	json.Unmarshal([]byte(strengthsJSON), &eval.Strengths)
	json.Unmarshal([]byte(weaknessesJSON), &eval.Weaknesses)

	return &eval, nil
}

// GetEvaluationsByInterview retrieves all evaluations for an interview
func (d *Database) GetEvaluationsByInterview(interviewID string) ([]*models.Evaluation, error) {
	query := `
		SELECT id, interview_id, evaluator_id, evaluator_name, evaluator_email,
			technical_score, communication_score, problem_solving_score, cultural_fit_score, overall_score,
			strengths, weaknesses, recommendation, comments, status, created_at, updated_at
		FROM evaluations
		WHERE interview_id = ?
		ORDER BY created_at DESC
	`

	rows, err := d.db.Query(query, interviewID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var evaluations []*models.Evaluation
	for rows.Next() {
		var eval models.Evaluation
		var createdAt, updatedAt string
		var strengthsJSON, weaknessesJSON string

		err := rows.Scan(
			&eval.ID,
			&eval.InterviewID,
			&eval.EvaluatorID,
			&eval.EvaluatorName,
			&eval.EvaluatorEmail,
			&eval.TechnicalScore,
			&eval.CommunicationScore,
			&eval.ProblemSolvingScore,
			&eval.CultureFitScore,
			&eval.OverallScore,
			&strengthsJSON,
			&weaknessesJSON,
			&eval.Recommendation,
			&eval.Comments,
			&eval.Status,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, err
		}

		eval.CreatedAt, _ = parseTime(createdAt)
		eval.UpdatedAt, _ = parseTime(updatedAt)
		json.Unmarshal([]byte(strengthsJSON), &eval.Strengths)
		json.Unmarshal([]byte(weaknessesJSON), &eval.Weaknesses)

		evaluations = append(evaluations, &eval)
	}

	return evaluations, nil
}

// ListEvaluations retrieves all evaluations with optional filtering
func (d *Database) ListEvaluations(status string, limit, offset int) ([]*models.Evaluation, int, error) {
	var evaluations []*models.Evaluation
	var query string
	var args []interface{}

	if status != "" && status != "all" {
		query = `
			SELECT id, interview_id, evaluator_id, evaluator_name, evaluator_email,
				technical_score, communication_score, problem_solving_score, cultural_fit_score, overall_score,
				strengths, weaknesses, recommendation, comments, status, created_at, updated_at
			FROM evaluations
			WHERE status = ?
			ORDER BY created_at DESC
			LIMIT ? OFFSET ?
		`
		args = []interface{}{status, limit, offset}
	} else {
		query = `
			SELECT id, interview_id, evaluator_id, evaluator_name, evaluator_email,
				technical_score, communication_score, problem_solving_score, cultural_fit_score, overall_score,
				strengths, weaknesses, recommendation, comments, status, created_at, updated_at
			FROM evaluations
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
		var eval models.Evaluation
		var createdAt, updatedAt string
		var strengthsJSON, weaknessesJSON string

		err := rows.Scan(
			&eval.ID,
			&eval.InterviewID,
			&eval.EvaluatorID,
			&eval.EvaluatorName,
			&eval.EvaluatorEmail,
			&eval.TechnicalScore,
			&eval.CommunicationScore,
			&eval.ProblemSolvingScore,
			&eval.CultureFitScore,
			&eval.OverallScore,
			&strengthsJSON,
			&weaknessesJSON,
			&eval.Recommendation,
			&eval.Comments,
			&eval.Status,
			&createdAt,
			&updatedAt,
		)
		if err != nil {
			return nil, 0, err
		}

		eval.CreatedAt, _ = parseTime(createdAt)
		eval.UpdatedAt, _ = parseTime(updatedAt)
		json.Unmarshal([]byte(strengthsJSON), &eval.Strengths)
		json.Unmarshal([]byte(weaknessesJSON), &eval.Weaknesses)

		evaluations = append(evaluations, &eval)
	}

	// Get total count
	var countQuery string
	var countArgs []interface{}
	if status != "" && status != "all" {
		countQuery = "SELECT COUNT(*) FROM evaluations WHERE status = ?"
		countArgs = []interface{}{status}
	} else {
		countQuery = "SELECT COUNT(*) FROM evaluations"
	}

	var total int
	err = d.db.QueryRow(countQuery, countArgs...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	return evaluations, total, nil
}

// UpdateEvaluationStatus updates the status of an evaluation
func (d *Database) UpdateEvaluationStatus(id string, status models.EvaluationStatus) error {
	query := `UPDATE evaluations SET status = ?, updated_at = ? WHERE id = ?`
	_, err := d.db.Exec(query, status, formatTime(time.Now()), id)
	return err
}

// GetEvaluationsByStatus counts evaluations by status
func (d *Database) GetEvaluationsByStatus() (map[string]int, error) {
	query := `SELECT status, COUNT(*) FROM evaluations GROUP BY status`

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
