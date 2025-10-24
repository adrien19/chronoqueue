package db

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	db *sql.DB
}

// NewDatabase initializes the SQLite database
func NewDatabase(path string) (*Database, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Enable foreign keys
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		return nil, fmt.Errorf("failed to enable foreign keys: %w", err)
	}

	database := &Database{db: db}

	// Initialize schema
	if err := database.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return database, nil
}

// Close closes the database connection
func (d *Database) Close() error {
	return d.db.Close()
}

// initSchema creates the database tables
func (d *Database) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS interviews (
		id TEXT PRIMARY KEY,
		candidate_name TEXT NOT NULL,
		candidate_email TEXT NOT NULL,
		position TEXT NOT NULL,
		scheduled_at DATETIME NOT NULL,
		duration INTEGER NOT NULL,
		status TEXT NOT NULL,
		interviewer_ids TEXT NOT NULL,
		tags TEXT,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS evaluations (
		id TEXT PRIMARY KEY,
		interview_id TEXT NOT NULL,
		evaluator_id TEXT NOT NULL,
		evaluator_name TEXT NOT NULL,
		evaluator_email TEXT NOT NULL,
		technical_score INTEGER NOT NULL,
		communication_score INTEGER NOT NULL,
		problem_solving_score INTEGER NOT NULL,
		cultural_fit_score INTEGER NOT NULL,
		overall_score REAL NOT NULL,
		strengths TEXT NOT NULL,
		weaknesses TEXT NOT NULL,
		recommendation TEXT NOT NULL,
		comments TEXT NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		FOREIGN KEY (interview_id) REFERENCES interviews(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS reports (
		id TEXT PRIMARY KEY,
		interview_id TEXT NOT NULL,
		candidate_name TEXT NOT NULL,
		position TEXT NOT NULL,
		interview_date DATETIME NOT NULL,
		evaluation_count INTEGER NOT NULL,
		average_technical REAL NOT NULL,
		average_communication REAL NOT NULL,
		average_culture_fit REAL NOT NULL,
		average_overall REAL NOT NULL,
		final_recommendation TEXT NOT NULL,
		status TEXT NOT NULL,
		generated_at DATETIME,
		sent_at DATETIME,
		created_at DATETIME NOT NULL,
		updated_at DATETIME NOT NULL,
		FOREIGN KEY (interview_id) REFERENCES interviews(id) ON DELETE CASCADE
	);

	CREATE TABLE IF NOT EXISTS queue_messages (
		id TEXT PRIMARY KEY,
		queue_name TEXT NOT NULL,
		message_type TEXT NOT NULL,
		payload TEXT NOT NULL,
		priority INTEGER NOT NULL,
		status TEXT NOT NULL,
		created_at DATETIME NOT NULL,
		processed_at DATETIME
	);

	CREATE INDEX IF NOT EXISTS idx_interviews_status ON interviews(status);
	CREATE INDEX IF NOT EXISTS idx_interviews_scheduled_at ON interviews(scheduled_at);
	CREATE INDEX IF NOT EXISTS idx_evaluations_interview_id ON evaluations(interview_id);
	CREATE INDEX IF NOT EXISTS idx_evaluations_status ON evaluations(status);
	CREATE INDEX IF NOT EXISTS idx_reports_interview_id ON reports(interview_id);
	CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);
	CREATE INDEX IF NOT EXISTS idx_queue_messages_queue_name ON queue_messages(queue_name);
	CREATE INDEX IF NOT EXISTS idx_queue_messages_status ON queue_messages(status);
	`

	_, err := d.db.Exec(schema)
	return err
}

// Helper function to format time for SQLite
func formatTime(t time.Time) string {
	return t.Format(time.RFC3339)
}

// Helper function to parse time from SQLite
func parseTime(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}
