package helpers

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// FixtureData represents the structure of fixture files
type FixtureData map[string]json.RawMessage

// LoadFixture loads a JSON fixture file and returns the parsed data
func LoadFixture(t *testing.T, filename string) FixtureData {
	t.Helper()

	// Try to find the fixtures directory
	fixturesDir := findFixturesDir(t)
	filePath := filepath.Join(fixturesDir, filename)

	data, err := os.ReadFile(filePath)
	require.NoError(t, err, "Failed to read fixture file: %s", filename)

	var fixtures FixtureData
	err = json.Unmarshal(data, &fixtures)
	require.NoError(t, err, "Failed to parse fixture file: %s", filename)

	return fixtures
}

// LoadFixtureItem loads a specific item from a fixture file
func LoadFixtureItem(t *testing.T, filename, itemName string) json.RawMessage {
	t.Helper()

	fixtures := LoadFixture(t, filename)
	item, exists := fixtures[itemName]
	require.True(t, exists, "Fixture item '%s' not found in %s", itemName, filename)

	return item
}

// UnmarshalFixtureItem loads and unmarshals a specific fixture item into the provided struct
func UnmarshalFixtureItem(t *testing.T, filename, itemName string, v interface{}) {
	t.Helper()

	item := LoadFixtureItem(t, filename, itemName)
	err := json.Unmarshal(item, v)
	require.NoError(t, err, "Failed to unmarshal fixture item '%s' from %s", itemName, filename)
}

// LoadJSONSchema loads a JSON schema file from fixtures/schemas/
func LoadJSONSchema(t *testing.T, schemaFile string) string {
	t.Helper()

	fixturesDir := findFixturesDir(t)
	schemaPath := filepath.Join(fixturesDir, "schemas", schemaFile)

	data, err := os.ReadFile(schemaPath)
	require.NoError(t, err, "Failed to read schema file: %s", schemaFile)

	return string(data)
}

// findFixturesDir tries to locate the fixtures directory relative to the test
func findFixturesDir(t *testing.T) string {
	// Try common paths relative to where tests might run
	possiblePaths := []string{
		"./fixtures",
		"../fixtures",
		"../../fixtures",
		"./tests/fixtures",
		"../tests/fixtures",
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path); err == nil {
			absPath, _ := filepath.Abs(path)
			return absPath
		}
	}

	t.Fatalf("Could not find fixtures directory")
	return ""
}

// MessageFixture represents a message from fixtures/messages.json
type MessageFixture struct {
	Content     interface{} `json:"content"`
	ContentType string      `json:"content_type"`
	Priority    int32       `json:"priority"`
	Note        string      `json:"note,omitempty"`
}

// LoadMessageFixture loads a specific message fixture
func LoadMessageFixture(t *testing.T, messageName string) *MessageFixture {
	t.Helper()

	var msg MessageFixture
	UnmarshalFixtureItem(t, "messages.json", messageName, &msg)
	return &msg
}

// GetMessageContent converts the message content to JSON string
func (m *MessageFixture) GetContentAsJSON(t *testing.T) string {
	t.Helper()

	if str, ok := m.Content.(string); ok {
		return str
	}

	data, err := json.Marshal(m.Content)
	require.NoError(t, err, "Failed to marshal message content")
	return string(data)
}

// GetContentAsBytes converts the message content to bytes
func (m *MessageFixture) GetContentAsBytes(t *testing.T) []byte {
	t.Helper()

	if str, ok := m.Content.(string); ok {
		return []byte(str)
	}

	data, err := json.Marshal(m.Content)
	require.NoError(t, err, "Failed to marshal message content")
	return data
}

// QueueFixture represents a queue configuration from fixtures/queues.json
type QueueFixture struct {
	Name     string                 `json:"name"`
	Metadata map[string]interface{} `json:"metadata"`
}

// LoadQueueFixture loads a specific queue fixture
func LoadQueueFixture(t *testing.T, queueName string) *QueueFixture {
	t.Helper()

	var queue QueueFixture
	UnmarshalFixtureItem(t, "queues.json", queueName, &queue)
	return &queue
}

// ScheduleFixture represents a schedule from fixtures/schedules.json
type ScheduleFixture struct {
	Type           string                 `json:"type"`
	CronExpression string                 `json:"cron_expression,omitempty"`
	QueueName      string                 `json:"queue_name"`
	Message        MessageFixture         `json:"message"`
	CalendarRules  map[string]interface{} `json:"calendar_rules,omitempty"`
	Description    string                 `json:"description"`
}

// LoadScheduleFixture loads a specific schedule fixture
func LoadScheduleFixture(t *testing.T, scheduleName string) *ScheduleFixture {
	t.Helper()

	var schedule ScheduleFixture
	UnmarshalFixtureItem(t, "schedules.json", scheduleName, &schedule)
	return &schedule
}

// GenerateUniqueQueueName generates a unique queue name for testing
func GenerateUniqueQueueName(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s-%s", prefix, GenerateRandomID(8))
}

// GenerateUniqueMessageID generates a unique message ID for testing
func GenerateUniqueMessageID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("msg-%s", GenerateRandomID(16))
}

// GenerateRandomID generates a random alphanumeric ID of specified length
func GenerateRandomID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		// Generate a truly random index
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}
