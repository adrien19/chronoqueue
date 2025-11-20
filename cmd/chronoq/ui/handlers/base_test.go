package handlers

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/adrien19/chronoqueue/pkg/log"
)

func TestBaseHandler_renderError(t *testing.T) {
	logger := log.NewLogger()
	// Create template with error template defined
	tmpl := template.Must(template.New("error").Parse(`
		{{define "error"}}
		Error {{.Code}}: {{.Error}}
		{{end}}
	`))

	handler := &BaseHandler{
		templates: tmpl,
		logger:    logger,
	}

	tests := []struct {
		name           string
		statusCode     int
		message        string
		expectedStatus int
	}{
		{
			name:           "render 404 error",
			statusCode:     http.StatusNotFound,
			message:        "Queue not found",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "render 500 error",
			statusCode:     http.StatusInternalServerError,
			message:        "Internal server error",
			expectedStatus: http.StatusInternalServerError,
		},
		{
			name:           "render 400 error",
			statusCode:     http.StatusBadRequest,
			message:        "Invalid request",
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.renderError(w, tt.statusCode, tt.message)

			assert.Equal(t, tt.expectedStatus, w.Code)
			assert.Contains(t, w.Body.String(), tt.message)
		})
	}
}

func TestBaseHandler_renderTemplate_UnknownTemplate(t *testing.T) {
	logger := log.NewLogger()
	tmpl := template.New("test")

	handler := &BaseHandler{
		templates: tmpl,
		logger:    logger,
	}

	w := httptest.NewRecorder()
	handler.renderTemplate(w, "unknown.html", nil)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.Contains(t, w.Body.String(), "Internal Server Error")
}

func TestBaseHandler_renderTemplate_WithVersionInfo(t *testing.T) {
	// Create a simple template
	tmpl := template.Must(template.New("layout.html").Parse(`
		{{define "layout.html"}}
		Version: {{.VersionInfo}}
		{{template "page-content" .}}
		{{end}}
	`))

	template.Must(tmpl.New("dashboard-page-content").Parse(`
		{{define "dashboard-page-content"}}
		Dashboard Content
		{{end}}
	`))

	logger := log.NewLogger()
	handler := &BaseHandler{
		templates: tmpl,
		logger:    logger,
	}

	w := httptest.NewRecorder()
	data := map[string]interface{}{
		"Title": "Test Dashboard",
	}

	handler.renderTemplate(w, "dashboard.html", data)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Version:")
	assert.Contains(t, w.Body.String(), "Dashboard Content")

	// Verify VersionInfo was added to data
	require.Contains(t, data, "VersionInfo")
	assert.NotEmpty(t, data["VersionInfo"])
}

func TestBaseHandler_renderComponent(t *testing.T) {
	// Create a simple component template
	tmpl := template.Must(template.New("test-component").Parse(`
		{{define "test-component"}}
		<div>{{.Message}}</div>
		{{end}}
	`))

	logger := log.NewLogger()
	handler := &BaseHandler{
		templates: tmpl,
		logger:    logger,
	}

	w := httptest.NewRecorder()
	data := map[string]interface{}{
		"Message": "Test Component",
	}

	handler.renderComponent(w, "test-component", data)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "Test Component")
	assert.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
}

func TestBaseHandler_renderComponent_NonExistentTemplate(t *testing.T) {
	tmpl := template.New("test")
	logger := log.NewLogger()

	handler := &BaseHandler{
		templates: tmpl,
		logger:    logger,
	}

	w := httptest.NewRecorder()
	handler.renderComponent(w, "non-existent", nil)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}
