package handlers

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

func testFormErrorBaseHandler(t *testing.T) BaseHandler {
	t.Helper()

	tmpl := template.Must(template.New("").Parse(`
{{ define "base" }}<!doctype html><html><body><main>{{ if eq .ContentTemplate "error_content" }}{{ template "error_content" . }}{{ end }}</main></body></html>{{ end }}
{{ define "error_content" }}<h1>{{ .ErrorTitle }}</h1><div>{{ .ErrorMessage }}</div><span>{{ .EnvName }}</span>{{ end }}
`))
	store := clusterstore.NewStore("")
	store.Seed("Local", "localhost:9000", true)

	return BaseHandler{
		templates: tmpl,
		store:     store,
		logger:    log.NewLogger(),
	}
}

func TestWriteFormError_StatusCode(t *testing.T) {
	h := &QueuesHandler{}

	t.Run("htmx request returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/queues/create", nil)
		req.Header.Set("HX-Request", "true")
		rr := httptest.NewRecorder()

		h.writeInlineFormError(rr, req, "validation failed")

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "validation failed") {
			t.Fatalf("expected error body to contain message, got %q", rr.Body.String())
		}
	})

	t.Run("non-htmx request returns full error page with 400", func(t *testing.T) {
		h := &QueuesHandler{BaseHandler: testFormErrorBaseHandler(t)}
		req := httptest.NewRequest(http.MethodPost, "/api/queues/create", nil)
		rr := httptest.NewRecorder()

		h.writeInlineFormError(rr, req, "validation failed")

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "<!doctype html>") || !strings.Contains(body, "validation failed") {
			t.Fatalf("expected full error page to contain message, got %q", body)
		}
	})
}

func TestWriteSchemaFormError_StatusCode(t *testing.T) {
	h := &SchemasHandler{}

	t.Run("htmx request returns 200", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/schemas/register", nil)
		req.Header.Set("HX-Request", "true")
		rr := httptest.NewRecorder()

		h.writeInlineFormError(rr, req, "validation failed")

		if rr.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d", http.StatusOK, rr.Code)
		}
		if !strings.Contains(rr.Body.String(), "validation failed") {
			t.Fatalf("expected error body to contain message, got %q", rr.Body.String())
		}
	})

	t.Run("non-htmx request returns full error page with 400", func(t *testing.T) {
		h := &SchemasHandler{BaseHandler: testFormErrorBaseHandler(t)}
		req := httptest.NewRequest(http.MethodPost, "/api/schemas/register", nil)
		rr := httptest.NewRecorder()

		h.writeInlineFormError(rr, req, "validation failed")

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "<!doctype html>") || !strings.Contains(body, "validation failed") {
			t.Fatalf("expected full error page to contain message, got %q", body)
		}
	})
}

func TestQueueMessageNewSchemaDefaultLookupState(t *testing.T) {
	tmpl := template.Must(template.ParseFiles("../templates/pages/queue_message_new.gohtml"))

	t.Run("shows queue default when lookup succeeds", func(t *testing.T) {
		var body strings.Builder
		err := tmpl.ExecuteTemplate(&body, "queue_message_new_content", map[string]any{
			"QueueName":           "orders",
			"QueueSchemaID":       "order-schema",
			"QueueSchemaRequired": true,
		})
		if err != nil {
			t.Fatalf("execute template: %v", err)
		}

		rendered := body.String()
		if !strings.Contains(rendered, "Queue default: order-schema") {
			t.Fatalf("expected queue default badge, got %q", rendered)
		}
		if !strings.Contains(rendered, "This queue requires schema validation") {
			t.Fatalf("expected required schema notice, got %q", rendered)
		}
	})

	t.Run("shows neutral state when lookup fails", func(t *testing.T) {
		var body strings.Builder
		err := tmpl.ExecuteTemplate(&body, "queue_message_new_content", map[string]any{
			"QueueName":               "orders",
			"QueueSchemaLookupFailed": true,
		})
		if err != nil {
			t.Fatalf("execute template: %v", err)
		}

		rendered := body.String()
		if !strings.Contains(rendered, "Queue default unavailable") {
			t.Fatalf("expected lookup failure badge, got %q", rendered)
		}
		if !strings.Contains(rendered, "Queue schema defaults could not be loaded") {
			t.Fatalf("expected lookup failure notice, got %q", rendered)
		}
		if strings.Contains(rendered, "No queue default schema") {
			t.Fatalf("did not expect missing-default badge on lookup failure, got %q", rendered)
		}
	})
}
