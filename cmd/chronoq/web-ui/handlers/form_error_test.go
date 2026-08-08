package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

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

	t.Run("non-htmx request returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/queues/create", nil)
		rr := httptest.NewRecorder()

		h.writeInlineFormError(rr, req, "validation failed")

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
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

	t.Run("non-htmx request returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/api/schemas/register", nil)
		rr := httptest.NewRecorder()

		h.writeInlineFormError(rr, req, "validation failed")

		if rr.Code != http.StatusBadRequest {
			t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rr.Code)
		}
	})
}
