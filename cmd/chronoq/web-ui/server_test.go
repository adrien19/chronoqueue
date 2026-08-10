package webui

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestUISecurityHeadersAndLocalAssets(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	uiSecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(recorder, request)

	if recorder.Header().Get("Content-Security-Policy") == "" {
		t.Fatal("Content-Security-Policy header is missing")
	}

	baseTemplate, err := content.ReadFile("templates/layouts/base.gohtml")
	if err != nil {
		t.Fatalf("read base template: %v", err)
	}
	if strings.Contains(string(baseTemplate), "https://unpkg.com") {
		t.Fatal("base template still references unpkg.com")
	}
	if _, err := content.ReadFile("static/third-party/htmx-1.9.12.min.js"); err != nil {
		t.Fatalf("read embedded HTMX asset: %v", err)
	}
	if _, err := content.ReadFile("static/third-party/htmx-sse-2.2.1.js"); err != nil {
		t.Fatalf("read embedded HTMX SSE asset: %v", err)
	}
}

func TestUIInlineScriptAllowedByHash(t *testing.T) {
	page, err := content.ReadFile("templates/pages/queue_message_new.gohtml")
	if err != nil {
		t.Fatalf("read message template: %v", err)
	}
	_, scriptAndRemainder, found := strings.Cut(string(page), "<script>")
	if !found {
		t.Fatal("inline script not found")
	}
	script, _, found := strings.Cut(scriptAndRemainder, "</script>")
	if !found {
		t.Fatal("inline script closing tag not found")
	}
	digest := sha256.Sum256([]byte(script))
	directive := "'sha256-" + base64.StdEncoding.EncodeToString(digest[:]) + "'"
	if !strings.Contains(uiContentSecurityPolicy, directive) {
		t.Fatalf("CSP does not allow the inline script hash %s", directive)
	}
}

func TestTemplateFuncs(t *testing.T) {
	fns := templateFuncs()

	t.Run("formatTime", func(t *testing.T) {
		fn, ok := fns["formatTime"].(func(time.Time) string)
		if !ok {
			t.Fatal("formatTime not registered")
		}
		ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
		got := fn(ts)
		if got != "2024-01-15 10:30:00" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("formatDuration", func(t *testing.T) {
		fn, ok := fns["formatDuration"].(func(time.Duration) string)
		if !ok {
			t.Fatal("formatDuration not registered")
		}
		cases := []struct {
			d    time.Duration
			want string
		}{
			{30 * time.Second, "30s"},
			{90 * time.Second, "1m 30s"},
			{2*time.Hour + 15*time.Minute, "2h 15m"},
		}
		for _, c := range cases {
			if got := fn(c.d); got != c.want {
				t.Errorf("formatDuration(%v) = %q, want %q", c.d, got, c.want)
			}
		}
	})

	t.Run("add", func(t *testing.T) {
		fn, ok := fns["add"].(func(int, int) int)
		if !ok {
			t.Fatal("add not registered")
		}
		if fn(2, 3) != 5 {
			t.Error("expected 5")
		}
	})

	t.Run("sub", func(t *testing.T) {
		fn, ok := fns["sub"].(func(int, int) int)
		if !ok {
			t.Fatal("sub not registered")
		}
		if fn(5, 2) != 3 {
			t.Error("expected 3")
		}
	})
}

func TestValidateMutationOrigin(t *testing.T) {
	configured := normalizedOrigin{Scheme: "https", Host: "console.example", Port: "8443"}

	t.Run("rejects missing origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "https://console.example:8443/api/queues/create", nil)
		req.Host = "console.example:8443"

		err := validateMutationOrigin(req, configured, true, false)
		if err == nil {
			t.Fatal("expected error for missing origin")
		}
	})

	t.Run("accepts exact origin", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "https://console.example:8443/api/queues/create", nil)
		req.Host = "console.example:8443"
		req.Header.Set("Origin", "https://console.example:8443")

		err := validateMutationOrigin(req, configured, true, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("rejects mismatched port", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "https://console.example/api/queues/create", nil)
		req.Host = "console.example"
		req.Header.Set("Origin", "https://console.example")

		err := validateMutationOrigin(req, configured, true, false)
		if err == nil {
			t.Fatal("expected mismatch error")
		}
	})

	t.Run("rejects when configured origin is missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "https://console.example:8443/api/queues/create", nil)
		req.Host = "console.example:8443"
		req.Header.Set("Origin", "https://console.example:8443")

		err := validateMutationOrigin(req, normalizedOrigin{}, false, false)
		if err == nil {
			t.Fatal("expected error when configured origin is missing")
		}
	})

	t.Run("honors proxy headers only when explicitly trusted", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "http://internal:8080/api/queues/create", nil)
		req.Host = "internal:8080"
		req.Header.Set("Origin", "https://console.example:8443")
		req.Header.Set("X-Forwarded-Proto", "https")
		req.Header.Set("X-Forwarded-Host", "console.example:8443")

		err := validateMutationOrigin(req, configured, true, false)
		if err == nil {
			t.Fatal("expected mismatch when proxy headers are not trusted")
		}

		err = validateMutationOrigin(req, configured, true, true)
		if err != nil {
			t.Fatalf("expected success when proxy headers are trusted: %v", err)
		}
	})
}
