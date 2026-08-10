package webui

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/adrien19/chronoqueue/pkg/log"
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

func TestUIAuthMiddleware(t *testing.T) {
	config := uiAuthConfig{enabled: true, username: "operator", password: "secret"}
	handlerCalls := 0
	handler := uiAuthMiddleware(config, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		name       string
		path       string
		username   string
		password   string
		wantStatus int
	}{
		{name: "missing credentials", path: "/queues", wantStatus: http.StatusUnauthorized},
		{name: "invalid credentials", path: "/queues", username: "operator", password: "wrong", wantStatus: http.StatusUnauthorized},
		{name: "valid credentials", path: "/queues", username: "operator", password: "secret", wantStatus: http.StatusOK},
		{name: "health remains public", path: "/health", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, tt.path, nil)
			if tt.username != "" || tt.password != "" {
				request.SetBasicAuth(tt.username, tt.password)
			}

			handler.ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusUnauthorized && recorder.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("WWW-Authenticate header is missing")
			}
		})
	}
	if handlerCalls != 2 {
		t.Fatalf("handler calls = %d, want 2", handlerCalls)
	}
}

func TestUIServerAuthenticationGate(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("CHRONOQUEUE_UI_AUTH_ENABLED", "true")
	t.Setenv("CHRONOQUEUE_UI_AUTH_USERNAME", "operator")
	t.Setenv("CHRONOQUEUE_UI_AUTH_PASSWORD", "secret")
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))

	server, err := NewUIServer("localhost:9000", true, logger)
	if err != nil {
		t.Fatalf("create UI server: %v", err)
	}
	defer server.store.CloseAll()
	handler, err := server.httpHandler()
	if err != nil {
		t.Fatalf("create UI handler: %v", err)
	}

	for _, test := range []struct {
		name   string
		method string
		path   string
	}{
		{name: "page", method: http.MethodGet, path: "/"},
		{name: "static asset", method: http.MethodGet, path: "/static/js/app.js"},
		{name: "fragment", method: http.MethodGet, path: "/fragments/dashboard-stats"},
		{name: "mutation", method: http.MethodPost, path: "/api/queues/create"},
	} {
		t.Run("rejects unauthenticated "+test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, nil))

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnauthorized)
			}
			if recorder.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("WWW-Authenticate header is missing")
			}
		})
	}

	t.Run("accepts authenticated static request", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/static/js/app.js", nil)
		request.SetBasicAuth("operator", "secret")
		handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
		}
		if recorder.Body.Len() == 0 {
			t.Fatal("static asset response is empty")
		}
	})

	t.Run("health remains public", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

		if recorder.Code != http.StatusOK || recorder.Body.String() != "OK" {
			t.Fatalf("health response = (%d, %q), want (200, %q)", recorder.Code, recorder.Body.String(), "OK")
		}
	})
}

func TestNewUIServerRejectsIncompleteAuthentication(t *testing.T) {
	t.Setenv("CHRONOQUEUE_UI_AUTH_ENABLED", "true")
	t.Setenv("CHRONOQUEUE_UI_AUTH_USERNAME", "operator")
	t.Setenv("CHRONOQUEUE_UI_AUTH_PASSWORD", "")
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))

	_, err := NewUIServer("localhost:9000", true, logger)
	if err == nil || !strings.Contains(err.Error(), "username or password is missing") {
		t.Fatalf("expected missing credentials error, got %v", err)
	}
}

func TestNewUIServerRejectsIncompleteTLSConfiguration(t *testing.T) {
	t.Setenv("CHRONOQUEUE_UI_TLS_CERT_FILE", "/certs/ui.crt")
	t.Setenv("CHRONOQUEUE_UI_TLS_KEY_FILE", "")
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))

	_, err := NewUIServer("localhost:9000", true, logger)
	if err == nil || !strings.Contains(err.Error(), "certificate and key must be configured together") {
		t.Fatalf("expected incomplete TLS configuration error, got %v", err)
	}
}

func TestUITLSConfigFromEnvironment(t *testing.T) {
	t.Run("loads valid certificate pair", func(t *testing.T) {
		privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatalf("generate private key: %v", err)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			NotBefore:    time.Now().Add(-time.Minute),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			DNSNames:     []string{"localhost"},
		}
		certDER, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
		if err != nil {
			t.Fatalf("create certificate: %v", err)
		}
		certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
		keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
		if err != nil {
			t.Fatalf("marshal private key: %v", err)
		}
		keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
		certFile := filepath.Join(t.TempDir(), "ui.crt")
		keyFile := filepath.Join(filepath.Dir(certFile), "ui.key")
		if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
			t.Fatalf("write certificate: %v", err)
		}
		if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
			t.Fatalf("write private key: %v", err)
		}
		t.Setenv("CHRONOQUEUE_UI_TLS_CERT_FILE", certFile)
		t.Setenv("CHRONOQUEUE_UI_TLS_KEY_FILE", keyFile)

		config, err := uiTLSConfigFromEnvironment()
		if err != nil {
			t.Fatalf("load TLS configuration: %v", err)
		}
		if !config.enabled || len(config.certificate.Certificate) == 0 {
			t.Fatal("expected TLS configuration with a certificate")
		}
		if config.serverConfig().MinVersion != tls.VersionTLS12 {
			t.Fatalf("minimum TLS version = %d, want TLS 1.2", config.serverConfig().MinVersion)
		}
	})

	t.Run("rejects invalid certificate pair", func(t *testing.T) {
		certFile := filepath.Join(t.TempDir(), "ui.crt")
		keyFile := filepath.Join(filepath.Dir(certFile), "ui.key")
		if err := os.WriteFile(certFile, []byte("invalid"), 0o600); err != nil {
			t.Fatalf("write certificate: %v", err)
		}
		if err := os.WriteFile(keyFile, []byte("invalid"), 0o600); err != nil {
			t.Fatalf("write private key: %v", err)
		}
		t.Setenv("CHRONOQUEUE_UI_TLS_CERT_FILE", certFile)
		t.Setenv("CHRONOQUEUE_UI_TLS_KEY_FILE", keyFile)

		_, err := uiTLSConfigFromEnvironment()
		if err == nil || !strings.Contains(err.Error(), "load web UI TLS certificate") {
			t.Fatalf("expected invalid certificate error, got %v", err)
		}
	})
}

func TestMutationRequiresAuthenticationAndValidOrigin(t *testing.T) {
	logger := log.NewLogger(log.WithLevel(logrus.PanicLevel))
	server := &UIServer{
		logger:          logger,
		publicOrigin:    normalizedOrigin{Scheme: "https", Host: "console.example", Port: "443"},
		hasPublicOrigin: true,
	}
	handlerCalls := 0
	mutation := server.protectMutationRoute(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusOK)
	})
	handler := uiAuthMiddleware(uiAuthConfig{enabled: true, username: "operator", password: "secret"}, mutation)

	request := func(authenticated, validOrigin bool) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "https://console.example/api/queues/create", nil)
		if authenticated {
			req.SetBasicAuth("operator", "secret")
		}
		if validOrigin {
			req.Header.Set("Origin", "https://console.example")
		}
		return req
	}

	unauthenticated := httptest.NewRecorder()
	handler.ServeHTTP(unauthenticated, request(false, true))
	if unauthenticated.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want %d", unauthenticated.Code, http.StatusUnauthorized)
	}

	invalidOrigin := httptest.NewRecorder()
	handler.ServeHTTP(invalidOrigin, request(true, false))
	if invalidOrigin.Code != http.StatusForbidden {
		t.Fatalf("invalid-origin status = %d, want %d", invalidOrigin.Code, http.StatusForbidden)
	}

	valid := httptest.NewRecorder()
	handler.ServeHTTP(valid, request(true, true))
	if valid.Code != http.StatusOK {
		t.Fatalf("valid status = %d, want %d", valid.Code, http.StatusOK)
	}
	if handlerCalls != 1 {
		t.Fatalf("handler calls = %d, want 1", handlerCalls)
	}
}

func TestLoopbackListenAddress(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8081", "localhost:8081", "[::1]:8081"} {
		if !isLoopbackListenAddress(addr) {
			t.Fatalf("expected %s to be loopback", addr)
		}
	}
	for _, addr := range []string{":8081", "0.0.0.0:8081", "[::]:8081"} {
		if isLoopbackListenAddress(addr) {
			t.Fatalf("expected %s to be non-loopback", addr)
		}
	}
	if err := validateUIListenAddress("127.0.0.1:8081", false); err != nil {
		t.Fatalf("expected plaintext loopback bind to be accepted, got %v", err)
	}
	if err := validateUIListenAddress("0.0.0.0:8081", true); err != nil {
		t.Fatalf("expected TLS public bind to be accepted, got %v", err)
	}
	if err := validateUIListenAddress("0.0.0.0:8081", false); err == nil || !strings.Contains(err.Error(), "TLS is required") {
		t.Fatalf("expected plaintext public bind rejection, got %v", err)
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
