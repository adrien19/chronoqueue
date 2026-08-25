package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/adrien19/chronoqueue/client"
	clusterstore "github.com/adrien19/chronoqueue/cmd/chronoq/web-ui/cluster"
	"github.com/adrien19/chronoqueue/pkg/log"
)

// SchemaListItem is the view model for a schema in list pages.
type SchemaListItem struct {
	SchemaID      string
	Name          string
	Description   string
	LatestVersion int32
	VersionCount  int32
	CreatedAt     string
	IsActive      bool
}

// SchemaDetailView is the view model for schema detail pages.
type SchemaDetailView struct {
	SchemaID    string
	Version     int32
	Name        string
	Description string
	ContentType string
	Content     string
	CreatedAt   string
	UpdatedAt   string
	IsActive    bool
	Metadata    map[string]string
}

// SchemasHandler handles schema registry pages and HTMX actions.
type SchemasHandler struct {
	BaseHandler
}

// schemaIDPattern validates schema IDs: letters, digits, dot, hyphen and underscore, starting with a letter or digit.
var schemaIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// NewSchemasHandler creates a SchemasHandler.
func NewSchemasHandler(
	templates *template.Template,
	store *clusterstore.Store,
	logger *log.Logger,
) *SchemasHandler {
	return &SchemasHandler{
		BaseHandler: BaseHandler{
			templates: templates,
			store:     store,
			logger:    logger,
		},
	}
}

// List renders the schema registry listing page.
func (h *SchemasHandler) List(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	activeOnly := true
	if values, ok := r.URL.Query()["active_only"]; ok && len(values) > 0 {
		raw := strings.TrimSpace(values[len(values)-1])
		activeOnly = raw == "true" || raw == "1" || strings.EqualFold(raw, "on")
	}

	schemas, err := activeClient.ListSchemas(ctx, prefix, 200, activeOnly)
	if err != nil {
		h.writeRPCError(w, r, "list schemas", err)
		return
	}

	rows := make([]SchemaListItem, 0, len(schemas))
	for _, item := range schemas {
		rows = append(rows, SchemaListItem{
			SchemaID:      mapString(item, "schema_id"),
			Name:          mapString(item, "name"),
			Description:   mapString(item, "description"),
			LatestVersion: mapInt32(item, "version"),
			VersionCount:  mapInt32(item, "versions"),
			CreatedAt:     formatMillis(mapInt64(item, "created_at")),
			IsActive:      mapBool(item, "is_active"),
		})
	}

	data := map[string]any{
		"PageTitle":   "Schemas",
		"Active":      "schemas",
		"Prefix":      prefix,
		"ActiveOnly":  activeOnly,
		"SchemaItems": rows,
	}
	h.render(w, "schemas_content", data)
}

// New renders the schema registration form.
func (h *SchemasHandler) New(w http.ResponseWriter, r *http.Request) {
	h.render(w, "schema_new_content", map[string]any{
		"PageTitle": "Register Schema",
		"Active":    "schemas",
	})
}

// Create handles schema registration (HTMX POST).
func (h *SchemasHandler) Create(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	if err := r.ParseForm(); err != nil {
		h.logger.ErrorWithFields("Failed to parse schema form", "error", err)
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}

	schemaID := strings.TrimSpace(r.FormValue("schema_id"))
	if schemaID == "" {
		h.writeInlineFormError(w, r, "Schema ID is required")
		return
	}
	if !schemaIDPattern.MatchString(schemaID) {
		h.writeInlineFormError(w, r, "Schema ID may only contain letters, digits, dots, hyphens, and underscores, and must start with a letter or digit")
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	description := strings.TrimSpace(r.FormValue("description"))
	contentType := strings.TrimSpace(r.FormValue("content_type"))
	content := strings.TrimSpace(r.FormValue("content"))
	metadataText := strings.TrimSpace(r.FormValue("metadata"))

	if content == "" {
		h.writeInlineFormError(w, r, "Schema content is required")
		return
	}
	if contentType == "" {
		contentType = "json-schema"
	}

	var schemaJSON map[string]any
	if err := json.Unmarshal([]byte(content), &schemaJSON); err != nil {
		h.writeInlineFormError(w, r, fmt.Sprintf("Invalid schema JSON: %v", err))
		return
	}

	metadata, err := parseSchemaMetadata(metadataText)
	if err != nil {
		h.writeInlineFormError(w, r, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	err = activeClient.RegisterSchema(ctx, schemaID, client.SchemaOptions{
		Name:        name,
		Description: description,
		Content:     content,
		ContentType: contentType,
		Metadata:    metadata,
	})
	if err != nil {
		h.writeRPCError(w, r, "register schema", err)
		return
	}

	redirectTarget := "/schemas/" + url.PathEscape(schemaID)
	if isHTMXRequest(r) {
		w.Header().Set("HX-Redirect", redirectTarget)
		w.WriteHeader(http.StatusCreated)
		if _, writeErr := w.Write([]byte("Schema registered")); writeErr != nil {
			h.logger.ErrorWithFields("Failed to write schema create response", "error", writeErr)
		}
		return
	}

	http.Redirect(w, r, redirectTarget, http.StatusSeeOther)
}

// Detail renders a schema detail page for a schema id and optional version.
func (h *SchemasHandler) Detail(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	schemaID := strings.TrimSpace(r.PathValue("schemaId"))
	if schemaID == "" {
		h.renderError(w, http.StatusBadRequest, "Schema ID required")
		return
	}

	version := int32(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("version")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || parsed < 0 {
			h.renderError(w, http.StatusBadRequest, "Version must be a non-negative integer")
			return
		}
		version = int32(parsed)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	schemaMap, err := activeClient.GetSchema(ctx, schemaID, version)
	if err != nil {
		h.writeRPCError(w, r, "load schema", err)
		return
	}

	view := SchemaDetailView{
		SchemaID:    mapString(schemaMap, "schema_id"),
		Version:     mapInt32(schemaMap, "version"),
		Name:        mapString(schemaMap, "name"),
		Description: mapString(schemaMap, "description"),
		ContentType: mapString(schemaMap, "content_type"),
		Content:     mapString(schemaMap, "content"),
		CreatedAt:   formatMillis(mapInt64(schemaMap, "created_at")),
		UpdatedAt:   formatMillis(mapInt64(schemaMap, "updated_at")),
		IsActive:    mapBool(schemaMap, "is_active"),
		Metadata:    mapStringMap(schemaMap, "metadata"),
	}

	data := map[string]any{
		"PageTitle": "Schema: " + schemaID,
		"Active":    "schemas",
		"Schema":    view,
	}
	h.render(w, "schema_detail_content", data)
}

// Validate validates a payload against a schema (HTMX POST).
func (h *SchemasHandler) Validate(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	schemaID := strings.TrimSpace(r.PathValue("schemaId"))
	if schemaID == "" {
		h.writeInlineFormError(w, r, "Schema ID required")
		return
	}

	if err := r.ParseForm(); err != nil {
		h.writeInlineFormError(w, r, "Invalid form data")
		return
	}

	payload := strings.TrimSpace(r.FormValue("payload"))
	if payload == "" {
		h.writeInlineFormError(w, r, "Payload is required")
		return
	}

	var payloadJSON any
	if err := json.Unmarshal([]byte(payload), &payloadJSON); err != nil {
		h.writeInlineFormError(w, r, fmt.Sprintf("Invalid payload JSON: %v", err))
		return
	}

	version := int32(0)
	if raw := strings.TrimSpace(r.FormValue("version")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || parsed < 0 {
			h.writeInlineFormError(w, r, "Version must be a non-negative integer")
			return
		}
		version = int32(parsed)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	err := activeClient.ValidatePayload(ctx, schemaID, version, payload)
	if err != nil {
		h.writeRPCError(w, r, "validate schema payload", err)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if _, writeErr := w.Write([]byte(`<div class="rounded-lg border border-emerald-500/30 bg-emerald-500/10 px-4 py-3 text-sm text-emerald-300">Payload is valid for the selected schema.</div>`)); writeErr != nil {
		h.logger.ErrorWithFields("Failed to write schema validate response", "error", writeErr)
	}
}

// Delete removes a positive schema version and redirects.
func (h *SchemasHandler) Delete(w http.ResponseWriter, r *http.Request) {
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	schemaID := strings.TrimSpace(r.PathValue("schemaId"))
	if schemaID == "" {
		h.renderError(w, http.StatusBadRequest, "Schema ID required")
		return
	}

	versionRaw := strings.TrimSpace(r.PathValue("version"))
	versionVal, err := strconv.ParseInt(versionRaw, 10, 32)
	if err != nil || versionVal < 1 {
		h.renderError(w, http.StatusBadRequest, "Version must be a positive integer")
		return
	}
	version := int32(versionVal)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if err := activeClient.DeleteSchema(ctx, schemaID, version); err != nil {
		h.writeRPCError(w, r, "delete schema", err)
		return
	}

	http.Redirect(w, r, "/schemas/"+url.PathEscape(schemaID), http.StatusSeeOther)
}

// DeactivateAll deactivates every version of a schema through the version-zero server contract.
func (h *SchemasHandler) DeactivateAll(w http.ResponseWriter, r *http.Request) {
	schemaID := strings.TrimSpace(r.PathValue("schemaId"))
	if schemaID == "" {
		h.renderError(w, http.StatusBadRequest, "Schema ID required")
		return
	}
	activeClient, ok := h.requireActiveClient(w)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := activeClient.DeleteSchema(ctx, schemaID, 0); err != nil {
		h.writeRPCError(w, r, "deactivate all schema versions", err)
		return
	}
	http.Redirect(w, r, "/schemas/"+url.PathEscape(schemaID), http.StatusSeeOther)
}

func parseSchemaMetadata(raw string) (map[string]string, error) {
	metadata := make(map[string]string)
	if raw == "" {
		return metadata, nil
	}

	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		parts := strings.SplitN(trimmed, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid metadata line %q (expected key=value)", trimmed)
		}
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			return nil, fmt.Errorf("metadata key cannot be empty")
		}
		metadata[key] = value
	}
	return metadata, nil
}

func mapString(m map[string]interface{}, key string) string {
	val, ok := m[key]
	if !ok || val == nil {
		return ""
	}
	s, ok := val.(string)
	if ok {
		return s
	}
	return fmt.Sprint(val)
}

func mapInt32(m map[string]interface{}, key string) int32 {
	return int32(mapInt64(m, key))
}

func mapInt64(m map[string]interface{}, key string) int64 {
	val, ok := m[key]
	if !ok || val == nil {
		return 0
	}
	switch v := val.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0
		}
		return n
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

func mapBool(m map[string]interface{}, key string) bool {
	val, ok := m[key]
	if !ok || val == nil {
		return false
	}
	v, ok := val.(bool)
	if ok {
		return v
	}
	return strings.EqualFold(fmt.Sprint(val), "true")
}

func mapStringMap(m map[string]interface{}, key string) map[string]string {
	out := make(map[string]string)
	val, ok := m[key]
	if !ok || val == nil {
		return out
	}

	switch meta := val.(type) {
	case map[string]string:
		for k, v := range meta {
			out[k] = v
		}
	case map[string]interface{}:
		for k, v := range meta {
			out[k] = fmt.Sprint(v)
		}
	}

	return out
}

func formatMillis(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05 UTC")
}
