package schema

import (
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestValidateSchemaContent_Draft7AndSize(t *testing.T) {
	require.NoError(t, validateSchemaContent("json-schema", `{"$schema":"http://json-schema.org/draft-07/schema#","allOf":[{"type":"object"}]}`))
	require.ErrorContains(t, validateSchemaContent("json-schema", `{"type":42}`), "invalid Draft 7 schema")
	require.ErrorContains(t, validateSchemaContent("json-schema", strings.Repeat(" ", maxSchemaContentBytes+1)), "exceeds maximum size")
}

func TestValidateSchemaContentRejectsExternalReferenceWithoutResolvingIt(t *testing.T) {
	var requests atomic.Int32
	previousTransport := http.DefaultTransport
	http.DefaultTransport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return nil, errors.New("unexpected external schema request")
	})
	t.Cleanup(func() { http.DefaultTransport = previousTransport })

	err := validateSchemaContent("json-schema", `{"type":"object","properties":{"value":{"$ref":"https://schemas.example.test/schema.json"}}}`)
	require.ErrorContains(t, err, "external schema reference")
	require.Zero(t, requests.Load())
	require.NoError(t, validateSchemaContent("json-schema", `{"$defs":{"value":{"type":"string"}},"$ref":"#/$defs/value"}`))
}
