package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSchemaContent_Draft7AndSize(t *testing.T) {
	require.NoError(t, validateSchemaContent("json-schema", `{"$schema":"http://json-schema.org/draft-07/schema#","allOf":[{"type":"object"}]}`))
	require.ErrorContains(t, validateSchemaContent("json-schema", `{"type":42}`), "invalid Draft 7 schema")
	require.ErrorContains(t, validateSchemaContent("json-schema", strings.Repeat(" ", maxSchemaContentBytes+1)), "exceeds maximum size")
}
