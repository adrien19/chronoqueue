package pagination

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPageSize(t *testing.T) {
	tests := []struct {
		name      string
		requested int32
		want      int32
		wantError bool
	}{
		{name: "default", want: DefaultPageSize},
		{name: "minimum", requested: 1, want: 1},
		{name: "maximum", requested: MaxPageSize, want: MaxPageSize},
		{name: "negative", requested: -1, wantError: true},
		{name: "above maximum", requested: MaxPageSize + 1, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := PageSize(test.requested)
			if test.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestCursorRoundTripAndValidation(t *testing.T) {
	token, err := Encode("peek", "orders:1:3", 250)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	offset, err := Decode(token, "peek", "orders:1:3")
	require.NoError(t, err)
	require.EqualValues(t, 250, offset)

	for name, input := range map[string]struct {
		token  string
		scope  string
		filter string
	}{
		"malformed":        {token: "%%%", scope: "peek", filter: "orders:1:3"},
		"different scope":  {token: token, scope: "dlq", filter: "orders:1:3"},
		"different filter": {token: token, scope: "peek", filter: "payments:1:3"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Decode(input.token, input.scope, input.filter)
			require.Error(t, err)
		})
	}
}
