package webui

import (
	"testing"
	"time"
)

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
