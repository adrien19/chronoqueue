package cluster

import (
	"errors"
	"testing"
)

func TestActiveClient(t *testing.T) {
	t.Run("no active cluster", func(t *testing.T) {
		store := NewStore("")
		client, err := store.ActiveClient()
		if client != nil || !errors.Is(err, ErrNoActiveCluster) {
			t.Fatalf("ActiveClient() = (%v, %v), want (nil, ErrNoActiveCluster)", client, err)
		}
	})

	t.Run("invalid endpoint", func(t *testing.T) {
		store := NewStore("")
		store.Seed("invalid", "://", true)
		client, err := store.ActiveClient()
		if client != nil || err == nil {
			t.Fatalf("ActiveClient() = (%v, %v), want construction error", client, err)
		}
	})

	t.Run("successful cached acquisition", func(t *testing.T) {
		store := NewStore("")
		store.Seed("local", "127.0.0.1:1", true)
		first, err := store.ActiveClient()
		if err != nil {
			t.Fatalf("first ActiveClient(): %v", err)
		}
		defer store.CloseAll()
		second, err := store.ActiveClient()
		if err != nil {
			t.Fatalf("second ActiveClient(): %v", err)
		}
		if first != second {
			t.Fatal("ActiveClient() did not return the cached client")
		}
	})
}
