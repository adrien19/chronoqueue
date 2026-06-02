package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"google.golang.org/grpc/credentials"

	"github.com/adrien19/chronoqueue/client"
)

// Cluster holds connection details for a single ChronoQueue gRPC backend.
type Cluster struct {
	Slug          string `json:"slug"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	BrokerAddress string `json:"brokerAddress"`
	SkipSSLCheck  bool   `json:"skipSSLCheck"`
	IsActive      bool   `json:"isActive"`
}

// Store manages cluster definitions and their cached gRPC clients.
type Store struct {
	mu       sync.RWMutex
	clusters []*Cluster
	clients  map[string]*client.ChronoQueueClient
	filePath string
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

// SlugFor converts a broker address into a URL-safe slug (e.g. "localhost:9000" → "localhost-9000").
func SlugFor(brokerAddr string) string {
	s := strings.ToLower(brokerAddr)
	s = slugRe.ReplaceAllString(s, "-")
	return strings.Trim(s, "-")
}

// NewStore returns a Store with an optional file path for persistence.
func NewStore(filePath string) *Store {
	return &Store{
		clients:  make(map[string]*client.ChronoQueueClient),
		filePath: filePath,
	}
}

// Load reads persisted clusters from disk. Missing file is silently ignored.
func (s *Store) Load() error {
	if s.filePath == "" {
		return nil
	}
	data, err := os.ReadFile(s.filePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read cluster store: %w", err)
	}
	var tmp []*Cluster
	if err := json.Unmarshal(data, &tmp); err != nil {
		return fmt.Errorf("parse cluster store: %w", err)
	}
	s.mu.Lock()
	s.clusters = tmp
	s.mu.Unlock()
	return nil
}

// Seed adds a bootstrap cluster when the store is empty (e.g. first run).
func (s *Store) Seed(name, brokerAddr string, skipSSL bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.clusters) == 0 {
		s.clusters = append(s.clusters, &Cluster{
			Slug:          SlugFor(brokerAddr),
			Name:          name,
			Description:   "Default ChronoQueue server",
			BrokerAddress: brokerAddr,
			SkipSSLCheck:  skipSSL,
			IsActive:      true,
		})
	}
}

// List returns a snapshot of all clusters.
func (s *Store) List() []*Cluster {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Cluster, len(s.clusters))
	for i, c := range s.clusters {
		cp := *c
		out[i] = &cp
	}
	return out
}

// Get returns a single cluster by slug, or (nil, false) if not found.
func (s *Store) Get(slug string) (*Cluster, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.clusters {
		if c.Slug == slug {
			cp := *c
			return &cp, true
		}
	}
	return nil, false
}

// Add appends a new cluster. Returns an error if the slug or name already exists.
func (s *Store) Add(c Cluster) error {
	if c.Slug == "" {
		c.Slug = SlugFor(c.BrokerAddress)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.clusters {
		if existing.Slug == c.Slug {
			return fmt.Errorf("a cluster with address %q already exists", c.BrokerAddress)
		}
		if strings.EqualFold(existing.Name, c.Name) {
			return fmt.Errorf("a cluster named %q already exists", c.Name)
		}
	}
	if len(s.clusters) == 0 {
		c.IsActive = true
	}
	s.clusters = append(s.clusters, &c)
	return s.save()
}

// Update replaces editable fields of an existing cluster. Invalidates the cached client
// when the broker address changes.
func (s *Store) Update(slug string, updated Cluster) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.clusters {
		if c.Slug == slug {
			continue
		}
		if strings.EqualFold(c.Name, updated.Name) {
			return fmt.Errorf("a cluster named %q already exists", updated.Name)
		}
	}
	for _, c := range s.clusters {
		if c.Slug != slug {
			continue
		}
		if c.BrokerAddress != updated.BrokerAddress || c.SkipSSLCheck != updated.SkipSSLCheck {
			if cl, ok := s.clients[slug]; ok {
				cl.Close()
				delete(s.clients, slug)
			}
		}
		c.Name = updated.Name
		c.Description = updated.Description
		c.BrokerAddress = updated.BrokerAddress
		c.SkipSSLCheck = updated.SkipSSLCheck
		return s.save()
	}
	return fmt.Errorf("cluster %q not found", slug)
}

// Delete removes a cluster and closes its cached client.
// Returns an error when attempting to delete the currently-active cluster.
func (s *Store) Delete(slug string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, c := range s.clusters {
		if c.Slug != slug {
			continue
		}
		if c.IsActive {
			return fmt.Errorf("cannot delete the active cluster; switch to another cluster first")
		}
		if cl, ok := s.clients[slug]; ok {
			cl.Close()
			delete(s.clients, slug)
		}
		s.clusters = append(s.clusters[:i], s.clusters[i+1:]...)
		return s.save()
	}
	return fmt.Errorf("cluster %q not found", slug)
}

// SetActive marks a cluster as active and all others as inactive.
func (s *Store) SetActive(slug string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	found := false
	for _, c := range s.clusters {
		if c.Slug == slug {
			c.IsActive = true
			found = true
		} else {
			c.IsActive = false
		}
	}
	if !found {
		return fmt.Errorf("cluster %q not found", slug)
	}
	return s.save()
}

// ActiveCluster returns the currently-active cluster, or the first cluster as a fallback.
func (s *Store) ActiveCluster() *Cluster {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.clusters {
		if c.IsActive {
			cp := *c
			return &cp
		}
	}
	if len(s.clusters) > 0 {
		cp := *s.clusters[0]
		return &cp
	}
	return nil
}

// ActiveClient returns the cached gRPC client for the active cluster, creating it lazily if needed.
func (s *Store) ActiveClient() *client.ChronoQueueClient {
	active := s.ActiveCluster()
	if active == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl, ok := s.clients[active.Slug]; ok {
		return cl
	}
	opts := client.ClientOptions{
		MaxRetries: 3,
	}
	if !active.SkipSSLCheck {
		opts.TLSCredentials = credentials.NewClientTLSFromCert(nil, "")
	}
	cl, err := client.NewChronoQueueClient(active.BrokerAddress, opts)
	if err != nil {
		return nil
	}
	s.clients[active.Slug] = cl
	return cl
}

// CloseAll closes every cached gRPC client.
func (s *Store) CloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cl := range s.clients {
		cl.Close()
	}
	s.clients = make(map[string]*client.ChronoQueueClient)
}

// save persists the cluster list to disk atomically (caller must hold mu).
func (s *Store) save() error {
	if s.filePath == "" {
		return nil
	}
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s.clusters, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "clusters-*.json")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, s.filePath)
}
