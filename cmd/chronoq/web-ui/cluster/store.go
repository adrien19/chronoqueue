package cluster

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc/credentials"

	"github.com/adrien19/chronoqueue/client"
)

var ErrNoActiveCluster = errors.New("no active cluster configured")

const (
	TransportPlaintext = "plaintext"
	TransportTLS       = "tls"
)

// Cluster holds connection details for a single ChronoQueue gRPC backend.
type Cluster struct {
	Slug           string `json:"slug"`
	Name           string `json:"name"`
	Description    string `json:"description"`
	BrokerAddress  string `json:"brokerAddress"`
	TransportMode  string `json:"transportMode"`
	SkipTLSVerify  bool   `json:"skipTLSVerify,omitempty"`
	TLSServerName  string `json:"tlsServerName,omitempty"`
	CACertFile     string `json:"caCertFile,omitempty"`
	ClientCertFile string `json:"clientCertFile,omitempty"`
	ClientKeyFile  string `json:"clientKeyFile,omitempty"`
	APIKeyEnv      string `json:"apiKeyEnv,omitempty"`
	SkipSSLCheck   bool   `json:"skipSSLCheck,omitempty"`
	IsActive       bool   `json:"isActive"`
}

// Store manages cluster definitions and their cached gRPC clients.
type Store struct {
	mu       sync.RWMutex
	clusters []*Cluster
	clients  map[string]*client.ChronoQueueClient
	filePath string
}

var (
	slugRe                = regexp.MustCompile(`[^a-z0-9]+`)
	environmentVariableRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

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
	for index, cluster := range tmp {
		if cluster == nil {
			return fmt.Errorf("parse cluster store: cluster at index %d is null", index)
		}
		normalizeCluster(cluster)
		if err := validateCluster(*cluster); err != nil {
			return fmt.Errorf("validate cluster %q: %w", cluster.Name, err)
		}
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
			TransportMode: mapLegacyTransport(skipSSL),
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
	normalizeCluster(&c)
	if err := validateCluster(c); err != nil {
		return err
	}
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
// when connection or authentication settings change.
func (s *Store) Update(slug string, updated Cluster) error {
	normalizeCluster(&updated)
	if err := validateCluster(updated); err != nil {
		return err
	}
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
		if connectionConfigChanged(*c, updated) {
			if cl, ok := s.clients[slug]; ok {
				cl.Close()
				delete(s.clients, slug)
			}
		}
		c.Name = updated.Name
		c.Description = updated.Description
		c.BrokerAddress = updated.BrokerAddress
		c.TransportMode = updated.TransportMode
		c.SkipTLSVerify = updated.SkipTLSVerify
		c.TLSServerName = updated.TLSServerName
		c.CACertFile = updated.CACertFile
		c.ClientCertFile = updated.ClientCertFile
		c.ClientKeyFile = updated.ClientKeyFile
		c.APIKeyEnv = updated.APIKeyEnv
		c.SkipSSLCheck = false
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
func (s *Store) ActiveClient() (*client.ChronoQueueClient, error) {
	active := s.ActiveCluster()
	if active == nil {
		return nil, ErrNoActiveCluster
	}
	if err := validateBrokerAddress(active.BrokerAddress); err != nil {
		return nil, fmt.Errorf("invalid broker address for cluster %q: %w", active.Name, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cl, ok := s.clients[active.Slug]; ok {
		return cl, nil
	}
	opts, err := clientOptions(*active)
	if err != nil {
		return nil, fmt.Errorf("configure client for cluster %q: %w", active.Name, err)
	}
	cl, err := client.NewChronoQueueClient(active.BrokerAddress, opts)
	if err != nil {
		return nil, fmt.Errorf("create client for cluster %q: %w", active.Name, err)
	}
	s.clients[active.Slug] = cl
	return cl, nil
}

func clientOptions(cluster Cluster) (client.ClientOptions, error) {
	normalizeCluster(&cluster)
	if err := validateCluster(cluster); err != nil {
		return client.ClientOptions{}, err
	}

	apiKeyEnv := cluster.APIKeyEnv
	if apiKeyEnv == "" {
		apiKeyEnv = "CHRONOQUEUE_API_KEY"
	}
	apiKey := os.Getenv(apiKeyEnv)
	if cluster.APIKeyEnv != "" && apiKey == "" {
		return client.ClientOptions{}, fmt.Errorf("API key environment variable %q is not set", apiKeyEnv)
	}

	opts := client.ClientOptions{MaxRetries: 3, APIKey: apiKey}
	if cluster.TransportMode == TransportPlaintext {
		return opts, nil
	}

	tlsConfig, err := clusterTLSConfig(cluster)
	if err != nil {
		return client.ClientOptions{}, err
	}
	opts.TLSCredentials = credentials.NewTLS(tlsConfig)
	return opts, nil
}

func clusterTLSConfig(cluster Cluster) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cluster.TLSServerName,
		InsecureSkipVerify: cluster.SkipTLSVerify, //nolint:gosec // explicitly configured for development clusters
	}
	if cluster.CACertFile != "" {
		caPEM, err := os.ReadFile(cluster.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read CA certificate: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse CA certificate")
		}
		tlsConfig.RootCAs = roots
	}
	if cluster.ClientCertFile != "" {
		certificate, err := tls.LoadX509KeyPair(cluster.ClientCertFile, cluster.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}

func validateBrokerAddress(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("host is required")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	return nil
}

func normalizeCluster(cluster *Cluster) {
	if cluster == nil {
		return
	}
	if cluster.TransportMode == "" {
		cluster.TransportMode = mapLegacyTransport(cluster.SkipSSLCheck)
	}
	cluster.SkipSSLCheck = false
}

func mapLegacyTransport(skipSSL bool) string {
	if skipSSL {
		return TransportPlaintext
	}
	return TransportTLS
}

func validateCluster(cluster Cluster) error {
	if err := validateBrokerAddress(cluster.BrokerAddress); err != nil {
		return fmt.Errorf("invalid broker address: %w", err)
	}
	if cluster.TransportMode != TransportPlaintext && cluster.TransportMode != TransportTLS {
		return fmt.Errorf("transport mode must be %q or %q", TransportPlaintext, TransportTLS)
	}
	if cluster.TransportMode == TransportPlaintext && (cluster.SkipTLSVerify || cluster.TLSServerName != "" || cluster.CACertFile != "" || cluster.ClientCertFile != "" || cluster.ClientKeyFile != "") {
		return fmt.Errorf("TLS settings require TLS transport")
	}
	if (cluster.ClientCertFile == "") != (cluster.ClientKeyFile == "") {
		return fmt.Errorf("client certificate and key files must be configured together")
	}
	if cluster.APIKeyEnv != "" && !environmentVariableRe.MatchString(cluster.APIKeyEnv) {
		return fmt.Errorf("API key environment variable name is invalid")
	}
	return nil
}

func connectionConfigChanged(current, updated Cluster) bool {
	return current.BrokerAddress != updated.BrokerAddress ||
		current.TransportMode != updated.TransportMode ||
		current.SkipTLSVerify != updated.SkipTLSVerify ||
		current.TLSServerName != updated.TLSServerName ||
		current.CACertFile != updated.CACertFile ||
		current.ClientCertFile != updated.ClientCertFile ||
		current.ClientKeyFile != updated.ClientKeyFile ||
		current.APIKeyEnv != updated.APIKeyEnv
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
