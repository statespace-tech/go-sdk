package statespace

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sync"
	"time"
)

const (
	defaultEndpoint = "https://api.statespace.com"
	userAgent       = "statespace-go/0.1.1"
	maxEventBytes   = 64 * 1024
	maxBatch        = 100
	maxQueued       = 10_000
	refreshInterval = time.Minute
	maxArtifact     = 64 * 1024 * 1024
)

// Options configure a Client. Empty fields fall back to SSP_API_KEY and
// STATESPACE_URL, then to the session saved by `ssp login`.
type Options struct {
	APIKey   string
	Endpoint string
}

// Client connects an application to Statespace. It keeps one handle per
// experiment, refreshes configurations in the background, and delivers events
// in batches. Close it before the process exits to deliver queued events.
type Client struct {
	key      string
	endpoint string
	http     *http.Client

	mu          sync.Mutex
	experiments map[string]*Experiment
	artifacts   map[string][]byte
	sandbox     *sandbox

	events  chan event
	pending sync.WaitGroup
	dropped int
	done    chan struct{}
	stopped sync.WaitGroup
}

type event struct {
	kind string
	data map[string]any
}

// NewClient creates a client.
func NewClient(options Options) (*Client, error) {
	savedKey, savedEndpoint := savedLogin()
	key := firstNonEmpty(options.APIKey, os.Getenv("SSP_API_KEY"), savedKey)
	if key == "" {
		return nil, errors.New("statespace: set SSP_API_KEY or run `ssp login`")
	}
	endpoint := firstNonEmpty(options.Endpoint, os.Getenv("STATESPACE_URL"), savedEndpoint, defaultEndpoint)
	client := &Client{
		key:         key,
		endpoint:    trimSlash(endpoint),
		http:        &http.Client{Timeout: 60 * time.Second},
		experiments: map[string]*Experiment{},
		artifacts:   map[string][]byte{},
		events:      make(chan event, maxQueued),
		done:        make(chan struct{}),
	}
	client.stopped.Add(2)
	go client.deliver()
	go client.refresh()
	return client, nil
}

// Load returns the handle for one experiment, loading it on first use. If
// Statespace is unreachable, the handle serves defaults until a refresh
// succeeds. An unknown experiment or an invalid key is an error.
func (client *Client) Load(ctx context.Context, name string) (*Experiment, error) {
	if name == "" {
		return nil, errors.New("statespace: experiment name must not be empty")
	}
	client.mu.Lock()
	experiment := client.experiments[name]
	client.mu.Unlock()
	if experiment != nil {
		return experiment, nil
	}
	experiment = &Experiment{client: client, Name: name}
	config, err := client.load(ctx, name)
	var permanent *permanentError
	switch {
	case errors.As(err, &permanent):
		return nil, err
	case err != nil:
		slog.Warn("statespace: serving defaults", "experiment", name, "error", err)
	default:
		experiment.config.Store(config)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if existing := client.experiments[name]; existing != nil {
		return existing, nil
	}
	client.experiments[name] = experiment
	return experiment, nil
}

// Flush waits until queued events are delivered. It returns an error if ctx
// ends first or if any event was dropped since the last flush.
func (client *Client) Flush(ctx context.Context) error {
	delivered := make(chan struct{})
	go func() {
		client.pending.Wait()
		close(delivered)
	}()
	select {
	case <-delivered:
	case <-ctx.Done():
		return ctx.Err()
	}
	client.mu.Lock()
	dropped := client.dropped
	client.dropped = 0
	client.mu.Unlock()
	if dropped > 0 {
		return fmt.Errorf("statespace: %d events were dropped", dropped)
	}
	return nil
}

// Close delivers queued events and stops background work.
func (client *Client) Close(ctx context.Context) error {
	err := client.Flush(ctx)
	close(client.done)
	client.stopped.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.sandbox != nil {
		err = errors.Join(err, client.sandbox.close())
	}
	return err
}

type runtimeConfig struct {
	Name        string        `json:"name"`
	Version     int           `json:"version"`
	Status      string        `json:"status"`
	Salt        string        `json:"salt"`
	Eligibility *string       `json:"eligibility"`
	Groups      []groupConfig `json:"groups"`
	StaleAfter  float64       `json:"stale_after_seconds"`

	eligibility *eligibility
	fetchedAt   time.Time
}

func (client *Client) load(ctx context.Context, name string) (*runtimeConfig, error) {
	body, err := client.request(ctx, http.MethodGet, "/v1/runtime/experiments/"+url.PathEscape(name), nil)
	if err != nil {
		return nil, err
	}
	var config runtimeConfig
	if err := json.Unmarshal(body, &config); err != nil {
		return nil, fmt.Errorf("statespace: invalid configuration for %s: %w", name, err)
	}
	if config.eligibility, err = compileEligibility(config.Eligibility); err != nil {
		return nil, fmt.Errorf("statespace: invalid eligibility for %s: %w", name, err)
	}
	if config.StaleAfter == 0 {
		config.StaleAfter = (48 * time.Hour).Seconds()
	}
	config.fetchedAt = time.Now()
	for _, group := range config.Groups {
		for _, raw := range group.Parameters {
			var parameter struct{ Kind, SHA256 string }
			if json.Unmarshal(raw, &parameter) == nil && parameter.Kind == "function" {
				if _, err := client.artifact(ctx, parameter.SHA256); err != nil {
					return nil, err
				}
			}
		}
	}
	return &config, nil
}

func (client *Client) refresh() {
	defer client.stopped.Done()
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-client.done:
			return
		case <-ticker.C:
		}
		client.mu.Lock()
		experiments := make([]*Experiment, 0, len(client.experiments))
		for _, experiment := range client.experiments {
			experiments = append(experiments, experiment)
		}
		client.mu.Unlock()
		for _, experiment := range experiments {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			config, err := client.load(ctx, experiment.Name)
			cancel()
			if err != nil {
				slog.Warn("statespace: could not refresh", "experiment", experiment.Name, "error", err)
				continue
			}
			experiment.config.Store(config)
		}
	}
}

func (client *Client) artifact(ctx context.Context, sha256Hex string) ([]byte, error) {
	client.mu.Lock()
	artifact, ok := client.artifacts[sha256Hex]
	client.mu.Unlock()
	if ok {
		return artifact, nil
	}
	artifact, err := client.request(ctx, http.MethodGet, "/v1/runtime/artifacts/"+url.PathEscape(sha256Hex), nil)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(artifact)
	if hex.EncodeToString(digest[:]) != sha256Hex {
		return nil, fmt.Errorf("statespace: component %s does not match its hash", sha256Hex)
	}
	client.mu.Lock()
	client.artifacts[sha256Hex] = artifact
	client.mu.Unlock()
	return artifact, nil
}

func (client *Client) execute(ctx context.Context, sha256Hex string, input []byte) ([]byte, error) {
	artifact, err := client.artifact(ctx, sha256Hex)
	if err != nil {
		return nil, err
	}
	client.mu.Lock()
	if client.sandbox == nil {
		client.sandbox, err = newSandbox()
	}
	sandbox := client.sandbox
	client.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return sandbox.execute(ctx, sha256Hex, artifact, input)
}

// enqueue queues an event without blocking. A full queue drops the event.
func (client *Client) enqueue(kind string, data map[string]any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("statespace: events must contain only JSON values: %w", err)
	}
	if len(encoded) > maxEventBytes {
		return errors.New("statespace: an event must serialize to at most 64 KiB")
	}
	client.pending.Add(1)
	select {
	case client.events <- event{kind: kind, data: data}:
	default:
		slog.Warn("statespace: the event queue is full; dropping an event", "kind", kind)
		client.settle(1, true)
	}
	return nil
}

func (client *Client) settle(count int, dropped bool) {
	if dropped {
		client.mu.Lock()
		client.dropped += count
		client.mu.Unlock()
	}
	for range count {
		client.pending.Done()
	}
}

func (client *Client) deliver() {
	defer client.stopped.Done()
	for {
		var first event
		select {
		case first = <-client.events:
		case <-client.done:
			return
		}
		batch := []event{first}
	collect:
		for len(batch) < maxBatch {
			select {
			case next := <-client.events:
				batch = append(batch, next)
			default:
				break collect
			}
		}
		body := map[string][]map[string]any{"runs": {}, "outcomes": {}}
		for _, item := range batch {
			key := item.kind + "s"
			body[key] = append(body[key], item.data)
		}
		client.settle(len(batch), !client.post(body))
	}
}

// post sends one batch, retrying transient failures.
func (client *Client) post(body any) bool {
	encoded, _ := json.Marshal(body)
	delay := 500 * time.Millisecond
	for range 6 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err := client.request(ctx, http.MethodPost, "/v1/events", encoded)
		cancel()
		var permanent *permanentError
		switch {
		case err == nil:
			return true
		case errors.As(err, &permanent):
			slog.Error("statespace: events rejected", "error", err)
			return false
		}
		select {
		case <-time.After(delay):
		case <-client.done:
			return false
		}
		delay = min(delay*2, 8*time.Second)
	}
	slog.Error("statespace: could not deliver events")
	return false
}

// permanentError is a request that retrying cannot fix.
type permanentError struct{ message string }

func (err *permanentError) Error() string { return err.message }

func (client *Client) request(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+client.key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", userAgent)
	response, err := client.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("statespace: %s %s failed: %w", method, path, err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxArtifact+1))
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		message := fmt.Sprintf("statespace: %s %s returned HTTP %d", method, path, response.StatusCode)
		code := response.StatusCode
		if code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests {
			return nil, &permanentError{message}
		}
		return nil, errors.New(message)
	}
	return data, nil
}

// savedLogin reads the session saved by `ssp login`.
func savedLogin() (key, endpoint string) {
	path := os.Getenv("STATESPACE_CONFIG")
	if path == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return "", ""
		}
		if runtime.GOOS == "darwin" {
			base = filepath.Join(os.Getenv("HOME"), "Library", "Application Support")
		}
		path = filepath.Join(base, "statespace", "config.toml")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", ""
	}
	read := func(name string) string {
		match := regexp.MustCompile(`(?m)^` + name + `\s*=\s*"([^"]+)"\s*$`).FindSubmatch(contents)
		if match == nil {
			return ""
		}
		return string(match[1])
	}
	return read("token"), read("endpoint")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func trimSlash(value string) string {
	for len(value) > 0 && value[len(value)-1] == '/' {
		value = value[:len(value)-1]
	}
	return value
}
