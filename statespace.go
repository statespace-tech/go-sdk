// Package statespace runs Statespace experiments on parameter values and
// functions.
//
//	ranking, err := statespace.Load(ctx, "ranking")
//	group := ranking.Assign("u_42", map[string]any{"country": "US"})
//	rank := statespace.Function(group, "ranker", rerank)
//	topK := statespace.Value(group, "top_k", 20)
//	ranked, err := rank(ctx, items)
//	ranking.Log("u_42", "click", nil)
//
// Every read takes the application's current code or value as the default,
// which control receives and every failure falls back to.
package statespace

import (
	"context"
	"sync"
)

var (
	defaultClient *Client
	defaultErr    error
	defaultOnce   sync.Once
)

// Default returns the client that Load and Flush use. It reads SSP_API_KEY
// and STATESPACE_URL, or the session saved by `ssp login`.
func Default() (*Client, error) {
	defaultOnce.Do(func() {
		defaultClient, defaultErr = NewClient(Options{})
	})
	return defaultClient, defaultErr
}

// Load returns the experiment name from the default client. Handles are
// cached, so call it anywhere.
func Load(ctx context.Context, name string) (*Experiment, error) {
	client, err := Default()
	if err != nil {
		return nil, err
	}
	return client.Load(ctx, name)
}

// Flush waits until the default client's queued events are delivered.
func Flush(ctx context.Context) error {
	client, err := Default()
	if err != nil {
		return err
	}
	return client.Flush(ctx)
}
