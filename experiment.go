package statespace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"sync/atomic"
	"time"
)

// ErrorOutcome is the outcome recorded when a parameter falls back to the
// application's default.
const ErrorOutcome = "statespace.error"

const defaultTimeout = 5 * time.Second

// Experiment is one experiment. Get one with Load or Client.Load.
type Experiment struct {
	Name   string
	client *Client
	config atomic.Pointer[runtimeConfig]
}

// Assign assigns a subject to a group and records the assignment. context is
// the JSON the experiment's eligibility rule reads, and may be nil. The same
// subject always gets the same group. While the experiment is not running,
// or for ineligible subjects, the group has no parameters, so every read
// returns the application's default.
func (experiment *Experiment) Assign(subjectID string, context map[string]any) *Group {
	outside := &Group{experiment: experiment, subjectID: subjectID}
	config := experiment.config.Load()
	if subjectID == "" || config == nil || config.Status != "running" {
		return outside
	}
	if time.Since(config.fetchedAt).Seconds() > config.StaleAfter {
		slog.Warn("statespace: the configuration is stale; serving defaults", "experiment", experiment.Name)
		return outside
	}
	context = maps.Clone(context)
	if context == nil {
		context = map[string]any{}
	}
	reason := "ineligible"
	eligible, err := config.eligibility.evaluate(context)
	switch {
	case err != nil:
		slog.Warn("statespace: eligibility failed", "experiment", experiment.Name, "error", err)
		reason = "eligibility-error"
	case eligible:
		reason = "assigned"
	}
	var assigned *groupConfig
	if reason == "assigned" {
		assigned = choose(config.Groups, bucket(config.Salt, subjectID))
	}
	var groupName any
	if assigned != nil {
		groupName = assigned.Name
	}
	if err := experiment.client.enqueue("run", map[string]any{
		"id":         "run_" + randomID(),
		"experiment": config.Name,
		"version":    config.Version,
		"subject_id": subjectID,
		"group":      groupName,
		"reason":     reason,
		"context":    context,
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		slog.Warn("statespace: could not record the assignment", "error", err)
	}
	if assigned == nil {
		return outside
	}
	outside.Name = assigned.Name
	outside.parameters = assigned.Parameters
	return outside
}

// Log records an outcome, such as a click or a purchase, for a subject.
// Outcomes can come from any process; results count each one for the group
// the subject was assigned to before it. data may be nil.
func (experiment *Experiment) Log(subjectID, name string, data map[string]any) error {
	if subjectID == "" || name == "" {
		return errors.New("statespace: subjectID and name must not be empty")
	}
	if data == nil {
		data = map[string]any{}
	}
	return experiment.client.enqueue("outcome", map[string]any{
		"id":         "out_" + randomID(),
		"experiment": experiment.Name,
		"subject_id": subjectID,
		"name":       name,
		"data":       data,
		"timestamp":  time.Now().UTC().Format(time.RFC3339Nano),
	})
}

// Group is the group a subject was assigned to. Name is the group's name,
// "control", or empty when the subject is not in the experiment. Read its
// parameters with Value and Function.
type Group struct {
	Name       string
	experiment *Experiment
	subjectID  string
	parameters map[string]json.RawMessage
}

type parameter struct {
	Kind   string          `json:"kind"`
	Value  json.RawMessage `json:"value"`
	SHA256 string          `json:"sha256"`
}

func (group *Group) parameter(name string) (parameter, bool) {
	raw, ok := group.parameters[name]
	if !ok {
		return parameter{}, false
	}
	var decoded parameter
	if json.Unmarshal(raw, &decoded) != nil {
		return parameter{}, false
	}
	return decoded, true
}

// Value returns the group's value for name, decoded into the fallback's type,
// or fallback when the group does not set it or the value does not fit.
func Value[T any](group *Group, name string, fallback T) T {
	parameter, ok := group.parameter(name)
	if !ok {
		return fallback
	}
	if parameter.Kind != "value" {
		group.fail(name, "type", "is a function; read it with Function", 0)
		return fallback
	}
	var value T
	if err := json.Unmarshal(parameter.Value, &value); err != nil {
		group.fail(name, "type", err.Error(), 0)
		return fallback
	}
	return value
}

// FunctionOption configures a function returned by Function.
type FunctionOption func(*functionOptions)

type functionOptions struct{ timeout time.Duration }

// WithTimeout limits each call. The default is five seconds.
func WithTimeout(timeout time.Duration) FunctionOption {
	return func(options *functionOptions) { options.timeout = timeout }
}

// Function returns the group's function for name, or fallback. The function
// takes and returns JSON values and runs locally in a sandbox. If it fails,
// times out, or returns a value that does not decode into O, fallback runs
// instead, so the error is always fallback's own.
func Function[I, O any](
	group *Group,
	name string,
	fallback func(I) (O, error),
	options ...FunctionOption,
) func(context.Context, I) (O, error) {
	settings := functionOptions{timeout: defaultTimeout}
	for _, option := range options {
		option(&settings)
	}
	unassigned := func(_ context.Context, input I) (O, error) { return fallback(input) }
	parameter, ok := group.parameter(name)
	if !ok {
		return unassigned
	}
	if parameter.Kind != "function" {
		group.fail(name, "type", "is a value; read it with Value", 0)
		return unassigned
	}
	return func(ctx context.Context, input I) (O, error) {
		started := time.Now()
		output, failure, err := call[I, O](ctx, group.experiment.client, parameter.SHA256, input, settings.timeout)
		if err == nil {
			return output, nil
		}
		group.fail(name, failure, err.Error(), time.Since(started))
		return fallback(input)
	}
}

// call runs a component once and decodes its output.
func call[I, O any](ctx context.Context, client *Client, sha256 string, input I, timeout time.Duration) (O, string, error) {
	var output O
	encoded, err := json.Marshal(input)
	if err != nil {
		return output, "failed", fmt.Errorf("encode the input: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := client.execute(ctx, sha256, encoded)
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return output, "timeout", err
		}
		return output, "failed", err
	}
	if err := json.Unmarshal(result, &output); err != nil {
		return output, "invalid-output", err
	}
	return output, "", nil
}

func (group *Group) fail(parameter, kind, detail string, duration time.Duration) {
	slog.Warn("statespace: using the default", "group", group.Name, "parameter", parameter, "error", kind, "detail", detail)
	data := map[string]any{"group": group.Name, "parameter": parameter, "error": kind}
	if duration > 0 {
		data["duration_ms"] = float64(duration.Microseconds()) / 1000
	}
	_ = group.experiment.Log(group.subjectID, ErrorOutcome, data)
}

func randomID() string {
	var bytes [16]byte
	_, _ = rand.Read(bytes[:])
	return hex.EncodeToString(bytes[:])
}
