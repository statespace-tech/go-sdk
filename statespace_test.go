package statespace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	treated = "u_1" // bucket 0.18, inside [0, 0.5)
	control = "u_2" // bucket 0.65
)

var us = map[string]any{"country": "US"}

// service is a fake Statespace service.
type service struct {
	mu          sync.Mutex
	experiments map[string]map[string]any
	runs        []map[string]any
	outcomes    []map[string]any
	identity    []byte
	sha256      string
}

func newService(t *testing.T) (*service, *Client) {
	t.Helper()
	identity, err := os.ReadFile("testdata/identity.wasm")
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(identity)
	state := &service{experiments: map[string]map[string]any{}, identity: identity, sha256: hex.EncodeToString(digest[:])}
	server := httptest.NewServer(http.HandlerFunc(state.serve))
	t.Cleanup(server.Close)
	client, err := NewClient(Options{APIKey: "ssp_test", Endpoint: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return state, client
}

func (state *service) serve(writer http.ResponseWriter, request *http.Request) {
	state.mu.Lock()
	defer state.mu.Unlock()
	name := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
	switch {
	case request.Method == http.MethodPost:
		var body struct{ Runs, Outcomes []map[string]any }
		_ = json.NewDecoder(request.Body).Decode(&body)
		state.runs = append(state.runs, body.Runs...)
		state.outcomes = append(state.outcomes, body.Outcomes...)
		writer.WriteHeader(http.StatusAccepted)
	case strings.HasPrefix(request.URL.Path, "/v1/runtime/experiments/") && state.experiments[name] != nil:
		_ = json.NewEncoder(writer).Encode(state.experiments[name])
	case request.URL.Path == "/v1/runtime/artifacts/"+state.sha256:
		_, _ = writer.Write(state.identity)
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (state *service) publishRanking(name, status string) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.experiments[name] = map[string]any{
		"name": name, "version": 1, "status": status, "salt": "salt",
		"eligibility": `context.country == "US"`, "stale_after_seconds": 172800,
		"groups": []map[string]any{
			{"name": "control", "ranges": [][2]float64{}, "parameters": map[string]any{}},
			{"name": "bm25", "ranges": [][2]float64{{0, 0.5}}, "parameters": map[string]any{
				"top_k":  map[string]any{"kind": "value", "value": 50},
				"label":  map[string]any{"kind": "value", "value": "bm25"},
				"ranker": map[string]any{"kind": "function", "sha256": state.sha256},
			}},
		},
	}
}

func flush(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// Every SDK asserts these values, so a subject lands in the same group in
// Python, TypeScript, and Go.
func TestBucketsMatchEverySDK(t *testing.T) {
	for _, vector := range []struct {
		salt, subject string
		expected      float64
	}{
		{"salt", "u_1", 0.18452190011276326},
		{"salt", "u_2", 0.6489778519534802},
		{"3f9a", "user-42", 0.19165740483602034},
	} {
		if got := bucket(vector.salt, vector.subject); got != vector.expected {
			t.Errorf("bucket(%q, %q) = %v, want %v", vector.salt, vector.subject, got, vector.expected)
		}
	}
}

func TestGroupsReturnValuesAndControlReturnsDefaults(t *testing.T) {
	state, client := newService(t)
	state.publishRanking("ranking", "running")
	ranking, err := client.Load(context.Background(), "ranking")
	if err != nil {
		t.Fatal(err)
	}

	group := ranking.Assign(treated, us)
	if group.Name != "bm25" || Value(group, "top_k", 10) != 50 || Value(group, "missing", "x") != "x" {
		t.Fatalf("unexpected treated values in %q", group.Name)
	}
	identity := Function(group, "ranker", func([]int) ([]int, error) { return nil, nil })
	if output, err := identity(context.Background(), []int{3, 1, 2}); err != nil || !slices.Equal(output, []int{3, 1, 2}) {
		t.Fatalf("function returned %v, %v", output, err)
	}

	// Every call runs in a fresh instance, so repeated calls behave the same.
	for range 3 {
		if output, err := identity(context.Background(), []int{2, 1}); err != nil || !slices.Equal(output, []int{2, 1}) {
			t.Fatalf("a repeated call returned %v, %v", output, err)
		}
	}

	group = ranking.Assign(control, us)
	sorted := Function(group, "ranker", func(items []int) ([]int, error) { return slices.Sorted(slices.Values(items)), nil })
	if output, _ := sorted(context.Background(), []int{3, 1, 2}); group.Name != "control" || !slices.Equal(output, []int{1, 2, 3}) {
		t.Fatalf("control returned %v in %q", output, group.Name)
	}

	flush(t, client)
	if len(state.runs) != 2 || state.runs[0]["group"] != "bm25" || state.runs[1]["group"] != "control" {
		t.Fatalf("unexpected runs %v", state.runs)
	}
}

func TestWrongTypesFallBackAndAreRecorded(t *testing.T) {
	state, client := newService(t)
	state.publishRanking("ranking", "running")
	ranking, _ := client.Load(context.Background(), "ranking")
	group := ranking.Assign(treated, us)
	if Value(group, "top_k", "ten") != "ten" || Value(group, "label", 1.5) != 1.5 || Value(group, "ranker", 0) != 0 {
		t.Fatal("mismatched values must return the default")
	}
	count := Function(group, "top_k", func(items []int) (int, error) { return len(items), nil })
	if output, _ := count(context.Background(), []int{1}); output != 1 {
		t.Fatalf("a value read as a function must return the default, got %d", output)
	}
	flush(t, client)
	if len(state.outcomes) != 4 || state.outcomes[0]["name"] != ErrorOutcome {
		t.Fatalf("unexpected outcomes %v", state.outcomes)
	}
}

func TestIneligibleAndStoppedExperimentsServeDefaults(t *testing.T) {
	state, client := newService(t)
	state.publishRanking("ranking", "running")
	state.publishRanking("stopped", "stopped")
	ranking, _ := client.Load(context.Background(), "ranking")
	if group := ranking.Assign(treated, map[string]any{"country": "CA"}); group.Name != "" || Value(group, "top_k", 10) != 10 {
		t.Fatal("ineligible subjects must get defaults")
	}
	stopped, _ := client.Load(context.Background(), "stopped")
	if stopped.Assign(treated, us).Name != "" {
		t.Fatal("a stopped experiment must not assign")
	}
	flush(t, client)
	if len(state.runs) != 1 || state.runs[0]["reason"] != "ineligible" {
		t.Fatalf("unexpected runs %v", state.runs)
	}
}

func TestUnknownExperimentsAreErrors(t *testing.T) {
	_, client := newService(t)
	if _, err := client.Load(context.Background(), "missing"); err == nil {
		t.Fatal("loading an unknown experiment must fail")
	}
}

func TestOutcomesNeedNoAssignment(t *testing.T) {
	state, client := newService(t)
	state.publishRanking("ranking", "running")
	ranking, _ := client.Load(context.Background(), "ranking")
	if err := ranking.Log("u_9", "purchase", map[string]any{"value": 12.5}); err != nil {
		t.Fatal(err)
	}
	flush(t, client)
	if state.outcomes[0]["subject_id"] != "u_9" {
		t.Fatalf("unexpected outcomes %v", state.outcomes)
	}
}
