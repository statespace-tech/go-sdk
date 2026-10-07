package statespace

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"cel.dev/cel-go/cel"
)

// groupConfig is one group of a published version.
type groupConfig struct {
	Name       string                     `json:"name"`
	Ranges     [][2]float64               `json:"ranges"`
	Parameters map[string]json.RawMessage `json:"parameters"`
}

// bucket maps a subject to a uniform point in [0, 1). The first 64 bits of
// SHA-256(salt:subjectID) keep 53 bits, which a float64 holds exactly. Every
// Statespace SDK computes the same value.
func bucket(salt, subjectID string) float64 {
	digest := sha256.Sum256([]byte(salt + ":" + subjectID))
	return float64(binary.BigEndian.Uint64(digest[:8])>>11) / (1 << 53)
}

// choose returns the group whose ranges contain point, or control.
func choose(groups []groupConfig, point float64) *groupConfig {
	var control *groupConfig
	for index := range groups {
		group := &groups[index]
		for _, interval := range group.Ranges {
			if interval[0] <= point && point < interval[1] {
				return group
			}
		}
		if group.Name == "control" {
			control = group
		}
	}
	return control
}

// eligibility is a CEL expression over the assignment context, compiled once.
type eligibility struct {
	program cel.Program
}

func compileEligibility(expression *string) (*eligibility, error) {
	if expression == nil {
		return &eligibility{}, nil
	}
	environment, err := cel.NewEnv(cel.Variable("context", cel.DynType))
	if err != nil {
		return nil, err
	}
	ast, issues := environment.Compile(*expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := environment.Program(ast)
	if err != nil {
		return nil, err
	}
	return &eligibility{program: program}, nil
}

// evaluate reports whether context is eligible. Evaluation errors are returned.
func (rule *eligibility) evaluate(context map[string]any) (bool, error) {
	if rule.program == nil {
		return true, nil
	}
	value, _, err := rule.program.Eval(map[string]any{"context": context})
	if err != nil {
		return false, err
	}
	eligible, ok := value.Value().(bool)
	if !ok {
		return false, fmt.Errorf("eligibility returned %T, not a boolean", value.Value())
	}
	return eligible, nil
}
