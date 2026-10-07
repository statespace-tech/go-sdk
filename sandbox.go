package statespace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/partite-ai/wacogo"
	"github.com/partite-ai/wacogo/wasi"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/experimental"
)

// sandbox compiles each component once and runs every call in a fresh
// instance with its own WASI host, which has no filesystem, network, or
// environment access.
type sandbox struct {
	engine     *wacogo.Engine
	mu         sync.Mutex
	components map[string]*wacogo.Component
}

func newSandbox() (*sandbox, error) {
	config := wazero.NewRuntimeConfigInterpreter().
		WithCoreFeatures(api.CoreFeaturesV2 | experimental.CoreFeaturesExtendedConst).
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(memoryLimitPages())
	engine := wacogo.NewEngine(context.Background(), wacogo.WithRuntimeConfig(config))
	return &sandbox{engine: engine, components: map[string]*wacogo.Component{}}, nil
}

// execute calls the component's execute export once. The context's deadline
// interrupts the guest.
func (box *sandbox) execute(ctx context.Context, sha256 string, artifact []byte, input []byte) ([]byte, error) {
	box.mu.Lock()
	component := box.components[sha256]
	var err error
	if component == nil {
		component, err = box.engine.LoadComponent(ctx, bytes.NewReader(artifact))
		if err == nil {
			box.components[sha256] = component
		}
	}
	box.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// A WASI host's resources belong to one instance, so each call gets its own.
	world, err := wasi.NewWorld(ctx, box.engine, &wasi.Config{
		Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard,
	})
	if err != nil {
		return nil, fmt.Errorf("create the WASI host: %w", err)
	}
	defer world.Close(context.Background())
	instance, err := component.Instantiate(ctx, wasiImports(world)...)
	if err != nil {
		return nil, err
	}
	defer instance.Close(context.Background())
	execute := instance.ExportedFunc("execute")
	if execute == nil {
		return nil, errors.New("the component does not export execute")
	}
	values, err := execute.Call(ctx, wacogo.ValString(input))
	if err != nil {
		return nil, err
	}
	if len(values) != 1 {
		return nil, errors.New("the component returned the wrong number of values")
	}
	result, ok := values[0].(wacogo.ValString)
	if !ok {
		return nil, errors.New("the component returned a non-string value")
	}
	return []byte(result), nil
}

func (box *sandbox) close() error {
	return box.engine.Close(context.Background())
}

// wasiImports provides the WASI 0.2 interfaces a component may import. The
// world has no preopened directories, network, or environment.
func wasiImports(world *wasi.World) []wacogo.InstantiateOption {
	const version = "@0.2.12"
	return []wacogo.InstantiateOption{
		wacogo.WithInstanceImport("wasi:cli/environment"+version, world.Environment.Core()),
		wacogo.WithInstanceImport("wasi:cli/exit"+version, world.Exit.Core()),
		wacogo.WithInstanceImport("wasi:cli/stdin"+version, world.Stdin.Core()),
		wacogo.WithInstanceImport("wasi:cli/stdout"+version, world.Stdout.Core()),
		wacogo.WithInstanceImport("wasi:cli/stderr"+version, world.Stderr.Core()),
		wacogo.WithInstanceImport("wasi:cli/terminal-input"+version, world.TerminalInput.Core()),
		wacogo.WithInstanceImport("wasi:cli/terminal-output"+version, world.TerminalOutput.Core()),
		wacogo.WithInstanceImport("wasi:cli/terminal-stdin"+version, world.TerminalStdin.Core()),
		wacogo.WithInstanceImport("wasi:cli/terminal-stdout"+version, world.TerminalStdout.Core()),
		wacogo.WithInstanceImport("wasi:cli/terminal-stderr"+version, world.TerminalStderr.Core()),
		wacogo.WithInstanceImport("wasi:io/error"+version, world.Error.Core()),
		wacogo.WithInstanceImport("wasi:io/poll"+version, world.Poll.Core()),
		wacogo.WithInstanceImport("wasi:io/streams"+version, world.Streams.Core()),
		wacogo.WithInstanceImport("wasi:clocks/monotonic-clock"+version, world.MonotonicClock.Core()),
		wacogo.WithInstanceImport("wasi:clocks/wall-clock"+version, world.WallClock.Core()),
		wacogo.WithInstanceImport("wasi:random/random"+version, world.Random.Core()),
		wacogo.WithInstanceImport("wasi:filesystem/types"+version, world.FilesystemTypes.Core()),
		wacogo.WithInstanceImport("wasi:filesystem/preopens"+version, world.FilesystemPreopens.Core()),
	}
}

// memoryLimitPages reads STATESPACE_MAX_MEMORY_BYTES, 256 MiB by default.
func memoryLimitPages() uint32 {
	const pageBytes = 65536
	limit, err := strconv.ParseUint(os.Getenv("STATESPACE_MAX_MEMORY_BYTES"), 10, 64)
	if err != nil || limit < pageBytes {
		limit = 256 * 1024 * 1024
	}
	return uint32(min(limit/pageBytes, 65536))
}
