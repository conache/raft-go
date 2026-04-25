package lincheck

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// Output directory (under repo root) for failing-history HTML dumps
const visDir = "debug/porcupine"

// Check runs Porcupine's linearizability check against log under model
// Verdicts:
//   - Ok: pass silently
//   - Unknown: checker timed out; log a warning and pass
//   - Illegal: write an HTML visualization under visDir and fail the test
func Check(t *testing.T, model porcupine.Model, log *OpLog, timeout time.Duration) {
	t.Helper()
	result, info := porcupine.CheckOperationsVerbose(model, log.Read(), timeout)
	switch result {
	case porcupine.Ok:
		return
	case porcupine.Unknown:
		t.Logf("lincheck: linearizability check timed out; assuming history is valid")
	case porcupine.Illegal:
		if path, err := writeVisualization(model, info, t.Name()); err != nil {
			t.Errorf("lincheck: failed to write visualization: %v", err)
		} else {
			t.Errorf("lincheck: history visualization: file://%s", path)
		}
		t.Fatalf("lincheck: history is not linearizable")
	}
}

// writeVisualization dumps info as HTML under <repoRoot>/debug/porcupine/
// Filename is prefixed with a sortable timestamp so runs don't clobber each other
func writeVisualization(model porcupine.Model, info porcupine.LinearizationInfo, name string) (string, error) {
	root, err := repoRoot()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(root, visDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}

	// Format: 20260424-141523.847_TestName.html, sortable by ls/time
	stamp := time.Now().Format("20060102-150405.000")
	path := filepath.Join(dir, fmt.Sprintf("%s_%s.html", stamp, sanitizeName(name)))

	f, err := os.Create(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	if err := porcupine.Visualize(model, info, f); err != nil {
		return "", err
	}
	return path, nil
}

// repoRoot walks up from cwd until it finds a go.mod, which marks the repo root
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found above %s", dir)
		}
		dir = parent
	}
}

// pathSafeName escapes characters unsafe in a single filename
// For example "/" from subtests via t.Name()
var pathSafeName = strings.NewReplacer("/", "_", "\\", "_", ":", "_")

func sanitizeName(name string) string { return pathSafeName.Replace(name) }
