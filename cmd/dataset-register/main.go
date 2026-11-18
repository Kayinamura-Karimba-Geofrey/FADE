// Command dataset-register verifies a dataset manifest and updates the replay
// dataset registry.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/replay/dataset"
)

func main() {
	dir := flag.String("dataset", "", "dataset directory")
	out := flag.String("out", "replay/datasets.json", "registry path")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "dataset-register: -dataset is required")
		os.Exit(1)
	}
	ds, err := dataset.Load(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dataset-register:", err)
		os.Exit(1)
	}
	reg := struct {
		Datasets map[string]any `json:"datasets"`
	}{Datasets: map[string]any{}}
	if raw, err := os.ReadFile(*out); err == nil {
		_ = json.Unmarshal(raw, &reg)
		if reg.Datasets == nil {
			reg.Datasets = map[string]any{}
		}
	}
	reg.Datasets[ds.Name] = map[string]any{
		"name": ds.Name, "path": *dir, "hash": ds.Hash,
		"events": ds.Events, "accounts": len(ds.Accounts),
		"verified_at": time.Now().UTC().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "dataset-register:", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "dataset-register:", err)
		os.Exit(1)
	}
	if err := os.WriteFile(*out, append(raw, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "dataset-register:", err)
		os.Exit(1)
	}
	names := make([]string, 0, len(reg.Datasets))
	for name := range reg.Datasets {
		names = append(names, name)
	}
	sort.Strings(names)
	fmt.Printf("dataset-register: %s verified (%s), registry has %d datasets\n", ds.Name, ds.Hash[:12], len(names))
}
