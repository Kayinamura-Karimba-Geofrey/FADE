// Command rules-validate validates every rules/*.json file offline.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

func main() {
	dir := flag.String("dir", "rules", "rules directory")
	flag.Parse()

	files, err := os.ReadDir(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rules-validate:", err)
		os.Exit(2)
	}
	var paths []string
	for _, e := range files {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			paths = append(paths, filepath.Join(*dir, e.Name()))
		}
	}
	sort.Strings(paths)
	if len(paths) == 0 {
		fmt.Fprintln(os.Stderr, "rules-validate: no *.json files in", *dir)
		os.Exit(2)
	}
	failed := false
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("FAIL %s: %v\n", path, err)
			failed = true
			continue
		}
		rs, err := rules.Load(data)
		if err != nil {
			fmt.Printf("FAIL %s\n", path)
			var ce *rules.CompileError
			if errors.As(err, &ce) {
				for _, e := range ce.Errors {
					fmt.Printf("  %s: %s\n", e.Path, e.Message)
				}
			} else {
				fmt.Printf("  %v\n", err)
			}
			failed = true
			continue
		}
		fmt.Printf("OK   %s rule_set=%s version=%d rules=%d hash=%s\n",
			path, rs.RuleSet, rs.Version, len(rs.Rules()), rs.Hash[:12])
	}
	if failed {
		os.Exit(1)
	}
}
