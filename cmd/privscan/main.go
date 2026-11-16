// Command privscan fails when a raw personal identifier appears in source,
// fixtures or build artefacts. It is the automated guard behind the Phase 5
// privacy requirement: FADE stores hashed identifiers only.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/redaction"
)

// Finding is one reported leak.
type Finding struct {
	Path   string `json:"path"`
	Line   int    `json:"line"`
	Kind   string `json:"kind"`
	Offset int    `json:"offset"`
}

// Report is the machine-readable output.
type Report struct {
	Scanned  int       `json:"scanned"`
	Skipped  int       `json:"skipped"`
	Findings []Finding `json:"findings"`
	OK       bool      `json:"ok"`
}

// skipDirs are never scanned.
var skipDirs = map[string]bool{
	".git": true, "node_modules": true, "vendor": true, ".venv": true,
	"dist": true, "build": true, "__pycache__": true, ".idea": true,
}

// skipExt are binary or generated extensions.
var skipExt = map[string]bool{
	".png": true, ".jpg": true, ".jpeg": true, ".gif": true, ".webp": true,
	".pdf": true, ".zip": true, ".gz": true, ".tar": true, ".whl": true,
	".pyc": true, ".so": true, ".dylib": true, ".exe": true, ".ico": true,
	".woff": true, ".woff2": true, ".ttf": true, ".parquet": true, ".onnx": true,
	".pkl": true, ".joblib": true, ".bin": true,
}

const maxFileBytes = 4 << 20

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "privscan:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("privscan", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "emit a JSON report")
	quiet := fs.Bool("quiet", false, "only print findings")
	allow := fs.String("allow", "", "comma-separated paths/globs to skip (test fixtures)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	roots := fs.Args()
	if len(roots) == 0 {
		roots = []string{"."}
	}
	allowList := splitList(*allow)

	report := Report{Findings: []Finding{}}
	for _, root := range roots {
		if err := scanPath(root, allowList, &report); err != nil {
			return err
		}
	}
	sortFindings(report.Findings)
	report.OK = len(report.Findings) == 0

	if *asJSON {
		if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
			return err
		}
	} else if len(report.Findings) > 0 {
		for _, f := range report.Findings {
			fmt.Fprintf(os.Stdout, "%s:%d: %s at offset %d\n", f.Path, f.Line, f.Kind, f.Offset)
		}
	}
	if !*quiet && !*asJSON {
		fmt.Fprintf(os.Stderr, "privscan: scanned %d files, skipped %d, findings %d\n",
			report.Scanned, report.Skipped, len(report.Findings))
	}
	if !report.OK {
		return fmt.Errorf("%d raw personal identifier(s) found", len(report.Findings))
	}
	return nil
}

func splitList(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func allowed(path string, allow []string) bool {
	base := filepath.Base(path)
	for _, a := range allow {
		if ok, _ := filepath.Match(a, path); ok {
			return true
		}
		// A bare glob such as "*_test.go" matches the file name at any depth.
		if ok, _ := filepath.Match(a, base); ok {
			return true
		}
		if strings.HasSuffix(path, string(filepath.Separator)+a) || strings.HasPrefix(path, a) {
			return true
		}
	}
	return false
}

func scanPath(root string, allow []string, report *Report) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return scanFile(root, allow, report)
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if skipDirs[d.Name()] && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		return scanFile(path, allow, report)
	})
}

func scanFile(path string, allow []string, report *Report) error {
	if allowed(path, allow) || skipExt[strings.ToLower(filepath.Ext(path))] {
		report.Skipped++
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if info.Size() > maxFileBytes {
		report.Skipped++
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	report.Scanned++

	reader := bufio.NewReader(f)
	line := 0
	for {
		text, err := reader.ReadString('\n')
		if text != "" {
			line++
			for _, finding := range redaction.Scan(text) {
				report.Findings = append(report.Findings, Finding{
					Path: path, Line: line, Kind: string(finding.Kind), Offset: finding.Offset,
				})
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// sortFindings makes output deterministic.
func sortFindings(f []Finding) {
	sort.Slice(f, func(i, j int) bool {
		if f[i].Path != f[j].Path {
			return f[i].Path < f[j].Path
		}
		if f[i].Line != f[j].Line {
			return f[i].Line < f[j].Line
		}
		return f[i].Offset < f[j].Offset
	})
}
