package rules

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Persister stores rule-set versions and the hash-chained audit log outside the
// process. SaveActivation must be non-blocking: activation never waits on
// Postgres, and an unreachable database is queued to a disk spool.
type Persister interface {
	SaveActivation(ctx context.Context, rs *RuleSet, actor string) error
	Active(ctx context.Context) (*RuleSet, error)
	Versions(ctx context.Context) ([]Snapshot, error)
}

// Hooks receive observability events; any hook may be nil.
type Hooks struct {
	OnReload   func(result string, version int)
	OnActivate func(version int)
	OnRollback func(from, to int)
	OnVersion  func(version int)
}

const historyLimit = 20

// Manager owns the active rule set. Readers never lock: the pointer is swapped
// atomically after a full compile, so in-flight requests finish on the old set.
type Manager struct {
	dir     string
	logger  *slog.Logger
	persist Persister
	hooks   Hooks

	current atomic.Pointer[RuleSet]
	source  atomic.Value // string

	mu           sync.Mutex
	history      []*RuleSet
	previous     *RuleSet
	lastVersions map[string]int
	lastErr      error
	loadedAt     time.Time
	swapAt       time.Time
	rollbackDone bool
	lastActor    string

	evalOK   atomic.Int64
	evalFail atomic.Int64

	// Rollback monitor knobs. Defaults implement the 30 s / 1% policy;
	// tests shrink them.
	rollbackTick       time.Duration
	rollbackWindow     time.Duration
	rollbackMinSamples int64
}

// NewManager builds a manager for a rules directory. Nothing is loaded until
// LoadStartup or Reload is called.
func NewManager(dir string, logger *slog.Logger, persist Persister, hooks Hooks) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Manager{
		dir:                dir,
		logger:             logger,
		persist:            persist,
		hooks:              hooks,
		lastVersions:       map[string]int{},
		rollbackTick:       time.Second,
		rollbackWindow:     30 * time.Second,
		rollbackMinSamples: 20,
	}
	if persist == nil {
		m.persist = nil
	}
	return m
}

// Path returns the rules directory.
func (m *Manager) Path() string { return m.dir }

// Current returns the active rule set. It is never nil after construction.
func (m *Manager) Current() *RuleSet {
	if rs := m.current.Load(); rs != nil {
		return rs
	}
	return EmptyRuleSet()
}

// Degraded reports whether no persistent rule set is active.
func (m *Manager) Degraded() bool {
	rs := m.current.Load()
	return rs == nil || rs.Empty
}

// Source returns "postgres", "file" or "empty".
func (m *Manager) Source() string {
	if v, ok := m.source.Load().(string); ok {
		return v
	}
	return "empty"
}

// LoadedAt returns when the active set was installed.
func (m *Manager) LoadedAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.loadedAt
}

// LastError returns the last rejected reload error, if any.
func (m *Manager) LastError() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastErr
}

// Snapshot returns metadata for the active set.
func (m *Manager) Snapshot() Snapshot {
	rs := m.Current()
	return rs.Snapshot(true, m.Source())
}

// LoadStartup installs the Postgres active set, else the newest valid file,
// else the built-in empty set (allow-all, degraded readiness).
func (m *Manager) LoadStartup(ctx context.Context) error {
	if m.persist != nil {
		if rs, err := m.persist.Active(ctx); err == nil && rs != nil {
			m.install(rs, "postgres")
			m.logger.Info("rules loaded from postgres", "rule_set", rs.RuleSet,
				"version", rs.Version, "hash", rs.Hash)
			return nil
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			m.logger.Warn("postgres rule set unavailable; falling back to file", "error", err)
		}
	}
	if _, err := m.Reload(ctx, "startup"); err != nil {
		m.setError(err)
		if m.current.Load() == nil {
			m.install(EmptyRuleSet(), "empty")
			m.logger.Error("no valid rule set; starting degraded", "error", err)
		}
		return err
	}
	return nil
}

// ListFiles returns the rule JSON files in the directory, sorted.
func (m *Manager) ListFiles() ([]string, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return nil, fmt.Errorf("read rules dir %s: %w", m.dir, err)
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		files = append(files, filepath.Join(m.dir, e.Name()))
	}
	sort.Strings(files)
	return files, nil
}

type candidate struct {
	path string
	data []byte
	file RuleSetFile
}

// Reload scans rules/*.json, validates every file, activates the highest
// version, and only then swaps. Any invalid file rejects the whole reload and
// leaves the previous set serving.
func (m *Manager) Reload(ctx context.Context, actor string) (*RuleSet, error) {
	start := time.Now()
	files, err := m.ListFiles()
	if err != nil {
		m.rejected(err)
		return nil, err
	}
	if len(files) == 0 {
		err := fmt.Errorf("no rule files in %s", m.dir)
		m.rejected(err)
		return nil, err
	}

	var errs []ValidationError
	candidates := make([]candidate, 0, len(files))
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			errs = append(errs, ValidationError{Path: path, Message: err.Error()})
			continue
		}
		f, err := ParseFile(data)
		if err != nil {
			var ce *CompileError
			if errors.As(err, &ce) {
				for _, e := range ce.Errors {
					errs = append(errs, ValidationError{Path: path + ": " + e.Path, Message: e.Message})
				}
			} else {
				errs = append(errs, ValidationError{Path: path, Message: err.Error()})
			}
			continue
		}
		candidates = append(candidates, candidate{path: path, data: data, file: f})
	}
	if len(errs) > 0 {
		err := &CompileError{Errors: errs}
		m.rejected(err)
		return nil, err
	}
	if len(candidates) == 0 {
		err := fmt.Errorf("no valid rule files in %s", m.dir)
		m.rejected(err)
		return nil, err
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		vi, vj := candidates[i].file.Version, candidates[j].file.Version
		if vi != vj {
			return vi > vj
		}
		ni, nj := candidates[i].file.RuleSet, candidates[j].file.RuleSet
		if ni != nj {
			return ni < nj
		}
		return candidates[i].path < candidates[j].path
	})
	pick := candidates[0]
	rs, err := Compile(pick.file)
	if err != nil {
		m.rejected(err)
		return nil, err
	}

	cur := m.current.Load()
	if cur != nil && !cur.Empty && cur.RuleSet == rs.RuleSet {
		if cur.Hash == rs.Hash {
			m.mu.Lock()
			m.loadedAt = time.Now()
			m.mu.Unlock()
			m.logger.Info("rules unchanged", "version", rs.Version, "hash", rs.Hash)
			return cur, nil
		}
		if rs.Version <= m.lastVersion(rs.RuleSet) {
			err := &CompileError{Errors: []ValidationError{{
				Path: pick.path + ": version",
				Message: fmt.Sprintf("version %d must increase over active version %d for rule_set %q",
					rs.Version, m.lastVersion(rs.RuleSet), rs.RuleSet),
			}}}
			m.rejected(err)
			return nil, err
		}
	}

	m.install(rs, "file")
	if m.persist != nil {
		if err := m.persist.SaveActivation(ctx, rs, actor); err != nil {
			m.logger.Warn("rule activation queued for persistence", "error", err)
		}
	}
	m.logger.Info("rules reloaded", "rule_set", rs.RuleSet, "version", rs.Version,
		"hash", rs.Hash, "rules", len(rs.rules), "file", pick.path,
		"duration", time.Since(start).String())
	return rs, nil
}

// Activate installs an already compiled rule set, enforcing monotonic versions.
func (m *Manager) Activate(ctx context.Context, rs *RuleSet, actor string) error {
	cur := m.current.Load()
	if cur != nil && !cur.Empty && cur.RuleSet == rs.RuleSet && cur.Hash != rs.Hash &&
		rs.Version <= m.lastVersion(rs.RuleSet) {
		return &CompileError{Errors: []ValidationError{{
			Path:    "version",
			Message: fmt.Sprintf("version %d must increase over %d", rs.Version, m.lastVersion(rs.RuleSet)),
		}}}
	}
	m.install(rs, "file")
	if m.persist != nil {
		if err := m.persist.SaveActivation(ctx, rs, actor); err != nil {
			m.logger.Warn("rule activation queued for persistence", "error", err)
		}
	}
	return nil
}

// PutRule creates version+1 of the active set with one rule added or replaced.
func (m *Manager) PutRule(ctx context.Context, id string, rule Rule, actor string) (*RuleSet, error) {
	cur := m.Current()
	file := cur.File()
	if file.RuleSet == "empty" {
		file = RuleSetFile{RuleSet: "prod", Author: actor, Config: Config{}}
	}
	file.Version = cur.Version + 1
	file.Author = actor
	file.Changelog = "PUT " + id
	found := false
	for i := range file.Rules {
		if file.Rules[i].ID == id {
			file.Rules[i] = rule
			found = true
			break
		}
	}
	if !found {
		file.Rules = append(file.Rules, rule)
	}
	if err := checkRuleIDMatch(id, rule.ID); err != nil {
		return nil, err
	}
	rs, err := Compile(file)
	if err != nil {
		return nil, err
	}
	if err := m.Activate(ctx, rs, actor); err != nil {
		return nil, err
	}
	return rs, nil
}

func checkRuleIDMatch(pathID, ruleID string) error {
	if pathID != ruleID {
		return &CompileError{Errors: []ValidationError{{
			Path: "rules.id", Message: fmt.Sprintf("body id %q does not match path id %q", ruleID, pathID),
		}}}
	}
	return nil
}

// install swaps a compiled set and records history. It is the only writer.
func (m *Manager) install(rs *RuleSet, source string) {
	m.mu.Lock()
	cur := m.current.Load()
	if cur != nil && cur.Hash == rs.Hash && cur.RuleSet == rs.RuleSet {
		m.loadedAt = time.Now()
		m.mu.Unlock()
		return
	}
	if cur != nil {
		m.history = append(m.history, cur)
		if len(m.history) > historyLimit {
			m.history = m.history[len(m.history)-historyLimit:]
		}
		m.previous = cur
	}
	m.current.Store(rs)
	m.source.Store(source)
	m.lastVersions[rs.RuleSet] = rs.Version
	m.loadedAt = time.Now()
	m.swapAt = time.Now()
	m.rollbackDone = false
	m.evalOK.Store(0)
	m.evalFail.Store(0)
	m.lastErr = nil
	m.mu.Unlock()

	if m.hooks.OnActivate != nil {
		m.hooks.OnActivate(rs.Version)
	}
	if m.hooks.OnVersion != nil {
		m.hooks.OnVersion(rs.Version)
	}
	if m.hooks.OnReload != nil {
		m.hooks.OnReload("ok", rs.Version)
	}
}

func (m *Manager) rejected(err error) {
	m.setError(err)
	if m.hooks.OnReload != nil {
		m.hooks.OnReload("rejected", 0)
	}
}

func (m *Manager) setError(err error) {
	m.mu.Lock()
	m.lastErr = err
	m.mu.Unlock()
	m.logger.Error("rule reload rejected; previous set stays active", "error", err)
}

func (m *Manager) lastVersion(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastVersions[name]
}

// RecordEval feeds the auto-rollback monitor. Pass nil on success.
func (m *Manager) RecordEval(err error) {
	if err != nil {
		m.evalFail.Add(1)
	} else {
		m.evalOK.Add(1)
	}
}

// SetRollbackPolicy tunes the rollback monitor. The production defaults are
// 30 s, 20 samples and a 1 s tick; tests shorten them.
func (m *Manager) SetRollbackPolicy(window time.Duration, minSamples int64, tick time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if window > 0 {
		m.rollbackWindow = window
	}
	if minSamples > 0 {
		m.rollbackMinSamples = minSamples
	}
	if tick > 0 {
		m.rollbackTick = tick
	}
}

// StartRollbackMonitor checks the post-swap failure rate every second. More
// than 1% failures over the first 30 seconds (minimum 20 evaluations) rolls the
// previous set back exactly once.
func (m *Manager) StartRollbackMonitor(ctx context.Context) {
	tick := m.rollbackTick
	if tick <= 0 {
		tick = time.Second
	}
	go func() {
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.maybeRollback(ctx)
			}
		}
	}()
}

func (m *Manager) maybeRollback(ctx context.Context) {
	m.mu.Lock()
	swapAt := m.swapAt
	done := m.rollbackDone
	prev := m.previous
	m.mu.Unlock()
	window := m.rollbackWindow
	if window <= 0 {
		window = 30 * time.Second
	}
	minSamples := m.rollbackMinSamples
	if minSamples <= 0 {
		minSamples = 20
	}
	if done || prev == nil || prev.Empty || time.Since(swapAt) < window {
		return
	}
	ok, fail := m.evalOK.Load(), m.evalFail.Load()
	total := ok + fail
	if total < minSamples || fail*100 <= total {
		return
	}
	m.mu.Lock()
	if m.rollbackDone {
		m.mu.Unlock()
		return
	}
	m.rollbackDone = true
	from := m.Current().Version
	m.current.Store(prev)
	m.source.Store("file")
	m.loadedAt = time.Now()
	m.evalOK.Store(0)
	m.evalFail.Store(0)
	m.mu.Unlock()
	if m.hooks.OnRollback != nil {
		m.hooks.OnRollback(from, prev.Version)
	}
	m.logger.Error("rules auto-rolled back", "from", from, "to", prev.Version,
		"fail", fail, "total", total)
	if m.persist != nil {
		_ = m.persist.SaveActivation(ctx, prev, "auto-rollback")
	}
}

// RollbackNow reactivates the previous set (manual/admin).
func (m *Manager) RollbackNow(ctx context.Context, actor string) (*RuleSet, error) {
	m.mu.Lock()
	prev := m.previous
	m.mu.Unlock()
	if prev == nil {
		return nil, errors.New("no previous rule set to roll back to")
	}
	m.mu.Lock()
	from := m.Current().Version
	m.current.Store(prev)
	m.source.Store("file")
	m.rollbackDone = true
	m.loadedAt = time.Now()
	m.evalOK.Store(0)
	m.evalFail.Store(0)
	m.mu.Unlock()
	if m.hooks.OnRollback != nil {
		m.hooks.OnRollback(from, prev.Version)
	}
	if m.persist != nil {
		_ = m.persist.SaveActivation(ctx, prev, actor)
	}
	return prev, nil
}

// Versions returns known snapshots newest first, merging memory and Postgres.
func (m *Manager) Versions(ctx context.Context) []Snapshot {
	m.mu.Lock()
	hist := append([]*RuleSet(nil), m.history...)
	m.mu.Unlock()
	out := []Snapshot{m.Snapshot()}
	for i := len(hist) - 1; i >= 0; i-- {
		out = append(out, hist[i].Snapshot(false, "file"))
	}
	if m.persist != nil {
		if stored, err := m.persist.Versions(ctx); err == nil {
			seen := map[[2]any]bool{}
			for _, s := range out {
				seen[[2]any{s.RuleSet, s.Version}] = true
			}
			for _, s := range stored {
				if !seen[[2]any{s.RuleSet, s.Version}] {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

// Watch debounces fsnotify events on the rules directory and reloads. The
// debounce is 300 ms.
func (m *Manager) Watch(ctx context.Context) error {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("fsnotify: %w", err)
	}
	if err := w.Add(m.dir); err != nil {
		_ = w.Close()
		return fmt.Errorf("watch %s: %w", m.dir, err)
	}
	go func() {
		defer func() { _ = w.Close() }()
		var timer *time.Timer
		var timerC <-chan time.Time
		reset := func() {
			if timer != nil {
				timer.Stop()
			}
			timer = time.NewTimer(300 * time.Millisecond)
			timerC = timer.C
		}
		for {
			select {
			case <-ctx.Done():
				if timer != nil {
					timer.Stop()
				}
				return
			case ev, ok := <-w.Events:
				if !ok {
					return
				}
				if !strings.HasSuffix(ev.Name, ".json") {
					continue
				}
				if ev.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) != 0 {
					reset()
				}
			case <-timerC:
				timerC = nil
				if _, err := m.Reload(ctx, "fsnotify"); err != nil {
					continue // previous set stays active
				}
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				m.logger.Error("rules watcher error", "error", err)
			}
		}
	}()
	return nil
}
