// Command fade is the Phase 5 operator tool: decision explanation,
// deterministic reproduction, audit verification and retention maintenance.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/audit"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/combiner"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/explain"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/pipeline"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/retention"
	"github.com/Kayinamura-Karimba-Geofrey/FADE/internal/rules"
)

// version is the Phase 5 tool version.
const version = "0.6.0"

const usage = `fade - FADE explainability and audit tools

Usage: fade <command> [flags]

Commands:
  explain     render the explanation for a decision
  reproduce   re-run a decision from its stored facts and compare
  audit       verify or checkpoint the audit hash chain
  retention   plan or apply partition retention
  version     print the version

Run "fade <command> -h" for command flags.
`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "fade:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stdout, usage)
		return errors.New("a command is required")
	}
	switch args[0] {
	case "explain":
		return cmdExplain(args[1:])
	case "reproduce":
		return cmdReproduce(args[1:])
	case "audit":
		return cmdAudit(args[1:])
	case "retention":
		return cmdRetention(args[1:])
	case "version", "-version", "--version":
		fmt.Fprintln(os.Stdout, version)
		return nil
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return nil
	default:
		fmt.Fprint(os.Stdout, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// ----------------------------------------------------------------- explain

func cmdExplain(args []string) error {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	file := fs.String("decision", "", "decision JSON file (default stdin)")
	id := fs.String("id", "", "decision id to load from Postgres (requires -dsn)")
	dsn := fs.String("dsn", "", "Postgres DSN for -id")
	aud := fs.String("audience", "analyst", "analyst, customer or regulator")
	lang := fs.String("lang", "en", "language code")
	if err := fs.Parse(args); err != nil {
		return err
	}
	audience, err := explain.ParseAudience(*aud)
	if err != nil {
		return err
	}
	d, err := loadDecision(*file, *id, *dsn)
	if err != nil {
		return err
	}
	view, err := explain.NewBuilder().Build(d, explain.Options{
		Audience: audience, Language: explain.NormalizeLanguage(*lang),
	})
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, view)
}

func loadDecision(file, id, dsn string) (*pipeline.Decision, error) {
	if id != "" {
		if dsn == "" {
			return nil, errors.New("-id requires -dsn")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("connect postgres: %w", err)
		}
		defer pool.Close()
		rec, err := pipeline.NewPostgresStore(pool).GetDecision(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("load decision %s: %w", id, err)
		}
		return rec.Decision, nil
	}
	var raw []byte
	var err error
	if file == "" || file == "-" {
		raw, err = io.ReadAll(io.LimitReader(os.Stdin, 8<<20))
	} else {
		raw, err = os.ReadFile(file)
	}
	if err != nil {
		return nil, err
	}
	var d pipeline.Decision
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, fmt.Errorf("decode decision: %w", err)
	}
	if d.DecisionID == "" && d.EventID == "" {
		return nil, errors.New("input does not look like a decision document")
	}
	return &d, nil
}

// --------------------------------------------------------------- reproduce

// fixture is the offline reproduction input: a decision plus the event facts
// that are not part of the public decision contract.
type fixture struct {
	Decision      pipeline.Decision `json:"decision"`
	Amount        int64             `json:"amount"`
	Fee           int64             `json:"fee"`
	TxnType       string            `json:"txn_type"`
	Channel       string            `json:"channel"`
	KYCTier       int64             `json:"kyc_tier"`
	BalanceBefore int64             `json:"balance_before"`
}

// reproduceResult is the tool output.
type reproduceResult struct {
	DecisionID  string   `json:"decision_id"`
	RuleSet     string   `json:"rule_set"`
	RuleSetHash string   `json:"rule_set_hash,omitempty"`
	OK          bool     `json:"ok"`
	Mismatches  []string `json:"mismatches,omitempty"`
	Stored      string   `json:"stored"`
	Reproduced  string   `json:"reproduced"`
	EngineScore int      `json:"engine_rule_score"`
	RefScore    int      `json:"reference_rule_score"`
	ExplainHash string   `json:"explanation_hash"`
}

func cmdReproduce(args []string) error {
	fs := flag.NewFlagSet("reproduce", flag.ContinueOnError)
	file := fs.String("fixture", "", "fixture JSON file (default stdin)")
	rulesFile := fs.String("rules", "", "rule set JSON file (required)")
	id := fs.String("id", "", "decision id to load from Postgres")
	dsn := fs.String("dsn", "", "Postgres DSN for -id")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *rulesFile == "" {
		return errors.New("-rules is required")
	}
	content, err := os.ReadFile(*rulesFile)
	if err != nil {
		return err
	}
	rs, err := rules.Load(content)
	if err != nil {
		return fmt.Errorf("load rule set: %w", err)
	}
	fx, err := loadFixture(*file, *id, *dsn)
	if err != nil {
		return err
	}
	res, err := reproduce(rs, fx)
	if err != nil {
		return err
	}
	if err := writeJSON(os.Stdout, res); err != nil {
		return err
	}
	if !res.OK {
		return fmt.Errorf("reproduction mismatch: %s", strings.Join(res.Mismatches, "; "))
	}
	return nil
}

func loadFixture(file, id, dsn string) (fixture, error) {
	if id != "" {
		if dsn == "" {
			return fixture{}, errors.New("-id requires -dsn")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return fixture{}, fmt.Errorf("connect postgres: %w", err)
		}
		defer pool.Close()
		rec, err := pipeline.NewPostgresStore(pool).GetDecision(ctx, id)
		if err != nil {
			return fixture{}, fmt.Errorf("load decision %s: %w", id, err)
		}
		return fixture{
			Decision: *rec.Decision, Amount: rec.Amount, TxnType: rec.TxnType,
			KYCTier: kycTierOf(rec.Decision),
		}, nil
	}
	raw, err := readInput(file)
	if err != nil {
		return fixture{}, err
	}
	var fx fixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		return fixture{}, fmt.Errorf("decode fixture: %w", err)
	}
	if fx.Decision.DecisionID == "" {
		return fixture{}, errors.New("fixture is missing decision.decision_id")
	}
	if fx.KYCTier == 0 {
		fx.KYCTier = kycTierOf(&fx.Decision)
	}
	return fx, nil
}

func kycTierOf(d *pipeline.Decision) int64 {
	if d == nil || d.Features == nil {
		return 0
	}
	return d.Features.KYCTier
}

func readInput(file string) ([]byte, error) {
	if file == "" || file == "-" {
		return io.ReadAll(io.LimitReader(os.Stdin, 8<<20))
	}
	return os.ReadFile(file)
}

// reproduce re-evaluates the rules twice (compiled engine and reference
// interpreter), recombines, and compares everything to the stored decision.
func reproduce(rs *rules.RuleSet, fx fixture) (*reproduceResult, error) {
	d := fx.Decision
	if d.Features == nil {
		return nil, errors.New("decision has no feature snapshot; reproduction needs features_snapshot")
	}
	vector := *d.Features
	ctx := rules.EvalContextFor(&vector, fx.Amount, fx.Fee, fx.TxnType, fx.Channel, fx.KYCTier, fx.BalanceBefore)

	var engine rules.Result
	rs.Evaluate(&vector, ctx, &engine)
	ref, err := rules.InterpretFile(ptr(rs.File()), &ctx)
	if err != nil {
		return nil, fmt.Errorf("reference interpretation: %w", err)
	}

	out := &reproduceResult{
		DecisionID: d.DecisionID, RuleSet: rs.RuleSet, RuleSetHash: rs.Hash,
		EngineScore: engine.RuleScore, RefScore: ref.RuleScore,
	}
	if engine.RuleScore != ref.RuleScore || engine.RuleAction != ref.RuleAction {
		out.Mismatches = append(out.Mismatches, fmt.Sprintf(
			"engine and reference disagree: score %d/%d action %s/%s",
			engine.RuleScore, ref.RuleScore, engine.RuleAction, ref.RuleAction))
	}
	if !sameIDs(engine.FiredIDsSorted(), ref.FiredIDsSorted()) {
		out.Mismatches = append(out.Mismatches, fmt.Sprintf(
			"fired rules differ: engine %v reference %v", engine.FiredIDsSorted(), ref.FiredIDsSorted()))
	}

	ml := rules.MLResult{Status: d.MLStatus, MLScore: d.MLScore}
	risk, decision := combiner.CombineCore(engine.RuleScore, engine.RuleAction, ml,
		combiner.Overrides{}, combiner.FromRuleSet(rs))
	out.Reproduced = fmt.Sprintf("%s/%d", decision, risk)
	out.Stored = fmt.Sprintf("%s/%d", d.Decision, d.RiskScore)
	if decision != d.Decision || risk != d.RiskScore {
		out.Mismatches = append(out.Mismatches, fmt.Sprintf(
			"decision differs: stored %s reproduced %s", out.Stored, out.Reproduced))
	}
	if d.RuleScore != engine.RuleScore {
		out.Mismatches = append(out.Mismatches, fmt.Sprintf(
			"rule score differs: stored %d reproduced %d", d.RuleScore, engine.RuleScore))
	}
	if d.RuleSetVersion != 0 && rs.Version != d.RuleSetVersion {
		out.Mismatches = append(out.Mismatches, fmt.Sprintf(
			"rule set version differs: stored %d supplied %d", d.RuleSetVersion, rs.Version))
	}
	hash, err := explain.DecisionHash(&d)
	if err != nil {
		return nil, err
	}
	out.ExplainHash = hash
	out.OK = len(out.Mismatches) == 0
	return out, nil
}

func sameIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ptr[T any](v T) *T { return &v }

// ------------------------------------------------------------------- audit

func cmdAudit(args []string) error {
	if len(args) == 0 {
		return errors.New("audit subcommand required: verify or checkpoint")
	}
	switch args[0] {
	case "verify":
		fs := flag.NewFlagSet("audit verify", flag.ContinueOnError)
		dsn := fs.String("dsn", "", "Postgres DSN")
		from := fs.Int64("from", 0, "first sequence to check (0 = newest checkpoint)")
		to := fs.Int64("to", 0, "last sequence to check (0 = chain head)")
		dir := fs.String("dir", "audit/checkpoints", "checkpoint export directory")
		key := fs.String("key", os.Getenv("AUDIT_HMAC_KEY"), "checkpoint HMAC key (default AUDIT_HMAC_KEY)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dsn == "" {
			return errors.New("-dsn is required")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		pool, err := pgxpool.New(ctx, *dsn)
		if err != nil {
			return fmt.Errorf("connect postgres: %w", err)
		}
		defer pool.Close()
		store := audit.NewPostgresStore(pool)
		res, err := audit.VerifyChain(ctx, store, audit.VerifyOptions{
			From: *from, To: *to, CheckpointDir: *dir, HMACKey: []byte(*key),
		})
		if err != nil {
			return err
		}
		if err := writeJSON(os.Stdout, res); err != nil {
			return err
		}
		if !res.OK {
			return fmt.Errorf("audit chain broken at seq %d: %+v", res.FirstBreak.Seq, res.FirstBreak)
		}
		return nil
	case "checkpoint":
		fs := flag.NewFlagSet("audit checkpoint", flag.ContinueOnError)
		dsn := fs.String("dsn", "", "Postgres DSN")
		dir := fs.String("dir", "audit/checkpoints", "checkpoint export directory")
		key := fs.String("key", os.Getenv("AUDIT_HMAC_KEY"), "checkpoint HMAC key (default AUDIT_HMAC_KEY)")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *dsn == "" {
			return errors.New("-dsn is required")
		}
		if *key == "" {
			return errors.New("AUDIT_HMAC_KEY or -key is required; checkpoints are never written unsigned")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		pool, err := pgxpool.New(ctx, *dsn)
		if err != nil {
			return fmt.Errorf("connect postgres: %w", err)
		}
		defer pool.Close()
		cp, err := audit.NewCheckpointService(audit.NewPostgresStore(pool), []byte(*key), *dir, audit.Hooks{}).Run(ctx)
		if err != nil {
			return err
		}
		return writeJSON(os.Stdout, cp)
	default:
		return fmt.Errorf("unknown audit subcommand %q", args[0])
	}
}

// --------------------------------------------------------------- retention

type pgExecer struct{ pool *pgxpool.Pool }

func (p pgExecer) Exec(ctx context.Context, sql string) (int64, error) {
	tag, err := p.pool.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func cmdRetention(args []string) error {
	if len(args) == 0 {
		return errors.New("retention subcommand required: plan or apply")
	}
	plan := args[0] == "plan"
	if !plan && args[0] != "apply" {
		return fmt.Errorf("unknown retention subcommand %q", args[0])
	}
	fs := flag.NewFlagSet("retention", flag.ContinueOnError)
	dsn := fs.String("dsn", "", "Postgres DSN (required for apply)")
	table := fs.String("table", "decisions", "partitioned table")
	days := fs.Int("retain-days", 90, "retention window in days")
	ahead := fs.Int("months-ahead", 3, "future partitions to keep")
	nowFlag := fs.String("now", "", "RFC3339 clock override for reproducible plans")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	now := time.Now().UTC()
	if *nowFlag != "" {
		parsed, err := time.Parse(time.RFC3339, *nowFlag)
		if err != nil {
			return fmt.Errorf("-now must be RFC3339: %w", err)
		}
		now = parsed.UTC()
	}
	policy := retention.Policy{Table: *table, RetainDays: *days, MonthsAhead: *ahead}
	actions, err := retention.Plan(policy, now)
	if err != nil {
		return err
	}
	if plan {
		return writeJSON(os.Stdout, map[string]any{"policy": policy, "actions": actions})
	}
	if *dsn == "" {
		return errors.New("-dsn is required for apply")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	done, err := retention.Apply(ctx, pgExecer{pool: pool}, actions)
	if err != nil {
		return err
	}
	return writeJSON(os.Stdout, map[string]any{"applied": done, "total": len(actions)})
}

// ------------------------------------------------------------------ output

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
