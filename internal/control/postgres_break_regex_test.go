package control_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

// The SQLite schema expressed its character-class rules with GLOB and LIKE;
// the PostgreSQL port rewrote every one as a regular expression, a LIKE or a
// strpos. These tests replay a few thousand generated inputs per predicate
// through the shipped PostgreSQL schema and compare each verdict with the one
// SQLite returned for the same input on the final SQLite schema.
//
// SQLite is leaving the build, so its verdicts are frozen in
// sqlite_verdicts.tsv. Regenerate them (only needed when the cases or the
// corpus generator change) with the procedure in the testdata README.
const (
	breakRegexCasesPath    = "testdata/postgres_break_regex/cases.json"
	breakRegexVerdictsPath = "testdata/postgres_break_regex/sqlite_verdicts.tsv"
	breakRegexDumpEnv      = "OPEN_SPLUNK_BREAK_REGEX_DUMP"
	breakRegexInputs       = 2500
	breakRegexReportLimit  = 8
	// breakRegexMinimumVerdicts is how many true and how many false SQLite
	// verdicts every case's corpus must reach.
	breakRegexMinimumVerdicts = 4
)

// breakRegexCase is one predicate evaluated in both engines. Kind "atom" is a
// bare GLOB/LIKE predicate over a text column x, paired with its translation;
// "check" is a whole CHECK constraint, located in the PostgreSQL catalog by
// table and name; "trigger" is the firing condition of a trigger function.
// Target is the column the corpus fuzzes. Every other column takes its value
// from Contexts, cycling by input position.
type breakRegexCase struct {
	ID         string                       `json:"id"`
	Kind       string                       `json:"kind"`
	SQLite     string                       `json:"sqlite"`
	PG         string                       `json:"pg,omitempty"`
	Table      string                       `json:"table,omitempty"`
	Constraint string                       `json:"constraint,omitempty"`
	Function   string                       `json:"function,omitempty"`
	Columns    []string                     `json:"columns"`
	Target     string                       `json:"target"`
	Contexts   []map[string]json.RawMessage `json:"contexts,omitempty"`
	Seeds      []string                     `json:"seeds,omitempty"`
	// SQLiteConstraint and Triggers name the SQLite objects the case was
	// taken from; the constraint is empty for an unnamed SQLite CHECK.
	SQLiteConstraint string `json:"sqlite_constraint"`
	// SQLiteTable is set when a PostgreSQL-only constraint, on a table SQLite
	// never had, is judged by the identical CHECK of another SQLite table.
	SQLiteTable string   `json:"sqlite_table,omitempty"`
	Triggers    []string `json:"sqlite_triggers,omitempty"`
}

type breakRegexVerdicts struct {
	corpusSHA256 string
	verdicts     string
}

var (
	// breakRegexOperator finds a regex or LIKE operator applied to a string
	// literal, as both pg_get_constraintdef and hand-written PL/pgSQL spell it.
	breakRegexOperator = regexp.MustCompile(`(!?~~?\*?)\s*'((?:[^']|'')*)'`)
	// breakRegexBackslashStrpos finds strpos(column, '\...'), the spelling of
	// SQLite's escape-free LIKE '%\%'.
	breakRegexBackslashStrpos = regexp.MustCompile(`strpos\((?:NEW\.)?([a-z_][a-z0-9_]*),\s*'(\\+)'`)
	// breakRegexOperand is the column a regex or LIKE operator tests, possibly
	// wrapped in substr(...) or replace(...).
	breakRegexOperand = regexp.MustCompile(`([a-z_][a-z0-9_]*)(?:,[^()]*)?\)*\s*!?~~?\*?\s*'`)
	// breakRegexMarker flags any catalog text that pattern-matches strings.
	breakRegexMarker = regexp.MustCompile(
		`(?:^|[\s(])!?~~?\*?\s*'|(?i:\bI?LIKE\b|\bSIMILAR\s+TO\b|\bregexp_[a-z]+\()|strpos\([^()]*,\s*'\\`,
	)
	breakRegexCondition = regexp.MustCompile(`(?s)\bIF\s+(.*?)\s+THEN\b`)
	breakRegexNewColumn = regexp.MustCompile(`\bNEW\.([a-z_][a-z0-9_]*)`)
	breakRegexComment   = regexp.MustCompile(`--[^\n]*`)
	breakRegexLiteral   = regexp.MustCompile(`'((?:[^']|'')*)'`)
	breakRegexClass     = regexp.MustCompile(`\[\^?([^\]]+)\]`)
	breakRegexNumber    = regexp.MustCompile(`\b[0-9]+\b`)
	breakRegexCharCall  = regexp.MustCompile(`char\(([0-9]+)\)`)
)

// breakRegexSpecials are the characters most likely to separate a GLOB
// bracket expression from a regular expression: bracket and range syntax,
// anchors, escapes, line terminators, wildcards of both dialects, control
// and C1 characters, and multi-byte and astral code points.
var breakRegexSpecials = []rune{
	'-', ']', '[', '^', '$', '\\', '\n', '\r', '\t', '\v', '\f', '.', '*', '?', '%', '_', '/', ':', ' ', '~', '!',
	'|', '(', ')', '{', '}', '+', '\'', '"', '`', '@', '#', '&', '=', ',', ';', '<', '>',
	0x01, 0x08, 0x0b, 0x0c, 0x0e, 0x1f, 0x7f, 0x80, 0x85, 0x9f, 0xa0, 0xad,
	'é', 'É', 'ß', 'ſ', 'K', 'İ', 'ı', 'Ａ', '０', '٣', '\u2028', '\u2029', '\u200b', '\ufeff', '\ufffd', '\uffff',
	0x1F600, 0x10FFFF,
}

// breakRegexSpecialStrings are whole inputs that mean something to one
// dialect only: regex anchors, bracket classes and ARE directors, LIKE
// escapes and doubled backslashes, text that spells an escape sequence the
// translations use, and Unicode case and normalization traps.
var breakRegexSpecialStrings = []string{
	`\\`, `\\\\`, `a\\b`, `a\\\\b`, `\\\\\\`, `%\\%`, `\%`, `\_`, `[]`, `[^]`, `]-[`, `[a-]`, `[]]`, `[!a]`,
	`*`, `**`, `?`, `%%`, `_%`, `^`, `$`, `^$`, `.*`, `a.b`, `\u0001`, `\x01`, `\x`, `\n`, `[:alpha:]`,
	`[[:alpha:]]`, `[[.hyphen.]]`, `(?i)A`, `***:a`, `***=a`, `(?n)a`, "a\nb", "\n\n", "a\r\nb", " a",
	"ǅ", "ﬀ", "Ω", "Å", "é", "\U0001F600\U0001F600",
}

func TestPostgresGlobAndLikePortsMatchSQLiteVerdicts(t *testing.T) {
	cases := loadBreakRegexCases(t)
	corpora := make(map[string][]*string, len(cases))
	for _, item := range cases {
		corpora[item.ID] = breakRegexCorpus(t, item)
	}
	if path := os.Getenv(breakRegexDumpEnv); path != "" {
		writeBreakRegexDump(t, path, cases, corpora)
		t.Skipf("wrote the corpus to %s for the SQLite oracle; verdicts were not compared", path)
	}
	expected := loadBreakRegexVerdicts(t)

	db := testsupport.NewPostgresControlDB(t).SQLDB()
	catalog := loadBreakRegexCatalog(t, db)
	assertBreakRegexCoverage(t, cases, catalog)

	for _, item := range cases {
		corpus := corpora[item.ID]
		want, ok := expected[item.ID]
		if !ok {
			t.Errorf("%s: no SQLite verdicts in %s", item.ID, breakRegexVerdictsPath)
			continue
		}
		if got := breakRegexCorpusSHA256(corpus); got != want.corpusSHA256 {
			t.Errorf("%s: corpus SHA-256 %s, but the SQLite verdicts were recorded for %s; regenerate %s",
				item.ID, got, want.corpusSHA256, breakRegexVerdictsPath)
			continue
		}
		expression, types := breakRegexPostgresExpression(t, db, catalog, item)
		got := evaluateBreakRegexCase(t, db, item, expression, types, corpus)
		compareBreakRegexVerdicts(t, item, expression, corpus, want.verdicts, got)
	}
}

func loadBreakRegexCases(t *testing.T) []breakRegexCase {
	t.Helper()
	raw, err := os.ReadFile(breakRegexCasesPath)
	if err != nil {
		t.Fatalf("read %s: %v", breakRegexCasesPath, err)
	}
	var document struct {
		Cases []breakRegexCase `json:"cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode %s: %v", breakRegexCasesPath, err)
	}
	seen := make(map[string]bool, len(document.Cases))
	for _, item := range document.Cases {
		if seen[item.ID] {
			t.Fatalf("duplicate case %s", item.ID)
		}
		seen[item.ID] = true
		if !slices.Contains(item.Columns, item.Target) {
			t.Fatalf("%s: target %q is not one of its columns %v", item.ID, item.Target, item.Columns)
		}
		switch item.Kind {
		case "atom":
			if item.PG == "" || !slices.Equal(item.Columns, []string{"x"}) {
				t.Fatalf("%s: an atom needs a PostgreSQL translation over the single column x", item.ID)
			}
		case "check":
			if item.Table == "" || item.Constraint == "" {
				t.Fatalf("%s: a check needs its table and constraint", item.ID)
			}
		case "trigger":
			if item.Table == "" || item.Function == "" || len(item.Triggers) == 0 {
				t.Fatalf("%s: a trigger case needs its table, function and SQLite triggers", item.ID)
			}
		default:
			t.Fatalf("%s: unknown kind %q", item.ID, item.Kind)
		}
		for index, context := range item.Contexts {
			for name := range context {
				if name == item.Target || !slices.Contains(item.Columns, name) {
					t.Fatalf("%s: context %d sets %q, which is the target or not a column", item.ID, index, name)
				}
			}
		}
	}
	if len(document.Cases) == 0 {
		t.Fatalf("%s holds no cases", breakRegexCasesPath)
	}
	return document.Cases
}

func loadBreakRegexVerdicts(t *testing.T) map[string]breakRegexVerdicts {
	t.Helper()
	raw, err := os.ReadFile(breakRegexVerdictsPath)
	if err != nil {
		t.Fatalf("read %s: %v", breakRegexVerdictsPath, err)
	}
	out := make(map[string]breakRegexVerdicts)
	for number, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 3 || strings.Trim(fields[2], "tfn") != "" {
			t.Fatalf("%s:%d: want <id> TAB <corpus sha256> TAB <t|f|n verdicts>", breakRegexVerdictsPath, number+1)
		}
		out[fields[0]] = breakRegexVerdicts{corpusSHA256: fields[1], verdicts: fields[2]}
	}
	return out
}

// breakRegexCorpus deterministically generates breakRegexInputs distinct
// values for the case's target column: NULL, the empty string, every code
// point below U+0100 and every special on its own, the expression's literals
// and the case's seeds with edits, then random strings shaped by the
// expression (its bracket classes and length bounds) with a few edits drawn
// from the specials, the class boundaries and the whole Unicode range.
func breakRegexCorpus(t *testing.T, item breakRegexCase) []*string {
	t.Helper()
	seed := sha256.Sum256([]byte(item.ID))
	source := rand.NewPCG(binary.BigEndian.Uint64(seed[:8]), binary.BigEndian.Uint64(seed[8:16]))
	intn := func(n int) int { return int(source.Uint64() % uint64(n)) }

	literals, alphabet, boundaries, lengths := breakRegexShape(item.SQLite)
	interesting := slices.Concat(breakRegexSpecials, boundaries)
	if len(alphabet) == 0 {
		for r := rune(' '); r <= '~'; r++ {
			alphabet = append(alphabet, r)
		}
		alphabet = append(alphabet, 'é', 'ß', '€', 0xa0, 0x1F600)
	}
	anyRune := func() rune {
		r := rune(1 + intn(utf8.MaxRune))
		if !utf8.ValidRune(r) {
			return utf8.RuneError
		}
		return r
	}
	editRune := func() rune {
		if intn(10) < 7 {
			return interesting[intn(len(interesting))]
		}
		return anyRune()
	}
	edit := func(value []rune) []rune {
		position := 0
		if len(value) > 0 {
			position = intn(len(value) + 1)
		}
		switch op := intn(3); {
		case op == 0 && position < len(value):
			value[position] = editRune()
		case op == 1 && position < len(value):
			value = slices.Delete(value, position, position+1)
		default:
			value = slices.Insert(value, position, editRune())
		}
		return value
	}

	corpus := []*string{nil}
	seen := map[string]bool{}
	add := func(value string) {
		if !seen[value] && !strings.ContainsRune(value, 0) && utf8.ValidString(value) && len(corpus) < breakRegexInputs {
			seen[value] = true
			corpus = append(corpus, &value)
		}
	}
	add("")
	for r := rune(1); r < 0x100; r++ {
		add(string(r))
	}
	for _, r := range interesting {
		add(string(r))
	}
	for _, value := range breakRegexSpecialStrings {
		add(value)
	}
	starts := slices.Concat(literals, item.Seeds)
	for _, value := range starts {
		runes := []rune(value)
		add(value)
		add(value + "x")
		add(" " + value)
		add(value + " ")
		add(value + "\n")
		add(strings.ToUpper(value))
		if len(runes) > 0 {
			add(string(runes[:len(runes)-1]))
		}
	}
	for attempts := 0; len(corpus) < breakRegexInputs; attempts++ {
		if attempts > 100*breakRegexInputs {
			t.Fatalf("%s: corpus generation stalled at %d inputs", item.ID, len(corpus))
		}
		var value []rune
		edits := []int{0, 1, 1, 1, 2, 3}[intn(6)]
		mode := intn(100)
		if len(starts) == 0 && mode >= 35 && mode < 75 {
			mode = 0
		}
		switch {
		case mode < 35:
			// Class characters at one of the expression's length bounds,
			// sometimes behind a literal or seed prefix.
			length := intn(13)
			if len(lengths) > 0 && intn(2) == 0 {
				length = max(lengths[intn(len(lengths))]+intn(3)-1, 0)
			}
			value = make([]rune, length)
			for index := range value {
				value[index] = alphabet[intn(len(alphabet))]
			}
			if len(starts) > 0 && intn(4) == 0 {
				copy(value, []rune(starts[intn(len(starts))]))
			}
		case mode < 55:
			// A literal or seed with hostile edits.
			value = []rune(starts[intn(len(starts))])
			edits = 1 + intn(2)
		case mode < 75:
			// A literal or seed with in-class substitutions, which keeps
			// structured values such as UUIDs and app ids mostly valid.
			value = []rune(starts[intn(len(starts))])
			for range 1 + intn(3) {
				if len(value) > 0 {
					value[intn(len(value))] = alphabet[intn(len(alphabet))]
				}
			}
			edits = []int{0, 0, 0, 1}[intn(4)]
		default:
			// Regex and GLOB syntax fragments mixed with class characters.
			for range intn(6) {
				switch intn(3) {
				case 0:
					value = append(value, []rune(breakRegexSpecialStrings[intn(len(breakRegexSpecialStrings))])...)
				case 1:
					value = append(value, alphabet[intn(len(alphabet))])
				default:
					value = append(value, editRune())
				}
			}
			edits = 0
		}
		for range edits {
			value = edit(value)
		}
		add(string(value))
	}
	return corpus
}

// breakRegexShape reads the SQLite expression for the plain string literals,
// the characters of its bracket classes, the code points just outside each
// class range or char() argument, and the small integer bounds it compares
// lengths against.
func breakRegexShape(expression string) (literals []string, alphabet, boundaries []rune, lengths []int) {
	members := map[rune]bool{}
	edges := map[rune]bool{}
	for _, match := range breakRegexLiteral.FindAllStringSubmatch(expression, -1) {
		literal := strings.ReplaceAll(match[1], "''", "'")
		if !strings.ContainsAny(literal, "*[%") {
			literals = append(literals, literal)
			continue
		}
		for _, class := range breakRegexClass.FindAllStringSubmatch(literal, -1) {
			runes := []rune(class[1])
			for index := 0; index < len(runes); index++ {
				low, high := runes[index], runes[index]
				if index+2 < len(runes) && runes[index+1] == '-' {
					high = runes[index+2]
					index += 2
				}
				for r := low; r <= high; r++ {
					members[r] = true
				}
				edges[low-1], edges[high+1] = true, true
			}
		}
	}
	for _, match := range breakRegexCharCall.FindAllStringSubmatch(expression, -1) {
		code, _ := strconv.Atoi(match[1])
		for _, r := range []rune{rune(code) - 1, rune(code), rune(code) + 1} {
			edges[r] = true
		}
	}
	withoutCalls := breakRegexCharCall.ReplaceAllString(expression, "")
	seenLength := map[int]bool{}
	for _, number := range breakRegexNumber.FindAllString(withoutCalls, -1) {
		value, _ := strconv.Atoi(number)
		if value >= 1 && value <= 400 && !seenLength[value] {
			seenLength[value] = true
			lengths = append(lengths, value)
		}
	}
	for r := range members {
		alphabet = append(alphabet, r)
	}
	for r := range edges {
		if r > 0 && utf8.ValidRune(r) {
			boundaries = append(boundaries, r)
		}
	}
	slices.Sort(alphabet)
	slices.Sort(boundaries)
	return literals, alphabet, boundaries, lengths
}

func breakRegexCorpusSHA256(corpus []*string) string {
	hasher := sha256.New()
	for _, value := range corpus {
		if value == nil {
			_, _ = hasher.Write([]byte{0})
			continue
		}
		_, _ = hasher.Write([]byte{1})
		_, _ = hasher.Write(binary.AppendUvarint(nil, uint64(len(*value))))
		_, _ = hasher.Write([]byte(*value))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func writeBreakRegexDump(t *testing.T, path string, cases []breakRegexCase, corpora map[string][]*string) {
	t.Helper()
	type dumped struct {
		ID           string    `json:"id"`
		CorpusSHA256 string    `json:"corpus_sha256"`
		Inputs       []*string `json:"inputs"`
	}
	out := make([]dumped, 0, len(cases))
	for _, item := range cases {
		corpus := corpora[item.ID]
		out = append(out, dumped{ID: item.ID, CorpusSHA256: breakRegexCorpusSHA256(corpus), Inputs: corpus})
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("encode corpus dump: %v", err)
	}
	if err := os.WriteFile(filepath.Clean(path), raw, 0o600); err != nil {
		t.Fatalf("write corpus dump: %v", err)
	}
}

// breakRegexCatalog is the pattern-matching surface of the migrated schema.
type breakRegexCatalog struct {
	checks    map[[2]string]breakRegexCheck // (table, constraint)
	functions map[string]string             // name -> body without comments
	// others holds every other expression the schema evaluates: index
	// predicates, column defaults and generation expressions, and trigger
	// WHEN clauses.
	others map[string]string
}

type breakRegexCheck struct {
	definition string
	columns    []string
}

func loadBreakRegexCatalog(t *testing.T, db *sql.DB) breakRegexCatalog {
	t.Helper()
	ctx := t.Context()
	catalog := breakRegexCatalog{
		checks:    map[[2]string]breakRegexCheck{},
		functions: map[string]string{},
		others:    map[string]string{},
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.relname, con.conname, pg_get_constraintdef(con.oid),
		       coalesce((SELECT string_agg(a.attname, ',' ORDER BY a.attname)
		                 FROM unnest(con.conkey) AS k(attnum)
		                 JOIN pg_attribute AS a ON a.attrelid = con.conrelid AND a.attnum = k.attnum), '')
		FROM pg_constraint AS con
		JOIN pg_class AS c ON c.oid = con.conrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND con.contype = 'c'`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list CHECK constraints: %v", err)
	}
	for rows.Next() {
		var table, name, definition, columns string
		if err := rows.Scan(&table, &name, &definition, &columns); err != nil {
			t.Fatalf("scan CHECK constraint: %v", err)
		}
		catalog.checks[[2]string{table, name}] = breakRegexCheck{definition: definition, columns: strings.Split(columns, ",")}
	}
	closeBreakRegexRows(t, rows)

	rows, err = db.QueryContext(ctx, `
		SELECT p.proname, p.prosrc
		FROM pg_proc AS p
		JOIN pg_namespace AS n ON n.oid = p.pronamespace
		WHERE n.nspname = $1`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list functions: %v", err)
	}
	for rows.Next() {
		var name, body string
		if err := rows.Scan(&name, &body); err != nil {
			t.Fatalf("scan function: %v", err)
		}
		catalog.functions[name] = breakRegexComment.ReplaceAllString(body, "")
	}
	closeBreakRegexRows(t, rows)

	rows, err = db.QueryContext(ctx, `
		SELECT 'index ' || i.indexrelid::regclass::text, pg_get_expr(i.indpred, i.indrelid)
		FROM pg_index AS i
		JOIN pg_class AS c ON c.oid = i.indrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND i.indpred IS NOT NULL
		UNION ALL
		SELECT 'index expression ' || i.indexrelid::regclass::text, pg_get_expr(i.indexprs, i.indrelid)
		FROM pg_index AS i
		JOIN pg_class AS c ON c.oid = i.indrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND i.indexprs IS NOT NULL
		UNION ALL
		SELECT 'default ' || c.relname || '.' || a.attname, pg_get_expr(d.adbin, d.adrelid)
		FROM pg_attrdef AS d
		JOIN pg_class AS c ON c.oid = d.adrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE n.nspname = $1
		UNION ALL
		SELECT 'trigger ' || t.tgname, pg_get_triggerdef(t.oid)
		FROM pg_trigger AS t
		JOIN pg_class AS c ON c.oid = t.tgrelid
		JOIN pg_namespace AS n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND NOT t.tgisinternal`, control.PostgresSchema)
	if err != nil {
		t.Fatalf("list other schema expressions: %v", err)
	}
	for rows.Next() {
		var name, expression string
		if err := rows.Scan(&name, &expression); err != nil {
			t.Fatalf("scan schema expression: %v", err)
		}
		catalog.others[name] = expression
	}
	closeBreakRegexRows(t, rows)
	return catalog
}

func closeBreakRegexRows(t *testing.T, rows *sql.Rows) {
	t.Helper()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close rows: %v", err)
	}
}

// breakRegexAtomKeys names each pattern a text applies: the operator and its
// literal, or the backslash run of an escape-free LIKE spelled as strpos.
func breakRegexAtomKeys(text string) []string {
	var keys []string
	for _, match := range breakRegexOperator.FindAllStringSubmatch(text, -1) {
		keys = append(keys, match[1]+" "+match[2])
	}
	for _, match := range breakRegexBackslashStrpos.FindAllStringSubmatch(text, -1) {
		keys = append(keys, "strpos "+match[2])
	}
	return keys
}

// breakRegexOperandColumns names the columns the text pattern-matches.
func breakRegexOperandColumns(text string) []string {
	columns := map[string]bool{}
	for _, match := range breakRegexOperand.FindAllStringSubmatch(text, -1) {
		columns[match[1]] = true
	}
	for _, match := range breakRegexBackslashStrpos.FindAllStringSubmatch(text, -1) {
		columns[match[1]] = true
	}
	return slices.Sorted(maps.Keys(columns))
}

// assertBreakRegexCoverage fails unless the cases cover the schema's whole
// pattern-matching surface: every CHECK and function that pattern-matches,
// every column such an object pattern-matches, and every distinct pattern
// literal. A new regex or LIKE in a later migration therefore needs a case
// and SQLite-side reasoning before this test passes again.
func assertBreakRegexCoverage(t *testing.T, cases []breakRegexCase, catalog breakRegexCatalog) {
	t.Helper()
	targets := map[string][]string{}
	atomKeys := map[string]bool{}
	for _, item := range cases {
		switch item.Kind {
		case "atom":
			keys := breakRegexAtomKeys(item.PG)
			if len(keys) != 1 {
				t.Errorf("%s: translation %q names %d patterns, want 1", item.ID, item.PG, len(keys))
			}
			for _, key := range keys {
				atomKeys[key] = true
			}
		case "check":
			key := "check " + item.Table + "." + item.Constraint
			targets[key] = append(targets[key], item.Target)
		case "trigger":
			key := "function " + item.Function
			targets[key] = append(targets[key], item.Target)
		}
	}

	catalogKeys := map[string]bool{}
	objects := map[string][]string{}
	for key, check := range catalog.checks {
		if breakRegexMarker.MatchString(check.definition) {
			objects["check "+key[0]+"."+key[1]] = breakRegexOperandColumns(check.definition)
			for _, atom := range breakRegexAtomKeys(check.definition) {
				catalogKeys[atom] = true
			}
		}
	}
	for name, body := range catalog.functions {
		if breakRegexMarker.MatchString(body) {
			objects["function "+name] = breakRegexOperandColumns(body)
			for _, atom := range breakRegexAtomKeys(body) {
				catalogKeys[atom] = true
			}
		}
	}
	for name, expression := range catalog.others {
		if breakRegexMarker.MatchString(expression) {
			t.Errorf("%s pattern-matches strings but no case covers it: %s", name, expression)
		}
	}
	for object, columns := range objects {
		covered := slices.Sorted(slices.Values(targets[object]))
		if !slices.Equal(covered, columns) {
			t.Errorf("%s pattern-matches columns %v, but the cases fuzz %v", object, columns, covered)
		}
	}
	for object := range targets {
		if _, ok := objects[object]; !ok {
			t.Errorf("case object %s no longer pattern-matches strings in the schema", object)
		}
	}
	for key := range catalogKeys {
		if !atomKeys[key] {
			t.Errorf("schema pattern %q has no atom case", key)
		}
	}
	for key := range atomKeys {
		if !catalogKeys[key] {
			t.Errorf("atom pattern %q no longer appears in the schema", key)
		}
	}
	if len(objects) == 0 || len(catalogKeys) == 0 {
		t.Fatal("found no pattern-matching objects in the schema; the catalog scan is broken")
	}
}

// breakRegexPostgresExpression returns the case's PostgreSQL predicate over
// the columns of a row named new_row, and each column's type and collation.
func breakRegexPostgresExpression(
	t *testing.T,
	db *sql.DB,
	catalog breakRegexCatalog,
	item breakRegexCase,
) (string, map[string]string) {
	t.Helper()
	switch item.Kind {
	case "atom":
		return item.PG, map[string]string{"x": `text COLLATE "C"`}
	case "check":
		check, ok := catalog.checks[[2]string{item.Table, item.Constraint}]
		if !ok {
			t.Fatalf("%s: CHECK %s.%s is missing", item.ID, item.Table, item.Constraint)
		}
		if columns := slices.Sorted(slices.Values(item.Columns)); !slices.Equal(columns, check.columns) {
			t.Fatalf("%s: the CHECK reads %v, the case binds %v", item.ID, check.columns, columns)
		}
		body, ok := strings.CutPrefix(check.definition, "CHECK (")
		if !ok || !strings.HasSuffix(body, ")") {
			t.Fatalf("%s: unexpected constraint definition %q", item.ID, check.definition)
		}
		return strings.TrimSuffix(body, ")"), breakRegexColumnTypes(t, db, item)
	default:
		body, ok := catalog.functions[item.Function]
		if !ok {
			t.Fatalf("%s: function %s is missing", item.ID, item.Function)
		}
		assertBreakRegexTriggers(t, db, item)
		match := breakRegexCondition.FindStringSubmatch(body)
		if match == nil {
			t.Fatalf("%s: function %s has no IF ... THEN condition", item.ID, item.Function)
		}
		read := map[string]bool{}
		for _, column := range breakRegexNewColumn.FindAllStringSubmatch(match[1], -1) {
			read[column[1]] = true
		}
		columns := slices.Sorted(maps.Keys(read))
		if want := slices.Sorted(slices.Values(item.Columns)); !slices.Equal(columns, want) {
			t.Fatalf("%s: the condition reads NEW columns %v, the case binds %v", item.ID, columns, want)
		}
		return breakRegexNewColumn.ReplaceAllString(match[1], "new_row.$1"), breakRegexColumnTypes(t, db, item)
	}
}

// assertBreakRegexTriggers checks that the function fires for exactly the
// SQLite triggers the case was ported from, on the same table.
func assertBreakRegexTriggers(t *testing.T, db *sql.DB, item breakRegexCase) {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `
		SELECT t.tgname, c.relname
		FROM pg_trigger AS t
		JOIN pg_class AS c ON c.oid = t.tgrelid
		JOIN pg_proc AS p ON p.oid = t.tgfoid
		JOIN pg_namespace AS n ON n.oid = p.pronamespace
		WHERE n.nspname = $1 AND p.proname = $2 AND NOT t.tgisinternal`, control.PostgresSchema, item.Function)
	if err != nil {
		t.Fatalf("%s: list triggers: %v", item.ID, err)
	}
	var names []string
	for rows.Next() {
		var name, table string
		if err := rows.Scan(&name, &table); err != nil {
			t.Fatalf("%s: scan trigger: %v", item.ID, err)
		}
		if table != item.Table {
			t.Fatalf("%s: trigger %s fires on %s, want %s", item.ID, name, table, item.Table)
		}
		names = append(names, name)
	}
	closeBreakRegexRows(t, rows)
	if want := slices.Sorted(slices.Values(item.Triggers)); !slices.Equal(slices.Sorted(slices.Values(names)), want) {
		t.Fatalf("%s: function %s backs triggers %v, want the SQLite triggers %v", item.ID, item.Function, names, want)
	}
}

func breakRegexColumnTypes(t *testing.T, db *sql.DB, item breakRegexCase) map[string]string {
	t.Helper()
	types := map[string]string{}
	for _, column := range item.Columns {
		var typeName, collation string
		err := db.QueryRowContext(t.Context(), `
			SELECT format_type(a.atttypid, a.atttypmod), coalesce(co.collname, '')
			FROM pg_attribute AS a
			LEFT JOIN pg_collation AS co ON co.oid = a.attcollation
			WHERE a.attrelid = to_regclass($1) AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped`,
			control.PostgresSchema+"."+item.Table, column).Scan(&typeName, &collation)
		if err != nil {
			t.Fatalf("%s: type of %s.%s: %v", item.ID, item.Table, column, err)
		}
		if collation != "" {
			typeName += " COLLATE " + pgx.Identifier{collation}.Sanitize()
		}
		types[column] = typeName
	}
	return types
}

// evaluateBreakRegexCase evaluates the predicate over every input in one
// statement and returns the t/f/n verdict string in corpus order.
func evaluateBreakRegexCase(
	t *testing.T,
	db *sql.DB,
	item breakRegexCase,
	expression string,
	types map[string]string,
	corpus []*string,
) string {
	t.Helper()
	arguments := make([]any, len(item.Columns))
	parameters := make([]string, len(item.Columns))
	projections := make([]string, len(item.Columns))
	for index, column := range item.Columns {
		values := make([]*string, len(corpus))
		for row, input := range corpus {
			if column == item.Target {
				values[row] = input
				continue
			}
			values[row] = breakRegexContextValue(t, item, row, column)
		}
		arguments[index] = values
		parameters[index] = fmt.Sprintf("$%d::text[]", index+1)
		cast, collation, _ := strings.Cut(types[column], " COLLATE ")
		projection := fmt.Sprintf("CAST(input.c%d AS %s)", index, cast)
		if collation != "" {
			projection += " COLLATE " + collation
		}
		projections[index] = projection + " AS " + pgx.Identifier{column}.Sanitize()
	}
	aliases := make([]string, len(item.Columns))
	for index := range aliases {
		aliases[index] = fmt.Sprintf("c%d", index)
	}
	// The predicate is schema text, so it is spliced into the statement; the
	// inputs travel as array parameters.
	var query strings.Builder
	query.WriteString("SELECT new_row.breakregex_ordinal, (")
	query.WriteString(expression)
	query.WriteString(")\nFROM (SELECT input.breakregex_ordinal, ")
	query.WriteString(strings.Join(projections, ", "))
	query.WriteString("\n  FROM unnest(")
	query.WriteString(strings.Join(parameters, ", "))
	query.WriteString(") WITH ORDINALITY AS input(")
	query.WriteString(strings.Join(aliases, ", "))
	query.WriteString(", breakregex_ordinal)\n) AS new_row\nORDER BY new_row.breakregex_ordinal")
	rows, err := db.QueryContext(t.Context(), query.String(), arguments...)
	if err != nil {
		t.Fatalf("%s: evaluate in PostgreSQL: %v\n%s", item.ID, err, query.String())
	}
	verdicts := make([]byte, 0, len(corpus))
	for rows.Next() {
		var ordinal int64
		var verdict sql.NullBool
		if err := rows.Scan(&ordinal, &verdict); err != nil {
			t.Fatalf("%s: scan verdict: %v", item.ID, err)
		}
		if ordinal != int64(len(verdicts)+1) {
			t.Fatalf("%s: verdict for input %d arrived at position %d", item.ID, ordinal, len(verdicts)+1)
		}
		verdicts = append(verdicts, breakRegexVerdictByte(verdict))
	}
	closeBreakRegexRows(t, rows)
	if len(verdicts) != len(corpus) {
		t.Fatalf("%s: %d verdicts for %d inputs", item.ID, len(verdicts), len(corpus))
	}
	return string(verdicts)
}

func breakRegexVerdictByte(verdict sql.NullBool) byte {
	switch {
	case !verdict.Valid:
		return 'n'
	case verdict.Bool:
		return 't'
	default:
		return 'f'
	}
}

// breakRegexContextValue renders a context value as the text PostgreSQL
// casts to the column type: a JSON string as itself, an integer in decimal,
// and {"hex": "..."} as bytea hex input.
func breakRegexContextValue(t *testing.T, item breakRegexCase, row int, column string) *string {
	t.Helper()
	if len(item.Contexts) == 0 {
		t.Fatalf("%s: column %s needs a context value", item.ID, column)
	}
	raw, ok := item.Contexts[row%len(item.Contexts)][column]
	if !ok {
		t.Fatalf("%s: context %d does not set %s", item.ID, row%len(item.Contexts), column)
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("%s: decode context %s: %v", item.ID, column, err)
	}
	var text string
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		text = typed
	case float64:
		text = strconv.FormatInt(int64(typed), 10)
	case map[string]any:
		blob, ok := typed["hex"].(string)
		if !ok || len(typed) != 1 {
			t.Fatalf("%s: context %s must be {\"hex\": \"...\"}", item.ID, column)
		}
		text = `\x` + blob
	default:
		t.Fatalf("%s: unsupported context value %s for %s", item.ID, raw, column)
	}
	return &text
}

func compareBreakRegexVerdicts(
	t *testing.T,
	item breakRegexCase,
	expression string,
	corpus []*string,
	want string,
	got string,
) {
	t.Helper()
	if len(want) != len(corpus) {
		t.Errorf("%s: %d SQLite verdicts for %d inputs", item.ID, len(want), len(corpus))
		return
	}
	// A predicate that one side of the corpus never reaches proves nothing.
	// Four is the smallest true set any case has: x GLOB '[AQgw]' holds for
	// exactly four strings, and the corpus holds every code point below
	// U+0100 on its own.
	trueCount, falseCount := strings.Count(want, "t"), strings.Count(want, "f")
	if trueCount < breakRegexMinimumVerdicts || falseCount < breakRegexMinimumVerdicts {
		t.Errorf("%s: the corpus reaches %d true and %d false SQLite verdicts; it must reach at least %d of each",
			item.ID, trueCount, falseCount, breakRegexMinimumVerdicts)
	}
	mismatches := 0
	for index := range corpus {
		if want[index] == got[index] {
			continue
		}
		mismatches++
		if mismatches > breakRegexReportLimit {
			continue
		}
		input := "NULL"
		if corpus[index] != nil {
			input = strconv.QuoteToASCII(*corpus[index])
		}
		context := ""
		if len(item.Contexts) > 0 {
			raw, _ := json.Marshal(item.Contexts[index%len(item.Contexts)])
			context = " with " + string(raw)
		}
		t.Errorf("%s: %s = %s%s: SQLite %c, PostgreSQL %c", item.ID, item.Target, input, context, want[index], got[index])
	}
	if mismatches > 0 {
		t.Errorf("%s: %d of %d verdicts differ\n  SQLite:     %s\n  PostgreSQL: %s",
			item.ID, mismatches, len(corpus), item.SQLite, expression)
	}
}

// TestPostgresGlobAndLikeCorpusCatchesMistranslations proves the corpus has
// teeth: each plausible porting mistake below must disagree with SQLite on at
// least one generated input. A corpus that let one of them through would let
// the same mistake in a real migration through too.
func TestPostgresGlobAndLikeCorpusCatchesMistranslations(t *testing.T) {
	cases := loadBreakRegexCases(t)
	expected := loadBreakRegexVerdicts(t)
	atoms := map[string]breakRegexCase{}
	for _, item := range cases {
		if item.Kind == "atom" {
			atoms[item.ID] = item
		}
	}
	mistakes := []struct {
		atom, translation, mistake string
	}{
		{"atom/glob-base64-last-sextet", `x ~ '[AQgw]'`, "unanchored single-character class"},
		{"atom/glob-alnum", `x ~ '^[A-Za-z0-9]'`, "missing end anchor"},
		{"atom/glob-lower-alnum", `x ~* '^[a-z0-9]$'`, "case-insensitive operator"},
		{"atom/glob-identifier-start", `x ~ '(?n)^[A-Za-z_]$'`, "newline-sensitive anchors"},
		{"atom/glob-not-url-safe-base64", `x !~ '[^A-Za-z0-9_]'`, "dropped class member -"},
		{"atom/glob-not-collector-id", `x !~ '[^A-Za-z0-9.:-]'`, "dropped class member _"},
		{"atom/glob-not-identifier", `x !~ '[^A-Za-z0-9]'`, "dropped class member _"},
		{"atom/glob-not-index-name", `x ~ '^[a-z0-9_-]+$'`, "anchored + rejects the empty string"},
		{"atom/glob-not-lower-hex", `x !~ '[^0-9a-fA-F]'`, "accepts upper-case hex"},
		{"atom/glob-not-printable-ascii", `x !~ '[^!-~]'`, "range starts after space"},
		{"atom/glob-not-visible-ascii", `x !~ '[^ -~]'`, "range starts at space"},
		{"atom/glob-not-control", `x !~ '[\u0001-\u001f]'`, "forgot DEL and the C1 controls"},
		{"atom/glob-not-control-keep-tab-lf-cr", `x !~ '[\u0001-\u001f\u007f-\u009f]'`, "rejects tab, LF and CR"},
		{"atom/like-not-backslash", `x NOT LIKE '%\%'`, "LIKE with PostgreSQL's default backslash escape"},
		{"atom/like-not-double-backslash", `x NOT LIKE '%\\%'`, "LIKE with PostgreSQL's default backslash escape"},
		{"atom/like-not-slash", `x NOT LIKE '%/_%'`, "stray _ wildcard"},
	}
	db := testsupport.NewPostgresControlDB(t).SQLDB()
	types := map[string]string{"x": `text COLLATE "C"`}
	for _, mistake := range mistakes {
		item, ok := atoms[mistake.atom]
		if !ok {
			t.Fatalf("no atom %s", mistake.atom)
		}
		if mistake.translation == item.PG {
			t.Fatalf("%s: the mistake is the shipped translation", mistake.atom)
		}
		corpus := breakRegexCorpus(t, item)
		got := evaluateBreakRegexCase(t, db, item, mistake.translation, types, corpus)
		want := expected[item.ID].verdicts
		if len(want) != len(got) {
			t.Fatalf("%s: %d SQLite verdicts for %d inputs", item.ID, len(want), len(got))
		}
		differing := 0
		for index := range want {
			if want[index] != got[index] {
				differing++
			}
		}
		if differing == 0 {
			t.Errorf("%s: the corpus cannot tell %s (%s) from %s", item.ID, mistake.translation, mistake.mistake, item.SQLite)
		}
	}
}

// The ported LIKE and bytea literals are backslash-bearing: strpos(x, '\'),
// strpos(x, '\\') and '\x'::bytea. With standard_conforming_strings off,
// which a database or role default can still impose, PostgreSQL reads a
// backslash in an ordinary literal as an escape: '\\' becomes one backslash,
// '\x' becomes "x", and '\') no longer ends the literal. The migrations and
// every PL/pgSQL body, which is parsed again by each session that calls it,
// must therefore never be read in such a session.
const breakRegexBackslashFixture = `
CREATE TABLE artifact_names (
	name text COLLATE "C" NOT NULL
		CONSTRAINT artifact_names_no_double_backslash CHECK (strpos(name, '\\') = 0)
);
`

// setNonstandardStringsDefault makes every new session on the database read
// backslashes in ordinary literals as escapes, and returns the database's
// connection string.
func setNonstandardStringsDefault(t *testing.T) string {
	t.Helper()
	dsn := newEmptyPostgresDatabase(t)
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database connection string: %v", err)
	}
	admin := openRawPostgres(t, testsupport.PostgresAdminDSN(t))
	if _, err := admin.ExecContext(t.Context(),
		`ALTER DATABASE `+pgx.Identifier{config.Database}.Sanitize()+` SET standard_conforming_strings = off`,
	); err != nil {
		t.Fatalf("set the database default: %v", err)
	}
	var setting string
	if err := openRawPostgres(t, dsn).QueryRowContext(t.Context(), `SHOW standard_conforming_strings`).
		Scan(&setting); err != nil || setting != "off" {
		t.Fatalf("a plain session on the database runs with standard_conforming_strings = %q (%v), want off", setting, err)
	}
	return dsn
}

func TestApplyPostgresMigrationsReadsBackslashesStandardly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn := setNonstandardStringsDefault(t)
	// The caller's pool inherits the database default, as an operator tool
	// or the test template builder would.
	raw := openRawPostgres(t, dsn)
	migrationFS := fstest.MapFS{"0001_artifact_names.sql": {Data: []byte(breakRegexBackslashFixture)}}
	if err := control.ApplyPostgresMigrations(ctx, raw, migrationFS); err != nil {
		t.Fatalf("ApplyPostgresMigrations() = %v", err)
	}
	insert := `INSERT INTO open_splunk.artifact_names (name) VALUES ($1)`
	if _, err := raw.ExecContext(ctx, insert, `a\b`); err != nil {
		t.Fatalf("a single backslash was rejected, so the migration read '\\\\' as one backslash: %v", err)
	}
	_, err := raw.ExecContext(ctx, insert, `a\\b`)
	if state := breakRegexSQLState(err); state != "23514" {
		t.Fatalf("a double backslash: %v (SQLSTATE %q), want a CHECK violation", err, state)
	}
}

func TestOpenPostgresReadsBackslashesStandardly(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	dsn := setNonstandardStringsDefault(t)
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	// Neither the database default nor a caller-supplied startup parameter
	// may change how the control plane reads literals.
	db, err := control.OpenPostgres(ctx, dsn+separator+"standard_conforming_strings=off")
	if err != nil {
		t.Fatalf("OpenPostgres() on a database defaulting to nonstandard strings = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("Close() = %v", err)
		}
	})

	for name, want := range map[string]string{
		"durable_export_job_artifact_shape_is_exact": `strpos(artifact_name, '\'::text) = 0`,
		"durable_search_jobs_check":                  `strpos(artifact_name, '\\'::text) = 0`,
	} {
		var definition string
		if err := db.SQLDB().QueryRowContext(ctx,
			`SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = $1`, name,
		).Scan(&definition); err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if !strings.Contains(definition, want) {
			t.Fatalf("%s = %s, want it to contain %s", name, definition, want)
		}
	}
	var outboxDefault string
	if err := db.SQLDB().QueryRowContext(ctx, `
		SELECT pg_get_expr(d.adbin, d.adrelid)
		FROM pg_attrdef AS d
		JOIN pg_attribute AS a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
		WHERE d.adrelid = 'open_splunk.ingest_visibility_reservations'::regclass AND a.attname = 'outbox_sha256'`,
	).Scan(&outboxDefault); err != nil || outboxDefault != `'\x'::bytea` {
		t.Fatalf("outbox_sha256 default = %q (%v), want the empty bytea '\\x'::bytea", outboxDefault, err)
	}

	// A PL/pgSQL body is parsed by the session that calls it, so
	// knowledge_object_version_writer_semantics_are_exact's
	// coalesce(NEW.definition_digest, '\x'::bytea) depends on the caller's
	// setting. Every control-plane session must read it as the empty bytea;
	// the plain session shows what the database default alone would do.
	const probe = `
		CREATE OR REPLACE FUNCTION pg_temp.empty_digest_length() RETURNS integer LANGUAGE plpgsql AS $$
		BEGIN
			RETURN octet_length(coalesce(NULL::bytea, '\x'::bytea));
		END
		$$`
	lengthIn := func(conn *sql.Conn) int {
		t.Helper()
		if _, err := conn.ExecContext(ctx, probe); err != nil {
			t.Fatalf("create probe: %v", err)
		}
		var length int
		if err := conn.QueryRowContext(ctx, `SELECT pg_temp.empty_digest_length()`).Scan(&length); err != nil {
			t.Fatalf("call probe: %v", err)
		}
		return length
	}
	plain, err := openRawPostgres(t, dsn).Conn(ctx)
	if err != nil {
		t.Fatalf("plain connection: %v", err)
	}
	if length := lengthIn(plain); length != 1 {
		t.Fatalf("a plain session read '\\x' as %d bytes; the database default no longer reproduces the hazard", length)
	}
	if err := plain.Close(); err != nil {
		t.Fatalf("release plain connection: %v", err)
	}
	conns := make([]*sql.Conn, 3)
	for index := range conns {
		conn, err := db.SQLDB().Conn(ctx)
		if err != nil {
			t.Fatalf("acquire control-plane connection: %v", err)
		}
		conns[index] = conn
	}
	for _, conn := range conns {
		var setting string
		if err := conn.QueryRowContext(ctx, `SHOW standard_conforming_strings`).Scan(&setting); err != nil || setting != "on" {
			t.Fatalf("control-plane session standard_conforming_strings = %q (%v), want on", setting, err)
		}
		if length := lengthIn(conn); length != 0 {
			t.Fatalf("a control-plane session read '\\x'::bytea as %d bytes, want the empty bytea", length)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("release connection: %v", err)
		}
	}
}

func breakRegexSQLState(err error) string {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return pgErr.Code
	}
	return ""
}
