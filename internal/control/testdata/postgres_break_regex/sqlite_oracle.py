#!/usr/bin/env python3
"""Record SQLite's verdicts for the GLOB/LIKE differential test.

Usage: sqlite_oracle.py <final SQLite control-plane database> <corpus dump>

The dump is written by TestPostgresGlobAndLikePortsMatchSQLiteVerdicts when
OPEN_SPLUNK_BREAK_REGEX_DUMP names a file. This script

  * checks that every case's SQLite text is taken verbatim from the database
    (the CHECK of that table and name, or the WHEN clause of each named
    trigger, or for an atom a pattern the schema uses);
  * checks that the cases cover every CHECK and trigger that uses GLOB or LIKE,
    and that no other schema object does;
  * evaluates every case over its corpus and writes sqlite_verdicts.tsv next to
    this file.

It only reads the database. Run it with python3 -I.
"""
import hashlib
import json
import os
import re
import sqlite3
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PATTERN_WORD = re.compile(r"\b(GLOB|LIKE)\b")


def normalize(text):
    return " ".join(text.split())


def split_items(body):
    """Split a CREATE TABLE body on top-level commas, skipping strings and
    comments."""
    items, buf, depth, i = [], [], 0, 0
    while i < len(body):
        c = body[i]
        if c == "'":
            j = body.index("'", i + 1)
            while j + 1 < len(body) and body[j + 1] == "'":
                j = body.index("'", j + 2)
            buf.append(body[i:j + 1])
            i = j + 1
            continue
        if body.startswith("--", i):
            i = body.index("\n", i)
            continue
        depth += c == "("
        depth -= c == ")"
        if c == "," and depth == 0:
            items.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
        i += 1
    items.append("".join(buf).strip())
    return items


def table_checks(sql):
    """Yield (name or None, normalized expression) for every CHECK."""
    body = sql[sql.index("(") + 1:sql.rindex(")")]
    for item in split_items(body):
        for m in re.finditer(r"(?:CONSTRAINT\s+([a-z_0-9]+)\s+)?CHECK\s*\(", item):
            start = m.end() - 1
            depth, j = 0, start
            while True:
                if item[j] == "'":
                    j = item.index("'", j + 1)
                elif item[j] == "(":
                    depth += 1
                elif item[j] == ")":
                    depth -= 1
                    if depth == 0:
                        break
                j += 1
            yield m.group(1), normalize(item[start:j + 1])


def when_clause(sql):
    m = re.search(r"\bWHEN\b(.*)\bBEGIN\b", sql, re.S)
    return normalize(m.group(1)) if m else None


def strip_literals(sql):
    return re.sub(r"'(?:[^']|'')*'", "''", sql)


def context_value(value):
    if isinstance(value, dict):
        return bytes.fromhex(value["hex"])
    return value


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    database, dump_path = sys.argv[1], sys.argv[2]
    db = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    cases = json.load(open(os.path.join(HERE, "cases.json")))["cases"]
    dump = {entry["id"]: entry for entry in json.load(open(dump_path))}

    schema = list(db.execute("SELECT type, name, tbl_name, sql FROM sqlite_schema WHERE sql IS NOT NULL"))
    checks = {}
    triggers = {}
    for kind, name, table, sql in schema:
        if kind == "table":
            checks[table] = list(table_checks(sql))
        elif kind == "trigger":
            triggers[name] = (table, when_clause(sql), sql)
        elif PATTERN_WORD.search(strip_literals(sql)):
            sys.exit(f"{kind} {name} uses GLOB or LIKE but the cases cannot cover a {kind}")
    # Atom patterns are compared without whitespace, since the schema spells
    # the control-character class over several lines.
    compact_schema = re.sub(r"\s+", "", " ".join(sql for _, _, _, sql in schema))

    covered_checks, covered_triggers = set(), set()
    for case in cases:
        if case["kind"] == "check":
            # A PostgreSQL-only table names the SQLite table whose identical
            # CHECK stands in for it.
            table = case.get("sqlite_table", case["table"])
            matches = [n for n, expr in checks.get(table, []) if expr == case["sqlite"]]
            if not matches or (case["sqlite_constraint"] or None) not in matches:
                sys.exit(f"{case['id']}: not a CHECK of {table} named {case['sqlite_constraint']!r}")
            covered_checks.add((table, case["sqlite"]))
        elif case["kind"] == "trigger":
            for name in case["sqlite_triggers"]:
                table, when, _ = triggers[name]
                if table != case["table"] or when != case["sqlite"]:
                    sys.exit(f"{case['id']}: trigger {name} does not have this WHEN clause on {case['table']}")
                covered_triggers.add(name)
        else:
            pattern = re.sub(r"^x (NOT )?(GLOB|LIKE) ", "", case["sqlite"])
            if pattern == case["sqlite"] or re.sub(r"\s+", "", pattern) not in compact_schema:
                sys.exit(f"{case['id']}: pattern {pattern} is not used by the schema")
    for table, items in checks.items():
        for name, expr in items:
            if PATTERN_WORD.search(strip_literals(expr)) and (table, expr) not in covered_checks:
                sys.exit(f"CHECK {name or '(unnamed)'} on {table} uses GLOB or LIKE but has no case")
    for name, (_, _, sql) in triggers.items():
        if PATTERN_WORD.search(strip_literals(sql)) and name not in covered_triggers:
            sys.exit(f"trigger {name} uses GLOB or LIKE but has no case")

    digest = hashlib.sha256(open(database, "rb").read()).hexdigest()
    lines = [
        "# SQLite verdicts for testdata/postgres_break_regex/cases.json, one line per case:",
        "# <case id> TAB <corpus sha256> TAB <t|f|n per input>, in corpus order.",
        f"# Recorded by sqlite_oracle.py with SQLite {sqlite3.sqlite_version} from a database with sha256 {digest}.",
    ]
    for case in cases:
        entry = dump.get(case["id"])
        if entry is None:
            sys.exit(f"{case['id']}: missing from the dump")
        expression = case["sqlite"]
        if case["kind"] == "trigger":
            expression = re.sub(r"\bNEW\.", "new_row.", expression)
        columns = case["columns"]
        contexts = case.get("contexts") or [{}]
        statement = (f"SELECT ({expression}) FROM (SELECT "
                     + ", ".join(f'? AS "{column}"' for column in columns) + ") AS new_row")
        verdicts = []
        for index, value in enumerate(entry["inputs"]):
            context = contexts[index % len(contexts)]
            row = [value if column == case["target"] else context_value(context[column]) for column in columns]
            result = db.execute(statement, row).fetchone()[0]
            verdicts.append("n" if result is None else ("t" if result else "f"))
        lines.append(f"{case['id']}\t{entry['corpus_sha256']}\t{''.join(verdicts)}")
    with open(os.path.join(HERE, "sqlite_verdicts.tsv"), "w") as out:
        out.write("\n".join(lines) + "\n")
    print(f"{len(cases)} cases, {sum(len(dump[c['id']]['inputs']) for c in cases)} verdicts")


main()
