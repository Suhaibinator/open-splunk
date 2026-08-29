package officialspl

import (
	"strings"
	"testing"
)

const validCorpus = `{
  "format_version": 1,
  "cases": [{
    "id": "sort.ascending",
    "command": "sort",
    "source": {
      "url": "https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.0/search-commands/sort",
      "release": "10.0",
      "section": "Syntax",
      "kind": "grammar-derived",
      "fragment": "sort + host",
      "verified_on": "2026-08-29"
    },
    "query": "index=main | sort + host",
    "expect": {"commands": ["sort"]}
  }]
}`

func TestDecodeAcceptsPinnedOfficialSource(t *testing.T) {
	t.Parallel()
	corpus, err := Decode([]byte(validCorpus))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(corpus.Cases) != 1 || corpus.Cases[0].Source.Release != "10.0" {
		t.Fatalf("corpus = %#v", corpus)
	}
}

func TestDecodeRejectsUntraceableOrAmbiguousCases(t *testing.T) {
	t.Parallel()

	duplicateCase := strings.Replace(
		validCorpus,
		"]\n}",
		`, {"id":"sort.ascending","command":"sort","source":{"url":"https://help.splunk.com/en/splunk-enterprise/search/spl-search-reference/10.0/search-commands/sort","release":"10.0","section":"Syntax","kind":"grammar-derived","fragment":"sort host","verified_on":"2026-08-29"},"query":"index=main | sort host","expect":{"commands":["sort"]}}]}`,
		1,
	)
	tests := []struct {
		name, encoded, want string
	}{
		{name: "unknown schema field", encoded: strings.Replace(validCorpus, `"format_version": 1`, `"format_version": 1, "typo": true`, 1), want: "unknown field"},
		{name: "duplicate JSON key", encoded: strings.Replace(validCorpus, `"format_version": 1`, `"format_version": 1, "format_version": 1`, 1), want: "duplicates object key"},
		{name: "duplicate case id", encoded: duplicateCase, want: "duplicates id"},
		{name: "non-official host", encoded: strings.Replace(validCorpus, "help.splunk.com", "example.com", 1), want: "HTTPS help.splunk.com"},
		{name: "unpinned release", encoded: strings.Replace(validCorpus, `"release": "10.0"`, `"release": "latest"`, 1), want: "invalid release"},
		{name: "URL release mismatch", encoded: strings.Replace(validCorpus, `/10.0/search-commands/`, `/9.4/search-commands/`, 1), want: "url path must end"},
		{name: "URL command mismatch", encoded: strings.Replace(validCorpus, `/search-commands/sort`, `/search-commands/stats`, 1), want: "url path must end"},
		{name: "missing section", encoded: strings.Replace(validCorpus, `"section": "Syntax"`, `"section": ""`, 1), want: "invalid section"},
		{name: "unclassified source", encoded: strings.Replace(validCorpus, `"kind": "grammar-derived"`, `"kind": "guess"`, 1), want: "invalid kind"},
		{name: "fragment not exercised", encoded: strings.Replace(validCorpus, `"query": "index=main | sort + host"`, `"query": "index=main | sort other"`, 1), want: "does not contain"},
		{name: "command not asserted", encoded: strings.Replace(validCorpus, `"commands": ["sort"]`, `"commands": ["stats"]`, 1), want: "final expected command"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Decode([]byte(test.encoded))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Decode error = %v, want substring %q", err, test.want)
			}
		})
	}
}
