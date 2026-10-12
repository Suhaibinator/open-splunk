package common_util

import (
	"strings"

	"github.com/Suhaibinator/go-common/pkg/proto_models"
)

// detectMarkdownContent returns true when content contains markdown syntax that
// materially changes rendering. It is intentionally conservative because plain
// user text should not become markdown by accident.
func detectMarkdownContent(content string) bool {
	content = strings.TrimSpace(content)
	if content == "" {
		return false
	}

	if strings.Contains(content, "```") || hasPaired(content, "**") ||
		hasPaired(content, "__") || hasPaired(content, "~~") ||
		hasInlineCode(content) || hasMarkdownLink(content) {
		return true
	}

	lines := strings.Split(content, "\n")
	if hasMarkdownTable(lines) {
		return true
	}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if isMarkdownHeading(trimmed) || strings.HasPrefix(trimmed, "> ") ||
			isMarkdownListItem(trimmed) {
			return true
		}
	}
	return false
}

// DetectMessageContentType maps detected content into the persisted chat
// content_type enum used by frontend renderers.
func DetectMessageContentType(content string) proto_models.MessageContentType {
	if detectMarkdownContent(content) {
		return proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_MARKDOWN
	}
	return proto_models.MessageContentType_MESSAGE_CONTENT_TYPE_TEXT
}

func hasPaired(content, marker string) bool {
	_, after, ok := strings.Cut(content, marker)
	if !ok {
		return false
	}
	return strings.Contains(after, marker)
}

func hasInlineCode(content string) bool {
	_, after, ok := strings.Cut(content, "`")
	if !ok {
		return false
	}
	return strings.Contains(after, "`")
}

func hasMarkdownLink(content string) bool {
	close := strings.Index(content, "](")
	if close <= 0 {
		return false
	}
	return strings.Contains(content[close+2:], ")")
}

func hasMarkdownTable(lines []string) bool {
	for i := 0; i+1 < len(lines); i++ {
		if !strings.Contains(lines[i], "|") {
			continue
		}
		if isMarkdownTableSeparator(lines[i+1]) {
			return true
		}
	}
	return false
}

func isMarkdownTableSeparator(line string) bool {
	line = strings.TrimSpace(line)
	if !strings.Contains(line, "|") {
		return false
	}
	cells := strings.Split(line, "|")
	usable := 0
	for _, cell := range cells {
		cell = strings.TrimSpace(cell)
		if cell == "" {
			continue
		}
		cell = strings.Trim(cell, ":")
		if len(cell) < 3 || strings.Trim(cell, "-") != "" {
			return false
		}
		usable++
	}
	return usable > 0
}

func isMarkdownHeading(line string) bool {
	count := 0
	for count < len(line) && line[count] == '#' {
		count++
	}
	return count > 0 && count <= 6 && count < len(line) && line[count] == ' '
}

func isMarkdownListItem(line string) bool {
	if len(line) >= 3 {
		switch line[0] {
		case '-', '*', '+':
			if line[1] == ' ' {
				return true
			}
		}
	}

	i := 0
	for i < len(line) && line[i] >= '0' && line[i] <= '9' {
		i++
	}
	return i > 0 && i+1 < len(line) && (line[i] == '.' || line[i] == ')') && line[i+1] == ' '
}
