// Package sdl implements the .loom schema definition language — the
// hand-authored source of truth a service is generated from.
package sdl

import (
	"fmt"
	"strings"
	"unicode"
)

type tokKind int

const (
	tEOF tokKind = iota
	tIdent
	tNumber
	tString
	tPunct
)

type token struct {
	kind tokKind
	text string
	line int
	file string // the source file's path; "" for a lone unnamed source
	// doc is the doc comment written directly above this token: whole
	// `///` lines ending on the line before it, with nothing between.
	// Only the first token of a line carries one. Plain `//` comments
	// are never docs: a doc is published to every caller of the gateway,
	// so publishing one is something the author writes on purpose.
	doc string
}

// pos renders a token's position for an error: "path:line" when the
// token came from a named file, "line N" when it did not.
func pos(file string, line int) string {
	if file == "" {
		return fmt.Sprintf("line %d", line)
	}
	return fmt.Sprintf("%s:%d", file, line)
}

func lex(src string) ([]token, error) { return lexFile("", src) }

// lexFile lexes one source file, stamping every token with its path.
func lexFile(file, src string) ([]token, error) {
	var toks []token
	line := 1
	i := 0
	n := len(src)
	// the doc comment being gathered: its lines, and the line its last
	// one sat on. A token on the very next line takes it as its doc; a
	// blank line, a plain comment or any other token drops it.
	var doc []string
	docEnd := 0
	emit := func(t token) {
		if len(doc) > 0 && docEnd == t.line-1 {
			t.doc = docText(doc)
		}
		doc = nil
		toks = append(toks, t)
	}
	for i < n {
		c := src[i]
		switch {
		case c == '\n':
			line++
			i++
		case c == ' ' || c == '\t' || c == '\r':
			i++
		case c == '/' && i+1 < n && src[i+1] == '/':
			start := i
			for i < n && src[i] != '\n' {
				i++
			}
			// `///` is a doc comment: published. Plain `//` is a note
			// for whoever reads the schema, and ends any doc block above
			// it. A comment after a token on its own line is neither.
			text := src[start:i]
			trailing := len(toks) > 0 && toks[len(toks)-1].line == line
			if trailing || !strings.HasPrefix(text, "///") || (len(doc) > 0 && docEnd != line-1) {
				doc = nil
			}
			if !trailing && strings.HasPrefix(text, "///") {
				doc = append(doc, strings.TrimSpace(text[3:]))
				docEnd = line
			}
		case c == '-' && i+1 < n && src[i+1] == '>':
			emit(token{kind: tPunct, text: "->", line: line, file: file})
			i += 2
		case c == '+' && i+1 < n && src[i+1] == '=':
			emit(token{kind: tPunct, text: "+=", line: line, file: file})
			i += 2
		case isDigit(c):
			start := i
			for i < n && isDigit(src[i]) {
				i++
			}
			// A unit suffix glued to the digits (90d, 24h, 2w) is one
			// token: the duration literal @retain takes. Anything that
			// Atoi's the text still fails loudly where a bare count is
			// expected.
			for i < n && isIdentPart(rune(src[i])) {
				i++
			}
			emit(token{kind: tNumber, text: src[start:i], line: line, file: file})
		case c == '"':
			start := i
			i++
			for i < n && src[i] != '"' && src[i] != '\n' {
				i++
			}
			if i == n || src[i] == '\n' {
				return nil, fmt.Errorf("%s: unterminated string", pos(file, line))
			}
			i++
			emit(token{kind: tString, text: src[start+1 : i-1], line: line, file: file})
		case isIdentStart(rune(c)):
			start := i
			for i < n && isIdentPart(rune(src[i])) {
				i++
			}
			emit(token{kind: tIdent, text: src[start:i], line: line, file: file})
		case strings.ContainsRune("{}()[]:,.@!?", rune(c)):
			emit(token{kind: tPunct, text: string(c), line: line, file: file})
			i++
		default:
			return nil, fmt.Errorf("%s: unexpected character %q", pos(file, line), c)
		}
	}
	toks = append(toks, token{kind: tEOF, line: line, file: file})
	return toks, nil
}

// docText joins a comment block into prose: hard-wrapped lines run
// together, and an empty comment line starts a new paragraph.
func docText(lines []string) string {
	var b strings.Builder
	gap := ""
	for _, l := range lines {
		if l == "" {
			if b.Len() > 0 {
				gap = "\n\n"
			}
			continue
		}
		b.WriteString(gap + l)
		gap = " "
	}
	return b.String()
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }

func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
