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
	for i < n {
		c := src[i]
		switch {
		case c == '\n':
			line++
			i++
		case c == ' ' || c == '\t' || c == '\r':
			i++
		case c == '/' && i+1 < n && src[i+1] == '/':
			for i < n && src[i] != '\n' {
				i++
			}
		case c == '-' && i+1 < n && src[i+1] == '>':
			toks = append(toks, token{tPunct, "->", line, file})
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
			toks = append(toks, token{tNumber, src[start:i], line, file})
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
			toks = append(toks, token{tString, src[start+1 : i-1], line, file})
		case isIdentStart(rune(c)):
			start := i
			for i < n && isIdentPart(rune(src[i])) {
				i++
			}
			toks = append(toks, token{tIdent, src[start:i], line, file})
		case strings.ContainsRune("{}()[]:,.@!?", rune(c)):
			toks = append(toks, token{tPunct, string(c), line, file})
			i++
		default:
			return nil, fmt.Errorf("%s: unexpected character %q", pos(file, line), c)
		}
	}
	toks = append(toks, token{tEOF, "", line, file})
	return toks, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }

func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
