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
}

func lex(src string) ([]token, error) {
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
			toks = append(toks, token{tPunct, "->", line})
			i += 2
		case isDigit(c):
			start := i
			for i < n && isDigit(src[i]) {
				i++
			}
			toks = append(toks, token{tNumber, src[start:i], line})
		case c == '"':
			start := i
			i++
			for i < n && src[i] != '"' && src[i] != '\n' {
				i++
			}
			if i == n || src[i] == '\n' {
				return nil, fmt.Errorf("line %d: unterminated string", line)
			}
			i++
			toks = append(toks, token{tString, src[start+1 : i-1], line})
		case isIdentStart(rune(c)):
			start := i
			for i < n && isIdentPart(rune(src[i])) {
				i++
			}
			toks = append(toks, token{tIdent, src[start:i], line})
		case strings.ContainsRune("{}()[]:,.@!?", rune(c)):
			toks = append(toks, token{tPunct, string(c), line})
			i++
		default:
			return nil, fmt.Errorf("line %d: unexpected character %q", line, c)
		}
	}
	toks = append(toks, token{tEOF, "", line})
	return toks, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }

func isIdentPart(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}
