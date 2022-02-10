package rsql

import (
	"strings"
)

// Token is a lexical token.
type Token int

// These are os list of tokens rqlite detects.
const (
	// ILLEGAL Token, EOF, WS are rqlite tokens.
	ILLEGAL Token = iota
	EOF
	WS

	// Tokens that are recognized
	RANDOM // random()
	STRING // '....' or "....."
	IDENT  // Any other non-string token
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	RANDOM: "RANDOM()",
	STRING: "STRING",
	IDENT:  "IDENT",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keywordBeg + 1; tok < keywordEnd; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	for _, tok := range []Token{AND, OR} {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
	keywords["true"] = TRUE
	keywords["false"] = FALSE
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}
