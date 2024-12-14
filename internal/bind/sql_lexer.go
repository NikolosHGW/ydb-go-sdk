package bind

import "unicode/utf8"

type sqlLexer struct {
	src        string
	start      int
	pos        int
	nested     int // multiline comment nesting level.
	stateFn    stateFn
	rawStateFn stateFn
	parts      []interface{}
}

type (
	positionalArg struct{}
	numericArg    int

	stateFn func(*sqlLexer) stateFn
)

func isLetter(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

func isNumber(r rune) bool {
	return r >= '0' && r <= '9'
}

func backtickState(lexer *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
		lexer.pos += width

		switch r {
		case '`':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune != '`' {
				return lexer.rawStateFn
			}
			lexer.pos += width
		case utf8.RuneError:
			if lexer.pos-lexer.start > 0 {
				lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos])
				lexer.start = lexer.pos
			}

			return nil
		}
	}
}

func singleQuoteState(lexer *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
		lexer.pos += width

		switch r {
		case '\'':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune != '\'' {
				return lexer.rawStateFn
			}
			lexer.pos += width
		case utf8.RuneError:
			if lexer.pos-lexer.start > 0 {
				lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos])
				lexer.start = lexer.pos
			}

			return nil
		}
	}
}

func doubleQuoteState(lexer *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
		lexer.pos += width

		switch r {
		case '"':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune != '"' {
				return lexer.rawStateFn
			}
			lexer.pos += width
		case utf8.RuneError:
			if lexer.pos-lexer.start > 0 {
				lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos])
				lexer.start = lexer.pos
			}

			return nil
		}
	}
}

func oneLineCommentState(lexer *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
		lexer.pos += width

		switch r {
		case '\\':
			_, width = utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			lexer.pos += width
		case '\n', '\r':
			return lexer.rawStateFn
		case utf8.RuneError:
			if lexer.pos-lexer.start > 0 {
				lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos])
				lexer.start = lexer.pos
			}

			return nil
		}
	}
}

func multilineCommentState(lexer *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
		lexer.pos += width

		switch r {
		case '/':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune == '*' {
				lexer.pos += width
				lexer.nested++
			}
		case '*':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune != '/' {
				continue
			}

			lexer.pos += width
			if lexer.nested == 0 {
				return lexer.rawStateFn
			}
			lexer.nested--

		case utf8.RuneError:
			if lexer.pos-lexer.start > 0 {
				lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos])
				lexer.start = lexer.pos
			}

			return nil
		}
	}
}
