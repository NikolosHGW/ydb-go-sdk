package bind

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type PositionalArgs struct{}

func (m PositionalArgs) blockID() blockID {
	return blockYQL
}

func (m PositionalArgs) RewriteQuery(sql string, args ...interface{}) (
	yql string, newArgs []interface{}, err error,
) {
	lexer := &sqlLexer{
		src:        sql,
		stateFn:    positionalArgsStateFn,
		rawStateFn: positionalArgsStateFn,
	}

	for lexer.stateFn != nil {
		lexer.stateFn = lexer.stateFn(lexer)
	}

	var (
		buffer   = xstring.Buffer()
		position = 0
		param    table.ParameterOption
	)
	defer buffer.Free()

	for _, p := range lexer.parts {
		switch p := p.(type) {
		case string:
			buffer.WriteString(p)
		case positionalArg:
			if position > len(args)-1 {
				return "", nil, xerrors.WithStackTrace(
					fmt.Errorf("%w: position %d, len(args) = %d", ErrInconsistentArgs, position, len(args)),
				)
			}
			paramName := "$p" + strconv.Itoa(position)
			param, err = toYdbParam(paramName, args[position])
			if err != nil {
				return "", nil, xerrors.WithStackTrace(err)
			}
			newArgs = append(newArgs, param)
			buffer.WriteString(paramName)
			position++
		}
	}

	if len(args) != position {
		return "", nil, xerrors.WithStackTrace(
			fmt.Errorf("%w: (positional args %d, query args %d)", ErrInconsistentArgs, position, len(args)),
		)
	}

	if position > 0 {
		const prefix = "-- origin query with positional args replacement\n"

		return prefix + buffer.String(), newArgs, nil
	}

	return buffer.String(), newArgs, nil
}

func positionalArgsStateFn(lexer *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
		lexer.pos += width

		switch r {
		case '`':
			return backtickState
		case '\'':
			return singleQuoteState
		case '"':
			return doubleQuoteState
		case '?':
			lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos-1], positionalArg{})
			lexer.start = lexer.pos
		case '-':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune == '-' {
				lexer.pos += width

				return oneLineCommentState
			}
		case '/':
			nextRune, width := utf8.DecodeRuneInString(lexer.src[lexer.pos:])
			if nextRune == '*' {
				lexer.pos += width

				return multilineCommentState
			}
		case utf8.RuneError:
			if lexer.pos-lexer.start > 0 {
				lexer.parts = append(lexer.parts, lexer.src[lexer.start:lexer.pos])
				lexer.start = lexer.pos
			}

			return nil
		}
	}
}
