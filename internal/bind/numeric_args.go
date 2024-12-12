package bind

import (
	"fmt"
	"strconv"
	"unicode/utf8"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

type NumericArgs struct{}

func (m NumericArgs) blockID() blockID {
	return blockYQL
}

func (m NumericArgs) RewriteQuery(sql string, args ...interface{}) (yql string, newArgs []interface{}, err error) {
	lexer := &sqlLexer{
		src:        sql,
		stateFn:    numericArgsStateFn,
		rawStateFn: numericArgsStateFn,
	}

	for lexer.stateFn != nil {
		lexer.stateFn = lexer.stateFn(lexer)
	}

	buffer := xstring.Buffer()
	defer buffer.Free()

	if len(args) > 0 {
		parameters, err := parsePositionalParameters(args)
		if err != nil {
			return "", nil, err
		}
		newArgs = make([]interface{}, len(parameters))
		for i, param := range parameters {
			newArgs[i] = param
		}
	}

	for _, p := range lexer.parts {
		switch partType := p.(type) {
		case string:
			buffer.WriteString(partType)
		case numericArg:
			if partType == 0 {
				return "", nil, xerrors.WithStackTrace(ErrUnexpectedNumericArgZero)
			}
			if int(partType) > len(args) {
				return "", nil, xerrors.WithStackTrace(
					fmt.Errorf("%w: $%d, len(args) = %d", ErrInconsistentArgs, partType, len(args)),
				)
			}
			paramIndex := int(partType - 1)
			val, ok := newArgs[paramIndex].(table.ParameterOption)
			if !ok {
				panic(fmt.Sprintf("unsupported type conversion from %T to table.ParameterOption", val))
			}
			buffer.WriteString(val.Name())
		}
	}

	yql = buffer.String()
	if len(newArgs) > 0 {
		yql = "-- origin query with numeric args replacement\n" + yql
	}

	return yql, newArgs, nil
}

func numericArgsStateFn(l *sqlLexer) stateFn {
	for {
		r, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch r {
		case '`':
			return backtickState
		case '\'':
			return singleQuoteState
		case '"':
			return doubleQuoteState
		case '$':
			nextRune, _ := utf8.DecodeRuneInString(l.src[l.pos:])
			if isNumber(nextRune) {
				if l.pos-l.start > 0 {
					l.parts = append(l.parts, l.src[l.start:l.pos-width])
				}
				l.start = l.pos

				return numericArgState
			}
		case '-':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '-' {
				l.pos += width

				return oneLineCommentState
			}
		case '/':
			nextRune, width := utf8.DecodeRuneInString(l.src[l.pos:])
			if nextRune == '*' {
				l.pos += width

				return multilineCommentState
			}
		case utf8.RuneError:
			if l.pos-l.start > 0 {
				l.parts = append(l.parts, l.src[l.start:l.pos])
				l.start = l.pos
			}

			return nil
		}
	}
}

func parsePositionalParameters(args []interface{}) ([]*params.Parameter, error) {
	newArgs := make([]*params.Parameter, len(args))
	for i, arg := range args {
		paramName := fmt.Sprintf("$p%d", i)
		param, err := toYdbParam(paramName, arg)
		if err != nil {
			return nil, err
		}
		newArgs[i] = param
	}

	return newArgs, nil
}

func numericArgState(l *sqlLexer) stateFn {
	numbers := ""
	defer func() {
		if len(numbers) > 0 {
			i, err := strconv.Atoi(numbers)
			if err != nil {
				panic(err)
			}
			l.parts = append(l.parts, numericArg(i))
			l.start = l.pos
		} else {
			l.parts = append(l.parts, l.src[l.start-1:l.pos])
			l.start = l.pos
		}
	}()
	for {
		rn, width := utf8.DecodeRuneInString(l.src[l.pos:])
		l.pos += width

		switch {
		case isNumber(rn):
			numbers += string(rn)
		case isLetter(rn):
			numbers = ""

			return l.rawStateFn
		default:
			l.pos -= width

			return l.rawStateFn
		}
	}
}
