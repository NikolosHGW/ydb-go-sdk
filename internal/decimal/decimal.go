package decimal

import (
	"math/big"
	"math/bits"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xstring"
)

const (
	wordSize   = bits.UintSize / 8
	bufferSize = 40
	negMask    = 0x80
)

var (
	ten  = big.NewInt(10) //nolint:gomnd
	zero = big.NewInt(0)
	one  = big.NewInt(1)
	inf  = big.NewInt(0).Mul(
		big.NewInt(100000000000000000),  //nolint:gomnd
		big.NewInt(1000000000000000000), //nolint:gomnd
	)
	nan    = big.NewInt(0).Add(inf, one)
	err    = big.NewInt(0).Add(nan, one)
	neginf = big.NewInt(0).Neg(inf)
	negnan = big.NewInt(0).Neg(nan)
)

const (
	errorTag = "<error>"
)

// IsInf reports whether x is an infinity.
func IsInf(x *big.Int) bool { return x.CmpAbs(inf) == 0 }

// IsNaN reports whether x is a "not-a-number" value.
func IsNaN(x *big.Int) bool { return x.CmpAbs(nan) == 0 }

// IsErr reports whether x is an "error" value.
func IsErr(x *big.Int) bool { return x.Cmp(err) == 0 }

// Inf returns infinity value.
func Inf() *big.Int { return big.NewInt(0).Set(inf) }

// NaN returns "not-a-number" value.
func NaN() *big.Int { return big.NewInt(0).Set(nan) }

// Err returns "error" value.
func Err() *big.Int { return big.NewInt(0).Set(err) }

// FromBytes converts bytes representation of decimal to big integer.
// Most callers should use FromInt128().
//
// If given bytes contains value that is greater than given precision it
// returns infinity or negative infinity value accordingly the bytes sign.
func FromBytes(bts []byte, precision, scale uint32) *big.Int {
	bigIntValue := big.NewInt(0)
	if len(bts) == 0 {
		return bigIntValue
	}

	bigIntValue.SetBytes(bts)
	neg := bts[0]&negMask != 0 //nolint:ifshort
	if neg {
		// Given bytes contains negative value.
		// Interpret is as two's complement.
		not(bigIntValue)
		bigIntValue.Add(bigIntValue, one)
		bigIntValue.Neg(bigIntValue)
	}
	if bigIntValue.CmpAbs(pow(ten, precision)) >= 0 {
		if neg {
			bigIntValue.Set(neginf)
		} else {
			bigIntValue.Set(inf)
		}
	}

	return bigIntValue
}

// FromInt128 returns big integer from given array. That is, it interprets
// 16-byte array as 128-bit integer.
func FromInt128(p [16]byte, precision, scale uint32) *big.Int {
	return FromBytes(p[:], precision, scale)
}

// Parse interprets a string s with the given precision and scale and returns
// the corresponding big integer.
//
//nolint:funlen
func Parse(str string, precision, scale uint32) (*big.Int, error) {
	if scale > precision {
		return nil, precisionError(str, precision, scale)
	}

	result := big.NewInt(0)
	if str == "" {
		return result, nil
	}

	neg := str[0] == '-' //nolint:ifshort,nolintlint
	if neg || str[0] == '+' {
		str = str[1:]
	}
	if isInf(str) {
		if neg {
			return result.Set(neginf), nil
		}

		return result.Set(inf), nil
	}
	if isNaN(str) {
		if neg {
			return result.Set(negnan), nil
		}

		return result.Set(nan), nil
	}

	integral := precision - scale

	var dot bool
	for ; len(str) > 0; str = str[1:] {
		currentChar := str[0]
		if currentChar == '.' {
			if dot {
				return nil, syntaxError(str)
			}
			dot = true

			continue
		}
		if dot {
			if scale > 0 {
				scale--
			} else {
				break
			}
		}

		if !isDigit(currentChar) {
			return nil, syntaxError(str)
		}

		result.Mul(result, ten)
		result.Add(result, big.NewInt(int64(currentChar-'0')))

		if !dot && result.Cmp(zero) > 0 && integral == 0 {
			if neg {
				return neginf, nil
			}

			return inf, nil
		}
		integral--
	}
	//nolint:nestif
	if len(str) > 0 { // Characters remaining.
		currentChar := str[0]
		if !isDigit(currentChar) {
			return nil, syntaxError(str)
		}
		plus := currentChar > '5'
		if !plus && currentChar == '5' {
			var tempInt big.Int
			plus = tempInt.And(result, one).Cmp(zero) != 0 // Last digit is not a zero.
			for !plus && len(str) > 1 {
				str = str[1:]
				currentChar := str[0]
				if !isDigit(currentChar) {
					return nil, syntaxError(str)
				}
				plus = currentChar != '0'
			}
		}
		if plus {
			result.Add(result, one)
			if result.Cmp(pow(ten, precision)) >= 0 {
				result.Set(inf)
			}
		}
	}
	result.Mul(result, pow(ten, scale))
	if neg {
		result.Neg(result)
	}

	return result, nil
}

// Format returns the string representation of x with the given precision and
// scale.
//
//nolint:funlen
func Format(value *big.Int, precision, scale uint32) string {
	switch {
	case value.CmpAbs(inf) == 0:
		if value.Sign() < 0 {
			return "-inf"
		}

		return "inf"

	case value.CmpAbs(nan) == 0:
		if value.Sign() < 0 {
			return "-nan"
		}

		return "nan"

	case value == nil:
		return "0"
	}

	positiveValue := big.NewInt(0).Set(value)
	neg := value.Sign() < 0 //nolint:ifshort,nolintlint
	if neg {
		// Convert negative to positive.
		positiveValue.Neg(value)
	}

	// log_{10}(2^120) ~= 36.12, 37 decimal places
	// plus dot, zero before dot, sign.
	bts := make([]byte, bufferSize)
	pos := len(bts)

	var digit big.Int
	for ; positiveValue.Cmp(zero) > 0; positiveValue.Div(positiveValue, ten) {
		if precision == 0 {
			return errorTag
		}
		precision--

		digit.Mod(positiveValue, ten)
		d := int(digit.Int64())
		if d != 0 || scale == 0 || pos > 0 {
			const numbers = "0123456789"
			pos--
			bts[pos] = numbers[d]
		}
		if scale > 0 {
			scale--
			if scale == 0 && pos > 0 {
				pos--
				bts[pos] = '.'
			}
		}
	}
	if scale > 0 {
		for ; scale > 0; scale-- {
			if precision == 0 {
				return errorTag
			}
			precision--
			pos--
			bts[pos] = '0'
		}

		pos--
		bts[pos] = '.'
	}
	if bts[pos] == '.' {
		pos--
		bts[pos] = '0'
	}
	if neg {
		pos--
		bts[pos] = '-'
	}

	return xstring.FromBytes(bts[pos:])
}

// BigIntToByte returns the 16-byte array representation of x.
//
// If x value does not fit in 16 bytes with given precision, it returns 16-byte
// representation of infinity or negative infinity value accordingly to x's sign.
func BigIntToByte(x *big.Int, precision, scale uint32) (p [16]byte) {
	if !IsInf(x) && !IsNaN(x) && !IsErr(x) && x.CmpAbs(pow(ten, precision)) >= 0 {
		if x.Sign() < 0 {
			x = neginf
		} else {
			x = inf
		}
	}
	put(x, p[:])

	return p
}

func put(x *big.Int, data []byte) {
	neg := x.Sign() < 0 //nolint:ifshort
	if neg {
		x = complement(x)
	}
	i := len(data)
	for _, d := range x.Bits() {
		for j := 0; j < wordSize; j++ {
			i--
			data[i] = byte(d)
			d >>= 8
		}
	}
	var pad byte
	if neg {
		pad = 0xff
	}
	for 0 < i && i < len(data) {
		i--
		data[i] = pad
	}
}

func Append(p []byte, x *big.Int) []byte {
	n := len(p)
	p = ensure(p, size(x))
	put(x, p[n:])

	return p
}

func size(x *big.Int) int {
	if x.Sign() < 0 {
		x = complement(x)
	}

	return len(x.Bits()) * wordSize
}

func ensure(data []byte, additionalBytes int) []byte {
	var (
		currentLength   = len(data)
		currentCapacity = cap(data)
	)
	if currentCapacity-currentLength < additionalBytes {
		cp := make([]byte, currentLength+additionalBytes)
		copy(cp, data)
		data = cp
	}

	return data[:currentLength+additionalBytes]
}

// not is almost the same as x.Not() but without handling the sign of x.
// That is, it more similar to x.Xor(ones) where ones is x bits all set to 1.
func not(x *big.Int) {
	abs := x.Bits()
	for i, d := range abs {
		abs[i] = ^d
	}
}

// pow returns new instance of big.Int equal to x^n.
func pow(base *big.Int, exponent uint32) *big.Int {
	var (
		result     = big.NewInt(1)
		multiplier = big.NewInt(0).Set(base)
	)
	for exponent > 0 {
		if exponent&1 != 0 {
			result.Mul(result, multiplier)
		}
		exponent >>= 1
		multiplier.Mul(multiplier, multiplier)
	}

	return result
}

// complement returns two's complement of x.
// x must be negative.
func complement(x *big.Int) *big.Int {
	x = big.NewInt(0).Set(x)
	not(x)
	x.Neg(x)
	x.Add(x, one)

	return x
}

func isInf(s string) bool {
	return len(s) >= 3 && (s[0] == 'i' || s[0] == 'I') && (s[1] == 'n' || s[1] == 'N') && (s[2] == 'f' || s[2] == 'F')
}

func isNaN(s string) bool {
	return len(s) >= 3 && (s[0] == 'n' || s[0] == 'N') && (s[1] == 'a' || s[1] == 'A') && (s[2] == 'n' || s[2] == 'N')
}

func isDigit(c byte) bool {
	return '0' <= c && c <= '9'
}
