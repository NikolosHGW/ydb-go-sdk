package bind

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errUnsupportedType         = errors.New("unsupported type")
	errUnnamedParam            = errors.New("unnamed param")
	errMultipleQueryParameters = errors.New("only one query arg *table.QueryParameters allowed")
)

var (
	uuidType    = reflect.TypeOf(uuid.UUID{})
	uuidPtrType = reflect.TypeOf((*uuid.UUID)(nil))
)

func asUUID(val interface{}) (value.Value, bool) {
	switch reflect.TypeOf(val) {
	case uuidType:
		return value.Uuid(val.(uuid.UUID)), true //nolint:forcetypeassert
	case uuidPtrType:
		vv := val.(*uuid.UUID) //nolint:forcetypeassert
		if vv == nil {
			return value.NullValue(types.UUID), true
		}

		return value.OptionalValue(value.Uuid(*(val.(*uuid.UUID)))), true //nolint:forcetypeassert
	}

	return nil, false
}

func toType(v interface{}) (_ types.Type, err error) { //nolint:funlen
	switch valueType := v.(type) {
	case bool:
		return types.Bool, nil
	case int:
		return types.Int32, nil
	case uint:
		return types.Uint32, nil
	case int8:
		return types.Int8, nil
	case uint8:
		return types.Uint8, nil
	case int16:
		return types.Int16, nil
	case uint16:
		return types.Uint16, nil
	case int32:
		return types.Int32, nil
	case uint32:
		return types.Uint32, nil
	case int64:
		return types.Int64, nil
	case uint64:
		return types.Uint64, nil
	case float32:
		return types.Float, nil
	case float64:
		return types.Double, nil
	case []byte:
		return types.Bytes, nil
	case string:
		return types.Text, nil
	case [16]byte:
		return nil, xerrors.Wrap(value.ErrIssue1501BadUUID)
	case time.Time:
		return types.Timestamp, nil
	case time.Duration:
		return types.Interval, nil
	default:
		kind := reflect.TypeOf(valueType).Kind()
		switch kind {
		case reflect.Slice, reflect.Array:
			v := reflect.ValueOf(valueType)
			elemType, err := toType(reflect.New(v.Type().Elem()).Elem().Interface())
			if err != nil {
				return nil, xerrors.WithStackTrace(
					fmt.Errorf("cannot parse slice item type %T: %w",
						valueType, errUnsupportedType,
					),
				)
			}

			return types.NewList(elemType), nil
		case reflect.Map:
			reflectValue := reflect.ValueOf(valueType)

			keyType, err := toType(reflect.New(reflectValue.Type().Key()).Interface())
			if err != nil {
				return nil, fmt.Errorf("cannot parse %T map key: %w",
					reflect.New(reflectValue.Type().Key()).Interface(), err,
				)
			}
			valueType, err := toType(reflect.New(reflectValue.Type().Elem()).Interface())
			if err != nil {
				return nil, fmt.Errorf("cannot parse %T map value: %w",
					reflectValue.MapKeys()[0].Interface(), err,
				)
			}

			return types.NewDict(keyType, valueType), nil
		case reflect.Struct:
			reflectValue := reflect.ValueOf(valueType)

			fields := make([]types.StructField, reflectValue.NumField())

			for i := range fields {
				kk, has := reflectValue.Type().Field(i).Tag.Lookup("sql")
				if !has {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as key field of struct: %w",
							reflectValue.Field(i).Interface(), errUnsupportedType,
						),
					)
				}
				tt, err := toType(reflectValue.Field(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as values of dict: %w",
							reflectValue.Field(i).Interface(), errUnsupportedType,
						),
					)
				}

				fields[i] = types.StructField{
					Name: kk,
					T:    tt,
				}
			}

			return types.NewStruct(fields...), nil
		default:
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%T: %w. Create issue for support new type %s",
					valueType, errUnsupportedType, supportNewTypeLink(valueType),
				),
			)
		}
	}
}

//nolint:gocyclo,funlen
func toValue(val interface{}) (_ value.Value, err error) {
	if x, ok := asUUID(val); ok {
		return x, nil
	}

	switch x := val.(type) {
	case nil:
		return value.VoidValue(), nil
	case value.Value:
		return x, nil
	}

	if vv := reflect.ValueOf(val); vv.Kind() == reflect.Pointer {
		if vv.IsNil() {
			tt, err := toType(reflect.New(vv.Type().Elem()).Elem().Interface())
			if err != nil {
				return nil, xerrors.WithStackTrace(
					fmt.Errorf("cannot parse type of %T: %w",
						val, err,
					),
				)
			}

			return value.NullValue(tt), nil
		}

		vv, err := toValue(vv.Elem().Interface())
		if err != nil {
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("cannot parse value of %T: %w",
					val, err,
				),
			)
		}

		return value.OptionalValue(vv), nil
	}

	switch elemType := val.(type) {
	case nil:
		return value.VoidValue(), nil
	case value.Value:
		return elemType, nil
	case bool:
		return value.BoolValue(elemType), nil
	case int:
		return value.Int32Value(int32(elemType)), nil
	case uint:
		return value.Uint32Value(uint32(elemType)), nil
	case int8:
		return value.Int8Value(elemType), nil
	case uint8:
		return value.Uint8Value(elemType), nil
	case int16:
		return value.Int16Value(elemType), nil
	case uint16:
		return value.Uint16Value(elemType), nil
	case int32:
		return value.Int32Value(elemType), nil
	case uint32:
		return value.Uint32Value(elemType), nil
	case int64:
		return value.Int64Value(elemType), nil
	case uint64:
		return value.Uint64Value(elemType), nil
	case float32:
		return value.FloatValue(elemType), nil
	case float64:
		return value.DoubleValue(elemType), nil
	case []byte:
		return value.BytesValue(elemType), nil
	case string:
		return value.TextValue(elemType), nil
	case []string:
		items := make([]value.Value, len(elemType))
		for i := range elemType {
			items[i] = value.TextValue(elemType[i])
		}

		return value.ListValue(items...), nil
	case value.UUIDIssue1501FixedBytesWrapper:
		return value.UUIDWithIssue1501Value(elemType.AsBytesArray()), nil
	case [16]byte:
		return nil, xerrors.Wrap(value.ErrIssue1501BadUUID)
	case time.Time:
		return value.TimestampValueFromTime(elemType), nil
	case time.Duration:
		return value.IntervalValueFromDuration(elemType), nil
	default:
		kind := reflect.TypeOf(elemType).Kind()
		switch kind {
		case reflect.Slice, reflect.Array:
			v := reflect.ValueOf(elemType)
			list := make([]value.Value, v.Len())

			for i := range list {
				list[i], err = toValue(v.Index(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %d item of slice %T: %w",
							i, elemType, err,
						),
					)
				}
			}

			return value.ListValue(list...), nil
		case reflect.Map:
			v := reflect.ValueOf(elemType)
			fields := make([]value.DictValueField, 0, len(v.MapKeys()))
			iter := v.MapRange()
			for iter.Next() {
				kk, err := toValue(iter.Key().Interface())
				if err != nil {
					return nil, fmt.Errorf("cannot parse %v map key: %w",
						iter.Key().Interface(), err,
					)
				}
				vv, err := toValue(iter.Value().Interface())
				if err != nil {
					return nil, fmt.Errorf("cannot parse %v map value: %w",
						iter.Value().Interface(), err,
					)
				}
				fields = append(fields, value.DictValueField{
					K: kk,
					V: vv,
				})
			}

			return value.DictValue(fields...), nil
		case reflect.Struct:
			reflectValue := reflect.ValueOf(elemType)

			fields := make([]value.StructValueField, reflectValue.NumField())

			for i := range fields {
				kk, has := reflectValue.Type().Field(i).Tag.Lookup("sql")
				if !has {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %q as key field of struct: %w",
							reflectValue.Type().Field(i).Name, errUnsupportedType,
						),
					)
				}
				vv, err := toValue(reflectValue.Field(i).Interface())
				if err != nil {
					return nil, xerrors.WithStackTrace(
						fmt.Errorf("cannot parse %v as values of dict: %w",
							reflectValue.Index(i).Interface(), err,
						),
					)
				}

				fields[i] = value.StructValueField{
					Name: kk,
					V:    vv,
				}
			}

			return value.StructValue(fields...), nil
		default:
			return nil, xerrors.WithStackTrace(
				fmt.Errorf("%T: %w. Create issue for support new type %s",
					elemType, errUnsupportedType, supportNewTypeLink(elemType),
				),
			)
		}
	}
}

func supportNewTypeLink(x interface{}) string {
	v := url.Values{}
	v.Add("labels", "enhancement,database/sql")
	v.Add("template", "02_FEATURE_REQUEST.md")
	v.Add("title", fmt.Sprintf("feat: Support new type `%T` in `database/sql` query args", x))

	return "https://github.com/ydb-platform/ydb-go-sdk/issues/new?" + v.Encode()
}

func toYdbParam(name string, value interface{}) (*params.Parameter, error) {
	if na, ok := value.(driver.NamedValue); ok {
		n, v := na.Name, na.Value
		if n != "" {
			name = n
		}
		value = v
	}
	if v, ok := value.(*params.Parameter); ok {
		return v, nil
	}
	val, err := toValue(value)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}
	if name == "" {
		return nil, xerrors.WithStackTrace(errUnnamedParam)
	}
	if name[0] != '$' {
		name = "$" + name
	}

	return params.Named(name, val), nil
}

func Params(args ...interface{}) ([]*params.Parameter, error) {
	parameters := make([]*params.Parameter, 0, len(args))
	for i, arg := range args {
		var newParam *params.Parameter
		var newParams []*params.Parameter
		var err error
		switch elemType := arg.(type) {
		case driver.NamedValue:
			newParams, err = paramHandleNamedValue(elemType, i, len(args))
		case sql.NamedArg:
			if elemType.Name == "" {
				return nil, xerrors.WithStackTrace(errUnnamedParam)
			}
			newParam, err = toYdbParam(elemType.Name, elemType.Value)
			newParams = append(newParams, newParam)
		case *params.Params:
			if len(args) > 1 {
				return nil, xerrors.WithStackTrace(errMultipleQueryParameters)
			}
			parameters = *elemType
		case *params.Parameter:
			newParams = append(newParams, elemType)
		default:
			newParam, err = toYdbParam(fmt.Sprintf("$p%d", i), elemType)
			newParams = append(newParams, newParam)
		}
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		parameters = append(parameters, newParams...)
	}
	sort.Slice(parameters, func(i, j int) bool {
		return parameters[i].Name() < parameters[j].Name()
	})

	return parameters, nil
}

func paramHandleNamedValue(arg driver.NamedValue, paramNumber, argsLen int) ([]*params.Parameter, error) {
	if arg.Name == "" {
		switch driverType := arg.Value.(type) {
		case *params.Params:
			if argsLen > 1 {
				return nil, xerrors.WithStackTrace(errMultipleQueryParameters)
			}

			return *driverType, nil
		case *params.Parameter:
			return []*params.Parameter{driverType}, nil
		default:
			arg.Name = fmt.Sprintf("$p%d", paramNumber)
			param, err := toYdbParam(arg.Name, arg.Value)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return []*params.Parameter{param}, nil
		}
	} else {
		param, err := toYdbParam(arg.Name, arg.Value)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return []*params.Parameter{param}, nil
	}
}
