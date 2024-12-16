package rawtopiccommon

import (
	"errors"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var errUnexpectedProtobufInOffsets = xerrors.Wrap(errors.New("ydb: unexpected protobuf nil offsets"))

type OffsetRange struct {
	Start Offset
	End   Offset
}

func (r *OffsetRange) FromProto(proto *Ydb_Topic.OffsetsRange) error {
	if proto == nil {
		return xerrors.WithStackTrace(errUnexpectedProtobufInOffsets)
	}

	r.Start.FromInt64(proto.GetStart())
	r.End.FromInt64(proto.GetEnd())

	return nil
}

func (r *OffsetRange) ToProto() *Ydb_Topic.OffsetsRange {
	return &Ydb_Topic.OffsetsRange{
		Start: r.Start.ToInt64(),
		End:   r.End.ToInt64(),
	}
}
