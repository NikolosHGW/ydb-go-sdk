package rawtopicreader

import (
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errUnexpectedNilStreamReadMessageReadResponse = xerrors.Wrap(errors.New("ydb: unexpected nil Ydb_Topic.StreamReadMessage_ReadResponse")) //nolint:lll
	errNilPartitionData                           = xerrors.Wrap(errors.New("ydb: unexpected nil partition data"))
	errUnexpectedNilBatchInPartitionData          = xerrors.Wrap(errors.New("ydb: unexpected nil batch in partition data"))   //nolint:lll
	errUnexpectedMessageNilInPartitionData        = xerrors.Wrap(errors.New("ydb: unexpected message nil in partition data")) //nolint:lll

	errUnexpectedProtoNilStartPartitionSessionRequest = xerrors.Wrap(errors.New("ydb: unexpected proto nil start partition session request"))                      //nolint:lll
	errUnexpectedNilPartitionSession                  = xerrors.Wrap(errors.New("ydb: unexpected proto nil partition session in start partition session request")) //nolint:lll
	errUnexpectedGrpcNilStopPartitionSessionRequest   = xerrors.Wrap(errors.New("ydb: unexpected grpc nil stop partition session request"))                        //nolint:lll
)

type PartitionSessionID int64

func (id *PartitionSessionID) FromInt64(v int64) {
	*id = PartitionSessionID(v)
}

func (id PartitionSessionID) ToInt64() int64 {
	return int64(id)
}

type OptionalOffset struct {
	Offset   rawtopiccommon.Offset
	HasValue bool
}

func (offset *OptionalOffset) FromInt64Pointer(value *int64) {
	if value == nil {
		offset.HasValue = false
		offset.Offset.FromInt64(-1)
	} else {
		offset.HasValue = true
		offset.Offset.FromInt64(*value)
	}
}

func (offset *OptionalOffset) FromInt64(v int64) {
	offset.FromInt64Pointer(&v)
}

func (offset OptionalOffset) ToInt64() int64 {
	return offset.Offset.ToInt64()
}

func (offset OptionalOffset) ToInt64Pointer() *int64 {
	if offset.HasValue {
		v := offset.Offset.ToInt64()

		return &v
	}

	return nil
}

//
// UpdateTokenRequest
//

type UpdateTokenRequest struct {
	clientMessageImpl

	rawtopiccommon.UpdateTokenRequest
}

type UpdateTokenResponse struct {
	rawtopiccommon.UpdateTokenResponse

	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata
}

//
// InitRequest
//

type InitRequest struct {
	clientMessageImpl

	TopicsReadSettings []TopicReadSettings

	Consumer string
}

func (r *InitRequest) toProto() *Ydb_Topic.StreamReadMessage_InitRequest {
	proto := &Ydb_Topic.StreamReadMessage_InitRequest{
		Consumer: r.Consumer,
	}

	proto.TopicsReadSettings = make([]*Ydb_Topic.StreamReadMessage_InitRequest_TopicReadSettings, len(r.TopicsReadSettings))
	for topicSettingsIndex := range r.TopicsReadSettings {
		srcTopicSettings := &r.TopicsReadSettings[topicSettingsIndex]
		dstTopicSettings := &Ydb_Topic.StreamReadMessage_InitRequest_TopicReadSettings{}
		proto.TopicsReadSettings[topicSettingsIndex] = dstTopicSettings

		dstTopicSettings.Path = srcTopicSettings.Path
		dstTopicSettings.MaxLag = srcTopicSettings.MaxLag.ToProto()
		dstTopicSettings.ReadFrom = srcTopicSettings.ReadFrom.ToProto()

		partitionsIDs := make([]int64, len(srcTopicSettings.PartitionsID))
		copy(partitionsIDs, srcTopicSettings.PartitionsID)

		dstTopicSettings.PartitionIds = partitionsIDs
	}

	return proto
}

// GetConsumer for implement trace.TopicReadStreamInitRequestInfo
func (r *InitRequest) GetConsumer() string {
	return r.Consumer
}

// GetTopics for implement trace.TopicReadStreamInitRequestInfo
func (r *InitRequest) GetTopics() []string {
	res := make([]string, len(r.TopicsReadSettings))
	for i := range res {
		res[i] = r.TopicsReadSettings[i].Path
	}

	return res
}

type TopicReadSettings struct {
	Path         string
	PartitionsID []int64

	MaxLag   rawoptional.Duration
	ReadFrom rawoptional.Time
}

type InitResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	SessionID string
}

func (g *InitResponse) fromProto(p *Ydb_Topic.StreamReadMessage_InitResponse) {
	g.SessionID = p.GetSessionId()
}

//
// ReadRequest
//

type ReadRequest struct {
	clientMessageImpl

	BytesSize int
}

func (r *ReadRequest) toProto() *Ydb_Topic.StreamReadMessage_ReadRequest {
	return &Ydb_Topic.StreamReadMessage_ReadRequest{BytesSize: int64(r.BytesSize)}
}

type ReadResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	BytesSize     int
	PartitionData []PartitionData
}

// GetBytesSize implements trace.TopicReaderDataResponseInfo
func (r *ReadResponse) GetBytesSize() int {
	return r.BytesSize
}

// GetPartitionBatchMessagesCounts implements trace.TopicReaderDataResponseInfo
func (r *ReadResponse) GetPartitionBatchMessagesCounts() (partitionDataCount, batchCount, messagesCount int) {
	partitionDataCount = len(r.PartitionData)
	for partitionIndex := range r.PartitionData {
		partitionData := &r.PartitionData[partitionIndex]
		batchCount += len(partitionData.Batches)
		for batchIndex := range partitionData.Batches {
			messagesCount += len(partitionData.Batches[batchIndex].MessageData)
		}
	}

	return partitionDataCount, batchCount, messagesCount
}

func (r *ReadResponse) fromProto(proto *Ydb_Topic.StreamReadMessage_ReadResponse) error {
	if proto == nil {
		return xerrors.WithStackTrace(errUnexpectedNilStreamReadMessageReadResponse)
	}
	r.BytesSize = int(proto.GetBytesSize())

	r.PartitionData = make([]PartitionData, len(proto.GetPartitionData()))
	for partitionIndex := range proto.GetPartitionData() {
		srcPartition := proto.GetPartitionData()[partitionIndex]
		if srcPartition == nil {
			return xerrors.WithStackTrace(errNilPartitionData)
		}
		dstPartition := &r.PartitionData[partitionIndex]
		dstPartition.PartitionSessionID.FromInt64(srcPartition.GetPartitionSessionId())

		dstPartition.Batches = make([]Batch, len(srcPartition.GetBatches()))

		for batchIndex := range srcPartition.GetBatches() {
			srcBatch := srcPartition.GetBatches()[batchIndex]
			if srcBatch == nil {
				return xerrors.WithStackTrace(errUnexpectedNilBatchInPartitionData)
			}
			dstBatch := &dstPartition.Batches[batchIndex]

			dstBatch.ProducerID = srcBatch.GetProducerId()
			dstBatch.WriteSessionMeta = srcBatch.GetWriteSessionMeta()
			dstBatch.Codec.MustFromProto(Ydb_Topic.Codec(srcBatch.GetCodec()))

			dstBatch.WrittenAt = srcBatch.GetWrittenAt().AsTime()

			dstBatch.MessageData = make([]MessageData, len(srcBatch.GetMessageData()))
			for messageIndex := range srcBatch.GetMessageData() {
				srcMessage := srcBatch.GetMessageData()[messageIndex]
				if srcMessage == nil {
					return xerrors.WithStackTrace(errUnexpectedMessageNilInPartitionData)
				}
				dstMessage := &dstBatch.MessageData[messageIndex]

				dstMessage.Offset.FromInt64(srcMessage.GetOffset())
				dstMessage.SeqNo = srcMessage.GetSeqNo()
				dstMessage.CreatedAt = srcMessage.GetCreatedAt().AsTime()
				dstMessage.Data = srcMessage.GetData()
				dstMessage.UncompressedSize = srcMessage.GetUncompressedSize()
				dstMessage.MessageGroupID = srcMessage.GetMessageGroupId()
				if len(srcMessage.GetMetadataItems()) > 0 {
					dstMessage.MetadataItems = make([]rawtopiccommon.MetadataItem, 0, len(srcMessage.GetMetadataItems()))
					for _, protoItem := range srcMessage.GetMetadataItems() {
						dstMessage.MetadataItems = append(dstMessage.MetadataItems, rawtopiccommon.MetadataItem{
							Key:   protoItem.GetKey(),
							Value: protoItem.GetValue()[:len(protoItem.GetValue()):len(protoItem.GetValue())],
						})
					}
				}
			}
		}
	}

	return nil
}

type PartitionData struct {
	PartitionSessionID PartitionSessionID

	Batches []Batch
}
type Batch struct {
	Codec rawtopiccommon.Codec

	ProducerID       string
	WriteSessionMeta map[string]string // nil if session meta is empty
	WrittenAt        time.Time

	MessageData []MessageData
}

type MessageData struct {
	Offset           rawtopiccommon.Offset
	SeqNo            int64
	CreatedAt        time.Time
	Data             []byte
	UncompressedSize int64
	MessageGroupID   string
	MetadataItems    []rawtopiccommon.MetadataItem
}

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	clientMessageImpl

	CommitOffsets []PartitionCommitOffset
}

func (r *CommitOffsetRequest) toProto() *Ydb_Topic.StreamReadMessage_CommitOffsetRequest {
	res := &Ydb_Topic.StreamReadMessage_CommitOffsetRequest{}
	res.CommitOffsets = make(
		[]*Ydb_Topic.StreamReadMessage_CommitOffsetRequest_PartitionCommitOffset,
		len(r.CommitOffsets),
	)

	for sessionIndex := range r.CommitOffsets {
		srcPartitionCommitOffset := &r.CommitOffsets[sessionIndex]
		dstCommitOffset := &Ydb_Topic.StreamReadMessage_CommitOffsetRequest_PartitionCommitOffset{
			PartitionSessionId: srcPartitionCommitOffset.PartitionSessionID.ToInt64(),
		}
		res.CommitOffsets[sessionIndex] = dstCommitOffset

		dstCommitOffset.Offsets = make([]*Ydb_Topic.OffsetsRange, len(srcPartitionCommitOffset.Offsets))
		for offsetIndex := range srcPartitionCommitOffset.Offsets {
			dstCommitOffset.Offsets[offsetIndex] = srcPartitionCommitOffset.Offsets[offsetIndex].ToProto()
		}
	}

	return res
}

type PartitionCommitOffset struct {
	PartitionSessionID PartitionSessionID
	Offsets            []rawtopiccommon.OffsetRange
}

type CommitOffsetResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionsCommittedOffsets []PartitionCommittedOffset
}

func (r *CommitOffsetResponse) fromProto(proto *Ydb_Topic.StreamReadMessage_CommitOffsetResponse) error {
	r.PartitionsCommittedOffsets = make([]PartitionCommittedOffset, len(proto.GetPartitionsCommittedOffsets()))
	for i := range r.PartitionsCommittedOffsets {
		srcCommitted := proto.GetPartitionsCommittedOffsets()[i]
		if srcCommitted == nil {
			return xerrors.WithStackTrace(errors.New("unexpected nil while parse commit offset response"))
		}
		dstCommitted := &r.PartitionsCommittedOffsets[i]

		dstCommitted.PartitionSessionID.FromInt64(srcCommitted.GetPartitionSessionId())
		dstCommitted.CommittedOffset.FromInt64(srcCommitted.GetCommittedOffset())
	}

	return nil
}

type PartitionCommittedOffset struct {
	PartitionSessionID PartitionSessionID
	CommittedOffset    rawtopiccommon.Offset
}

//
// PartitionSessionStatusRequest
//

type PartitionSessionStatusRequest struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}

func (r *PartitionSessionStatusRequest) toProto() *Ydb_Topic.StreamReadMessage_PartitionSessionStatusRequest {
	return &Ydb_Topic.StreamReadMessage_PartitionSessionStatusRequest{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
	}
}

type PartitionSessionStatusResponse struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionSessionID     PartitionSessionID
	PartitionOffsets       rawtopiccommon.OffsetRange
	WriteTimeHighWatermark time.Time
}

func (r *PartitionSessionStatusResponse) fromProto(
	proto *Ydb_Topic.StreamReadMessage_PartitionSessionStatusResponse,
) error {
	r.PartitionSessionID.FromInt64(proto.GetPartitionSessionId())
	if err := r.PartitionOffsets.FromProto(proto.GetPartitionOffsets()); err != nil {
		return err
	}
	r.WriteTimeHighWatermark = proto.GetWriteTimeHighWatermark().AsTime()

	return nil
}

//
// StartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionSession PartitionSession
	CommittedOffset  rawtopiccommon.Offset
	PartitionOffsets rawtopiccommon.OffsetRange
}

func (r *StartPartitionSessionRequest) fromProto(proto *Ydb_Topic.StreamReadMessage_StartPartitionSessionRequest) error {
	if proto == nil {
		return xerrors.WithStackTrace(errUnexpectedProtoNilStartPartitionSessionRequest)
	}

	if proto.GetPartitionSession() == nil {
		return xerrors.WithStackTrace(errUnexpectedNilPartitionSession)
	}
	r.PartitionSession.PartitionID = proto.GetPartitionSession().GetPartitionId()
	r.PartitionSession.Path = proto.GetPartitionSession().GetPath()
	r.PartitionSession.PartitionSessionID.FromInt64(proto.GetPartitionSession().GetPartitionSessionId())

	r.CommittedOffset.FromInt64(proto.GetCommittedOffset())

	return r.PartitionOffsets.FromProto(proto.GetPartitionOffsets())
}

type PartitionSession struct {
	PartitionSessionID PartitionSessionID
	Path               string // Topic path of partition
	PartitionID        int64
}

type StartPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
	ReadOffset         OptionalOffset
	CommitOffset       OptionalOffset
}

func (r *StartPartitionSessionResponse) toProto() *Ydb_Topic.StreamReadMessage_StartPartitionSessionResponse {
	res := &Ydb_Topic.StreamReadMessage_StartPartitionSessionResponse{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
		ReadOffset:         r.ReadOffset.ToInt64Pointer(),
		CommitOffset:       r.CommitOffset.ToInt64Pointer(),
	}

	return res
}

//
// StopPartitionSessionRequest
//

type StopPartitionSessionRequest struct {
	serverMessageImpl

	rawtopiccommon.ServerMessageMetadata

	PartitionSessionID PartitionSessionID
	Graceful           bool
	CommittedOffset    rawtopiccommon.Offset
}

func (r *StopPartitionSessionRequest) fromProto(proto *Ydb_Topic.StreamReadMessage_StopPartitionSessionRequest) error {
	if proto == nil {
		return xerrors.WithStackTrace(errUnexpectedGrpcNilStopPartitionSessionRequest)
	}
	r.PartitionSessionID.FromInt64(proto.GetPartitionSessionId())
	r.Graceful = proto.GetGraceful()
	r.CommittedOffset.FromInt64(proto.GetCommittedOffset())

	return nil
}

type StopPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}

func (r *StopPartitionSessionResponse) toProto() *Ydb_Topic.StreamReadMessage_StopPartitionSessionResponse {
	return &Ydb_Topic.StreamReadMessage_StopPartitionSessionResponse{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
	}
}
