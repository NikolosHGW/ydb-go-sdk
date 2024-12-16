package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

var _ baseTx.Transaction = &Transaction{}

func TestBegin(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "123",
			},
		}, nil)
		t.Log("begin")
		txID, err := begin(ctx, client, "123", query.TxSettings())
		require.NoError(t, err)
		require.Equal(t, "123", txID)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
		t.Log("begin")
		_, err := begin(ctx, client, "123", query.TxSettings())
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("begin")
		_, err := begin(ctx, client, "123", query.TxSettings())
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}

func TestCommitTx(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.CommitTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil,
		)
		t.Log("commit")
		err := commitTx(ctx, service, "123", "456")
		require.NoError(t, err)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			nil, grpcStatus.Error(grpcCodes.Unavailable, ""),
		)
		t.Log("commit")
		err := commitTx(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("commit")
		err := commitTx(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}

func TestTxOnCompleted(t *testing.T) {
	t.Run("OnCommitTxSuccess", func(t *testing.T) {
		envt := fixenv.New(t)

		QueryGrpcMock(envt).EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.CommitTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil,
		)

		tx := TransactionOverGrpcMock(envt)

		var completed []error
		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})
		err := tx.CommitTx(sf.Context(envt))
		require.NoError(t, err)
		require.Equal(t, []error{nil}, completed)
	})
	t.Run("OnCommitTxFailed", func(t *testing.T) {
		envt := fixenv.New(t)

		testError := errors.New("test-error")

		QueryGrpcMock(envt).EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil,
			testError,
		)

		tx := TransactionOverGrpcMock(envt)

		var completed []error
		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})
		err := tx.CommitTx(sf.Context(envt))
		require.ErrorIs(t, err, testError)
		require.Len(t, completed, 1)
		require.ErrorIs(t, completed[0], err)
	})
	t.Run("OnRollback", func(t *testing.T) {
		envt := fixenv.New(t)

		rollbackCalled := false
		QueryGrpcMock(envt).EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				ctx context.Context,
				request *Ydb_Query.RollbackTransactionRequest,
				option ...grpc.CallOption,
			) (
				*Ydb_Query.RollbackTransactionResponse,
				error,
			) {
				rollbackCalled = true

				return &Ydb_Query.RollbackTransactionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil
			})

		tx := TransactionOverGrpcMock(envt)
		var completed error

		tx.OnCompleted(func(transactionResult error) {
			// notification before call to the server
			require.False(t, rollbackCalled)
			completed = transactionResult
		})

		_ = tx.Rollback(sf.Context(envt))
		require.ErrorIs(t, completed, ErrTransactionRollingBack)
	})
	t.Run("OnExecWithoutCommitTxSuccess", func(t *testing.T) {
		envt := fixenv.New(t)

		responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
		responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		responseStream.EXPECT().Recv().Return(nil, io.EOF)

		QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

		tx := TransactionOverGrpcMock(envt)
		var completed []error

		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})

		err := tx.Exec(sf.Context(envt), "")
		require.NoError(t, err)
		require.Empty(t, completed)
	})
	t.Run("OnQueryWithoutCommitTxSuccess", func(t *testing.T) {
		envt := fixenv.New(t)

		responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
		responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)

		QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

		tx := TransactionOverGrpcMock(envt)
		var completed []error

		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})

		_, err := tx.Query(sf.Context(envt), "")
		require.NoError(t, err)
		require.Empty(t, completed)
	})
	t.Run("OnExecWithoutTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			envt := fixenv.New(t)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(envt)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			err := tx.Exec(sf.Context(envt), "")
			require.NoError(t, err)
			require.Empty(t, completed)
		})
	})
	t.Run("OnQueryWithoutTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(tb testing.TB) {
			envt := fixenv.New(tb)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(envt)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			res, err := tx.Query(sf.Context(envt), "")
			require.NoError(tb, err)
			_ = res.Close(sf.Context(envt))
			time.Sleep(time.Millisecond) // time for reaction for closing channel
			require.Empty(tb, completed)
		})
	})
	t.Run("OnExecWithTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(tb testing.TB) {
			envt := fixenv.New(tb)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(envt)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			err := tx.Exec(sf.Context(envt), "", options.WithCommit())
			require.NoError(tb, err)
			xtest.SpinWaitCondition(tb, &completedMutex, func() bool {
				return len(completed) != 0
			})
			require.Equal(tb, []error{nil}, completed)
		})
	})
	t.Run("OnQueryWithTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(tb testing.TB) {
			envt := fixenv.New(tb)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(envt)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			res, err := tx.Query(sf.Context(envt), "", options.WithCommit())
			_ = res.Close(sf.Context(envt))
			require.NoError(tb, err)
			xtest.SpinWaitCondition(tb, &completedMutex, func() bool {
				return len(completed) != 0
			})
			require.Equal(tb, []error{nil}, completed)
		})
	})
	t.Run("OnQueryWithTxSuccessWithTwoResultSet", func(t *testing.T) {
		xtest.TestManyTimes(t, func(tb testing.TB) {
			envt := fixenv.New(tb)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				ResultSetIndex: 0,
				ResultSet:      &Ydb.ResultSet{},
			}, nil)
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet:      &Ydb.ResultSet{},
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(envt)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			res, err := tx.Query(sf.Context(envt), "", options.WithCommit())
			require.NoError(tb, err)

			// time for event happened if is
			time.Sleep(time.Millisecond)
			require.Empty(tb, completed)

			_, err = res.NextResultSet(sf.Context(envt))
			require.NoError(tb, err)
			// time for event happened if is
			time.Sleep(time.Millisecond)
			require.Empty(tb, completed)

			_, err = res.NextResultSet(sf.Context(envt))
			require.NoError(tb, err)

			_ = res.Close(sf.Context(envt))
			require.NoError(tb, err)
			xtest.SpinWaitCondition(tb, &completedMutex, func() bool {
				return len(completed) != 0
			})
			require.Equal(tb, []error{nil}, completed)
		})
	})
	t.Run("OnExecuteFailedOnInitResponse", func(t *testing.T) {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprint("commit:", commit), func(t *testing.T) {
				envt := fixenv.New(t)

				testErr := errors.New("test")
				responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
				responseStream.EXPECT().Recv().Return(nil, testErr)

				QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

				tx := TransactionOverGrpcMock(envt)
				var transactionResult error

				tx.OnCompleted(func(err error) {
					transactionResult = err
				})

				err := tx.Exec(sf.Context(envt), "", query.WithCommit())
				require.ErrorIs(t, err, testErr)
				require.Error(t, transactionResult)
				require.ErrorIs(t, transactionResult, testErr)
			})
		}
	})
	t.Run("OnExecuteFailedInResponsePart", func(t *testing.T) {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprint("commit:", commit), func(t *testing.T) {
				xtest.TestManyTimes(t, func(tb testing.TB) {
					envt := fixenv.New(tb)

					errorReturned := false
					responseStream := NewMockQueryService_ExecuteQueryClient(MockController(envt))
					responseStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
						errorReturned = true

						return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
					})

					QueryGrpcMock(envt).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

					tx := TransactionOverGrpcMock(envt)
					var transactionResult error

					tx.OnCompleted(func(err error) {
						transactionResult = err
					})

					err := tx.Exec(sf.Context(envt), "", query.WithCommit())
					require.True(tb, errorReturned)
					require.True(tb, xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION))
					require.Error(tb, transactionResult)
					require.True(tb, xerrors.IsOperationError(transactionResult, Ydb.StatusIds_BAD_SESSION))
				})
			})
		}
	})
}

func TestRollback(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.RollbackTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil,
		)
		t.Log("rollback")
		err := rollback(ctx, service, "123", "456")
		require.NoError(t, err)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(
			nil, grpcStatus.Error(grpcCodes.Unavailable, ""),
		)
		t.Log("rollback")
		err := rollback(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("rollback")
		err := rollback(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}
