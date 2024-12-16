package query

import (
	"fmt"

	"github.com/rekby/fixenv"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

func TransactionOverGrpcMock(testEnv fixenv.Env) *Transaction {
	fn := func() (*fixenv.GenericResult[*Transaction], error) {
		return fixenv.NewGenericResult(&Transaction{
			LazyID: func() (id tx.LazyID) {
				id.SetTxID(fmt.Sprintf("test-transaction-id-%v", testEnv.T().Name()))

				return id
			}(),
			s: SessionOverGrpcMock(testEnv),
		}), nil
	}

	return fixenv.CacheResult(testEnv, fn)
}
