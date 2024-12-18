package query

import (
	"fmt"

	"github.com/rekby/fixenv"
	"go.uber.org/mock/gomock"
)

func SessionOverGrpcMock(testEnv fixenv.Env) *Session {
	fn := func() (*fixenv.GenericResult[*Session], error) {
		s := newTestSession(fmt.Sprintf("test-session-id-%v", testEnv.T().Name()))
		s.client = QueryGrpcMock(testEnv)

		return fixenv.NewGenericResult(s), nil
	}

	return fixenv.CacheResult(testEnv, fn)
}

func QueryGrpcMock(testEnv fixenv.Env) *MockQueryServiceClient {
	fn := func() (*fixenv.GenericResult[*MockQueryServiceClient], error) {
		m := NewMockQueryServiceClient(MockController(testEnv))

		return fixenv.NewGenericResult(m), nil
	}

	return fixenv.CacheResult(testEnv, fn)
}

func MockController(testEnv fixenv.Env) *gomock.Controller {
	fn := func() (*fixenv.GenericResult[*gomock.Controller], error) {
		mc := gomock.NewController(testEnv.T().(gomock.TestReporter))

		return fixenv.NewGenericResult(mc), nil
	}

	return fixenv.CacheResult(testEnv, fn)
}
