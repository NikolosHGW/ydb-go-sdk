package otel

import (
	"net/url"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// table makes table.ClientTrace with solomon metrics publishing
//
//nolint:funlen
func table(cfg Config) (t trace.Table) { //nolint:gocyclo
	nodeID := func(sessionID string) string {
		u, err := url.Parse(sessionID)
		if err != nil {
			return ""
		}

		return u.Query().Get("node_id")
	}
	t.OnCreateSession = func(info trace.TableCreateSessionStartInfo) func(trace.TableCreateSessionDoneInfo) {
		if cfg.Details()&trace.TableEvents != 0 {
			fieldsStore := fieldsStoreFromContext(info.Context)
			*info.Context = withFunctionID(*info.Context, info.Call.FunctionID())

			return func(info trace.TableCreateSessionDoneInfo) {
				if info.Error == nil {
					fieldsStore.fields = append(fieldsStore.fields,
						kv.String("session_id", safeID(info.Session)),
						kv.String("session_status", safeStatus(info.Session)),
						kv.String("node_id", nodeID(safeID(info.Session))),
					)
				}
			}
		}

		return nil
	}
	t.OnDo = func(info trace.TableDoStartInfo) func(trace.TableDoDoneInfo) {
		if cfg.Details()&trace.TableEvents != 0 {
			*info.Context = noTraceRetry(*info.Context)
			operationName := info.Label
			if operationName == "" {
				operationName = info.Call.FunctionID()
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				operationName,
				kv.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.Msg("nested call", kv.Error(errNestedCall))
			}

			return func(info trace.TableDoDoneInfo) {
				fields := []KeyValue{
					kv.Int("attempts", info.Attempts),
				}
				if info.Error != nil {
					fields = append(fields, kv.Error(info.Error))
				}
				start.End(fields...)
			}
		}

		return nil
	}
	t.OnDoTx = func(info trace.TableDoTxStartInfo) func(trace.TableDoTxDoneInfo) {
		if cfg.Details()&trace.TableEvents != 0 {
			*info.Context = noTraceRetry(*info.Context)
			operationName := info.Label
			if operationName == "" {
				operationName = info.Call.FunctionID()
			}
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				operationName,
				kv.Bool("idempotent", info.Idempotent),
			)
			if info.NestedCall {
				start.Msg("nested call", kv.Error(errNestedCall))
			}

			return func(info trace.TableDoTxDoneInfo) {
				fields := []KeyValue{
					kv.Int("attempts", info.Attempts),
				}
				if info.Error != nil {
					fields = append(fields, kv.Error(info.Error))
				}
				start.End(fields...)
			}
		}

		return nil
	}
	t.OnSessionNew = func(info trace.TableSessionNewStartInfo) func(trace.TableSessionNewDoneInfo) {
		if cfg.Details()&trace.TableSessionLifeCycleEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.TableSessionNewDoneInfo) {
				finish(
					start,
					info.Error,
					kv.String("status", safeStatus(info.Session)),
					kv.String("node_id", nodeID(safeID(info.Session))),
					kv.String("session_id", safeID(info.Session)),
				)
			}
		}

		return nil
	}
	t.OnSessionDelete = func(info trace.TableSessionDeleteStartInfo) func(trace.TableSessionDeleteDoneInfo) {
		if cfg.Details()&trace.TableSessionLifeCycleEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TableSessionDeleteDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnSessionKeepAlive = func(info trace.TableKeepAliveStartInfo) func(trace.TableKeepAliveDoneInfo) {
		if cfg.Details()&trace.TableSessionLifeCycleEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TableKeepAliveDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnSessionBulkUpsert = func(info trace.TableSessionBulkUpsertStartInfo) func(trace.TableSessionBulkUpsertDoneInfo) {
		if cfg.Details()&trace.TableSessionQueryEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TableSessionBulkUpsertDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnSessionQueryPrepare = func(
		info trace.TablePrepareDataQueryStartInfo,
	) func(
		trace.TablePrepareDataQueryDoneInfo,
	) {
		if cfg.Details()&trace.TableSessionQueryInvokeEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", info.Query),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TablePrepareDataQueryDoneInfo) {
				finish(
					start,
					info.Error,
					kv.String("result", safeStringer(info.Result)),
				)
			}
		}

		return nil
	}
	t.OnSessionQueryExecute = func(
		info trace.TableExecuteDataQueryStartInfo,
	) func(
		trace.TableExecuteDataQueryDoneInfo,
	) {
		if cfg.Details()&trace.TableSessionQueryInvokeEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
				kv.String("query", safeStringer(info.Query)),
				kv.Bool("keep_in_cache", info.KeepInCache),
			)

			return func(info trace.TableExecuteDataQueryDoneInfo) {
				if info.Error == nil {
					finish(
						start,
						safeErr(info.Result),
						kv.Bool("prepared", info.Prepared),
						kv.String("transaction_id", safeID(info.Tx)),
					)
				} else {
					finish(
						start,
						info.Error,
					)
				}
			}
		}

		return nil
	}
	t.OnSessionQueryStreamExecute = func(
		info trace.TableSessionQueryStreamExecuteStartInfo,
	) func(
		trace.TableSessionQueryStreamExecuteDoneInfo,
	) {
		if cfg.Details()&trace.TableSessionQueryStreamEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("query", safeStringer(info.Query)),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TableSessionQueryStreamExecuteDoneInfo) {
				if info.Error != nil {
					start.End(kv.Error(info.Error))
				} else {
					start.End()
				}
			}
		}

		return nil
	}
	t.OnSessionQueryStreamRead = func(
		info trace.TableSessionQueryStreamReadStartInfo,
	) func(
		trace.TableSessionQueryStreamReadDoneInfo,
	) {
		if cfg.Details()&trace.TableSessionQueryStreamEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TableSessionQueryStreamReadDoneInfo) {
				if info.Error != nil {
					start.End(kv.Error(info.Error))
				} else {
					start.End()
				}
			}
		}

		return nil
	}
	t.OnTxBegin = func(info trace.TableTxBeginStartInfo) func(trace.TableTxBeginDoneInfo) {
		if cfg.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TableTxBeginDoneInfo) {
				finish(
					start,
					info.Error,
					kv.String("transaction_id", safeID(info.Tx)),
				)
			}
		}

		return nil
	}
	t.OnTxCommit = func(info trace.TableTxCommitStartInfo) func(trace.TableTxCommitDoneInfo) {
		if cfg.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.TableTxCommitDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnTxRollback = func(info trace.TableTxRollbackStartInfo) func(trace.TableTxRollbackDoneInfo) {
		if cfg.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
				kv.String("transaction_id", safeID(info.Tx)),
			)

			return func(info trace.TableTxRollbackDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnTxExecute = func(info trace.TableTransactionExecuteStartInfo) func(trace.TableTransactionExecuteDoneInfo) {
		if cfg.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
				kv.String("transaction_id", safeID(info.Tx)),
				kv.String("query", safeStringer(info.Query)),
			)

			return func(info trace.TableTransactionExecuteDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnTxExecuteStatement = func(
		info trace.TableTransactionExecuteStatementStartInfo,
	) func(
		info trace.TableTransactionExecuteStatementDoneInfo,
	) {
		if cfg.Details()&trace.TableSessionTransactionEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
				kv.String("transaction_id", safeID(info.Tx)),
				kv.String("query", safeStringer(info.StatementQuery)),
			)

			return func(info trace.TableTransactionExecuteStatementDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnInit = func(info trace.TableInitStartInfo) func(trace.TableInitDoneInfo) {
		if cfg.Details()&trace.TableEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.TableInitDoneInfo) {
				finish(
					start,
					nil,
					kv.Int("limit", info.Limit),
				)
			}
		}

		return nil
	}
	t.OnClose = func(info trace.TableCloseStartInfo) func(trace.TableCloseDoneInfo) {
		if cfg.Details()&trace.TableEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.TableCloseDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnPoolPut = func(info trace.TablePoolPutStartInfo) func(trace.TablePoolPutDoneInfo) {
		if cfg.Details()&trace.TablePoolAPIEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
				kv.String("node_id", nodeID(safeID(info.Session))),
				kv.String("session_id", safeID(info.Session)),
			)

			return func(info trace.TablePoolPutDoneInfo) {
				finish(start, info.Error)
			}
		}

		return nil
	}
	t.OnPoolGet = func(info trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
		if cfg.Details()&trace.TablePoolAPIEvents != 0 {
			start := childSpanWithReplaceCtx(
				cfg,
				info.Context,
				info.Call.FunctionID(),
			)

			return func(info trace.TablePoolGetDoneInfo) {
				finish(
					start,
					info.Error,
					kv.Int("attempts", info.Attempts),
					kv.String("status", safeStatus(info.Session)),
					kv.String("node_id", nodeID(safeID(info.Session))),
					kv.String("session_id", safeID(info.Session)),
				)
			}
		}

		return nil
	}

	return t
}
