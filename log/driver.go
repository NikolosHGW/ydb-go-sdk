package log

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/kv"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/secret"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// Driver makes trace.Driver with logging events from details
func Driver(l Logger, d trace.Detailer, opts ...Option) (t trace.Driver) {
	return internalDriver(wrapLogger(l, opts...), d)
}

//nolint:gocyclo,funlen
func internalDriver(l Logger, d trace.Detailer) trace.Driver {
	return trace.Driver{
		OnResolve: func(
			info trace.DriverResolveStartInfo,
		) func(
			trace.DriverResolveDoneInfo,
		) {
			if d.Details()&trace.DriverResolverEvents == 0 {
				return nil
			}
			ctx := with(context.Background(), TRACE, "ydb", "driver", "resolver", "update")
			target := info.Target
			addresses := info.Resolved
			l.Log(ctx, "start",
				kv.String("target", target),
				kv.Strings("resolved", addresses),
			)

			return func(info trace.DriverResolveDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.String("target", target),
						kv.Strings("resolved", addresses),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.String("target", target),
						kv.Strings("resolved", addresses),
						kv.Version(),
					)
				}
			}
		},
		OnInit: func(info trace.DriverInitStartInfo) func(trace.DriverInitDoneInfo) {
			if d.Details()&trace.DriverEvents == 0 {
				return nil
			}
			endpoint := info.Endpoint
			database := info.Database
			secure := info.Secure
			ctx := with(*info.Context, DEBUG, "ydb", "driver", "resolver", "init")
			l.Log(ctx, "start",
				kv.String("endpoint", endpoint),
				kv.String("database", database),
				kv.Bool("secure", secure),
			)
			start := time.Now()

			return func(info trace.DriverInitDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.String("endpoint", endpoint),
						kv.String("database", database),
						kv.Bool("secure", secure),
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						kv.Error(info.Error),
						kv.String("endpoint", endpoint),
						kv.String("database", database),
						kv.Bool("secure", secure),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnClose: func(info trace.DriverCloseStartInfo) func(trace.DriverCloseDoneInfo) {
			if d.Details()&trace.DriverEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "resolver", "close")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnDial: func(info trace.DriverConnDialStartInfo) func(trace.DriverConnDialDoneInfo) {
			if d.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "dial")
			endpoint := info.Endpoint
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
			)
			start := time.Now()

			return func(info trace.DriverConnDialDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Stringer("endpoint", endpoint),
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Stringer("endpoint", endpoint),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnStateChange: func(info trace.DriverConnStateChangeStartInfo) func(trace.DriverConnStateChangeDoneInfo) {
			if d.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			ctx := with(context.Background(), TRACE, "ydb", "driver", "conn", "state", "change")
			endpoint := info.Endpoint
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
				kv.Stringer("state", info.State),
			)
			start := time.Now()

			return func(info trace.DriverConnStateChangeDoneInfo) {
				l.Log(ctx, "done",
					kv.Stringer("endpoint", endpoint),
					kv.Latency(start),
					kv.Stringer("state", info.State),
				)
			}
		},
		OnConnClose: func(info trace.DriverConnCloseStartInfo) func(trace.DriverConnCloseDoneInfo) {
			if d.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "close")
			endpoint := info.Endpoint
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
			)
			start := time.Now()

			return func(info trace.DriverConnCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Stringer("endpoint", endpoint),
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Stringer("endpoint", endpoint),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnInvoke: func(info trace.DriverConnInvokeStartInfo) func(trace.DriverConnInvokeDoneInfo) {
			if d.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "invoke")
			endpoint := info.Endpoint
			method := string(info.Method)
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
				kv.String("method", method),
			)
			start := time.Now()

			return func(info trace.DriverConnInvokeDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Stringer("endpoint", endpoint),
						kv.String("method", method),
						kv.Latency(start),
						kv.Stringer("metadata", kv.Metadata(info.Metadata)),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Stringer("endpoint", endpoint),
						kv.String("method", method),
						kv.Latency(start),
						kv.Stringer("metadata", kv.Metadata(info.Metadata)),
						kv.Version(),
					)
				}
			}
		},
		OnConnNewStream: func(
			info trace.DriverConnNewStreamStartInfo,
		) func(
			trace.DriverConnNewStreamDoneInfo,
		) {
			if d.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "stream", "New")
			endpoint := info.Endpoint
			method := string(info.Method)
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
				kv.String("method", method),
			)
			start := time.Now()

			return func(info trace.DriverConnNewStreamDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Stringer("endpoint", endpoint),
						kv.String("method", method),
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Stringer("endpoint", endpoint),
						kv.String("method", method),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnStreamCloseSend: func(info trace.DriverConnStreamCloseSendStartInfo) func(
			trace.DriverConnStreamCloseSendDoneInfo,
		) {
			if d.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "stream", "CloseSend")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverConnStreamCloseSendDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnStreamSendMsg: func(info trace.DriverConnStreamSendMsgStartInfo) func(trace.DriverConnStreamSendMsgDoneInfo) {
			if d.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "stream", "SendMsg")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverConnStreamSendMsgDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnStreamRecvMsg: func(info trace.DriverConnStreamRecvMsgStartInfo) func(trace.DriverConnStreamRecvMsgDoneInfo) {
			if d.Details()&trace.DriverConnStreamEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "stream", "RecvMsg")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverConnStreamRecvMsgDoneInfo) {
				if xerrors.HideEOF(info.Error) == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnConnBan: func(info trace.DriverConnBanStartInfo) func(trace.DriverConnBanDoneInfo) {
			if d.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "ban")
			endpoint := info.Endpoint
			cause := info.Cause
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
				kv.NamedError("cause", cause),
			)
			start := time.Now()

			return func(info trace.DriverConnBanDoneInfo) {
				l.Log(WithLevel(ctx, WARN), "done",
					kv.Stringer("endpoint", endpoint),
					kv.Latency(start),
					kv.Stringer("state", info.State),
					kv.NamedError("cause", cause),
					kv.Version(),
				)
			}
		},
		OnConnAllow: func(info trace.DriverConnAllowStartInfo) func(trace.DriverConnAllowDoneInfo) {
			if d.Details()&trace.DriverConnEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "conn", "allow")
			endpoint := info.Endpoint
			l.Log(ctx, "start",
				kv.Stringer("endpoint", endpoint),
			)
			start := time.Now()

			return func(info trace.DriverConnAllowDoneInfo) {
				l.Log(ctx, "done",
					kv.Stringer("endpoint", endpoint),
					kv.Latency(start),
					kv.Stringer("state", info.State),
				)
			}
		},
		OnRepeaterWakeUp: func(info trace.DriverRepeaterWakeUpStartInfo) func(trace.DriverRepeaterWakeUpDoneInfo) {
			if d.Details()&trace.DriverRepeaterEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "repeater", "wake", "up")
			name := info.Name
			event := info.Event
			l.Log(ctx, "start",
				kv.String("name", name),
				kv.String("event", event),
			)
			start := time.Now()

			return func(info trace.DriverRepeaterWakeUpDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.String("name", name),
						kv.String("event", event),
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						kv.Error(info.Error),
						kv.String("name", name),
						kv.String("event", event),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnBalancerInit: func(info trace.DriverBalancerInitStartInfo) func(trace.DriverBalancerInitDoneInfo) {
			if d.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "init")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverBalancerInitDoneInfo) {
				l.Log(WithLevel(ctx, INFO), "done",
					kv.Latency(start),
				)
			}
		},
		OnBalancerClose: func(info trace.DriverBalancerCloseStartInfo) func(trace.DriverBalancerCloseDoneInfo) {
			if d.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "close")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverBalancerCloseDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
					)
				} else {
					l.Log(WithLevel(ctx, WARN), "failed",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnBalancerChooseEndpoint: func(
			info trace.DriverBalancerChooseEndpointStartInfo,
		) func(
			trace.DriverBalancerChooseEndpointDoneInfo,
		) {
			if d.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "choose", "endpoint")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverBalancerChooseEndpointDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
						kv.Stringer("endpoint", info.Endpoint),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "failed",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.Version(),
					)
				}
			}
		},
		OnBalancerUpdate: func(
			info trace.DriverBalancerUpdateStartInfo,
		) func(
			trace.DriverBalancerUpdateDoneInfo,
		) {
			if d.Details()&trace.DriverBalancerEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "balancer", "update")
			l.Log(ctx, "start",
				kv.Bool("needLocalDC", info.NeedLocalDC),
			)
			start := time.Now()

			return func(info trace.DriverBalancerUpdateDoneInfo) {
				l.Log(ctx, "done",
					kv.Latency(start),
					kv.Stringer("endpoints", kv.Endpoints(info.Endpoints)),
					kv.Stringer("added", kv.Endpoints(info.Added)),
					kv.Stringer("dropped", kv.Endpoints(info.Dropped)),
					kv.String("detectedLocalDC", info.LocalDC),
				)
			}
		},
		OnGetCredentials: func(info trace.DriverGetCredentialsStartInfo) func(trace.DriverGetCredentialsDoneInfo) {
			if d.Details()&trace.DriverCredentialsEvents == 0 {
				return nil
			}
			ctx := with(*info.Context, TRACE, "ydb", "driver", "credentials", "get")
			l.Log(ctx, "start")
			start := time.Now()

			return func(info trace.DriverGetCredentialsDoneInfo) {
				if info.Error == nil {
					l.Log(ctx, "done",
						kv.Latency(start),
						kv.String("token", secret.Token(info.Token)),
					)
				} else {
					l.Log(WithLevel(ctx, ERROR), "done",
						kv.Error(info.Error),
						kv.Latency(start),
						kv.String("token", secret.Token(info.Token)),
						kv.Version(),
					)
				}
			}
		},
	}
}
