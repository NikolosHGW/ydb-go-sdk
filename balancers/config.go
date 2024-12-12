package balancers

import (
	"encoding/json"
	"fmt"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type balancerType string

const (
	typeRoundRobin   = balancerType("round_robin")
	typeRandomChoice = balancerType("random_choice")
	typeSingle       = balancerType("single")
	typeDisable      = balancerType("disable")
)

type preferType string

const (
	preferTypeNearestDC = preferType("nearest_dc")
	preferTypeLocations = preferType("locations")

	// Deprecated
	// Will be removed after March 2025.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	preferTypeLocalDC = preferType("local_dc")
)

type balancersConfig struct {
	Type      balancerType `json:"type"`
	Prefer    preferType   `json:"prefer,omitempty"`
	Fallback  bool         `json:"fallback,omitempty"`
	Locations []string     `json:"locations,omitempty"`
}

type fromConfigOptionsHolder struct {
	fallbackBalancer *balancerConfig.Config
	errorHandler     func(error)
}

type fromConfigOption func(h *fromConfigOptionsHolder)

func WithParseErrorFallbackBalancer(b *balancerConfig.Config) fromConfigOption {
	return func(h *fromConfigOptionsHolder) {
		h.fallbackBalancer = b
	}
}

func WithParseErrorHandler(errorHandler func(error)) fromConfigOption {
	return func(h *fromConfigOptionsHolder) {
		h.errorHandler = errorHandler
	}
}

func createByType(bType balancerType) (*balancerConfig.Config, error) {
	switch bType {
	case typeDisable:
		return SingleConn(), nil
	case typeSingle:
		return SingleConn(), nil
	case typeRandomChoice:
		return RandomChoice(), nil
	case typeRoundRobin:
		return RoundRobin(), nil
	default:
		return nil, xerrors.WithStackTrace(fmt.Errorf("unknown type of balancer: %s", bType))
	}
}

func CreateFromConfig(str string) (*balancerConfig.Config, error) {
	// try to parse s as identifier of balancer
	if c, err := createByType(balancerType(str)); err == nil {
		return c, nil
	}

	var (
		balancerCfg *balancerConfig.Config
		err         error
		cfgBalancer balancersConfig
	)

	// try to parse s as json
	if err = json.Unmarshal([]byte(str), &cfgBalancer); err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	balancerCfg, err = createByType(cfgBalancer.Type)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	switch cfgBalancer.Prefer {
	case preferTypeLocalDC:
		if cfgBalancer.Fallback {
			return PreferNearestDCWithFallBack(balancerCfg), nil
		}

		return PreferNearestDC(balancerCfg), nil
	case preferTypeNearestDC:
		if cfgBalancer.Fallback {
			return PreferNearestDCWithFallBack(balancerCfg), nil
		}

		return PreferNearestDC(balancerCfg), nil
	case preferTypeLocations:
		if len(cfgBalancer.Locations) == 0 {
			return nil, xerrors.WithStackTrace(fmt.Errorf("empty locations list in balancer '%s' config", cfgBalancer.Type))
		}
		if cfgBalancer.Fallback {
			return PreferLocationsWithFallback(balancerCfg, cfgBalancer.Locations...), nil
		}

		return PreferLocations(balancerCfg, cfgBalancer.Locations...), nil
	default:
		return balancerCfg, nil
	}
}

func FromConfig(config string, opts ...fromConfigOption) *balancerConfig.Config {
	var (
		holder = fromConfigOptionsHolder{
			fallbackBalancer: Default(),
		}
		balancerCfg *balancerConfig.Config
		err         error
	)
	for _, opt := range opts {
		if opt != nil {
			opt(&holder)
		}
	}

	balancerCfg, err = CreateFromConfig(config)
	if err != nil {
		if holder.errorHandler != nil {
			holder.errorHandler(err)
		}

		return holder.fallbackBalancer
	}

	return balancerCfg
}
