package hermesacp

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const (
	routeMetaKey           = "acp-go.dev/route"
	routeVersion           = 1
	routeTurnNonceMaxBytes = 4 * 1024
	routeFieldVer          = "version"
	routeFieldID           = "sessionId"
	routeFieldTurn         = "turnNonce"
	routeFieldTool         = "toolCallId"
	routeFieldReq          = "requestId"
)

type inboundTurnRoute struct {
	turnNonce string
}

type turnRouteContextKey struct{}

var routeRandRead = rand.Read

// parseInboundTurnRoute reads the reserved route envelope a prompt or an active
// cancel must carry.
//
// The refusal splits into exactly two verdicts on one field path, because a host
// acts on them differently: an absent key is `missing` on the bare key path — the
// host forgot the envelope — and a present but unacceptable value is
// `unsupported` naming the member at fault, or the bare key path when the value
// is not an object at all.
func parseInboundTurnRoute(meta map[string]any) (inboundTurnRoute, error) {
	value, ok := meta[routeMetaKey]
	if !ok {
		return inboundTurnRoute{}, routeMissing()
	}

	object, ok := value.(map[string]any)
	if !ok {
		return inboundTurnRoute{}, routeUnsupported()
	}

	for key := range object {
		if key != routeFieldVer && key != routeFieldTurn {
			return inboundTurnRoute{}, routeUnsupported(key)
		}
	}

	if !routeVersionIsOne(object[routeFieldVer]) {
		return inboundTurnRoute{}, routeUnsupported(routeFieldVer)
	}

	nonce, ok := object[routeFieldTurn].(string)
	if !ok || strings.TrimSpace(nonce) == "" || len(nonce) > routeTurnNonceMaxBytes {
		return inboundTurnRoute{}, routeUnsupported(routeFieldTurn)
	}

	return inboundTurnRoute{turnNonce: nonce}, nil
}

func routeVersionIsOne(value any) bool {
	switch version := value.(type) {
	case int:
		return version == routeVersion
	case float64:
		return version == routeVersion
	default:
		return false
	}
}

// routeMetaPath is the request path a route refusal names, spelled the way the
// host wrote the key.
const routeMetaPath = `_meta["` + routeMetaKey + `"]`

func routeMissing() error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valMissing,
		jsonFieldField: routeMetaPath,
	})
}

// routeCorrelationError is one adapter-internal turn-correlation invariant
// failing closed: a stale nonce, a cycle that owns no route, a callback that crossed its
// owning cycle. None of them is a defect in a well-formed request the caller
// could restate, so none is invalid params; each is an unclassified internal
// failure whose reason stays on the Go error and off the wire.
type routeCorrelationError struct {
	reason string
}

func (e *routeCorrelationError) Error() string { return e.reason }

func (e *routeCorrelationError) requestError() *acp.RequestError {
	return acp.NewInternalError(map[string]any{
		jsonFieldError: valHermesInternalFailure,
		keyClass:       classRouteCorrelation,
	})
}

func routeInvalid(reason string) error {
	return &routeCorrelationError{reason: reason}
}

func routeUnsupported(members ...string) error {
	field := routeMetaPath
	for _, member := range members {
		field += "." + member
	}

	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: valUnsupported,
		jsonFieldField: field,
	})
}

func stampRouteMeta(meta map[string]any, scope elicitationScope) (map[string]any, error) {
	if scope.SessionID == "" || strings.TrimSpace(scope.TurnNonce) == "" {
		return nil, fmt.Errorf("route metadata requires sessionId and turnNonce")
	}

	if len(scope.TurnNonce) > routeTurnNonceMaxBytes {
		return nil, fmt.Errorf("route metadata turnNonce exceeds the maximum size")
	}

	if _, exists := meta[routeMetaKey]; exists {
		return nil, fmt.Errorf("reserved route metadata collision")
	}

	correlationCount := 0

	route := map[string]any{
		routeFieldVer:  routeVersion,
		routeFieldID:   scope.SessionID,
		routeFieldTurn: scope.TurnNonce,
	}
	if scope.ToolCallID != "" {
		correlationCount++
		route[routeFieldTool] = scope.ToolCallID
	}

	if scope.RequestID != nil && *scope.RequestID != "" {
		correlationCount++
		route[routeFieldReq] = *scope.RequestID
	}

	if correlationCount == 0 {
		requestID, err := newRouteRequestID()
		if err != nil {
			return nil, err
		}

		correlationCount++
		route[routeFieldReq] = requestID
	}

	if correlationCount != 1 {
		return nil, fmt.Errorf("route metadata requires exactly one callback correlation")
	}

	out := cloneAnyMap(meta)
	if out == nil {
		out = map[string]any{}
	}

	out[routeMetaKey] = route

	return out, nil
}

func newRouteRequestID() (string, error) {
	var data [16]byte
	if _, err := routeRandRead(data[:]); err != nil {
		return "", err
	}

	return hex.EncodeToString(data[:]), nil
}

func turnRouteMeta(turnNonce string) map[string]any {
	if strings.TrimSpace(turnNonce) == "" || len(turnNonce) > routeTurnNonceMaxBytes {
		return nil
	}

	return map[string]any{routeMetaKey: map[string]any{
		routeFieldVer:  routeVersion,
		routeFieldTurn: turnNonce,
	}}
}

func withTurnRoute(ctx context.Context, turnNonce string) context.Context {
	if strings.TrimSpace(turnNonce) == "" || len(turnNonce) > routeTurnNonceMaxBytes {
		turnNonce = ""
	}

	return context.WithValue(ctx, turnRouteContextKey{}, turnNonce)
}

func turnRouteMetaFromContext(ctx context.Context) map[string]any {
	turnNonce := turnNonceFromContext(ctx)
	if turnNonce == "" {
		return nil
	}

	return turnRouteMeta(turnNonce)
}

func turnNonceFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	turnNonce, _ := ctx.Value(turnRouteContextKey{}).(string)

	return turnNonce
}
