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
	routeMetaKey   = "acp-go.dev/route"
	routeVersion   = 1
	routeFieldVer  = "version"
	routeFieldID   = "sessionId"
	routeFieldTurn = "turnNonce"
	routeFieldTool = "toolCallId"
	routeFieldReq  = "requestId"
)

type inboundTurnRoute struct {
	turnNonce string
}

type turnRouteContextKey struct{}

var routeRandRead = rand.Read

func parseInboundTurnRoute(meta map[string]any) (inboundTurnRoute, error) {
	value, ok := meta[routeMetaKey]
	if !ok {
		return inboundTurnRoute{}, routeInvalid("missing reserved route metadata")
	}

	object, ok := value.(map[string]any)
	if !ok || len(object) != 2 {
		return inboundTurnRoute{}, routeInvalid("route metadata must contain exactly version and turnNonce")
	}

	if !routeVersionIsOne(object[routeFieldVer]) {
		return inboundTurnRoute{}, routeInvalid("unsupported route metadata version")
	}

	nonce, ok := object[routeFieldTurn].(string)
	if !ok || strings.TrimSpace(nonce) == "" {
		return inboundTurnRoute{}, routeInvalid("route turnNonce is required")
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

func routeInvalid(message string) error {
	return acp.NewInvalidParams(map[string]any{
		jsonFieldError: message,
		keyField:       routeMetaKey,
	})
}

func stampRouteMeta(meta map[string]any, scope elicitationScope) (map[string]any, error) {
	if scope.SessionID == "" || strings.TrimSpace(scope.TurnNonce) == "" {
		return nil, fmt.Errorf("route metadata requires sessionId and turnNonce")
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
	return map[string]any{routeMetaKey: map[string]any{
		routeFieldVer:  routeVersion,
		routeFieldTurn: turnNonce,
	}}
}

func withTurnRoute(ctx context.Context, turnNonce string) context.Context {
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
