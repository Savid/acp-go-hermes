package hermesacp

import (
	"context"
	"errors"
	"sync"

	"github.com/coder/acp-go-sdk"
)

// ExtensionNotificationHandler is the optional client surface that receives the
// agent's namespaced extension notifications. A directly embedded client that
// implements it receives RawEventMethod exactly as a JSON-RPC peer does; one
// that does not is never sent them.
type ExtensionNotificationHandler interface {
	ExtensionNotification(ctx context.Context, method string, params any) error
}

// elicitingClient is the optional client surface that answers elicitations. It
// is the SDK's own unstable client method, so a host that already implements
// the experimental client interface satisfies it without writing an adapter.
type elicitingClient interface {
	UnstableCreateElicitation(
		ctx context.Context,
		params acp.UnstableCreateElicitationRequest,
	) (acp.UnstableCreateElicitationResponse, error)
}

// errElicitationUnsupported refuses an elicitation the embedding client cannot
// answer. Refusing is the whole point: a dropped elicitation would leave the
// native turn waiting on an answer that can never arrive.
var errElicitationUnsupported = errors.New("embedded ACP client does not implement elicitation")

// errServeClientSupplied refuses the one combination that would install two
// clients on one agent.
var errServeClientSupplied = errors.New("Serve installs its own ACP client: build the agent with NewAgent to use WithClient")

// embeddedAgentClient adapts a host-supplied acp.Client to the outbound surface
// the agent publishes on.
//
// Every method mirrors what the JSON-RPC connection does for the same call, so
// an embedding host sees the same notifications, the same reserved `_meta`, and
// the same concurrency admission as a stdio peer. The one structural difference
// is delivery: an in-process call has no frame to wait for, so a registered
// caller's write signal resolves as soon as the request is handed to the
// client.
type embeddedAgentClient struct {
	agent  *Agent
	client acp.Client

	closeOnce sync.Once
	done      chan struct{}
}

func newEmbeddedAgentClient(agent *Agent, client acp.Client) *embeddedAgentClient {
	return &embeddedAgentClient{agent: agent, client: client, done: make(chan struct{})}
}

// Done reports the agent's own lifetime. An embedded client has no transport to
// lose, so the channel closes when the agent closes and never before.
func (c *embeddedAgentClient) Done() <-chan struct{} {
	return c.done
}

// CloseTransport is the agent close hook. It closes Done exactly once.
func (c *embeddedAgentClient) CloseTransport(context.Context) error {
	c.closeOnce.Do(func() { close(c.done) })

	return nil
}

func (c *embeddedAgentClient) SessionUpdate(ctx context.Context, params acp.SessionNotification) error {
	return c.client.SessionUpdate(ctx, params)
}

func (c *embeddedAgentClient) NotifyExtension(ctx context.Context, method string, params any) error {
	handler, ok := c.client.(ExtensionNotificationHandler)
	if !ok {
		return nil
	}

	return handler.ExtensionNotification(ctx, method, params)
}

func (c *embeddedAgentClient) RequestPermission(
	ctx context.Context,
	params acp.RequestPermissionRequest,
) (acp.RequestPermissionResponse, error) {
	return c.RequestPermissionRegistered(ctx, params, nil)
}

func (c *embeddedAgentClient) RequestPermissionRegistered(
	ctx context.Context,
	params acp.RequestPermissionRequest,
	written chan<- error,
) (acp.RequestPermissionResponse, error) {
	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		signalHostWrite(written, err)

		return acp.RequestPermissionResponse{}, err
	}
	defer release()

	signalHostWrite(written, nil)

	return c.client.RequestPermission(ctx, params)
}

func (c *embeddedAgentClient) UnstableCreateElicitation(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.CreateElicitation(ctx, params, elicitationScope{})
}

func (c *embeddedAgentClient) CreateElicitation(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (acp.UnstableCreateElicitationResponse, error) {
	return c.CreateElicitationRegistered(ctx, params, scope, nil)
}

func (c *embeddedAgentClient) CreateElicitationRegistered(
	ctx context.Context,
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
	written chan<- error,
) (acp.UnstableCreateElicitationResponse, error) {
	eliciting, ok := c.client.(elicitingClient)
	if !ok {
		signalHostWrite(written, errElicitationUnsupported)

		return acp.UnstableCreateElicitationResponse{}, errElicitationUnsupported
	}

	scoped, err := scopedElicitationRequest(params, scope)
	if err != nil {
		signalHostWrite(written, err)

		return acp.UnstableCreateElicitationResponse{}, err
	}

	release, err := c.agent.acquireClientCall(ctx)
	if err != nil {
		signalHostWrite(written, err)

		return acp.UnstableCreateElicitationResponse{}, err
	}
	defer release()

	signalHostWrite(written, nil)

	return eliciting.UnstableCreateElicitation(ctx, scoped)
}

// scopedElicitationRequest stamps the reserved route object onto the typed
// request the embedding client receives. It is the typed counterpart of the raw
// payload the JSON-RPC connection marshals, so both transports publish the same
// `_meta` for the same elicitation.
func scopedElicitationRequest(
	params acp.UnstableCreateElicitationRequest,
	scope elicitationScope,
) (acp.UnstableCreateElicitationRequest, error) {
	switch {
	case params.Form != nil:
		stamped, err := stampRouteMeta(params.Form.Meta, scope)
		if err != nil {
			return acp.UnstableCreateElicitationRequest{}, err
		}

		form := *params.Form
		form.Meta = stamped
		params.Form = &form
	case params.Url != nil:
		stamped, err := stampRouteMeta(params.Url.Meta, scope)
		if err != nil {
			return acp.UnstableCreateElicitationRequest{}, err
		}

		url := *params.Url
		url.Meta = stamped
		params.Url = &url
	default:
		return acp.UnstableCreateElicitationRequest{}, errors.New("elicitation request must include form or url")
	}

	return params, nil
}
