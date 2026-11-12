package opa

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/open-policy-agent/opa/v1/sdk"

	"github.com/YOUR_GITHUB_USER/resilientbank/services/payments-api/internal/config"
)

// defaultOPAConfig is the minimal in-process OPA configuration used by the SDK client.
func newDefaultOPAConfig() *bytes.Reader {
	return bytes.NewReader([]byte(`{"services":{"default":{"url":"http://localhost:8181"}}}`))
}

// Client evaluates OPA policies that guard payment mutations.
type Client struct {
	opa      *sdk.OPA
	decision string
}

// New returns a payment mutation policy client. When OPA is disabled in the
// configuration a nil client is returned and callers treat all mutations as allowed.
func New(ctx context.Context, cfg *config.Config) (*Client, error) {
	if cfg == nil || !cfg.OPAEnabled {
		return nil, nil
	}
	opaClient, err := sdk.New(ctx, sdk.Options{
		ID:     "resilientbank-payment-mutations",
		Config: newDefaultOPAConfig(),
	})
	if err != nil {
		return nil, fmt.Errorf("opa client init: %w", err)
	}
	return &Client{
		opa:      opaClient,
		decision: cfg.OPAPolicyName,
	}, nil
}

// Allow reports whether the payment mutation passes the configured OPA policy.
func (c *Client) Allow(ctx context.Context, input map[string]any) (bool, string, error) {
	if c == nil || c.opa == nil {
		return true, "", nil
	}
	result, err := c.opa.Decision(ctx, sdk.DecisionOptions{
		Now:   time.Now(),
		Path:  c.decision,
		Input: input,
	})
	if err != nil {
		return false, "", fmt.Errorf("opa decision %s: %w", c.decision, err)
	}
	allowed, ok := result.Result.(bool)
	if !ok {
		return false, fmt.Sprintf("opa decision %s returned non-boolean result %#v", c.decision, result.Result), nil
	}
	return allowed, "", nil
}
