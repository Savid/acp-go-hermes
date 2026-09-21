package hermes

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"

	"go.yaml.in/yaml/v3"

	"github.com/savid/acp-go-core/usage/gateway"
)

// GatewayRoutes lists the providers config.yaml under home declares with
// their own API base: the routes hermes sends requests through, each with the
// key the environment holds under the provider's key_env.
func GatewayRoutes(home string, lookup func(string) (string, bool)) ([]gateway.Route, error) {
	data, err := os.ReadFile(filepath.Join(home, "config.yaml"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("native config: %w", err)
	}

	var config struct {
		Providers map[string]struct {
			API    string `yaml:"api"`
			KeyEnv string `yaml:"key_env"` //nolint:tagliatelle // hermes uses this native spelling.
		} `yaml:"providers"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("native config: %w", err)
	}

	routes := make([]gateway.Route, 0, len(config.Providers))

	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for name := range config.Providers {
			if !yield(name) {
				return
			}
		}
	}) {
		provider := config.Providers[name]
		if provider.API == "" {
			continue
		}

		token := ""
		if provider.KeyEnv != "" {
			token, _ = lookup(provider.KeyEnv)
		}

		routes = append(routes, gateway.Route{Provider: name, BaseURL: provider.API, Token: token})
	}

	return routes, nil
}
