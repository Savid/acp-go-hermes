//nolint:gosec,goconst,govet // Prefix is a key namespace, not a credential; config checks use narrow scopes.
package hermes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coder/acp-go-sdk"
	"gopkg.in/yaml.v3"
)

// The config transaction's own artifacts are adapter control material: official
// Hermes never reads them, and the control root is where control material lives
// so that none of it is placed beneath HERMES_HOME.
const (
	sharedConfigLockName        = "config.lock"
	sharedConfigFingerprintName = "config.sha256"
	sharedMCPSecretEnvPrefix    = "ACP_GO_HERMES_MCP_"
)

var (
	sharedConfigJSONMarshal = json.Marshal
	sharedConfigTryLock     = tryLockHermesFile
)

// materializeSharedHermesConfig serializes the adapter-owned config mutation
// across Agents and processes. The durable fingerprint makes a different MCP
// or seed configuration fail closed instead of replacing config beneath a live
// official Hermes process. It contains only a digest, never MCP environment
// values or seed contents.
func materializeSharedHermesConfig(ctx context.Context, home string, servers []acp.McpServer, files map[string]string) error {
	fingerprint, err := sharedHermesConfigFingerprint(servers, files)
	if err != nil {
		return err
	}

	return withHermesConfigLock(ctx, home, func(control string) error {
		if err := validateSharedHermesConfigFile(home); err != nil {
			return err
		}

		if err := validateSharedHermesDotEnv(home, files); err != nil {
			return err
		}

		if seeded, ok := files[hermesConfigFileName]; ok {
			if err := validateSharedHermesConfigBytes([]byte(seeded)); err != nil {
				return err
			}
		}

		fingerprintPath := filepath.Join(control, sharedConfigFingerprintName)
		current, readErr := os.ReadFile(fingerprintPath)

		fingerprintExists := readErr == nil
		switch {
		case readErr == nil:
			if string(current) != fingerprint {
				return errors.New("shared Hermes home is already bound to a different managed configuration")
			}
		case errors.Is(readErr, os.ErrNotExist):
		default:
			return fmt.Errorf("read shared Hermes config fingerprint: %w", readErr)
		}

		if err := materializeHermesConfigWithWriter(home, servers, files, sharedAtomicWriteFile); err != nil {
			return err
		}

		if err := validateSharedHermesConfigFile(home); err != nil {
			return err
		}

		if !fingerprintExists {
			if err := sharedAtomicWriteFile(fingerprintPath, []byte(fingerprint), 0o600); err != nil {
				return fmt.Errorf("commit shared Hermes config fingerprint: %w", err)
			}
		}

		return nil
	})
}

func validateSharedHermesDotEnv(home string, files map[string]string) error {
	check := func(data []byte) error {
		if bytes.Contains(bytes.ToUpper(data), []byte(sharedMCPSecretEnvPrefix)) {
			return errors.New("shared Hermes .env must not define or reference adapter-reserved MCP secret environment names")
		}

		return nil
	}

	data, err := os.ReadFile(filepath.Join(home, ".env"))
	if err == nil {
		if err := check(data); err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read shared Hermes .env: %w", err)
	}

	for relative, contents := range files {
		if strings.EqualFold(filepath.ToSlash(filepath.Clean(relative)), ".env") {
			if err := check([]byte(contents)); err != nil {
				return err
			}
		}
	}

	return nil
}

func validateSharedHermesConfigFile(home string) error {
	data, err := os.ReadFile(filepath.Join(home, hermesConfigFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("read shared Hermes config: %w", err)
	}

	return validateSharedHermesConfigBytes(data)
}

func validateSharedHermesConfigBytes(data []byte) error {
	var config map[string]any
	if err := yaml.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("parse shared Hermes config: %w", err)
	}

	for _, forbidden := range []struct {
		section string
		key     string
	}{
		{section: "dashboard", key: "turn_isolation"},
		{section: "model", key: "persist_switch_by_default"},
	} {
		section, ok := config[forbidden.section].(map[string]any)
		if !ok {
			continue
		}

		value, present := section[forbidden.key]
		if !present {
			continue
		}

		if disabled, ok := value.(bool); !ok || disabled {
			return fmt.Errorf("shared Hermes home requires %s.%s to be exactly false", forbidden.section, forbidden.key)
		}
	}

	return nil
}

func sharedHermesConfigFingerprint(servers []acp.McpServer, files map[string]string) (string, error) {
	payload, err := sharedConfigJSONMarshal(struct {
		Servers []acp.McpServer   `json:"servers"`
		Files   map[string]string `json:"files"`
	}{Servers: servers, Files: files})
	if err != nil {
		return "", fmt.Errorf("encode shared Hermes managed configuration: %w", err)
	}

	digest := sha256.Sum256(payload)

	return hex.EncodeToString(digest[:]), nil
}

func withHermesConfigLock(ctx context.Context, home string, run func(control string) error) (err error) {
	control, err := EnsureSharedHermesAdapterControlDir(home)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filepath.Join(control, sharedConfigLockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("open shared Hermes config lock: %w", err)
	}

	var unlock func() error

	for {
		var acquired bool

		unlock, acquired, err = sharedConfigTryLock(file)
		if err != nil {
			return errors.Join(err, file.Close())
		}

		if acquired {
			break
		}

		select {
		case <-ctx.Done():
			return errors.Join(fmt.Errorf("lock shared Hermes config: %w", context.Cause(ctx)), file.Close())
		case <-time.After(10 * time.Millisecond):
		}
	}

	defer func() {
		err = errors.Join(err, unlock(), file.Close())
	}()

	return run(control)
}
