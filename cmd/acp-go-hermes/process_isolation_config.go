package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

const processIsolationConfigFlag = "process-isolation-config"

type processIsolationConfig struct {
	UID                uint32            `json:"uid"`
	GID                uint32            `json:"gid"`
	BaseEnvironment    map[string]string `json:"baseEnvironment"`
	InheritEnvironment []string          `json:"inheritEnvironment"`
}

var processIsolationConfigLoader = loadProcessIsolationConfig

func decodeProcessIsolationConfig(data []byte) (processIsolationConfig, error) {
	if err := rejectDuplicateJSONKeys(json.NewDecoder(bytes.NewReader(data))); err != nil {
		return processIsolationConfig{}, fmt.Errorf("decode policy: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var config processIsolationConfig
	if err := decoder.Decode(&config); err != nil {
		return processIsolationConfig{}, fmt.Errorf("decode policy: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return processIsolationConfig{}, fmt.Errorf("decode policy: trailing JSON value")
		}

		return processIsolationConfig{}, fmt.Errorf("decode policy: %w", err)
	}

	return config, nil
}

//nolint:wsl_v5 // The token parser keeps each structural check adjacent.
func rejectDuplicateJSONKeys(decoder *json.Decoder) error {
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON value")
		}

		return err
	}

	return nil
}

//nolint:wsl_v5 // The token parser keeps each structural check adjacent.
func scanJSONValue(decoder *json.Decoder) error {
	token, tokenErr := decoder.Token()
	if tokenErr != nil {
		return tokenErr
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}

	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return keyErr
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			seen[key] = struct{}{}
			if valueErr := scanJSONValue(decoder); valueErr != nil {
				return valueErr
			}
		}
		_, endErr := decoder.Token()

		return endErr
	case '[':
		for decoder.More() {
			if valueErr := scanJSONValue(decoder); valueErr != nil {
				return valueErr
			}
		}
		_, endErr := decoder.Token()

		return endErr
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}
