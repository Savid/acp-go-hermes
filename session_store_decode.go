//nolint:wsl_v5 // Token validation keeps each member's ambiguity checks together.
package hermesacp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/coder/acp-go-sdk"
)

const jsonFieldFormat = "format"

// strictJSONShape describes the closed object vocabulary of one durable store
// value. A nil shape accepts any JSON value while still rejecting duplicate
// object members recursively. Dynamic admits arbitrary exact keys whose values
// all have the same shape (the session environment and archive-name maps).
type strictJSONShape struct {
	fields  map[string]*strictJSONShape
	dynamic *strictJSONShape
	element *strictJSONShape
}

var (
	strictJSONScalar = &strictJSONShape{}

	idmapJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		jsonFieldSessionID:      strictJSONScalar,
		hermesNativeIDMetaKey:   strictJSONScalar,
		"parentSessionId":       strictJSONScalar,
		"nativeParentSessionId": strictJSONScalar,
		jsonFieldFormat:         strictJSONScalar,
		"createdAtUnixMilli":    strictJSONScalar,
		"updatedAtUnixMilli":    strictJSONScalar,
	}}

	stateSnapshotModelJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		"providerID": strictJSONScalar,
		"modelID":    strictJSONScalar,
	}}
	stateSnapshotTerminalJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		"messageId": strictJSONScalar,
		"role":      strictJSONScalar,
		"finish":    strictJSONScalar,
	}}
	stateSnapshotForegroundJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		"streamId":            strictJSONScalar,
		"turnId":              strictJSONScalar,
		"outcome":             strictJSONScalar,
		"stopReason":          strictJSONScalar,
		"messageId":           strictJSONScalar,
		valText:               strictJSONScalar,
		"capturedAtUnixMilli": strictJSONScalar,
	}}
	stateSnapshotWrapperJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		"foreground": stateSnapshotForegroundJSONShape,
	}}
	archiveInfoJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		"subpath": strictJSONScalar,
		"sha256":  strictJSONScalar,
		"bytes":   strictJSONScalar,
	}}
	stateSnapshotSessionJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		jsonFieldSessionID:      strictJSONScalar,
		hermesNativeIDMetaKey:   strictJSONScalar,
		"parentSessionId":       strictJSONScalar,
		"nativeParentSessionId": strictJSONScalar,
		"cwd":                   strictJSONScalar,
		"title":                 strictJSONScalar,
		configModel:             stateSnapshotModelJSONShape,
		"env":                   {dynamic: strictJSONScalar},
		"extraPathDirs":         {element: strictJSONScalar},
	}}
	stateSnapshotJSONShape = &strictJSONShape{fields: map[string]*strictJSONShape{
		jsonFieldFormat:        strictJSONScalar,
		"capturedAtUnixMilli":  strictJSONScalar,
		capabilityScopeSession: stateSnapshotSessionJSONShape,
		valTerminal:            stateSnapshotTerminalJSONShape,
		"archives":             {dynamic: archiveInfoJSONShape},
		"wrapper":              stateSnapshotWrapperJSONShape,
	}}
)

// decodeStrictStoreJSON rejects ambiguity before decoding into a Go struct.
// encoding/json otherwise accepts duplicate members and case-insensitive field
// aliases, both of which would let two producers spell different durable state
// that hydrates to the same value.
func decodeStrictStoreJSON(raw []byte, destination any, shape *strictJSONShape) error {
	validator := json.NewDecoder(bytes.NewReader(raw))
	validator.UseNumber()
	if err := walkStrictJSONValue(validator, shape, "$"); err != nil {
		return err
	}
	if err := requireJSONEOF(validator); err != nil {
		return err
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}

	return requireJSONEOF(decoder)
}

func walkStrictJSONValue(decoder *json.Decoder, shape *strictJSONShape, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}

	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}

	if delimiter == '{' {
		seen := map[string]struct{}{}
		for decoder.More() {
			member, memberErr := decoder.Token()
			if memberErr != nil {
				return memberErr
			}
			// encoding/json returns every successfully decoded object member name as a string.
			key, _ := member.(string)

			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("%s contains duplicate field %q", path, key)
			}
			seen[key] = struct{}{}

			var memberShape *strictJSONShape
			if shape != nil {
				var allowed bool
				memberShape, allowed = shape.fields[key]
				if !allowed && shape.dynamic != nil {
					memberShape = shape.dynamic
					allowed = true
				}
				if !allowed {
					for allowedKey := range shape.fields {
						if strings.EqualFold(allowedKey, key) {
							return fmt.Errorf("%s field %q is a case alias of %q", path, key, allowedKey)
						}
					}

					return fmt.Errorf("%s contains unknown field %q", path, key)
				}
			}

			if walkErr := walkStrictJSONValue(decoder, memberShape, path+"."+key); walkErr != nil {
				return walkErr
			}
		}

		_, err = decoder.Token()

		return err
	}

	// A successful composite token at a JSON value boundary is either an object above or an array here.
	index := 0
	for decoder.More() {
		var elementShape *strictJSONShape
		if shape != nil {
			elementShape = shape.element
		}
		if walkErr := walkStrictJSONValue(decoder, elementShape, fmt.Sprintf("%s[%d]", path, index)); walkErr != nil {
			return walkErr
		}
		index++
	}

	_, err = decoder.Token()

	return err
}

// restoreFailedError marks a store entry that exists for the addressed session
// and cannot be replayed. It is the one durable-state failure a host can act on
// without adapter internals: the entry is neither deleted nor tombstoned, so a
// host can repair or drop it deliberately.
type restoreFailedError struct {
	cause error
}

func (e *restoreFailedError) Error() string {
	return "restore Hermes session state: " + e.cause.Error()
}

func (e *restoreFailedError) Unwrap() error { return e.cause }

func (e *restoreFailedError) requestError() *acp.RequestError {
	return acp.NewInternalError(map[string]any{jsonFieldError: valHermesRestoreFailed})
}

func restoreFailed(cause error) error {
	return &restoreFailedError{cause: cause}
}
