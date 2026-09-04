package lifecycle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// UnmarshalJSON strictly decodes the independently carried lifecycle answer.
func (n *Negotiated) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	// encoding/json validates the complete outer value before invoking this
	// method, so the opening token cannot fail.
	opening, _ := decoder.Token()
	if opening != json.Delim('{') {
		return errors.New("lifecycle capability must be an object")
	}

	decoded := Negotiated{}
	seen := make(map[string]struct{})

	for decoder.More() {
		// A validated object supplies only string member-name tokens.
		token, _ := decoder.Token()
		field, _ := token.(string)

		if _, duplicate := seen[field]; duplicate {
			return fmt.Errorf("duplicate lifecycle capability field %q", field)
		}

		seen[field] = struct{}{}

		switch field {
		case fieldVersion:
			var version json.RawMessage
			if err := decoder.Decode(&version); err != nil || !bytes.Equal(bytes.TrimSpace(version), []byte("1")) {
				return errors.New("lifecycle capability version must be exact integer 1")
			}

			decoded.Version = Version
		case fieldUpdatesOutsidePrompt:
			if err := decoder.Decode(&decoded.UpdatesOutsidePrompt); err != nil {
				return fmt.Errorf("decode lifecycle capability %s: %w", field, err)
			}
		case fieldAuthoritativeQuiescence:
			if err := decoder.Decode(&decoded.AuthoritativeQuiescence); err != nil {
				return fmt.Errorf("decode lifecycle capability %s: %w", field, err)
			}
		case fieldQuiescenceSource:
			if err := decoder.Decode(&decoded.QuiescenceSource); err != nil {
				return fmt.Errorf("decode lifecycle capability %s: %w", field, err)
			}
		case fieldActivityKinds:
			if err := decoder.Decode(&decoded.ActivityKinds); err != nil {
				return fmt.Errorf("decode lifecycle capability %s: %w", field, err)
			}
		default:
			return fmt.Errorf("unknown lifecycle capability field %q", field)
		}
	}

	// The validated outer object guarantees its closing delimiter and excludes
	// trailing input before UnmarshalJSON is called.
	_, _ = decoder.Token()

	if _, present := seen[fieldVersion]; !present {
		return errors.New("lifecycle capability version is missing")
	}

	*n = decoded

	return nil
}
