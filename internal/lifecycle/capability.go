package lifecycle

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// UnmarshalJSON strictly decodes the independently carried lifecycle answer.
// The empty object represents an omitted capability in canonical fixtures; any
// present answer carries the one exact integer version.
func (n *Negotiated) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return errors.New("lifecycle capability must be an object")
	}

	decoded := Negotiated{}
	seen := make(map[string]struct{})

	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("decode lifecycle capability member: %w", err)
		}

		field, ok := token.(string)
		if !ok {
			return errors.New("lifecycle capability member name must be a string")
		}

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

	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close lifecycle capability: %w", err)
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("lifecycle capability carries trailing input")
	}

	if len(seen) != 0 {
		if _, present := seen[fieldVersion]; !present {
			return errors.New("lifecycle capability version is missing")
		}
	}

	*n = decoded

	return nil
}
