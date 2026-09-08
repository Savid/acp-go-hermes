package hermesacp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/acp-go-sdk"
	"github.com/stretchr/testify/require"
)

const (
	rateLimitsFixtureKindRequest  = "request"
	rateLimitsFixtureKindResponse = "response"
)

const (
	rateLimitsFixtureTypeObject  = "object"
	rateLimitsFixtureTypeArray   = "array"
	rateLimitsFixtureTypeString  = "string"
	rateLimitsFixtureTypeBoolean = "boolean"
	rateLimitsFixtureTypeNumber  = "number"
	rateLimitsFixtureTypeInteger = "integer"
	rateLimitsFixturePools       = "pools"
	rateLimitsFixtureWindows     = "windows"
	rateLimitsFixtureBalances    = "balances"
	rateLimitsFixtureUsed        = "used"
	rateLimitsFixtureLimit       = "limit"
	rateLimitsFixtureRemaining   = "remaining"
	rateLimitsFixtureID          = "id"
	rateLimitsFixtureCurrency    = "currency"
)

type rateLimitsFixture struct {
	Name   string  `json:"name"`
	Raw    *string `json:"raw"`
	Expect struct {
		Valid      bool              `json:"valid"`
		Normalized RateLimitsRequest `json:"normalized"`
		Error      string            `json:"error"`
		Field      string            `json:"field"`
	} `json:"expect"`
}

func TestRateLimitsFixtures(t *testing.T) {
	t.Parallel()
	root := "testdata/rate-limits"
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	require.NoError(t, err)
	var manifest struct {
		Version  int               `json:"version"`
		Schemas  map[string]string `json:"schemas"`
		Fixtures []struct {
			Kind string `json:"kind"`
			File string `json:"file"`
		} `json:"fixtures"`
	}
	require.NoError(t, json.Unmarshal(raw, &manifest))
	require.Equal(t, 1, manifest.Version)
	require.NotEmpty(t, manifest.Fixtures)
	schemaBytes, err := os.ReadFile(filepath.Join(root, manifest.Schemas[rateLimitsFixtureKindResponse]))
	require.NoError(t, err)
	var schema rateLimitsFixtureSchema
	require.NoError(t, json.Unmarshal(schemaBytes, &schema))
	for _, entry := range manifest.Fixtures {
		t.Run(entry.File, func(t *testing.T) {
			t.Parallel()
			raw, err := os.ReadFile(filepath.Join(root, entry.File))
			require.NoError(t, err)
			var fixture rateLimitsFixture
			require.NoError(t, json.Unmarshal(raw, &fixture))
			var params json.RawMessage
			if fixture.Raw != nil {
				params = json.RawMessage(*fixture.Raw)
			}
			switch entry.Kind {
			case rateLimitsFixtureKindRequest:
				decoded, decodeErr := decodeRateLimitsRequest(params)
				agent := newRateLimitsFixtureAgent(t)
				result, callErr := agent.HandleExtensionMethod(t.Context(), RateLimitsMethod, params)
				if !fixture.Expect.Valid {
					requireRateLimitsRequestError(t, decodeErr, fixture.Expect.Error, fixture.Expect.Field)
					requireRateLimitsRequestError(t, callErr, fixture.Expect.Error, fixture.Expect.Field)

					return
				}
				require.NoError(t, decodeErr)
				require.Equal(t, fixture.Expect.Normalized, decoded)
				require.NoError(t, callErr)
				encoded, err := json.Marshal(result)
				require.NoError(t, err)
				require.Empty(t, rateLimitsFixtureResponseField(encoded, &schema))
				var response RateLimitsResponse
				require.NoError(t, json.Unmarshal(encoded, &response))
				if decoded.ProviderID != "" {
					require.Equal(t, decoded.ProviderID, response.ProviderID)
				}
			case rateLimitsFixtureKindResponse:
				field := rateLimitsFixtureResponseField(params, &schema)
				if !fixture.Expect.Valid {
					require.Equal(t, fixture.Expect.Field, field)

					return
				}
				require.Empty(t, field)
				var response RateLimitsResponse
				require.NoError(t, json.Unmarshal(params, &response))
				encoded, err := json.Marshal(response)
				require.NoError(t, err)
				require.Empty(t, rateLimitsFixtureResponseField(encoded, &schema))
				require.JSONEq(t, string(params), string(encoded))
			default:
				t.Fatalf("unknown fixture kind %q", entry.Kind)
			}
		})
	}
}

func requireRateLimitsRequestError(t *testing.T, err error, kind, field string) {
	t.Helper()
	var requestErr *acp.RequestError
	require.ErrorAs(t, err, &requestErr)
	require.Equal(t, -32602, requestErr.Code)
	raw, marshalErr := json.Marshal(requestErr.Data)
	require.NoError(t, marshalErr)
	expected, marshalErr := json.Marshal(map[string]string{rateLimitsFieldError: kind, rateLimitsFieldField: field})
	require.NoError(t, marshalErr)
	require.JSONEq(t, string(expected), string(raw))
}

func TestRateLimitsEmptyResults(t *testing.T) {
	t.Parallel()
	for _, response := range []RateLimitsResponse{
		rateLimitsUnsupported("provider"),
		rateLimitsUnavailable("provider", "not_observed"),
	} {
		encoded, err := json.Marshal(response)
		require.NoError(t, err)
		require.Contains(t, string(encoded), `"pools":[]`)
		require.NotContains(t, string(encoded), `null`)
	}
}

// This test-only evaluator executes the copied response schema. Production
// readers retain their own native decoding and normalized response mapping.
type rateLimitsFixtureSchema struct {
	Ref                  string                              `json:"$ref"`
	Defs                 map[string]*rateLimitsFixtureSchema `json:"$defs"`
	Type                 string                              `json:"type"`
	Properties           map[string]*rateLimitsFixtureSchema `json:"properties"`
	AdditionalProperties *bool                               `json:"additionalProperties"`
	Required             []string                            `json:"required"`
	Enum                 []any                               `json:"enum"`
	Const                json.RawMessage                     `json:"const"`
	Items                *rateLimitsFixtureSchema            `json:"items"`
	MinItems             *int                                `json:"minItems"`
	MaxItems             *int                                `json:"maxItems"`
	MinLength            *int                                `json:"minLength"`
	Minimum              *json.Number                        `json:"minimum"`
	Maximum              *json.Number                        `json:"maximum"`
	Pattern              string                              `json:"pattern"`
	Format               string                              `json:"format"`
	AllOf                []*rateLimitsFixtureSchema          `json:"allOf"`
	AnyOf                []*rateLimitsFixtureSchema          `json:"anyOf"`
	Not                  *rateLimitsFixtureSchema            `json:"not"`
	If                   *rateLimitsFixtureSchema            `json:"if"`
	Then                 *rateLimitsFixtureSchema            `json:"then"`
	Else                 *rateLimitsFixtureSchema            `json:"else"`
}

func rateLimitsFixtureResponseField(raw []byte, schema *rateLimitsFixtureSchema) string {
	if !json.Valid(raw) || !rateLimitsValidUnicode(raw) {
		return rateLimitsFixtureKindResponse
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, field := rateLimitsFixtureJSON(decoder, rateLimitsFixtureKindResponse)
	if field != "" {
		return field
	}
	if field := rateLimitsFixtureValidate(value, schema, schema, rateLimitsFixtureKindResponse); field != "" {
		return field
	}

	return rateLimitsFixtureCollections(value)
}

func rateLimitsFixtureJSON(decoder *json.Decoder, path string) (any, string) {
	token, err := decoder.Token()
	if err != nil {
		return nil, path
	}
	switch token {
	case json.Delim('{'):
		value := map[string]any{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, path
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, path
			}
			childPath := rateLimitsFixturePath(path, key)
			if _, duplicate := value[key]; duplicate {
				return nil, childPath
			}
			child, field := rateLimitsFixtureJSON(decoder, childPath)
			if field != "" {
				return nil, field
			}
			value[key] = child
		}
		if _, err := decoder.Token(); err != nil {
			return nil, path
		}

		return value, ""
	case json.Delim('['):
		value := []any{}
		for decoder.More() {
			child, field := rateLimitsFixtureJSON(decoder, fmt.Sprintf("%s[%d]", path, len(value)))
			if field != "" {
				return nil, field
			}
			value = append(value, child)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, path
		}

		return value, ""
	default:
		return token, ""
	}
}

func rateLimitsFixturePath(parent, child string) string {
	if parent == rateLimitsFixtureKindResponse {
		return child
	}

	return parent + "." + child
}

func rateLimitsFixtureValidate(value any, schema, document *rateLimitsFixtureSchema, path string) string {
	if schema == nil {
		return ""
	}
	if schema.Ref != "" {
		target := document.Defs[strings.TrimPrefix(schema.Ref, "#/$defs/")]
		if target == nil {
			return path
		}
		if field := rateLimitsFixtureValidate(value, target, document, path); field != "" {
			return field
		}
	}
	if schema.Type != "" && !rateLimitsFixtureType(value, schema.Type) {
		return path
	}
	if schema.Enum != nil && !slices.ContainsFunc(schema.Enum, func(want any) bool { return reflect.DeepEqual(value, want) }) {
		return path
	}
	if schema.Const != nil {
		var want any
		if err := json.Unmarshal(schema.Const, &want); err != nil || !reflect.DeepEqual(value, want) {
			return path
		}
	}
	if field := rateLimitsFixtureMembers(value, schema, document, path); field != "" {
		return field
	}
	if field := rateLimitsFixtureScalar(value, schema, path); field != "" {
		return field
	}
	for _, rule := range schema.AllOf {
		if field := rateLimitsFixtureValidate(value, rule, document, path); field != "" {
			return field
		}
	}
	matches := func(rule *rateLimitsFixtureSchema) bool {
		return rateLimitsFixtureValidate(value, rule, document, path) == ""
	}
	if len(schema.AnyOf) != 0 && !slices.ContainsFunc(schema.AnyOf, matches) {
		return path
	}
	if schema.Not != nil && matches(schema.Not) {
		object, _ := value.(map[string]any)
		rules := append([]*rateLimitsFixtureSchema{schema.Not}, schema.Not.AnyOf...)
		for _, rule := range rules {
			for _, key := range rule.Required {
				if _, present := object[key]; present {
					return rateLimitsFixturePath(path, key)
				}
			}
		}

		return path
	}
	if schema.If != nil {
		rule := schema.Else
		if matches(schema.If) {
			rule = schema.Then
		}
		if field := rateLimitsFixtureValidate(value, rule, document, path); field != "" {
			return field
		}
	}

	return ""
}

func rateLimitsFixtureType(value any, kind string) bool {
	switch kind {
	case rateLimitsFixtureTypeObject:
		_, ok := value.(map[string]any)

		return ok
	case rateLimitsFixtureTypeArray:
		_, ok := value.([]any)

		return ok
	case rateLimitsFixtureTypeString:
		_, ok := value.(string)

		return ok
	case rateLimitsFixtureTypeBoolean:
		_, ok := value.(bool)

		return ok
	case rateLimitsFixtureTypeNumber:
		_, ok := value.(json.Number)

		return ok
	case rateLimitsFixtureTypeInteger:
		number, ok := value.(json.Number)
		if !ok {
			return false
		}
		rat, ok := new(big.Rat).SetString(number.String())

		return ok && rat.IsInt()
	default:
		return false
	}
}

func rateLimitsFixtureMembers(value any, schema, document *rateLimitsFixtureSchema, path string) string {
	switch value := value.(type) {
	case map[string]any:
		keys := slices.Sorted(maps.Keys(value))
		if schema.AdditionalProperties != nil && !*schema.AdditionalProperties {
			for _, key := range keys {
				if _, found := schema.Properties[key]; !found {
					return rateLimitsFixturePath(path, key)
				}
			}
		}
		for _, key := range schema.Required {
			if _, found := value[key]; !found {
				return rateLimitsFixturePath(path, key)
			}
		}
		for _, key := range keys {
			if rule := schema.Properties[key]; rule != nil {
				if field := rateLimitsFixtureValidate(value[key], rule, document, rateLimitsFixturePath(path, key)); field != "" {
					return field
				}
			}
		}
	case []any:
		if schema.MinItems != nil && len(value) < *schema.MinItems || schema.MaxItems != nil && len(value) > *schema.MaxItems {
			return path
		}
		for index, item := range value {
			if field := rateLimitsFixtureValidate(item, schema.Items, document, fmt.Sprintf("%s[%d]", path, index)); field != "" {
				return field
			}
		}
	}

	return ""
}

func rateLimitsFixtureScalar(value any, schema *rateLimitsFixtureSchema, path string) string {
	switch value := value.(type) {
	case string:
		if schema.MinLength != nil && utf8.RuneCountInString(value) < *schema.MinLength {
			return path
		}
		if schema.Pattern != "" {
			// Go's regexp spells Unicode escapes differently from JSON Schema's ECMA syntax.
			escapes := regexp.MustCompile(`\\u([0-9a-fA-F]{4})`)
			pattern := escapes.ReplaceAllStringFunc(schema.Pattern, func(part string) string { return `\x{` + part[2:] + `}` })
			compiled, err := regexp.Compile(pattern)
			if err != nil || !compiled.MatchString(value) {
				return path
			}
		}
		if schema.Format == "date-time" {
			if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
				return path
			}
		}
	case json.Number:
		number, ok := new(big.Rat).SetString(value.String())
		if !ok {
			return path
		}
		if schema.Minimum != nil {
			bound, ok := new(big.Rat).SetString(schema.Minimum.String())
			if !ok || number.Cmp(bound) < 0 {
				return path
			}
		}
		if schema.Maximum != nil {
			bound, ok := new(big.Rat).SetString(schema.Maximum.String())
			if !ok || number.Cmp(bound) > 0 {
				return path
			}
		}
	}

	return ""
}

func rateLimitsFixtureCollections(value any) string {
	object, _ := value.(map[string]any)
	pools, _ := object[rateLimitsFixturePools].([]any)
	if field := rateLimitsFixtureDistinct(pools, rateLimitsFixturePools); field != "" {
		return field
	}
	for poolIndex, item := range pools {
		pool, _ := item.(map[string]any)
		path := fmt.Sprintf("pools[%d]", poolIndex)
		for _, name := range []string{rateLimitsFixtureWindows, rateLimitsFixtureBalances} {
			values, _ := pool[name].([]any)
			if field := rateLimitsFixtureDistinct(values, path+"."+name); field != "" {
				return field
			}
		}
		balances, _ := pool[rateLimitsFixtureBalances].([]any)
		for balanceIndex, item := range balances {
			balance, _ := item.(map[string]any)
			currency := ""
			for _, name := range []string{rateLimitsFixtureUsed, rateLimitsFixtureLimit, rateLimitsFixtureRemaining} {
				money, exists := balance[name].(map[string]any)
				if !exists {
					continue
				}
				current, _ := money[rateLimitsFixtureCurrency].(string)
				if currency != "" && currency != current {
					return fmt.Sprintf("%s.balances[%d].%s.currency", path, balanceIndex, name)
				}
				currency = current
			}
		}
	}

	return ""
}

func rateLimitsFixtureDistinct(items []any, path string) string {
	seen := map[string]bool{}
	for index, item := range items {
		object, _ := item.(map[string]any)
		id, _ := object[rateLimitsFixtureID].(string)
		if seen[id] {
			return fmt.Sprintf("%s[%d].id", path, index)
		}
		seen[id] = true
	}

	return ""
}
