package hermesacp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateHermesSessionMeta(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateHermesSessionMeta(nil))
	require.NoError(t, ValidateHermesSessionMeta(NewHermesOptions(WithHermesEnv(map[string]string{"A": "1"})).Meta()))
	require.Error(t, ValidateHermesSessionMeta(map[string]any{"hermes": "x"}))
}
