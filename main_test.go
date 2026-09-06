package hermesacp

import (
	"fmt"
	"os"
	"testing"
)

// TestMain gives the suite one temp root. A runtime generation's XDG root lands under
// os.TempDir until the Agent closes, and many tests never close the Agents
// they start, so the root removes on the way out whatever a test left behind.
// TMP and TEMP cover the Windows lookup.
func TestMain(m *testing.M) {
	suiteTemp, err := os.MkdirTemp("", "acp-go-hermes-suite-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create suite temp root:", err)
		os.Exit(1)
	}

	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		if err = os.Setenv(name, suiteTemp); err != nil {
			fmt.Fprintln(os.Stderr, "set suite "+name+":", err)
			os.Exit(1)
		}
	}

	code := m.Run()
	_ = os.RemoveAll(suiteTemp)
	os.Exit(code)
}
