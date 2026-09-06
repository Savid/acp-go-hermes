package hermesacp

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScratchParent(t *testing.T) {
	if got := scratchParent(""); got != os.TempDir() {
		t.Fatalf("scratchParent(\"\") = %q, want %q", got, os.TempDir())
	}

	if got := scratchParent("/custom/scratch"); got != "/custom/scratch" {
		t.Fatalf("scratchParent = %q, want /custom/scratch", got)
	}
}

func TestEnsureScratchParent(t *testing.T) {
	t.Run("empty resolves to system temp", func(t *testing.T) {
		got, err := ensureScratchParent("")
		if err != nil {
			t.Fatalf("ensureScratchParent(\"\"): %v", err)
		}
		if got != os.TempDir() {
			t.Fatalf("ensureScratchParent(\"\") = %q, want %q", got, os.TempDir())
		}
	})

	t.Run("missing nested dir created 0700", func(t *testing.T) {
		dir := filepath.Join(durableTempDir(t), "nested", "scratch")
		got, err := ensureScratchParent(dir)
		if err != nil {
			t.Fatalf("ensureScratchParent: %v", err)
		}
		if got != dir {
			t.Fatalf("ensureScratchParent = %q, want %q", got, dir)
		}
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat created dir: %v", err)
		}
		if !info.IsDir() {
			t.Fatalf("created scratch parent is not a directory")
		}
		if perm, want := info.Mode().Perm(), wantRestrictedPerm(true); perm != want {
			t.Fatalf("created scratch parent perm = %o, want %o", perm, want)
		}
	})

	t.Run("regular-file parent errors", func(t *testing.T) {
		file := filepath.Join(durableTempDir(t), "not-a-dir")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		child := filepath.Join(file, "scratch")
		if _, err := ensureScratchParent(child); err == nil {
			t.Fatal("ensureScratchParent accepted regular-file parent")
		}
	})
}
