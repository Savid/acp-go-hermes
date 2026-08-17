//go:build unix

package hermes

import "testing"

func TestProviderDescendantInventoryUnavailableOnUnix(t *testing.T) {
	var nilProcess *Process
	count, available := nilProcess.ProviderDescendantCount()
	if count != 0 || available {
		t.Fatalf("nil process inventory = (%d, %v)", count, available)
	}

	process := &Process{}
	count, available = process.ProviderDescendantCount()
	if count != 0 || available {
		t.Fatalf("process without containment inventory = (%d, %v)", count, available)
	}

	process.tree = &processContainment{processGroupID: 123}
	count, available = process.ProviderDescendantCount()
	if count != 0 || available {
		t.Fatalf("Unix process-group inventory = (%d, %v)", count, available)
	}

	var nilServer *hermesServer
	count, available = nilServer.ProviderDescendantCount()
	if count != 0 || available {
		t.Fatalf("nil server inventory = (%d, %v)", count, available)
	}

	server := &hermesServer{}
	count, available = server.ProviderDescendantCount()
	if count != 0 || available {
		t.Fatalf("server without process inventory = (%d, %v)", count, available)
	}

	server.process = process
	count, available = server.ProviderDescendantCount()
	if count != 0 || available {
		t.Fatalf("Unix server inventory = (%d, %v)", count, available)
	}
}
