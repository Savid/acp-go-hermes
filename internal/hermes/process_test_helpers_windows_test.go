//go:build windows

package hermes

func testProcessIsolation() *ProcessIsolation {
	return &ProcessIsolation{
		UID:                  1,
		GID:                  1,
		BaseEnvironment:      map[string]string{"PATH": `C:\Windows\System32`},
		TestOnlyNoCredential: true,
	}
}

func provedProcessContainment() *processContainment {
	return &processContainment{}
}
