//go:build linux

package hermesacp

import (
	"errors"
	"strings"
	"testing"
)

func TestInspectHermesProcessReadBranches(t *testing.T) {
	oldReadFile := procReadFile
	t.Cleanup(func() { procReadFile = oldReadFile })

	if _, err := inspectHermesProcess(0); err == nil {
		t.Fatal("zero pid inspected successfully")
	}
	if _, err := procStartTime("1 (hermes"); err == nil {
		t.Fatal("malformed proc stat accepted")
	}
	if _, err := procStartTime("1 (hermes) S 0"); err == nil {
		t.Fatal("short proc stat accepted")
	}

	validStat := procStatWithStart("123")
	for _, tt := range []struct {
		name string
		read func(string) ([]byte, error)
		err  bool
	}{
		{
			name: "stat read error",
			read: func(path string) ([]byte, error) {
				if strings.HasSuffix(path, "/stat") {
					return nil, errors.New("stat failed")
				}
				return nil, nil
			},
			err: true,
		},
		{
			name: "stat parse error",
			read: func(path string) ([]byte, error) {
				if strings.HasSuffix(path, "/stat") {
					return []byte("malformed"), nil
				}
				return nil, nil
			},
			err: true,
		},
		{
			name: "cmdline read error",
			read: func(path string) ([]byte, error) {
				switch {
				case strings.HasSuffix(path, "/stat"):
					return []byte(validStat), nil
				case strings.HasSuffix(path, "/cmdline"):
					return nil, errors.New("cmdline failed")
				default:
					return nil, nil
				}
			},
			err: true,
		},
		{
			name: "env read error",
			read: func(path string) ([]byte, error) {
				switch {
				case strings.HasSuffix(path, "/stat"):
					return []byte(validStat), nil
				case strings.HasSuffix(path, "/cmdline"):
					return []byte("hermes\x00serve\x00"), nil
				case strings.HasSuffix(path, "/environ"):
					return nil, errors.New("env failed")
				default:
					return nil, nil
				}
			},
			err: true,
		},
		{
			name: "success",
			read: func(path string) ([]byte, error) {
				switch {
				case strings.HasSuffix(path, "/stat"):
					return []byte(validStat), nil
				case strings.HasSuffix(path, "/cmdline"):
					return []byte("hermes\x00serve\x00"), nil
				case strings.HasSuffix(path, "/environ"):
					return []byte("HERMES_HOME=/tmp/home\x00"), nil
				default:
					return nil, nil
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			procReadFile = tt.read
			identity, err := inspectHermesProcess(123)
			if tt.err {
				if err == nil {
					t.Fatal("inspect succeeded unexpectedly")
				}
				return
			}
			if err != nil || identity.StartTime != "123" || len(identity.Cmdline) != 2 || identity.Env["HERMES_HOME"] != "/tmp/home" {
				t.Fatalf("identity=%#v err=%v", identity, err)
			}
		})
	}
}

func procStatWithStart(start string) string {
	fields := make([]string, 20)
	for i := range fields {
		fields[i] = "0"
	}
	fields[0] = "S"
	fields[19] = start
	return "1 (hermes) " + strings.Join(fields, " ")
}
