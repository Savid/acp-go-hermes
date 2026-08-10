package hermes

import (
	"errors"
	"os/exec"
	"time"
)

// ordinaryContainmentDeadline bounds the teardown of the boundary ordinary mode
// owns. It matches the Darwin best-effort deadline: both are waiting on a
// directly-started child rather than on a proof.
const ordinaryContainmentDeadline = 5 * time.Second

// ordinaryChild is the started native child an ordinary boundary controls. The
// signalling is supplied by the platform so this launch path stays one function
// on every supported operating system.
type ordinaryChild struct {
	pid       int
	terminate func() error
	kill      func() error
}

// startOrdinaryProcess launches the native harness as the identity this adapter
// already runs as, root or non-root alike. It is reached only when the host
// omitted WithProcessIsolation, and it is deliberately the whole of what that
// omission selects: the command is started directly, with no supervisor
// interposed, no credential requested, and no authority consulted.
func startOrdinaryProcess(cmd *exec.Cmd) (*processContainment, error) {
	if cmd == nil || cmd.Path == "" || len(cmd.Args) == 0 {
		return nil, errors.New("hermes ordinary launch target is unavailable")
	}

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	direct := installDirectChildWait(cmd, false)

	return newOrdinaryContainment(ordinaryChild{
		pid:       cmd.Process.Pid,
		terminate: func() error { return terminateProcess(cmd) },
		kill:      func() error { return killProcess(cmd) },
	}, direct), nil
}
