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

// completeOrdinaryDirectChild tears down the only process a non-Unix ordinary
// boundary can authoritatively observe. A waiter may have reaped the child
// before completion begins, or concurrently with the kill attempt; in either
// case the observed reap is the completion proof and a late signalling error
// cannot make the already-complete boundary fail. Windows makes this concrete:
// Wait releases the process handle, so a kill against the reaped process
// returns EINVAL rather than a done sentinel.
func completeOrdinaryDirectChild(process ordinaryChild, direct *directChildWait, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = ordinaryContainmentDeadline
	}

	if direct == nil {
		return ErrProcessContainmentIncomplete
	}

	select {
	case <-direct.done:
		return nil
	default:
	}

	killErr := process.kill()
	if waitErr := direct.awaitReaped(timeout); waitErr != nil {
		return errors.Join(killErr, waitErr)
	}

	return nil
}
