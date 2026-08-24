//go:build unix && !darwin

package hermes

import "time"

// processContainment holds the boundary an authoritative backend establishes
// around one native root.
type processContainment struct {
	processGroupID    int
	terminateFn       func() error
	killFn            func() error
	proof             <-chan bool
	closeFn           func() error
	descendantCountFn func() (int, bool)
	treeVacantFn      func() (bool, bool)
	direct            *directChildWait
	completeFn        func(time.Duration) error
}
