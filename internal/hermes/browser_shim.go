package hermes

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// ErrBrowserLaunchUncontained reports that a login cannot run because the
// process that would perform it is not shadowing the launchers it execs. It is
// the login leg's answer on a platform where no shim can contain the launch;
// every other leg of a session is unaffected, because none of them opens a
// browser.
var ErrBrowserLaunchUncontained = errors.New("hermes browser launch is not contained")

// browserShimPrefix names every shim directory created beneath a scratch
// parent.
const browserShimPrefix = "acp-go-hermes-browser-shim-"

const (
	browserShimPathEnv    = "PATH"
	browserShimBrowserEnv = "BROWSER"
)

// browserShimScript is what each shadowed launcher becomes: a program that
// accepts any arguments, opens nothing, and reports success. Launchers walk a
// candidate list and stop at the first success, so failing here would hand the
// URL to a candidate this directory does not shadow.
var browserShimScript = []byte("#!/bin/sh\nexit 0\n")

// browserLauncherNames are the programs a harness execs to open a URL. Darwin's
// `open` is the entry a BROWSER-only remedy misses: the launcher execs it
// directly and never reads BROWSER.
var browserLauncherNames = []string{
	"open",
	"xdg-open",
	"x-www-browser",
	"www-browser",
	"sensible-browser",
}

// browserShim is a scratch directory of no-op launchers that shadows every
// browser a native login leg would otherwise open on the operator's desktop.
type browserShim struct {
	dir string
}

// environ returns env with the shim ahead of PATH and BROWSER pointed at one of
// its no-ops. A nil shim leaves env alone: the platform has no shim to install,
// and the login leg refuses rather than the whole session.
func (s *browserShim) environ(env []string) []string {
	if s == nil {
		return env
	}

	return browserShimEnviron(env, s.dir)
}

// remove deletes the shim directory.
func (s *browserShim) remove() error {
	if s == nil {
		return nil
	}

	return os.RemoveAll(s.dir)
}

// browserShimEnviron rewrites a child environment so dir precedes every other
// PATH entry and BROWSER names a no-op inside it. Both mechanisms are set
// because harnesses split across them: one execs a launcher off PATH, another
// reads BROWSER, and neither alone covers both.
func browserShimEnviron(env []string, dir string) []string {
	kept := make([]string, 0, len(env)+2)
	search := dir

	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			kept = append(kept, entry)

			continue
		}

		switch key {
		case browserShimPathEnv:
			search = dir + string(os.PathListSeparator) + value
		case browserShimBrowserEnv:
		default:
			kept = append(kept, entry)
		}
	}

	return append(kept,
		browserShimPathEnv+"="+search,
		browserShimBrowserEnv+"="+browserShimCommand(dir),
	)
}

// browserShimCommand is the value BROWSER carries: an absolute no-op that
// ignores the URL it is handed.
func browserShimCommand(dir string) string {
	return filepath.Join(dir, browserLauncherNames[0])
}
