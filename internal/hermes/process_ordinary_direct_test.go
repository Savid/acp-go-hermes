package hermes

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestOrdinaryDirectChildCompletionUsesObservedReaping proves completion is
// governed by the direct-child state the non-Unix ordinary boundary owns. In
// particular, Windows releases the process handle from Wait and reports a late
// Kill as EINVAL; neither a child reaped before completion nor one reaped while
// that kill races may turn an observed completion into a failure.
func TestOrdinaryDirectChildCompletionUsesObservedReaping(t *testing.T) {
	t.Run("already reaped", func(t *testing.T) {
		direct := &directChildWait{done: make(chan struct{})}
		close(direct.done)

		killCalls := 0
		err := completeOrdinaryDirectChild(ordinaryChild{kill: func() error {
			killCalls++

			return errors.New("late kill must not run")
		}}, direct, 0)

		require.NoError(t, err)
		require.Zero(t, killCalls)
	})

	t.Run("reaped while kill races", func(t *testing.T) {
		direct := &directChildWait{done: make(chan struct{})}
		lateKill := errors.New("late kill lost the race")

		err := completeOrdinaryDirectChild(ordinaryChild{kill: func() error {
			close(direct.done)

			return lateKill
		}}, direct, time.Second)

		require.NoError(t, err)
	})
}

func TestOrdinaryDirectChildCompletionReportsMissingOrUnreapedState(t *testing.T) {
	require.ErrorIs(t,
		completeOrdinaryDirectChild(ordinaryChild{kill: func() error { return nil }}, nil, time.Second),
		ErrProcessContainmentIncomplete,
	)

	direct := &directChildWait{done: make(chan struct{})}
	killErr := errors.New("kill failed")
	err := completeOrdinaryDirectChild(
		ordinaryChild{kill: func() error { return killErr }},
		direct,
		time.Nanosecond,
	)

	require.ErrorIs(t, err, killErr)
	require.ErrorIs(t, err, ErrProcessContainmentIncomplete)
}
