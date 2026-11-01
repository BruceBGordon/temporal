package matching

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
)

// Invariant: a brief Unavailable blip (a member of the declared transient-persistence group,
// common.IsPersistenceTransientError) that arrives while a backlog reader is already backing off
// for ResourceExhausted must not switch the reader to its fast retrier. Same failure shape as
// https://github.com/temporalio/temporal/issues/11547, found in matching by the same static check.

func retryDelayTestRetrier(initial time.Duration) backoff.Retrier {
	return backoff.NewRetrier(
		backoff.NewExponentialRetryPolicy(initial).WithExpirationInterval(backoff.NoInterval),
		clock.NewRealTimeSource(),
	)
}

var (
	persistenceOverloaded = serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "persistence limit")
	databaseBlip          = serviceerror.NewUnavailable("database failover")
)

func newRetryDelayFairReader() *fairTaskReader {
	return &fairTaskReader{
		retrier:         retryDelayTestRetrier(10 * time.Millisecond),
		throttleRetrier: retryDelayTestRetrier(2 * time.Second),
	}
}

func TestFairTaskReaderRetryDelay_BlipWhileThrottledKeepsThrottleBackoff(t *testing.T) {
	tr := newRetryDelayFairReader()
	require.GreaterOrEqual(t, tr.readRetryDelay(persistenceOverloaded), time.Second) // throttled
	require.GreaterOrEqual(t, tr.readRetryDelay(databaseBlip), time.Second)          // still throttled
}

// Control: with no overload, a blip keeps the fast retrier so ordinary recovery stays quick.
func TestFairTaskReaderRetryDelay_BlipWithoutOverloadUsesFastRetrier(t *testing.T) {
	tr := newRetryDelayFairReader()
	require.Less(t, tr.readRetryDelay(databaseBlip), time.Second)
	require.Less(t, tr.readRetryDelay(errors.New("boom")), time.Second)
}

func newRetryDelayPriReader() *priTaskReader {
	return &priTaskReader{retrier: retryDelayTestRetrier(10 * time.Millisecond)}
}

func TestPriTaskReaderRetryDelay_BlipWhileThrottledKeepsThrottleBackoff(t *testing.T) {
	tr := newRetryDelayPriReader()
	require.GreaterOrEqual(t, tr.readRetryDelay(persistenceOverloaded), taskReaderThrottleRetryDelay/2)
	require.GreaterOrEqual(t, tr.readRetryDelay(databaseBlip), taskReaderThrottleRetryDelay/2)
}

func TestPriTaskReaderRetryDelay_BlipWithoutOverloadUsesFastRetrier(t *testing.T) {
	tr := newRetryDelayPriReader()
	require.Less(t, tr.readRetryDelay(databaseBlip), time.Second)
}

func TestPriTaskReaderRetryDelay_ThrottleIsJittered(t *testing.T) {
	seen := map[time.Duration]bool{}
	for range 50 {
		tr := newRetryDelayPriReader()
		d := tr.readRetryDelay(persistenceOverloaded)
		require.GreaterOrEqual(t, d, taskReaderThrottleRetryDelay/2, "delay is not on the throttle path")
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "50 throttled readers all chose the same delay: retries are synchronized")
}
