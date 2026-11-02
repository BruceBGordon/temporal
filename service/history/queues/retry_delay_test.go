package queues

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

// Invariant: every member of the declared transient-persistence error group
// (common.IsPersistenceTransientError: ResourceExhausted and Unavailable) gets the protective
// throttle delay, and throttle delays are jittered so shards do not retry in lockstep.
// See https://github.com/temporalio/temporal/issues/11547.

func transientPersistenceErrors() map[string]error {
	return map[string]error{
		"ResourceExhausted": serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "persistence limit"),
		"Unavailable":       serviceerror.NewUnavailable("database failover"),
	}
}

func fastRetrier() backoff.Retrier {
	return backoff.NewRetrier(
		backoff.NewExponentialRetryPolicy(10*time.Millisecond).WithExpirationInterval(backoff.NoInterval),
		clock.NewRealTimeSource(),
	)
}

func TestReaderRetryDelay_TransientPersistenceErrorsAreThrottled(t *testing.T) {
	for name, err := range transientPersistenceErrors() {
		t.Run(name, func(t *testing.T) {
			require.GreaterOrEqual(t, readerRetryDelay(err, fastRetrier()), throttleRetryDelay/2)
		})
	}
}

// Control: errors outside the group keep the fast retrier.
func TestReaderRetryDelay_OtherErrorsUseFastRetrier(t *testing.T) {
	require.Less(t, readerRetryDelay(errors.New("boom"), fastRetrier()), time.Second)
}

// Uses ResourceExhausted, which takes the throttle path on every version of the code, so this
// measures the throttle delay itself rather than whichever path an error happens to take.
func TestReaderRetryDelay_ThrottleIsJittered(t *testing.T) {
	requireJittered(t, throttleRetryDelay/2, func() time.Duration {
		return readerRetryDelay(transientPersistenceErrors()["ResourceExhausted"], fastRetrier())
	})
}

func TestLookAheadRetryTime_TransientPersistenceErrorsAreThrottled(t *testing.T) {
	now := time.Now()
	fastPath := now.Add(time.Second) // ~TimerProcessorMaxTimeShift
	for name, err := range transientPersistenceErrors() {
		t.Run(name, func(t *testing.T) {
			require.GreaterOrEqual(t, lookAheadRetryTime(err, now, fastPath).Sub(now), lookAheadRateLimitDelay/2)
		})
	}
}

func TestLookAheadRetryTime_OtherErrorsUseFastPath(t *testing.T) {
	now := time.Now()
	fastPath := now.Add(time.Second)
	require.Equal(t, fastPath, lookAheadRetryTime(errors.New("boom"), now, fastPath))
}

func TestLookAheadRetryTime_ThrottleIsJittered(t *testing.T) {
	now := time.Now()
	requireJittered(t, lookAheadRateLimitDelay/2, func() time.Duration {
		return lookAheadRetryTime(transientPersistenceErrors()["ResourceExhausted"], now, now.Add(time.Second)).Sub(now)
	})
}

// requireJittered fails if many calls all return the same delay, which means every shard
// would retry at the same instant after a shared blip. It also checks every delay is on the
// throttle path (at least minDelay), so variation from some other, faster path cannot make
// the test pass by accident.
func requireJittered(t *testing.T, minDelay time.Duration, delay func() time.Duration) {
	t.Helper()
	seen := map[time.Duration]bool{}
	for range 50 {
		d := delay()
		require.GreaterOrEqual(t, d, minDelay, "delay is not on the throttle path")
		seen[d] = true
	}
	require.Greater(t, len(seen), 1, "50 retries all chose the same delay: retries are synchronized")
}
