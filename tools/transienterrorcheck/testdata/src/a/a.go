package a

import (
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	tc "go.temporal.io/server/common"
)

const throttle = 3 * time.Second

// The bug shape: overload gets the long delay, a blip falls through to the fast path.
func splitDelay(err error) time.Duration {
	if tc.IsResourceExhausted(err) { // want `chooses a delay with it but never considers serviceerror.Unavailable`
		return throttle
	}
	return 10 * time.Millisecond
}

// Protection that is not a delay (skip an immediate resubmit) is still caught.
func splitResubmit(err error, count int) bool {
	if !errors.Is(err, errBusy) && tc.IsResourceExhausted(err) && count > 1 { // want `branches on it but never considers`
		return false
	}
	return true
}

// Whole group handled: fine.
func wholeGroup(err error) time.Duration {
	if tc.IsPersistenceTransientError(err) {
		return throttle
	}
	return 10 * time.Millisecond
}

// Overload-aware with the other member considered: fine.
func considersUnavailable(err error, throttled bool) time.Duration {
	if tc.IsResourceExhausted(err) {
		return throttle
	}
	var u *serviceerror.Unavailable
	if throttled && errors.As(err, &u) {
		return throttle
	}
	return 10 * time.Millisecond
}

// Reviewed exception with a reason: fine.
func logOnly(err error) bool {
	//transienterror:ignore decides what to log, not how to retry
	if tc.IsResourceExhausted(err) {
		return false
	}
	return true
}

// Exception without a reason is reported, and still suppresses.
func noReason(err error) bool {
	/* want `needs a reason` */ //transienterror:ignore
	if tc.IsResourceExhausted(err) {
		return false
	}
	return true
}

// Exception that suppresses nothing is reported, so the list cannot rot.
func stale(err error) bool {
	/* want `does not suppress anything` */ //transienterror:ignore left over from old code
	return err != nil
}

// A local function that merely shares the name is not the common one: fine.
func IsResourceExhausted(err error) bool { return false }

func sameNameElsewhere(err error) time.Duration {
	if IsResourceExhausted(err) {
		return throttle
	}
	return 0
}

// Function literals are checked on their own.
func closure() func(error) time.Duration {
	return func(err error) time.Duration {
		if tc.IsResourceExhausted(err) { // want `chooses a delay with it`
			return throttle
		}
		return 0
	}
}

var errBusy = errors.New("busy")
