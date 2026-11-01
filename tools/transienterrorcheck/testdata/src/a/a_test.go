package a

import (
	"time"

	tc "go.temporal.io/server/common"
)

// Test files are out of scope: no diagnostic expected.
func retryInTest(err error) time.Duration {
	if tc.IsResourceExhausted(err) {
		return time.Second
	}
	return 0
}
