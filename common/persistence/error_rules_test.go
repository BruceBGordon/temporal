package persistence_test

// Error rules, checked against each other.
//
// The server classifies errors in many places, and each classifier is a declaration: "these
// errors are transient", "this write may have succeeded", "this error means persistence is
// unhealthy". Each one is tested on its own. This test checks them as a set: it runs the real
// classifiers on a catalog of the errors persistence really returns, builds the table, and
// checks rules that relate one declaration to another. A gap between two declarations is the
// kind of bug no single unit test can see.
//
// Every finding is triaged in knownGaps with a reason. The test fails on a new gap (a change
// that splits two declarations) and on a stale entry (a gap that was fixed but not removed),
// so the triage list cannot rot. Each rule must also have applied to at least one error, so a
// rule cannot pass by checking nothing.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
)

type namedErr struct {
	name string
	err  error
}

// errorCatalog is what persistence and its callers really see. It covers every error the fault
// injector can produce (faultinjection/fault.go newFault), the timeout the real stores return
// (cassandra/gocql/errors.go ConvertError -> persistence.TimeoutError), the rate limiter's error
// (persistence_rate_limited_clients.go), and the context errors.
func errorCatalog() []namedErr {
	return []namedErr{
		{"Unavailable", serviceerror.NewUnavailable("x")},
		{"ResourceExhausted(system, persistence limit)", persistence.ErrPersistenceSystemLimitExceeded},
		{"ResourceExhausted(system, overloaded)", &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, Message: "x"}},
		{"ResourceExhausted(namespace)", persistence.ErrPersistenceNamespaceLimitExceeded},
		{"persistence.TimeoutError", &persistence.TimeoutError{Msg: "x"}},
		{"persistence.AppendHistoryTimeoutError", &persistence.AppendHistoryTimeoutError{Msg: "x"}},
		{"serviceerror.DeadlineExceeded", serviceerror.NewDeadlineExceeded("x")},
		{"context.DeadlineExceeded", context.DeadlineExceeded},
		{"serviceerror.Canceled", serviceerror.NewCanceled("x")},
		{"context.Canceled", context.Canceled},
		{"DataLoss", serviceerror.NewDataLoss("x")},
		{"Internal", serviceerror.NewInternal("x")},
		{"NotFound", serviceerror.NewNotFound("x")},
		{"ConditionFailedError", &persistence.ConditionFailedError{Msg: "x"}},
		{"ShardOwnershipLostError", &persistence.ShardOwnershipLostError{Msg: "x"}},
	}
}

// throttledByRetryHelper runs the real backoff.ThrottleRetryContext and reports whether it gave
// err the throttle backoff (>= 1s) rather than the caller's policy (10ms here). The context
// deadline (300ms) makes the difference observable without waiting: a throttled error stops
// after one attempt because the next wait would pass the deadline.
func throttledByRetryHelper(err error) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	attempts := 0
	_ = backoff.ThrottleRetryContext(ctx, func(context.Context) error {
		attempts++
		return err
	}, backoff.NewConstantDelayRetryPolicy(10*time.Millisecond), func(error) bool { return true })
	return attempts == 1
}

// The declarations, each a real function in the server.
var predicates = []struct {
	name string
	f    func(error) bool
}{
	{"common.IsPersistenceTransientError (queue retry paths back off)", common.IsPersistenceTransientError},
	{"client.IsPersistenceTransientError (persistence client retries)", persistenceclient.IsPersistenceTransientError},
	{"persistence.OperationPossiblySucceeded (write ambiguous: shard re-acquires)", persistence.OperationPossiblySucceeded},
	{"persistence health signal: isUnhealthyError", persistence.IsUnhealthyErrorForTest},
	{"backoff.ThrottleRetryContext gives throttle backoff", throttledByRetryHelper},
	{"common.IsResourceExhausted", common.IsResourceExhausted},
	{"common.IsServiceTransientError", common.IsServiceTransientError},
	{"common.IsServiceClientTransientError", common.IsServiceClientTransientError},
	{"common.IsContextDeadlineExceededErr", common.IsContextDeadlineExceededErr},
}

func isCancellation(err error) bool { return common.IsContextCanceledErr(err) }

// rule relates two or more declarations. applies says whether the rule speaks about err;
// holds says whether the declarations agree about it.
type rule struct {
	id, statement  string
	applies, holds func(err error) bool
}

var rules = []rule{
	{
		id:        "same-name",
		statement: "Two functions named IsPersistenceTransientError mean the same thing",
		applies:   func(error) bool { return true },
		holds: func(e error) bool {
			return common.IsPersistenceTransientError(e) == persistenceclient.IsPersistenceTransientError(e)
		},
	},
	{
		id:        "unhealthy-is-throttled",
		statement: "An error the health signal counts as persistence being unhealthy is backed off by the queue retry paths",
		applies:   func(e error) bool { return persistence.IsUnhealthyErrorForTest(e) && !isCancellation(e) },
		holds:     common.IsPersistenceTransientError,
	},
	{
		id:        "expensive-recovery-is-throttled",
		statement: "A load-related error that makes the shard re-acquire (ambiguous write) is backed off by the queue retry paths",
		applies: func(e error) bool {
			loadRelated := common.IsPersistenceTransientError(e) || persistence.IsUnhealthyErrorForTest(e)
			return persistence.OperationPossiblySucceeded(e) && loadRelated && !isCancellation(e)
		},
		holds: common.IsPersistenceTransientError,
	},
	{
		id:        "helper-throttles-whole-group",
		statement: "backoff.ThrottleRetryContext gives every member of the transient group the throttle backoff it gives ResourceExhausted",
		applies:   common.IsPersistenceTransientError,
		holds:     throttledByRetryHelper,
	},
}

// knownGaps are the triaged findings: rule id + error name -> why it is listed.
var knownGaps = map[string]string{
	"same-name|ResourceExhausted(system, persistence limit)": "candidate: the client layer must not retry the limiter's own rejection; the shared name hides that the two groups differ",
	"same-name|ResourceExhausted(system, overloaded)":        "candidate: same as above",
	"same-name|ResourceExhausted(namespace)":                 "candidate: same as above",
	"same-name|DataLoss":                                     "candidate: client layer retries read-after-write DataLoss (comment in factory.go); queue paths do not",

	"unhealthy-is-throttled|persistence.TimeoutError":              "FINDING (hypothesis 4): the real store timeout counts as unhealthy but is retried without the throttle backoff",
	"unhealthy-is-throttled|persistence.AppendHistoryTimeoutError": "candidate: counted unhealthy; not in the transient group",
	"unhealthy-is-throttled|serviceerror.DeadlineExceeded":         "candidate: counted unhealthy; not in the transient group",
	"unhealthy-is-throttled|context.DeadlineExceeded":              "candidate: counted unhealthy; not in the transient group",

	"expensive-recovery-is-throttled|persistence.TimeoutError":      "FINDING (hypothesis 4): each timeout can make the shard re-acquire and redo work, and nothing backs it off",
	"expensive-recovery-is-throttled|serviceerror.DeadlineExceeded": "candidate: same mechanism if a deadline error reaches the shard",
	"expensive-recovery-is-throttled|context.DeadlineExceeded":      "candidate: same mechanism if a deadline error reaches the shard",

	"helper-throttles-whole-group|Unavailable": "FINDING (#11547 class, library level): ThrottleRetryContext throttles only ResourceExhausted; 450+ call sites inherit the split",
}

func TestErrorRulesAgree(t *testing.T) {
	cat := errorCatalog()

	// The table, so a reader can see what every declaration says about every error.
	var b strings.Builder
	fmt.Fprintf(&b, "\n%-46s", "error")
	for i := range predicates {
		fmt.Fprintf(&b, " P%d", i+1)
	}
	b.WriteString("\n")
	for _, c := range cat {
		fmt.Fprintf(&b, "%-46s", c.name)
		for _, p := range predicates {
			if p.f(c.err) {
				b.WriteString("  Y")
			} else {
				b.WriteString("  .")
			}
		}
		b.WriteString("\n")
	}
	for i, p := range predicates {
		fmt.Fprintf(&b, "P%d = %s\n", i+1, p.name)
	}
	t.Log(b.String())

	found := map[string]bool{}
	for _, r := range rules {
		applied := 0
		for _, c := range cat {
			if !r.applies(c.err) {
				continue
			}
			applied++
			if !r.holds(c.err) {
				found[r.id+"|"+c.name] = true
			}
		}
		if applied == 0 {
			t.Errorf("rule %q applied to no error in the catalog: it checks nothing", r.id)
		}
	}

	var keys []string
	for k := range found {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if why, ok := knownGaps[k]; ok {
			t.Logf("known gap  %-70s %s", k, why)
		} else {
			t.Errorf("NEW GAP    %s: two declarations disagree; triage it in knownGaps or fix it", k)
		}
	}
	for k := range knownGaps {
		if !found[k] {
			t.Errorf("STALE      %s: no longer a gap; remove it from knownGaps", k)
		}
	}
}

// TestErrorRulesUnderWrapping reports (does not fail) which declarations change their answer
// when an error is wrapped with %w. Type switches see only the outer type; errors.As sees
// through. Persistence paths do not wrap today, so this is latent: it matters the day one does.
func TestErrorRulesUnderWrapping(t *testing.T) {
	for _, p := range predicates {
		var changed []string
		for _, c := range errorCatalog() {
			if p.f(c.err) != p.f(fmt.Errorf("op: %w", c.err)) {
				changed = append(changed, c.name)
			}
		}
		if len(changed) > 0 {
			t.Logf("latent: %s changes its answer for wrapped %s", p.name, strings.Join(changed, ", "))
		}
	}
	if errors.Is(fmt.Errorf("op: %w", context.Canceled), context.Canceled) != true {
		t.Fatal("sanity: wrapping must preserve errors.Is")
	}
}
