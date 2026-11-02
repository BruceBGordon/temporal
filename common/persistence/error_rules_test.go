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
	name    string
	err     error
	timeout bool // the database timed out (whatever the error says)
}

// errorCatalog is what persistence and its callers really see. It covers every error the fault
// injector can produce (faultinjection/fault.go newFault), the timeouts each store returns
// (Cassandra: cassandra/gocql/errors.go ConvertError -> persistence.TimeoutError; SQL, the
// #11547 incident's store: a transaction wraps the context deadline with %w, sql/common.go
// convertSQLError, and a call outside a transaction formats it into an Unavailable with %v,
// e.g. sql/execution_tasks.go GetHistoryTasks), the rate limiter's error
// (persistence_rate_limited_clients.go), and the context errors.
func errorCatalog() []namedErr {
	return []namedErr{
		{"Unavailable", serviceerror.NewUnavailable("x"), false},
		{"ResourceExhausted(system, persistence limit)", persistence.ErrPersistenceSystemLimitExceeded, false},
		{"ResourceExhausted(system, overloaded)", &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_SYSTEM_OVERLOADED, Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, Message: "x"}, false},
		{"ResourceExhausted(namespace)", persistence.ErrPersistenceNamespaceLimitExceeded, false},
		{"persistence.TimeoutError (Cassandra timeout)", &persistence.TimeoutError{Msg: "x"}, true},
		{"SQL transaction timeout (wrapped deadline)", fmt.Errorf("UpdateShard: %w", context.DeadlineExceeded), true},
		{"SQL read timeout (Unavailable, cause formatted away)", serviceerror.NewUnavailablef("GetHistoryTasks operation failed. Select failed. Error: %v", context.DeadlineExceeded), true},
		{"persistence.AppendHistoryTimeoutError", &persistence.AppendHistoryTimeoutError{Msg: "x"}, true},
		{"serviceerror.DeadlineExceeded", serviceerror.NewDeadlineExceeded("x"), true},
		{"context.DeadlineExceeded", context.DeadlineExceeded, true},
		{"serviceerror.Canceled", serviceerror.NewCanceled("x"), false},
		{"context.Canceled", context.Canceled, false},
		{"DataLoss", serviceerror.NewDataLoss("x"), false},
		{"Internal", serviceerror.NewInternal("x"), false},
		{"NotFound", serviceerror.NewNotFound("x"), false},
		{"ConditionFailedError", &persistence.ConditionFailedError{Msg: "x"}, false},
		{"ShardOwnershipLostError", &persistence.ShardOwnershipLostError{Msg: "x"}, false},
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
	applies, holds func(c namedErr) bool
}

// on adapts a predicate on the error to a rule's signature.
func on(f func(error) bool) func(namedErr) bool { return func(c namedErr) bool { return f(c.err) } }

var rules = []rule{
	{
		id:        "same-name",
		statement: "Two functions named IsPersistenceTransientError mean the same thing",
		applies:   func(namedErr) bool { return true },
		holds: on(func(e error) bool {
			return common.IsPersistenceTransientError(e) == persistenceclient.IsPersistenceTransientError(e)
		}),
	},
	{
		id:        "unhealthy-is-throttled",
		statement: "An error the health signal counts as persistence being unhealthy is backed off by the queue retry paths",
		applies:   on(func(e error) bool { return persistence.IsUnhealthyErrorForTest(e) && !isCancellation(e) }),
		holds:     on(common.IsPersistenceTransientError),
	},
	{
		id:        "expensive-recovery-is-throttled",
		statement: "A load-related error that makes the shard re-acquire (ambiguous write) is backed off by the queue retry paths",
		applies: on(func(e error) bool {
			loadRelated := common.IsPersistenceTransientError(e) || persistence.IsUnhealthyErrorForTest(e)
			return persistence.OperationPossiblySucceeded(e) && loadRelated && !isCancellation(e)
		}),
		holds: on(common.IsPersistenceTransientError),
	},
	{
		id:        "helper-throttles-whole-group",
		statement: "backoff.ThrottleRetryContext gives every member of the transient group the throttle backoff it gives ResourceExhausted",
		applies:   on(common.IsPersistenceTransientError),
		holds:     on(throttledByRetryHelper),
	},
	{
		id:        "health-signal-sees-timeouts",
		statement: "A database timeout counts toward the health signal (it drives the adaptive persistence limiter)",
		applies:   func(c namedErr) bool { return c.timeout },
		holds:     on(persistence.IsUnhealthyErrorForTest),
	},
}

// knownGaps are the triaged findings: rule id + error name -> why it is listed.
var knownGaps = map[string]string{
	"same-name|ResourceExhausted(system, persistence limit)": "by design (an inner retry on overload would amplify it); listed so the difference is pinned and the shared name stops hiding it",
	"same-name|ResourceExhausted(system, overloaded)":        "by design (same as above)",
	"same-name|ResourceExhausted(namespace)":                 "by design (same as above)",
	"same-name|DataLoss":                                     "by design (factory.go comment: read-after-write DataLoss); pinned",

	"unhealthy-is-throttled|persistence.TimeoutError (Cassandra timeout)": "FINDING (hypothesis 4): counted unhealthy, but gets no throttle, and the executable resets its overload history on it",
	"unhealthy-is-throttled|persistence.AppendHistoryTimeoutError":        "candidate: counted unhealthy; not in the transient group",
	"unhealthy-is-throttled|serviceerror.DeadlineExceeded":                "candidate: counted unhealthy; not in the transient group",
	"unhealthy-is-throttled|context.DeadlineExceeded":                     "candidate: counted unhealthy; not in the transient group",

	"expensive-recovery-is-throttled|persistence.TimeoutError (Cassandra timeout)": "FINDING (hypothesis 4): each timeout can make the shard re-acquire and redo work, and nothing backs it off",
	"expensive-recovery-is-throttled|serviceerror.DeadlineExceeded":                "candidate: same mechanism if a deadline error reaches the shard",
	"expensive-recovery-is-throttled|context.DeadlineExceeded":                     "candidate: same mechanism if a deadline error reaches the shard",

	"unhealthy-is-throttled|SQL transaction timeout (wrapped deadline)":                 "FINDING (hypothesis 4 on SQL): a transaction timeout counts as unhealthy but is outside the transient group",
	"expensive-recovery-is-throttled|SQL transaction timeout (wrapped deadline)":        "FINDING (hypothesis 4 on SQL): a transaction timeout makes the shard re-acquire, with no throttle",
	"health-signal-sees-timeouts|SQL read timeout (Unavailable, cause formatted away)":  "FINDING: on SQL a read timeout becomes an Unavailable with the cause formatted away (%v), so the health signal, and the adaptive limiter it drives, cannot see it",
	"helper-throttles-whole-group|SQL read timeout (Unavailable, cause formatted away)": "same as Unavailable below",
	"helper-throttles-whole-group|Unavailable":                                          "candidate (design question): ThrottleRetryContext throttles only ResourceExhausted (its own TODO notes this); its callers are mostly RPC clients, where a fast retry on Unavailable may be intended",
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
			if !r.applies(c) {
				continue
			}
			applied++
			if !r.holds(c) {
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
// through. The SQL stores already wrap context deadline and cancel errors (convertSQLError); the
// catalog's SQL transaction timeout shows the effect. Other wrapping is latent.
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
