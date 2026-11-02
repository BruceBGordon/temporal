package shard

import (
	"context"
	"testing"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence"
)

// TestWriteErrorRuleMatchesDeclaredRule: handleWriteErrorLocked restates the server's declared
// rule persistence.OperationPossiblySucceeded instead of calling it. Two copies of one rule drift
// apart silently, so this checks they agree: the shard re-acquires exactly when the declared rule
// says the write may have succeeded. The catalog is every error either rule names, plus the
// errors that fall to the default branch in practice.
func TestWriteErrorRuleMatchesDeclaredRule(t *testing.T) {
	errs := []error{
		&persistence.AppendHistoryTimeoutError{Msg: "x"},
		&persistence.CurrentWorkflowConditionFailedError{Msg: "x"},
		&persistence.WorkflowConditionFailedError{Msg: "x"},
		&persistence.WorkflowRunAlreadyExistsError{Msg: "x"},
		&persistence.ConditionFailedError{Msg: "x"},
		&persistence.InvalidPersistenceRequestError{Msg: "x"},
		&persistence.TransactionSizeLimitError{Msg: "x"},
		&persistence.ShardOwnershipLostError{Msg: "x"},
		&persistence.TimeoutError{Msg: "x"},
		&serviceerror.ResourceExhausted{Message: "x"},
		serviceerror.NewNotFound("x"),
		serviceerror.NewNamespaceNotFound("x"),
		serviceerror.NewUnavailable("x"),
		serviceerror.NewDeadlineExceeded("x"),
		serviceerror.NewInternal("x"),
		context.DeadlineExceeded,
	}
	for _, err := range errs {
		reacquires := classifyWriteError(err) == writeOutcomeUnknown
		if declared := persistence.OperationPossiblySucceeded(err); reacquires != declared {
			t.Errorf("%T: shard re-acquires=%v but persistence.OperationPossiblySucceeded=%v", err, reacquires, declared)
		}
	}
}
