package queues_test

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/queues"
	"go.uber.org/mock/gomock"
)

// Invariant: the declared transient-persistence error group (common.IsPersistenceTransientError:
// ResourceExhausted and Unavailable) is handled as one group. In particular, a brief Unavailable
// blip (for example a database failover) must not erase the backoff a task has built up while
// persistence was overloaded. See https://github.com/temporalio/temporal/issues/11547.

func (s *executableSuite) failAttempts(executable queues.Executable, errs ...error) {
	for _, taskErr := range errs {
		s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{
			ExecutedAsActive: true,
			ExecutionErr:     taskErr,
		}).Times(1)
		err := executable.Execute()
		s.ErrorIs(err, taskErr) // the attempt really failed with the error we injected
		_ = executable.HandleErr(err)
	}
}

func persistenceOverloaded() error {
	return &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
		Message: "persistence limit",
	}
}

func (s *executableSuite) TestTaskNack_UnavailableAfterResourceExhausted_KeepsOverloadBackoff() {
	executable := s.newTestExecutable()
	unavailable := serviceerror.NewUnavailable("database failover")

	// Two attempts hit persistence overload, then one hits a brief blip.
	s.failAttempts(executable, persistenceOverloaded(), persistenceOverloaded(), unavailable)

	// The blip must not trigger an immediate resubmit...
	s.mockScheduler.EXPECT().TrySubmit(gomock.Any()).Times(0)
	// ...and the task must be rescheduled with the overload backoff, not the fast one.
	var rescheduledAt time.Time
	s.mockRescheduler.EXPECT().Add(executable, gomock.AssignableToTypeOf(time.Now())).
		Do(func(_ queues.Executable, t time.Time) { rescheduledAt = t }).Times(1)

	executable.Nack(unavailable)

	s.False(rescheduledAt.IsZero(), "rescheduler was never called")
	// Overload policy starts at 3s; the normal policy is about 1s at this attempt.
	s.GreaterOrEqual(rescheduledAt.Sub(s.timeSource.Now()), 2*time.Second)
}

// Control: with no overload history, an Unavailable error keeps today's fast resubmit path.
// The fix must stay narrow and not slow down ordinary retries.
func (s *executableSuite) TestTaskNack_UnavailableWithoutOverload_KeepsFastPath() {
	executable := s.newTestExecutable()
	unavailable := serviceerror.NewUnavailable("database failover")

	s.failAttempts(executable, unavailable)

	s.mockScheduler.EXPECT().TrySubmit(executable).Return(true).Times(1)
	executable.Nack(unavailable)
}
