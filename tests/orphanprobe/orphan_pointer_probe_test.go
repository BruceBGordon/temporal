package orphanprobe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

// Issue #10841: a current-execution pointer whose run has no mutable state. The existing
// SignalWithStartOrphanPointerSuite covers a completed run on Cassandra and skips SQL ("SQL
// surfaces this as a unique constraint, not a hang"). This suite pins what SQL users get, for a
// run that completed and for one deleted while running, on every start path.
//
// Known gap on SQL (a ledger entry, with its reason): lockCurrentExecutionIfExists locks the
// current row through an INNER JOIN with executions (LockCurrentExecutionsJoinExecutions), so an
// orphaned pointer is invisible to it. A BrandNew create then inserts a second current row, hits
// the unique key, and returns Unavailable: the workflow ID is unusable on every start path,
// permanently, behind an error clients treat as transient (about 16 failed transactions per call
// from internal retries alone). The test fails with STALE when that changes.

type OrphanPointerProbeSuite struct {
	parallelsuite.Suite[*OrphanPointerProbeSuite]
}

func TestOrphanPointerProbeSuite(t *testing.T) {
	parallelsuite.Run(t, &OrphanPointerProbeSuite{})
}

// orphan starts a workflow, optionally completes it, deletes its mutable state and history but
// not its current-execution pointer, and drops the shard's cache. It returns the orphaned run ID.
func orphan(s *OrphanPointerProbeSuite, env *testcore.TestEnv, tv *testvars.TestVars, complete bool) string {
	ctx := s.Context()
	shardID := common.WorkflowIDToHistoryShard(env.NamespaceID().String(), tv.WorkflowID(),
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards)
	execMgr := env.GetTestCluster().ExecutionManager()
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId: uuid.NewString(), Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(),
		WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
	})
	s.Require().NoError(err)
	if complete {
		poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())
		_, err = poller.PollAndHandleWorkflowTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
			}}}, nil
		})
		s.Require().NoError(err)
	}
	s.Require().NoError(execMgr.DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
		ShardID: shardID, NamespaceID: env.NamespaceID().String(), WorkflowID: tv.WorkflowID(),
		RunID: startResp.RunId, ArchetypeID: chasm.WorkflowArchetypeID,
	}))
	waitForMutableStateGone(ctx, env, shardID, execMgr, tv.WorkflowID(), startResp.RunId)
	current, err := execMgr.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
		ShardID: shardID, NamespaceID: env.NamespaceID().String(), WorkflowID: tv.WorkflowID(),
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.Require().NoError(err)
	s.Require().Equal(startResp.RunId, current.RunID, "the pointer must survive: that is the bug shape")
	s.T().Logf("orphaned pointer: run %s, state %v, status %v", current.RunID, current.State, current.Status)
	env.CloseShard(env.NamespaceID().String(), tv.WorkflowID())
	return startResp.RunId
}

// attempt runs one start call twice (a retry must not change the answer), logs the outcome and,
// on SQL, checks it against the known gap.
func attempt(s *OrphanPointerProbeSuite, name string, call func(ctx context.Context) (string, error)) {
	for i := 1; i <= 2; i++ {
		ctx, cancel := context.WithTimeout(s.Context(), 10*time.Second)
		start := time.Now()
		runID, err := call(ctx)
		cancel()
		outcome := "ok, run " + runID
		if err != nil {
			outcome = fmt.Sprintf("%T: %v", err, err)
		}
		s.T().Logf("%-40s try %d after %-7v %s", name, i, time.Since(start).Round(10*time.Millisecond), outcome)
		if testcore.UseCassandraPersistence() {
			continue // covered by SignalWithStartOrphanPointerSuite
		}
		var unavailable *serviceerror.Unavailable
		if !errors.As(err, &unavailable) || !strings.Contains(err.Error(), "current_executions") {
			s.T().Fatalf("STALE: %s no longer fails with the known gap (got %v); update the ledger", name, outcome)
		}
	}
}

func (s *OrphanPointerProbeSuite) probe(complete bool) {
	for _, op := range []struct {
		name string
		call func(env *testcore.TestEnv, tv *testvars.TestVars) func(ctx context.Context) (string, error)
	}{
		{"SignalWithStart (allow duplicate)", func(env *testcore.TestEnv, tv *testvars.TestVars) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) {
				r, err := env.FrontendClient().SignalWithStartWorkflowExecution(ctx, &workflowservice.SignalWithStartWorkflowExecutionRequest{
					RequestId: uuid.NewString(), Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(),
					WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(), Identity: tv.WorkerIdentity(),
					SignalName: tv.SignalName(), WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				})
				return r.GetRunId(), err
			}
		}},
		{"StartWorkflow (allow duplicate)", func(env *testcore.TestEnv, tv *testvars.TestVars) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) {
				r, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
					RequestId: uuid.NewString(), Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(),
					WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
					WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
				})
				return r.GetRunId(), err
			}
		}},
		{"StartWorkflow (terminate existing)", func(env *testcore.TestEnv, tv *testvars.TestVars) func(context.Context) (string, error) {
			return func(ctx context.Context) (string, error) {
				r, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
					RequestId: uuid.NewString(), Namespace: env.Namespace().String(), WorkflowId: tv.WorkflowID(),
					WorkflowType: tv.WorkflowType(), TaskQueue: tv.TaskQueue(),
					WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
				})
				return r.GetRunId(), err
			}
		}},
	} {
		s.Run(op.name, func(s *OrphanPointerProbeSuite) {
			env := testcore.NewEnv(s.T(), testcore.WithDedicatedCluster()) // CloseShard is cluster-wide
			tv := testvars.New(s.T())
			orphan(s, env, tv, complete)
			attempt(s, op.name, op.call(env, tv))
		})
	}
}

func (s *OrphanPointerProbeSuite) TestProbeCompletedRun() { s.probe(true) }
func (s *OrphanPointerProbeSuite) TestProbeRunningRun()   { s.probe(false) }

// waitForMutableStateGone is copied from tests/history_node_cleanup_test.go (this probe lives in its
// own package so it compiles without the whole functional-test package).
func waitForMutableStateGone(ctx context.Context, env *testcore.TestEnv, shardID int32, execMgr persistence.ExecutionManager, workflowID, runID string) {
	env.Eventually(func() bool {
		_, err := execMgr.GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: env.NamespaceID().String(),
			WorkflowID:  workflowID,
			RunID:       runID,
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		return common.IsNotFoundError(err)
	}, 10*time.Second, 100*time.Millisecond,
		"timed out waiting for mutable state of run %s to be deleted", runID)
}
