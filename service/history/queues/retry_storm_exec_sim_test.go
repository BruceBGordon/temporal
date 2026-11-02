package queues

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistenceclient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

// Real code in a simulated world. This extends retry_storm_sim_test.go: instead of modeling task
// execution, every loaded task is a real executable (NewExecutable), driven exactly the way the
// host scheduler's workers drive it (Execute, then HandleErr, then Nack or Ack; see
// FIFOScheduler.executeTask). Its real resubmit-or-reschedule decision (shouldResubmitOnNack,
// backoffDuration) and the real reader decision (readerRetryDelay) run unmodified, in whichever
// version is checked out.
//
// Simulated: the persistence limiter and blip, the virtual clock (clock.EventTimeSource), the host
// scheduler (worker pool + small per-channel queue, as TrySubmit sees it), the rescheduler timer,
// shard readers, task arrival, and ack-level advancement. Defaults are Temporal's real ones where
// one exists (system.persistenceQPSBurstRatio=1.0, transferProcessorSchedulerWorkerCount=512, scheduler channel queue=10, rescheduler
// full-channel backoff 2s +/-50%, transferTaskBatchSize=100, queuePendingTasksMaxCount=10000,
// pollBackoff 5s, ack interval 30s).

type execSimParams struct {
	shards          int
	limitPerSec     float64
	burst           float64
	arrivalPerShard float64
	batchSize       int
	maxPending      int
	pollBackoff     time.Duration
	idlePoll        time.Duration
	ackInterval     time.Duration
	workers         int
	queueCap        int
	execTime        time.Duration
	blipStart       time.Duration
	blipLength      time.Duration
	horizon         time.Duration
	// divergeAt > 0 stops a run once outstanding work after the blip exceeds it, and reports the
	// run as diverged (a separate outcome from "never recovered"): the backlog is growing without
	// bound and the simulation's memory grows with it.
	divergeAt        float64
	sharedAckLimiter bool
	noise            float64
	seed             uint64

	// Hypothesis 1: shard reload on ambiguous write errors, redoing work since the last saved
	// queue position. Grounded in shard.ContextImpl (handleWriteErrorLocked, updateShardInfo,
	// acquireShard) and queueBase.checkpoint.
	shardReload            bool
	shardUpdateMinInterval time.Duration // history.shardUpdateMinInterval
	shardUpdateMinTasks    int           // history.shardUpdateMinTasksCompleted
	writeFraction          float64       // ASSUMPTION: share of task executions that are shard writes
	// apiWrites models the write traffic that creates the tasks: every transfer task is created by a
	// workflow write through the shard (one write per task, sharing history.persistenceMaxQPS).
	// An ambiguous error on such a write makes the shard re-acquire, as in handleWriteErrorLocked.
	apiWrites bool

	// Hypothesis 4 (ASSUMPTION, not from Temporal code): the database's own capacity, in calls/s, is
	// slightly below what the host limiters admit (many hosts share one database). Calls the
	// limiter admits beyond it time out with DeadlineExceeded. 0 disables.
	dbCapacity float64
	// clientRetry adds the persistence client's own retry layer (hypothesis 5).
	clientRetry bool
	// healthErrorThreshold > 0 enables the server's health-based adaptive limiter
	// (client.HealthRequestRateLimiterImpl, history.persistenceDynamicRateLimitingParams; off by
	// default) with this ErrorThreshold, an operator choice. Everything else uses the real
	// defaults (dynamicconfig.DefaultDynamicRateLimitingParams). Hypothesis 6.
	healthErrorThreshold float64
	// sqlBackend shapes database timeouts the way the SQL stores return them (the #11547 incident
	// ran on Aurora PostgreSQL): a call outside a transaction (reads, range deletes,
	// GetOrCreateShard) returns Unavailable with the cause formatted away; a transaction (shard
	// updates, workflow writes, txExecute) returns the context deadline wrapped with %w
	// (sql/common.go convertSQLError). Otherwise timeouts are Cassandra's persistence.TimeoutError.
	sqlBackend bool
}

func defaultExecSimParams() execSimParams {
	return execSimParams{
		shards:           16,
		limitPerSec:      500,
		burst:            500, // system.persistenceQPSBurstRatio=1.0: burst is one second of capacity
		arrivalPerShard:  25,  // 400 tasks/s against 500 persistence calls/s: ~80% utilization
		batchSize:        100,
		maxPending:       10000,
		pollBackoff:      5 * time.Second,
		idlePoll:         time.Second,
		ackInterval:      30 * time.Second,
		workers:          512,
		queueCap:         10,
		execTime:         10 * time.Millisecond,
		blipStart:        2 * time.Minute,
		blipLength:       5 * time.Second,
		horizon:          10 * time.Minute,
		sharedAckLimiter: true,
		noise:            0.2,
		seed:             1,

		shardUpdateMinInterval: 5 * time.Minute,
		shardUpdateMinTasks:    1000,
	}
}

type execSimResult struct {
	recoveredAfter   time.Duration // after the blip, until outstanding work is back to baseline; -1 if never
	divergedAt       time.Duration // when outstanding passed divergeAt (run stopped); 0 if it did not
	peakOutstanding  float64
	finalOutstanding float64
	failedCalls      int // persistence calls that failed (reads + executions)
	execAttempts     int
	reloads          int     // shard re-acquisitions
	reworkTasks      int     // tasks re-read because the saved queue position was behind
	failedWrites     int     // API writes that failed (rejected or ambiguous)
	timeouts         int     // calls admitted by the limiter that the database timed out
	clientRetries    int     // extra attempts made by the persistence client's own retry layer
	minMultiplier    float64 // lowest rate multiplier the adaptive limiter reached
}

type execEvent struct {
	at    time.Duration
	kind  int // 0 read, 1 checkpoint, 2 worker done, 3 reschedule fire, 4 shard acquire, 5 API write
	shard int
	ex    Executable
	gen   int // shard generation the event belongs to; stale events are dropped
	seq   int
}

type execQueue []execEvent

func (q execQueue) Len() int { return len(q) }
func (q execQueue) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q execQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *execQueue) Push(x any)   { *q = append(*q, x.(execEvent)) }
func (q *execQueue) Pop() any     { o := *q; e := o[len(o)-1]; *q = o[:len(o)-1]; return e }

type execSim struct {
	p          execSimParams
	t0         time.Time
	now        time.Duration
	ts         *clock.EventTimeSource
	q          execQueue
	seq        int
	rng        *rand.Rand
	limiter    *tokenBucket
	ackLimiter *tokenBucket
	db         *tokenBucket
	// health signal window (system.persistenceHealthSignalWindowSize = 10s, the same as the
	// limiter's refresh interval), counted where the metrics client sits: above the rate limiter,
	// so limiter rejections are recorded calls too.
	hCalls, hUnhealthy int
	multiplier         float64
	busy               int
	waiting            []Executable
	shardOf            map[Executable]int
	backlog            []float64
	loaded             []int
	executed           []int
	retriers           []backoff.Retrier
	res                execSimResult

	gen               []int
	exGen             map[Executable]int
	completed         []int // tasks completed, cumulative
	persisted         []int // saved queue position, in completed-task units
	tasksSinceUpdate  []int
	lastShardUpdate   []time.Duration
	reloading         []bool
	checkpointRetrier []backoff.Retrier
	acquireRetrier    []backoff.Retrier
}

func (s *execSim) schedule(e execEvent) {
	s.seq++
	e.seq = s.seq
	heap.Push(&s.q, e)
}

func (s *execSim) vary(d time.Duration) time.Duration {
	if s.p.noise == 0 || d == 0 {
		return d
	}
	return time.Duration(float64(d) * (1 - s.p.noise + 2*s.p.noise*s.rng.Float64()))
}

func (s *execSim) inBlip() bool {
	return s.now >= s.p.blipStart && s.now < s.p.blipStart+s.p.blipLength
}

// persistenceCall is one call through the persistence client stack. With clientRetry, the real
// retryable client layer (common/persistence/client/factory.go) wraps every call: it retries errors
// in its own transient group (persistenceclient.IsPersistenceTransientError) under the real policy
// (common.CreatePersistenceClientRetryPolicy). Approximation: the retry's 50ms wait is not modeled;
// the extra attempt is charged at the same instant.
func (s *execSim) persistenceCall(l *tokenBucket, tx bool) error {
	err := s.persistenceAttempt(l, tx)
	if !s.p.clientRetry {
		return err
	}
	r := backoff.NewRetrier(common.CreatePersistenceClientRetryPolicy(), s.ts)
	for err != nil && persistenceclient.IsPersistenceTransientError(err) {
		if r.NextBackOff(err) < 0 {
			break
		}
		s.res.clientRetries++
		err = s.persistenceAttempt(l, tx)
	}
	return err
}

// persistenceAttempt decides the outcome of one attempt at the current virtual time, and records
// it in the health signal window the way the metrics client does (persistence.isUnhealthyError
// counts the store's TimeoutError; Unavailable and ResourceExhausted count as healthy).
func (s *execSim) persistenceAttempt(l *tokenBucket, tx bool) error {
	err := s.persistenceOutcome(l, tx)
	s.hCalls++
	// persistence.isUnhealthyError (unexported): the store's TimeoutError, or a context deadline
	// (errors.Is, so wrapped ones count); Unavailable and ResourceExhausted count as healthy.
	var te *persistence.TimeoutError
	if errors.As(err, &te) || common.IsContextDeadlineExceededErr(err) {
		s.hUnhealthy++
	}
	return err
}

// refreshHealthLimiter applies HealthRequestRateLimiterImpl.refreshRate's rule with the real
// default step sizes and bounds: over the error threshold, cut the multiplier by the backoff step
// (not below the minimum); otherwise raise it by the increase step (not above the maximum).
func (s *execSim) refreshHealthLimiter() {
	d := dynamicconfig.DefaultDynamicRateLimitingParams
	ratio := 0.0
	if s.hCalls > 0 {
		ratio = float64(s.hUnhealthy) / float64(s.hCalls)
	}
	s.hCalls, s.hUnhealthy = 0, 0
	if ratio > s.p.healthErrorThreshold {
		s.multiplier = max(d.RateMultiMin, s.multiplier-d.RateBackoffStepSize)
	} else if s.multiplier < d.RateMultiMax {
		s.multiplier = min(d.RateMultiMax, s.multiplier+d.RateIncreaseStepSize)
	}
	s.res.minMultiplier = min(s.res.minMultiplier, s.multiplier)
	s.limiter.rate = s.multiplier * s.p.limitPerSec
	s.limiter.burst = s.multiplier * s.p.burst
	s.limiter.tokens = min(s.limiter.tokens, s.limiter.burst)
}

func (s *execSim) persistenceOutcome(l *tokenBucket, tx bool) error {
	switch {
	case s.inBlip():
		return serviceerror.NewUnavailable("database failover")
	case !l.take(s.now):
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: "persistence limit",
		}
	case s.db != nil && !s.db.take(s.now):
		s.res.timeouts++
		switch {
		case s.p.sqlBackend && tx:
			return fmt.Errorf("UpdateWorkflowExecution: %w", context.DeadlineExceeded)
		case s.p.sqlBackend:
			return serviceerror.NewUnavailablef("GetHistoryTasks operation failed. Select failed. Error: %v", context.DeadlineExceeded)
		}
		// Cassandra converts timeouts to persistence.TimeoutError
		// (nosqlplugin/cassandra/gocql/errors.go ConvertError).
		return &persistence.TimeoutError{Msg: "persistence call timed out"}
	}
	return nil
}

// --- fakes for the executable's collaborators, all on the virtual clock

type simExecutor struct{ s *execSim }

func (x simExecutor) Execute(_ context.Context, e Executable) ExecuteResponse {
	x.s.res.execAttempts++
	write := x.s.p.writeFraction > 0 && x.s.rng.Float64() < x.s.p.writeFraction
	err := x.s.persistenceCall(x.s.limiter, write)
	if err != nil {
		x.s.res.failedCalls++
		if write && isAmbiguousWriteError(err) {
			x.s.startReload(x.s.shardOf[e])
		}
	}
	return ExecuteResponse{ExecutedAsActive: true, ExecutionErr: err}
}

type simScheduler struct{ s *execSim }

func (x simScheduler) Start()                             {}
func (x simScheduler) Stop()                              {}
func (x simScheduler) TaskChannelKeyFn() TaskChannelKeyFn { return nil }
func (x simScheduler) Submit(e Executable)                { x.s.enqueue(e, true) }
func (x simScheduler) TrySubmit(e Executable) bool        { return x.s.enqueue(e, false) }

type simRescheduler struct{ s *execSim }

func (x simRescheduler) Add(e Executable, at time.Time) {
	x.s.schedule(execEvent{at: max(x.s.now, at.Sub(x.s.t0)), kind: 3, ex: e, gen: x.s.exGen[e]})
}
func (x simRescheduler) Reschedule(string) {}
func (x simRescheduler) Len() int          { return 0 }
func (x simRescheduler) Start()            {}
func (x simRescheduler) Stop()             {}

// enqueue mirrors TrySubmit: a free worker or room in the small channel queue accepts the task.
func (s *execSim) enqueue(e Executable, force bool) bool {
	if s.busy < s.p.workers {
		s.busy++
		s.schedule(execEvent{at: s.now + s.vary(s.p.execTime), kind: 2, ex: e})
		return true
	}
	if force || len(s.waiting) < s.p.queueCap {
		s.waiting = append(s.waiting, e)
		return true
	}
	return false
}

func (s *execSim) done(e Executable) {
	sh := s.shardOf[e]
	delete(s.shardOf, e)
	delete(s.exGen, e)
	s.loaded[sh]--
	s.executed[sh]++
	s.completed[sh]++
	s.tasksSinceUpdate[sh]++
}

// stale reports whether e belongs to a shard generation that has since been reloaded.
func (s *execSim) stale(e Executable) bool {
	g, ok := s.exGen[e]
	if !ok {
		return true
	}
	sh, ok := s.shardOf[e]
	return !ok || g != s.gen[sh]
}

// isAmbiguousWriteError is the server's own declared rule, persistence.OperationPossiblySucceeded,
// which shard.ContextImpl.handleWriteErrorLocked restates: a write error that may have been
// committed makes the shard re-acquire.
func isAmbiguousWriteError(err error) bool {
	return err != nil && persistence.OperationPossiblySucceeded(err)
}

// startReload models contextRequestLost: the shard re-acquires; its queues restart from the saved
// position, so work completed since then is read and executed again.
func (s *execSim) startReload(sh int) {
	if !s.p.shardReload || s.reloading[sh] {
		return
	}
	s.reloading[sh] = true
	s.res.reloads++
	s.gen[sh]++
	rework := s.completed[sh] - s.persisted[sh]
	s.res.reworkTasks += rework
	s.backlog[sh] += float64(rework + s.loaded[sh]) // in-flight tasks are dropped and re-read too
	s.completed[sh] = s.persisted[sh]
	s.loaded[sh], s.executed[sh], s.tasksSinceUpdate[sh] = 0, 0, 0
	for e, owner := range s.shardOf {
		if owner == sh {
			delete(s.shardOf, e)
		}
	}
	s.schedule(execEvent{at: s.now, kind: 4, shard: sh, gen: s.gen[sh]})
}

func runExecRetryStormSim(t *testing.T, p execSimParams) execSimResult {
	ctrl := gomock.NewController(t)
	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("sim"), nil).AnyTimes()
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, errors.New("not needed in simulation")).AnyTimes()
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{"active": {ShardCount: int32(p.shards)}}).AnyTimes()
	chasmRegistry := chasm.NewRegistry(log.NewNoopLogger())

	s := &execSim{
		p:        p,
		t0:       time.Unix(1_700_000_000, 0),
		ts:       clock.NewEventTimeSource(),
		rng:      rand.New(rand.NewPCG(p.seed, p.seed^0x9e3779b97f4a7c15)),
		limiter:  &tokenBucket{rate: p.limitPerSec, burst: p.burst, tokens: p.burst},
		shardOf:  map[Executable]int{},
		backlog:  make([]float64, p.shards),
		loaded:   make([]int, p.shards),
		executed: make([]int, p.shards),
		retriers: make([]backoff.Retrier, p.shards),

		gen:               make([]int, p.shards),
		exGen:             map[Executable]int{},
		completed:         make([]int, p.shards),
		persisted:         make([]int, p.shards),
		tasksSinceUpdate:  make([]int, p.shards),
		lastShardUpdate:   make([]time.Duration, p.shards),
		reloading:         make([]bool, p.shards),
		checkpointRetrier: make([]backoff.Retrier, p.shards),
		acquireRetrier:    make([]backoff.Retrier, p.shards),
	}
	s.ackLimiter = s.limiter
	if p.dbCapacity > 0 {
		// A database queues requests; a call times out only if it would wait longer than its deadline
		// (queueIOTimeout = 5s), so up to 5 seconds of capacity can be absorbed as a backlog.
		dbBurst := p.dbCapacity * queueIOTimeout.Seconds()
		s.db = &tokenBucket{rate: p.dbCapacity, burst: dbBurst, tokens: dbBurst}
	}
	if !p.sharedAckLimiter {
		s.ackLimiter = &tokenBucket{rate: p.limitPerSec, burst: p.burst, tokens: p.burst}
	}
	s.ts.Update(s.t0)
	exec, sched, resched := simExecutor{s}, simScheduler{s}, simRescheduler{s}
	newTask := func() Executable {
		return NewExecutable(DefaultReaderId,
			tasks.NewFakeTask(definition.NewWorkflowKey("sim-ns", "wf", "run"), tasks.CategoryTransfer, s.t0),
			exec, sched, resched, NewNoopPriorityAssigner(), s.ts, nsRegistry, clusterMetadata,
			chasmRegistry, GetTaskTypeTagValue, log.NewNoopLogger(), metrics.NoopMetricsHandler, telemetry.NoopTracer)
	}
	for sh := range p.shards {
		s.retriers[sh] = backoff.NewRetrier(common.CreateReadTaskRetryPolicy(), clock.NewRealTimeSource())
		s.checkpointRetrier[sh] = backoff.NewRetrier(createCheckpointRetryPolicy(), s.ts)
		// shard.ContextImpl.acquireShard default policy
		s.acquireRetrier[sh] = backoff.NewRetrier(backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(5*time.Minute), s.ts)
		s.schedule(execEvent{at: time.Duration(sh) * time.Millisecond, kind: 0, shard: sh})
		s.schedule(execEvent{at: p.ackInterval + time.Duration(sh)*time.Millisecond, kind: 1, shard: sh})
		if p.apiWrites {
			s.schedule(execEvent{at: time.Duration(sh) * time.Millisecond, kind: 5, shard: sh})
		}
	}

	s.multiplier, s.res.minMultiplier = 1, 1
	if p.healthErrorThreshold > 0 {
		s.schedule(execEvent{at: dynamicconfig.DefaultDynamicRateLimitingParams.RefreshInterval, kind: 6})
	}

	s.res.recoveredAfter = -1
	blipEnd := p.blipStart + p.blipLength
	baseline := 0.0
	lastArrival := time.Duration(0)
	for s.q.Len() > 0 {
		e := heap.Pop(&s.q).(execEvent)
		if e.at > p.horizon {
			break
		}
		s.now = e.at
		s.ts.Update(s.t0.Add(s.now))
		dt := (s.now - lastArrival).Seconds()
		for sh := range p.shards {
			s.backlog[sh] += p.arrivalPerShard * dt
		}
		lastArrival = s.now

		outstanding := 0.0
		for sh := range p.shards {
			outstanding += s.backlog[sh] + float64(s.loaded[sh])
		}
		s.res.peakOutstanding = max(s.res.peakOutstanding, outstanding)
		if s.now < p.blipStart {
			baseline = max(baseline, outstanding)
		} else if p.blipLength > 0 && s.now >= blipEnd && s.res.recoveredAfter < 0 && outstanding <= baseline*1.1+float64(p.batchSize) {
			s.res.recoveredAfter = s.now - blipEnd
		}
		if p.divergeAt > 0 && s.now >= blipEnd && s.res.recoveredAfter < 0 && outstanding > p.divergeAt {
			s.res.divergedAt = s.now
			break
		}

		switch e.kind {
		case 6: // HealthRequestRateLimiterImpl refresh tick
			s.refreshHealthLimiter()
			s.schedule(execEvent{at: s.now + dynamicconfig.DefaultDynamicRateLimitingParams.RefreshInterval, kind: 6})
			continue
		case 0: // shard reader
			sh := e.shard
			if e.gen != s.gen[sh] || s.reloading[sh] {
				continue // this reader belongs to a shard generation that was reloaded
			}
			if s.loaded[sh]+s.executed[sh] >= p.maxPending {
				s.schedule(execEvent{at: s.now + p.pollBackoff, kind: 0, shard: sh, gen: s.gen[sh]})
				continue
			}
			if err := s.persistenceCall(s.limiter, false); err != nil {
				s.res.failedCalls++
				s.schedule(execEvent{at: s.now + readerRetryDelay(err, s.retriers[sh]), kind: 0, shard: sh, gen: s.gen[sh]})
				continue
			}
			s.retriers[sh].Reset()
			n := int(min(float64(p.batchSize), s.backlog[sh], float64(p.maxPending-s.loaded[sh]-s.executed[sh])))
			s.backlog[sh] -= float64(n)
			for range n {
				ex := newTask()
				s.shardOf[ex] = sh
				s.exGen[ex] = s.gen[sh]
				s.loaded[sh]++
				ex.SetScheduledTime(s.ts.Now()) // as ReaderImpl.submit
				if !sched.TrySubmit(ex) {
					ex.Reschedule()
				}
			}
			next := p.idlePoll
			if s.backlog[sh] >= 1 {
				next = 0
			}
			s.schedule(execEvent{at: s.now + s.vary(2*time.Millisecond) + next, kind: 0, shard: sh, gen: s.gen[sh]})
		case 1: // queueBase.checkpoint
			sh := e.shard
			next := s.vary(p.ackInterval)
			if !s.reloading[sh] {
				s.executed[sh] = 0                                             // ShrinkSlices: completed tasks leave memory regardless of what follows
				if err := s.persistenceCall(s.ackLimiter, false); err != nil { // RangeCompleteHistoryTasks
					s.res.failedCalls++
					next = s.checkpointRetrier[sh].NextBackOff(err)
				} else if s.tasksSinceUpdate[sh] >= p.shardUpdateMinTasks || s.now-s.lastShardUpdate[sh] >= p.shardUpdateMinInterval {
					// SetQueueState -> updateShardInfo -> UpdateShard
					if err := s.persistenceCall(s.ackLimiter, true); err != nil {
						s.res.failedCalls++
						next = s.checkpointRetrier[sh].NextBackOff(err)
						if isAmbiguousWriteError(err) {
							s.startReload(sh)
						}
					} else {
						s.persisted[sh] = s.completed[sh]
						s.tasksSinceUpdate[sh] = 0
						s.lastShardUpdate[sh] = s.now
						s.checkpointRetrier[sh].Reset()
					}
				} else {
					s.checkpointRetrier[sh].Reset()
				}
			}
			s.schedule(execEvent{at: s.now + next, kind: 1, shard: sh})
		case 5: // a workflow write through the shard (the source of the tasks)
			sh := e.shard
			if !s.reloading[sh] {
				if err := s.persistenceCall(s.limiter, true); err != nil {
					s.res.failedWrites++
					if isAmbiguousWriteError(err) {
						s.startReload(sh)
					}
				}
			}
			s.schedule(execEvent{at: s.now + s.vary(time.Duration(float64(time.Second)/p.arrivalPerShard)), kind: 5, shard: sh})
		case 4: // shard.ContextImpl.acquireShard: GetOrCreateShard, then UpdateShard with a new range ID
			sh := e.shard
			if e.gen != s.gen[sh] {
				continue
			}
			err := s.persistenceCall(s.limiter, false)
			if err == nil {
				err = s.persistenceCall(s.limiter, true)
			}
			if err != nil {
				s.res.failedCalls++
				delay := s.acquireRetrier[sh].NextBackOff(err)
				if delay < 0 {
					delay = 5 * time.Second // acquisition expired: the controller acquires the shard again
					s.acquireRetrier[sh].Reset()
				}
				s.schedule(execEvent{at: s.now + delay, kind: 4, shard: sh, gen: s.gen[sh]})
				continue
			}
			s.acquireRetrier[sh].Reset()
			s.reloading[sh] = false
			s.lastShardUpdate[sh] = s.now
			s.schedule(execEvent{at: s.now, kind: 0, shard: sh, gen: s.gen[sh]})
		case 2: // a worker finishes running one executable, as FIFOScheduler.executeTask does
			ex := e.ex
			if s.stale(ex) {
				ex.Cancel()
			} else if err := ex.Execute(); err != nil {
				if err = ex.HandleErr(err); err != nil {
					ex.Nack(err)
				} else {
					ex.Ack()
					s.done(ex)
				}
			} else {
				ex.Ack()
				s.done(ex)
			}
			s.busy--
			if len(s.waiting) > 0 {
				next := s.waiting[0]
				s.waiting = s.waiting[1:]
				s.enqueue(next, true)
			}
		case 3: // rescheduler timer fires, as reschedulerImpl.reschedule does
			if s.stale(e.ex) {
				continue
			}
			e.ex.SetScheduledTime(s.ts.Now())
			if !sched.TrySubmit(e.ex) {
				s.schedule(execEvent{at: s.now + backoff.Jitter(2*time.Second, 0.5), kind: 3, ex: e.ex, gen: e.gen})
			}
		}
	}
	for sh := range p.shards {
		s.res.finalOutstanding += s.backlog[sh] + float64(s.loaded[sh])
	}
	return s.res
}

func formatExecSim(r execSimResult) string {
	rec := "never"
	if r.recoveredAfter >= 0 {
		rec = r.recoveredAfter.Round(time.Millisecond).String()
	}
	return fmt.Sprintf("recovered after blip: %-10s peak outstanding: %7.0f  final: %6.0f  failed calls: %8d  failed writes: %7d  attempts: %8d  reloads: %4d  rework tasks: %7d",
		rec, r.peakOutstanding, r.finalOutstanding, r.failedCalls, r.failedWrites, r.execAttempts, r.reloads, r.reworkTasks)
}

// TestExecRetryStormSim checks the model is calibrated (stable without a blip), then reports
// recovery from a blip with ack advancement sharing the limiter and with its own budget.
func TestExecRetryStormSim(t *testing.T) {
	calm := defaultExecSimParams()
	calm.blipLength = 0
	cr := runExecRetryStormSim(t, calm)
	t.Log("no blip:           " + formatExecSim(cr))
	if cr.finalOutstanding > float64(4*calm.batchSize*calm.shards) {
		t.Fatalf("model is unstable without a blip (final outstanding %.0f); results would be meaningless", cr.finalOutstanding)
	}
	for _, shared := range []bool{true, false} {
		p := defaultExecSimParams()
		p.sharedAckLimiter = shared
		t.Log(fmt.Sprintf("shared ack=%-5v    ", shared) + formatExecSim(runExecRetryStormSim(t, p)))
	}
}

// TestExecRetryStormSimSweep reports recovery across load, blip length and seeds. Report-only.
func TestExecRetryStormSimSweep(t *testing.T) {
	for _, util := range []float64{0.80, 0.90, 0.95} {
		for _, blipLen := range []time.Duration{5 * time.Second, 30 * time.Second} {
			worst, never, failed, attempts := time.Duration(0), 0, 0, 0
			for seed := uint64(1); seed <= 3; seed++ {
				p := defaultExecSimParams()
				p.arrivalPerShard = util * p.limitPerSec / float64(p.shards)
				p.blipLength, p.seed = blipLen, seed
				r := runExecRetryStormSim(t, p)
				if r.recoveredAfter < 0 {
					never++
				} else {
					worst = max(worst, r.recoveredAfter)
				}
				failed += r.failedCalls
				attempts += r.execAttempts
			}
			t.Logf("utilization %.0f%%  blip %-4v  worst recovery %-10v  never recovered %d/3  failed calls %8d  execution attempts %9d",
				util*100, blipLen, worst.Round(time.Millisecond), never, failed, attempts)
		}
	}
}

// TestExecRetryStormSimLong runs the hardest sweep case (95% utilization, 30s blip) for a simulated
// hour, to tell a slow drain (recovers eventually) from a wedge (never recovers). Report-only.
func TestExecRetryStormSimLong(t *testing.T) {
	for seed := uint64(1); seed <= 3; seed++ {
		p := defaultExecSimParams()
		p.arrivalPerShard = 0.95 * p.limitPerSec / float64(p.shards)
		p.blipLength, p.seed, p.horizon = 30*time.Second, seed, time.Hour
		// Spare capacity is 5%, so the 30s backlog alone needs roughly 30s/0.05 = 10 minutes to drain.
		t.Log(fmt.Sprintf("95%% / 30s blip / 1h, seed %d: ", seed) + formatExecSim(runExecRetryStormSim(t, p)))
	}
}

// TestShardReloadHypothesis tests hypothesis 1: ambiguous write errors (Unavailable) make shards
// re-acquire and redo work since the last saved queue position, which could sustain an outage.
// Each configuration must first be stable without a blip. Report-only.
func TestShardReloadHypothesis(t *testing.T) {
	configs := []struct {
		name          string
		reload        bool
		writeFraction float64
	}{
		{"reload off", false, 0},
		{"reload on, grounded only", true, 0},
	}
	for _, c := range configs {
		calm := defaultExecSimParams()
		calm.blipLength, calm.horizon = 0, 30*time.Minute
		calm.arrivalPerShard = 0.95 * calm.limitPerSec / float64(calm.shards)
		calm.shardReload, calm.writeFraction, calm.apiWrites = c.reload, c.writeFraction, true
		calm.arrivalPerShard /= 2 // writes and executions together make up the utilization
		cr := runExecRetryStormSim(t, calm)
		stable := cr.finalOutstanding <= float64(4*calm.batchSize*calm.shards)
		t.Logf("[%s] no blip at 95%%: stable=%v  %s", c.name, stable, formatExecSim(cr))
		if !stable {
			t.Errorf("[%s] model unstable without a blip; skipping its results", c.name)
			continue
		}
		for _, util := range []float64{0.90, 0.95} {
			for _, blipLen := range []time.Duration{5 * time.Second, 30 * time.Second} {
				worst, never, reloads, rework := time.Duration(0), 0, 0, 0
				for seed := uint64(1); seed <= 3; seed++ {
					p := defaultExecSimParams()
					p.arrivalPerShard = util * p.limitPerSec / float64(p.shards)
					p.blipLength, p.seed, p.horizon = blipLen, seed, 30*time.Minute
					p.shardReload, p.writeFraction, p.apiWrites = c.reload, c.writeFraction, true
					p.arrivalPerShard /= 2
					r := runExecRetryStormSim(t, p)
					if r.recoveredAfter < 0 {
						never++
					} else {
						worst = max(worst, r.recoveredAfter)
					}
					reloads += r.reloads
					rework += r.reworkTasks
				}
				t.Logf("[%s] util %.0f%% blip %-4v worst recovery %-12v never(30m) %d/3  reloads %4d  rework tasks %7d",
					c.name, util*100, blipLen, worst.Round(time.Second), never, reloads, rework)
				if c.reload && reloads == 0 {
					t.Errorf("[%s] the reload mechanism never fired; this result says nothing about hypothesis 1", c.name)
				}
			}
		}
	}
}

// TestTimeoutHypothesis tests hypothesis 4 (an ASSUMPTION about the database, labeled as such):
// if the database itself is slightly smaller than what the host limiters admit, timeouts are
// ambiguous writes, so shards reload and redo work, which adds load and more timeouts.
//
// Metastability is tested by its definition: at each load level the system must first be stable
// without a blip; only then is it hit with a 5s blip, and we ask whether it comes back. Load levels
// whose baseline is already unstable are reported and skipped. Report-only.
func TestTimeoutHypothesis(t *testing.T) {
	for _, dbFactor := range []float64{0, 0.99, 0.97} {
		name := fmt.Sprintf("db capacity %.0f%% of limiter", dbFactor*100)
		if dbFactor == 0 {
			name = "no database timeouts"
		}
		base := func(util float64) execSimParams {
			p := defaultExecSimParams()
			p.shardReload, p.apiWrites, p.horizon = true, true, 30*time.Minute
			p.dbCapacity = dbFactor * p.limitPerSec
			p.arrivalPerShard = util * p.limitPerSec / float64(p.shards) / 2
			return p
		}
		for _, util := range []float64{0.80, 0.85, 0.90, 0.95} {
			calm := base(util)
			calm.blipLength = 0
			cr := runExecRetryStormSim(t, calm)
			if cr.finalOutstanding > float64(4*calm.batchSize*calm.shards) || cr.reloads > 0 {
				t.Logf("[%s] util %.0f%%: baseline UNSTABLE without a blip (reloads %d, final outstanding %.0f); skipped",
					name, util*100, cr.reloads, cr.finalOutstanding)
				continue
			}
			worst, never, reloads, timeouts := time.Duration(0), 0, 0, 0
			for seed := uint64(1); seed <= 3; seed++ {
				p := base(util)
				p.seed = seed
				r := runExecRetryStormSim(t, p)
				if r.recoveredAfter < 0 {
					never++
				} else {
					worst = max(worst, r.recoveredAfter)
				}
				reloads += r.reloads
				timeouts += r.timeouts
			}
			t.Logf("[%s] util %.0f%%: baseline stable; after 5s blip worst recovery %-8v NEVER RECOVERED(30m) %d/3  reloads %5d  timeouts %8d",
				name, util*100, worst.Round(time.Second), never, reloads, timeouts)
		}
	}
}

// TestTimeoutTippingPoint asks the metastability question directly: a system that is stable
// without a blip is hit with blips of increasing length. Does a large enough trigger push it into
// the timeout -> reload -> redo loop, so that it does not come back after the blip ends?
// Uses the hypothesis 4 database ASSUMPTION. Report-only.
func TestTimeoutTippingPoint(t *testing.T) {
	for _, dbFactor := range []float64{0.99, 0.97} {
		for _, util := range []float64{0.85, 0.90} {
			calm := defaultExecSimParams()
			calm.shardReload, calm.apiWrites, calm.horizon = true, true, 30*time.Minute
			calm.dbCapacity = dbFactor * calm.limitPerSec
			calm.arrivalPerShard = util * calm.limitPerSec / float64(calm.shards) / 2
			calm.blipLength = 0
			if cr := runExecRetryStormSim(t, calm); cr.reloads > 0 || cr.finalOutstanding > float64(4*calm.batchSize*calm.shards) {
				t.Logf("db %.0f%% util %.0f%%: baseline unstable; skipped", dbFactor*100, util*100)
				continue
			}
			for _, blipLen := range []time.Duration{30 * time.Second, 60 * time.Second} {
				worst, never, reloads, timeouts := time.Duration(0), 0, 0, 0
				for seed := uint64(1); seed <= 3; seed++ {
					p := calm
					p.blipLength, p.seed = blipLen, seed
					r := runExecRetryStormSim(t, p)
					if r.recoveredAfter < 0 {
						never++
					} else {
						worst = max(worst, r.recoveredAfter)
					}
					reloads += r.reloads
					timeouts += r.timeouts
				}
				t.Logf("db %.0f%% util %.0f%% blip %-4v: worst recovery %-8v NEVER RECOVERED(30m) %d/3  reloads %5d  timeouts %8d",
					dbFactor*100, util*100, blipLen, worst.Round(time.Second), never, reloads, timeouts)
			}
		}
	}
}

// TestTippingPointDetail runs the tipping case (database ASSUMPTION at 97% of the limiter, 90%
// load, 60s blip) for a simulated hour, with the real setting history.shardUpdateMinTasksCompleted
// = 1000 and with a candidate mitigation of 100 (persist the queue position more often, so a reload
// redoes less work). Report-only.
func TestTippingPointDetail(t *testing.T) {
	for _, minTasks := range []int{1000, 100} {
		for seed := uint64(1); seed <= 3; seed++ {
			p := defaultExecSimParams()
			p.shardReload, p.apiWrites, p.horizon = true, true, time.Hour
			p.dbCapacity = 0.97 * p.limitPerSec
			p.arrivalPerShard = 0.90 * p.limitPerSec / float64(p.shards) / 2
			p.blipLength, p.seed = 60*time.Second, seed
			p.shardUpdateMinTasks = minTasks
			t.Logf("shardUpdateMinTasks=%-4d seed %d: %s", minTasks, seed, formatExecSim(runExecRetryStormSim(t, p)))
		}
	}
}

// TestTippingPointLimiterBelowDatabase tests the classic mitigation for the tipping case: keep what
// the limiter admits below what the database can serve, so the redo burst queues at the limiter
// (ResourceExhausted: "not written", no reload) instead of timing out at the database (ambiguous,
// reload). Same absolute load and the same 60s blip as TestTippingPointDetail. Report-only.
func TestTippingPointLimiterBelowDatabase(t *testing.T) {
	for _, limit := range []float64{500, 470} {
		for seed := uint64(1); seed <= 3; seed++ {
			p := defaultExecSimParams()
			p.shardReload, p.apiWrites, p.horizon = true, true, 30*time.Minute
			p.dbCapacity = 0.97 * 500 // the database: 485 calls/s, as in the tipping case
			p.arrivalPerShard = 0.90 * 500 / float64(p.shards) / 2
			p.limitPerSec, p.burst = limit, limit
			p.blipLength, p.seed = 60*time.Second, seed
			t.Logf("limiter %3.0f/s vs database 485/s, seed %d: %s", limit, seed, formatExecSim(runExecRetryStormSim(t, p)))
		}
	}
}

// TestClientRetryLayerHypothesis tests hypothesis 5: the persistence client has its own retry layer
// under every queue retry path, with its own transient group (Unavailable and DataLoss, not
// ResourceExhausted). During a blip each failed call is attempted twice. Does that layer change
// recovery or the tipping point? Same cases as the earlier hypotheses, layer off vs on. Report-only.
func TestClientRetryLayerHypothesis(t *testing.T) {
	type tc struct {
		name     string
		dbFactor float64
		util     float64
		blip     time.Duration
	}
	for _, c := range []tc{
		{"no DB timeouts, util 95%, 30s blip", 0, 0.95, 30 * time.Second},
		{"DB 97%, util 90%, 30s blip", 0.97, 0.90, 30 * time.Second},
		{"DB 97%, util 90%, 60s blip", 0.97, 0.90, 60 * time.Second},
		{"DB 99%, util 90%, 30s blip", 0.99, 0.90, 30 * time.Second},
	} {
		for _, layer := range []bool{false, true} {
			base := defaultExecSimParams()
			base.shardReload, base.apiWrites, base.horizon = true, true, 30*time.Minute
			base.dbCapacity = c.dbFactor * base.limitPerSec
			base.arrivalPerShard = c.util * base.limitPerSec / float64(base.shards) / 2
			base.clientRetry = layer
			calm := base
			calm.blipLength = 0
			if cr := runExecRetryStormSim(t, calm); cr.reloads > 0 || cr.finalOutstanding > float64(4*calm.batchSize*calm.shards) {
				t.Logf("%-36s client retry %-5v: baseline unstable; skipped", c.name, layer)
				continue
			}
			worst, never, reloads, timeouts, retries := time.Duration(0), 0, 0, 0, 0
			for seed := uint64(1); seed <= 3; seed++ {
				p := base
				p.blipLength, p.seed = c.blip, seed
				r := runExecRetryStormSim(t, p)
				if r.recoveredAfter < 0 {
					never++
				} else {
					worst = max(worst, r.recoveredAfter)
				}
				reloads += r.reloads
				timeouts += r.timeouts
				retries += r.clientRetries
			}
			t.Logf("%-36s client retry %-5v: worst recovery %-8v NEVER RECOVERED(30m) %d/3  reloads %5d  timeouts %8d  client retries %8d",
				c.name, layer, worst.Round(time.Second), never, reloads, timeouts, retries)
		}
	}
}

// TestAdaptiveLimiterHypothesis (Cassandra-shaped errors; TestPostgresShapedHypotheses has the
// SQL version, where the limiter mostly does not fire) tests hypothesis 6: the server already has a mitigation for the
// hypothesis 4 loop, the health-based adaptive limiter, off by default. When the share of calls
// that time out passes an operator-chosen threshold, it lowers the limiter (real defaults: by 0.3
// per 10s, to no lower than 0.8x), so overflow becomes ResourceExhausted ("not written", no
// reload) instead of a timeout. Does turning it on prevent the wedge? Two databases: one 3% below
// the limiter (inside the 0.8x floor) and one 22% below (outside it). Report-only.
//
// The signal is measured where the server measures it: the metrics client wraps the rate-limited
// client, so the limiter's own rejections are recorded calls that count as healthy. During a
// retry storm they dilute the timeout ratio, so thresholds well below 1% are tried.
func TestAdaptiveLimiterHypothesis(t *testing.T) {
	cases := []struct {
		name           string
		dbFactor, util float64
	}{
		{"DB 97% of limiter, util 90%, 60s blip", 0.97, 0.90},
		{"DB 78% of limiter, util 70%, 60s blip", 0.78, 0.70},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, threshold := range []float64{0, 0.001, 0.003, 0.01} {
				base := defaultExecSimParams()
				base.shardReload, base.apiWrites, base.horizon = true, true, 30*time.Minute
				base.dbCapacity = c.dbFactor * base.limitPerSec
				base.arrivalPerShard = c.util * base.limitPerSec / float64(base.shards) / 2
				base.healthErrorThreshold = threshold
				base.divergeAt = 200_000
				label := "off"
				if threshold > 0 {
					label = fmt.Sprintf("on, threshold %.3f", threshold)
				}
				calm := base
				calm.blipLength = 0
				if cr := runExecRetryStormSim(t, calm); cr.reloads > 0 || cr.finalOutstanding > float64(4*calm.batchSize*calm.shards) {
					t.Logf("%-40s adaptive limiter %-20s: baseline unstable; skipped", c.name, label)
					continue
				}
				worst, never, diverged, reloads, timeouts, minMult := time.Duration(0), 0, 0, 0, 0, 1.0
				for seed := uint64(1); seed <= 3; seed++ {
					p := base
					p.blipLength, p.seed = 60*time.Second, seed
					r := runExecRetryStormSim(t, p)
					if r.divergedAt > 0 {
						diverged++
					} else if r.recoveredAfter < 0 {
						never++
					} else {
						worst = max(worst, r.recoveredAfter)
					}
					reloads += r.reloads
					timeouts += r.timeouts
					minMult = min(minMult, r.minMultiplier)
				}
				t.Logf("%-40s adaptive limiter %-20s: worst recovery %-8v NEVER RECOVERED(30m) %d/3  DIVERGED(>200k outstanding) %d/3  reloads %5d  timeouts %8d  lowest multiplier %.1f",
					c.name, label, worst.Round(time.Second), never, diverged, reloads, timeouts, minMult)
			}
		})
	}
}

// TestPostgresShapedHypotheses reruns hypotheses 4 and 6 with the database errors shaped as the
// SQL stores return them, because the #11547 incident ran on Aurora PostgreSQL: a timeout outside
// a transaction is Unavailable (in the transient group, so the fix throttles it; the health signal
// ignores it), and a transaction timeout is a wrapped context deadline (outside the group; the
// shard treats it as ambiguous; the health signal counts it). Report-only.
func TestPostgresShapedHypotheses(t *testing.T) {
	run := func(t *testing.T, label string, util, dbFactor float64, blip time.Duration, threshold float64) {
		base := defaultExecSimParams()
		base.shardReload, base.apiWrites, base.horizon = true, true, 30*time.Minute
		base.sqlBackend, base.divergeAt = true, 200_000
		base.dbCapacity = dbFactor * base.limitPerSec
		base.arrivalPerShard = util * base.dbCapacity / float64(base.shards) / 2
		base.healthErrorThreshold = threshold
		calm := base
		calm.blipLength = 0
		if cr := runExecRetryStormSim(t, calm); cr.reloads > 0 || cr.finalOutstanding > float64(4*calm.batchSize*calm.shards) {
			t.Logf("%-58s: baseline unstable; skipped", label)
			return
		}
		worst, never, diverged, reloads, timeouts, minMult := time.Duration(0), 0, 0, 0, 0, 1.0
		for seed := uint64(1); seed <= 3; seed++ {
			p := base
			p.blipLength, p.seed = blip, seed
			r := runExecRetryStormSim(t, p)
			switch {
			case r.divergedAt > 0:
				diverged++
			case r.recoveredAfter < 0:
				never++
			default:
				worst = max(worst, r.recoveredAfter)
			}
			reloads += r.reloads
			timeouts += r.timeouts
			minMult = min(minMult, r.minMultiplier)
		}
		t.Logf("%-58s: worst recovery %-8v NEVER(30m) %d/3  DIVERGED %d/3  reloads %5d  timeouts %7d  lowest multiplier %.1f",
			label, worst.Round(time.Second), never, diverged, reloads, timeouts, minMult)
	}
	// Load is a fraction of what the DATABASE can serve, so every row is stable without a blip;
	// the database is a fraction of what the limiter admits (the gap the assumption is about).
	t.Run("gap sweep", func(t *testing.T) {
		for _, db := range []float64{0.97, 0.85, 0.70, 0.50} {
			for _, blip := range []time.Duration{30 * time.Second, 60 * time.Second} {
				run(t, fmt.Sprintf("DB %3.0f%% of limiter, load 90%% of DB, blip %v", db*100, blip), 0.90, db, blip, 0)
			}
		}
	})
	t.Run("adaptive limiter", func(t *testing.T) {
		for _, threshold := range []float64{0, 0.001, 0.003, 0.01} {
			run(t, fmt.Sprintf("DB 97%%, load 90%% of DB, 60s blip, limiter threshold %.3f", threshold), 0.90, 0.97, 60*time.Second, threshold)
		}
	})
}
