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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
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
	shards           int
	limitPerSec      float64
	burst            float64
	arrivalPerShard  float64
	batchSize        int
	maxPending       int
	pollBackoff      time.Duration
	idlePoll         time.Duration
	ackInterval      time.Duration
	workers          int
	queueCap         int
	execTime         time.Duration
	blipStart        time.Duration
	blipLength       time.Duration
	horizon          time.Duration
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
	peakOutstanding  float64
	finalOutstanding float64
	failedCalls      int // persistence calls that failed (reads + executions)
	execAttempts     int
	reloads          int // shard re-acquisitions
	reworkTasks      int // tasks re-read because the saved queue position was behind
	failedWrites     int // API writes that failed (rejected or ambiguous)
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
	busy       int
	waiting    []Executable
	shardOf    map[Executable]int
	backlog    []float64
	loaded     []int
	executed   []int
	retriers   []backoff.Retrier
	res        execSimResult

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

// persistenceCall decides the outcome of one persistence call at the current virtual time.
func (s *execSim) persistenceCall(l *tokenBucket) error {
	switch {
	case s.inBlip():
		return serviceerror.NewUnavailable("database failover")
	case !l.take(s.now):
		return &serviceerror.ResourceExhausted{
			Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT,
			Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			Message: "persistence limit",
		}
	}
	return nil
}

// --- fakes for the executable's collaborators, all on the virtual clock

type simExecutor struct{ s *execSim }

func (x simExecutor) Execute(_ context.Context, e Executable) ExecuteResponse {
	x.s.res.execAttempts++
	err := x.s.persistenceCall(x.s.limiter)
	if err != nil {
		x.s.res.failedCalls++
		if x.s.p.writeFraction > 0 && x.s.rng.Float64() < x.s.p.writeFraction && isAmbiguousWriteError(err) {
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

// isAmbiguousWriteError mirrors shard.ContextImpl.handleWriteErrorLocked: ResourceExhausted means
// "definitely not written"; Unavailable falls to the default branch and the shard re-acquires.
func isAmbiguousWriteError(err error) bool {
	var u *serviceerror.Unavailable
	return errors.As(err, &u)
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

		switch e.kind {
		case 0: // shard reader
			sh := e.shard
			if e.gen != s.gen[sh] || s.reloading[sh] {
				continue // this reader belongs to a shard generation that was reloaded
			}
			if s.loaded[sh]+s.executed[sh] >= p.maxPending {
				s.schedule(execEvent{at: s.now + p.pollBackoff, kind: 0, shard: sh, gen: s.gen[sh]})
				continue
			}
			if err := s.persistenceCall(s.limiter); err != nil {
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
				s.executed[sh] = 0                                      // ShrinkSlices: completed tasks leave memory regardless of what follows
				if err := s.persistenceCall(s.ackLimiter); err != nil { // RangeCompleteHistoryTasks
					s.res.failedCalls++
					next = s.checkpointRetrier[sh].NextBackOff(err)
				} else if s.tasksSinceUpdate[sh] >= p.shardUpdateMinTasks || s.now-s.lastShardUpdate[sh] >= p.shardUpdateMinInterval {
					// SetQueueState -> updateShardInfo -> UpdateShard
					if err := s.persistenceCall(s.ackLimiter); err != nil {
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
				if err := s.persistenceCall(s.limiter); err != nil {
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
			err := s.persistenceCall(s.limiter)
			if err == nil {
				err = s.persistenceCall(s.limiter)
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
			t.Log(fmt.Sprintf("utilization %.0f%%  blip %-4v  worst recovery %-10v  never recovered %d/3  failed calls %8d  execution attempts %9d",
				util*100, blipLen, worst.Round(time.Millisecond), never, failed, attempts))
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
		t.Log(fmt.Sprintf("[%s] no blip at 95%%: stable=%v  %s", c.name, stable, formatExecSim(cr)))
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
				t.Log(fmt.Sprintf("[%s] util %.0f%% blip %-4v worst recovery %-12v never(30m) %d/3  reloads %4d  rework tasks %7d",
					c.name, util*100, blipLen, worst.Round(time.Second), never, reloads, rework))
				if c.reload && reloads == 0 {
					t.Errorf("[%s] the reload mechanism never fired; this result says nothing about hypothesis 1", c.name)
				}
			}
		}
	}
}
