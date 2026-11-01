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
	}
}

type execSimResult struct {
	recoveredAfter   time.Duration // after the blip, until outstanding work is back to baseline; -1 if never
	peakOutstanding  float64
	finalOutstanding float64
	failedCalls      int // persistence calls that failed (reads + executions)
	execAttempts     int
}

type execEvent struct {
	at    time.Duration
	kind  int // 0 read, 1 ack, 2 worker done, 3 reschedule fire
	shard int
	ex    Executable
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

func (x simExecutor) Execute(_ context.Context, _ Executable) ExecuteResponse {
	x.s.res.execAttempts++
	err := x.s.persistenceCall(x.s.limiter)
	if err != nil {
		x.s.res.failedCalls++
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
	x.s.schedule(execEvent{at: max(x.s.now, at.Sub(x.s.t0)), kind: 3, ex: e})
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
	s.loaded[sh]--
	s.executed[sh]++
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
		s.schedule(execEvent{at: time.Duration(sh) * time.Millisecond, kind: 0, shard: sh})
		s.schedule(execEvent{at: p.ackInterval + time.Duration(sh)*time.Millisecond, kind: 1, shard: sh})
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
			if s.loaded[sh]+s.executed[sh] >= p.maxPending {
				s.schedule(execEvent{at: s.now + p.pollBackoff, kind: 0, shard: sh})
				continue
			}
			if err := s.persistenceCall(s.limiter); err != nil {
				s.res.failedCalls++
				s.schedule(execEvent{at: s.now + readerRetryDelay(err, s.retriers[sh]), kind: 0, shard: sh})
				continue
			}
			s.retriers[sh].Reset()
			n := int(min(float64(p.batchSize), s.backlog[sh], float64(p.maxPending-s.loaded[sh]-s.executed[sh])))
			s.backlog[sh] -= float64(n)
			for range n {
				ex := newTask()
				s.shardOf[ex] = sh
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
			s.schedule(execEvent{at: s.now + s.vary(2*time.Millisecond) + next, kind: 0, shard: sh})
		case 1: // ack-level advancement
			if s.persistenceCall(s.ackLimiter) == nil {
				s.executed[e.shard] = 0
			} else {
				s.res.failedCalls++
			}
			s.schedule(execEvent{at: s.now + s.vary(p.ackInterval), kind: 1, shard: e.shard})
		case 2: // a worker finishes running one executable, as FIFOScheduler.executeTask does
			ex := e.ex
			if err := ex.Execute(); err != nil {
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
			e.ex.SetScheduledTime(s.ts.Now())
			if !sched.TrySubmit(e.ex) {
				s.schedule(execEvent{at: s.now + backoff.Jitter(2*time.Second, 0.5), kind: 3, ex: e.ex})
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
	return fmt.Sprintf("recovered after blip: %-10s peak outstanding: %7.0f  final: %6.0f  failed persistence calls: %7d  execution attempts: %8d",
		rec, r.peakOutstanding, r.finalOutstanding, r.failedCalls, r.execAttempts)
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
