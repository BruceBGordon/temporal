package queues

import (
	"container/heap"
	"math/rand/v2"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
)

// A discrete-event model of issue #11547: many shard queue readers share one persistence rate
// limit while running near saturation, and a brief Unavailable blip hits all of them at once.
//
// The model drives the real retry decision (readerRetryDelay, with the real read retry policy),
// so the same simulation exercises whatever version of that decision is checked out. Time is
// virtual: an hour of simulated time runs in milliseconds.
//
// What is modeled (from the issue text, nothing more):
//   - one token-bucket persistence limiter shared by all shards' task reads
//   - task completion (ack-level advancement, RangeCompleteHistoryTasks) either sharing that
//     limiter or having its own budget
//   - a per-shard cap on loaded-but-unacked tasks: a full reader pauses until acks drain it
//   - task execution: each loaded task needs one persistence call (from the same limiter) before
//     it can be acked; executing tasks is most of the persistence load
//   - steady task arrival near the limiter's capacity, and an Unavailable blip of fixed length
//
// What is not modeled: task execution cost, multiple hosts, shard movement, real queue slices.
// The decision code uses process-global randomness for jitter, so runs are not bit-for-bit
// reproducible; the scenario outcomes are compared with wide margins.

type simParams struct {
	shards           int
	limitPerSec      float64       // persistence limiter rate (tokens/s)
	burst            float64       // limiter bucket size
	arrivalPerShard  float64       // tasks/s arriving per shard
	batchSize        int           // tasks per successful read
	maxPending       int           // loaded-but-unacked cap per shard
	pollBackoff      time.Duration // reader pause when the pending cap is hit
	idlePoll         time.Duration // reader poll when there is no backlog
	ackInterval      time.Duration // ack-level advancement attempt interval
	execRetry        time.Duration // retry delay for a task execution that hit the limiter or a blip
	execTime         time.Duration // time one shard's executor spends per task (paces its requests)
	blipStart        time.Duration
	blipLength       time.Duration
	horizon          time.Duration
	sharedAckLimiter bool
	// noise is the relative spread (e.g. 0.2 = +/-20%) applied to every latency, execution time and
	// ack interval, from a seeded source. Real systems always have this; without it, any fixed
	// delay keeps readers in artificial lockstep forever.
	noise float64
	seed  uint64
}

type simResult struct {
	recoveredAfter time.Duration // time after the blip until total backlog is back to baseline; -1 if never
	peakBacklog    float64
	readErrors     int
	finalBacklog   float64
	ackStarvation  time.Duration // longest gap between successful acks on any shard, after the blip
}

type tokenBucket struct {
	rate, burst, tokens float64
	last                time.Duration
}

func (b *tokenBucket) take(now time.Duration) bool {
	b.tokens = min(b.burst, b.tokens+b.rate*(now-b.last).Seconds())
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

type simEvent struct {
	at    time.Duration
	shard int
	kind  int // 0 read, 1 ack, 2 execute
}

type simQueue []simEvent

func (q simQueue) Len() int             { return len(q) }
func (q simQueue) Less(i, j int) bool   { return q[i].at < q[j].at }
func (q simQueue) Swap(i, j int)        { q[i], q[j] = q[j], q[i] }
func (q *simQueue) Push(x any)          { *q = append(*q, x.(simEvent)) }
func (q *simQueue) Pop() any            { o := *q; e := o[len(o)-1]; *q = o[:len(o)-1]; return e }
func (q *simQueue) schedule(e simEvent) { heap.Push(q, e) }

func runRetryStormSim(t testing.TB, p simParams) simResult {
	readLimiter := &tokenBucket{rate: p.limitPerSec, burst: p.burst, tokens: p.burst}
	ackLimiter := readLimiter
	if !p.sharedAckLimiter {
		ackLimiter = &tokenBucket{rate: p.limitPerSec, burst: p.burst, tokens: p.burst}
	}
	overloaded := serviceerror.NewResourceExhausted(enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT, "persistence limit")
	blip := serviceerror.NewUnavailable("database failover")

	backlog := make([]float64, p.shards) // tasks in persistence, not yet loaded
	loaded := make([]int, p.shards)      // loaded, not yet executed
	executed := make([]int, p.shards)    // executed, waiting for ack-level advancement
	lastAck := make([]time.Duration, p.shards)
	retriers := make([]backoff.Retrier, p.shards)
	q := &simQueue{}
	rng := rand.New(rand.NewPCG(p.seed, p.seed^0x9e3779b97f4a7c15))
	vary := func(d time.Duration) time.Duration {
		if p.noise == 0 || d == 0 {
			return d
		}
		return time.Duration(float64(d) * (1 - p.noise + 2*p.noise*rng.Float64()))
	}
	const callLatency = 2 * time.Millisecond
	for s := range p.shards {
		retriers[s] = backoff.NewRetrier(common.CreateReadTaskRetryPolicy(), clock.NewRealTimeSource())
		q.schedule(simEvent{at: time.Duration(s) * time.Millisecond, shard: s, kind: 0})
		q.schedule(simEvent{at: p.ackInterval + time.Duration(s)*time.Millisecond, shard: s, kind: 1})
		q.schedule(simEvent{at: time.Duration(s) * time.Millisecond, shard: s, kind: 2})
	}

	var res simResult
	res.recoveredAfter = -1
	blipEnd := p.blipStart + p.blipLength
	baseline := -1.0
	lastArrival := time.Duration(0)
	inBlip := func(t time.Duration) bool { return t >= p.blipStart && t < blipEnd }

	for q.Len() > 0 {
		e := heap.Pop(q).(simEvent)
		if e.at > p.horizon {
			break
		}
		now := e.at
		// Fluid task arrival up to now.
		dt := (now - lastArrival).Seconds()
		for s := range p.shards {
			backlog[s] += p.arrivalPerShard * dt
		}
		lastArrival = now

		total := 0.0
		for _, b := range backlog {
			total += b
		}
		res.peakBacklog = max(res.peakBacklog, total)
		if now < p.blipStart {
			baseline = max(baseline, total)
		} else if p.blipLength > 0 && now >= blipEnd && res.recoveredAfter < 0 && total <= baseline*1.1+float64(p.shards) {
			res.recoveredAfter = now - blipEnd
		}

		s := e.shard
		switch e.kind {
		case 0: // read
			if loaded[s]+executed[s] >= p.maxPending {
				q.schedule(simEvent{at: now + p.pollBackoff, shard: s, kind: 0})
				continue
			}
			var err error
			switch {
			case inBlip(now):
				err = blip
			case !readLimiter.take(now):
				err = overloaded
			default:
				// within budget and no blip: the read goes ahead
			}
			if err != nil {
				res.readErrors++
				q.schedule(simEvent{at: now + vary(callLatency) + readerRetryDelay(err, retriers[s]), shard: s, kind: 0})
				continue
			}
			retriers[s].Reset()
			n := min(float64(p.batchSize), backlog[s], float64(p.maxPending-loaded[s]-executed[s]))
			backlog[s] -= n
			loaded[s] += int(n)
			next := p.idlePoll
			if backlog[s] >= 1 {
				next = 0 // more tasks: read again immediately
			}
			q.schedule(simEvent{at: now + vary(callLatency) + next, shard: s, kind: 0})
		case 1: // ack-level advancement
			if !inBlip(now) && ackLimiter.take(now) {
				if now >= blipEnd {
					res.ackStarvation = max(res.ackStarvation, now-lastAck[s])
				}
				lastAck[s] = now
				executed[s] = 0
			}
			q.schedule(simEvent{at: now + vary(p.ackInterval), shard: s, kind: 1})
		case 2: // execute one loaded task
			switch {
			case loaded[s] == 0:
				q.schedule(simEvent{at: now + 50*time.Millisecond, shard: s, kind: 2})
			case inBlip(now) || !readLimiter.take(now):
				q.schedule(simEvent{at: now + vary(p.execRetry), shard: s, kind: 2})
			default:
				loaded[s]--
				executed[s]++
				q.schedule(simEvent{at: now + vary(p.execTime), shard: s, kind: 2})
			}
		default:
			t.Fatalf("unknown simulation event kind %d", e.kind)
		}
	}
	for _, b := range backlog {
		res.finalBacklog += b
	}
	return res
}

func defaultSimParams() simParams {
	return simParams{
		shards:          64,
		limitPerSec:     2000,
		burst:           100,
		arrivalPerShard: 25, // 1600 tasks/s of execution against a 2000/s limit: ~80% utilization
		// Real defaults: history.transferTaskBatchSize=100, history.queuePendingTasksMaxCount=10000,
		// history.transferProcessorPollBackoffInterval=5s, history.transferProcessorUpdateAckInterval=30s.
		batchSize:        100,
		maxPending:       10000,
		pollBackoff:      5 * time.Second,
		idlePoll:         time.Second,
		ackInterval:      30 * time.Second,
		execRetry:        time.Second,
		execTime:         10 * time.Millisecond,
		blipStart:        2 * time.Minute,
		blipLength:       5 * time.Second,
		horizon:          30 * time.Minute,
		sharedAckLimiter: true,
		noise:            0.2,
		seed:             1,
	}
}

// TestRetryStormSim reports how the system recovers from a brief blip near saturation, with
// ack-level advancement sharing the read limiter (as in issue #11547) and with its own budget.
func TestRetryStormSim(t *testing.T) {
	// The model must be stable without a blip, or any "stall" it shows is the model's, not Temporal's.
	calm := defaultSimParams()
	calm.blipLength = 0
	cr := runRetryStormSim(t, calm)
	t.Logf("no blip: final backlog %.0f, peak %.0f, read errors %d", cr.finalBacklog, cr.peakBacklog, cr.readErrors)
	if cr.finalBacklog > 10*float64(calm.shards*calm.batchSize) {
		t.Fatalf("model is unstable without a blip (final backlog %.0f); results would be meaningless", cr.finalBacklog)
	}
	for _, shared := range []bool{true, false} {
		p := defaultSimParams()
		p.sharedAckLimiter = shared
		r := runRetryStormSim(t, p)
		recovered := "never (within 30m)"
		if r.recoveredAfter >= 0 {
			recovered = r.recoveredAfter.String()
		}
		t.Logf("ack budget shared=%-5v  recovered after blip: %-18s  peak backlog: %8.0f  read errors: %7d  longest ack gap after blip: %v  final backlog: %.0f",
			shared, recovered, r.peakBacklog, r.readErrors, r.ackStarvation, r.finalBacklog)
	}
}

// TestRetryStormSimSweep reports recovery across load levels, blip lengths and seeds, to look for
// any regime where the retry decision changes the outcome. Report-only.
func TestRetryStormSimSweep(t *testing.T) {
	for _, util := range []float64{0.80, 0.90, 0.95} {
		for _, blipLen := range []time.Duration{5 * time.Second, 30 * time.Second} {
			worst, errs := time.Duration(0), 0
			never := 0
			for seed := uint64(1); seed <= 3; seed++ {
				p := defaultSimParams()
				p.arrivalPerShard = util * p.limitPerSec / float64(p.shards)
				p.blipLength = blipLen
				p.seed = seed
				r := runRetryStormSim(t, p)
				if r.recoveredAfter < 0 {
					never++
				} else {
					worst = max(worst, r.recoveredAfter)
				}
				errs += r.readErrors
			}
			t.Logf("utilization %.0f%%  blip %-4v  worst recovery %-14v  never recovered %d/3  read errors (3 seeds) %d",
				util*100, blipLen, worst.Round(time.Millisecond), never, errs)
		}
	}
}
