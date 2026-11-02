package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
)

// The flaw behind #10841, searched for elsewhere: a check that cannot see what was deleted while
// something still depends on it. Queue v1 (namespace replication and its DLQ) picks the next
// message ID as the highest existing ID + 1, and restarts at 0 when the table is empty. Queue v2
// guards this ("Never actually delete the last message", queue_v2_util.go GetDeleteRange); v1 does
// not. The DLQ's Purge and Merge delete through the given ID and move the DLQ ack level there, and
// reads and merges only return messages after the ack level.
//
// So after a purge (or a merge) that empties the DLQ, the next message sent to it is written with
// an ID below the ack level. This test checks whether the DLQ handler can still see it, through
// the real namespace replication queue and DLQ handler on a real SQLite store.
//
// Known gap (a ledger entry): it cannot. The test pins that and fails with STALE when it changes.
func TestNamespaceReplicationDLQAfterFullPurge(t *testing.T) {
	ctx := context.Background()
	factory := sql.NewFactory(*NewSQLiteMemoryConfig(), resolver.NewNoopResolver(), testSQLiteClusterName,
		log.NewNoopLogger(), metrics.NoopMetricsHandler, serialization.NewSerializer())
	defer factory.Close()
	queue, err := factory.NewQueue(persistence.NamespaceReplicationQueueType)
	require.NoError(t, err)
	nrq, err := persistence.NewNamespaceReplicationQueue(queue, serialization.NewSerializer(), "active",
		metrics.NoopMetricsHandler, log.NewNoopLogger())
	require.NoError(t, err)
	dlq := nsreplication.NewDLQMessageHandler(nil, nrq, log.NewNoopLogger())

	task := func() *replicationspb.ReplicationTask {
		return &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK}
	}
	readAll := func() []*replicationspb.ReplicationTask {
		msgs, _, err := dlq.Read(ctx, persistence.MaxQueueMessageID, 100, nil)
		require.NoError(t, err)
		return msgs
	}

	for range 3 {
		require.NoError(t, nrq.PublishToDLQ(ctx, task()))
	}
	before := readAll()
	require.Len(t, before, 3, "the DLQ handler sees the first messages")
	last := before[len(before)-1].SourceTaskId
	t.Logf("DLQ before purge: %d messages, last ID %d", len(before), last)

	require.NoError(t, dlq.Purge(ctx, last)) // the operator purges everything
	require.Empty(t, readAll())
	ackLevel, err := nrq.GetDLQAckLevel(ctx)
	require.NoError(t, err)

	require.NoError(t, nrq.PublishToDLQ(ctx, task())) // a new failure lands in the DLQ
	raw, _, err := nrq.GetMessagesFromDLQ(ctx, persistence.EmptyQueueMessageID, persistence.MaxQueueMessageID, 100, nil)
	require.NoError(t, err)
	require.Len(t, raw, 1, "the new message is in the store")
	t.Logf("after purge: ack level %d, new message stored with ID %d", ackLevel, raw[0].SourceTaskId)

	visible := readAll()
	if len(visible) == 1 {
		t.Fatalf("STALE: the DLQ handler now sees a message sent after a full purge; update the ledger")
	}
	t.Logf("known gap: the DLQ handler sees %d messages; the new message (ID %d) is below the ack level (%d), "+
		"so Read and Merge never return it", len(visible), raw[0].SourceTaskId, ackLevel)
}
