package sql

import (
	"context"
	stdsql "database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

// The SQL history store maps a timed-out history append to AppendHistoryTimeoutError, which the
// server declares safe to retry blindly (shard.ContextImpl.handleWriteErrorLocked) and "not
// started" (persistence.OperationPossiblySucceeded). Any other error becomes Unavailable, which
// the shard treats as "may have been written" and answers by re-acquiring the shard.
//
// These tests check that mapping against what really reaches it.

// stallingPostgres is a minimal Postgres wire-protocol server: it completes the startup
// handshake and then never answers a query, so a client's context deadline expires mid-query,
// exactly as against an overloaded database.
func stallingPostgres(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				var hdr [8]byte
				if _, err := io.ReadFull(c, hdr[:]); err != nil {
					return
				}
				n := binary.BigEndian.Uint32(hdr[:4])
				if code := binary.BigEndian.Uint32(hdr[4:]); code == 80877102 { // CancelRequest
					return
				}
				if _, err := io.CopyN(io.Discard, c, int64(n)-8); err != nil {
					return
				}
				msg := func(typ byte, body []byte) []byte {
					b := []byte{typ, 0, 0, 0, 0}
					binary.BigEndian.PutUint32(b[1:], uint32(len(body)+4))
					return append(b, body...)
				}
				var out []byte
				out = append(out, msg('R', []byte{0, 0, 0, 0})...)             // AuthenticationOk
				out = append(out, msg('K', []byte{0, 0, 0, 1, 0, 0, 0, 2})...) // BackendKeyData
				out = append(out, msg('Z', []byte{'I'})...)                    // ReadyForQuery
				if _, err := c.Write(out); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, c) // read queries, never answer
			}(conn)
		}
	}()
	return ln.Addr().String()
}

// driverTimeoutError returns the error the Postgres driver Temporal uses (pgx v5 through
// database/sql, as sqlplugin/postgresql/driver/pgx.go opens it) when a query's deadline expires.
func driverTimeoutError(t *testing.T) error {
	t.Helper()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("postgres://temporal@%s/temporal?sslmode=disable", stallingPostgres(t)))
	require.NoError(t, err)
	db := stdsql.OpenDB(stdlib.GetConnector(*cfg))
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx) // handshake done (no ping: it is a query): the deadline below expires mid-query
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	qctx, qcancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer qcancel()
	_, err = conn.ExecContext(qctx, "INSERT INTO history_node VALUES ($1)", 1)
	require.Error(t, err)
	return err
}

// TestPostgresDriverWrapsDeadline documents the driver's behavior the mapping depends on: the
// deadline is wrapped, so == comparisons miss it and errors.Is finds it.
func TestPostgresDriverWrapsDeadline(t *testing.T) {
	err := driverTimeoutError(t)
	t.Logf("driver error: %T: %v", err, err)
	require.NotEqual(t, context.DeadlineExceeded, err, "pgx returns the deadline wrapped")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

type timeoutDB struct {
	sqlplugin.DB
	nodeErr, treeErr error
}

func (d *timeoutDB) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (stdsql.Result, error) {
	return driverResult(1), d.nodeErr
}
func (d *timeoutDB) IsDupEntryError(error) bool { return false }
func (d *timeoutDB) BeginTx(context.Context) (sqlplugin.Tx, error) {
	return &timeoutTx{db: d}, nil
}

type timeoutTx struct {
	sqlplugin.Tx
	db *timeoutDB
}

func (x *timeoutTx) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (stdsql.Result, error) {
	return driverResult(1), nil
}
func (x *timeoutTx) InsertIntoHistoryTree(context.Context, *sqlplugin.HistoryTreeRow) (stdsql.Result, error) {
	return driverResult(1), x.db.treeErr
}
func (x *timeoutTx) Rollback() error { return nil }
func (x *timeoutTx) Commit() error   { return nil }

type driverResult int64

func (r driverResult) LastInsertId() (int64, error) { return 0, nil }
func (r driverResult) RowsAffected() (int64, error) { return int64(r), nil }

func appendRequest(newBranch bool) *p.InternalAppendHistoryNodesRequest {
	return &p.InternalAppendHistoryNodesRequest{
		IsNewBranch: newBranch,
		BranchInfo:  &persistencespb.HistoryBranch{TreeId: uuid.NewString(), BranchId: uuid.NewString()},
		TreeInfo:    &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		Node: p.InternalHistoryNode{
			NodeID: 1, TransactionID: 1,
			Events: &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3},
		},
		ShardID: 1,
	}
}

// TestAppendHistoryTimeoutIsRetryable: a timed-out history append must come back as
// AppendHistoryTimeoutError, not as an ambiguous error that makes the shard re-acquire.
func TestAppendHistoryTimeoutIsRetryable(t *testing.T) {
	realTimeout := driverTimeoutError(t)
	cases := []struct {
		name string
		db   *timeoutDB
		new  bool
	}{
		{"existing branch, Postgres driver timeout (wrapped deadline)", &timeoutDB{nodeErr: realTimeout}, false},
		{"existing branch, bare context deadline", &timeoutDB{nodeErr: context.DeadlineExceeded}, false},
		{"new branch, Postgres driver timeout on the tree insert", &timeoutDB{treeErr: realTimeout}, true},
		{"new branch, bare context deadline on the tree insert", &timeoutDB{treeErr: context.DeadlineExceeded}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := &sqlExecutionStore{SqlStore: NewSQLStore(c.db, log.NewNoopLogger(), nil)}
			err := store.AppendHistoryNodes(context.Background(), appendRequest(c.new))
			var timeoutErr *p.AppendHistoryTimeoutError
			require.ErrorAsf(t, err, &timeoutErr,
				"got %T (%v): persistence.OperationPossiblySucceeded=%v, so the shard would re-acquire",
				err, err, p.OperationPossiblySucceeded(err))
		})
	}
}
