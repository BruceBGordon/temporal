package driver

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

// The history shard holds its I/O semaphore (history.shardIOConcurrency, default 1) for the whole
// of each shard-scoped persistence call, and service/history/shard/context_impl.go declares that
// this is bounded: "ioSemaphore is for the duration of a persistence op which has a persistence
// connection timeout". The bound is the call's context deadline (history.shardIOTimeout, 5s).
// It only holds if the driver gives up when the context does.
//
// Issue #12246 reports a shard wedged after a Postgres primary failover, with the deadlock
// detector naming the io-semaphore, and suspects a read on a connection to the dead primary that
// cancellation never reaches. These tests check that suspicion for each Postgres driver
// Temporal ships, through Temporal's own driver constructors, against a server that accepts
// the connection and then goes silent mid-statement: what a client sees of a primary that
// stopped answering, before the kernel's TCP timers give up on it (minutes).

// silentPostgres speaks enough of the wire protocol to connect and answer pings (simple
// queries), then never answers a statement sent with the extended protocol (as every
// parameterized query is). CancelRequests are accepted and ignored, as by a dead primary.
func silentPostgres(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	msg := func(typ byte, body []byte) []byte {
		b := []byte{typ, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(b[1:], uint32(len(body)+4))
		return append(b, body...)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				var hdr [8]byte
				if _, err := io.ReadFull(c, hdr[:]); err != nil {
					return
				}
				if binary.BigEndian.Uint32(hdr[4:]) == 80877102 { // CancelRequest: ignored
					_, _ = io.Copy(io.Discard, c)
					return
				}
				if _, err := io.CopyN(io.Discard, c, int64(binary.BigEndian.Uint32(hdr[:4]))-8); err != nil {
					return
				}
				var out []byte
				out = append(out, msg('R', []byte{0, 0, 0, 0})...)             // AuthenticationOk
				out = append(out, msg('K', []byte{0, 0, 0, 1, 0, 0, 0, 2})...) // BackendKeyData
				out = append(out, msg('Z', []byte{'I'})...)                    // ReadyForQuery
				if _, err := c.Write(out); err != nil {
					return
				}
				for {
					var h [5]byte
					if _, err := io.ReadFull(c, h[:]); err != nil {
						return
					}
					if _, err := io.CopyN(io.Discard, c, int64(binary.BigEndian.Uint32(h[1:]))-4); err != nil {
						return
					}
					switch h[0] {
					case 'Q': // simple query (ping): EmptyQueryResponse, ReadyForQuery
						if _, err := c.Write(append(msg('I', nil), msg('Z', []byte{'I'})...)); err != nil {
							return
						}
					case 'X':
						return
					default: // extended protocol: go silent, keep the connection open
						_, _ = io.Copy(io.Discard, c)
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestStatementHonorsContextDeadlineOnSilentServer(t *testing.T) {
	const deadline = 300 * time.Millisecond
	grace := 3 * time.Second
	if g, err := time.ParseDuration(os.Getenv("SILENT_SERVER_GRACE")); err == nil {
		grace = g // e.g. 20s: longer than history.shardIOTimeout (5s) and lib/pq's cancel dial (10s)
	}
	for _, d := range []struct {
		plugin string
		create func(dsn string) (*sqlx.DB, error)
		// knownGap: the driver is known to ignore the deadline here (a ledger entry, with the
		// reason). The test pins it, so a change in either direction is noticed.
		knownGap string
	}{
		{"postgres12 (lib/pq)", (&PQDriver{}).CreateConnection,
			"lib/pq answers a context deadline by sending a CancelRequest on a new connection; it " +
				"never sets a deadline on or closes the blocked one (conn_go18.go watchCancel), so the " +
				"statement waits for a server that will not answer. Candidate mechanism for #12246"},
		{"postgres12_pgx (pgx)", (&PGXDriver{}).CreateConnection, ""},
	} {
		t.Run(d.plugin, func(t *testing.T) {
			db, err := d.create(fmt.Sprintf("postgres://temporal@%s/temporal?sslmode=disable", silentPostgres(t)))
			require.NoError(t, err)
			// Close in the background: with a statement still blocked, Close waits for it.
			t.Cleanup(func() { go func() { _ = db.Close() }() })

			ctx, cancel := context.WithTimeout(context.Background(), deadline)
			defer cancel()
			start := time.Now()
			done := make(chan error, 1)
			go func() {
				_, err := db.ExecContext(ctx, "UPDATE shards SET range_id = $1 WHERE shard_id = $2", 2, 1)
				done <- err
			}()
			select {
			case err := <-done:
				t.Logf("returned after %v: %v", time.Since(start).Round(time.Millisecond), err)
				require.Error(t, err)
				if d.knownGap != "" {
					t.Fatalf("STALE: %s now honors the deadline; remove the known gap", d.plugin)
				}
			case <-time.After(deadline + grace):
				if d.knownGap == "" {
					t.Fatalf("NEW GAP: still blocked %v after a %v deadline: a caller holding the shard's "+
						"io-semaphore would hold it until the server answers or the kernel gives up", deadline+grace, deadline)
				}
				t.Logf("known gap: still blocked %v after a %v deadline. %s", deadline+grace, deadline, d.knownGap)
			}
		})
	}
}
