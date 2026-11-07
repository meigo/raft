// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// An install never stores the entry at the snapshot index S. After the
// leader that installed it appends S+1 and dies before committing it, the
// next leader's append with PrevLogEntry S must match S against the
// snapshot, not the log store. Otherwise the follower rejects it, the leader
// falls back to the same snapshot, which the follower keeps along with S+1
// (S is committed here), and the append is rejected again.
func TestRaft_AppendEntries_PrevLogAtSnapshotIndexWithSuffix(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			f := newTestFollowerOn(t, st.new(), 1, confLog(), cmd(2, 1, "a"), cmd(3, 1, "b"))
			f.setTrailingLogs(t, 2)
			f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 3, PrevLogTerm: 1, LeaderCommitIndex: 3})
			f.requireCommitted(t, 3, 3)

			// Lagging, so the leader of term 2 installs its snapshot at 10.
			f.installSnapshot(t, 2, 10, 2)
			require.NotContains(t, f.stored(t), uint64(10), "entry 10 is not stored after the install")

			// That leader appends 11 with 10 committed, and dies before 11 commits.
			f.appendEntries(t, AppendEntriesRequest{Term: 2, PrevLogEntry: 10, PrevLogTerm: 2, LeaderCommitIndex: 10,
				Entries: []*Log{cmd(11, 2, "stale-11")}})
			f.requireCommitted(t, 10, 10)

			// The leader of term 3, with the same snapshot at 10, has its own entry 11.
			req := AppendEntriesRequest{RPCHeader: f.header(), Term: 3, PrevLogEntry: 10, PrevLogTerm: 2, LeaderCommitIndex: 10,
				Entries: []*Log{cmd(11, 3, "leader-11")}}
			var resp AppendEntriesResponse
			require.NoError(t, f.leader.AppendEntries("f", "f", &req, &resp))
			require.True(t, resp.Success, "append after the snapshot index rejected: %+v", resp)
			require.Equal(t, uint64(3), f.stored(t)[11], "entry 11 replaced by the leader's")

			f.appendEntries(t, AppendEntriesRequest{Term: 3, PrevLogEntry: 11, PrevLogTerm: 3, LeaderCommitIndex: 11})
			f.requireCommitted(t, 11, 11)
			f.fsm.waitApplied(t, 11)
			for _, l := range f.fsm.logs() {
				require.NotContains(t, string(l.Data), "stale", "applied a never-committed entry")
			}
		})
	}
}

// fakeMember answers a leader's RPCs like an empty server that accepts
// whatever continues its log, and records every entry it is sent.
type fakeMember struct {
	mu      sync.Mutex
	last    uint64
	entries []Log
}

func (m *fakeMember) serve(trans *InmemTransport, stop <-chan struct{}) {
	for {
		select {
		case rpc := <-trans.Consumer():
			switch req := rpc.Command.(type) {
			case *AppendEntriesRequest:
				m.mu.Lock()
				resp := &AppendEntriesResponse{Term: req.Term, LastLog: m.last}
				if req.PrevLogEntry > m.last {
					resp.NoRetryBackoff = true
				} else {
					for _, e := range req.Entries {
						m.entries = append(m.entries, *e)
						m.last = e.Index
					}
					resp.Success = true
					resp.LastLog = m.last
				}
				m.mu.Unlock()
				rpc.Respond(resp, nil)
			case *InstallSnapshotRequest:
				_, _ = io.Copy(io.Discard, rpc.Reader)
				m.mu.Lock()
				m.last = req.LastLogIndex
				m.mu.Unlock()
				rpc.Respond(&InstallSnapshotResponse{Term: req.Term, Success: true}, nil)
			default:
				rpc.Respond(nil, fmt.Errorf("unexpected rpc %T", rpc.Command))
			}
		case <-stop:
			return
		}
	}
}

func (m *fakeMember) received() []Log {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]Log(nil), m.entries...)
}

// An install publishes and restores the snapshot before it discards the
// entries that conflict with it. A crash in between leaves those entries
// below the snapshot. After a restart, this server as leader must not send
// them to a member behind the snapshot, which would apply them up to the
// leader's commit index before the snapshot reaches it.
func TestRaft_Restart_DropsLogsBelowSnapshotAfterInterruptedInstall(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			store := st.new()
			conf := Configuration{Servers: []Server{{Suffrage: Voter, ID: "f", Address: "f"}}}
			logs := []*Log{{Index: 1, Term: 1, Type: LogConfiguration, Data: EncodeConfiguration(conf)}, cmd(2, 1, "c-2")}
			for i := uint64(3); i <= 6; i++ {
				logs = append(logs, cmd(i, 2, fmt.Sprintf("stale-%d", i)))
			}
			require.NoError(t, store.StoreLogs(logs))
			stable := NewInmemStore()
			require.NoError(t, stable.SetUint64(keyCurrentTerm, 3))

			_, fTrans := NewInmemTransport("f")
			_, mTrans := NewInmemTransport("m")
			fTrans.Connect("m", mTrans)
			mTrans.Connect("f", fTrans)

			// The snapshot at 10 (term 3) the install published before the crash.
			snaps := NewInmemSnapshotStore()
			sink, err := snaps.Create(SnapshotVersionMax, 10, 3, conf, 1, fTrans)
			require.NoError(t, err)
			_, err = sink.Write([]byte("snapshot"))
			require.NoError(t, err)
			require.NoError(t, sink.Close())

			cfg := inmemConfig(t)
			cfg.LocalID = "f"
			cfg.MaxAppendEntries = 2
			cfg.SnapshotThreshold = 1 << 20
			cfg.TrailingLogs = 1 << 20
			r, err := NewRaft(cfg, &recordingFSM{}, store, stable, snaps, fTrans)
			require.NoError(t, err)
			t.Cleanup(func() { _ = r.Shutdown().Error() })
			require.Equal(t, uint64(10), r.LastIndex())

			// Alone in its configuration, f elects itself.
			waitForCondition(t, "f leads", 5*time.Second, func() bool { return r.State() == Leader })

			// An empty server joins; f replicates to it.
			m := &fakeMember{}
			stop := make(chan struct{})
			t.Cleanup(func() { close(stop) })
			go m.serve(mTrans, stop)
			require.NoError(t, r.AddVoter("m", "m", 0, 5*time.Second).Error())
			waitForCondition(t, "m caught up", 5*time.Second, func() bool {
				m.mu.Lock()
				defer m.mu.Unlock()
				return m.last >= 12
			})

			require.NotEmpty(t, m.received(), "the member got no entries")
			var below []string
			for _, e := range m.received() {
				if e.Index <= 10 {
					below = append(below, fmt.Sprintf("%d/term %d %q", e.Index, e.Term, e.Data))
				}
			}
			require.Emptyf(t, below, "the leader sent entries from below its snapshot at 10: %v", below)
		})
	}
}
