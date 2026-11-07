// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"errors"
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

// After an install that leaves the log store's last index below the
// snapshot, there is nothing new to snapshot. The unsigned delta in
// shouldSnapshot wrapped, so a snapshot was due at every interval.
func TestRaft_ShouldSnapshot_AfterInstallAboveLog(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			f := newTestFollowerOn(t, st.new(), 1, confLog(), cmd(2, 1, "a"), cmd(3, 1, "b"))
			f.setTrailingLogs(t, 2)
			f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 3, PrevLogTerm: 1, LeaderCommitIndex: 3})
			f.installSnapshot(t, 2, 10, 2)
			last, err := f.store.LastIndex()
			require.NoError(t, err)
			require.Less(t, last, uint64(10))
			require.False(t, f.r.shouldSnapshot(), "a snapshot is due right after an install at 10 with the store's last index at %d", last)
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

var errInjected = errors.New("injected log store failure")

// faultyLogStore fails one call of method with errInjected once armed, and
// then behaves normally. GetLog fails only at getLogAt. A failing DeleteRange
// first deletes [min, deleteUpTo] when deleteUpTo is set, like a delete that
// was interrupted after its first chunk.
type faultyLogStore struct {
	LogStore
	mu         sync.Mutex
	armed      bool
	method     string
	getLogAt   uint64
	deleteUpTo uint64
}

func (s *faultyLogStore) arm() {
	s.mu.Lock()
	s.armed = true
	s.mu.Unlock()
}

// fail reports whether this call of method fails, and disarms if so.
func (s *faultyLogStore) fail(method string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.armed || s.method != method {
		return false
	}
	s.armed = false
	return true
}

func (s *faultyLogStore) FirstIndex() (uint64, error) {
	if s.fail("FirstIndex") {
		return 0, errInjected
	}
	return s.LogStore.FirstIndex()
}

func (s *faultyLogStore) LastIndex() (uint64, error) {
	if s.fail("LastIndex") {
		return 0, errInjected
	}
	return s.LogStore.LastIndex()
}

func (s *faultyLogStore) GetLog(index uint64, log *Log) error {
	if index == s.getLogAt && s.fail("GetLog") {
		return errInjected
	}
	return s.LogStore.GetLog(index, log)
}

func (s *faultyLogStore) DeleteRange(min, max uint64) error {
	if s.fail("DeleteRange") {
		if s.deleteUpTo >= min {
			if err := s.LogStore.DeleteRange(min, s.deleteUpTo); err != nil {
				return err
			}
		}
		return errInjected
	}
	return s.LogStore.DeleteRange(min, max)
}

// armingFSM arms store when the snapshot is restored, which NewRaft does
// right before it checks the log below the snapshot. Earlier calls in NewRaft
// (the last index and entry) see a healthy store.
type armingFSM struct {
	recordingFSM
	store *faultyLogStore
}

func (f *armingFSM) Restore(rc io.ReadCloser) error {
	if f.store != nil {
		f.store.arm()
	}
	return f.recordingFSM.Restore(rc)
}

// restartConf is the configuration in the snapshot and at index 1.
var restartConf = Configuration{Servers: []Server{{Suffrage: Voter, ID: "f", Address: "f"}}}

// restartOnSnapshot runs NewRaft on store with a snapshot at 10 of term 3,
// without starting the background loops, so the store and the Raft state are
// inspected exactly as NewRaft left them.
func restartOnSnapshot(t *testing.T, store LogStore, fsm FSM) (*Raft, error) {
	t.Helper()
	stable := NewInmemStore()
	require.NoError(t, stable.SetUint64(keyCurrentTerm, 3))
	_, trans := NewInmemTransport("f")
	snaps := NewInmemSnapshotStore()
	sink, err := snaps.Create(SnapshotVersionMax, 10, 3, restartConf, 1, trans)
	require.NoError(t, err)
	_, err = sink.Write([]byte("snapshot"))
	require.NoError(t, err)
	require.NoError(t, sink.Close())

	cfg := inmemConfig(t)
	cfg.LocalID = "f"
	cfg.skipStartup = true
	return NewRaft(cfg, fsm, store, stable, snaps, trans)
}

// contents returns every entry in store by index.
func contents(t *testing.T, store LogStore) map[uint64]Log {
	t.Helper()
	first, err := store.FirstIndex()
	require.NoError(t, err)
	last, err := store.LastIndex()
	require.NoError(t, err)
	out := map[uint64]Log{}
	for i := first; i <= last && last > 0; i++ {
		var l Log
		err := store.GetLog(i, &l)
		if errors.Is(err, ErrLogNotFound) {
			continue
		}
		require.NoError(t, err)
		out[i] = l
	}
	return out
}

// interruptedInstallLogs is the log an interrupted install of a snapshot at
// 10 of term 3 leaves: entries 1-2 committed, 3 to below of term 2, and
// (when above is set) entries the term-3 leader appended after the snapshot,
// a command at 11 and a configuration at 12. Each call returns new values.
func interruptedInstallLogs(below uint64, above bool) []*Log {
	logs := []*Log{{Index: 1, Term: 1, Type: LogConfiguration, Data: EncodeConfiguration(restartConf)}, cmd(2, 1, "c-2")}
	for i := uint64(3); i <= below; i++ {
		logs = append(logs, cmd(i, 2, fmt.Sprintf("stale-%d", i)))
	}
	if above {
		next := Configuration{Servers: append(append([]Server(nil), restartConf.Servers...), Server{Suffrage: Nonvoter, ID: "m", Address: "m"})}
		logs = append(logs,
			&Log{Index: 11, Term: 3, Type: LogCommand, Data: []byte("post-11 \x00\xff payload"), Extensions: []byte("ext-11")},
			&Log{Index: 12, Term: 3, Type: LogConfiguration, Data: EncodeConfiguration(next)})
	}
	return logs
}

func byIndex(logs []*Log) map[uint64]Log {
	out := map[uint64]Log{}
	for _, l := range logs {
		out[l.Index] = *l
	}
	return out
}

// A GetLog error at the snapshot index other than ErrLogNotFound says nothing
// about the entry there, so start-up fails and deletes nothing.
func TestRaft_Restart_GetLogErrorAtSnapshotIndexFailsClosed(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			store := &faultyLogStore{LogStore: st.new(), method: "GetLog", getLogAt: 10}
			require.NoError(t, store.StoreLogs(interruptedInstallLogs(10, true)))
			before := contents(t, store)

			_, err := restartOnSnapshot(t, store, &armingFSM{store: store})
			require.ErrorIs(t, err, errInjected)
			require.Equal(t, before, contents(t, store), "the log store changed")
		})
	}
}

// The append fallback to the snapshot's term applies only when the entry at
// the snapshot index is absent, not when reading it fails.
func TestRaft_AppendEntries_GetLogErrorAtSnapshotIndexRejected(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			store := &faultyLogStore{LogStore: st.new(), method: "GetLog", getLogAt: 10}
			f := newTestFollowerOn(t, store, 1, confLog(), cmd(2, 1, "a"), cmd(3, 1, "b"))
			f.setTrailingLogs(t, 2)
			f.appendEntries(t, AppendEntriesRequest{Term: 1, PrevLogEntry: 3, PrevLogTerm: 1, LeaderCommitIndex: 3})
			f.installSnapshot(t, 2, 10, 2)
			f.appendEntries(t, AppendEntriesRequest{Term: 2, PrevLogEntry: 10, PrevLogTerm: 2, LeaderCommitIndex: 10,
				Entries: []*Log{cmd(11, 2, "stale-11")}})

			store.arm()
			req := AppendEntriesRequest{RPCHeader: f.header(), Term: 3, PrevLogEntry: 10, PrevLogTerm: 2, LeaderCommitIndex: 10,
				Entries: []*Log{cmd(11, 3, "leader-11")}}
			var resp AppendEntriesResponse
			require.NoError(t, f.leader.AppendEntries("f", "f", &req, &resp))
			require.False(t, resp.Success, "append accepted although reading the entry at the snapshot index failed")
			require.Equal(t, uint64(2), f.stored(t)[11], "entry 11 replaced")
		})
	}
}

// The entry at the snapshot index decides: of another term, every entry up to
// it goes; of the snapshot's term, the log is kept.
func TestRaft_Restart_EntryAtSnapshotIndex(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name+"/other term", func(t *testing.T) {
			store := st.new()
			require.NoError(t, store.StoreLogs(interruptedInstallLogs(10, false)))

			r, err := restartOnSnapshot(t, store, &recordingFSM{})
			require.NoError(t, err)
			require.Empty(t, contents(t, store), "entries at or below the snapshot kept")
			lastIdx, lastTerm := r.getLastLog()
			require.Equal(t, [2]uint64{0, 0}, [2]uint64{lastIdx, lastTerm}, "last log")
			require.Equal(t, uint64(10), r.LastIndex())
		})
		t.Run(st.name+"/snapshot's term", func(t *testing.T) {
			store := st.new()
			logs := interruptedInstallLogs(9, false)
			logs = append(logs, cmd(10, 3, "c-10"))
			require.NoError(t, store.StoreLogs(logs))
			before := contents(t, store)

			r, err := restartOnSnapshot(t, store, &recordingFSM{})
			require.NoError(t, err)
			require.Equal(t, before, contents(t, store), "a log matching the snapshot changed")
			lastIdx, lastTerm := r.getLastLog()
			require.Equal(t, [2]uint64{10, 3}, [2]uint64{lastIdx, lastTerm}, "last log")
		})
	}
}

// Entries above the snapshot stay exactly as stored, and the configuration
// among them is the latest after start-up.
func TestRaft_Restart_KeepsEntriesAboveSnapshot(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			store := st.new()
			require.NoError(t, store.StoreLogs(interruptedInstallLogs(9, true)))
			want := byIndex(interruptedInstallLogs(9, true)[9:])
			require.Len(t, want, 2)

			r, err := restartOnSnapshot(t, store, &recordingFSM{})
			require.NoError(t, err)
			require.Equal(t, want, contents(t, store))
			lastIdx, lastTerm := r.getLastLog()
			require.Equal(t, [2]uint64{12, 3}, [2]uint64{lastIdx, lastTerm}, "last log")
			require.Equal(t, uint64(12), r.configurations.latestIndex, "latest configuration index")
			require.Len(t, r.configurations.latest.Servers, 2)
		})
	}
}

// A delete interrupted after its first chunk fails start-up; the next start
// finishes it and ends where an uninterrupted start would.
func TestRaft_Restart_InterruptedDropConverges(t *testing.T) {
	for _, st := range testLogStores {
		t.Run(st.name, func(t *testing.T) {
			store := &faultyLogStore{LogStore: st.new(), method: "DeleteRange", deleteUpTo: 5}
			require.NoError(t, store.StoreLogs(interruptedInstallLogs(9, true)))

			_, err := restartOnSnapshot(t, store, &armingFSM{store: store})
			require.ErrorIs(t, err, errInjected)
			partial := contents(t, store)
			require.NotContains(t, partial, uint64(5), "the first chunk was not deleted")
			require.Contains(t, partial, uint64(6), "the delete was not interrupted")

			r, err := restartOnSnapshot(t, store, &recordingFSM{})
			require.NoError(t, err)
			require.Equal(t, byIndex(interruptedInstallLogs(9, true)[9:]), contents(t, store))
			lastIdx, _ := r.getLastLog()
			require.Equal(t, uint64(12), lastIdx, "last log")
		})
	}
}

// A failing call in the check below the snapshot fails start-up with that
// error and leaves the log store as it was.
func TestRaft_Restart_DropErrorsSurface(t *testing.T) {
	for _, st := range testLogStores {
		for _, method := range []string{"FirstIndex", "LastIndex", "DeleteRange"} {
			t.Run(st.name+"/"+method, func(t *testing.T) {
				store := &faultyLogStore{LogStore: st.new(), method: method}
				require.NoError(t, store.StoreLogs(interruptedInstallLogs(9, true)))
				before := contents(t, store)

				_, err := restartOnSnapshot(t, store, &armingFSM{store: store})
				require.ErrorIs(t, err, errInjected)
				require.Equal(t, before, contents(t, store), "the log store changed")
			})
		}
	}
}
