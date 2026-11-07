// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"testing"

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
