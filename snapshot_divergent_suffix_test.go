// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// End to end (StorageMesh #421): a follower whose divergent, never-committed
// suffix extends past the index of the snapshot the new leader sends it. With
// TrailingLogs as in StorageMesh's tests (32), the install only front-compacts
// a non-monotonic store, so on v1.8.0 the stale tail beyond the snapshot index
// survives. The follower must still catch up, end with the leader's log at
// every index it holds, and never apply a never-committed entry.
func TestRaft_InstallSnapshot_DivergentSuffixPastSnapshot(t *testing.T) {
	conf := inmemConfig(t)
	conf.TrailingLogs = 32
	conf.SnapshotThreshold = 1 << 40
	conf.SnapshotInterval = time.Hour
	mk := func() FSM { return &histFSM{MockFSM: &MockFSM{}} }
	c := MakeClusterCustom(t, &MakeClusterOpts{Peers: 5, Bootstrap: true, Conf: conf, MakeFSMFunc: mk})
	defer c.Close()

	// 1. A committed prefix on every server.
	A := c.Leader()
	for i := 0; i < 10; i++ {
		require.NoError(t, A.Apply([]byte(fmt.Sprintf("good-%d", i)), time.Second).Error())
	}
	waitForCondition(t, "prefix applied everywhere", 5*time.Second, func() bool {
		for _, r := range c.rafts {
			if r.AppliedIndex() < A.getLastIndex() {
				return false
			}
		}
		return true
	})
	prefix := A.getLastIndex()
	F := c.Followers()[0]
	var fFSM *histFSM
	for i, r := range c.rafts {
		if r == F {
			fFSM = c.fsms[i].(*histFSM)
		}
	}

	// 2. Partition {A, F}. A appends entries that reach only F.
	c.Partition([]ServerAddress{A.localAddr, F.localAddr})
	for i := 0; i < 300; i++ {
		A.Apply([]byte(fmt.Sprintf("STALE-%d", i)), 50*time.Millisecond)
	}
	var fLast uint64
	waitForCondition(t, "F to receive stale entries", 5*time.Second, func() bool {
		l := F.getLastIndex()
		stable := l == fLast
		fLast = l
		return stable && l >= prefix+150
	})
	require.NoError(t, A.Shutdown().Error())

	// 3. The majority elects L, which commits fewer entries than F's stale
	// tail, snapshots and compacts below F's last matching entry (prefix).
	var L *Raft
	waitForCondition(t, "new leader on majority side", 5*time.Second, func() bool {
		for _, r := range c.rafts {
			if r != A && r != F && r.State() == Leader {
				L = r
				return true
			}
		}
		return false
	})
	var fut Future
	for i := 0; i < 80; i++ {
		fut = L.Apply([]byte(fmt.Sprintf("new-%d", i)), 5*time.Second)
	}
	require.NoError(t, fut.Error())
	require.NoError(t, L.Snapshot().Error())
	snapIdx, snapTerm := L.getLastSnapshot()
	first, err := L.logs.FirstIndex()
	require.NoError(t, err)

	// The scenario holds: F's stale tail runs past the snapshot index, its
	// entry there conflicts, and L can't reach F's match point by appends.
	require.Greater(t, F.getLastIndex(), snapIdx+32, "F's stale tail doesn't reach past the snapshot")
	var fe Log
	require.NoError(t, F.logs.GetLog(snapIdx, &fe))
	require.NotEqual(t, snapTerm, fe.Term)
	require.Greater(t, first, prefix+1, "L kept the match point; no snapshot would be sent")
	t.Logf("prefix=%d snapshot=%d/t%d L.first=%d F.last=%d (term %d at snapshot index)",
		prefix, snapIdx, snapTerm, first, F.getLastIndex(), fe.Term)

	// 4. F rejoins. It has to install the snapshot and then accept appends.
	c.FullyConnect()
	c.Disconnect(A.localAddr)
	for i := 0; i < 5; i++ {
		fut = c.Leader().Apply([]byte(fmt.Sprintf("after-%d", i)), 5*time.Second)
	}
	require.NoError(t, fut.Error())
	waitForCondition(t, "F to catch up past the snapshot", 20*time.Second, func() bool {
		ld := c.Leader()
		return ld != nil && F.AppliedIndex() >= ld.getLastIndex() && F.getLastIndex() == ld.getLastIndex()
	})

	// 5. F's log matches the leader's at every index both hold. F's store
	// may have a gap below its snapshot (the committed prefix stays).
	ld := c.Leader()
	fSnap, _ := F.getLastSnapshot()
	fFirst, err := F.logs.FirstIndex()
	require.NoError(t, err)
	lFirst, err := ld.logs.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, ld.getLastIndex(), F.getLastIndex())
	for i := max(max(fFirst, lFirst), fSnap+1); i <= ld.getLastIndex(); i++ {
		var a, b Log
		require.NoError(t, F.logs.GetLog(i, &a))
		require.NoError(t, ld.logs.GetLog(i, &b))
		require.Equal(t, b.Term, a.Term, "term differs at index %d", i)
		require.Equal(t, b.Data, a.Data, "data differs at index %d", i)
	}

	// F's FSM state equals the leader's, and F never applied a stale entry.
	for _, e := range fFSM.history() {
		require.False(t, strings.Contains(e, "STALE-"), "F applied a never-committed entry: %s", e)
	}
	for i, r := range c.rafts {
		if r == ld {
			require.Equal(t, getMockFSM(c.fsms[i]).Logs(), fFSM.Logs(), "F's FSM differs from the leader's")
		}
	}
}
