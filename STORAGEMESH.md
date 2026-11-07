# StorageMesh patches

This fork of [hashicorp/raft](https://github.com/hashicorp/raft) carries safety
fixes that StorageMesh needs before upstream has them. The branch
`storagemesh-v1.8.0` is the upstream tag `v1.8.0`; the patches sit on top of it
(branch `storagemesh-fixes`), one commit each with its regression test.

| Patch | Files | Upstream |
| --- | --- | --- |
| A follower caps its commit index at the last entry the AppendEntries request covers (`PrevLogEntry + len(Entries)`), not at its own last index, and never lowers it. Otherwise an append without entries can make it commit and apply a stale, never-committed tail. | `raft.go`, `follower_commit_test.go` | No issue yet; [#721](https://github.com/hashicorp/raft/issues/721) cites the same line. |
| InstallSnapshot discards the follower's entries above its commit index unless its log agrees with the snapshot (Raft paper, Figure 13, steps 6 and 7). Otherwise divergent entries survive the install and are later replicated and applied, or make every following AppendEntries fail. The last log is then the store's last entry, not the snapshot index (so compaction counts trailing logs from the store's real end), and compaction leaves an empty store alone; otherwise a follower whose log ended below the snapshot could make `InmemStore` loop for ever on the next install. | `raft.go`, `snapshot.go`, `snapshot_divergence_test.go`, `snapshot_lagging_test.go` | [#721](https://github.com/hashicorp/raft/issues/721), [#697](https://github.com/hashicorp/raft/issues/697), [#704](https://github.com/hashicorp/raft/pull/704) |
| An AppendEntries whose `PrevLogEntry` is the last snapshot's index, with that entry absent from the log store (an install never stores it), is matched against the snapshot's term, as the leader's `setPreviousLog` does. Otherwise a follower holding entries beyond its snapshot rejects it, and the leader's fallback to the same snapshot doesn't help. | `raft.go`, `snapshot_boundary_test.go` | No issue yet. |
| At start-up, entries at or below the restored snapshot's index are deleted unless the entry at that index has the snapshot's term. Otherwise entries an interrupted install didn't discard stay below the snapshot, and this server as leader replicates them to a member behind it. | `api.go`, `snapshot.go`, `snapshot_boundary_test.go` | No issue yet. |
| `shouldSnapshot` returns false when the log store ends below the last snapshot, instead of wrapping the unsigned delta and taking a snapshot at every interval. | `snapshot.go`, `snapshot_boundary_test.go` | No issue yet. |

Each patch is dropped once an upstream release fixes the same problem.

## License

hashicorp/raft is licensed under the [Mozilla Public License 2.0](LICENSE).
The files changed by these patches remain under MPL-2.0, and their source is
published in this repository.
