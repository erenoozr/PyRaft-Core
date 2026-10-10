# PyRaft-Core

A **Raft consensus engine** and **chaos network simulator** in pure Python. It implements the rules from
Ongaro & Ousterhout's paper "In Search of an Understandable Consensus Algorithm". It uses only the
standard library (`asyncio`, `random`, `time`, `dataclasses`, `enum`, `typing`, `logging`).

## Features

| Stage | Content | File |
|---|---|---|
| 1 | Virtual chaos network: 5–50 ms latency jitter, 0–30% packet loss, splitting the network into isolated groups and healing it | `network.py` |
| 2 | Follower / Candidate / Leader states, randomized 150–300 ms election timeout, `RequestVote`, majority (N/2+1) vote | `raft.py` |
| 3 | Heartbeats and replication via `AppendEntries`; log repair with `nextIndex`/`matchIndex` (with fast backtracking) | `raft.py` |
| 4 | Majority-based `commitIndex` advancement (current-term entries only, Figure 8); in-order apply to a `KeyValueStore` | `raft.py` |
| 5 | Snapshot and log compaction once a threshold is reached; `InstallSnapshot` for lagging followers | `raft.py` |
| 6 | 5-node split-brain and chaos test suite | `chaos_test.py` |

### Invariants checked at runtime (`InvariantChecker`)

- **Election Safety:** at most one leader per term.
- **Leader Completeness:** a newly elected leader holds every committed entry.
- **State Machine Safety:** the entry committed at an index is the same on every node; a committed entry is never removed.
- **Log Matching:** if two logs hold an entry with the same index and term, all preceding entries match too (checked for every node pair at the end of the test).

Any violation raises `InvariantViolation`.

## Running

Requires Python 3.10+.

```bash
python chaos_test.py                # 5 seeds, 10% packet loss
python chaos_test.py 20 0.30        # 20 seeds, 30% packet loss
```

## Split-brain scenario

1. The 5-node cluster elects a leader; 5 values are written and committed.
2. The network is partitioned: the old leader ends up in a 2-node minority, 3 nodes form the majority, and the majority elects a new leader with a higher term.
3. The minority follower rejects writes; the old leader accepts writes but **cannot commit** them. The majority commits 15 writes and compacts its log with a snapshot.
4. The network heals: the old leader accepts the new term, its uncommitted writes are discarded, and the minority nodes catch up via `InstallSnapshot`. All five nodes end up with the same data and commit index.

Sample output:

```
=== seed 1, packet loss 10% ===
  [1] leader=node 1 (term 2), 5 writes committed
  [2] partition [1, 2] | [3, 4, 5]: new leader in majority: node 5 (term 3)
  [3] minority: 3 writes accepted but commit=0; majority: 15 writes committed; leader snapshot@20
  [4] healed: all nodes commit=22, kv equal, old leader 1 accepted term 3, minority caught up via InstallSnapshot
      network: {'sent': 410, 'dropped': 169, 'delivered': 239}
PASSED: 1 scenarios, no Raft invariant violated
```

## Limitations

- Persistent state lives in memory; node crashes and restarts are not simulated.
- No duplicate detection for client requests; the `set` command used in the tests is idempotent.
- Snapshots are sent in a single message, not in chunks.
- Raft guarantees safety only; under heavy packet loss leader election can take longer (liveness has no time bound).
