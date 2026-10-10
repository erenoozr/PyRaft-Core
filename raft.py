"""Stages 2-5: Raft node: leader election, log replication, commit/apply, snapshots.

Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (Figures 2 and 13).
Persistent state (term, voted_for, log, snapshot) is kept in memory; the simulation has no node crashes.
"""
import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from network import VirtualNetwork

log = logging.getLogger("raft")


class Role(Enum):
    FOLLOWER = "Follower"
    CANDIDATE = "Candidate"
    LEADER = "Leader"


@dataclass(frozen=True)
class Entry:
    term: int
    command: Any  # ("set", k, v) | ("del", k, None) | None (leader no-op)


# ---- RPC messages (requests and replies are separate one-way messages on the network) ----
@dataclass
class RequestVote:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    term: int
    granted: bool


@dataclass
class AppendEntries:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: list
    leader_commit: int


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int
    conflict_index: int


@dataclass
class InstallSnapshot:
    term: int
    leader_id: int
    last_included_index: int
    last_included_term: int
    data: dict


@dataclass
class InstallSnapshotReply:
    term: int
    last_included_index: int


class KeyValueStore:
    """Deterministic state machine underneath Raft."""

    def __init__(self):
        self.data = {}

    def apply(self, command):
        if command is None:
            return
        op, key, value = command
        if op == "set":
            self.data[key] = value
        elif op == "del":
            self.data.pop(key, None)
        else:
            raise ValueError(f"unknown command: {op}")

    def snapshot(self):
        return dict(self.data)

    def restore(self, data):
        self.data = dict(data)


class InvariantViolation(AssertionError):
    pass


class InvariantChecker:
    """Checks Raft safety properties across the whole cluster at runtime."""

    def __init__(self):
        self.leaders = {}    # term -> leader id           (Election Safety)
        self.committed = {}  # index -> Entry (all nodes)   (State Machine Safety)

    def on_leader(self, node):
        prev = self.leaders.setdefault(node.term, node.id)
        if prev != node.id:
            raise InvariantViolation(f"Election Safety: two leaders {prev}, {node.id} in term {node.term}")
        # Leader Completeness: a new leader must hold every committed entry
        for idx, entry in self.committed.items():
            if idx > node.snap_index and node.entry_at(idx) != entry:
                raise InvariantViolation(f"Leader Completeness: leader {node.id} is missing/differs at index {idx}")

    def on_commit(self, node, idx, entry):
        known = self.committed.setdefault(idx, entry)
        if known != entry:
            raise InvariantViolation(
                f"State Machine Safety: index {idx} is {entry} on node {node.id} but {known} elsewhere")

    def check_log_matching(self, nodes):
        """Log Matching: if two logs share an index+term, all preceding entries are identical."""
        for a in nodes:
            for b in nodes:
                if a.id >= b.id:
                    continue
                lo = max(a.snap_index, b.snap_index) + 1
                hi = min(a.last_index, b.last_index)
                top = next((i for i in range(hi, lo - 1, -1) if a.term_at(i) == b.term_at(i)), None)
                if top is None:
                    continue
                for i in range(lo, top + 1):
                    if a.entry_at(i) != b.entry_at(i):
                        raise InvariantViolation(f"Log Matching: nodes {a.id}/{b.id} index {i}")


class RaftNode:
    def __init__(self, node_id, cluster_ids, net, checker, *, rng=None,
                 election_timeout=(0.150, 0.300), heartbeat=0.050, snapshot_threshold=10):
        self.id = node_id
        self.peers = [p for p in cluster_ids if p != node_id]
        self.majority = len(cluster_ids) // 2 + 1
        self.net, self.checker = net, checker
        self.rng = rng or random.Random()
        self.election_timeout, self.heartbeat = election_timeout, heartbeat
        self.snapshot_threshold = snapshot_threshold
        # persistent state
        self.term = 0
        self.voted_for: Optional[int] = None
        self.log: list[Entry] = []
        self.snap_index, self.snap_term, self.snap_data = 0, 0, {}
        # volatile state
        self.role = Role.FOLLOWER
        self.leader_id = None
        self.commit_index = self.last_applied = 0
        self.kv = KeyValueStore()
        self.votes = set()
        self.next_index, self.match_index = {}, {}
        self.running = False
        self.snapshots_installed = 0
        self.next_heartbeat = 0.0
        self._reset_election_timer()
        net.register(self)

    # ---- log helpers (snapshot offset, 1-based index) ----
    @property
    def last_index(self):
        return self.snap_index + len(self.log)

    def term_at(self, i):
        if i == self.snap_index:
            return self.snap_term
        if self.snap_index < i <= self.last_index:
            return self.log[i - self.snap_index - 1].term
        return None

    def entry_at(self, i):
        return self.log[i - self.snap_index - 1] if self.snap_index < i <= self.last_index else None

    # ---- main loop ----
    async def run(self):
        self.running = True
        while self.running:
            now = time.monotonic()
            if self.role is Role.LEADER:
                if now >= self.next_heartbeat:
                    self._broadcast_append()
            elif now >= self.election_deadline:
                self._start_election()
            await asyncio.sleep(0.005)

    def stop(self):
        self.running = False

    def receive(self, src, msg):
        if msg.term > self.term:
            self._step_down(msg.term)
        {
            RequestVote: self._on_request_vote,
            RequestVoteReply: self._on_vote_reply,
            AppendEntries: self._on_append_entries,
            AppendEntriesReply: self._on_append_reply,
            InstallSnapshot: self._on_install_snapshot,
            InstallSnapshotReply: self._on_snapshot_reply,
        }[type(msg)](src, msg)

    # ---- Stage 2: leader election ----
    def _reset_election_timer(self):
        self.election_deadline = time.monotonic() + self.rng.uniform(*self.election_timeout)

    def _step_down(self, term):
        if self.role is not Role.FOLLOWER:
            log.info("node %d: %s -> Follower (term %d -> %d)", self.id, self.role.value, self.term, term)
        self.term, self.voted_for, self.role = term, None, Role.FOLLOWER

    def _start_election(self):
        self.role = Role.CANDIDATE
        self.term += 1
        self.voted_for, self.votes, self.leader_id = self.id, {self.id}, None
        self._reset_election_timer()
        msg = RequestVote(self.term, self.id, self.last_index, self.term_at(self.last_index))
        for p in self.peers:
            self.net.send(self.id, p, msg)
        self._maybe_win()

    def _on_request_vote(self, src, m):
        # Is the candidate's log at least as up-to-date as ours? Lexicographic (lastTerm, lastIndex)
        up_to_date = (m.last_log_term, m.last_log_index) >= (self.term_at(self.last_index), self.last_index)
        granted = m.term == self.term and self.voted_for in (None, m.candidate_id) and up_to_date
        if granted:
            self.voted_for = m.candidate_id
            self._reset_election_timer()
        self.net.send(self.id, src, RequestVoteReply(self.term, granted))

    def _on_vote_reply(self, src, m):
        if self.role is Role.CANDIDATE and m.term == self.term and m.granted:
            self.votes.add(src)
            self._maybe_win()

    def _maybe_win(self):
        if self.role is not Role.CANDIDATE or len(self.votes) < self.majority:
            return
        self.role, self.leader_id = Role.LEADER, self.id
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.checker.on_leader(self)
        log.info("node %d: became LEADER (term %d, votes %s)", self.id, self.term, sorted(self.votes))
        # no-op: lets entries from earlier terms get committed in this term (§8)
        self.log.append(Entry(self.term, None))
        self._advance_commit()
        self._broadcast_append()

    # ---- Stage 3: log replication ----
    def submit(self, command):
        """Client request. Returns None if not leader (client should redirect to leader_id)."""
        if self.role is not Role.LEADER:
            return None
        entry = Entry(self.term, command)
        self.log.append(entry)
        idx = self.last_index
        self._advance_commit()
        self._broadcast_append()
        return idx, entry

    def _broadcast_append(self):
        self.next_heartbeat = time.monotonic() + self.heartbeat
        for p in self.peers:
            self._replicate(p)

    def _replicate(self, p):
        ni = self.next_index[p]
        if ni <= self.snap_index:  # entries the follower needs were compacted -> send snapshot
            msg = InstallSnapshot(self.term, self.id, self.snap_index, self.snap_term, self.snap_data)
        else:
            msg = AppendEntries(self.term, self.id, ni - 1, self.term_at(ni - 1),
                                self.log[ni - self.snap_index - 1:], self.commit_index)
        self.net.send(self.id, p, msg)

    def _accept_leader(self, src):
        if self.role is Role.LEADER:
            raise InvariantViolation(f"Election Safety: second leader {src} in term {self.term}")
        self.role, self.leader_id = Role.FOLLOWER, src
        self._reset_election_timer()

    def _on_append_entries(self, src, m):
        if m.term < self.term:
            self.net.send(self.id, src, AppendEntriesReply(self.term, False, 0, 0))
            return
        self._accept_leader(src)
        prev, prev_term, entries = m.prev_log_index, m.prev_log_term, m.entries
        if prev < self.snap_index:  # prefix is already in our snapshot (committed, hence matching)
            entries = entries[self.snap_index - prev:]
            prev, prev_term = self.snap_index, self.snap_term
        if prev > self.last_index:
            self.net.send(self.id, src, AppendEntriesReply(self.term, False, 0, self.last_index + 1))
            return
        if self.term_at(prev) != prev_term:
            # Fast backtracking: report our first index of the conflicting term
            bad, ci = self.term_at(prev), prev
            while ci - 1 > self.snap_index and self.term_at(ci - 1) == bad:
                ci -= 1
            self.net.send(self.id, src, AppendEntriesReply(self.term, False, 0, ci))
            return
        for k, e in enumerate(entries):
            i = prev + 1 + k
            if i <= self.last_index:
                if self.term_at(i) == e.term:
                    continue  # already present; never delete a matching entry (delayed RPCs)
                if i <= self.commit_index:
                    raise InvariantViolation(f"node {self.id}: attempted to delete committed index {i}")
                del self.log[i - self.snap_index - 1:]
            self.log.append(e)
        match = prev + len(entries)
        if m.leader_commit > self.commit_index:
            self._set_commit(min(m.leader_commit, match))
        self.net.send(self.id, src, AppendEntriesReply(self.term, True, match, 0))

    def _on_append_reply(self, src, m):
        if self.role is not Role.LEADER or m.term != self.term:
            return
        if m.success:
            self.match_index[src] = max(self.match_index[src], m.match_index)
            self.next_index[src] = self.match_index[src] + 1
            self._advance_commit()
        else:
            self.next_index[src] = max(self.match_index[src] + 1, m.conflict_index)
            self._replicate(src)  # retry immediately

    # ---- Stage 4: commit and apply ----
    def _advance_commit(self):
        for n in range(self.last_index, self.commit_index, -1):
            if self.term_at(n) != self.term:
                break  # Figure 8: older-term entries are never committed by counting; they commit indirectly
            if 1 + sum(mi >= n for mi in self.match_index.values()) >= self.majority:
                self._set_commit(n)
                break

    def _set_commit(self, n):
        if n <= self.commit_index:
            return
        for i in range(self.commit_index + 1, n + 1):
            self.checker.on_commit(self, i, self.entry_at(i))
        self.commit_index = n
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.kv.apply(self.entry_at(self.last_applied).command)
        if self.last_applied - self.snap_index >= self.snapshot_threshold:
            self._take_snapshot()

    # ---- Stage 5: snapshots / log compaction ----
    def _take_snapshot(self):
        self.snap_term = self.term_at(self.last_applied)
        del self.log[:self.last_applied - self.snap_index]
        self.snap_index = self.last_applied
        self.snap_data = self.kv.snapshot()
        log.debug("node %d: snapshot @%d (log %d entries)", self.id, self.snap_index, len(self.log))

    def _on_install_snapshot(self, src, m):
        if m.term < self.term:
            self.net.send(self.id, src, InstallSnapshotReply(self.term, 0))
            return
        self._accept_leader(src)
        idx = m.last_included_index
        if idx > self.commit_index:
            if self.term_at(idx) == m.last_included_term:
                del self.log[:idx - self.snap_index]  # keep the matching tail after the snapshot
            else:
                self.log = []
            self.snap_index, self.snap_term, self.snap_data = idx, m.last_included_term, dict(m.data)
            self.kv.restore(m.data)
            self.commit_index = self.last_applied = idx
            self.snapshots_installed += 1
            log.info("node %d: installed snapshot from leader %d @%d", self.id, src, idx)
        self.net.send(self.id, src, InstallSnapshotReply(self.term, idx))

    def _on_snapshot_reply(self, src, m):
        if self.role is not Role.LEADER or m.term != self.term:
            return
        self.match_index[src] = max(self.match_index[src], m.last_included_index)
        self.next_index[src] = self.match_index[src] + 1
        self._advance_commit()


class Cluster:
    """Simulated cluster + client helpers."""

    def __init__(self, n=5, seed=None, drop_rate=0.0, snapshot_threshold=10):
        self.rng = random.Random(seed)
        self.net = VirtualNetwork(drop_rate=drop_rate, rng=self.rng)
        self.checker = InvariantChecker()
        ids = list(range(1, n + 1))
        self.nodes = {i: RaftNode(i, ids, self.net, self.checker, rng=self.rng,
                                  snapshot_threshold=snapshot_threshold) for i in ids}
        self.tasks = []

    async def start(self):
        self.tasks = [asyncio.create_task(n.run()) for n in self.nodes.values()]

    async def stop(self):
        for n in self.nodes.values():
            n.stop()
        await asyncio.gather(*self.tasks)

    def _among(self, among):
        return [self.nodes[i] for i in (among or self.nodes)]

    async def wait_leader(self, among=None, timeout=3.0):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            nodes = self._among(among)
            top = max(n.term for n in nodes)
            # don't return a stale leader (stuck in an old term, not yet stepped down)
            leader = next((n for n in nodes if n.role is Role.LEADER and n.term == top), None)
            if leader:
                return leader
            await asyncio.sleep(0.01)
        raise TimeoutError(f"no leader elected in {among or 'cluster'}")

    async def wait_committed(self, idx, entry, timeout=1.0):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if self.checker.committed.get(idx) == entry:
                return True
            await asyncio.sleep(0.01)
        return False

    async def write(self, key, value, among=None, timeout=5.0):
        """Retries (with a new leader if needed) until committed. set is idempotent."""
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            leader = await self.wait_leader(among, timeout=end - time.monotonic())
            res = leader.submit(("set", key, value))
            if res and await self.wait_committed(*res):
                return True
        return False

