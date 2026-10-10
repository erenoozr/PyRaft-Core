"""Stage 6: 5-node split-brain and chaos test.

Usage: python chaos_test.py [num_seeds] [drop_rate]
"""
import asyncio
import logging
import sys
import time

from raft import Cluster, Role


async def eventually(cond, timeout, what):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"timed out: {what}")


def dump(c):
    for n in c.nodes.values():
        print(f"    node {n.id}: {n.role.value} term={n.term} voted={n.voted_for} "
              f"last=({n.last_index},{n.term_at(n.last_index)}) commit={n.commit_index} snap={n.snap_index}")


async def split_brain_scenario(seed, drop_rate):
    c = Cluster(n=5, seed=seed, drop_rate=drop_rate, snapshot_threshold=5)
    await c.start()
    try:
        # 1) Leader election + 5 writes
        old = await c.wait_leader(timeout=15)
        for i in range(5):
            assert await c.write(f"k{i}", i, timeout=15), f"k{i} was not committed"
        old = await c.wait_leader()  # the leader may have changed under chaos
        print(f"  [1] leader=node {old.id} (term {old.term}), 5 writes committed")

        # 2) Partition: old leader + 1 node in the minority, 3 nodes in the majority
        buddy = next(i for i in c.nodes if i != old.id)
        minority = {old.id, buddy}
        majority = set(c.nodes) - minority
        old_term = old.term
        c.net.partition(minority, majority)
        # under heavy loss the majority may hold a stale leader from an old term; wait for a newer term
        await eventually(lambda: any(c.nodes[i].role is Role.LEADER and c.nodes[i].term > old_term
                                     for i in majority), 15, "new leader in majority")
        new = await c.wait_leader(among=majority)
        new_term = new.term
        print(f"  [2] partition {sorted(minority)} | {sorted(majority)}: "
              f"new leader in majority: node {new.id} (term {new.term})")

        # 3) Writes on both sides
        old_commit = old.commit_index
        assert c.nodes[buddy].submit(("set", "m_x", 0)) is None, "follower must reject writes"
        minority_writes = [old.submit(("set", f"m{i}", i)) for i in range(3)]  # old leader still believes it leads
        for i in range(15):
            assert await c.write(f"x{i}", i, among=majority, timeout=15), f"x{i} was not committed in majority"
        assert old.commit_index == old_commit, "minority leader must not advance commit"
        for res in minority_writes:
            if res:  # may be None if the old leader stepped down meanwhile
                idx, entry = res
                assert c.checker.committed.get(idx) != entry, f"minority write was committed: {entry}"
        assert not any(k.startswith("m") for n in c.nodes.values() for k in n.kv.data)
        print(f"  [3] minority: {sum(r is not None for r in minority_writes)} writes accepted but commit=0; "
              f"majority: 15 writes committed; leader snapshot@{new.snap_index}")

        # 4) Heal and converge
        c.net.heal()
        expected = {**{f"k{i}": i for i in range(5)}, **{f"x{i}": i for i in range(15)}}
        nodes = list(c.nodes.values())
        await eventually(lambda: all(n.kv.data == expected for n in nodes)
                         and len({n.commit_index for n in nodes}) == 1, 15, "cluster convergence")
        leader = await c.wait_leader()
        # old leader accepted the new term; being re-elected later with an up-to-date log is legal in Raft
        assert old.term >= new_term, "old leader did not step down"
        assert all(c.nodes[i].snapshots_installed >= 1 for i in minority), "minority should have caught up via snapshot"
        c.checker.check_log_matching(nodes)
        print(f"  [4] healed: all nodes commit={leader.commit_index}, kv equal, "
              f"old leader {old.id} accepted term {old.term}, minority caught up via InstallSnapshot")
        print(f"      network: {c.net.stats}")
    except AssertionError:
        dump(c)
        raise
    finally:
        await c.stop()


async def main():
    seeds = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    drop = float(sys.argv[2]) if len(sys.argv) > 2 else 0.10
    for seed in range(seeds):
        print(f"=== seed {seed}, packet loss {drop * 100:.0f}% ===")
        await split_brain_scenario(seed, drop)
    print(f"PASSED: {seeds} scenarios, no Raft invariant violated")


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(name)s %(message)s")
    asyncio.run(main())
