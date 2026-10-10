"""Stage 1: Virtual chaos network: latency jitter, packet loss, network partitions."""
import asyncio
import logging
import random

log = logging.getLogger("net")


class VirtualNetwork:
    def __init__(self, min_latency=0.005, max_latency=0.050, drop_rate=0.0, rng=None):
        if not 0.0 <= drop_rate <= 0.30:
            raise ValueError("drop_rate must be within 0-0.30")
        self.min_latency, self.max_latency = min_latency, max_latency
        self.drop_rate = drop_rate
        self.rng = rng or random.Random()
        self.nodes = {}
        self.groups = None  # None => fully connected network
        self.stats = {"sent": 0, "dropped": 0, "delivered": 0}

    def register(self, node):
        self.nodes[node.id] = node

    def partition(self, *groups):
        """Split the network into isolated groups; all cross-group traffic (including in-flight) is dropped."""
        self.groups = [frozenset(g) for g in groups]
        log.warning("NETWORK PARTITIONED: %s", " | ".join(str(sorted(g)) for g in self.groups))

    def heal(self):
        self.groups = None
        log.warning("NETWORK HEALED")

    def can_talk(self, a, b):
        return self.groups is None or any(a in g and b in g for g in self.groups)

    def send(self, src, dst, msg):
        self.stats["sent"] += 1
        if not self.can_talk(src, dst) or self.rng.random() < self.drop_rate:
            self.stats["dropped"] += 1
            return
        delay = self.rng.uniform(self.min_latency, self.max_latency)
        asyncio.get_running_loop().call_later(delay, self._deliver, src, dst, msg)

    def _deliver(self, src, dst, msg):
        # Do not deliver if a partition happened while the packet was in flight
        node = self.nodes.get(dst)
        if not self.can_talk(src, dst) or node is None or not node.running:
            self.stats["dropped"] += 1
            return
        self.stats["delivered"] += 1
        node.receive(src, msg)
