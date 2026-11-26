"""世界装配：把时钟、网络、WAL、协调者集群、资源、客户端组装起来。

并提供两个横切校验器：
* TokenMonotonicityWatcher：全局监听审计日志，断言整个历史中观察到的
  令牌只增不减（覆盖发放、写入、换主恢复等所有路径）；
* make_lease_validator：为资源构造“向协调者在线验租”的回调。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .client import LeaseClient
from .coordinator import CoordinatorGroup
from .resource import FencedResource
from .simulation import Network, Simulation
from .storage import InMemoryWAL


@dataclass
class World:
    sim: Simulation
    net: Network
    group: CoordinatorGroup
    resource: FencedResource
    clients: dict[str, LeaseClient]
    resource_id: str
    strict_validation: bool

    def client(self, name: str) -> LeaseClient:
        return self.clients[name]

    def leader(self) -> Any:
        return self.group.leader


class TokenMonotonicityWatcher:
    """审计日志监查器：令牌在任何路径上都不允许回退。

    监听两类事件：
    * lease_granted 的 token（协调者发放序列，必须严格递增）；
    * write_accepted 的 token（资源实际接受序列，必须单调不降——
      同一持有者租约期内可以用同一令牌多次写）。
    """

    def __init__(self) -> None:
        self.grant_tokens: list[int] = []
        self.accepted_tokens: list[int] = []
        self.violations: list[dict[str, Any]] = []

    def feed(self, entry: dict[str, Any]) -> None:
        if entry["event"] == "lease_granted":
            seq = self.grant_tokens
            tok = entry["token"]
            label = "grant"
            strict = True
        elif entry["event"] == "write_accepted":
            seq = self.accepted_tokens
            tok = entry["token"]
            label = "accepted"
            strict = False
        else:
            return
        if seq and (tok < seq[-1] or (strict and tok <= seq[-1])):
            self.violations.append(
                {"t": entry["t"], "kind": label, "prev": seq[-1], "got": tok,
                 "actor": entry["actor"]}
            )
        seq.append(tok)

    def verify(self, audit: list[dict[str, Any]]) -> dict[str, Any]:
        for entry in audit:
            self.feed(entry)
        grant_strict = all(
            b > a for a, b in zip(
                self.grant_tokens, self.grant_tokens[1:])
        )
        accept_nondecreasing = all(
            b >= a for a, b in zip(
                self.accepted_tokens, self.accepted_tokens[1:])
        )
        return {
            "ok": not self.violations and grant_strict
            and accept_nondecreasing,
            "grant_tokens": list(self.grant_tokens),
            "accepted_tokens": list(self.accepted_tokens),
            "violations": self.violations,
        }


def make_lease_validator(group: CoordinatorGroup, sim: Simulation,
                         resource_id: str):
    """构造资源侧在线验租回调（strict 模式使用）。"""

    def validate(rid: str, token: int, holder: str) -> tuple[bool, str]:
        leader = group.leader
        if leader is None:
            return False, "NO_LEADER"
        lease = leader.current_lease(rid)
        now = sim.clock.now()
        if lease is None:
            return False, "NO_ACTIVE_LEASE"
        if lease.token != token or lease.holder != holder:
            return (False,
                    f"TOKEN_NOT_CURRENT(current={lease.token}/{lease.holder})")
        if not lease.alive(now):
            return False, "LEASE_EXPIRED_AT_COORDINATOR"
        return True, "OK"

    return validate


def build_world(
    client_names: list[str] | None = None,
    lease_ttl: float = 10.0,
    rpc_delay: float = 0.5,
    renew_interval: float = 3.0,
    strict_validation: bool = False,
    wal: Any | None = None,
    coordinator_names: list[str] | None = None,
    resource_id: str = "res-1",
) -> World:
    """组装一个完整的模拟世界并选出第一任领导者。"""
    sim = Simulation()
    net = Network(sim, rpc_delay=rpc_delay)
    wal = wal if wal is not None else InMemoryWAL()
    coord_names = coordinator_names or ["coord-A", "coord-B", "coord-C"]
    group = CoordinatorGroup(sim, wal, coord_names, lease_ttl=lease_ttl)
    group.elect(coord_names[0])

    validator = (
        make_lease_validator(group, sim, resource_id)
        if strict_validation else None
    )
    resource = FencedResource("store-1", sim, validator=validator,
                              resource_id=resource_id)

    clients: dict[str, LeaseClient] = {}
    for nm in client_names or ["client-1", "client-2"]:
        clients[nm] = LeaseClient(
            name=nm,
            sim=sim,
            network=net,
            coordinator=group.leader,
            resource=resource,
            resource_id=resource_id,
            renew_interval=renew_interval,
        )

    return World(
        sim=sim, net=net, group=group, resource=resource,
        clients=clients, resource_id=resource_id,
        strict_validation=strict_validation,
    )


def rebind_all_clients(world: World) -> None:
    """换主后把所有客户端的连接指向新领导者。"""
    for cli in world.clients.values():
        cli.rebind_coordinator(world.group.leader)
