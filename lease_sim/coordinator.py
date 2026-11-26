"""租约协调者与领导者任期。

安全职责：
1. 每次发放租约都产生一个严格递增的栅栏令牌（fencing token），
   令牌计数先落 WAL 再回复，进程崩溃/换主后从 WAL 恢复，绝不回退。
2. 同一受保护资源同一时刻只存在一份有效租约：新持有者拿到租约时，
   旧持有者的租约立即在协调者侧失效（无论旧进程是否知道）。
3. 每个领导者带有单调递增的任期号（term）。旧领导者被降级后，
   即使收到迟到的 RPC 也只回复 NOT_LEADER；客户端拒绝旧任期的回复。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .storage import DurableWAL, InMemoryWAL, WalRecord


class RpcError(Exception):
    """协调者侧的逻辑错误，携带机器可读 code。"""

    def __init__(self, code: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.code = code
        self.extra = extra


@dataclass
class Lease:
    holder: str
    token: int
    term: int
    issued_at: float
    expire_at: float

    def alive(self, now: float) -> bool:
        return now < self.expire_at

    def to_dict(self) -> dict[str, Any]:
        return {
            "holder": self.holder,
            "token": self.token,
            "term": self.term,
            "issued_at": self.issued_at,
            "expire_at": self.expire_at,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Lease":
        return cls(
            holder=d["holder"],
            token=d["token"],
            term=d["term"],
            issued_at=d["issued_at"],
            expire_at=d["expire_at"],
        )


class Coordinator:
    """一个租约协调者实例（某一任领导者或已降级的跟随者）。"""

    def __init__(
        self,
        name: str,
        sim: Any,
        wal: DurableWAL | InMemoryWAL | None = None,
        lease_ttl: float = 10.0,
    ) -> None:
        self.name = name
        self.sim = sim
        self.wal = wal if wal is not None else InMemoryWAL()
        self.lease_ttl = lease_ttl
        self.is_leader = False
        self.term = 0
        self.token_counter = 0
        self.leases: dict[str, Lease] = {}          # resource_id -> 当前租约
        self.issued: dict[str, Lease] = {}          # holder -> 其最新租约
        self._recover()

    # ---- 恢复与选举 ------------------------------------------------
    def _recover(self) -> None:
        rec = self.wal.latest()
        if rec is None:
            return
        self.term = rec.term
        self.token_counter = rec.token_counter
        self.leases = {
            rid: Lease.from_dict(d) for rid, d in rec.leases.items()
        }
        self.issued = {ls.holder: ls for ls in self.leases.values()}
        self.sim.log(
            self.name,
            "wal_replay",
            term=self.term,
            token_counter=self.token_counter,
            leases=list(self.leases.keys()),
        )

    def become_leader(self, new_term: int) -> None:
        """以任期 new_term 上任。

        先重放 WAL 再上任；拒绝用更小的任期上任，防止脑脑后旧主复位
        令牌计数。
        """
        self._recover()
        if new_term <= self.term:
            raise RpcError(
                "STALE_TERM",
                f"{self.name} 拒绝任期回退 {new_term} <= {self.term}",
            )
        self.term = new_term
        self.is_leader = True
        self._persist()
        self.sim.log(
            self.name,
            "become_leader",
            term=self.term,
            token_counter=self.token_counter,
        )

    def step_down(self) -> None:
        if self.is_leader:
            self.sim.log(self.name, "step_down", term=self.term)
        self.is_leader = False

    def _persist(self) -> None:
        """把任期、令牌计数、租约快照整体写入 WAL（fsync 语义）。"""
        prev = self.wal.latest()
        if prev is not None and self.token_counter < prev.token_counter:
            # 最后一道防线：任何代码路径都不允许令牌计数回退
            raise RpcError(
                "TOKEN_REGRESSION",
                f"令牌计数回退 {self.token_counter} < {prev.token_counter}",
            )
        rec = WalRecord(
            term=self.term,
            token_counter=self.token_counter,
            leases={rid: ls.to_dict() for rid, ls in self.leases.items()},
        )
        self.wal.write(rec)

    # ---- RPC：获取租约 ---------------------------------------------
    def rpc_acquire(self, src: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_leader()
        resource_id = payload["resource_id"]
        ttl = float(payload.get("ttl", self.lease_ttl))
        now = self.sim.clock.now()

        old = self.leases.get(resource_id)
        if old is not None and old.alive(now):
            self.sim.log(
                self.name,
                "lease_revoke",
                resource=resource_id,
                old_holder=old.holder,
                old_token=old.token,
                new_holder=src,
            )

        self.token_counter += 1
        token = self.token_counter
        lease = Lease(
            holder=src,
            token=token,
            term=self.term,
            issued_at=now,
            expire_at=now + ttl,
        )
        self.leases[resource_id] = lease
        self.issued[src] = lease
        self._persist()  # 先落盘（含新令牌），再回复
        self.sim.log(
            self.name,
            "lease_granted",
            resource=resource_id,
            holder=src,
            token=token,
            term=self.term,
            expire_at=round(lease.expire_at, 6),
        )
        return {
            "ok": True,
            "token": token,
            "term": self.term,
            "issued_at": now,
            "expire_at": lease.expire_at,
            "ttl": ttl,
        }

    # ---- RPC：续约 -------------------------------------------------
    def rpc_renew(self, src: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_leader()
        resource_id = payload["resource_id"]
        token = int(payload["token"])
        now = self.sim.clock.now()
        lease = self.leases.get(resource_id)

        if lease is None or lease.holder != src or lease.token != token:
            # 租约已被别人拿走或持有者已变 —— 经典的“暂停过久”场景
            raise RpcError(
                "LEASE_LOST",
                f"{src} 的租约已失效（资源 {resource_id} 当前持有令牌 "
                f"{lease.token if lease else None}）",
                current_token=lease.token if lease else None,
            )
        if not lease.alive(now):
            raise RpcError(
                "LEASE_EXPIRED",
                f"{src} 的租约已于 t={lease.expire_at} 过期（现在 t={now}）",
                expire_at=lease.expire_at,
            )

        lease.expire_at = now + self.lease_ttl
        self._persist()
        self.sim.log(
            self.name,
            "lease_renewed",
            resource=resource_id,
            holder=src,
            token=token,
            expire_at=round(lease.expire_at, 6),
        )
        return {
            "ok": True,
            "token": token,
            "term": self.term,
            "expire_at": lease.expire_at,
        }

    # ---- 工具 ------------------------------------------------------
    def _require_leader(self) -> None:
        if not self.is_leader:
            raise RpcError(
                "NOT_LEADER",
                f"{self.name} 不是领导者（任期 {self.term}）",
                term=self.term,
            )

    def current_lease(self, resource_id: str) -> Lease | None:
        return self.leases.get(resource_id)

    def expire_if_needed(self, resource_id: str) -> None:
        """显式清理过期租约（惰性；获取新租约时也会覆盖）。"""
        lease = self.leases.get(resource_id)
        if lease is not None and not lease.alive(self.sim.clock.now()):
            self.sim.log(
                self.name,
                "lease_expired",
                resource=resource_id,
                holder=lease.holder,
                token=lease.token,
            )
            del self.leases[resource_id]
            self._persist()


class CoordinatorGroup:
    """协调者集群：持有共享 WAL，支持更换领导者。"""

    def __init__(self, sim: Any, wal: DurableWAL | InMemoryWAL, names: list[str],
                 lease_ttl: float = 10.0) -> None:
        self.sim = sim
        self.wal = wal
        self.members = [
            Coordinator(n, sim, wal, lease_ttl=lease_ttl) for n in names
        ]
        self.leader: Coordinator | None = None
        self.term = 0

    def elect(self, member_name: str) -> Coordinator:
        """选举新领导者，旧领导者降级，任期 +1。"""
        new_leader = next(m for m in self.members if m.name == member_name)
        if self.leader is not None:
            self.leader.step_down()
        self.term += 1
        new_leader.become_leader(self.term)
        self.leader = new_leader
        self.sim.log(
            "group",
            "leader_elected",
            leader=member_name,
            term=self.term,
            token_counter=new_leader.token_counter,
        )
        return new_leader

    def failover_to(self, member_name: str) -> Coordinator:
        """故障切换（语义同 elect，单独命名便于审计阅读）。"""
        return self.elect(member_name)
