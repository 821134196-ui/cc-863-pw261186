"""租约客户端。

模拟一个持有租约、向本地受保护资源写入的工作进程：

* 后台自动续约器按 renew_interval 周期性续约；
* ``pause()`` 模拟进程被挂起（GC、调度冻结、虚拟机暂停）——所有事件
  回调停止触发，包括续约器；
* ``resume()`` 恢复运行。若恢复瞬间本地还“以为”租约有效，会尝试带着
  旧栅栏令牌执行挂起的写，交给资源侧的栅栏检查拒绝；
* 支持本地时钟偏移/漂移，演示时钟差下的错误信念；
* 支持超时重试，重试沿用同一 request_id，资源侧做幂等去重。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .coordinator import RpcError

# 客户端状态
NO_LEASE = "NO_LEASE"
HAS_LEASE = "HAS_LEASE"
PAUSED = "PAUSED"


@dataclass
class LeaseView:
    """客户端本地看到的租约信息（可能已过期，客户端自己不知道）。"""
    token: int
    term: int
    issued_at: float
    expire_at: float
    ttl: float


@dataclass
class WriteResult:
    accepted: bool
    reason: str
    token: int | None
    value: Any
    request_id: str
    duplicate: bool = False


class LeaseClient:
    def __init__(
        self,
        name: str,
        sim: Any,
        network: Any,
        coordinator: Any,
        resource: Any,
        resource_id: str = "res-1",
        renew_interval: float = 3.0,
        clock_offset: float = 0.0,
        clock_drift: float = 0.0,
    ) -> None:
        self.name = name
        self.sim = sim
        self.net = network
        self.coordinator = coordinator      # 当前连接的协调者（换主后可重指向）
        self.resource = resource
        self.resource_id = resource_id
        self.renew_interval = renew_interval
        self.clock_offset = clock_offset    # 本地时钟固定偏移
        self.clock_drift = clock_drift      # 本地时钟漂移率
        self.state = NO_LEASE
        self.lease: LeaseView | None = None
        self._renew_event: int | None = None
        self._req_seq = 0
        self.suspended_write: dict[str, Any] | None = None  # 暂停期间挂起的写
        self.on_resume_hook: Any = None     # 操作台可挂载“恢复瞬间”回调
        self.history: list[WriteResult] = []

    # ---- 时钟 ------------------------------------------------------
    def local_now(self) -> float:
        return self.sim.clock.local_now(self.clock_offset, self.clock_drift)

    def believes_lease_valid(self) -> bool:
        """客户端基于本地时钟的自我判断（可能是错误信念）。"""
        return self.lease is not None and self.local_now() < self.lease.expire_at

    # ---- 获取租约 --------------------------------------------------
    def acquire(self, ttl: float | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"resource_id": self.resource_id}
        if ttl is not None:
            payload["ttl"] = ttl
        reply = self.net.call_sync(self.name, self.coordinator, "acquire", payload)
        if not reply.get("ok"):
            return reply
        self.lease = LeaseView(
            token=reply["token"],
            term=reply["term"],
            issued_at=reply["issued_at"],
            expire_at=reply["expire_at"],
            ttl=reply["ttl"],
        )
        self.state = HAS_LEASE
        self.sim.log(
            self.name, "acquired", token=self.lease.token,
            term=self.lease.term, expire_at=round(self.lease.expire_at, 6),
        )
        self._schedule_renew()
        return reply

    # ---- 自动续约 --------------------------------------------------
    def _schedule_renew(self) -> None:
        if self.state != HAS_LEASE:
            return
        self._renew_event = self.sim.schedule(self.renew_interval, self._renew_tick)

    def _renew_tick(self) -> None:
        self._renew_event = None
        if self.state != HAS_LEASE or self.lease is None:
            return
        payload = {
            "resource_id": self.resource_id,
            "token": self.lease.token,
        }
        self.net.call(
            self.name, self.coordinator, "renew", payload, self._on_renew_reply
        )

    def _on_renew_reply(self, reply: dict[str, Any]) -> None:
        if self.state != HAS_LEASE:
            return
        if not reply.get("ok"):
            err = reply.get("error", "unknown")
            code = reply.get("code", "ERROR")
            self._lose_lease(f"续约被拒: {code} {err}")
            return
        # 任期防护：旧领导者的迟到回复不能续命
        if self.lease is None or reply["term"] < self.lease.term:
            self.sim.log(
                self.name, "stale_term_reply_ignored",
                reply_term=reply.get("term"), my_term=self.lease.term
                if self.lease else None,
            )
            return
        self.lease.expire_at = reply["expire_at"]
        self.sim.log(
            self.name, "renew_ok", token=self.lease.token,
            expire_at=round(self.lease.expire_at, 6),
        )
        self._schedule_renew()

    def _lose_lease(self, reason: str) -> None:
        token = self.lease.token if self.lease else None
        self.state = NO_LEASE
        self.lease = None
        if self._renew_event is not None:
            self.sim.cancel(self._renew_event)
            self._renew_event = None
        self.sim.log(self.name, "lease_lost", reason=reason, last_token=token)

    def rebind_coordinator(self, new_coordinator: Any) -> None:
        """协调者换主后，把连接指向新领导者。"""
        self.sim.log(
            self.name, "rebind_coordinator",
            old=getattr(self.coordinator, "name", None),
            new=getattr(new_coordinator, "name", None),
        )
        self.coordinator = new_coordinator

    # ---- 暂停 / 恢复 -----------------------------------------------
    def pause(self, pending_write: Any = None) -> None:
        """暂停进程。可选地登记一笔“暂停期间想写”的挂起写。

        事件循环里尚未触发的续约事件不会被物理删除（恢复后若还在队列
        里会照常触发），但暂停期间到期的续约已经错过——这正是租约过期
        的根因。
        """
        if self.state != HAS_LEASE:
            self.sim.log(self.name, "pause_ignored", state=self.state)
            return
        self.state = PAUSED
        if pending_write is not None:
            self.suspended_write = {
                "value": pending_write,
                "request_id": self._next_req_id(),
                "token_at_pause": self.lease.token if self.lease else None,
            }
        self.sim.log(
            self.name, "paused",
            token=self.lease.token if self.lease else None,
            pending_write=pending_write,
            local_now=round(self.local_now(), 6),
        )

    def resume(self) -> dict[str, Any] | None:
        """恢复进程。

        恢复瞬间：
        1. 触发操作台挂载的对照回调（新持有者可以在同一刻真实写入）；
        2. 若客户端本地仍相信租约有效且有挂起写，立刻带着旧令牌尝试
           写入 —— 资源侧栅栏检查决定接受还是拒绝；
        3. 若本地时钟也显示租约过期，则主动放弃租约。
        """
        if self.state != PAUSED:
            self.sim.log(self.name, "resume_ignored", state=self.state)
            return None
        self.state = HAS_LEASE
        self.sim.log(
            self.name, "resumed",
            token=self.lease.token if self.lease else None,
            believes_valid=self.believes_lease_valid(),
            local_now=round(self.local_now(), 6),
            real_now=round(self.sim.clock.now(), 6),
            expire_at=round(self.lease.expire_at, 6) if self.lease else None,
        )

        if self.on_resume_hook is not None:
            self.on_resume_hook(self)

        result = None
        if self.suspended_write is not None:
            sw = self.suspended_write
            self.suspended_write = None
            if self.believes_lease_valid():
                self.sim.log(
                    self.name, "resume_attempt_stale_write",
                    token=sw["token_at_pause"], value=sw["value"],
                    request_id=sw["request_id"],
                )
                result = self.write(sw["value"], request_id=sw["request_id"])
            else:
                self.sim.log(
                    self.name, "resume_give_up",
                    reason="本地时钟也显示租约已过期",
                    token=sw["token_at_pause"],
                )
                self._lose_lease("恢复时发现租约已过期")
        elif self.believes_lease_valid():
            self._schedule_renew()
        else:
            self._lose_lease("恢复时发现租约已过期")
        return result

    # ---- 写入受保护资源 --------------------------------------------
    def _next_req_id(self) -> str:
        self._req_seq += 1
        return f"{self.name}-req-{self._req_seq}"

    def write(self, value: Any, request_id: str | None = None,
              timeout: float = 5.0) -> WriteResult:
        """通过 RPC 向受保护资源发起一次带栅栏令牌的写。

        timeout 必须大于一次完整 RPC 往返（2 * rpc_delay），否则未丢包
        也会误报超时。
        """
        if self.lease is None:
            res = WriteResult(False, "NO_LEASE", None, value,
                              request_id or self._next_req_id())
            self.history.append(res)
            self.sim.log(self.name, "write_rejected_local", reason="NO_LEASE")
            return res
        rid = request_id or self._next_req_id()
        payload = {
            "token": self.lease.token,
            "term": self.lease.term,
            "value": value,
            "request_id": rid,
            "holder": self.name,
        }
        reply = self.net.call_sync(self.name, self.resource, "write", payload,
                                   timeout=timeout)
        if "accepted" not in reply:
            # 传输层故障（丢包/超时），资源可能已落盘也可能没有 —— 可重试
            result = WriteResult(
                accepted=False,
                reason="TRANSPORT_ERROR:" + str(reply.get("error", "unknown")),
                token=self.lease.token,
                value=value,
                request_id=rid,
            )
            self.history.append(result)
            self.sim.log(
                self.name, "write_transport_error",
                token=result.token, reason=result.reason, request_id=rid,
            )
            return result
        result = WriteResult(
            accepted=bool(reply.get("accepted")),
            reason=reply.get("reason", "OK" if reply.get("accepted") else "REJECTED"),
            token=self.lease.token,
            value=value,
            request_id=rid,
            duplicate=bool(reply.get("duplicate")),
        )
        self.history.append(result)
        self.sim.log(
            self.name,
            "client_write_accepted" if result.accepted else "client_write_rejected",
            token=result.token, reason=result.reason,
            value=value, request_id=rid, duplicate=result.duplicate,
        )
        if not result.accepted and result.reason == "STALE_FENCING_TOKEN":
            # 被资源侧拒绝后，客户端必须承认自己已不是持有者
            self._lose_lease(f"资源 {self.resource.name} 拒绝旧令牌 {result.token}")
        return result

    def write_async(self, value: Any, request_id: str | None = None) -> str:
        """异步发起写：只把请求投进网络，不等回复。返回 request_id。

        用于演示真实网络窗口：调用返回时写尚未落盘，其他事件可以
        抢先执行。
        """
        if self.lease is None:
            raise RuntimeError(f"{self.name} 无租约，无法异步写")
        rid = request_id or self._next_req_id()
        payload = {
            "token": self.lease.token,
            "term": self.lease.term,
            "value": value,
            "request_id": rid,
            "holder": self.name,
        }

        def on_reply(reply: dict[str, Any]) -> None:
            result = WriteResult(
                accepted=bool(reply.get("accepted")),
                reason=reply.get("reason", "REJECTED"),
                token=payload["token"],
                value=value,
                request_id=rid,
                duplicate=bool(reply.get("duplicate")),
            )
            self.history.append(result)
            self.sim.log(
                self.name,
                "client_write_accepted" if result.accepted
                else "client_write_rejected",
                token=result.token, reason=result.reason,
                value=value, request_id=rid, duplicate=result.duplicate,
            )
            if not result.accepted and result.reason == "STALE_FENCING_TOKEN":
                self._lose_lease(
                    f"资源 {self.resource.name} 拒绝旧令牌 {result.token}"
                )

        self.net.call(self.name, self.resource, "write", payload, on_reply)
        return rid

    def write_with_retry(
        self, value: Any, max_attempts: int = 3, timeout: float = 2.0
    ) -> WriteResult:
        """带超时重试的写。

        所有尝试共用同一个 request_id：
        * 请求丢失 → 重试是新消息，服务端正常处理；
        * 回复丢失/超时 → 服务端可能已落盘，重试命中幂等去重，返回
          duplicate=True 且结果与首次一致。
        """
        rid = self._next_req_id()
        last: WriteResult | None = None
        for attempt in range(1, max_attempts + 1):
            self.sim.log(
                self.name, "write_attempt", attempt=attempt,
                request_id=rid, value=value,
            )
            result = self.write(value, request_id=rid)
            last = result
            if result.accepted or result.reason == "STALE_FENCING_TOKEN" \
                    or result.reason == "NO_LEASE":
                # 明确结局（含幂等重放）就不再重试；只有传输类失败才重试
                return result
        assert last is not None
        return last
