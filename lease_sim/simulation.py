"""模拟时钟、确定性事件循环与 RPC 网络。

整个世界由一个单调递增的虚拟时钟驱动，所有动作都通过事件队列调度，
因此同一场景可以被精确重放，测试结果是确定性的。
"""
from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Callable


class Clock:
    """虚拟时钟。

    每个参与者可以有自己的本地偏移（clock_offset）和漂移率（drift），
    用来模拟时钟差。真实时间 now() 只有事件循环能推进，参与者只能
    通过 local_now() 观察自己带偏差的本地时间。
    """

    def __init__(self) -> None:
        self._now: float = 0.0

    def now(self) -> float:
        return self._now

    def advance(self, delta: float) -> None:
        if delta < 0:
            raise ValueError("时钟不能倒退")
        self._now += delta

    def local_now(self, offset: float = 0.0, drift: float = 0.0) -> float:
        """本地观察时间 = 真实时间 + 固定偏移 + 漂移累计。"""
        return self._now + offset + drift * self._now


@dataclass(order=True)
class _Event:
    time: float
    seq: int
    callback: Callable[[], None] = field(compare=False)


class Simulation:
    """确定性离散事件模拟器。"""

    def __init__(self) -> None:
        self.clock = Clock()
        self._events: list[_Event] = []
        self._seq = 0
        self.network: "Network | None" = None
        self.audit: list[dict[str, Any]] = []
        self._max_events = 1_000_000

    def schedule(self, delay: float, callback: Callable[[], None]) -> int:
        """在 delay 个虚拟时间单位后执行 callback，返回事件 id。"""
        if delay < -1e-12:
            raise ValueError("不能调度到过去")
        self._seq += 1
        evt = _Event(self.clock.now() + max(delay, 0.0), self._seq, callback)
        heapq.heappush(self._events, evt)
        return evt.seq

    def cancel(self, event_id: int) -> None:
        """取消事件（用占位标记法，避免堆中线性查找）。"""
        self._events = [e for e in self._events if e.seq != event_id]
        heapq.heapify(self._events)

    def pending(self) -> int:
        return len(self._events)

    def log(self, actor: str, event: str, **fields: Any) -> None:
        """记录一条审计日志。"""
        entry = {"t": round(self.clock.now(), 6), "actor": actor, "event": event}
        entry.update(fields)
        self.audit.append(entry)

    def run(self, until: float | None = None, max_ticks: int | None = None) -> None:
        """推进事件循环。

        until: 跑到真实时间 >= until 为止（含该时刻之前调度的事件）。
        max_ticks: 最多处理多少个事件，防止失控循环。
        """
        ticks = 0
        limit = max_ticks if max_ticks is not None else self._max_events
        while self._events and ticks < limit:
            evt = heapq.heappop(self._events)
            if until is not None and evt.time > until + 1e-12:
                heapq.heappush(self._events, evt)
                break
            self.clock.advance(max(0.0, evt.time - self.clock.now()))
            ticks += 1
            try:
                evt.callback()
            except Exception as exc:
                # 不让单个事件的异常杀死整个事件循环，但必须显式记录
                self.log(
                    "sim", "event_error",
                    error=f"{type(exc).__name__}: {exc}",
                    at=evt.time,
                )
                raise

    def run_until_idle(self, max_ticks: int = 10_000) -> None:
        self.run(until=None, max_ticks=max_ticks)

    def step(self) -> bool:
        """处理下一个事件，返回是否还有事件。"""
        if not self._events:
            return False
        evt = heapq.heappop(self._events)
        self.clock.advance(max(0.0, evt.time - self.clock.now()))
        try:
            evt.callback()
        except Exception as exc:
            self.log(
                "sim", "event_error",
                error=f"{type(exc).__name__}: {exc}",
                at=evt.time,
            )
            raise
        return bool(self._events)

    def jump(self, target: float) -> None:
        """把时钟直接拨到 target，并触发期间所有到期事件。"""
        if target < self.clock.now():
            raise ValueError("时钟不能倒退")
        self.run(until=target)
        if self.clock.now() < target:
            self.clock.advance(target - self.clock.now())


@dataclass
class _Fault:
    """一次网络故障注入规则。"""
    kind: str            # "drop" 丢弃 | "delay" 延迟 | "corrupt" 乱序（通过延迟实现）
    predicate: Callable[[str, str, str], bool]
    ttl: int             # 剩余生效次数（-1 表示永久）
    extra_delay: float = 0.0


class Network:
    """模拟 RPC 网络。

    所有 RPC 都是 request -> response 的两阶段消息：
    请求在 rpc_delay 后送达服务端，服务端处理后回复再过 rpc_delay 回来。
    支持注入丢包、延迟，以及协调者换主后旧领导者的“过期回复”。
    """

    def __init__(self, sim: Simulation, rpc_delay: float = 0.5) -> None:
        self.sim = sim
        self.rpc_delay = rpc_delay
        self.faults: list[_Fault] = []
        sim.network = self

    # ---- 故障注入 -------------------------------------------------
    def inject_fault(
        self,
        kind: str,
        predicate: Callable[[str, str, str], bool],
        ttl: int = 1,
        extra_delay: float = 0.0,
    ) -> None:
        self.faults.append(_Fault(kind, predicate, ttl, extra_delay))

    def _match_fault(self, src: str, dst: str, method: str) -> _Fault | None:
        for f in list(self.faults):
            if f.predicate(src, dst, method):
                if f.ttl > 0:
                    f.ttl -= 1
                    if f.ttl == 0:
                        self.faults.remove(f)
                return f
        return None

    # ---- RPC 原语 -------------------------------------------------
    def call(
        self,
        src: str,
        dst: Any,
        method: str,
        payload: dict[str, Any],
        on_reply: Callable[[dict[str, Any]], None],
        timeout: float | None = None,
        tracked: set[Callable[[], None]] | None = None,
    ) -> None:
        """发起一次异步 RPC，回复到达时调用 on_reply。

        若注入了 drop 故障，则消息直接消失（触发超时语义由调用方自己
        用 schedule 实现，便于演示“超时重试”）。
        tracked：若提供集合，本次链路（请求/回复送达）的事件回调会
        登记进去，供 call_sync 区分本次 RPC 与无关事件。
        """
        fault = self._match_fault(src, getattr(dst, "name", dst), method)
        if fault is not None and fault.kind == "drop":
            self.sim.log(
                src, "network_drop", dst=getattr(dst, "name", str(dst)), method=method
            )
            return

        delay = self.rpc_delay
        if fault is not None and fault.kind == "delay":
            delay += fault.extra_delay

        def deliver_request() -> None:
            handler = getattr(dst, f"rpc_{method}", None)
            if handler is None:
                reply = {"ok": False, "error": f"no method {method}"}
            else:
                try:
                    reply = handler(src, payload)
                except Exception as exc:  # 服务端异常也要作为错误回复传回
                    err = {"ok": False, "error": str(exc)}
                    code = getattr(exc, "code", None)
                    if code is not None:
                        err["code"] = code
                        err.update(getattr(exc, "extra", {}) or {})
                    reply = err
            if reply is None:
                reply = {"ok": True}
            fault2 = self._match_fault(getattr(dst, "name", dst), src, method)
            back = self.rpc_delay
            if fault2 is not None and fault2.kind == "delay":
                back += fault2.extra_delay
            if fault2 is not None and fault2.kind == "drop":
                self.sim.log(
                    getattr(dst, "name", str(dst)),
                    "network_drop",
                    dst=src,
                    method=method,
                )
                return

            def deliver_reply() -> None:
                if tracked is not None:
                    tracked.discard(deliver_reply)
                on_reply(reply)

            if tracked is not None:
                tracked.add(deliver_reply)
            self.sim.schedule(back, deliver_reply)

        if tracked is not None:
            tracked.add(deliver_request)
        self.sim.schedule(delay, deliver_request)

    def call_sync(
        self,
        src: str,
        dst: Any,
        method: str,
        payload: dict[str, Any],
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """同步 RPC：阻塞直到拿到本次调用的回复。

        事件循环里可能混有续约等无关事件；这里只执行本次 RPC 链路
        （请求送达、回复送达）的事件，其余事件原样留在队列中，绝不
        偷吃。消息被丢或超过 timeout 则返回超时错误。
        """
        box: dict[str, Any] = {}
        done = _SyncFlag()
        tracked: set[Callable[[], None]] = set()

        def on_reply(reply: dict[str, Any]) -> None:
            if done.is_set():
                return
            box["reply"] = reply
            done.set()

        deadline = (self.sim.clock.now() + timeout) if timeout is not None else None
        self.call(src, dst, method, payload, on_reply,
                  timeout=timeout, tracked=tracked)

        guard = 0
        while not done.is_set():
            guard += 1
            if guard > self.sim._max_events:
                raise RuntimeError("call_sync 事件数超限")
            # 找出最早的“本次链路”事件
            idx = None
            for i, evt in enumerate(self.sim._events):
                if evt.callback in tracked:
                    idx = i
                    break
            if idx is None:
                # 队列中已无本次调用的事件：消息被丢
                return {"ok": False,
                        "error": "timeout" if deadline is not None
                        else "timeout (no more events)"}
            evt = self.sim._events.pop(idx)
            heapq.heapify(self.sim._events)
            if deadline is not None and evt.time > deadline + 1e-12:
                heapq.heappush(self.sim._events, evt)
                return {"ok": False, "error": "timeout"}
            self.sim.clock.advance(max(0.0, evt.time - self.sim.clock.now()))
            tracked.discard(evt.callback)
            evt.callback()

        return box["reply"]


class _SyncFlag:
    def __init__(self) -> None:
        self._v = False

    def set(self) -> None:
        self._v = True

    def is_set(self) -> bool:
        return self._v
