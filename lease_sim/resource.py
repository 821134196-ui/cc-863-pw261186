"""被保护的本地资源（fenced resource）。

这是整个安全模型的最后一道防线。资源自己维护一个单调水位
``high_water_mark``：只有携带令牌 >= 水位的写才被允许，令牌落后的
旧持有者一律拒绝，无论它的租约在自己看来是否还有效。

关键性质：
* 检查与提交是原子的（单线程事件循环天然保证）；
* 同一 request_id 的重试写做幂等去重，返回首次结果，不重复落盘；
* 完全不依赖任何时钟——只比较令牌大小，所以时钟差无法绕过防护。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class StoredWrite:
    token: int
    holder: str
    value: Any
    request_id: str
    at: float


class FencedResource:
    def __init__(
        self,
        name: str,
        sim: Any,
        validator: Any = None,
        resource_id: str | None = None,
    ) -> None:
        self.name = name
        self.sim = sim
        # 可选：在线验租回调 (resource_id, token, holder) -> (ok, reason)。
        # 开启后资源在水位检查之外再向协调者确认令牌仍是当前租约，
        # 堵住“新持有者尚未触达资源、水位还没推进”的窗口。
        self.validator = validator
        self.resource_id = resource_id or name
        self.high_water_mark = 0
        self.latest: StoredWrite | None = None
        self.history: list[StoredWrite] = []
        # request_id -> 首次结果（用于幂等重放）
        self._dedup: dict[str, dict[str, Any]] = {}
        # 记录所有被拒绝的尝试，便于审计“旧持有者被拒绝”
        self.rejections: list[dict[str, Any]] = []

    def rpc_write(self, src: str, payload: dict[str, Any]) -> dict[str, Any]:
        token = int(payload["token"])
        term = payload.get("term")
        value = payload["value"]
        request_id = str(payload["request_id"])
        holder = payload.get("holder", src)
        now = self.sim.clock.now()

        # 1) 幂等去重：重试请求返回首次的确定结果
        if request_id in self._dedup:
            first = self._dedup[request_id]
            self.sim.log(
                self.name, "dedup_hit", request_id=request_id,
                first_accepted=first["accepted"],
            )
            return dict(first, duplicate=True)

        # 2) 栅栏检查：令牌必须不低于已观察到的水位
        if token < self.high_water_mark:
            reply = {
                "accepted": False,
                "reason": "STALE_FENCING_TOKEN",
                "your_token": token,
                "high_water_mark": self.high_water_mark,
                "request_id": request_id,
            }
            self._record_rejection(reply, now, holder, token, value)
            return reply

        # 2b) 可选的强校验：向协调者确认该令牌仍是当前有效租约。
        #     纯本地水位无法区分“令牌不小于水位但已过期”的写（新持有者
        #     还没触达本资源的窗口），在线验租可以堵住这个窗口。
        if self.validator is not None:
            ok, vreason = self.validator(self.resource_id, token, holder)
            if not ok:
                reply = {
                    "accepted": False,
                    "reason": vreason,
                    "your_token": token,
                    "high_water_mark": self.high_water_mark,
                    "request_id": request_id,
                }
                self._record_rejection(reply, now, holder, token, value)
                return reply

        # 3) 接受写并推进水位（同令牌重复 request_id 已在上面拦截）
        if token == self.high_water_mark and self.latest is not None \
                and self.latest.token == token:
            # 同一持有者用同令牌、不同 request_id 的第二次写：
            # 允许（租约期内可多次写），但水位不变。
            pass

        record = StoredWrite(
            token=token, holder=holder, value=value,
            request_id=request_id, at=now,
        )
        self.latest = record
        self.history.append(record)
        if token > self.high_water_mark:
            self.sim.log(
                self.name, "watermark_advance",
                old=self.high_water_mark, new=token, holder=holder,
            )
            self.high_water_mark = token

        reply = {
            "accepted": True,
            "reason": "OK",
            "stored_token": token,
            "term": term,
            "high_water_mark": self.high_water_mark,
            "request_id": request_id,
        }
        self._dedup[request_id] = reply
        self.sim.log(
            self.name, "write_accepted", holder=holder, token=token,
            watermark=self.high_water_mark, value=value,
            request_id=request_id,
        )
        return reply

    def _record_rejection(self, reply: dict[str, Any], now: float,
                          holder: str, token: int, value: Any) -> None:
        request_id = reply["request_id"]
        self._dedup[request_id] = reply
        self.rejections.append(
            {"t": now, "holder": holder, "token": token,
             "watermark": self.high_water_mark, "request_id": request_id,
             "value": value}
        )
        self.sim.log(
            self.name, "write_rejected", holder=holder, token=token,
            watermark=self.high_water_mark, reason=reply["reason"],
            request_id=request_id, value=value,
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "high_water_mark": self.high_water_mark,
            "latest": None if self.latest is None else {
                "token": self.latest.token,
                "holder": self.latest.holder,
                "value": self.latest.value,
                "request_id": self.latest.request_id,
                "at": round(self.latest.at, 6),
            },
            "write_count": len(self.history),
            "rejection_count": len(self.rejections),
        }
