"""场景 2：超时重试下的幂等性。

两类传输故障：
  (a) 请求在途中丢失 -> 资源从未收到，重试是一条全新消息，正常落盘；
  (b) 回复在途中丢失 -> 资源其实已落盘，客户端超时后用同一个
      request_id 重试，资源命中幂等去重，返回 duplicate=True，
      值绝不重复写入。
"""
from __future__ import annotations

from ..console import OperationsConsole
from ..world import build_world
from .assertions import Assertion, ScenarioResult

KEY = "retry"
TITLE = "超时重试与幂等去重"


def run() -> ScenarioResult:
    world = build_world(client_names=["client-A"], lease_ttl=30.0,
                        rpc_delay=0.5)
    console = OperationsConsole(world)
    console.acquire("client-A")

    # (a) 第一次写：请求被丢，客户端收到超时，重试成功
    console.inject_drop(
        lambda src, dst, method: src == "client-A" and method == "write",
        ttl=1,
    )
    r1 = console.write_now_with_retry("client-A", "v1", max_attempts=3)

    # (b) 第二次写：请求送达但回复被丢；重试命中服务端幂等去重
    console.inject_drop(
        lambda src, dst, method: dst == "client-A" and method == "write",
        ttl=1,
    )
    r2 = console.write_now_with_retry("client-A", "v2", max_attempts=3)

    res = world.resource
    dedup_events = [e for e in res._dedup.items()]
    assertions: list[Assertion] = []

    assertions.append(Assertion(
        "请求丢失后重试最终成功",
        r1.accepted and r1.value == "v1" and not r1.duplicate,
        str(r1),
    ))
    assertions.append(Assertion(
        "回复丢失后重试返回首次结果且标记 duplicate",
        r2.accepted and r2.duplicate and r2.value == "v2",
        str(r2),
    ))
    assertions.append(Assertion(
        "两个 request_id 各自只落盘一次（共 2 条写记录）",
        len(res.history) == 2
        and [w.value for w in res.history] == ["v1", "v2"],
        str([(w.request_id, w.value) for w in res.history]),
    ))
    assertions.append(Assertion(
        "去重表中每个 request_id 只对应一个确定结果",
        len(res._dedup) == 2
        and all(reply["accepted"] for reply in res._dedup.values()),
        str({k: v["accepted"] for k, v in dedup_events}),
    ))
    assertions.append(Assertion(
        "同一 request_id 的两次尝试令牌一致（重试不换新令牌）",
        r2.request_id in res._dedup
        and res._dedup[r2.request_id]["stored_token"] == r2.token,
        f"token={r2.token}",
    ))

    summary = (
        "重试全程复用 request_id：请求丢失时重试为新消息正常处理；"
        "回复丢失时服务端幂等去重，返回首次确定结果，不重复落盘。"
    )
    return ScenarioResult(KEY, TITLE, world, assertions, summary)
