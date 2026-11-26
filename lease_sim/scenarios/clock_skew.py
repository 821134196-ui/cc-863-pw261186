"""场景 3：时钟差下的错误信念与资源侧防护。

客户端 A 的本地时钟比真实时间慢 8 个单位。租约在真实时间 t=10 过期，
但 A 的本地时钟到真实 t=15 时仍显示 t=7，于是 A “坚信”租约有效并继续
写。资源完全不看时钟，只按令牌水位决策：
  - B 拿到新令牌后，A 的旧令牌写被 STALE_FENCING_TOKEN 拒绝；
  - 开启 strict 在线验租的对照世界里，即使 B 还没写过、水位没推进，
    A 的写也会被协调者侧的 TOKEN_NOT_CURRENT 拒绝。
"""
from __future__ import annotations

from ..console import OperationsConsole
from ..world import build_world
from .assertions import Assertion, ScenarioResult

KEY = "clock-skew"
TITLE = "时钟差：慢时钟客户端的错误信念被资源侧纠正"


def run() -> ScenarioResult:
    world = build_world(client_names=["client-A", "client-B"],
                        lease_ttl=10.0, rpc_delay=0.5)
    console = OperationsConsole(world)

    console.acquire("client-A")
    # A 的本地时钟慢 20：真实 t 时 A 只观察到 t-20
    console.set_clock_skew("client-A", offset=-20.0)

    # 走到真实 t=24（租约已于真实 t=22.5 过期），B 获取新租约（令牌 2）。
    # 此刻 A 的本地时钟只显示 t=4，仍“坚信”自己的租约有效。
    # 注意 acquire 的 RPC 往返耗时 1.0，结束时真实时间为 25.0。
    console.advance(24.0)
    console.acquire("client-B")

    a = world.client("client-A")
    assertions: list[Assertion] = []
    assertions.append(Assertion(
        "慢时钟让 A 在真实过期后仍坚信租约有效",
        a.believes_lease_valid() is True
        and a.local_now() < 22.5
        and world.sim.clock.now() >= 22.5,
        f"本地={a.local_now()} 真实={world.sim.clock.now()} "
        f"到期={a.lease.expire_at}",
    ))

    # B 先写一笔把资源水位推进到 2
    console.write_now("client-B", "B-value")
    # A 基于错误信念继续写
    stale = console.write_now("client-A", "A-skewed-stale")

    assertions.append(Assertion(
        "资源不看时钟：A 的旧令牌写被水位拒绝",
        not stale.accepted and stale.reason == "STALE_FENCING_TOKEN",
        str(stale),
    ))

    # ---- strict 对照世界：B 还没触达资源，水位未推进也能拒绝 ----
    strict_world = build_world(
        client_names=["client-A", "client-B"], lease_ttl=10.0,
        rpc_delay=0.5, strict_validation=True,
    )
    sc = OperationsConsole(strict_world)
    sc.acquire("client-A")
    sc.set_clock_skew("client-A", offset=-20.0)
    sc.advance(24.0)
    sc.acquire("client-B")  # 水位仍为 1（B 没写过）
    strict_stale = sc.write_now("client-A", "A-skewed-stale")

    assertions.append(Assertion(
        "strict 模式：水位未推进时在线验租也拒绝旧持有者",
        not strict_stale.accepted
        and strict_stale.reason.startswith("TOKEN_NOT_CURRENT"),
        f"水位={strict_world.resource.high_water_mark} 结果={strict_stale}",
    ))
    assertions.append(Assertion(
        "普通世界最终值归 B；strict 世界僵尸写未落盘（无新写入记录）",
        world.resource.latest.holder == "client-B"
        and strict_world.resource.latest is None
        and strict_world.resource.high_water_mark == 0,
        f"普通世界={world.resource.latest.holder}, "
        f"strict世界 latest={strict_world.resource.latest}, "
        f"水位={strict_world.resource.high_water_mark}",
    ))

    summary = (
        "慢 8 个单位的时钟让 A 在租约真实过期后仍自认为持有者；"
        "普通模式由资源水位拦截，strict 模式由协调者在线验租拦截，"
        "正确性从不依赖客户端时钟。"
    )
    return ScenarioResult(KEY, TITLE, world, assertions, summary)
