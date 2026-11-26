"""场景 5：栅栏水位的防护窗口，以及在线验租如何关闭它。

纯本地水位模式有一个已知窗口：新持有者 B 拿到租约后、第一次写触达
资源之前，资源水位还停在旧令牌。此时“僵尸” A 用旧令牌写，令牌不
小于水位，会被错误接受。

本场景用两个对照世界展示：
  普通世界：构造该窗口，A 的过期写被接受（明确暴露风险）；
  strict 世界：资源在水位检查之外向协调者在线验租，A 的同一笔写被
  TOKEN_NOT_CURRENT 拒绝，窗口被关闭。
"""
from __future__ import annotations

from ..console import OperationsConsole
from ..world import build_world
from .assertions import Assertion, ScenarioResult

KEY = "fencing-gap"
TITLE = "水位防护窗口暴露与 strict 在线验租关闭窗口"


def _build(strict: bool):
    world = build_world(
        client_names=["client-A", "client-B"],
        lease_ttl=10.0,
        rpc_delay=0.5,
        strict_validation=strict,
    )
    console = OperationsConsole(world)
    console.acquire("client-A")
    console.write_now("client-A", "A-first")   # 水位推进到 1
    # A 的本地时钟慢 20：恢复后本地仍显示租约未过期，才会真正发起僵尸写
    console.set_clock_skew("client-A", offset=-20.0)
    console.pause("client-A", pending_write="A-zombie-in-gap")
    # 租约过期后 B 拿新令牌（令牌 2），但 B 此刻不写资源
    console.advance(12.0)
    console.acquire("client-B")
    return world, console


def run() -> ScenarioResult:
    # ---- 普通世界：暴露窗口 ---------------------------------------
    world, console = _build(strict=False)
    zombie_accept = console.resume("client-A")
    res = world.resource
    assertions: list[Assertion] = []
    assertions.append(Assertion(
        "普通模式暴露窗口：水位未推进时僵尸写被错误接受",
        zombie_accept is not None and zombie_accept.accepted,
        f"结果={zombie_accept} 水位={res.high_water_mark}",
    ))
    assertions.append(Assertion(
        "窗口写之后 B 再写，水位仍能推进并覆盖僵尸值（最终一致归 B）",
        (lambda r: (
            console.write_now("client-B", "B-real"),
            r.latest.holder == "client-B" and r.high_water_mark == 2,
        )[1])(res),
        str(res.snapshot()),
    ))

    # ---- strict 世界：在线验租关闭窗口 ----------------------------
    sworld, sconsole = _build(strict=True)
    s_zombie = sconsole.resume("client-A")
    sres = sworld.resource
    assertions.append(Assertion(
        "strict 模式：同一笔僵尸写在水位=1 时即被在线验租拒绝",
        s_zombie is not None and not s_zombie.accepted
        and s_zombie.reason.startswith("TOKEN_NOT_CURRENT"),
        f"结果={s_zombie} 水位={sres.high_water_mark}",
    ))
    assertions.append(Assertion(
        "strict 资源中没有任何来自僵尸 A 的过期落盘",
        all(w.holder == "client-A" and w.token == 1 for w in sres.history)
        and len(sres.history) == 1,
        str([(w.holder, w.token, w.value) for w in sres.history]),
    ))

    summary = (
        "栅栏令牌是必要防线但纯本地实现存在“新主未触达”窗口；"
        "资源额外向协调者在线验租（strict）可在水位未推进时即拒绝"
        "非当前持有者，代价是每次写多一次协调者查询。"
    )
    return ScenarioResult(KEY, TITLE, world, assertions, summary)
