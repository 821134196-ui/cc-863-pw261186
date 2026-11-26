"""场景 1（旗舰）：持有者暂停到租约过期后恢复，操作台同刻真实写入对照。

时间线：
  t=0  A 获取租约（令牌 1），开始周期续约
  t=5  A 带着挂起写被暂停；同刻 B 获取租约（令牌 2），A 的租约被撤销
  t=17 时钟跨过 A 视角的过期点（t=10），A 恢复：
        - 操作台先在存储节点本机命令 B 真实写入（零耗时，同一虚拟时刻）
        - 资源水位推进到令牌 2
        - A 紧接着用令牌 1 尝试挂起写 -> STALE_FENCING_TOKEN 被拒绝
"""
from __future__ import annotations

from ..console import OperationsConsole
from ..world import TokenMonotonicityWatcher, build_world
from .assertions import Assertion, ScenarioResult

KEY = "pause-expiry"
TITLE = "暂停到过期后恢复——旧持有者被栅栏拒绝 + 同刻真实写入对照"


def run() -> ScenarioResult:
    world = build_world(
        client_names=["client-A", "client-B"],
        lease_ttl=10.0,
        rpc_delay=0.5,
        renew_interval=3.0,
    )
    console = OperationsConsole(world)
    sim = world.sim

    # t=0: A 拿租约并写一笔，资源水位到 1
    console.acquire("client-A")
    console.write_now("client-A", "A-initial")
    # A 本地时钟慢 15：恢复后它仍“坚信”租约有效，才会真的发起旧令牌写
    console.set_clock_skew("client-A", offset=-15.0)

    # t=5: A 暂停（带挂起写），B 同刻获取新租约
    console.advance(5.0)
    console.pause("client-A", pending_write="A-stale-after-resume")
    console.acquire("client-B")  # A 的租约此刻在协调者侧被撤销

    # 武装恢复瞬间对照：A 恢复的同一时刻，B 在存储本机真实写入
    console.arm_resume_contrast(
        "client-A", "client-B",
        contrast_value="B-contrast-write", local=True,
    )

    # 时钟走过 A 的过期点（t=10）直到 t=17 再恢复
    console.advance(12.0)
    t_resume = sim.clock.now()
    zombie_result = console.resume("client-A")

    # ---- 断言 -----------------------------------------------------
    a = world.client("client-A")
    b = world.client("client-B")
    res = world.resource

    assertions: list[Assertion] = []

    assertions.append(Assertion(
        "旧持有者恢复后的写被资源拒绝",
        zombie_result is not None and not zombie_result.accepted
        and zombie_result.reason == "STALE_FENCING_TOKEN",
        f"A 的结果: {zombie_result}",
    ))

    # 找到对照写审计条目
    contrast_entries = [e for e in sim.audit
                        if e["event"] == "resume_contrast_write"]
    assertions.append(Assertion(
        "操作台在恢复瞬间发起了真实写入且被接受",
        bool(contrast_entries) and contrast_entries[0]["accepted"],
        str(contrast_entries),
    ))
    assertions.append(Assertion(
        "对照写与恢复动作落在完全相同的虚拟时刻（零网络耗时本机写）",
        bool(contrast_entries) and contrast_entries[0]["same_instant"] is True,
        f"恢复时刻 t={t_resume}",
    ))

    # 被拒记录里确实是 A 的旧令牌 1 撞在水位 2 上
    assertions.append(Assertion(
        "拒绝原因可追溯：旧令牌 1 < 资源高水位 2",
        len(res.rejections) == 1
        and res.rejections[0]["token"] == 1
        and res.rejections[0]["watermark"] == 2,
        str(res.rejections),
    ))

    assertions.append(Assertion(
        "资源最终保留的是新持有者 B 的值，旧值从未覆盖",
        res.latest is not None
        and res.latest.holder == "client-B"
        and res.latest.value == "B-contrast-write"
        and res.latest.token == 2,
        str(res.snapshot()),
    ))

    watcher = TokenMonotonicityWatcher().verify(sim.audit)
    assertions.append(Assertion(
        "全历史令牌单调（发放与接受两条序列）",
        watcher["ok"],
        f"发放={watcher['grant_tokens']} 接受={watcher['accepted_tokens']}",
    ))

    assertions.append(Assertion(
        "被拒后客户端 A 主动承认丢失租约",
        a.state == "NO_LEASE" and a.lease is None,
        f"state={a.state}",
    ))

    summary = (
        "A 暂停期间租约已被 B 取代；恢复瞬间操作台让 B 同刻真实写入，"
        "资源水位先推进到令牌 2，A 的令牌 1 写被栅栏拒绝。"
        "整个防护不依赖时钟，只比较令牌大小。"
    )
    return ScenarioResult(KEY, TITLE, world, assertions, summary)
