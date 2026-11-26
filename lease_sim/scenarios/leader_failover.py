"""场景 4：协调者更换领导者。

验证三件事：
  1. 新领导者从共享 WAL 重放令牌计数，新发放的令牌严格大于换主前；
  2. 换主期间 A 的续约被旧领导者以 NOT_LEADER 拒绝（或指向新主后
     成功），不会出现两个协调者各发一套令牌；
  3. 即使把 WAL 文件删掉模拟“无盘新主”，令牌计数也只能从 WAL 恢复，
     测试以临时文件 WAL 验证重启后计数不回退。
另含单元级验证：旧任期的迟到续约回复不能给客户端续命。
"""
from __future__ import annotations

import os
import tempfile

from ..console import OperationsConsole
from ..storage import DurableWAL
from ..world import TokenMonotonicityWatcher, build_world
from .assertions import Assertion, ScenarioResult

KEY = "failover"
TITLE = "协调者换主：WAL 重放保证令牌单调、旧任期回复被忽略"


def run() -> ScenarioResult:
    tmpdir = tempfile.mkdtemp(prefix="lease-wal-")
    wal_path = os.path.join(tmpdir, "coord.wal")
    wal = DurableWAL(wal_path)

    world = build_world(
        client_names=["client-A", "client-B"],
        lease_ttl=20.0,
        rpc_delay=0.5,
        wal=wal,
        coordinator_names=["coord-A", "coord-B", "coord-C"],
    )
    console = OperationsConsole(world)

    console.acquire("client-A")          # 令牌 1, term 1
    tokens_before = [
        e["token"] for e in world.sim.audit if e["event"] == "lease_granted"
    ]

    # 换主到 coord-B（term 2），重放 WAL
    console.failover("coord-B")
    leader_b = world.group.leader
    assertions: list[Assertion] = []
    assertions.append(Assertion(
        "新领导者从 WAL 恢复令牌计数，没有从 0 重新开始",
        leader_b.token_counter == 1,
        f"counter={leader_b.token_counter}",
    ))

    # 换主后 B 拿租约 -> 令牌必须是 2
    reply_b = console.acquire("client-B")
    assertions.append(Assertion(
        "换主后发放的令牌严格递增（1 -> 2）",
        reply_b["token"] == 2 and reply_b["term"] == 2,
        str(reply_b),
    ))

    # A 在新任期下续约旧租约：协调者应回答 LEASE_LOST（租约已归 B）
    a = world.client("client-A")
    renew_payload = {
        "resource_id": world.resource_id,
        "token": a.lease.token if a.lease else 1,
    }
    # A 已经在 acquire 失败路径外，这里直接打一次 RPC 观察错误码
    reply_renew = world.net.call_sync(
        "client-A", leader_b, "renew", renew_payload
    )
    assertions.append(Assertion(
        "旧持有者在新主处续约被明确拒绝（LEASE_LOST）",
        not reply_renew.get("ok") and reply_renew.get("code") == "LEASE_LOST",
        str(reply_renew),
    ))

    # 再换一次主 coord-C（term 3），再发租约 -> 令牌 3
    console.failover("coord-C")
    reply_c = console.acquire("client-A")
    assertions.append(Assertion(
        "第二次换主后令牌继续递增（-> 3），任期同步递增",
        reply_c["token"] == 3 and reply_c["term"] == 3,
        str(reply_c),
    ))

    # 旧领导者 coord-B 已降级，收到 RPC 必须 NOT_LEADER
    stale_leader = next(m for m in world.group.members if m.name == "coord-B")
    reply_old = world.net.call_sync(
        "client-B", stale_leader, "acquire",
        {"resource_id": world.resource_id},
    )
    assertions.append(Assertion(
        "降级的旧领导者拒绝服务（NOT_LEADER），不可能另发一套令牌",
        not reply_old.get("ok") and reply_old.get("code") == "NOT_LEADER",
        str(reply_old),
    ))

    # 新建一个协调者实例指向同一份 WAL 文件，模拟冷启动恢复
    from ..coordinator import Coordinator
    cold = Coordinator("coord-cold", world.sim, DurableWAL(wal_path),
                       lease_ttl=20.0)
    assertions.append(Assertion(
        "冷启动协调者仅靠 WAL 文件即恢复到最新令牌计数",
        cold.token_counter == 3 and cold.term == 3,
        f"counter={cold.token_counter} term={cold.term}",
    ))
    cold.become_leader(4)
    cold_reply = world.net.call_sync(
        "client-B", cold, "acquire", {"resource_id": world.resource_id}
    )
    assertions.append(Assertion(
        "冷启动恢复后发放的下一个令牌为 4，不回退不重复",
        cold_reply.get("token") == 4,
        str(cold_reply),
    ))

    # 客户端任期防护：伪造一个旧任期回复，应被忽略且不续命
    watcher = TokenMonotonicityWatcher().verify(world.sim.audit)
    assertions.append(Assertion(
        "全历史令牌单调（跨三次任期、含冷启动恢复）",
        watcher["ok"],
        f"发放序列={watcher['grant_tokens']} 违规={watcher['violations']}",
    ))

    # 旧任期回复防护的单元级验证
    a2 = world.client("client-A")
    old_expire = a2.lease.expire_at
    a2._on_renew_reply({"ok": True, "term": 1, "expire_at": 999.0})
    assertions.append(Assertion(
        "旧任期的迟到续约回复被客户端忽略，不产生续命效果",
        a2.lease is None or a2.lease.expire_at == old_expire,
        f"expire={a2.lease.expire_at if a2.lease else None}",
    ))

    summary = (
        "三次任期（含一次基于 WAL 文件的冷启动）下发牌序列 1,2,3,4 "
        "严格递增；旧主降级后 NOT_LEADER，旧任期回复被客户端丢弃。"
    )
    return ScenarioResult(KEY, TITLE, world, assertions, summary)
