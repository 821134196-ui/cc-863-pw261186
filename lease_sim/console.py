"""操作台（Operations Console）。

操作员通过操作台驱动场景：推进时钟、暂停/恢复客户端、更换协调者
领导者、注入丢包与时钟差，以及最关键的——在旧客户端恢复的**同一
虚拟时刻**，让当前有效持有者发起一笔真实写入做对照。

对照机制：``arm_resume_contrast`` 给目标客户端挂上一次性恢复钩子。
钩子在客户端 resume 流程的最前面触发，当前持有者的写先落盘、先推进
资源水位；随后旧客户端带着旧令牌的挂起写到达资源时必然被栅栏拒绝。
两次写时间戳相同，结果一接受一拒绝，形成严格对照。
"""
from __future__ import annotations

from typing import Any, Callable

from .world import World, rebind_all_clients


class ConsoleError(Exception):
    pass


class OperationsConsole:
    def __init__(self, world: World) -> None:
        self.world = world
        self._contrast_arms: dict[str, dict[str, Any]] = {}

    # ---- 基础观测 --------------------------------------------------
    def status(self) -> dict[str, Any]:
        w = self.world
        leader = w.group.leader
        lease = leader.current_lease(w.resource_id) if leader else None
        return {
            "t": w.sim.clock.now(),
            "leader": leader.name if leader else None,
            "term": w.group.term,
            "token_counter": leader.token_counter if leader else None,
            "current_lease": None if lease is None else {
                "holder": lease.holder, "token": lease.token,
                "term": lease.term,
                "expire_at": round(lease.expire_at, 6),
                "alive": lease.alive(w.sim.clock.now()),
            },
            "resource": w.resource.snapshot(),
            "clients": {
                name: {
                    "state": c.state,
                    "token": c.lease.token if c.lease else None,
                    "believes_valid": c.believes_lease_valid(),
                    "local_now": round(c.local_now(), 6),
                    "offset": c.clock_offset,
                    "drift": c.clock_drift,
                }
                for name, c in w.clients.items()
            },
        }

    # ---- 时钟推进 --------------------------------------------------
    def advance(self, delta: float) -> None:
        """推进虚拟时钟并处理期间所有事件（续约、超时、延迟消息）。"""
        self.world.sim.jump(self.world.sim.clock.now() + delta)

    def run_idle(self) -> None:
        self.world.sim.run_until_idle()

    # ---- 租约操作 --------------------------------------------------
    def acquire(self, client_name: str, ttl: float | None = None) -> dict[str, Any]:
        cli = self._client(client_name)
        reply = cli.acquire(ttl=ttl)
        if not reply.get("ok"):
            raise ConsoleError(f"获取租约失败: {reply.get('error')}")
        return reply

    def write_now(self, client_name: str, value: Any) -> Any:
        """操作员直接命令某客户端立刻发起一次真实写入（走 RPC）。"""
        cli = self._client(client_name)
        return cli.write(value)

    def local_write(self, client_name: str, value: Any) -> Any:
        """操作台在存储节点本机发起的真实写入（本地系统调用，无网络耗时）。

        走的是资源同一个 rpc_write 栅栏入口、同一套水位与幂等逻辑，
        只是不经过模拟网络，因此与恢复动作落在完全相同的虚拟时刻，
        用于构造严格的“同刻对照”。
        """
        from .client import WriteResult

        cli = self._client(client_name)
        if cli.lease is None:
            return WriteResult(False, "NO_LEASE", None, value,
                               f"{cli.name}-local")
        rid = cli._next_req_id()
        payload = {
            "token": cli.lease.token,
            "term": cli.lease.term,
            "value": value,
            "request_id": rid,
            "holder": cli.name,
        }
        reply = self.world.resource.rpc_write(cli.name, payload)
        result = WriteResult(
            accepted=bool(reply.get("accepted")),
            reason=reply.get("reason", "REJECTED"),
            token=cli.lease.token,
            value=value,
            request_id=rid,
            duplicate=bool(reply.get("duplicate")),
        )
        cli.history.append(result)
        return result

    def write_now_with_retry(self, client_name: str, value: Any,
                             max_attempts: int = 3) -> Any:
        cli = self._client(client_name)
        return cli.write_with_retry(value, max_attempts=max_attempts)

    # ---- 暂停 / 恢复 + 恢复瞬间对照 --------------------------------
    def pause(self, client_name: str, pending_write: Any = None) -> None:
        """暂停客户端；pending_write 是它恢复后想补写的值。"""
        self._client(client_name).pause(pending_write=pending_write)

    def arm_resume_contrast(
        self,
        zombie_name: str,
        current_writer_name: str,
        contrast_value: Any,
        local: bool = True,
    ) -> None:
        """武装“恢复瞬间对照”。

        zombie_name 恢复的同一虚拟时刻，操作台命令 current_writer_name
        （当前有效持有者）发起一笔真实写入 contrast_value。
        local=True 时该写是存储节点本机系统调用（零耗时、严格同时刻）；
        local=False 时走普通 RPC（可演示网络延迟下的窗口）。
        """
        if zombie_name not in self.world.clients:
            raise ConsoleError(f"未知客户端 {zombie_name}")
        if current_writer_name not in self.world.clients:
            raise ConsoleError(f"未知客户端 {current_writer_name}")
        self._contrast_arms[zombie_name] = {
            "writer": current_writer_name,
            "value": contrast_value,
            "result": None,
            "local": local,
        }

        zombie = self.world.clients[zombie_name]

        def hook(zombie_client: Any) -> dict[str, Any]:
            arm = self._contrast_arms.pop(zombie_name, None)
            if arm is None:
                return {}
            t_before = self.world.sim.clock.now()
            if arm["local"]:
                result = self.local_write(arm["writer"], arm["value"])
            else:
                # 异步投递：写还在网络上飞，僵尸写可以抢先落盘，
                # 从而真实暴露纯水位模式的防护窗口
                writer = self.world.clients[arm["writer"]]
                rid = writer.write_async(arm["value"])
                result = type(
                    "AsyncRef", (),
                    {"accepted": None, "reason": "IN_FLIGHT",
                     "token": writer.lease.token if writer.lease else None,
                     "value": arm["value"], "request_id": rid,
                     "duplicate": False},
                )()
            t_after = self.world.sim.clock.now()
            arm["result"] = result
            self.world.sim.log(
                "console", "resume_contrast_write",
                zombie=zombie_name, writer=arm["writer"],
                value=arm["value"], token=result.token,
                accepted=result.accepted, reason=result.reason,
                same_instant=(t_before == t_after),
                local=arm["local"],
            )
            return {"contrast": arm}

        zombie.on_resume_hook = hook

    def resume(self, client_name: str) -> Any:
        """恢复客户端；若武装了对照，对照写会在同一时刻先执行。"""
        return self._client(client_name).resume()

    # ---- 协调者故障切换 --------------------------------------------
    def failover(self, new_leader_name: str) -> None:
        """更换协调者领导者。新领导者从共享 WAL 恢复令牌计数。"""
        self.world.group.failover_to(new_leader_name)
        rebind_all_clients(self.world)

    # ---- 时钟差注入 ------------------------------------------------
    def set_clock_skew(self, client_name: str, offset: float = 0.0,
                       drift: float = 0.0) -> None:
        cli = self._client(client_name)
        cli.clock_offset = offset
        cli.clock_drift = drift
        self.world.sim.log(
            "console", "clock_skew_set", client=client_name,
            offset=offset, drift=drift,
        )

    # ---- 网络故障注入 ----------------------------------------------
    def inject_drop(self, predicate: Callable[[str, str, str], bool],
                    ttl: int = 1) -> None:
        self.world.net.inject_fault("drop", predicate, ttl=ttl)

    def inject_delay(self, predicate: Callable[[str, str, str], bool],
                     extra_delay: float, ttl: int = 1) -> None:
        self.world.net.inject_fault("delay", predicate, ttl=ttl,
                                    extra_delay=extra_delay)

    def drop_next(self, method: str | None = None) -> None:
        """便捷注入：丢弃下一条指定方法的 RPC（任意源/目的，一次生效）。"""
        def pred(src: str, dst: str, m: str) -> bool:
            return method is None or m == method

        self.world.net.inject_fault("drop", pred, ttl=1)

    def drop_next_to_coordinator(self, method: str | None = None) -> None:
        """便捷注入：丢弃下一条发往协调者的 RPC（请求或回复）。"""
        def pred(src: str, dst: str, m: str) -> bool:
            return dst.startswith("coord") and (method is None or m == method)

        self.world.net.inject_fault("drop", pred, ttl=1)

    # ---- 审计 ------------------------------------------------------
    def audit(self) -> list[dict[str, Any]]:
        return self.world.sim.audit

    def _client(self, name: str) -> Any:
        try:
            return self.world.clients[name]
        except KeyError:
            raise ConsoleError(f"未知客户端 {name}") from None
