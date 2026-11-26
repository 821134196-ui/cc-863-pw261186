#!/usr/bin/env python3
"""交互式操作台 REPL。

启动一个两客户端、三协调者的世界，操作员用文本命令驱动场景。
输入 help 查看命令列表。示例会话：

  acquire client-A
  write client-A hello
  pause client-A pending=stale-value
  acquire client-B
  arm client-A client-B contrast=new-value
  advance 12
  resume client-A
  status
  timeline
"""
from __future__ import annotations

import sys

from .console import OperationsConsole
from .report import full_report
from .world import build_world

HELP = """\
命令:
  status                         查看世界状态（时钟、领导者、租约、资源水位）
  acquire <client> [ttl=N]       客户端获取租约
  write <client> <value>         客户端通过 RPC 真实写入
  lwrite <client> <value>        存储节点本机写入（零网络耗时）
  pause <client> [pending=<v>] [skew=<f>]  暂停客户端；skew 注入慢时钟（负数）
  arm <zombie> <writer> <value>  武装恢复瞬间对照写（本机同刻）
  resume <client>                恢复客户端（先触发对照写，再跑挂起写）
  failover <coord>               协调者换主（coord-A/coord-B/coord-C）
  skew <client> offset=<f> [drift=<f>]  注入时钟差
  drop <method>                  丢弃下一条指定方法的 RPC（acquire/renew/write）
  advance <dt>                   推进虚拟时钟
  run                            跑空当前所有事件
  timeline                       打印审计时间线与验证报告
  reset [strict]                 重建世界（strict = 开启在线验租）
  help / quit
"""


def _parse_value(tok: str):
    if tok.startswith(("str:", "int:", "float:")):
        kind, raw = tok.split(":", 1)
        return {"str": str, "int": int, "float": float}[kind](raw)
    try:
        return int(tok)
    except ValueError:
        return tok


class Repl:
    def __init__(self) -> None:
        self.world = build_world(
            client_names=["client-A", "client-B"],
            coordinator_names=["coord-A", "coord-B", "coord-C"],
        )
        self.console = OperationsConsole(self.world)

    def reset(self, strict: bool = False) -> None:
        self.world = build_world(
            client_names=["client-A", "client-B"],
            coordinator_names=["coord-A", "coord-B", "coord-C"],
            strict_validation=strict,
        )
        self.console = OperationsConsole(self.world)
        print(f"世界已重建（strict_validation={strict}）")

    def exec_line(self, line: str) -> bool:
        parts = line.split()
        if not parts:
            return True
        cmd, args = parts[0], parts[1:]
        c = self.console

        if cmd in ("quit", "exit"):
            return False
        if cmd == "help":
            print(HELP)
        elif cmd == "status":
            import json
            print(json.dumps(c.status(), ensure_ascii=False, indent=2))
        elif cmd == "acquire":
            ttl = None
            pos = [a for a in args if not a.startswith("ttl=")]
            for a in args:
                if a.startswith("ttl="):
                    ttl = float(a.split("=", 1)[1])
            print(c.acquire(pos[0], ttl=ttl))
        elif cmd == "write":
            r = c.write_now(args[0], _parse_value(args[1]))
            print(r)
        elif cmd == "lwrite":
            print(c.local_write(args[0], _parse_value(args[1])))
        elif cmd == "pause":
            pending = None
            skew = None
            for a in args[1:]:
                if a.startswith("pending="):
                    pending = _parse_value(a.split("=", 1)[1])
                elif a.startswith("skew="):
                    skew = float(a.split("=", 1)[1])
            if skew is not None:
                # 慢时钟：恢复后本地仍以为租约有效，才会真的发起旧令牌写
                c.set_clock_skew(args[0], offset=skew)
            c.pause(args[0], pending_write=pending)
            print(f"{args[0]} 已暂停")
        elif cmd == "arm":
            c.arm_resume_contrast(args[0], args[1], _parse_value(args[2]))
            print(f"已武装对照: {args[0]} 恢复时 {args[1]} 同刻写入")
        elif cmd == "resume":
            print(c.resume(args[0]))
        elif cmd == "failover":
            c.failover(args[0])
            print(f"换主完成，当前领导者: {self.world.group.leader.name}")
        elif cmd == "skew":
            offset = drift = 0.0
            for a in args[1:]:
                k, v = a.split("=", 1)
                if k == "offset":
                    offset = float(v)
                elif k == "drift":
                    drift = float(v)
            c.set_clock_skew(args[0], offset=offset, drift=drift)
        elif cmd == "drop":
            c.drop_next(method=args[0] if args else None)
            print("已注入下一条丢包")
        elif cmd == "advance":
            c.advance(float(args[0]))
            print(f"当前虚拟时间 t={self.world.sim.clock.now()}")
        elif cmd == "run":
            c.run_idle()
            print(f"事件已排空 t={self.world.sim.clock.now()}")
        elif cmd == "timeline":
            print(full_report(self.world))
        elif cmd == "reset":
            self.reset(strict=bool(args and args[0] == "strict"))
        else:
            print(f"未知命令: {cmd}（输入 help 查看帮助）")
        return True

    def run_script(self, lines: list[str]) -> None:
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            print(f"> {line}")
            if not self.exec_line(line):
                break

    def interact(self) -> None:
        print("分布式租约模拟器操作台（输入 help 查看命令，quit 退出）")
        while True:
            try:
                line = input("lease> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if line and not self.exec_line(line):
                break


def main(argv: list[str] | None = None) -> int:
    repl = Repl()
    argv = argv if argv is not None else sys.argv[1:]
    if argv:
        repl.run_script(argv)
    else:
        repl.interact()
    return 0


if __name__ == "__main__":
    sys.exit(main())
