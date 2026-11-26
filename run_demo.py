#!/usr/bin/env python3
"""演示入口：把仓库根目录加入 sys.path 后转交 lease_sim.demo。

始终以隔离模式语义运行（不依赖当前工作目录、不读取用户 site 包）。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lease_sim.demo import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
