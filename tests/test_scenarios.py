"""端到端测试：五个预置场景全部通过 + 客户端行为细节。"""
import unittest

from lease_sim.client import HAS_LEASE, NO_LEASE, PAUSED
from lease_sim.console import OperationsConsole
from lease_sim.scenarios import SCENARIOS
from lease_sim.world import TokenMonotonicityWatcher, build_world


class TestScenarios(unittest.TestCase):
    def test_all_scenarios_pass(self):
        for key, mod in SCENARIOS.items():
            with self.subTest(scenario=key):
                result = mod.run()
                failed = [str(a) for a in result.assertions if not a.passed]
                self.assertEqual(failed, [], msg=f"场景 {key} 有失败断言")
                self.assertTrue(result.ok)

    def test_token_monotonicity_every_scenario(self):
        for key, mod in SCENARIOS.items():
            with self.subTest(scenario=key):
                result = mod.run()
                report = TokenMonotonicityWatcher().verify(
                    result.world.sim.audit
                )
                self.assertTrue(report["ok"], msg=str(report["violations"]))
                # 发放序列必须严格递增
                toks = report["grant_tokens"]
                self.assertEqual(toks, sorted(set(toks)))


class TestClientBehavior(unittest.TestCase):
    def test_pause_blocks_renewal_and_resume_recovers_state(self):
        world = build_world(client_names=["client-A"], lease_ttl=10.0,
                            renew_interval=3.0)
        console = OperationsConsole(world)
        console.acquire("client-A")
        a = world.client("client-A")
        console.advance(2.0)
        console.pause("client-A")
        self.assertEqual(a.state, PAUSED)
        # 暂停 20 个时间单位，期间续约事件不再生效
        console.advance(20.0)
        self.assertEqual(a.state, PAUSED)
        console.resume("client-A")
        # 本地时钟无偏差时，恢复后自己也发现过期
        self.assertEqual(a.state, NO_LEASE)

    def test_renewal_keeps_lease_alive_without_pause(self):
        world = build_world(client_names=["client-A"], lease_ttl=10.0,
                            renew_interval=3.0)
        console = OperationsConsole(world)
        console.acquire("client-A")
        console.advance(25.0)  # 远超 TTL，但续约一直在跑
        a = world.client("client-A")
        self.assertEqual(a.state, HAS_LEASE)
        self.assertGreater(a.lease.expire_at, world.sim.clock.now())

    def test_contrast_same_instant(self):
        """恢复瞬间对照写必须与恢复落在同一虚拟时刻。"""
        world = build_world(client_names=["client-A", "client-B"],
                            lease_ttl=10.0)
        console = OperationsConsole(world)
        console.acquire("client-A")
        console.set_clock_skew("client-A", offset=-15.0)
        console.advance(5.0)
        console.pause("client-A", pending_write="stale")
        console.acquire("client-B")
        console.arm_resume_contrast(
            "client-A", "client-B", "fresh", local=True
        )
        console.advance(12.0)
        t0 = world.sim.clock.now()
        zombie = console.resume("client-A")
        contrast = [e for e in world.sim.audit
                    if e["event"] == "resume_contrast_write"][0]
        # 对照写是存储节点本机系统调用，本身零耗时（同刻标记为真）；
        # 僵尸自己的写走 RPC，会有正常往返耗时
        self.assertTrue(contrast["same_instant"])
        self.assertFalse(zombie.accepted)
        self.assertEqual(zombie.reason, "STALE_FENCING_TOKEN")
        self.assertEqual(world.resource.latest.value, "fresh")

    def test_rpc_contrast_exposes_network_window_then_recovers(self):
        """走 RPC 的对照写有网络耗时，僵尸写可能先到 —— 窗口可观测。"""
        world = build_world(client_names=["client-A", "client-B"],
                            lease_ttl=10.0, rpc_delay=2.0)
        console = OperationsConsole(world)
        console.acquire("client-A")
        # 对照 RPC 往返 4 个时间单位，偏移要足够大，保证对照写结束后
        # A 的本地时钟仍显示租约有效，它才会真正发起僵尸写
        console.set_clock_skew("client-A", offset=-25.0)
        console.advance(5.0)
        console.pause("client-A", pending_write="stale")
        console.acquire("client-B")
        console.arm_resume_contrast(
            "client-A", "client-B", "fresh", local=False
        )
        console.advance(12.0)
        zombie = console.resume("client-A")
        # B 的 RPC 写还在路上，水位未推进，A 的旧令牌写此刻被接受
        self.assertTrue(zombie.accepted)
        console.run_idle()  # 等 B 的写到达，水位推进到 2
        self.assertEqual(world.resource.high_water_mark, 2)
        self.assertEqual(world.resource.latest.holder, "client-B")


if __name__ == "__main__":
    unittest.main()
