"""模拟器基础设施测试：时钟、事件循环确定性、网络故障注入。"""
import unittest

from lease_sim.simulation import Network, Simulation


class TestClockAndEvents(unittest.TestCase):
    def test_deterministic_ordering(self):
        sim = Simulation()
        order = []
        sim.schedule(2.0, lambda: order.append("c"))
        sim.schedule(1.0, lambda: order.append("b"))
        sim.schedule(1.0, lambda: order.append("b2"))  # 同时刻按插入序
        sim.schedule(0.0, lambda: order.append("a"))
        sim.run()
        self.assertEqual(order, ["a", "b", "b2", "c"])
        self.assertAlmostEqual(sim.clock.now(), 2.0)

    def test_cancel(self):
        sim = Simulation()
        fired = []
        eid = sim.schedule(1.0, lambda: fired.append(1))
        sim.schedule(2.0, lambda: fired.append(2))
        sim.cancel(eid)
        sim.run()
        self.assertEqual(fired, [2])

    def test_jump_runs_due_events(self):
        sim = Simulation()
        ticks = []
        sim.schedule(3.0, lambda: ticks.append(sim.clock.now()))
        sim.schedule(7.0, lambda: ticks.append(sim.clock.now()))
        sim.jump(5.0)
        self.assertEqual(ticks, [3.0])
        self.assertAlmostEqual(sim.clock.now(), 5.0)

    def test_local_clock_skew(self):
        sim = Simulation()
        sim.jump(10.0)
        self.assertAlmostEqual(sim.clock.local_now(offset=-3.0), 7.0)
        self.assertAlmostEqual(sim.clock.local_now(drift=0.1), 11.0)


class _Echo:
    def __init__(self, name):
        self.name = name
        self.calls = []

    def rpc_ping(self, src, payload):
        self.calls.append((src, payload))
        return {"ok": True, "echo": payload, "receiver": self.name}


class TestNetwork(unittest.TestCase):
    def test_rpc_roundtrip(self):
        sim = Simulation()
        net = Network(sim, rpc_delay=0.5)
        dst = _Echo("dst")
        box = {}
        net.call("src", dst, "ping", {"x": 1}, lambda r: box.update(r))
        sim.run()
        self.assertEqual(box["echo"], {"x": 1})
        self.assertEqual(dst.calls, [("src", {"x": 1})])
        # 请求 0.5 + 回复 0.5
        self.assertAlmostEqual(sim.clock.now(), 1.0)

    def test_drop_fault(self):
        sim = Simulation()
        net = Network(sim)
        dst = _Echo("dst")
        net.inject_fault(
            "drop", lambda s, d, m: m == "ping", ttl=1
        )
        box = {}
        net.call("src", dst, "ping", {}, lambda r: box.update(r))
        sim.run()
        self.assertEqual(box, {})
        self.assertEqual(dst.calls, [])
        # ttl 耗尽后故障自动消失
        net.call("src", dst, "ping", {}, lambda r: box.update(r))
        sim.run()
        self.assertTrue(box.get("ok"))

    def test_sync_timeout(self):
        sim = Simulation()
        net = Network(sim)
        dst = _Echo("dst")
        net.inject_fault("drop", lambda s, d, m: True, ttl=-1)
        reply = net.call_sync("src", dst, "ping", {}, timeout=3.0)
        self.assertFalse(reply["ok"])
        self.assertIn("timeout", reply["error"])

    def test_call_sync_does_not_consume_unrelated_events(self):
        """同步 RPC 不能偷吃队列里的无关事件（如周期续约）。"""
        sim = Simulation()
        net = Network(sim, rpc_delay=0.5)
        dst = _Echo("dst")
        side_fired = []
        # 一个与本次 RPC 无关的事件，时间夹在请求与回复之间
        sim.schedule(0.7, lambda: side_fired.append("side"))
        reply = net.call_sync("src", dst, "ping", {})
        self.assertTrue(reply["ok"])
        # 无关事件必须仍在队列里，由主循环稍后执行
        self.assertEqual(side_fired, [])
        sim.run()
        self.assertEqual(side_fired, ["side"])


if __name__ == "__main__":
    unittest.main()
