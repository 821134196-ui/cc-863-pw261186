"""协调者、WAL 与资源栅栏的单元测试。"""
import os
import tempfile
import unittest

from lease_sim.coordinator import Coordinator, RpcError
from lease_sim.resource import FencedResource
from lease_sim.simulation import Network, Simulation
from lease_sim.storage import DurableWAL


class TestCoordinator(unittest.TestCase):
    def _setup(self, wal=None, ttl=10.0):
        sim = Simulation()
        Network(sim)
        coord = Coordinator("coord", sim, wal, lease_ttl=ttl)
        coord.become_leader(1)
        return sim, coord

    def test_grant_monotonic_and_exclusive(self):
        _, coord = self._setup()
        r1 = coord.rpc_acquire("A", {"resource_id": "r"})
        r2 = coord.rpc_acquire("B", {"resource_id": "r"})
        self.assertEqual((r1["token"], r2["token"]), (1, 2))
        lease = coord.current_lease("r")
        self.assertEqual(lease.holder, "B")
        # A 续约旧令牌必须失败
        with self.assertRaises(RpcError) as ctx:
            coord.rpc_renew("A", {"resource_id": "r", "token": 1})
        self.assertEqual(ctx.exception.code, "LEASE_LOST")

    def test_expired_renew_rejected(self):
        sim, coord = self._setup(ttl=5.0)
        coord.rpc_acquire("A", {"resource_id": "r"})
        sim.jump(6.0)
        with self.assertRaises(RpcError) as ctx:
            coord.rpc_renew("A", {"resource_id": "r", "token": 1})
        self.assertEqual(ctx.exception.code, "LEASE_EXPIRED")

    def test_step_down_not_leader(self):
        _, coord = self._setup()
        coord.step_down()
        with self.assertRaises(RpcError) as ctx:
            coord.rpc_acquire("A", {"resource_id": "r"})
        self.assertEqual(ctx.exception.code, "NOT_LEADER")

    def test_term_cannot_go_backwards(self):
        _, coord = self._setup()
        with self.assertRaises(RpcError):
            coord.become_leader(1)  # 相同任期
        with self.assertRaises(RpcError):
            coord.become_leader(0)

    def test_wal_replay_preserves_counter(self):
        tmp = tempfile.mkdtemp()
        path = os.path.join(tmp, "wal.json")
        sim1 = Simulation()
        Network(sim1)
        wal1 = DurableWAL(path)
        c1 = Coordinator("c1", sim1, wal1)
        c1.become_leader(1)
        c1.rpc_acquire("A", {"resource_id": "r"})
        c1.rpc_acquire("B", {"resource_id": "r"})

        # 全新进程从同一文件恢复
        sim2 = Simulation()
        Network(sim2)
        c2 = Coordinator("c2", sim2, DurableWAL(path))
        c2.become_leader(2)
        self.assertEqual(c2.token_counter, 2)
        r3 = c2.rpc_acquire("C", {"resource_id": "r"})
        self.assertEqual(r3["token"], 3)
        self.assertEqual(c2.current_lease("r").holder, "C")


class TestFencedResource(unittest.TestCase):
    def _res(self):
        sim = Simulation()
        return sim, FencedResource("store", sim)

    def test_fencing_rejects_stale_and_advances_watermark(self):
        sim, res = self._res()
        ok1 = res.rpc_write("A", {"token": 1, "term": 1, "value": "a",
                                  "request_id": "r1", "holder": "A"})
        ok2 = res.rpc_write("B", {"token": 2, "term": 1, "value": "b",
                                  "request_id": "r2", "holder": "B"})
        stale = res.rpc_write("A", {"token": 1, "term": 1, "value": "old",
                                    "request_id": "r3", "holder": "A"})
        self.assertTrue(ok1["accepted"] and ok2["accepted"])
        self.assertFalse(stale["accepted"])
        self.assertEqual(stale["reason"], "STALE_FENCING_TOKEN")
        self.assertEqual(res.high_water_mark, 2)
        self.assertEqual(res.latest.value, "b")
        self.assertEqual(len(res.rejections), 1)

    def test_idempotent_retry(self):
        _, res = self._res()
        first = res.rpc_write("A", {"token": 1, "value": "x",
                                    "request_id": "dup", "holder": "A"})
        second = res.rpc_write("A", {"token": 1, "value": "x",
                                     "request_id": "dup", "holder": "A"})
        self.assertTrue(first["accepted"])
        self.assertTrue(second["accepted"])
        self.assertTrue(second["duplicate"])
        self.assertEqual(len(res.history), 1)  # 只落盘一次

    def test_rejection_is_also_deduped(self):
        _, res = self._res()
        res.rpc_write("B", {"token": 5, "value": "b",
                            "request_id": "b1", "holder": "B"})
        p1 = {"token": 1, "value": "old", "request_id": "a1", "holder": "A"}
        r1 = res.rpc_write("A", p1)
        r2 = res.rpc_write("A", p1)
        self.assertFalse(r1["accepted"])
        self.assertTrue(r2["duplicate"])
        self.assertEqual(len(res.rejections), 1)


if __name__ == "__main__":
    unittest.main()
