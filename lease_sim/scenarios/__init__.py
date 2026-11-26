"""预置演示场景集合。

每个场景返回一个 ScenarioResult，包含世界对象、断言结果与说明文本。
"""
from .assertions import Assertion, ScenarioResult
from . import (
    pause_expiry_fencing,
    retry_idempotency,
    clock_skew,
    leader_failover,
    fencing_gap_strict,
)

SCENARIOS = {
    "pause-expiry": pause_expiry_fencing,
    "retry": retry_idempotency,
    "clock-skew": clock_skew,
    "failover": leader_failover,
    "fencing-gap": fencing_gap_strict,
}

__all__ = ["SCENARIOS", "Assertion", "ScenarioResult"]
