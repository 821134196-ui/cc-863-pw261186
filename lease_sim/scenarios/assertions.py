"""场景断言助手。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Assertion:
    name: str
    passed: bool
    detail: str

    def __str__(self) -> str:
        mark = "PASS" if self.passed else "FAIL"
        return f"[{mark}] {self.name} -- {self.detail}"


@dataclass
class ScenarioResult:
    key: str
    title: str
    world: Any
    assertions: list[Assertion] = field(default_factory=list)
    summary: str = ""

    @property
    def ok(self) -> bool:
        return all(a.passed for a in self.assertions)

    def render(self) -> str:
        lines = [
            "#" * 78,
            f"场景: {self.title} ({self.key})",
            "#" * 78,
            self.summary,
            "",
            "断言:",
        ]
        lines.extend(f"  {a}" for a in self.assertions)
        lines.append("")
        lines.append(f"场景结论: {'全部通过' if self.ok else '存在失败断言'}")
        return "\n".join(lines)
