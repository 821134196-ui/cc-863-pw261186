"""审计报告渲染：时间线、对照结论、令牌单调性结论。"""
from __future__ import annotations

from typing import Any

from .world import TokenMonotonicityWatcher

# 事件 -> 人类可读模板
_EVENT_TEMPLATES = {
    "wal_replay": "WAL 重放: term={term}, 令牌计数={token_counter}, 租约={leases}",
    "become_leader": "成为领导者 term={term}，令牌计数={token_counter}",
    "step_down": "降级为跟随者 term={term}",
    "leader_elected": "新领导者 {leader} 上任 term={term}，令牌计数={token_counter}",
    "lease_granted": "发放租约 资源={resource} 持有者={holder} 令牌={token} term={term} 到期={expire_at}",
    "lease_revoke": "旧租约撤销 资源={resource} {old_holder}(令牌{old_token}) 转交给 {new_holder}",
    "lease_renewed": "续约成功 {holder} 令牌={token} 新到期={expire_at}",
    "lease_expired": "租约过期 资源={resource} {holder} 令牌={token}",
    "lease_lost": "客户端承认丢失租约: {reason}（最后令牌={last_token}）",
    "acquired": "获得租约 令牌={token} term={term} 到期={expire_at}",
    "renew_ok": "续约确认 令牌={token} 到期={expire_at}",
    "stale_term_reply_ignored": "忽略旧任期回复 reply_term={reply_term} my_term={my_term}",
    "paused": "进程暂停 令牌={token} 挂起写={pending_write} 本地时间={local_now}",
    "resumed": "进程恢复 令牌={token} 本地认为有效={believes_valid} 本地t={local_now} 真实t={real_now} 到期={expire_at}",
    "resume_attempt_stale_write": "恢复后尝试旧令牌写 令牌={token} 值={value} req={request_id}",
    "resume_contrast_write": "★对照写: {writer} 在 {zombie} 恢复同刻写入 值={value} 令牌={token} 接受={accepted}({reason}) 同时刻={same_instant}",
    "resume_give_up": "恢复后主动放弃: {reason} 令牌={token}",
    "write_accepted": "写接受 {holder} 令牌={token} 水位={watermark} 值={value} req={request_id}",
    "write_rejected": "写拒绝 {holder} 令牌={token} 水位={watermark} 原因={reason} 值={value}",
    "client_write_accepted": "客户端确认写接受 令牌={token} 原因={reason} 值={value} req={request_id} duplicate={duplicate}",
    "client_write_rejected": "客户端确认写拒绝 令牌={token} 原因={reason} 值={value}",
    "write_rejected_local": "本地拒绝写 原因={reason}",
    "write_transport_error": "写传输失败 令牌={token} 原因={reason} req={request_id}",
    "write_attempt": "重试第 {attempt} 次 req={request_id} 值={value}",
    "watermark_advance": "资源水位推进 {old} -> {new}（持有者 {holder}）",
    "dedup_hit": "幂等去重命中 req={request_id} 首次接受={first_accepted}",
    "network_drop": "网络丢包 -> {dst} 方法={method}",
    "rebind_coordinator": "重连协调者 {old} -> {new}",
    "clock_skew_set": "时钟差注入 客户端={client} 偏移={offset} 漂移={drift}",
}


def _format_entry(entry: dict[str, Any]) -> str:
    template = _EVENT_TEMPLATES.get(entry["event"], entry["event"])
    fields = {k: v for k, v in entry.items() if k not in ("t", "actor", "event")}
    try:
        detail = template.format(**fields)
    except (KeyError, IndexError):
        detail = entry["event"] + " " + str(fields)
    return f"t={entry['t']:>8}  {entry['actor']:<10}  {detail}"


def render_timeline(audit: list[dict[str, Any]]) -> str:
    lines = ["=" * 78, "审计时间线", "=" * 78]
    lines.extend(_format_entry(e) for e in audit)
    return "\n".join(lines)


def render_verification(audit: list[dict[str, Any]]) -> str:
    watcher = TokenMonotonicityWatcher()
    report = watcher.verify(audit)
    lines = ["=" * 78, "令牌单调性验证", "=" * 78]
    lines.append(f"协调者发放令牌序列: {report['grant_tokens']}")
    lines.append(f"资源接受写令牌序列: {report['accepted_tokens']}")
    grant_ok = all(
        b > a for a, b in zip(report["grant_tokens"], report["grant_tokens"][1:])
    )
    accept_ok = all(
        b >= a for a, b in zip(
            report["accepted_tokens"], report["accepted_tokens"][1:])
    )
    lines.append(f"发放序列严格递增: {'通过' if grant_ok else '失败'}")
    lines.append(f"接受序列单调不降: {'通过' if accept_ok else '失败'}")
    if report["violations"]:
        lines.append(f"违规: {report['violations']}")
    lines.append(f"总结论: {'通过 OK' if report['ok'] else '失败 FAIL'}")
    return "\n".join(lines)


def render_resource_check(resource_snapshot: dict[str, Any]) -> str:
    lines = ["=" * 78, "受保护资源最终状态", "=" * 78]
    lines.append(f"高水位标记: {resource_snapshot['high_water_mark']}")
    latest = resource_snapshot["latest"]
    if latest:
        lines.append(
            f"最新落盘值: {latest['value']!r}（持有者 {latest['holder']}，"
            f"令牌 {latest['token']}，t={latest['at']}）"
        )
    lines.append(f"接受写次数: {resource_snapshot['write_count']}")
    lines.append(f"拒绝写次数: {resource_snapshot['rejection_count']}")
    return "\n".join(lines)


def full_report(world: Any) -> str:
    parts = [
        render_timeline(world.sim.audit),
        "",
        render_verification(world.sim.audit),
        "",
        render_resource_check(world.resource.snapshot()),
    ]
    return "\n".join(parts)
