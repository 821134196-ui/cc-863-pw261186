# 分布式租约与栅栏令牌模拟器

一个纯 Python（无第三方依赖）的离散事件模拟器，用来研究分布式系统中
**租约（lease）+ 栅栏令牌（fencing token）** 的安全语义。重点回答一个
经典问题：

> 租约持有者在暂停（GC、虚拟机冻结、调度停顿）期间租约已经过期，
> 它恢复后还可能继续写——被保护的本地资源凭什么拒绝它？

答案是 Martin Kleppmann 在 *How to do distributed locking* 中给出的
**栅栏令牌**：协调者每次发放租约都产生一个严格递增的令牌，资源自己
维护一个单调水位，只接受令牌不低于水位的写。本模拟器把这套机制、
以及它在重试、时钟差、协调者换主下的行为完整实现并可确定性重放。

## 它能演示什么

| 场景 | 验证的安全性质 |
| --- | --- |
| `pause-expiry` | 持有者暂停到过期后恢复；操作台在恢复的**同一虚拟时刻**让新持有者真实写入做对照，旧持有者的写被 `STALE_FENCING_TOKEN` 拒绝 |
| `retry` | 超时重试下的幂等：请求丢失重试正常落盘；回复丢失重试命中 request_id 去重，绝不重复写 |
| `clock-skew` | 慢时钟客户端在真实过期后仍“坚信”租约有效；资源不看时钟，按令牌水位（或在线验租）拒绝 |
| `failover` | 协调者换主/冷启动后从 WAL 恢复令牌计数，跨任期发牌 1→2→3→4 严格递增；旧主 `NOT_LEADER`、旧任期回复被客户端丢弃 |
| `fencing-gap` | 诚实暴露纯本地水位的防护窗口（新持有者尚未触达资源），并用 strict 在线验租关闭该窗口 |

## 架构

```
lease_sim/
├── simulation.py   虚拟时钟、确定性事件循环、可注入丢包/延迟的 RPC 网络
├── storage.py      持久 WAL（fsync 语义的原子写），换主/冷启动恢复令牌计数
├── coordinator.py  协调者、领导者任期(term)、单调栅栏令牌发放、租约撤销
├── client.py       租约客户端：自动续约、暂停/恢复、挂起写、时钟偏移/漂移、重试
├── resource.py     被保护本地资源：栅栏水位检查 + request_id 幂等去重（+可选在线验租）
├── world.py        装配层、令牌单调性全局监查器
├── console.py      操作台：恢复瞬间同刻真实写入对照、换主、故障注入
├── report.py       审计时间线与验证报告渲染
├── repl.py         交互式操作台
└── scenarios/      五个预置场景与断言
```

### 关键设计

- **令牌先落盘再回复**：`Coordinator` 每次发牌前把新计数写入 WAL，
  进程崩溃或换主后重放，计数绝不回退；`_persist` 里还有一道
  `TOKEN_REGRESSION` 断言兜底。
- **资源是最后防线，且不依赖时钟**：`FencedResource` 只比较令牌与
  本地高水位，因此任何客户端时钟偏差都无法绕过。
- **同刻对照**：操作台的 `arm_resume_contrast` 在旧客户端 `resume()`
  流程最前面触发，本机写（零网络耗时）与恢复落在完全相同的虚拟时刻；
  也可切换为异步 RPC 写，真实暴露网络延迟下的防护窗口。
- **同步 RPC 不偷吃无关事件**：`call_sync` 只推进本次调用链路的事件，
  续约等周期事件原样留在队列里。

## 运行

需要 Python 3.10+，无第三方依赖。

```bash
# 全部场景 + 断言
python3 run_demo.py verify

# 单个场景（带完整审计时间线）
python3 run_demo.py run pause-expiry

# 交互式操作台
python3 run_console.py
# 或脚本化驱动
python3 run_console.py \
  "acquire client-A" "write client-A hello" \
  "pause client-A pending=stale skew=-15" \
  "acquire client-B" "arm client-A client-B fresh" \
  "advance 12" "resume client-A" "timeline"
```

操作台命令输入 `help` 查看；支持 `failover coord-B`、
`skew client-A offset=-20`、`drop write` 等故障注入。

## 测试

```bash
python3 -I -m unittest discover -s tests -t /workspace -v
```

21 个测试覆盖：时钟/事件循环确定性、网络丢包与超时、WAL 重放、
栅栏拒绝与幂等去重、五个端到端场景、以及每个场景的令牌单调性。
