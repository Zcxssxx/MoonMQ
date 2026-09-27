# MoonMQ 黑客松验收自查

自查更新日期：2026-09-27。参赛项目和时间节点以[2026 MoonBit 黑客松官方页面](https://moonbitlang.github.io/Hackathon2026/)为准；项目协议边界以 [`protocol-profile.md`](protocol-profile.md) 为准。

## 本地可检查的材料

| 验收关注点 | 项目证据 | 本地检查方式 |
| --- | --- | --- |
| MoonBit 是主要实现语言，项目包含可说明的功能 | `moon.mod`、`src/` 下的 MoonBit 源码 | 检查模块声明、Broker、协议 codec 与 portable Session 实现 |
| 项目用途、状态和运行方式清楚 | `README.md` | 从项目定位、Core Profile 范围、demo 和限制开始阅读 |
| 有可复现的示例 | `examples/embedded/README.md`、`cmd/moonmq`、`examples/embedded` | `moon run cmd/moonmq`；`moon run examples/embedded` |
| 有测试覆盖且 CI 检查相同后端 | `src/**/*_test.mbt`、`.github/workflows/ci.yml` | `moon check --deny-warn --target all`；`moon test --deny-warn --target all`；`moon fmt --check` |
| Native server 可构建 | `cmd/moonmq-server`、`src/transport/native` | `moon build --target native cmd/moonmq-server` |
| 开源许可证和依赖来源可查 | `moon.mod`、`LICENSE`、`THIRD_PARTY.md` | 确认模块 Apache-2.0 声明与许可证文件，并检查 async 依赖来源记录 |
| 项目仓库元数据有本地来源 | `moon.mod`、本地 Git `origin` | `repository` 指向 `https://github.com/Zcxssxx/MoonMQ`；无需读取 GitHub 凭据 |
| 开发过程和能力边界可追踪 | `docs/updates.md`、`docs/protocol-profile.md`、本地提交记录 | 更新记录填写实际验证结果；不把尚未远端发布的本地更改描述为已合并 |

官方页面没有源码行数下限。本项目自查以实现行为、测试和可复现运行证据为准，不通过拆文件或复制代码虚增体量。

## 本次验收的功能边界

- `exchange.delete` 可以删除命名 exchange 和对应 bindings；`if-unused` 在仍有绑定时拒绝；默认 exchange 不可删除。
- `queue.unbind` 精确删除 queue、exchange、routing-key binding；当前只接受空 arguments table。
- `queue.delete` 支持 idle queue，遵守 `if-unused`、`if-empty` 并返回 ready-message 数量。若队列有 active consumer 或 unsettled delivery，则明确拒绝且不修改状态，即使没有设置 `if-unused` 也一样。
- `basic.ack(multiple=true)` 仅确认当前 channel 上的投递；tag 0 表示当前 channel 的全部未确认投递。Broker 先校验完整 tag 集合，再一起修改状态。
- 这些行为构成受限 AMQP 0-9-1 Core Profile，不代表完整 RabbitMQ 兼容。

## 本地复核命令

在仓库根目录运行：

```text
moon version --all
moon check --deny-warn --target all
moon test --deny-warn --target all
moon fmt --check
moon build --target native cmd/moonmq-server
moon run cmd/moonmq
moon run examples/embedded
git diff --check
```

在 `docs/updates.md` 中只记录本次实际执行且退出码为 0 的命令结果。不同 MoonBit toolchain 版本可能改变后端测试数量；应记录版本和当前实测数字，不沿用旧记录。

## 需要在赛事/公开仓库页面完成的检查

本地文件无法证明线上提交表单是否完整，或参赛群/领奖流程是否完成。代码验收以[公开仓库](https://github.com/Zcxssxx/MoonMQ)和[现有 PR #13](https://github.com/Zcxssxx/MoonMQ/pull/13)页面为准；PR 合并前，验收扩展尚未进入默认分支。

预先存在的 `docs/project-proposal.md` 草案提及公开仓库与连续 Issue/PR 历史；本次自查没有核验这些线上状态，应在提交前按实际情况确认。
