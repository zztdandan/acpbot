# ACP SessionMap Real Test Fixtures

- `session_map.real_fixture.json` 是本目录实际落地的 sessionmap 文件，不是运行时临时拼出来的字符串。
- `cwd` 固定为 `/home/base/repo/harness/nanobot-refactor`，大对账和 ensure 都基于这个 cwd。
- 第二个测试唯一明确会 `ensure` 的映射是：`nanobot-sessionmap-target -> ses_279d9764affemAyT25zOD33zuU`。
- 该映射要求恢复后立即刷回：`boundAgent=build`、`boundModel=RCode_OpenAI/gpt-5.4`。
- `nanobot-sessionmap-stale -> ses_sessionmap_stale_for_reconcile_check` 是故意放进去的大对账脏数据，用来验证 `_reconcile_entries()` 会把不存在的 session 清掉。
