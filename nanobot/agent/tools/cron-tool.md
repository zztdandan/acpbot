# Cron Tool Guide (ACP Runtime)

In ACP mode, cron scheduling is managed by the ACP backend implementation.
Nanobot no longer provides `nanobot cron ...` CLI subcommands.

## Runtime file location

- Runtime data follows instance config root (`paths.root` / config directory).
- Cron store file path is:

`<config-root>/cron/jobs.json`

## Recommended ACP behavior

1. Read and write cron jobs from `<config-root>/cron/jobs.json`.
2. Keep the same `CronJob`/`CronSchedule` JSON shape used by native runtime.
3. Ensure cron edits are atomic (write temp file then replace).
4. Validate schedule fields before persisting.
5. Respect payload `sessionMode`: `continue` reuses `cron:{job.id}`, `new_each_run` starts a fresh session key each run.

## Operational notes

- Use `--dispatcher acp` to run ACP runtime.
- Optionally use `--acp-config` to override `dispatch.acp` at startup.
