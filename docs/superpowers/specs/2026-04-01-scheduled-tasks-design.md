# Scheduled Tasks Design

## Overview

Cron-like scheduled tasks for the Telegram multi-thread router bot. Users manage schedules via natural language through the orchestrator session, which calls MCP tools under the hood. Tasks execute in provider sessions (existing or fresh) with results visible in Telegram threads.

## Requirements

- **Two target modes**: enqueue into an existing session (with context) OR run in a fresh session (clean context, pinned thread per cron task)
- **Management**: via orchestrator natural language → MCP tools (create, list, update, delete, pause, resume)
- **Results**: appear in the target session's Telegram thread
- **Persistence**: SQLite, restored on bot restart
- **Full CRUD + pause/resume**
- **Busy session handling**: if target session is RUNNING, skip and retry next tick (60s)

## Architecture

### New files

| File | Purpose |
|------|---------|
| `src/sessions/scheduler.py` | `SchedulerService` — tick loop, CRUD, execution logic |
| Additions to `src/db/schema.py` | `scheduled_tasks` table + migration |
| Additions to `src/db/queries.py` | SQL query functions for scheduled tasks |
| Additions to `src/sessions/orchestrator.py` | 6 new MCP tools for schedule management |
| Additions to `src/bot/dispatcher.py` | Start/stop SchedulerService in lifecycle |

### Dependencies

- `croniter` — lightweight cron expression parser (~1 file). No heavy frameworks.

## Database Schema

```sql
CREATE TABLE IF NOT EXISTS scheduled_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cron_expr TEXT NOT NULL,
    prompt TEXT NOT NULL,
    
    -- Target mode: existing session
    target_thread_id INTEGER NULL,
    
    -- Target mode: fresh session (clean context, pinned thread)
    new_session_workdir TEXT NULL,
    new_session_server TEXT NOT NULL DEFAULT 'local',
    new_session_provider TEXT NULL,
    
    -- Pinned thread for fresh mode (auto-created on first run)
    pinned_thread_id INTEGER NULL,
    
    enabled INTEGER NOT NULL DEFAULT 1,
    last_run_at TEXT NULL,
    next_run_at TEXT NULL,
    run_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

### Target mode logic

- `target_thread_id IS NOT NULL` → **existing mode**: enqueue into that session with full context
- `target_thread_id IS NULL AND new_session_workdir IS NOT NULL` → **fresh mode**: use pinned thread, clear session_id before each run

### Validation constraint

Exactly one of `target_thread_id` or `new_session_workdir` must be set.

## SchedulerService

```python
class SchedulerService:
    def __init__(self, session_manager, bot, chat_id, permission_manager, question_manager):
        self._tasks: dict[int, ScheduledTask]  # id → dataclass
        self._tick_task: asyncio.Task | None
    
    async def start(self):
        """Load tasks from DB, compute next_run_at, start _tick_loop."""
    
    async def stop(self):
        """Cancel tick task."""
    
    async def _tick_loop(self):
        """Every 60 seconds, call _tick()."""
    
    async def _tick(self):
        """
        SELECT WHERE enabled=1 AND next_run_at <= datetime('now')
        For each due task:
          - existing mode: get runner, skip if RUNNING, enqueue if IDLE
          - fresh mode: ensure pinned thread exists, stop+clear session, create new session, enqueue
        Update last_run_at, run_count, recompute next_run_at via croniter.
        Send "clock Run #{n}" header message before enqueue.
        """
    
    async def _execute_existing(self, task) -> bool:
        """Enqueue into existing session. Return False if RUNNING (skip)."""
    
    async def _execute_fresh(self, task):
        """
        1. pinned_thread_id exists? Use it. Else create topic "clock {name}", save to DB.
        2. Stop current session in that thread (if any).
        3. Clear session_id in DB (fresh context).
        4. Create new session in pinned thread.
        5. Enqueue prompt.
        """
    
    # CRUD — called from MCP tools
    async def create(self, **kwargs) -> int
    async def update(self, task_id, **kwargs)
    async def delete(self, task_id)
    async def set_enabled(self, task_id, enabled: bool)
    async def list_all(self) -> list[ScheduledTask]
```

### ScheduledTask dataclass

```python
@dataclass
class ScheduledTask:
    id: int
    name: str
    cron_expr: str
    prompt: str
    target_thread_id: int | None
    new_session_workdir: str | None
    new_session_server: str
    new_session_provider: str | None
    pinned_thread_id: int | None
    enabled: bool
    last_run_at: datetime | None
    next_run_at: datetime | None
    run_count: int
```

## MCP Tools (Orchestrator)

Six new tools registered in the orchestrator's MCP server:

### create_schedule
```
Args: name, cron_expr, prompt, target_thread_id? | workdir+server?+provider?
Returns: task ID, next_run_at
```

### list_schedules
```
Args: none
Returns: all tasks with id, name, cron, enabled, next_run, last_run, target info
```

### update_schedule
```
Args: task_id, name?, cron_expr?, prompt?, target_thread_id?, workdir?, server?, provider?
Returns: updated task
```

### delete_schedule
```
Args: task_id
Returns: confirmation
```

### pause_schedule
```
Args: task_id
Returns: confirmation
```

### resume_schedule
```
Args: task_id
Returns: confirmation, next_run_at
```

## Lifecycle Integration

### Startup (dispatcher.py on_startup)
After `resume_all()` and before orchestrator setup:
```python
scheduler = SchedulerService(manager, bot, chat_id, permission_manager, question_manager)
await scheduler.start()
dispatcher["scheduler"] = scheduler
```

### Shutdown (dispatcher.py on_shutdown)
Before stopping sessions:
```python
scheduler = dispatcher.get("scheduler")
if scheduler:
    await scheduler.stop()
```

### Orchestrator setup
Pass `scheduler` reference to orchestrator MCP tool registration so tools can call `scheduler.create()`, etc.

## Fresh Mode Thread Behavior

Each fresh-mode scheduled task gets ONE pinned Telegram thread. All runs appear sequentially:

```
Topic: "clock daily log check"
  ├── "clock Run #1 — 2026-04-01 09:00"
  │   └── [Claude response]
  ├── "clock Run #2 — 2026-04-02 09:00"
  │   └── [Claude response]
  └── ...
```

Session is stopped + session_id cleared before each run = clean Claude context every time, but message history is visible in the Telegram thread for the user.

## Error Handling

- **croniter parse error on create**: reject with error message
- **Target session not found** (existing mode): log warning, skip this tick
- **Session creation failure** (fresh mode): log error, skip, retry next tick
- **Tick exception**: catch per-task, log, continue to next task. Never crash the tick loop.

## Testing

- Unit tests for SchedulerService CRUD (mock DB)
- Unit test for tick logic with mock session_manager
- Integration test: create schedule, advance time, verify enqueue called
- Validate cron_expr parsing edge cases
