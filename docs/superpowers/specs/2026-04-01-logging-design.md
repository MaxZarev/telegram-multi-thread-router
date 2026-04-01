# Logging System Design

## Goal

Add structured logging to diagnose two classes of problems:
1. **Session hangs** — turns that run too long, stuck RUNNING state
2. **Errors and crashes** — unexpected exceptions, lost messages, dead processes

## Architecture

### New module: `src/logging_config.py`

Single entry point for logging configuration. Called once from `__main__.py`.

**Components:**

1. **`JsonFormatter`** — custom `logging.Formatter` producing JSON lines:
   - Standard fields: `ts`, `level`, `logger`, `msg`
   - Context fields (optional, from `extra=`): `thread_id`, `session_id`, `duration_ms`

2. **`setup_logging(settings)`** — configures root logger:
   - `StreamHandler` → stderr, human-readable text format (same as current)
   - `RotatingFileHandler` → `{log_dir}/bot.log`, JSON format, 50MB max, 3 backups
   - Root logger level: INFO
   - External loggers (`aiogram`, `aiohttp`, `claude_agent_sdk`) → WARNING

3. **`timed(logger, msg, **ctx)`** — async context manager for measuring operations:
   ```python
   async with timed(logger, "Turn completed", thread_id=123):
       await self._drain_response(client)
   ```
   - Logs `duration_ms` automatically on exit
   - WARNING if `duration_ms > turn_warn_seconds * 1000`
   - ERROR if `duration_ms > turn_error_seconds * 1000`

### Output formats

**stdout (human-readable):**
```
2026-04-01 12:00:05 [INFO] sessions.runner: Turn completed thread=141680 duration=12.3s tokens=4521
```

**File (JSON lines):**
```json
{"ts": "2026-04-01T12:00:05Z", "level": "INFO", "logger": "sessions.runner", "msg": "Turn completed", "thread_id": 141680, "duration_ms": 12300, "tokens": 4521}
```

## Log points

### SessionRunner (`runner.py`) — primary focus

#### Turn lifecycle

| Event | Level | Fields | Condition |
|---|---|---|---|
| Turn started | INFO | `thread_id`, `session_id`, query preview (100 chars) | Every turn |
| Turn completed | INFO/WARN/ERROR | `thread_id`, `session_id`, `duration_ms`, `tool_count` | Always; level by duration thresholds |
| Message enqueued | INFO/WARN | `thread_id`, `queue_size` | Always; WARN if queue_size > 5 |
| Session started | INFO | `thread_id`, `session_id`, `workdir`, `provider` | Session creation |
| Session stopped | INFO | `thread_id`, `session_id` | Session shutdown |
| Session error | ERROR | `thread_id`, `session_id`, traceback | Unhandled exception |

#### Tool calls — every tool inside a turn

Log **every** tool call, not just permission requests. This is the key signal for hang diagnostics — shows exactly which tool the session is stuck on.

| Event | Level | Fields | Condition |
|---|---|---|---|
| Tool use | INFO | `thread_id`, `tool_name`, `tool_index` (ordinal in turn) | Every ToolUseBlock in AssistantMessage |
| Permission requested | INFO | `thread_id`, `tool_name` | Every permission prompt |
| Permission resolved | INFO | `thread_id`, `tool_name`, result (allow/deny/always), `duration_ms` | Every permission response |
| Permission timeout | ERROR | `thread_id`, `tool_name`, `duration_ms` | Timeout hit |

#### SDK events — message stream inside a turn

Log **type of every SDK message** with timestamp in `_drain_response`. Distinguishes "API not responding" from "Claude looping in tool calls".

| Event | Level | Fields | Condition |
|---|---|---|---|
| SDK message received | DEBUG | `thread_id`, `msg_type`, `msg_index` (ordinal) | Every SDK message |
| SDK silence | WARNING | `thread_id`, `last_msg_type`, `silence_duration_ms` | Soft watchdog (3 min no messages) |
| SDK hard silence | ERROR | `thread_id`, `last_msg_type`, `silence_duration_ms` | Hard watchdog (10 min no messages) |
| Rate limit hit | WARNING | `thread_id`, `resets_at` | RateLimitEvent status=rejected |
| Sub-agent started | INFO | `thread_id`, description | TaskStartedMessage |
| Sub-agent completed | INFO | `thread_id`, status, summary (100 chars) | TaskNotificationMessage |

#### Additional field `_last_tool_name`

`SessionRunner` gets field `_last_tool_name: str | None` — updated on each ToolUseBlock. Health check uses it in hang logs to indicate the last known tool.

### SessionManager (`manager.py`)

| Event | Level | Fields |
|---|---|---|
| Session create | INFO | `thread_id`, `workdir`, `provider`, `server` |
| Session resume started | INFO | count of sessions to resume |
| Session resume completed | INFO | `duration_ms`, resumed count |

### Dispatcher (`dispatcher.py`)

| Event | Level | Fields |
|---|---|---|
| Bot startup completed | INFO | `duration_ms` |
| Bot shutdown completed | INFO | `duration_ms`, stopped session count |

### Health check (`health.py`)

| Event | Level | Fields | Condition |
|---|---|---|---|
| Session stuck in RUNNING | WARNING | `thread_id`, `session_id`, `stuck_duration_ms`, `last_tool`, `sdk_msg_count` | `_turn_started_at` > 5 min ago |
| Session likely hung | ERROR | `thread_id`, `session_id`, `stuck_duration_ms`, `last_tool`, `sdk_msg_count` | `_turn_started_at` > 15 min ago |
| Health check summary | DEBUG | active session count, states | Every check cycle |
| Zombie detected | WARNING | (existing) | task.done() but not STOPPED |

### Voice (`voice.py`)

| Event | Level | Fields |
|---|---|---|
| Transcription completed | INFO | `duration_ms`, `file_size` |

## Hang detection

### Mechanism

- `SessionRunner` gets a new field: `_turn_started_at: float | None`
- Set to `time.monotonic()` when entering a turn
- Reset to `None` when turn completes (or session stops)
- Health check (every 60s) reads this field and logs warnings/errors based on thresholds

### Thresholds (configurable via Settings)

- `turn_warn_seconds = 300` (5 min) → WARNING
- `turn_error_seconds = 900` (15 min) → ERROR

### No automatic kill

Hung sessions are only logged, not killed. Rationale:
- Claude may legitimately run long (large refactors, many tool calls)
- Auto-kill risks losing progress
- Goal is observability, not automatic recovery

## Configuration

### New fields in `Settings` (`config.py`)

```python
log_dir: str = "logs"
log_max_bytes: int = 50_000_000      # 50 MB
log_backup_count: int = 3
log_level: str = "INFO"
turn_warn_seconds: int = 300          # 5 min
turn_error_seconds: int = 900         # 15 min
```

All optional with sensible defaults — works out of the box.

### New entries in `.env.example`

```
# Logging
# LOG_DIR=logs
# LOG_LEVEL=INFO
# TURN_WARN_SECONDS=300
# TURN_ERROR_SECONDS=900
```

### `.gitignore`

Add `logs/` directory.

## What we do NOT log

- User message content (privacy)
- Claude response content (too verbose)
- Tool call input data (may contain large code blocks) — only tool name is logged

## What we do NOT change

- Existing `logger.info/warning/error` calls continue working — they now also go to JSON file
- No new dependencies — stdlib `logging` only
- No changes to existing behavior or error handling logic

## Files to modify

1. **New:** `src/logging_config.py` (~150 lines)
2. **Modify:** `src/__main__.py` — call `setup_logging()`
3. **Modify:** `src/sessions/runner.py` — add `_turn_started_at`, `timed()` wrappers, queue depth logging
4. **Modify:** `src/sessions/manager.py` — timing for resume_all, session create logging
5. **Modify:** `src/bot/dispatcher.py` — timing for startup/shutdown
6. **Modify:** `src/sessions/health.py` — stuck session detection
7. **Modify:** `src/sessions/voice.py` — transcription timing
8. **Modify:** `src/config.py` — new Settings fields
9. **Modify:** `.env.example` — new entries
10. **Modify:** `.gitignore` — add `logs/`
