# Telegram Multi-Thread Router

## Architecture

Python asyncio bot using aiogram 3 + provider backends. Each Telegram bot thread = one provider session.

- **Bot**: aiogram 3 Dispatcher with Router-per-concern pattern
- **Sessions**: local Claude/Codex runners per thread, managed by SessionManager
- **Orchestrator**: Auto-created provider session with MCP tools for managing other sessions
- **Permissions**: can_use_tool callback → asyncio.Future → Telegram inline buttons
- **DB**: aiosqlite with WAL mode for session/topic persistence
- **Config**: pydantic-settings loading from .env

## Quick Start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env  # fill in values
python -m src
```

## Restart (pick up new code)

Two ways to restart:
1. **From any existing bot topic**: `/restart`
2. **Manual**: `kill $(pgrep -f 'python -m src') && python -m src`

On restart, all sessions resume automatically via `resume_all()` — sessions stay as `idle` in DB during graceful shutdown and are re-created with their `session_id` on next startup.

**CRITICAL — DO NOT restart the bot from sessions:**
- **NEVER** run `os.execv`, `kill`, `pkill`, or any command that restarts/kills the bot process from within a provider session running inside this bot.
- You ARE running inside this bot. Restarting it kills YOUR OWN process and causes infinite restart loops.
- If code changes need a restart, tell the user to run `/restart` from any existing bot topic manually.
- This applies to ALL sessions, including the orchestrator.

**IMPORTANT**: Never `kill -9` the bot — use SIGTERM so on_shutdown runs and preserves session state.

## Project Structure

```
src/
  __main__.py          - Entry point (asyncio.Runner + uvloop)
  config.py            - pydantic-settings BaseSettings
  bot/
    dispatcher.py      - Dispatcher factory, startup/shutdown lifecycle
    middlewares.py      - OwnerAuthMiddleware
    routers/
      general.py       - General topic fallback (minimal, rarely fires)
      session.py       - All commands (/new, /list, /restart, /stop, /close) + message forwarding
    status.py          - StatusUpdater (editable status message per turn)
    output.py          - split_message, TypingIndicator
  sessions/
    runner.py          - SessionRunner (ClaudeSDKClient wrapper, state machine)
    manager.py         - SessionManager (thread_id → runner mapping)
    permissions.py     - PermissionManager (asyncio.Future bridge to Telegram buttons)
    orchestrator.py    - Auto-created orchestrator session with management MCP tools
    mcp_tools.py       - Telegram output MCP tools (reply, send_file, react, edit_message)
    voice.py           - faster-whisper transcription
    health.py          - Zombie session detection
    scheduler.py       - SchedulerService (cron tick loop, task CRUD)
    state.py           - SessionState enum
    remote.py          - RemoteSession proxy for TCP workers
  db/
    schema.py          - SQLite schema + migrations
    connection.py      - aiosqlite connection helper
    queries.py         - Named SQL query functions
  ipc/
    protocol.py        - msgspec Struct message types for TCP
    server.py          - Bot-side TCP server for remote workers
  worker/
    __main__.py        - Worker entry point (python -m src.worker)
    client.py          - TCP client with reconnection
    output_channel.py  - Bot adapter for worker side
```

## Telegram Bot Commands

**Orchestrator thread (🎯 main interface):**
- Natural language → create/list/stop sessions, toggle auto-mode
- Also a full provider session (SSH, filesystem, commands)
- MCP tools: `create_session`, `list_sessions`, `stop_session`, `auto_mode`, `create_schedule`, `list_schedules`, `update_schedule`, `delete_schedule`, `pause_schedule`, `resume_schedule`

**All commands work from any thread:**
- `/new <name> <workdir> [server] [provider]` — create session in new thread
- `/list` — list active sessions
- `/restart` — restart bot, resume all sessions
- `/stop` — interrupt current turn (like Escape in CLI), session stays alive
- `/resume` — restart stopped session with preserved context
- `/clear` — restart session with fresh context (no conversation history)
- `/close` — kill session + delete thread
- Any other `/command` → forwarded to the active provider (`/model`, `/compact`, etc.)
- Text → forwarded to the active provider
- Voice → transcribed → forwarded
- Photo/Document → downloaded to workdir → path sent to the active provider

**General topic**: ignored (Telegram auto-creates new topics there)

## Scheduled Tasks

Cron-based task scheduler managed via orchestrator natural language.

**Two modes:**
- **Existing session**: `target_thread_id` — enqueue prompt with full context. Skips if session is RUNNING.
- **Fresh session**: `workdir` — pinned thread per task, session_id cleared each run (clean context).

**Orchestrator MCP tools:** `create_schedule`, `list_schedules`, `update_schedule`, `delete_schedule`, `pause_schedule`, `resume_schedule`

**Example:** "every day at 9am check logs in /home/deploy/myapp" → orchestrator calls `create_schedule`

**Persistence:** SQLite `scheduled_tasks` table, restored on restart via `SchedulerService.start()`.

## Security
- All secrets in `.env` (gitignored), chmod 600
- No credentials in source code
- OWNER_USER_ID enforced via outer middleware on all messages
- AUTH_TOKEN for TCP worker authentication

## Multi-server Architecture (UNUSED — cleanup planned)

Remote worker infrastructure exists in the code but is NOT active. Orchestrator prompt does not mention it.
When cleaning up, remove in this order:

1. **Whole files to delete:** `src/ipc/`, `src/worker/`, `src/sessions/remote.py`, `tests/test_ipc.py`, `tests/test_session_routing.py`
2. **manager.py:** remove `create_remote()`, `get_server()`, RemoteSession import
3. **orchestrator.py:** remove `server` param from create_session/create_schedule tools, `worker_registry` param
4. **orchestrator_mcp.py:** remove `server` param, `create_remote()` branch
5. **session.py (router):** remove `server` from `/new`, `isinstance(runner, RemoteSession)` branches in photo/document/list handlers
6. **dispatcher.py:** remove WorkerRegistry creation, IPC server startup
7. **middlewares.py:** remove `worker_registry` passthrough
8. **queries.py:** remove `get_worker_sessions()`
9. **DB schema:** keep `server` column as-is (default 'local', harmless), don't migrate
