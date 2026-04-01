# Telegram Multi-Thread Router

> Run AI-powered coding sessions in Telegram. Each forum thread is an isolated workspace backed by Claude Code (default) or Codex.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![aiogram 3](https://img.shields.io/badge/aiogram-3.x-blue.svg)](https://docs.aiogram.dev/)
[![Claude Agent SDK](https://img.shields.io/badge/Claude-Agent%20SDK-orange.svg)](https://docs.anthropic.com/en/docs/claude-code/sdk)

---

## What is this?

A Telegram bot that runs **multiple coding sessions in parallel**. Each Telegram forum thread is an isolated workspace backed by a provider session (Claude Code or Codex).

Key idea: **thread-per-workspace** — the bot creates a Telegram forum thread for each session and routes all messages, voice, photos, and files to the active provider.

### Key Features

- **1 thread = 1 session** — isolated Claude or Codex sessions per Telegram thread
- **Orchestrator** — a dedicated thread that can create/manage/stop other sessions via natural language
- **Scheduled tasks** — cron-based scheduler managed via orchestrator (e.g. "every day at 9am check logs")
- **Permission system** — write/exec tools confirmed via inline buttons, read-only auto-approved
- **Auto-mode** — per-session toggle to auto-approve all permissions
- **Voice messages** — transcribed via Whisper (local) or Deepgram nova-3 (cloud)
- **Photo & file support** — images sent natively, documents downloaded to workdir
- **Session persistence** — sessions survive bot restarts via SQLite + session resume
- **Real-time status** — editable status message shows current tool, context usage, and API rate limits
- **Telegram MCP tools** — sessions can reply, send files, react with emoji, and edit messages directly
- **Structured logging** — JSON file logs with turn timing and stuck-session detection

## Architecture

```
Telegram Bot (forum threads)
  ├── 🎯 Orchestrator   → Claude/Codex session that creates/manages others
  ├── 📁 my-project     → Claude or Codex session
  ├── 🔧 api-server     → Claude or Codex session
  └── ...
```

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│  Telegram   │────▶│  Bot (aiogram 3) │────▶│  Provider sessions       │
│  threads    │◀────│  + Dispatcher    │◀────│  Claude SDK / Codex app  │
└─────────────┘     └──────────────────┘     └──────────────────────────┘
                          │
                   ┌──────┴──────┐
                   │  SQLite DB  │
                   │  sessions,  │
                   │  topics,    │
                   │  schedules  │
                   └─────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated
- Optional: [Codex CLI](https://developers.openai.com/codex/cli) if you want Codex sessions
- A Telegram bot token (from [@BotFather](https://t.me/BotFather))

### Bot Setup in BotFather

1. Create a bot via [@BotFather](https://t.me/BotFather) and copy the token
2. Go to **Bot Settings → Threaded Mode → Enable**
3. **Send `/start` to your bot** before launching the script

### Get Your Telegram User ID

1. Open [@Get_myidrobot](https://t.me/Get_myidrobot)
2. Send `/start`
3. Copy your numeric Telegram user ID

### Installation

```bash
git clone https://github.com/MaxZarev/telegram-multi-thread-router.git
cd telegram-multi-thread-router

python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Configuration

```bash
cp .env.example .env
```

Edit `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `BOT_TOKEN` | Telegram bot token from @BotFather | required |
| `OWNER_USER_ID` | Your Telegram user ID | required |
| `AUTH_TOKEN` | Shared secret for IPC (any random string for local-only) | required |
| `ENABLE_CODEX` | `true` to allow Codex sessions | `false` |
| `DEFAULT_PROVIDER` | `claude` or `codex` for new sessions | `claude` |
| `STREAM_INTERMEDIATE_MESSAGES` | Stream assistant text mid-turn | `true` |
| `TRANSCRIBER` | `whisper` (local, CPU) or `deepgram` (cloud API) | `whisper` |
| `DEEPGRAM_API_KEY` | Deepgram API key (only if `TRANSCRIBER=deepgram`) | — |
| `CHAT_ID` | Fixed target chat; auto-detected if omitted | — |
| `LOG_DIR` | Directory for JSON log files | `logs` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `TURN_WARN_SECONDS` | Warn if a turn exceeds this (seconds) | `300` |
| `TURN_ERROR_SECONDS` | Error if a turn exceeds this (seconds) | `900` |

### Run

```bash
python -m src
```

First-run flow:

1. Start the bot process
2. Open your bot in Telegram
3. Send `/start`, then any message
4. The bot creates an **🎯 Orchestrator** thread automatically

## Usage

### Orchestrator (main interface)

Talk to it in natural language:

- *"Create a session for my-project in /home/user/my-project"*
- *"List all sessions"*
- *"Stop session 12345"*
- *"Enable auto-mode for session 12345"*
- *"Every day at 9am check logs in /home/deploy/myapp"*

The Orchestrator is also a full coding session — it can inspect repositories, run commands, etc.

### Commands

All commands work from **any thread**:

| Command | Description |
|---------|-------------|
| `/new <name> <workdir> [server] [provider]` | Create a new session in a new thread |
| `/list` | List all active sessions |
| `/restart` | Graceful restart (preserves sessions) |
| `/stop` | Interrupt current turn (like Escape in CLI) |
| `/resume` | Restart stopped session with preserved context |
| `/clear` | Reset session with fresh context (no history) |
| `/close` | Stop session + delete thread |
| Any other `/command` | Forwarded to provider (`/model`, `/compact`, etc.) |

### Input Types

| Input | Behavior |
|-------|----------|
| 💬 Text | Forwarded to the active provider |
| 🎤 Voice | Transcribed → shown as quote → forwarded |
| 📷 Photo | Sent natively to the provider |
| 📎 Document | Downloaded to workdir → path sent to provider |

### Scheduled Tasks

Cron-based scheduler managed via orchestrator natural language. Two modes:

- **Existing session** (`target_thread_id`) — enqueue prompt into an existing session. Skips if session is busy.
- **Fresh session** (`workdir`) — pinned thread per task, clean context each run.

Orchestrator MCP tools: `create_schedule`, `list_schedules`, `update_schedule`, `delete_schedule`, `pause_schedule`, `resume_schedule`.

Schedules persist in SQLite and restore on restart.

### Permission System

Write/exec tools prompt inline buttons:

```
🔧 Bash: rm -rf node_modules
[✅ Allow] [✅ Allow All] [❌ Deny]
```

Read-only tools are auto-approved. Use **auto-mode** to skip prompts for a session.

### Intermediate Message Streaming

- `STREAM_INTERMEDIATE_MESSAGES=true` — stream assistant text in real time
- `STREAM_INTERMEDIATE_MESSAGES=false` — quieter, send mostly final text

Status updates, permission prompts, and errors always appear regardless of this setting.

## For Agents

If you are an AI agent helping a user install this project:

1. Clone the repo, detect Python 3.11+, create `.venv`, install deps
2. Verify `claude --version` (and optionally `codex --version`)
3. Create `.env` with user-provided `BOT_TOKEN` and `OWNER_USER_ID`
4. Tell the user to: create bot in @BotFather, enable Threaded Mode, send `/start`, get their user ID from @Get_myidrobot
5. Start the bot and verify orchestrator creation

## Project Structure

```
src/
  __main__.py              Entry point (asyncio.Runner + uvloop)
  config.py                pydantic-settings configuration
  usage.py                 Anthropic OAuth usage fetcher (API rate limits)
  logging_config.py        Structured JSON logging, setup_logging, timed decorator
  bot/
    dispatcher.py          Dispatcher factory, startup/shutdown lifecycle
    middlewares.py         OwnerAuthMiddleware
    routers/
      general.py           General topic fallback (minimal)
      session.py           Commands + message forwarding + permissions
    status.py              StatusUpdater (editable status message per turn)
    output.py              Message splitting, HTML helpers, TypingIndicator
  sessions/
    runner.py              SessionRunner (Claude SDK client wrapper)
    manager.py             SessionManager (thread_id → runner mapping)
    permissions.py         PermissionManager (Future → inline buttons)
    questions.py           QuestionManager (AskUserQuestion → inline buttons)
    orchestrator.py        Orchestrator session with MCP management tools
    orchestrator_mcp.py    Orchestrator MCP tool definitions
    mcp_tools.py           Telegram MCP tools (reply, send_file, react)
    telegram_output_mcp.py Telegram output MCP server for sessions
    codex_runner.py        CodexRunner (Codex app-server wrapper)
    codex_app_server.py    Codex app-server transport
    voice.py               Voice transcription (Whisper / Deepgram)
    health.py              Zombie session detection
    scheduler.py           SchedulerService (cron tick loop, task CRUD)
    state.py               SessionState enum
    backend.py             Provider backend abstraction
  db/
    schema.py              SQLite schema + migrations
    connection.py          aiosqlite connection helper
    queries.py             Named SQL query functions
```

## Tech Stack

- **[aiogram 3](https://docs.aiogram.dev/)** — async Telegram bot framework
- **[Claude Agent SDK](https://docs.anthropic.com/en/docs/claude-code/sdk)** — programmatic Claude Code sessions
- **[Codex CLI](https://developers.openai.com/codex/cli)** — optional Codex-backed sessions
- **[aiosqlite](https://github.com/omnilib/aiosqlite)** — async SQLite with WAL mode
- **[uvloop](https://github.com/MagicStack/uvloop)** — high-performance event loop
- **[pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)** — configuration management
- **[faster-whisper](https://github.com/SYSTRAN/faster-whisper)** — local voice transcription
- **[Deepgram SDK](https://github.com/deepgram/deepgram-python-sdk)** — cloud voice transcription (nova-3)
- **[croniter](https://github.com/kiorky/croniter)** — cron expression parser for scheduled tasks
- **[MCP](https://modelcontextprotocol.io/)** — Telegram output tools and orchestrator tool surface

## Testing

```bash
pytest
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

[MIT](LICENSE)

## Authors

**Knyazev AI** — [@knyazev741](https://github.com/knyazev741)
- Telegram: [@manintg_blog](https://t.me/manintg_blog)

**Max Zarev** — [@MaxZarev](https://github.com/MaxZarev)
- Telegram: [@max_zarev](https://t.me/max_zarev)
- Channel: [@maxzarev](https://t.me/maxzarev)

## Support

If you find this project useful, consider supporting its development:

- **BTC:** `bc1qekweh0kxrgzxftefnlyuavqqrfgza60s0qq95g`
- **EVM (ETH/USDT/USDC):** `0x23a7A8eC8f9b4386a6714e5B5A0d8340f0AE1749`
- **SOL:** `5dcuDRDGCgwBXN72uUeaN5ahGvWzFY1hpv2kF7jUmf7R`
- **TON:** `UQB2fqymhGrMsA7MgRfIpd2qgc4_gXaCvtZ32l55tuirrukZ`

---

*Built with Claude Code, aiogram, and a lot of Telegram thread abuse.*
