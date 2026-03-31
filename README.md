# Telegram Multi-Thread Router

> Run provider-backed coding sessions in Telegram. Claude Code is the default path; Codex is available as an additional provider.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![aiogram 3](https://img.shields.io/badge/aiogram-3.x-blue.svg)](https://docs.aiogram.dev/)
[![Claude Agent SDK](https://img.shields.io/badge/Claude-Agent%20SDK-orange.svg)](https://docs.anthropic.com/en/docs/claude-code/sdk)

---

## What is this?

A Telegram bot that runs **multiple coding sessions in parallel**. Each Telegram thread is an isolated workspace backed by a provider session.

Current provider model:

- **Claude Code** is the default provider and the default orchestrator path
- **Codex** is supported as an additional provider and can be enabled per deployment

Think of it as a Telegram-native session router: thread-per-workspace, permission buttons, voice/photo/file ingestion, multi-server workers, and a built-in orchestrator thread that can create and manage other sessions.

### Key Features

- **1 thread = 1 session** — isolated Claude or Codex sessions per Telegram thread
- **Provider-aware orchestration** — orchestrator creates sessions on the default provider unless you ask for another one
- **Claude default, Codex optional** — `DEFAULT_PROVIDER=claude|codex`, `ENABLE_CODEX=true|false`
- **Auto-mode** — per-session toggle to auto-approve all permissions (no buttons needed)
- **Permission system** — read-only tools auto-approved, write/exec tools confirmed via inline buttons
- **Provider fallback for orchestrator** — if the active orchestrator provider is exhausted, it can fall back to the other enabled provider
- **Intermediate output mode** — configure whether assistant text streams mid-turn or is shown mostly at completion
- **Voice messages** — transcribed via Whisper and forwarded to the active provider
- **Photo support** — local sessions send images natively; remote workers receive photo bytes over IPC
- **File support** — documents are downloaded to the workdir; worker-produced files can be sent back to Telegram
- **Multi-server** — run sessions on different machines via TCP IPC workers
- **Session persistence** — sessions survive bot restarts via SQLite + session resume
- **Real-time status** — editable status message shows what the session is doing
- **Telegram MCP tools** — sessions can reply, send files, react with emoji, and edit messages directly in Telegram
- **Codex app-server integration** — Codex sessions run through `codex app-server`, including approvals, questions, Telegram output tools, and remote worker support

## Architecture

```
Telegram Bot (threads)
  ├── 🎯 Orchestrator   → Default provider session that creates/manages others
  ├── 📁 my-project     → Claude or Codex session (local)
  ├── 🔧 api-server     → Claude or Codex session (remote worker)
  └── ...
```

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│  Telegram   │────▶│  Bot (aiogram 3) │────▶│  Session backends         │
│  threads    │◀────│  + Dispatcher    │◀────│  Claude SDK / Codex app   │
└─────────────┘     └──────────────────┘     └──────────────────────────┘
                          │                            │
                   ┌──────┴──────┐              ┌──────┴──────┐
                   │  SQLite DB  │              │  TCP IPC     │
                   │  sessions,  │              │  remote      │
                   │  topics     │              │  workers     │
                   └─────────────┘              └─────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed and authenticated
- Optional: [Codex CLI](https://developers.openai.com/codex/cli) installed and authenticated if you want Codex sessions
- A Telegram bot token (from [@BotFather](https://t.me/BotFather))

Verify local prerequisites before you start:

```bash
python3.11 --version
claude --version
codex --version   # optional
```

If `python` on your machine is not Python 3.11+, use `python3.11` explicitly in the setup commands below.

### Bot Setup in BotFather

1. Create a bot via [@BotFather](https://t.me/BotFather) and copy the token
2. Go to **Bot Settings → Threaded Mode → Enable**
3. **Send `/start` to your bot** before launching the script — the bot needs an active chat with you to create threads

### Get Your Telegram User ID

1. Open [@Get_myidrobot](https://t.me/Get_myidrobot)
2. Send `/start`
3. Copy your numeric Telegram user ID
4. Put it into `.env` as `OWNER_USER_ID`

### Installation

```bash
git clone https://github.com/knyazev741/telegram-multi-thread-router.git
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

| Variable | Description |
|----------|-------------|
| `BOT_TOKEN` | Telegram bot token from @BotFather |
| `OWNER_USER_ID` | Your Telegram user ID from [@Get_myidrobot](https://t.me/Get_myidrobot) |
| `AUTH_TOKEN` | Shared secret for bot ↔ worker IPC authentication. If you run only one local bot process, this can be any random string. |
| `ENABLE_CODEX` | `true` to allow Codex sessions in addition to Claude |
| `DEFAULT_PROVIDER` | `claude` or `codex` for new sessions and orchestrator default |
| `STREAM_INTERMEDIATE_MESSAGES` | `true` to stream assistant text during a turn, `false` to keep Telegram quieter and send mostly final text |
| `CHAT_ID` | Optional fixed target chat/forum; if omitted, auto-detected on first owner message |
| `IPC_PORT` | Optional bot IPC port for remote workers; defaults to `9800` |

Example local-only `.env`:

```env
BOT_TOKEN=123456789:AA...
OWNER_USER_ID=123456789
AUTH_TOKEN=local-dev-secret-change-me
ENABLE_CODEX=true
DEFAULT_PROVIDER=claude
STREAM_INTERMEDIATE_MESSAGES=true
```

To generate a random `AUTH_TOKEN`:

```bash
python3.11 - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
```

### Run

```bash
python -m src
```

The expected first-run flow is:

1. Start the bot process
2. Open your bot in Telegram
3. Send `/start`
4. Send any normal message
5. The bot auto-detects the chat and creates an **🎯 Orchestrator** thread on the default provider

The Orchestrator thread is the main interface for managing sessions.

## For Agents

If you are an AI agent or automation assistant helping a user install this project, do as much of the setup as possible yourself and ask the user only for the Telegram-side actions that cannot be automated from the local machine.

Recommended flow:

1. Clone the repository into a clean directory.
2. Detect a usable Python 3.11+ interpreter. Do not assume `python` points to 3.11.
3. Create `.venv`, install dependencies, and verify `claude --version`.
4. If Codex support is desired, also verify `codex --version`.
5. Create `.env` yourself once the user provides:
   - `BOT_TOKEN`
   - `OWNER_USER_ID`
   - optional: choose or generate `AUTH_TOKEN` for them
   - optional: set `ENABLE_CODEX=true` and `DEFAULT_PROVIDER=claude|codex`
6. Explicitly tell the user to do these Telegram-side steps:
   - create the bot in `@BotFather`
   - enable **Threaded Mode**
   - send `/start` to the bot
   - get their Telegram numeric user ID from `@Get_myidrobot`
7. Start the bot yourself and watch logs for:
   - bot startup
   - chat auto-detection
   - orchestrator topic creation
   - permission prompts
8. After startup, ask the user to send one simple message and verify the Orchestrator appears.

What an agent should explain clearly:

- `BOT_TOKEN` is the bot token from `@BotFather`
- `OWNER_USER_ID` is the user's Telegram numeric ID, not chat title or username
- `AUTH_TOKEN` is just a shared secret for IPC; for local-only usage it can be any random string
- if setup commands fail, confirm Python version first
- if the bot starts but Telegram says unauthorized, the bot token is wrong
- if the bot starts and waits for chat detection, the user still needs to send `/start` and one normal message

## Usage

### Orchestrator (main interface)

The Orchestrator is a provider session with extra management tools. By default it starts on `DEFAULT_PROVIDER`, which is usually `claude`.

Talk to it in natural language:

- *"Create a session for my-project in /home/user/my-project"*
- *"Create a Codex session for my-project in /home/user/my-project"*
- *"Create a session on worker personal in /root/agent"*
- *"List all sessions"*
- *"Stop session 12345"*
- *"Enable auto-mode for session 12345"* — auto-approves all permissions

The Orchestrator is also a full coding session itself — it can inspect repositories, run commands, and manage worker-backed sessions.

### Commands

All commands work from **any thread** (including Orchestrator):

| Command | Description |
|---------|-------------|
| `/new <name> <workdir> [server] [provider]` | Create a new session in a new thread |
| `/list` | List all active sessions |
| `/restart` | Graceful restart (preserves sessions) |
| `/stop` | Interrupt current turn (like Escape in CLI) |
| `/close` | Stop session + delete thread |
| Any other `/command` | Forwarded to the active provider (`/model`, `/clear`, `/compact`, `/reset`, etc.) |

### Input Types

| Input | Behavior |
|-------|----------|
| 💬 Text | Forwarded to the active provider |
| 🎤 Voice | Transcribed via Whisper, then forwarded |
| 📷 Photo | Local sessions send images natively; remote sessions receive image bytes over IPC |
| 📎 Document | Downloaded to workdir; remote workers receive file bytes over IPC |

### Intermediate Message Streaming

The bot supports two output styles for assistant text:

- `STREAM_INTERMEDIATE_MESSAGES=true`
  - stream intermediate assistant text while a turn is still running
  - best when you want to watch Claude or Codex think in real time
- `STREAM_INTERMEDIATE_MESSAGES=false`
  - keep Telegram quieter
  - show status/progress updates during the turn, but send most assistant text only near the end

Notes:

- Claude already supports native partial output; this flag decides whether to surface that partial text in Telegram immediately
- Codex streams intermediate text in coalesced chunks when enabled, instead of waiting only for item completion
- regardless of this flag, status updates, permission prompts, rate-limit messages, and error messages still appear during the turn

### Permission System

When the active provider wants to use a write / exec / side-effecting tool, you get inline buttons:

```
🔧 Bash: rm -rf node_modules
[✅ Allow] [✅ Allow All] [❌ Deny]
```

Read-only tools are auto-approved. Tools that can change files, run commands, or perform external actions require explicit approval. Use **auto-mode** to skip prompts for a specific session.

Codex parity notes:

- Codex sessions support Telegram output tools (`reply`, `send_file`, `react`, `edit_message`)
- Codex sessions support interactive questions and approval prompts
- The orchestrator can fall back between Claude and Codex if the active provider is exhausted
- Claude remains the default and most conservative production path

## Multi-Server Support

Run provider sessions on different machines by connecting **workers** via TCP:

**On the worker machine:**
```bash
pip install -e ".[dev]"
python -m src.worker --host <bot-server-ip> --port 9800 --token $AUTH_TOKEN --worker-id my-worker
```

**Create a session on the worker** (via Orchestrator or `/new`):
```
/new my-project /path/to/project my-worker
/new my-project /path/to/project my-worker codex
```

The bot acts as a **hub**. It handles Telegram and dispatches to local or remote backends. Workers run provider sessions locally on their own machines and exchange messages, files, approvals, and Telegram output over msgspec-based TCP IPC.

## Project Structure

```
src/
  __main__.py              Entry point (asyncio.Runner + uvloop)
  config.py                pydantic-settings configuration
  bot/
    dispatcher.py          Dispatcher factory, startup/shutdown lifecycle
    middlewares.py          OwnerAuthMiddleware
    routers/
      general.py           General topic fallback (minimal)
      session.py           All commands + message forwarding + permissions
    status.py              StatusUpdater (editable status message per turn)
    output.py              Message splitting, TypingIndicator
  sessions/
    runner.py              SessionRunner (ClaudeSDKClient wrapper)
    manager.py             SessionManager (thread_id → runner mapping)
    permissions.py         PermissionManager (Future → inline buttons)
    orchestrator.py        Orchestrator session with MCP management tools
    mcp_tools.py           Telegram MCP tools (reply, send_file, react)
    voice.py               faster-whisper transcription
    health.py              Zombie session detection
    state.py               SessionState enum
    remote.py              RemoteSession proxy for TCP workers
  db/
    schema.py              SQLite schema + migrations
    connection.py          aiosqlite connection helper
    queries.py             Named SQL query functions
  ipc/
    protocol.py            msgspec message types for TCP
    server.py              Bot-side TCP server
  worker/
    __main__.py            Worker entry point
    client.py              TCP client with reconnection
    output_channel.py      Bot adapter for worker side
```

## Tech Stack

- **[aiogram 3](https://docs.aiogram.dev/)** — async Telegram bot framework
- **[Claude Agent SDK](https://docs.anthropic.com/en/docs/claude-code/sdk)** — programmatic Claude Code sessions
- **[Codex CLI / app-server](https://developers.openai.com/codex/cli)** — Codex-backed sessions
- **[aiosqlite](https://github.com/omnilib/aiosqlite)** — async SQLite with WAL mode
- **[uvloop](https://github.com/MagicStack/uvloop)** — high-performance event loop
- **[pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)** — configuration management
- **[faster-whisper](https://github.com/SYSTRAN/faster-whisper)** — voice transcription
- **[msgspec](https://github.com/jcrist/msgspec)** — fast binary serialization for IPC
- **[MCP](https://modelcontextprotocol.io/)** — Telegram output tools and orchestrator tool surface

## Testing

```bash
pytest
```

The suite covers database operations, IPC protocol, middleware, permissions, routing, orchestrator behavior, Codex app-server transport, and remote worker flows.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

[MIT](LICENSE)

## Author

**Knyazev AI** — [@knyazev741](https://github.com/knyazev741)

- Telegram: [@manintg_blog](https://t.me/manintg_blog)

## Support

If you find this project useful, consider supporting its development:

- **BTC:** `bc1qekweh0kxrgzxftefnlyuavqqrfgza60s0qq95g`
- **EVM (ETH/USDT/USDC):** `0x23a7A8eC8f9b4386a6714e5B5A0d8340f0AE1749`
- **SOL:** `5dcuDRDGCgwBXN72uUeaN5ahGvWzFY1hpv2kF7jUmf7R`
- **TON:** `UQB2fqymhGrMsA7MgRfIpd2qgc4_gXaCvtZ32l55tuirrukZ`

---

*Built with Claude Code, Codex, aiogram, and a lot of Telegram thread abuse.*
