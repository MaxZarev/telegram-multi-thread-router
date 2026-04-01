# Дизайн системы логирования

## Цель

Добавить структурированное логирование для диагностики двух классов проблем:
1. **Зависания сессий** — turn-ы которые выполняются слишком долго, застревание в состоянии RUNNING
2. **Ошибки и сбои** — необработанные исключения, потерянные сообщения, мёртвые процессы

## Архитектура

### Новый модуль: `src/logging_config.py`

Единая точка настройки логирования для всего проекта. Вызывается один раз из `__main__.py`.

**Компоненты:**

1. **`JsonFormatter`** — кастомный `logging.Formatter`, выдающий JSON-строки:
   - Стандартные поля: `ts`, `level`, `logger`, `msg`
   - Контекстные поля (опциональные, через `extra=`): `thread_id`, `session_id`, `duration_ms`

2. **`setup_logging(settings)`** — конфигурирует корневой логгер:
   - `StreamHandler` → stderr, текстовый формат (как сейчас)
   - `RotatingFileHandler` → `{log_dir}/bot.log`, JSON-формат, макс 50MB, 3 бэкапа
   - Уровень корневого логгера: INFO
   - Внешние логгеры (`aiogram`, `aiohttp`, `claude_agent_sdk`) → приглушены до WARNING

3. **`timed(logger, msg, **ctx)`** — async context manager для замера операций:
   ```python
   async with timed(logger, "Turn completed", thread_id=123):
       await self._drain_response(client)
   ```
   - Автоматически логирует `duration_ms` при выходе
   - WARNING если `duration_ms > turn_warn_seconds * 1000`
   - ERROR если `duration_ms > turn_error_seconds * 1000`

### Форматы вывода

**stdout (человекочитаемый):**
```
2026-04-01 12:00:05 [INFO] sessions.runner: Turn completed thread=141680 duration=12.3s tokens=4521
```

**Файл (JSON lines):**
```json
{"ts": "2026-04-01T12:00:05Z", "level": "INFO", "logger": "sessions.runner", "msg": "Turn completed", "thread_id": 141680, "duration_ms": 12300, "tokens": 4521}
```

## Точки логирования

### SessionRunner (`runner.py`) — основной фокус

#### Жизненный цикл turn-а

| Событие | Уровень | Поля | Условие |
|---|---|---|---|
| Turn started | INFO | `thread_id`, `session_id`, превью запроса (100 символов) | Каждый turn |
| Turn completed | INFO/WARN/ERROR | `thread_id`, `session_id`, `duration_ms`, `tool_count` | Всегда; уровень зависит от порогов длительности |
| Message enqueued | INFO/WARN | `thread_id`, `queue_size` | Всегда; WARN если queue_size > 5 |
| Session started | INFO | `thread_id`, `session_id`, `workdir`, `provider` | Создание сессии |
| Session stopped | INFO | `thread_id`, `session_id` | Остановка сессии |
| Session error | ERROR | `thread_id`, `session_id`, traceback | Необработанное исключение |

#### Tool calls — каждый инструмент внутри turn-а

Логируем **каждый** tool call, не только permission-запросы. Это ключевой сигнал для диагностики зависаний — позволяет определить, на каком именно инструменте застряла сессия.

| Событие | Уровень | Поля | Условие |
|---|---|---|---|
| Tool use | INFO | `thread_id`, `tool_name`, `tool_index` (порядковый номер в turn-е) | Каждый ToolUseBlock в AssistantMessage |
| Permission requested | INFO | `thread_id`, `tool_name` | Каждый запрос разрешения |
| Permission resolved | INFO | `thread_id`, `tool_name`, результат (allow/deny/always), `duration_ms` | Каждый ответ на разрешение |
| Permission timeout | ERROR | `thread_id`, `tool_name`, `duration_ms` | Сработал таймаут |

Пример цепочки в логах при зависании:
```
12:00:01 [INFO] Tool use thread=141680 tool=Read tool_index=1
12:00:02 [INFO] Tool use thread=141680 tool=Edit tool_index=2
12:00:03 [INFO] Tool use thread=141680 tool=Bash tool_index=3
... (тишина — значит Bash повис или API перестал отвечать)
12:05:03 [WARN] Session stuck in RUNNING thread=141680 stuck_duration=300s last_tool=Bash
```

#### SDK-события — поток сообщений внутри turn-а

Логируем **тип каждого SDK-сообщения** с timestamp в `_drain_response`. Это позволяет отличить "API не отвечает" от "Claude зацикливается в tool calls".

| Событие | Уровень | Поля | Условие |
|---|---|---|---|
| SDK message received | DEBUG | `thread_id`, `msg_type` (AssistantMessage/ResultMessage/...), `msg_index` (порядковый номер) | Каждое сообщение от SDK |
| SDK silence | WARNING | `thread_id`, `last_msg_type`, `silence_duration_ms` | Watchdog сработал (3 мин без сообщений) |
| SDK hard silence | ERROR | `thread_id`, `last_msg_type`, `silence_duration_ms` | Жёсткий watchdog (10 мин без сообщений) |
| Rate limit hit | WARNING | `thread_id`, `resets_at` | RateLimitEvent с status=rejected |
| Sub-agent started | INFO | `thread_id`, description | TaskStartedMessage |
| Sub-agent completed | INFO | `thread_id`, status, summary (100 символов) | TaskNotificationMessage |

Пример диагностики через логи:
```
# Сценарий 1: API перестал отвечать
12:00:01 [DEBUG] SDK msg thread=141680 type=AssistantMessage index=5
12:00:01 [INFO]  Tool use thread=141680 tool=Bash tool_index=3
... (3 минуты тишины)
12:03:01 [WARN]  SDK silence thread=141680 last_msg=AssistantMessage silence=180s

# Сценарий 2: Claude зацикливается
12:00:01 [INFO] Tool use thread=141680 tool=Read tool_index=1
12:00:02 [INFO] Tool use thread=141680 tool=Read tool_index=2
12:00:03 [INFO] Tool use thread=141680 tool=Read tool_index=3
... (сотни tool calls подряд — видно в логах)

# Сценарий 3: Застряло на permission
12:00:01 [INFO] Permission requested thread=141680 tool=Bash
... (2 минуты без ответа)
12:02:01 [ERROR] Permission timeout thread=141680 tool=Bash duration=120s
```

#### Дополнительное поле `_last_tool_name`

`SessionRunner` получает поле `_last_tool_name: str | None` — обновляется при каждом ToolUseBlock. Health check использует его в логе зависания для указания последнего известного инструмента.

### SessionManager (`manager.py`)

| Событие | Уровень | Поля |
|---|---|---|
| Session create | INFO | `thread_id`, `workdir`, `provider`, `server` |
| Session resume started | INFO | количество сессий для восстановления |
| Session resume completed | INFO | `duration_ms`, количество восстановленных |

### Dispatcher (`dispatcher.py`)

| Событие | Уровень | Поля |
|---|---|---|
| Bot startup completed | INFO | `duration_ms` |
| Bot shutdown completed | INFO | `duration_ms`, количество остановленных сессий |

### Health check (`health.py`)

| Событие | Уровень | Поля | Условие |
|---|---|---|---|
| Session stuck in RUNNING | WARNING | `thread_id`, `session_id`, `stuck_duration_ms`, `last_tool`, `sdk_msg_count` | `_turn_started_at` > 5 мин назад |
| Session likely hung | ERROR | `thread_id`, `session_id`, `stuck_duration_ms`, `last_tool`, `sdk_msg_count` | `_turn_started_at` > 15 мин назад |
| Health check summary | DEBUG | количество активных сессий, их состояния | Каждый цикл проверки |
| Zombie detected | WARNING | (существующий) | task.done() но не STOPPED |

### Voice (`voice.py`)

| Событие | Уровень | Поля |
|---|---|---|
| Transcription completed | INFO | `duration_ms`, `file_size` |

## Обнаружение зависаний

### Механизм

- `SessionRunner` получает новое поле: `_turn_started_at: float | None`
- Устанавливается в `time.monotonic()` при входе в turn
- Сбрасывается в `None` при завершении turn-а (или остановке сессии)
- Health check (каждые 60 сек) читает это поле и логирует предупреждения/ошибки по порогам

### Пороги (настраиваемые через Settings)

- `turn_warn_seconds = 300` (5 мин) → WARNING
- `turn_error_seconds = 900` (15 мин) → ERROR

### Без автоматического kill

Зависшие сессии только логируются, не убиваются. Причины:
- Claude может легитимно работать долго (большие рефакторинги, множество tool calls)
- Автоматический kill рискует потерять прогресс
- Цель — наблюдаемость, а не автоматическое восстановление

## Конфигурация

### Новые поля в `Settings` (`config.py`)

```python
log_dir: str = "logs"
log_max_bytes: int = 50_000_000      # 50 MB
log_backup_count: int = 3
log_level: str = "INFO"
turn_warn_seconds: int = 300          # 5 мин
turn_error_seconds: int = 900         # 15 мин
```

Все поля опциональные с разумными дефолтами — работает из коробки без изменений в `.env`.

### Новые записи в `.env.example`

```
# Logging
# LOG_DIR=logs
# LOG_LEVEL=INFO
# TURN_WARN_SECONDS=300
# TURN_ERROR_SECONDS=900
```

### `.gitignore`

Добавить директорию `logs/`.

## Что НЕ логируем

- Содержимое сообщений пользователя (приватность)
- Содержимое ответов Claude (слишком объёмно)
- Input данные tool calls (могут содержать большие блоки кода) — логируем только имя инструмента

## Что НЕ меняем

- Существующие вызовы `logger.info/warning/error` продолжают работать — теперь они также попадают в JSON-файл
- Никаких новых зависимостей — только stdlib `logging`
- Никаких изменений в существующей логике поведения или обработки ошибок

## Файлы для изменения

1. **Новый:** `src/logging_config.py` (~150 строк)
2. **Изменить:** `src/__main__.py` — вызов `setup_logging()`
3. **Изменить:** `src/sessions/runner.py` — добавить `_turn_started_at`, обёртки `timed()`, логирование глубины очереди
4. **Изменить:** `src/sessions/manager.py` — замеры для resume_all, логирование создания сессий
5. **Изменить:** `src/bot/dispatcher.py` — замеры для startup/shutdown
6. **Изменить:** `src/sessions/health.py` — детекция зависших сессий
7. **Изменить:** `src/sessions/voice.py` — замер транскрипции
8. **Изменить:** `src/config.py` — новые поля Settings
9. **Изменить:** `.env.example` — новые записи
10. **Изменить:** `.gitignore` — добавить `logs/`
