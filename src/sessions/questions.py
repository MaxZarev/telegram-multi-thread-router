"""AskUserQuestion bridge — proxies Claude's interactive questions to Telegram inline buttons."""

from __future__ import annotations

import asyncio
import html
import json
import logging
import uuid
from dataclasses import dataclass, field

from aiogram.filters.callback_data import CallbackData
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

logger = logging.getLogger(__name__)

# Special option indices
OPT_DONE = -1   # "Done" button for multiSelect
OPT_OTHER = -2  # "Other" (free text) — not supported yet, skip


class QuestionCallback(CallbackData, prefix="q"):
    """Inline button callback for AskUserQuestion options.

    Packs to "q:<8chars>:<1digit>:<2digits>" ≈ 18 bytes — well within 64-byte limit.
    """
    request_id: str  # 8-char hex
    q_idx: int       # question index (0-3)
    opt_idx: int     # option index (0-3), or -1 for Done


@dataclass
class PendingQuestionSet:
    """Tracks one AskUserQuestion invocation (1-4 questions)."""
    questions: list[dict]
    selected: dict[int, set[int]] = field(default_factory=dict)  # q_idx -> selected option indices
    answered: set[int] = field(default_factory=set)               # q_idx set of fully answered questions
    future: asyncio.Future = field(default=None)
    message_ids: list[int] = field(default_factory=list)          # for cleanup after answering


def build_question_keyboard(request_id: str, q_idx: int, question: dict, selected: set[int] | None = None) -> InlineKeyboardMarkup:
    """Build inline keyboard for one question.

    Single-select: one tap = answered.
    Multi-select: toggle options + "Done ✅" button.
    """
    builder = InlineKeyboardBuilder()
    multi = question.get("multiSelect", False)
    selected = selected or set()

    for i, opt in enumerate(question.get("options", [])):
        label = opt.get("label", f"Option {i}")
        if multi and i in selected:
            label = f"✅ {label}"
        builder.button(
            text=label,
            callback_data=QuestionCallback(request_id=request_id, q_idx=q_idx, opt_idx=i),
        )

    if multi:
        builder.button(
            text="Done ✅",
            callback_data=QuestionCallback(request_id=request_id, q_idx=q_idx, opt_idx=OPT_DONE),
        )
        # Options in rows of 1, Done on its own row
        builder.adjust(1)
    else:
        builder.adjust(1)

    return builder.as_markup()


def format_question_message(question: dict) -> str:
    """Format one question as HTML message with option descriptions."""
    header = question.get("header", "")
    text = question.get("question", "")
    multi = question.get("multiSelect", False)

    lines = []
    if header:
        lines.append(f"<b>❓ {html.escape(header)}</b>")
    lines.append(html.escape(text))
    if multi:
        lines.append("\n<i>Select one or more options, then tap Done ✅</i>")
    lines.append("")

    for i, opt in enumerate(question.get("options", [])):
        label = html.escape(opt.get("label", f"Option {i}"))
        desc = html.escape(opt.get("description", ""))
        lines.append(f"  <b>{i + 1}.</b> {label}")
        if desc:
            lines.append(f"      <i>{desc}</i>")

    return "\n".join(lines)


class QuestionManager:
    """Manages pending AskUserQuestion sessions with asyncio.Future bridge to Telegram buttons."""

    def __init__(self) -> None:
        self._pending: dict[str, PendingQuestionSet] = {}

    def create_request(self, questions: list[dict]) -> tuple[str, asyncio.Future]:
        """Create a pending question set. Returns (request_id, future).

        The future resolves with dict[str, str] — question_text -> answer_label(s).
        """
        request_id = uuid.uuid4().hex[:8]
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        pqs = PendingQuestionSet(
            questions=questions,
            selected={i: set() for i in range(len(questions))},
            future=future,
        )
        self._pending[request_id] = pqs
        return request_id, future

    def handle_selection(self, request_id: str, q_idx: int, opt_idx: int) -> dict | None:
        """Handle a button tap. Returns a dict if action needed:

        - {"action": "update_keyboard", "q_idx": int, "question": dict, "selected": set}
          → rebuild keyboard with new toggle state
        - {"action": "question_answered", "q_idx": int}
          → question done, can delete/edit message
        - {"action": "all_done", "answers": dict}
          → all questions answered, future resolved
        - None if request not found or stale
        """
        pqs = self._pending.get(request_id)
        if pqs is None or pqs.future.done():
            return None

        if q_idx < 0 or q_idx >= len(pqs.questions):
            return None

        question = pqs.questions[q_idx]
        multi = question.get("multiSelect", False)

        if multi:
            if opt_idx == OPT_DONE:
                # Finalize this question with current selections
                pqs.answered.add(q_idx)
                return self._check_complete(request_id, pqs, q_idx)
            else:
                # Toggle option
                if opt_idx in pqs.selected[q_idx]:
                    pqs.selected[q_idx].discard(opt_idx)
                else:
                    pqs.selected[q_idx].add(opt_idx)
                return {
                    "action": "update_keyboard",
                    "q_idx": q_idx,
                    "question": question,
                    "selected": pqs.selected[q_idx],
                    "request_id": request_id,
                }
        else:
            # Single select — immediately answered
            pqs.selected[q_idx] = {opt_idx}
            pqs.answered.add(q_idx)
            return self._check_complete(request_id, pqs, q_idx)

    def _check_complete(self, request_id: str, pqs: PendingQuestionSet, q_idx: int) -> dict:
        """Check if all questions answered. If so, resolve future and clean up."""
        if len(pqs.answered) >= len(pqs.questions):
            answers = self._build_answers(pqs)
            self._pending.pop(request_id, None)
            if not pqs.future.done():
                pqs.future.set_result(answers)
            return {"action": "all_done", "answers": answers}
        return {"action": "question_answered", "q_idx": q_idx}

    def _build_answers(self, pqs: PendingQuestionSet) -> dict[str, str]:
        """Build answers dict: question_text -> selected label(s)."""
        answers = {}
        for i, question in enumerate(pqs.questions):
            q_text = question.get("question", f"Question {i}")
            options = question.get("options", [])
            selected_indices = pqs.selected.get(i, set())
            selected_labels = [
                options[idx].get("label", f"Option {idx}")
                for idx in sorted(selected_indices)
                if idx < len(options)
            ]
            if question.get("multiSelect", False):
                answers[q_text] = ", ".join(selected_labels) if selected_labels else "(no selection)"
            else:
                answers[q_text] = selected_labels[0] if selected_labels else "(no selection)"
        return answers

    def get_pending(self, request_id: str) -> PendingQuestionSet | None:
        return self._pending.get(request_id)

    def expire(self, request_id: str) -> None:
        """Remove and cancel a pending question set."""
        pqs = self._pending.pop(request_id, None)
        if pqs and not pqs.future.done():
            pqs.future.cancel()

    def add_message_id(self, request_id: str, message_id: int) -> None:
        """Track a Telegram message_id for later cleanup."""
        pqs = self._pending.get(request_id)
        if pqs:
            pqs.message_ids.append(message_id)
