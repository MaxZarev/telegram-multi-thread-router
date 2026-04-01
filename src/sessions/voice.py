"""Voice transcription module with pluggable backends (whisper / deepgram)."""

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Whisper backend (local, CPU)
# ---------------------------------------------------------------------------

_whisper_model = None
_whisper_semaphore = asyncio.Semaphore(1)


def _get_whisper_model():
    """Lazy-load WhisperModel on first call."""
    global _whisper_model
    if _whisper_model is None:
        from faster_whisper import WhisperModel

        logger.info("Loading Whisper model...")
        _whisper_model = WhisperModel("medium", compute_type="int8", device="cpu")
    return _whisper_model


async def _transcribe_whisper(ogg_path: str) -> str:
    async with _whisper_semaphore:
        model = _get_whisper_model()
        segments, _info = await asyncio.to_thread(
            model.transcribe, ogg_path, beam_size=5
        )
        return " ".join(seg.text.strip() for seg in segments)


# ---------------------------------------------------------------------------
# Deepgram backend (cloud API)
# ---------------------------------------------------------------------------

_deepgram_client = None


def _get_deepgram_client():
    """Lazy-init Deepgram async client."""
    global _deepgram_client
    if _deepgram_client is None:
        from deepgram import AsyncDeepgramClient
        from src.config import settings

        if not settings.deepgram_api_key:
            raise RuntimeError("DEEPGRAM_API_KEY is not set")
        _deepgram_client = AsyncDeepgramClient(api_key=settings.deepgram_api_key)
    return _deepgram_client


async def _transcribe_deepgram(ogg_path: str) -> str:
    client = _get_deepgram_client()
    with open(ogg_path, "rb") as f:
        audio_bytes = f.read()
    response = await client.listen.v1.media.transcribe_file(
        request=audio_bytes,
        model="nova-3",
        language="ru",
        smart_format=True,
    )
    return response.results.channels[0].alternatives[0].transcript


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def transcribe_voice(ogg_path: str, backend: str = "whisper") -> str:
    """Transcribe a voice .ogg file to text."""
    try:
        file_size = os.path.getsize(ogg_path)
        start = time.monotonic()
        if backend == "deepgram":
            result = await _transcribe_deepgram(ogg_path)
        else:
            result = await _transcribe_whisper(ogg_path)
        duration_ms = int((time.monotonic() - start) * 1000)
        logger.info(
            "Transcription completed backend=%s duration=%dms file_size=%d",
            backend, duration_ms, file_size,
            extra={"duration_ms": duration_ms, "file_size": file_size},
        )
        return result
    except Exception as e:
        logger.error("Voice transcription failed (%s) for %s: %s", backend, ogg_path, e)
        raise
