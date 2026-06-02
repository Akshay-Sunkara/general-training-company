import asyncio
from collections import deque
from difflib import SequenceMatcher
import json
import logging
import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv
from livekit import rtc
from livekit.agents import (
    Agent,
    AgentSession,
    JobContext,
    WorkerOptions,
    cli,
)
from livekit.agents.voice.io import VideoInput
from livekit.agents.voice.room_io import RoomOptions
from livekit.plugins import openai
from openai.types import realtime

load_dotenv(Path(__file__).parent.parent / ".env.local")

logging.basicConfig(level=logging.INFO)
logging.getLogger("livekit.plugins.openai").setLevel(logging.DEBUG)
log = logging.getLogger("guidance-agent")

OPENAI_DIRECT_AUDIO = os.getenv("OPENAI_DIRECT_AUDIO", "0") == "1"
OPENAI_REALTIME_MODEL = os.getenv("OPENAI_REALTIME_MODEL", "gpt-realtime-2")
OPENAI_REALTIME_VOICE = os.getenv("OPENAI_REALTIME_VOICE", "marin")
OPENAI_REALTIME_SPEED = float(os.getenv("OPENAI_REALTIME_SPEED", "1.05"))
OPENAI_AUTO_RESPONSE = os.getenv("OPENAI_AUTO_RESPONSE", "1").lower() not in {"0", "false", "off"}
ENABLE_VIDEO_STREAM = os.getenv("ENABLE_VIDEO_STREAM", "1").lower() not in {"0", "false", "off"}
RESPONSE_DEBOUNCE_MS = int(os.getenv("RESPONSE_DEBOUNCE_MS", "650"))
ECHO_SUPPRESS_WINDOW_S = float(os.getenv("ECHO_SUPPRESS_WINDOW_S", "14"))
ECHO_MIN_CHARS = int(os.getenv("ECHO_MIN_CHARS", "14"))
ECHO_SIMILARITY_THRESHOLD = float(os.getenv("ECHO_SIMILARITY_THRESHOLD", "0.86"))

_SILENT_PLACEHOLDERS = {
    "",
    "silence",
    "silent",
    "no response",
    "no reply",
    "do not respond",
    "ignore",
}

_INTENT_CORRECTION_RE = re.compile(
    r"\b(?:actually|wait|hold on|scratch that|nevermind|never mind|instead|rather|no[, ]+no|before that)\b[,\s]*",
    re.IGNORECASE,
)


def _silence_key(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower().strip(" .,:;!?-—_[](){}<>\"'`")).strip()


def _is_silent_placeholder(text: str | None) -> bool:
    if text is None:
        return True
    return _silence_key(text) in _SILENT_PLACEHOLDERS


def _latest_user_intent(text: str | None) -> str:
    if not text:
        return ""
    parts = [part.strip(" ,.;:!?") for part in _INTENT_CORRECTION_RE.split(text) if part.strip(" ,.;:!?")]
    return parts[-1] if parts else text.strip()


async def _publish_assistant_text(ctx: "JobContext", text: str) -> None:
    """Send the assistant's full text reply to the Node mic-bridge on topic
    "guidance.tts". Node uses Mentra's session.audio.speak() to TTS it on the
    glasses — that path is reliable, unlike playAudio."""
    if _is_silent_placeholder(text):
        log.info("suppressing silent placeholder: %r", text)
        return
    try:
        await ctx.room.local_participant.publish_data(
            payload=text.encode("utf-8"),
            reliable=True,
            topic="guidance.tts",
        )
    except Exception as e:
        log.warning("publish assistant text failed: %s", e)


class GuidanceAgent(Agent):
    def __init__(self) -> None:
        super().__init__(
            instructions=(
                "You are mechie, a real-time guide for someone wearing smart glasses. "
                "You can hear them speak and you continuously see the live video feed from the "
                "glasses camera, so you always have their current view as context. "
                "Use what you see in the live video to answer anything about their surroundings, "
                "objects, screens, or whatever is visible — never ask them to show you or to take a "
                "picture, and never narrate camera mechanics or streaming status. "
                "If the user corrects themselves with phrases like 'actually', 'no no', 'instead', "
                "'rather', 'before that', or 'scratch that', follow the latest corrected intent and ignore "
                "the earlier abandoned request. "

                "Reply in ONE short sentence, max 12 words. Never list, elaborate, or restate the question. "
                "Always respond in English unless they speak another language. "

                "INPUT SELECTION (CRITICAL): only respond to speech that is clearly directed at you by "
                "the wearer of the glasses. Treat room audio, other people talking nearby, media playback, "
                "and repeated assistant audio as background context, not as requests. If an utterance is "
                "not clearly addressed to you or does not need a reply, produce no assistant content. "

                "SILENCE RULE (CRITICAL): if you should ignore the user, ignore an echo, "
                "or stay silent, produce no assistant content at all. Never say or output "
                "placeholders such as 'Silence', '[silence]', 'silent', 'no response', or '...'. "

                "ECHO HANDLING (CRITICAL): the glasses' speaker plays your voice and the "
                "mic picks it back up. Many incoming user transcripts will be ECHOES of "
                "what you JUST said — these are NOT the user speaking, and you must NOT "
                "respond to them. Before replying, check the recent conversation: if the "
                "incoming transcript is a paraphrase, near-copy, or substring of anything "
                "you said in the last 10 seconds, treat it as an echo and stay silent. "
                "Do not acknowledge echoes. Do not apologize. Stay completely silent. "
                "Only respond when the transcript is clearly a fresh utterance from the wearer. "

                "Ignore background voices; respond only to the primary speaker wearing the device. "
                "If the user wants you to ignore everything he says, make sure to ignore everything and not respond at all, even if they're talking about you or your capabilities. "
            )
        )


class GlassesVideoInput(VideoInput):
    def __init__(self, room: rtc.Room, participant_identity: str = "glasses") -> None:
        super().__init__(label=f"{participant_identity}-direct-video")
        self._room = room
        self._participant_identity = participant_identity
        self._requested = False
        self._attached = False
        self._queue: asyncio.Queue[rtc.VideoFrame] = asyncio.Queue(maxsize=1)
        self._stream: rtc.VideoStream | None = None
        self._forward_task: asyncio.Task[None] | None = None
        self._publication_sid: str | None = None
        self._room.on("track_subscribed", self._on_track_subscribed)
        self._room.on("track_unpublished", self._on_track_unpublished)

    def set_requested(self, requested: bool) -> None:
        if self._requested == requested:
            return
        self._requested = requested
        if requested and self._attached:
            self._attach_existing_track()
        elif not requested:
            self._close_stream()
            self._drain_queue()

    def on_attached(self) -> None:
        self._attached = True
        if self._requested:
            self._attach_existing_track()

    def on_detached(self) -> None:
        self._attached = False
        self._close_stream()
        self._drain_queue()

    async def __anext__(self) -> rtc.VideoFrame:
        return await self._queue.get()

    def _drain_queue(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return

    def _track_is_usable(self, track: rtc.Track, publication: rtc.RemoteTrackPublication, participant: rtc.RemoteParticipant) -> bool:
        return (
            self._requested
            and self._attached
            and participant.identity == self._participant_identity
            and track.kind == rtc.TrackKind.KIND_VIDEO
            and publication.source in {rtc.TrackSource.SOURCE_CAMERA, rtc.TrackSource.SOURCE_SCREENSHARE}
        )

    def _attach_existing_track(self) -> None:
        participant = self._room.remote_participants.get(self._participant_identity)
        if not participant:
            return
        for publication in participant.track_publications.values():
            track = publication.track
            if track and self._track_is_usable(track, publication, participant):
                self._start_stream(track, publication)
                return

    def _on_track_subscribed(self, track: rtc.Track, publication: rtc.RemoteTrackPublication, participant: rtc.RemoteParticipant) -> None:
        if self._track_is_usable(track, publication, participant):
            self._start_stream(track, publication)

    def _on_track_unpublished(self, publication: rtc.RemoteTrackPublication, participant: rtc.RemoteParticipant) -> None:
        if participant.identity == self._participant_identity and publication.sid == self._publication_sid:
            self._close_stream()

    def _start_stream(self, track: rtc.Track, publication: rtc.RemoteTrackPublication) -> None:
        if self._publication_sid == publication.sid and self._forward_task and not self._forward_task.done():
            return
        self._close_stream()
        self._publication_sid = publication.sid
        self._stream = rtc.VideoStream.from_track(track=track)
        self._forward_task = asyncio.create_task(self._forward_frames())

    def _close_stream(self) -> None:
        if self._forward_task and not self._forward_task.done():
            self._forward_task.cancel()
        self._forward_task = None
        if self._stream:
            asyncio.create_task(self._stream.aclose())
        self._stream = None
        self._publication_sid = None

    async def _forward_frames(self) -> None:
        if not self._stream:
            return
        try:
            async for event in self._stream:
                if not self._requested or not self._attached:
                    continue
                if self._queue.full():
                    try:
                        self._queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                await self._queue.put(event.frame)
        except asyncio.CancelledError:
            pass


async def entrypoint(ctx: JobContext) -> None:
    log.info("agent dispatched to room=%s", ctx.room.name)
    await ctx.connect()
    glasses_video_input = GlassesVideoInput(ctx.room)

    @ctx.room.on("participant_connected")
    def _on_join(p):
        log.info("participant joined: identity=%s", p.identity)

    @ctx.room.on("track_subscribed")
    def _on_track(track, pub, p):
        log.info("track subscribed: participant=%s kind=%s source=%s", p.identity, track.kind, pub.source)

    log.info("participants already in room: %s",
             [p.identity for p in ctx.room.remote_participants.values()])

    realtime_model = openai.realtime.RealtimeModel(
        model=OPENAI_REALTIME_MODEL,
        voice=OPENAI_REALTIME_VOICE,
        modalities=["audio"] if OPENAI_DIRECT_AUDIO else ["text"],
        speed=OPENAI_REALTIME_SPEED,
        input_audio_noise_reduction="near_field",
        turn_detection=realtime.realtime_audio_input_turn_detection.SemanticVad(
            type="semantic_vad",
            create_response=OPENAI_AUTO_RESPONSE,
            eagerness="high",
            interrupt_response=OPENAI_AUTO_RESPONSE,
        ),
    )
    log.info(
        "openai realtime configured: model=%s direct_audio=%s vad=semantic/high noise_reduction=near_field auto_response=%s fallback_debounce_ms=%s voice=%s speed=%.2f",
        OPENAI_REALTIME_MODEL,
        OPENAI_DIRECT_AUDIO,
        OPENAI_AUTO_RESPONSE,
        RESPONSE_DEBOUNCE_MS,
        OPENAI_REALTIME_VOICE,
        OPENAI_REALTIME_SPEED,
    )

    session = AgentSession(
        llm=realtime_model,
    )
    # Always-on video: the glasses camera feed is wired in and enabled for the whole
    # session, so the agent continuously has live visual context.
    session.input.video = glasses_video_input
    glasses_video_input.set_requested(True)
    session.input.set_video_enabled(True)

    # Sentence-streaming TTS: publish each completed sentence as it arrives so Node's
    # speak() can start TTS before the full reply is generated. The monkey-patch below
    # populates `_text_buffer` from text deltas; `_on_item` is a fallback for when
    # streaming hooks didn't fire (e.g., on plugin version mismatch).
    _SENTENCE_RE = re.compile(r"[.,;:!?—](?:\s+|$)")
    _text_buffer = {"v": ""}
    _streamed_this_turn = {"v": False}
    _muted = {"v": False}
    _user_state = {"v": "listening"}
    _last_user_item_at = {"v": 0.0}
    _last_user_activity_at = {"v": 0.0}
    _pending_reply_task: dict[str, asyncio.Task[None] | None] = {"v": None}
    _latest_user_seq = {"v": 0}
    _active_response_seq = {"v": 0}
    _response_in_flight = {"v": False}
    _stale_response_seqs: set[int] = set()
    _published_assistant_item_ids: set[str] = set()
    _last_assistant_publish = {"text": "", "at": 0.0}
    _recent_assistant_text: deque[tuple[str, float]] = deque(maxlen=24)

    def _remember_assistant_text(text: str) -> None:
        text_key = _silence_key(text)
        if len(text_key) < ECHO_MIN_CHARS:
            return
        now = time.monotonic()
        if _recent_assistant_text and _recent_assistant_text[-1][0] == text_key:
            _recent_assistant_text[-1] = (text_key, now)
            return
        _recent_assistant_text.append((text_key, now))

    def _is_recent_assistant_echo(text: str | None) -> bool:
        text_key = _silence_key(text or "")
        if len(text_key) < ECHO_MIN_CHARS:
            return False

        now = time.monotonic()
        while _recent_assistant_text and now - _recent_assistant_text[0][1] > ECHO_SUPPRESS_WINDOW_S:
            _recent_assistant_text.popleft()

        for assistant_key, spoken_at in _recent_assistant_text:
            if now - spoken_at > ECHO_SUPPRESS_WINDOW_S:
                continue
            if text_key == assistant_key:
                return True
            min_len = min(len(text_key), len(assistant_key))
            if min_len >= 24 and (text_key in assistant_key or assistant_key in text_key):
                return True
            if min_len >= 24 and SequenceMatcher(None, text_key, assistant_key).ratio() >= ECHO_SIMILARITY_THRESHOLD:
                return True
        return False

    def _cancel_pending_reply(reason: str) -> None:
        task = _pending_reply_task["v"]
        if task and not task.done():
            task.cancel()
            log.info("pending response canceled: %s", reason)
        _pending_reply_task["v"] = None

    async def _interrupt_agent(reason: str) -> None:
        _cancel_pending_reply(reason)
        _text_buffer["v"] = ""
        _streamed_this_turn["v"] = False
        try:
            await session.interrupt(force=True)
        except Exception as e:
            log.warning("%s failed: %s", reason, e)

    def _schedule_reply(user_text: str | None) -> None:
        text = (user_text or "").strip()
        if not text:
            return
        if _is_recent_assistant_echo(text):
            log.info("response skipped for assistant echo: %r", text)
            asyncio.create_task(_interrupt_agent("assistant echo"))
            return
        if _muted["v"]:
            log.info("response skipped while muted: %r", text)
            if OPENAI_AUTO_RESPONSE:
                asyncio.create_task(_interrupt_agent("muted auto response"))
            return
        latest_intent = _latest_user_intent(text)
        if latest_intent != text:
            log.info("latest corrected user intent: %r", latest_intent)

        now = time.monotonic()
        _latest_user_seq["v"] += 1
        user_seq = _latest_user_seq["v"]
        _last_user_item_at["v"] = now
        _last_user_activity_at["v"] = now
        _cancel_pending_reply("new user turn")

        if OPENAI_AUTO_RESPONSE:
            _active_response_seq["v"] = user_seq
            _response_in_flight["v"] = True
            log.info("auto response enabled; no manual response.create for seq=%s", user_seq)
            return

        async def _delayed_reply() -> None:
            try:
                debounce_s = RESPONSE_DEBOUNCE_MS / 1000
                while True:
                    await asyncio.sleep(debounce_s)
                    if user_seq != _latest_user_seq["v"]:
                        log.info(
                            "stale pending response skipped: seq=%s latest=%s",
                            user_seq,
                            _latest_user_seq["v"],
                        )
                        return
                    if _muted["v"]:
                        log.info("debounced response skipped while muted")
                        return
                    if str(_user_state["v"]).endswith("speaking"):
                        log.info("response waiting; user_state=%s", _user_state["v"])
                        continue
                    last_activity_at = max(_last_user_item_at["v"], _last_user_activity_at["v"])
                    quiet_ms = int((time.monotonic() - last_activity_at) * 1000)
                    if quiet_ms < RESPONSE_DEBOUNCE_MS:
                        log.info("response waiting; quiet_ms=%s", quiet_ms)
                        continue
                    if _response_in_flight["v"]:
                        log.info(
                            "response waiting; response already in flight seq=%s",
                            _active_response_seq["v"],
                        )
                        continue
                    log.info(
                        "manual response.create after %sms quiet window; user_state=%s",
                        RESPONSE_DEBOUNCE_MS,
                        _user_state["v"],
                    )
                    _active_response_seq["v"] = user_seq
                    _response_in_flight["v"] = True
                    session.generate_reply()
                    return
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning("manual response.create failed: %s", e)
            finally:
                if _pending_reply_task["v"] is asyncio.current_task():
                    _pending_reply_task["v"] = None

        _pending_reply_task["v"] = asyncio.create_task(_delayed_reply())

    @session.on("user_state_changed")
    def _on_user_state(ev):
        log.info("user_state: %s -> %s", ev.old_state, ev.new_state)
        _user_state["v"] = str(ev.new_state)
        _last_user_activity_at["v"] = time.monotonic()
        if str(ev.new_state).endswith("speaking"):
            if _response_in_flight["v"]:
                _stale_response_seqs.add(_active_response_seq["v"])
                _response_in_flight["v"] = False
                log.info(
                    "user resumed speaking; response seq=%s will be dropped if it arrives",
                    _active_response_seq["v"],
                )
            else:
                log.info("user resumed speaking; pending response will wait")

    @session.on("agent_state_changed")
    def _on_agent_state(ev):
        log.info("agent_state: %s -> %s", ev.old_state, ev.new_state)
        if str(ev.new_state).endswith("listening"):
            _response_in_flight["v"] = False

    @session.on("user_input_transcribed")
    def _on_user_text(ev):
        log.info(">>> user said: %r (final=%s)", getattr(ev, "transcript", None), getattr(ev, "is_final", None))

    @ctx.room.on("data_received")
    def _on_data(packet):
        if packet.topic == "guidance.button_interrupt":
            reason = packet.data.decode("utf-8", errors="replace") if packet.data else "button"
            log.info(
                "button interrupt received from %s: %s",
                getattr(packet.participant, "identity", "?"),
                reason,
            )
            asyncio.create_task(_interrupt_agent("button interrupt"))
            return

        if packet.topic != "guidance.control":
            return

        try:
            payload = json.loads(packet.data.decode("utf-8") if packet.data else "{}")
        except Exception as e:
            log.warning("invalid guidance.control payload: %s", e)
            return

        kind = payload.get("type")
        reason = payload.get("reason") or "control"
        log.info(
            "control received from %s: type=%s reason=%s",
            getattr(packet.participant, "identity", "?"),
            kind,
            reason,
        )
        if kind == "mute":
            _muted["v"] = True
            asyncio.create_task(_interrupt_agent("mute"))
        elif kind == "unmute":
            _muted["v"] = False
        elif kind == "interrupt":
            asyncio.create_task(_interrupt_agent("control interrupt"))

    def _flush_completed_sentences() -> None:
        while True:
            m = _SENTENCE_RE.search(_text_buffer["v"])
            if not m:
                break
            end = m.end()
            sentence = _text_buffer["v"][:end].strip()
            _text_buffer["v"] = _text_buffer["v"][end:]
            if _muted["v"]:
                log.info("[stream-tts] suppressed while muted: %r", sentence)
                continue
            if sentence and not _is_silent_placeholder(sentence):
                if _should_publish_assistant_text(None, sentence, "stream-tts"):
                    log.info("[stream-tts] sentence: %r", sentence)
                    asyncio.create_task(_publish_assistant_text(ctx, sentence))
            elif sentence:
                log.info("[stream-tts] suppressed silent placeholder: %r", sentence)

    def _should_publish_assistant_text(item, text: str, source: str) -> bool:
        item_id = getattr(item, "id", None) or getattr(item, "item_id", None)
        if item_id:
            item_id = str(item_id)
            if item_id in _published_assistant_item_ids:
                log.info("assistant duplicate item suppressed: id=%s source=%s", item_id, source)
                return False

        active_seq = _active_response_seq["v"]
        latest_seq = _latest_user_seq["v"]
        if active_seq in _stale_response_seqs or active_seq < latest_seq:
            log.info(
                "assistant stale response suppressed: seq=%s latest=%s source=%s text=%r",
                active_seq,
                latest_seq,
                source,
                text,
            )
            _response_in_flight["v"] = False
            _stale_response_seqs.discard(active_seq)
            return False

        text_key = _silence_key(text)
        now = time.monotonic()
        if (
            text_key
            and text_key == _last_assistant_publish["text"]
            and now - _last_assistant_publish["at"] < 4
        ):
            log.info("assistant duplicate text suppressed: source=%s text=%r", source, text)
            _response_in_flight["v"] = False
            return False

        if item_id:
            _published_assistant_item_ids.add(item_id)
        _remember_assistant_text(text)
        _last_assistant_publish["text"] = text_key
        _last_assistant_publish["at"] = now
        _response_in_flight["v"] = False
        _stale_response_seqs.discard(active_seq)
        return True

    @session.on("conversation_item_added")
    def _on_item(ev):
        role = getattr(ev.item, "role", "?")
        text = getattr(ev.item, "text_content", None)
        log.info("conversation_item: role=%s text=%r", role, text)
        if role == "user":
            _schedule_reply(text)
            return
        if role == "assistant" and text and not OPENAI_DIRECT_AUDIO:
            if _muted["v"]:
                log.info("conversation_item suppressed while muted: %r", text)
                _streamed_this_turn["v"] = False
                return
            if _is_silent_placeholder(text):
                log.info("conversation_item suppressed silent placeholder: %r", text)
                _streamed_this_turn["v"] = False
                return
            if _streamed_this_turn["v"]:
                _streamed_this_turn["v"] = False
                return  # already published sentence-by-sentence
            if not _should_publish_assistant_text(ev.item, text, "conversation_item"):
                _streamed_this_turn["v"] = False
                return
            asyncio.create_task(_publish_assistant_text(ctx, text))

    await session.start(
        agent=GuidanceAgent(),
        room=ctx.room,
        room_options=RoomOptions(
            participant_identity="mic-bridge",
            video_input=False,
            audio_output=OPENAI_DIRECT_AUDIO,
            text_output=False,
        ),
    )

    # In direct audio mode, OpenAI Realtime publishes agent audio into the LiveKit
    # room. The Node app subscribes to that track and streams it to Mentra as MP3.
    # Set OPENAI_DIRECT_AUDIO=0 to fall back to publishing assistant text on
    # guidance.tts for Mentra's session.audio.speak().


if __name__ == "__main__":
    cli.run_app(WorkerOptions(entrypoint_fnc=entrypoint, agent_name="guidance"))
