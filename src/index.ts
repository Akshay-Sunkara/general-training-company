import { AppServer, AppSession } from "@mentra/sdk";
import type { Request, Response } from "express";
import crypto from "node:crypto";
import { Mp3Encoder } from "@breezystack/lamejs";
import { AccessToken, AgentDispatchClient, IngressClient, IngressInput, RoomServiceClient } from "livekit-server-sdk";
import {
  AudioFrame,
  AudioSource,
  AudioStream,
  LocalAudioTrack,
  RemoteAudioTrack,
  RemoteVideoTrack,
  Room,
  RoomEvent,
  TrackKind,
  TrackPublishOptions,
  TrackSource,
  VideoStream,
} from "@livekit/rtc-node";

const PACKAGE_NAME = process.env.MENTRA_PACKAGE_NAME!;
const API_KEY = process.env.MENTRA_API_KEY!;
const PORT = Number(process.env.PORT ?? 3000);

const LIVEKIT_URL = process.env.LIVEKIT_URL!;
const LIVEKIT_API_KEY = process.env.LIVEKIT_API_KEY!;
const LIVEKIT_API_SECRET = process.env.LIVEKIT_API_SECRET!;
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL ?? `http://localhost:${PORT}`).replace(/\/$/, "");
const OPENAI_DIRECT_AUDIO = process.env.OPENAI_DIRECT_AUDIO === "1";
const DIRECT_AUDIO_SAMPLE_RATE = Number(process.env.DIRECT_AUDIO_SAMPLE_RATE ?? 48000);
const DIRECT_AUDIO_MP3_KBPS = Number(process.env.DIRECT_AUDIO_MP3_KBPS ?? 128);
const DIRECT_AUDIO_RMS_THRESHOLD = Number(process.env.DIRECT_AUDIO_RMS_THRESHOLD ?? 80);
const DIRECT_AUDIO_NOISE_GATE_RMS = Number(process.env.DIRECT_AUDIO_NOISE_GATE_RMS ?? 20);
const DIRECT_AUDIO_SILENCE_MS = Number(process.env.DIRECT_AUDIO_SILENCE_MS ?? 420);
const DIRECT_AUDIO_TRACK_ID = Number(process.env.DIRECT_AUDIO_TRACK_ID ?? 2);
const DIRECT_AUDIO_GAIN = Number(process.env.DIRECT_AUDIO_GAIN ?? 1.0);
const DIRECT_AUDIO_FRAME_MS = Number(process.env.DIRECT_AUDIO_FRAME_MS ?? 20);
const DIRECT_AUDIO_PREROLL_MS = Number(process.env.DIRECT_AUDIO_PREROLL_MS ?? 400);
const DIRECT_AUDIO_LEADING_SILENCE_MS = Number(process.env.DIRECT_AUDIO_LEADING_SILENCE_MS ?? 160);
const DIRECT_AUDIO_MAX_UTTERANCE_MS = Number(process.env.DIRECT_AUDIO_MAX_UTTERANCE_MS ?? 12_000);
const MIC_ECHO_SUPPRESS_AFTER_MS = Number(process.env.MIC_ECHO_SUPPRESS_AFTER_MS ?? 1600);
const ENABLE_VIDEO_STREAM = !/^(0|false|off)$/i.test(process.env.ENABLE_VIDEO_STREAM ?? "1");
const VIDEO_READY_FRAMES = Math.max(1, Number(process.env.VIDEO_READY_FRAMES ?? 3));
// If the glasses accept the stream request but never progress past "initializing"
// (encoder/ingress not ready) within this window, we stop and restart the stream.
// One calm restart with backoff if the managed stream dies — NOT an aggressive 15s loop
// (that created orphaned streams whose stale streamIds broke the cloud keep-alive ACK).
const VIDEO_RESTART_BACKOFF_MS = Number(process.env.VIDEO_RESTART_BACKOFF_MS ?? 6_000);
const MAX_VIDEO_RESTARTS = Math.max(1, Number(process.env.MAX_VIDEO_RESTARTS ?? 6));
// LiveKit ingress URLs are rtmps://. By default we hand the glasses a downgraded rtmp://
// URL (Mentra Live historically only spoke plain RTMP). Set RTMP_USE_TLS=1 to keep rtmps://
// in case LiveKit Cloud only accepts RTMPS on that host.
const RTMP_USE_TLS = /^(1|true|on)$/i.test(process.env.RTMP_USE_TLS ?? "0");
const serverStartedAt = Date.now();

// Latest managed-stream viewing URLs (set when startManagedStream resolves). Exposed via
// /managed-urls so we can inspect the WebRTC (WHEP) endpoint for the bridge.
let latestManagedUrls:
  | { streamId: string; webrtcUrl?: string; hlsUrl: string; dashUrl: string; previewUrl?: string; at: number }
  | undefined;

type FlowState = "idle" | "pending" | "active" | "quiet" | "error" | "disconnected";
type UiEventLevel = "info" | "good" | "warn" | "error";

type SessionMonitor = {
  sessionId: string;
  userId: string;
  roomName: string;
  connected: boolean;
  muted: boolean;
  connectedAt: number;
  disconnectedAt?: number;
  lastUpdatedAt: number;
  ingressId?: string;
  dispatchId?: string;
  rtmp: {
    state: FlowState | string;
    detail: string;
    lastAt?: number;
    startRequestedAt?: number;
  };
  micBridge: {
    state: FlowState;
    detail: string;
    lastAt?: number;
  };
  audio: {
    chunks: number;
    bytes: number;
    sampleRate?: number;
    lastAt?: number;
    captureErrors: number;
  };
  video: {
    forwarding: boolean;
    frames: number;
    state: FlowState;
    detail: string;
    lastAt?: number;
  };
  tts: {
    messages: number;
    speaking: boolean;
    queued: number;
    lastAt?: number;
    lastText: string;
    lastSpeakSuccess?: boolean;
    lastSpeakLatencyMs?: number;
  };
  directAudio: {
    enabled: boolean;
    subscribed: boolean;
    speaking: boolean;
    streams: number;
    frames: number;
    lastAt?: number;
    lastRms: number;
    detail: string;
  };
  errors: string[];
};

type UiEvent = {
  id: number;
  at: number;
  level: UiEventLevel;
  message: string;
};

const sessionMonitors = new Map<string, SessionMonitor>();
const uiEvents: UiEvent[] = [];
let uiEventId = 0;

function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function recordUiEvent(message: string, level: UiEventLevel = "info"): void {
  uiEvents.unshift({ id: ++uiEventId, at: Date.now(), level, message });
  uiEvents.splice(24);
}

function createSessionMonitor(sessionId: string, userId: string, roomName: string): SessionMonitor {
  const now = Date.now();
  return {
    sessionId,
    userId,
    roomName,
    connected: true,
    muted: false,
    connectedAt: now,
    lastUpdatedAt: now,
    rtmp: { state: "pending", detail: "waiting for camera stream" },
    micBridge: { state: "pending", detail: "joining LiveKit room" },
    audio: { chunks: 0, bytes: 0, captureErrors: 0 },
    video: { forwarding: false, frames: 0, state: "idle", detail: "waiting for glasses video" },
    tts: { messages: 0, speaking: false, queued: 0, lastText: "" },
    directAudio: {
      enabled: OPENAI_DIRECT_AUDIO,
      subscribed: false,
      speaking: false,
      streams: 0,
      frames: 0,
      lastRms: 0,
      detail: OPENAI_DIRECT_AUDIO ? "waiting for OpenAI audio track" : "disabled; using Mentra TTS",
    },
    errors: [],
  };
}

function touchMonitor(monitor: SessionMonitor): void {
  monitor.lastUpdatedAt = Date.now();
}

function addMonitorError(monitor: SessionMonitor, label: string, err: unknown): void {
  const message = `${label}: ${errMessage(err)}`;
  monitor.errors.unshift(message);
  monitor.errors.splice(6);
  touchMonitor(monitor);
  recordUiEvent(message, "error");
}

const SILENT_TTS_PLACEHOLDERS = new Set([
  "",
  "silence",
  "silent",
  "no response",
  "no reply",
  "do not respond",
  "ignore",
]);

function silenceKey(text: string): string {
  return text.trim().toLowerCase().replace(/^[\s.,:;!?_\-\[\](){}<>"'`]+|[\s.,:;!?_\-\[\](){}<>"'`]+$/g, "").replace(/\s+/g, " ");
}

function isSilentTtsPlaceholder(text: string): boolean {
  return SILENT_TTS_PLACEHOLDERS.has(silenceKey(text));
}

function currentMonitor(): SessionMonitor | undefined {
  return [...sessionMonitors.values()].sort((a, b) => {
    if (a.connected !== b.connected) return a.connected ? -1 : 1;
    return b.lastUpdatedAt - a.lastUpdatedAt;
  })[0];
}

// In-memory cache of generated WAV blobs that the glasses fetch via /audio/:id.wav.
const audioCache = new Map<string, { buf: Buffer; expires: number }>();
setInterval(() => {
  const now = Date.now();
  for (const [id, e] of audioCache) if (e.expires < now) audioCache.delete(id);
}, 30_000);

function pcm16ToMp3(pcm: Buffer, rate: number, kbps = 96): Buffer {
  const encoder = new Mp3Encoder(1, rate, kbps);
  const samples = new Int16Array(pcm.buffer, pcm.byteOffset, pcm.byteLength >> 1);
  const blockSize = 1152;
  const out: Buffer[] = [];
  for (let i = 0; i < samples.length; i += blockSize) {
    const chunk = samples.subarray(i, i + blockSize);
    const buf = encoder.encodeBuffer(chunk);
    if (buf.length > 0) out.push(Buffer.from(buf));
  }
  const tail = encoder.flush();
  if (tail.length > 0) out.push(Buffer.from(tail));
  return Buffer.concat(out);
}

// Streams MP3 frames over HTTP chunked transfer encoding as PCM is fed in.
// One instance per utterance — the Mp3Encoder is kept open across many pushPcm()
// calls so frame boundaries stay valid; close + flush only happens once at end().
// Multiple HTTP writers can subscribe (in practice we expect one — the phone).
class StreamingMp3Encoder {
  private encoder: Mp3Encoder;
  private writers = new Set<Response>();
  private history: Buffer[] = [];
  private ended = false;
  private bytesEncoded = 0;
  private chunksEncoded = 0;
  private replayHistory: boolean;
  readonly id: string;
  readonly createdAt = Date.now();

  get isEnded(): boolean {
    return this.ended;
  }

  get writerCount(): number {
    return this.writers.size;
  }

  get byteCount(): number {
    return this.bytesEncoded;
  }

  get chunkCount(): number {
    return this.chunksEncoded;
  }

  constructor(id: string, rate: number, kbps: number, options: { replayHistory?: boolean } = {}) {
    this.id = id;
    this.encoder = new Mp3Encoder(1, rate, kbps);
    this.replayHistory = options.replayHistory ?? true;
  }

  addWriter(res: Response): void {
    res.setHeader("Content-Type", "audio/mpeg");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("X-Accel-Buffering", "no");
    res.flushHeaders();
    if (this.replayHistory) {
      for (const buf of this.history) res.write(buf);
    }
    if (this.ended) {
      res.end();
      return;
    }
    this.writers.add(res);
    res.on("close", () => this.writers.delete(res));
    console.log(`[stream] writer attached id=${this.id} writers=${this.writers.size} replayed=${this.replayHistory ? this.history.length : 0}chunks`);
    recordUiEvent(`Stream writer attached (${this.id})`, "good");
  }

  pushPcm(samples: Int16Array): void {
    if (this.ended) return;
    const blockSize = 1152;
    for (let i = 0; i < samples.length; i += blockSize) {
      const chunk = samples.subarray(i, i + blockSize);
      const out = this.encoder.encodeBuffer(chunk);
      if (out.length > 0) {
        const buf = Buffer.from(out);
        this.bytesEncoded += buf.length;
        this.chunksEncoded++;
        if (this.replayHistory) this.history.push(buf);
        for (const w of this.writers) w.write(buf);
      }
    }
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    const tail = this.encoder.flush();
    if (tail.length > 0) {
      const buf = Buffer.from(tail);
      this.bytesEncoded += buf.length;
      this.chunksEncoded++;
      if (this.replayHistory) this.history.push(buf);
      for (const w of this.writers) w.write(buf);
    }
    for (const w of this.writers) w.end();
    this.writers.clear();
    console.log(`[stream] ended id=${this.id} bytes=${this.bytesEncoded}`);
    recordUiEvent(`Stream ended (${this.id}, ${this.bytesEncoded} bytes)`, "info");
  }
}

// Per-id map of active utterance streams. GC removes stale entries.
const streams = new Map<string, StreamingMp3Encoder>();
setInterval(() => {
  const cutoff = Date.now() - 180_000;
  for (const [id, s] of streams) {
    if (s.isEnded && s.createdAt < cutoff) streams.delete(id);
  }
}, 60_000);

function streamSummary() {
  const allStreams = [...streams.values()];
  return {
    total: allStreams.length,
    active: allStreams.filter((s) => !s.isEnded).length,
    writers: allStreams.reduce((total, s) => total + s.writerCount, 0),
    bytes: allStreams.reduce((total, s) => total + s.byteCount, 0),
    chunks: allStreams.reduce((total, s) => total + s.chunkCount, 0),
  };
}

function frameRms(samples: Int16Array): number {
  if (samples.length === 0) return 0;
  let sum = 0;
  for (let i = 0; i < samples.length; i++) {
    const v = samples[i] ?? 0;
    sum += v * v;
  }
  return Math.sqrt(sum / samples.length);
}

function applyGain(samples: Int16Array, gain: number): Int16Array {
  if (gain === 1) return samples;
  const boosted = new Int16Array(samples.length);
  for (let i = 0; i < samples.length; i++) {
    const v = Math.round((samples[i] ?? 0) * gain);
    boosted[i] = Math.max(-32768, Math.min(32767, v));
  }
  return boosted;
}

function noiseGateSamples(samples: Int16Array, rms: number): Int16Array {
  if (rms >= DIRECT_AUDIO_NOISE_GATE_RMS) return samples;
  return new Int16Array(samples.length);
}

function estimateTtsPlaybackMs(text: string): number {
  return Math.min(8_000, Math.max(1_600, text.length * 70));
}

function routeOpenAiAudioToMentra(
  track: RemoteAudioTrack,
  participantIdentity: string,
  session: AppSession,
  monitor: SessionMonitor,
  onSpeechState?: (speaking: boolean) => void,
): void {
  const stream = new AudioStream(track, {
    sampleRate: DIRECT_AUDIO_SAMPLE_RATE,
    numChannels: 1,
    frameSizeMs: DIRECT_AUDIO_FRAME_MS,
  });

  type DirectAudioUtterance = {
    id: string;
    encoder: StreamingMp3Encoder;
    startedAt: number;
    lastSpeechAt: number;
  };
  let current: DirectAudioUtterance | undefined;
  const prerollFrames: Array<{ samples: Int16Array; rms: number }> = [];
  const prerollFrameLimit = Math.max(1, Math.ceil(DIRECT_AUDIO_PREROLL_MS / DIRECT_AUDIO_FRAME_MS));
  const leadingSilenceSamples = new Int16Array(
    Math.max(1, Math.round((DIRECT_AUDIO_SAMPLE_RATE * DIRECT_AUDIO_LEADING_SILENCE_MS) / 1000)),
  );

  const startUtterance = (now: number): DirectAudioUtterance => {
    const id = crypto.randomBytes(8).toString("hex");
    const encoder = new StreamingMp3Encoder(id, DIRECT_AUDIO_SAMPLE_RATE, DIRECT_AUDIO_MP3_KBPS, {
      replayHistory: true,
    });
    const url = `${PUBLIC_BASE_URL}/stream/${id}.mp3`;
    const utterance = { id, encoder, startedAt: now, lastSpeechAt: now };
    current = utterance;
    streams.set(id, encoder);
    monitor.directAudio.streams++;
    monitor.directAudio.speaking = true;
    monitor.directAudio.detail = `streaming OpenAI audio from ${participantIdentity}`;
    touchMonitor(monitor);
    onSpeechState?.(true);
    recordUiEvent(`OpenAI audio utterance opened (${id})`, "good");
    console.log(`[openai-audio] playAudio utterance=${id} track=${DIRECT_AUDIO_TRACK_ID} url=${url}`);
    session.audio
      .playAudio({
        audioUrl: url,
        stopOtherAudio: true,
        volume: 1.0,
        trackId: DIRECT_AUDIO_TRACK_ID,
      })
      .then((r) => console.log(`[openai-audio] utterance=${id} result:`, JSON.stringify(r)))
      .catch((e) => {
        console.error(`[openai-audio] utterance=${id} failed:`, e?.message ?? e);
        addMonitorError(monitor, "OpenAI audio playback failed", e);
      });
    return utterance;
  };

  const endUtterance = (reason: string) => {
    if (!current) return;
    const ended = current;
    current = undefined;
    ended.encoder.end();
    onSpeechState?.(false);
    monitor.directAudio.speaking = false;
    monitor.directAudio.detail = `OpenAI audio utterance ended: ${reason}`;
    touchMonitor(monitor);
    recordUiEvent(`OpenAI audio utterance ended (${ended.id})`, "info");
  };

  (async () => {
    try {
      monitor.directAudio.subscribed = true;
      monitor.directAudio.detail = `subscribed to ${participantIdentity} audio`;
      touchMonitor(monitor);
      recordUiEvent(`Subscribed to OpenAI audio from ${participantIdentity}`, "good");

      for await (const frame of stream) {
        const samples = frame.data;
        const rms = frameRms(samples);
        const now = Date.now();
        const isSpeech = rms >= DIRECT_AUDIO_RMS_THRESHOLD;
        const gained = applyGain(samples, DIRECT_AUDIO_GAIN);
        let startedWithPreroll = false;

        monitor.directAudio.frames++;
        monitor.directAudio.lastAt = now;
        monitor.directAudio.lastRms = Math.round(rms);
        if (!current) {
          if (rms >= DIRECT_AUDIO_NOISE_GATE_RMS) prerollFrames.push({ samples: gained.slice(), rms });
          while (prerollFrames.length > prerollFrameLimit) prerollFrames.shift();
          if (isSpeech) {
            const utterance = startUtterance(now);
            if (DIRECT_AUDIO_LEADING_SILENCE_MS > 0) utterance.encoder.pushPcm(leadingSilenceSamples);
            for (const preroll of prerollFrames) {
              utterance.encoder.pushPcm(preroll.samples);
            }
            prerollFrames.length = 0;
            startedWithPreroll = true;
          }
        }
        if (current) {
          if (isSpeech) current.lastSpeechAt = now;
          if (!startedWithPreroll) current.encoder.pushPcm(noiseGateSamples(gained, rms));
          if (
            now - current.lastSpeechAt >= DIRECT_AUDIO_SILENCE_MS ||
            now - current.startedAt >= DIRECT_AUDIO_MAX_UTTERANCE_MS
          ) {
            endUtterance(now - current.startedAt >= DIRECT_AUDIO_MAX_UTTERANCE_MS ? "max duration" : "silence");
          }
        }
        if (!current && !isSpeech) {
          monitor.directAudio.speaking = false;
          monitor.directAudio.detail = `subscribed to ${participantIdentity} audio`;
        }
        touchMonitor(monitor);
      }
      endUtterance("track ended");
      onSpeechState?.(false);
      monitor.directAudio.subscribed = false;
      monitor.directAudio.speaking = false;
      monitor.directAudio.detail = "OpenAI audio track ended";
      touchMonitor(monitor);
      recordUiEvent(`OpenAI audio track ended (${participantIdentity})`, "info");
    } catch (err) {
      endUtterance("error");
      onSpeechState?.(false);
      monitor.directAudio.subscribed = false;
      monitor.directAudio.speaking = false;
      monitor.directAudio.detail = errMessage(err);
      addMonitorError(monitor, "OpenAI audio routing failed", err);
    }
  })();
}

function dashboardStatus() {
  const now = Date.now();
  const session = currentMonitor();
  const streamsNow = streamSummary();
  const audioFlowing = Boolean(session?.connected && session.audio.lastAt && now - session.audio.lastAt < 2_500);
  const videoFlowing = Boolean(session?.connected && session.video.lastAt && now - session.video.lastAt < 5_000);
  const cameraActive = Boolean(session?.connected && session.rtmp.state === "active");
  const ttsFlowing = Boolean(
    session?.directAudio.speaking ||
      session?.tts.speaking ||
      (session?.tts.lastAt && now - session.tts.lastAt < 5_000),
  );
  const activeSessionCount = [...sessionMonitors.values()].filter((s) => s.connected).length;

  return {
    server: {
      online: true,
      startedAt: serverStartedAt,
      uptimeMs: now - serverStartedAt,
      publicBaseUrl: PUBLIC_BASE_URL,
      updatedAt: now,
    },
    summary: {
      activeSessionCount,
      connected: Boolean(session?.connected),
      liveKitConnected: session?.micBridge.state === "active",
      audioFlowing,
      cameraActive,
      videoFlowing,
      streamFlowing: cameraActive || videoFlowing || streamsNow.active > 0,
      ttsFlowing,
      directAudioEnabled: OPENAI_DIRECT_AUDIO,
      muted: Boolean(session?.muted),
    },
    streams: streamsNow,
    session,
    sessions: [...sessionMonitors.values()].sort((a, b) => b.lastUpdatedAt - a.lastUpdatedAt).slice(0, 5),
    events: uiEvents.slice(0, 12),
  };
}

function dashboardHtml(): string {
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Guidance Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #fff;
      --ink: #050505;
      --muted: #646464;
      --line: #d8d8d8;
      --soft: #f5f5f5;
      --softer: #fafafa;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }
    main {
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 34px 0 44px;
    }
    header {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 20px;
      padding-bottom: 26px;
      border-bottom: 1px solid var(--ink);
    }
    h1 {
      margin: 0;
      font-size: clamp(32px, 5vw, 68px);
      line-height: 0.92;
      font-weight: 760;
      letter-spacing: 0;
    }
    .subhead {
      margin-top: 13px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.45;
      max-width: 560px;
    }
    .topline {
      min-width: 210px;
      display: grid;
      justify-items: end;
      gap: 8px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
    }
    .pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--ink);
      border-radius: 999px;
      padding: 8px 11px;
      color: var(--ink);
      background: var(--bg);
      white-space: nowrap;
      font-size: 12px;
      line-height: 1;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .dot {
      width: 9px;
      height: 9px;
      border: 1px solid var(--ink);
      border-radius: 999px;
      background: var(--bg);
      flex: 0 0 auto;
    }
    [data-state="active"] .dot,
    [data-state="flowing"] .dot,
    .pill[data-state="active"] .dot {
      background: var(--ink);
      animation: pulse 1.2s ease-in-out infinite;
    }
    [data-state="error"] .dot {
      background: var(--ink);
      border-radius: 2px;
      animation: none;
    }
    @keyframes pulse {
      0%, 100% { transform: scale(0.82); opacity: 0.55; }
      50% { transform: scale(1.2); opacity: 1; }
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: var(--bg);
      min-width: 0;
    }
    .card {
      min-height: 178px;
      padding: 18px;
      display: grid;
      align-content: space-between;
      gap: 22px;
    }
    .card[data-state="active"],
    .card[data-state="flowing"] {
      border-color: var(--ink);
    }
    .card[data-state="error"] {
      border-color: var(--ink);
      box-shadow: inset 0 0 0 2px var(--ink);
    }
    .card-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
    }
    .label {
      display: flex;
      align-items: center;
      gap: 9px;
      min-width: 0;
      font-size: 12px;
      text-transform: uppercase;
      color: var(--muted);
      line-height: 1.2;
    }
    .state {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 5px 8px;
      font-size: 11px;
      line-height: 1;
      text-transform: uppercase;
      white-space: nowrap;
      color: var(--ink);
    }
    .card[data-state="active"] .state,
    .card[data-state="flowing"] .state {
      border-color: var(--ink);
      background: var(--ink);
      color: var(--bg);
    }
    .metric {
      margin: 0;
      font-size: clamp(31px, 4.6vw, 54px);
      line-height: 0.96;
      font-weight: 720;
      letter-spacing: 0;
      overflow-wrap: anywhere;
    }
    .detail {
      margin: 8px 0 0;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.38;
      min-height: 36px;
    }
    .bars {
      height: 34px;
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      align-items: end;
      gap: 3px;
    }
    .bars span {
      display: block;
      min-height: 5px;
      background: var(--ink);
      opacity: 0.16;
      transform-origin: bottom;
    }
    .bars.is-flowing span {
      opacity: 1;
      animation: rise 840ms ease-in-out infinite;
    }
    .bars span:nth-child(2n) { height: 62%; animation-delay: 90ms; }
    .bars span:nth-child(3n) { height: 86%; animation-delay: 160ms; }
    .bars span:nth-child(4n) { height: 46%; animation-delay: 230ms; }
    .bars span:nth-child(5n) { height: 100%; animation-delay: 310ms; }
    @keyframes rise {
      0%, 100% { transform: scaleY(0.34); opacity: 0.35; }
      50% { transform: scaleY(1); opacity: 1; }
    }
    .section {
      margin-top: 34px;
      display: grid;
      grid-template-columns: 1.35fr 0.65fr;
      gap: 12px;
      align-items: start;
    }
    .section h2 {
      margin: 0 0 16px;
      font-size: 13px;
      line-height: 1;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .table,
    .events {
      padding: 18px;
    }
    .rows {
      display: grid;
      border-top: 1px solid var(--line);
    }
    .row {
      display: grid;
      grid-template-columns: 150px minmax(0, 1fr);
      gap: 14px;
      padding: 12px 0;
      border-bottom: 1px solid var(--line);
      font-size: 13px;
      line-height: 1.35;
    }
    .row b {
      font-weight: 560;
      color: var(--muted);
    }
    .row span {
      overflow-wrap: anywhere;
    }
    .events-list {
      display: grid;
      gap: 9px;
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .events-list li {
      display: grid;
      grid-template-columns: 68px minmax(0, 1fr);
      gap: 10px;
      align-items: baseline;
      border-bottom: 1px solid var(--line);
      padding-bottom: 9px;
      font-size: 12px;
      line-height: 1.35;
    }
    .events-list time {
      color: var(--muted);
      text-transform: uppercase;
    }
    .controls {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
    }
    button,
    a.button {
      appearance: none;
      border: 1px solid var(--ink);
      border-radius: 8px;
      background: var(--bg);
      color: var(--ink);
      padding: 10px 12px;
      font: inherit;
      font-size: 12px;
      line-height: 1;
      text-transform: uppercase;
      text-decoration: none;
      cursor: pointer;
    }
    button:hover,
    a.button:hover {
      background: var(--ink);
      color: var(--bg);
    }
    button:disabled,
    a.button[aria-disabled="true"] {
      border-color: var(--line);
      color: var(--muted);
      cursor: default;
      background: var(--soft);
    }
    .empty {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
      padding: 18px;
      border: 1px dashed var(--line);
      border-radius: 8px;
      background: var(--softer);
    }
    @media (max-width: 860px) {
      main { width: min(100vw - 22px, 720px); padding-top: 22px; }
      header { display: grid; }
      .topline { justify-items: start; min-width: 0; }
      .grid,
      .section { grid-template-columns: 1fr; }
      .card { min-height: 150px; }
      .row { grid-template-columns: 112px minmax(0, 1fr); }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>Guidance Monitor</h1>
        <div class="subhead" id="subhead">Watching connection, audio, camera, LiveKit, and TTS flow.</div>
      </div>
      <div class="topline">
        <span class="pill" id="server-pill" data-state="active"><span class="dot"></span><span>server online</span></span>
        <span id="updated">updated just now</span>
      </div>
    </header>
    <section class="grid" id="cards"></section>
    <section class="section">
      <div class="panel table">
        <h2>Session Detail</h2>
        <div id="details"></div>
        <div class="controls">
          <button type="button" data-action="/mute">Mute</button>
          <button type="button" data-action="/unmute">Unmute</button>
          <button type="button" data-action="/test-speak">Test speak</button>
          <button type="button" data-action="/test-tone">Test tone</button>
          <button type="button" data-action="/test-stream">Test stream</button>
          <a class="button" id="viewer-link" href="/viewer" target="_blank" rel="noreferrer">Open viewer</a>
        </div>
      </div>
      <div class="panel events">
        <h2>Recent Events</h2>
        <ul class="events-list" id="events"></ul>
      </div>
    </section>
  </main>
  <script>
    const cardsEl = document.getElementById("cards");
    const detailsEl = document.getElementById("details");
    const eventsEl = document.getElementById("events");
    const updatedEl = document.getElementById("updated");
    const subheadEl = document.getElementById("subhead");
    const viewerLink = document.getElementById("viewer-link");
    const controlButtons = Array.from(document.querySelectorAll("button[data-action]"));
    let latestStatus = null;

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, function (char) {
        return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[char];
      });
    }
    function ago(ts) {
      if (!ts) return "never";
      const seconds = Math.max(0, Math.round((Date.now() - ts) / 1000));
      if (seconds < 2) return "now";
      if (seconds < 60) return seconds + "s ago";
      const minutes = Math.round(seconds / 60);
      if (minutes < 60) return minutes + "m ago";
      return Math.round(minutes / 60) + "h ago";
    }
    function duration(ms) {
      const seconds = Math.max(0, Math.floor(ms / 1000));
      const minutes = Math.floor(seconds / 60);
      const hours = Math.floor(minutes / 60);
      if (hours > 0) return hours + "h " + (minutes % 60) + "m";
      if (minutes > 0) return minutes + "m " + (seconds % 60) + "s";
      return seconds + "s";
    }
    function bars(flowing) {
      let html = '<div class="bars ' + (flowing ? "is-flowing" : "") + '">';
      for (let i = 0; i < 12; i++) html += "<span></span>";
      return html + "</div>";
    }
    function card(item) {
      return [
        '<article class="panel card" data-state="' + escapeHtml(item.state) + '">',
        '<div class="card-top">',
        '<div class="label"><span class="dot"></span><span>' + escapeHtml(item.label) + '</span></div>',
        '<span class="state">' + escapeHtml(item.stateLabel) + '</span>',
        '</div>',
        '<div>',
        '<p class="metric">' + escapeHtml(item.metric) + '</p>',
        '<p class="detail">' + escapeHtml(item.detail) + '</p>',
        item.meter ? bars(item.state === "flowing" || item.state === "active") : "",
        '</div>',
        '</article>'
      ].join("");
    }
    function setControls(enabled, session) {
      controlButtons.forEach(function (button) {
        const action = button.getAttribute("data-action");
        button.disabled = !enabled || (action === "/mute" && session && session.muted) || (action === "/unmute" && session && !session.muted);
      });
      if (session && session.roomName) {
        viewerLink.href = "/viewer?room=" + encodeURIComponent(session.roomName);
        viewerLink.setAttribute("aria-disabled", "false");
      } else {
        viewerLink.href = "/viewer";
        viewerLink.setAttribute("aria-disabled", "true");
      }
    }
    function renderDetails(status) {
      const s = status.session;
      if (!s) {
        detailsEl.innerHTML = '<div class="empty">No Mentra session has connected yet. Start the app on the glasses and this panel will fill in live.</div>';
        return;
      }
      const rows = [
        ["User", s.userId],
        ["Session", s.connected ? "connected since " + ago(s.connectedAt) : "disconnected " + ago(s.disconnectedAt)],
        ["Room", s.roomName],
        ["Muted", s.muted ? "yes - responses paused" : "no"],
        ["Ingress", s.ingressId || "not created"],
        ["Dispatch", s.dispatchId || "not created"],
        ["Mic chunks", s.audio.chunks + " chunks, " + s.audio.bytes + " bytes"],
        ["Camera", s.rtmp.state + (s.rtmp.detail ? " - " + s.rtmp.detail : "")],
        ["Video", s.video.frames + " observed frames"],
        ["Audio out", s.directAudio.enabled
          ? s.directAudio.streams + " OpenAI streams, rms " + s.directAudio.lastRms
          : s.tts.messages + " TTS messages, " + s.tts.queued + " queued"]
      ];
      if (s.errors && s.errors.length) rows.push(["Last error", s.errors[0]]);
      detailsEl.innerHTML = '<div class="rows">' + rows.map(function (row) {
        return '<div class="row"><b>' + escapeHtml(row[0]) + '</b><span>' + escapeHtml(row[1]) + '</span></div>';
      }).join("") + '</div>';
    }
    function renderEvents(events) {
      if (!events.length) {
        eventsEl.innerHTML = '<li><time>idle</time><span>No events yet.</span></li>';
        return;
      }
      eventsEl.innerHTML = events.map(function (event) {
        return '<li data-level="' + escapeHtml(event.level) + '"><time>' + escapeHtml(ago(event.at)) + '</time><span>' + escapeHtml(event.message) + '</span></li>';
      }).join("");
    }
    function render(status) {
      latestStatus = status;
      const s = status.session;
      const summary = status.summary;
      const active = summary.connected;
      const audioState = summary.audioFlowing ? "flowing" : (s && s.audio.chunks > 0 ? "quiet" : "idle");
      const cameraState = summary.cameraActive ? "active" : (s && s.rtmp.state === "error" ? "error" : "idle");
      const videoState = summary.videoFlowing ? "flowing" : (s && s.video.frames > 0 ? "quiet" : "idle");
      const ttsState = summary.ttsFlowing
        ? "flowing"
        : (summary.muted ? "quiet" : (s && (s.directAudio.frames > 0 || s.tts.messages > 0) ? "quiet" : "idle"));
      updatedEl.textContent = "updated " + ago(status.server.updatedAt) + " - uptime " + duration(status.server.uptimeMs);
      subheadEl.textContent = active
        ? "Live session for " + s.userId + " - " + status.streams.active + " chunked stream(s) open"
        : "Waiting for a Mentra session - server is ready";
      cardsEl.innerHTML = [
        card({
          label: "Connection",
          state: active ? "active" : "idle",
          stateLabel: active ? "connected" : "waiting",
          metric: active ? "Online" : "Idle",
          detail: active ? "Session " + s.sessionId + " connected " + ago(s.connectedAt) : "No active glasses session.",
          meter: false
        }),
        card({
          label: "Mic audio",
          state: audioState,
          stateLabel: summary.audioFlowing ? "flowing" : audioState,
          metric: s ? String(s.audio.chunks) : "0",
          detail: s ? "Last chunk " + ago(s.audio.lastAt) + " at " + (s.audio.sampleRate || 16000) + " Hz" : "Waiting for PCM chunks.",
          meter: true
        }),
        card({
          label: "Camera stream",
          state: cameraState,
          stateLabel: summary.cameraActive ? "active" : cameraState,
          metric: s ? String(s.rtmp.state) : "idle",
          detail: s ? (s.rtmp.detail || "Last status " + ago(s.rtmp.lastAt)) : "Waiting for RTMP status.",
          meter: true
        }),
        card({
          label: "LiveKit bridge",
          state: summary.liveKitConnected ? "active" : (s && s.micBridge.state === "error" ? "error" : "idle"),
          stateLabel: summary.liveKitConnected ? "joined" : (s ? s.micBridge.state : "idle"),
          metric: summary.liveKitConnected ? "Ready" : "Idle",
          detail: s ? s.micBridge.detail : "Mic bridge has not joined a room.",
          meter: false
        }),
        card({
          label: "Video forward",
          state: videoState,
          stateLabel: summary.videoFlowing ? "flowing" : videoState,
          metric: s ? String(s.video.frames) : "0",
          detail: s ? "Last observed frame " + ago(s.video.lastAt) : "Waiting for glasses video.",
          meter: true
        }),
        card({
          label: summary.directAudioEnabled ? "OpenAI audio" : "TTS out",
          state: ttsState,
          stateLabel: summary.muted ? "muted" : (summary.ttsFlowing ? "flowing" : ttsState),
          metric: summary.muted ? "Muted" : (s ? String(s.directAudio.enabled ? s.directAudio.streams : s.tts.messages) : "0"),
          detail: s
            ? (s.muted ? "Responses are paused; mic context still flows." : (s.directAudio.enabled ? s.directAudio.detail : (s.tts.lastText ? s.tts.lastText.slice(0, 80) : "Waiting for guidance.tts data.")))
            : (summary.directAudioEnabled ? "Waiting for OpenAI audio." : "Waiting for guidance.tts data."),
          meter: true
        })
      ].join("");
      renderDetails(status);
      renderEvents(status.events);
      setControls(active, s);
    }
    async function refresh() {
      try {
        const res = await fetch("/status", { cache: "no-store" });
        if (!res.ok) throw new Error("status " + res.status);
        render(await res.json());
      } catch (err) {
        updatedEl.textContent = "status unavailable";
        cardsEl.innerHTML = card({
          label: "Server",
          state: "error",
          stateLabel: "error",
          metric: "Offline",
          detail: err.message || String(err),
          meter: false
        });
      }
    }
    controlButtons.forEach(function (button) {
      button.addEventListener("click", async function () {
        const action = button.getAttribute("data-action");
        button.disabled = true;
        try {
          await fetch(action, { cache: "no-store" });
          await refresh();
        } finally {
          setControls(Boolean(latestStatus && latestStatus.summary.connected), latestStatus && latestStatus.session);
        }
      });
    });
    refresh();
    setInterval(refresh, 900);
  </script>
</body>
</html>`;
}

const ingressClient = new IngressClient(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET);
const dispatchClient = new AgentDispatchClient(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET);
const roomServiceClient = new RoomServiceClient(LIVEKIT_URL, LIVEKIT_API_KEY, LIVEKIT_API_SECRET);
const AGENT_NAME = "guidance";

const roomNameFor = (userId: string) => `guidance-${userId.replace(/[^a-zA-Z0-9_-]/g, "_")}`;

// Wipe all stale state for a room: queued agent dispatches, RTMP ingresses, and the
// room itself (kicks any lingering participants — old agents, mic-bridges, viewers).
async function resetRoomState(roomName: string) {
  try {
    const dispatches = await dispatchClient.listDispatch(roomName);
    for (const d of dispatches) {
      try {
        await dispatchClient.deleteDispatch(d.id, roomName);
        console.log(`[livekit] cleaned stale dispatch ${d.id}`);
      } catch {}
    }
  } catch (err) {
    console.error("[livekit] listDispatch failed:", err);
  }
  try {
    const ingresses = await ingressClient.listIngress({ roomName });
    for (const i of ingresses) {
      try {
        await ingressClient.deleteIngress(i.ingressId);
        console.log(`[livekit] cleaned stale ingress ${i.ingressId}`);
      } catch {}
    }
  } catch (err) {
    console.error("[livekit] listIngress failed:", err);
  }
  try {
    await roomServiceClient.deleteRoom(roomName);
    console.log(`[livekit] deleted room ${roomName}`);
  } catch {
    // room may not exist yet — fine
  }
}

type ActiveSessionControl = {
  mute: (reason?: string) => void;
  unmute: (reason?: string) => void;
  toggleMute: (reason?: string) => boolean;
  isMuted: () => boolean;
};

// Active sessions, keyed by userId, so we can trigger test playback via HTTP.
const activeSessions = new Map<string, AppSession>();
const activeSessionControls = new Map<string, ActiveSessionControl>();

function currentSessionControl(): ActiveSessionControl | undefined {
  const monitor = currentMonitor();
  if (monitor) {
    const control = activeSessionControls.get(monitor.userId);
    if (control) return control;
  }
  return activeSessionControls.values().next().value;
}

class StreamApp extends AppServer {
  protected async onSession(session: AppSession, sessionId: string, userId: string) {
    console.log(`[session] connected ${sessionId} user=${userId}`);
    session.layouts.showTextWall("Connecting to LiveKit...");
    activeSessions.set(userId, session);

    const roomName = roomNameFor(userId);
    const monitor = createSessionMonitor(sessionId, userId, roomName);
    sessionMonitors.set(userId, monitor);
    recordUiEvent(`Session connected for ${userId}`, "good");
    await resetRoomState(roomName);

    let ingressId: string | undefined;
    let rtmpUrl: string | undefined;
    if (ENABLE_VIDEO_STREAM) {
      try {
        const ingress = await ingressClient.createIngress(IngressInput.RTMP_INPUT, {
          name: `glasses-${userId}-${Date.now()}`,
          roomName,
          participantIdentity: "glasses",
          participantName: "Glasses",
          enableTranscoding: true,
        });
        ingressId = ingress.ingressId;
        // The LiveKit ingress URL is rtmps://. By default we downgrade to plain rtmp://
        // (Mentra Live historically only spoke RTMP); set RTMP_USE_TLS=1 to keep rtmps://.
        const ingressBase = ingress.url.replace(/^rtmps?:\/\//, "");
        rtmpUrl = `${RTMP_USE_TLS ? "rtmps" : "rtmp"}://${ingressBase}/${ingress.streamKey}`;
        console.log(`[livekit] room=${roomName} ingress=${ingressId} tls=${RTMP_USE_TLS}`);
        console.log(`[livekit] rtmp=${rtmpUrl}`);
        monitor.ingressId = ingressId;
        monitor.rtmp.state = "idle";
        monitor.rtmp.detail = "video ingress ready";
        touchMonitor(monitor);
        recordUiEvent(`LiveKit video ingress ready for ${roomName}`, "good");
      } catch (err) {
        console.error("[livekit] createIngress failed:", err);
        monitor.rtmp.state = "error";
        monitor.rtmp.detail = errMessage(err);
        addMonitorError(monitor, "LiveKit ingress failed", err);
        session.layouts.showTextWall("LiveKit ingress failed");
        return;
      }
    } else {
      monitor.rtmp.state = "idle";
      monitor.rtmp.detail = "video disabled by ENABLE_VIDEO_STREAM";
      touchMonitor(monitor);
    }

    let dispatchId: string | undefined;
    try {
      const dispatch = await dispatchClient.createDispatch(roomName, AGENT_NAME);
      dispatchId = dispatch.id;
      console.log(`[livekit] agent dispatched: id=${dispatchId}`);
      monitor.dispatchId = dispatchId;
      touchMonitor(monitor);
      recordUiEvent(`Agent dispatched to ${roomName}`, "good");
    } catch (err) {
      console.error("[livekit] createDispatch failed:", err);
      addMonitorError(monitor, "Agent dispatch failed", err);
    }

    // mic-bridge: publish Mentra's raw 16 kHz PCM mic chunks directly into the room
    // as a server-side audio track. Video is always-on: once the room is ready we
    // start the glasses camera stream and leave it running for the whole session.
    let micRoom: Room | undefined;
    let micAudioTrack: LocalAudioTrack | undefined;
    let micAudioSource: AudioSource | undefined;
    let glassesVideoStream: VideoStream | undefined;
    let assistantAudioSpeaking = false;
    let suppressMicCaptureUntil = 0;
    let suppressedEchoMicChunks = 0;
    try {
      const micToken = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, {
        identity: "mic-bridge",
        ttl: "2h",
      });
      micToken.addGrant({ roomJoin: true, room: roomName, canPublish: true, canSubscribe: true });
      const tokenStr = await micToken.toJwt();
      micRoom = new Room();
      await micRoom.connect(LIVEKIT_URL, tokenStr);
      micAudioSource = new AudioSource(16000, 1);
      micAudioTrack = LocalAudioTrack.createAudioTrack("mic", micAudioSource);
      const aopts = new TrackPublishOptions();
      aopts.source = TrackSource.SOURCE_MICROPHONE;
      await micRoom.localParticipant!.publishTrack(micAudioTrack, aopts);
      console.log("[livekit] mic-bridge connected and publishing audio");
      monitor.micBridge.state = "active";
      monitor.micBridge.detail = "publishing server-side microphone track";
      monitor.micBridge.lastAt = Date.now();
      touchMonitor(monitor);
      recordUiEvent("Mic bridge joined LiveKit and published audio", "good");

      if (!OPENAI_DIRECT_AUDIO) {
        // Pre-warm Mentra -> ElevenLabs path so the first real reply skips the cold start
        // when running the text-to-TTS fallback.
        session.audio
          .speak(".", { volume: 0.01, stopOtherAudio: false, model_id: "eleven_flash_v2_5" })
          .then(() => console.log("[prewarm] speak() warmup done"))
          .catch((err) => console.error("[prewarm] speak() warmup err:", err?.message ?? err));
      }

      // Assistant text → Mentra TTS via session.audio.speak(). The agent runs in
      // text-only modality (modalities=["text"]) and publishes each reply on the
      // "guidance.tts" data topic. We use speak() instead of playAudio because the
      // latter has unfixable Mentra Cloud queue bugs; speak() works reliably.
      const speakQueue: string[] = [];
      let speaking = false;
      let speakGeneration = 0;
      let activeSpeakGeneration = 0;
      let suppressTtsUntil = 0;
      let muted = false;
      let lastQueuedTtsText = "";
      let lastQueuedTtsAt = 0;
      let videoRequested = false;
      let videoReady = false;
      let currentVideoRequestId = "";
      let videoStartInFlight: Promise<void> | undefined;
      let videoRestartTimer: ReturnType<typeof setTimeout> | undefined;
      let videoRestartCount = 0;
      // Glasses RTMP video only works on WiFi. WiFi state arrives via two cloud signals
      // (device-state observable + glasses_connection_state); treat WiFi as up if either
      // reports connected so the gate and the trigger stay consistent.
      const wifiUp = () =>
        session.isWifiConnected() || session.device.state.wifiConnected.value === true;

      const publishVideoStatus = (
        status: string,
        detail: string,
        requestId = currentVideoRequestId,
        extra: Record<string, unknown> = {},
      ) => {
        if (!micRoom?.localParticipant) return;
        const payload = {
          type: "video_status",
          status,
          detail,
          requestId,
          frames: monitor.video.frames,
          rtmpState: monitor.rtmp.state,
          ...extra,
        };
        micRoom.localParticipant
          .publishData(new TextEncoder().encode(JSON.stringify(payload)), {
            reliable: true,
            topic: "guidance.video_status",
          })
          .catch((err) => addMonitorError(monitor, "Video status publish failed", err));
      };

      const closeVideoForward = async (detail: string) => {
        try {
          if (glassesVideoStream) await glassesVideoStream.cancel();
        } catch (err) {
          console.error("[livekit] glasses video stream close failed:", err);
        }
        glassesVideoStream = undefined;
        monitor.video.forwarding = false;
        monitor.video.state = "idle";
        monitor.video.detail = detail;
        touchMonitor(monitor);
      };

      let startVideoContext: (requestId: string, reason: string) => Promise<void>;

      // Single calm restart with backoff if the managed stream fails or dies. Capped per
      // session so a persistent device fault can't loop forever (and can't keep spawning
      // orphaned streams). A surviving stream simply never triggers it.
      const scheduleManagedRestart = (reason: string) => {
        if (videoRestartTimer) return;
        if (!videoRequested || !monitor.connected) return;
        if (videoRestartCount >= MAX_VIDEO_RESTARTS) {
          console.warn(`[managed] gave up after ${videoRestartCount} restarts (${reason})`);
          addMonitorError(monitor, "Managed stream gave up", `${videoRestartCount} restarts: ${reason}`);
          return;
        }
        videoRestartCount++;
        console.log(`[managed] restart ${videoRestartCount} scheduled in ${VIDEO_RESTART_BACKOFF_MS}ms (${reason})`);
        videoRestartTimer = setTimeout(() => {
          videoRestartTimer = undefined;
          if (videoRequested && monitor.connected && !monitor.video.forwarding && !videoStartInFlight && wifiUp()) {
            startVideoContext(crypto.randomUUID(), `restart ${videoRestartCount}`).catch((e) =>
              addMonitorError(monitor, "Managed restart failed", e),
            );
          }
        }, VIDEO_RESTART_BACKOFF_MS);
      };

      startVideoContext = async (requestId: string, reason: string) => {
        currentVideoRequestId = requestId || crypto.randomUUID();
        if (!ENABLE_VIDEO_STREAM || !rtmpUrl) {
          monitor.rtmp.state = "idle";
          monitor.rtmp.detail = "video unavailable";
          touchMonitor(monitor);
          publishVideoStatus("unavailable", "video streaming is disabled", currentVideoRequestId);
          return;
        }

        videoRequested = true;
        videoReady = false;
        if (monitor.video.forwarding && monitor.video.frames > 0) {
          videoReady = true;
          publishVideoStatus("ready", "video is already flowing", currentVideoRequestId);
          return;
        }
        if (videoStartInFlight) {
          publishVideoStatus("starting", "video start already in progress", currentVideoRequestId);
          await videoStartInFlight;
          return;
        }

        monitor.rtmp.state = "pending";
        monitor.rtmp.detail = reason || "starting always-on video";
        monitor.rtmp.startRequestedAt = Date.now();
        monitor.video.state = "pending";
        monitor.video.detail = "waiting for glasses video";
        monitor.video.frames = 0;
        touchMonitor(monitor);
        publishVideoStatus("starting", monitor.rtmp.detail, currentVideoRequestId);
        recordUiEvent("Starting always-on video stream", "info");

        videoStartInFlight = (async () => {
          try {
            // Unmanaged RTMP straight to our LiveKit ingress: the glasses push video directly,
            // it lands as the "glasses" participant, and the agent's GlassesVideoInput consumes
            // it (audio comes from the mic-bridge). This is the original working setup — it only
            // broke when the glasses' WiFi hijacked DNS so they couldn't reach the ingress.
            await session.camera.startStream({
              rtmpUrl,
              video: { width: 640, height: 480, bitrate: 500_000 },
            });
            console.log(`[rtmp] startStream requested (${currentVideoRequestId}) -> ${rtmpUrl}`);
            monitor.rtmp.state = "pending";
            monitor.rtmp.detail = "camera stream starting";
            monitor.rtmp.startRequestedAt = Date.now();
            touchMonitor(monitor);
            publishVideoStatus("requested", "camera stream requested", currentVideoRequestId);
          } catch (err) {
            console.error("[rtmp] startStream failed:", err);
            monitor.rtmp.state = "error";
            monitor.rtmp.detail = errMessage(err);
            monitor.video.state = "error";
            monitor.video.detail = errMessage(err);
            addMonitorError(monitor, "Managed stream start failed", err);
            publishVideoStatus("error", errMessage(err), currentVideoRequestId);
            scheduleManagedRestart("start failed");
          } finally {
            videoStartInFlight = undefined;
          }
        })();

        await videoStartInFlight;
      };

      if (ENABLE_VIDEO_STREAM && rtmpUrl) {
        session.camera.onStreamStatus((status) => {
          const errorDetails = (status as any).errorDetails ?? "";
          const detail = errorDetails || `camera stream ${status.status}`;
          console.log(`[rtmp] status=${status.status} ${errorDetails}`);
          monitor.rtmp.state = status.status;
          monitor.rtmp.detail = detail;
          monitor.rtmp.lastAt = Date.now();
          touchMonitor(monitor);
          publishVideoStatus(status.status, detail, currentVideoRequestId);
          if (status.status === "active") {
            if (videoRestartTimer) { clearTimeout(videoRestartTimer); videoRestartTimer = undefined; }
            recordUiEvent("Camera stream is active", "good");
            session.layouts.showTextWall("Video context active");
          }
          if (status.status === "error") {
            addMonitorError(monitor, "Camera stream error", detail);
            session.layouts.showTextWall(`Stream error: ${detail}`);
          }
          // Stream died (not during our own start) -> one calm, capped restart.
          if ((status.status === "stopped" || status.status === "error") && !videoStartInFlight) {
            scheduleManagedRestart(`stream ${status.status}`);
          }
        });
      }

      // Always-on video context: keep the glasses camera stream running for the whole
      // session so the agent continuously has live visual context. Mentra Live can only
      // RTMP-stream over WiFi, and sending a camera command while WiFi is down makes the
      // cloud close the session (code 1008). So we gate the stream on WiFi: start it once
      // the glasses are on WiFi, and (re)start whenever WiFi comes back.
      if (ENABLE_VIDEO_STREAM && rtmpUrl) {
        // wifiUp() is defined above. We react to BOTH WiFi signals (device-state observable
        // and glasses_connection_state) so that whichever the cloud delivers, the gate and the
        // trigger stay consistent and we never send a camera command while WiFi is down (which
        // would close the session with code 1008).
        const ensureVideoStarted = (trigger: string) => {
          if (!wifiUp()) return;
          if (videoRequested || monitor.video.forwarding || videoStartInFlight) return;
          startVideoContext(crypto.randomUUID(), `always-on video (${trigger})`).catch((err) =>
            addMonitorError(monitor, "Always-on video start failed", err),
          );
        };

        const onWifiState = (trigger: string) => {
          if (wifiUp()) {
            ensureVideoStarted(trigger);
            return;
          }
          // WiFi is down: do NOT send camera commands. Reset local state so we cleanly
          // restart once WiFi returns. closeVideoForward only tears down our local LiveKit
          // consumer — it sends no camera command to the glasses.
          videoRequested = false;
          videoReady = false;
          monitor.rtmp.state = "idle";
          monitor.rtmp.detail = "waiting for glasses WiFi";
          touchMonitor(monitor);
          publishVideoStatus("unavailable", "glasses not on WiFi", currentVideoRequestId);
          closeVideoForward("glasses left WiFi; video paused").catch((err) =>
            addMonitorError(monitor, "Video forward close failed", err),
          );
        };

        session.device.state.wifiConnected.onChange(() => onWifiState("device wifi update"));
        session.onGlassesConnectionState(() => onWifiState("glasses connection update"));
        // Try immediately in case WiFi is already up at connect.
        onWifiState("session ready");

        // If WiFi still isn't up shortly after connect, prompt the user to set it up.
        setTimeout(() => {
          if (!monitor.connected || wifiUp() || monitor.video.forwarding || videoStartInFlight) return;
          // requestWifiSetup/showTextWall throw if the WebSocket already closed — guard so
          // a disconnect within the grace window can't crash an unhandled timer.
          try {
            session.requestWifiSetup("Video guidance needs the glasses connected to WiFi");
            recordUiEvent("Glasses not on WiFi; requested WiFi setup", "warn");
            session.layouts.showTextWall("Connect glasses to WiFi to enable video");
          } catch (err) {
            console.error("[wifi] requestWifiSetup failed:", errMessage(err));
          }
        }, 6_000);
      }

      const publishAgentControl = (payload: { type: "mute" | "unmute" | "interrupt"; reason?: string }) => {
        if (!micRoom?.localParticipant) return;
        micRoom.localParticipant
          .publishData(new TextEncoder().encode(JSON.stringify(payload)), {
            reliable: true,
            topic: "guidance.control",
          })
          .catch((err) => addMonitorError(monitor, "Agent control publish failed", err));
      };
      const drainSpeak = async () => {
        if (speaking) return;
        const generation = speakGeneration;
        activeSpeakGeneration = generation;
        speaking = true;
        assistantAudioSpeaking = true;
        monitor.tts.speaking = true;
        monitor.tts.queued = speakQueue.length;
        touchMonitor(monitor);
        try {
          while (speakQueue.length > 0 && generation === speakGeneration) {
            const text = speakQueue.shift()!;
            monitor.tts.queued = speakQueue.length;
            touchMonitor(monitor);
            const t0 = Date.now();
            console.log(`[speak] → "${text}"`);
            suppressMicCaptureUntil = Math.max(
              suppressMicCaptureUntil,
              Date.now() + estimateTtsPlaybackMs(text) + MIC_ECHO_SUPPRESS_AFTER_MS,
            );
            try {
              const r = await session.audio.speak(text, {
                volume: 1.0,
                stopOtherAudio: true,
                model_id: "eleven_flash_v2_5",
                voice_settings: { speed: 0.92 },
              });
              if (generation !== speakGeneration) {
                console.log(`[speak] stopped by button after ${Date.now() - t0}ms`);
                break;
              }
              console.log(`[speak] ✓ ${Date.now() - t0}ms success=${r?.success}`);
              suppressMicCaptureUntil = Math.max(suppressMicCaptureUntil, Date.now() + MIC_ECHO_SUPPRESS_AFTER_MS);
              monitor.tts.lastSpeakSuccess = Boolean(r?.success);
              monitor.tts.lastSpeakLatencyMs = Date.now() - t0;
              touchMonitor(monitor);
            } catch (err: any) {
              if (generation !== speakGeneration) {
                console.log(`[speak] stopped by button with pending request: ${err?.message ?? err}`);
                break;
              }
              console.error("[speak] err:", err?.message ?? err);
              monitor.tts.lastSpeakSuccess = false;
              addMonitorError(monitor, "Mentra speak failed", err);
            }
          }
        } finally {
          if (activeSpeakGeneration === generation) {
            speaking = false;
            assistantAudioSpeaking = false;
            suppressMicCaptureUntil = Math.max(suppressMicCaptureUntil, Date.now() + MIC_ECHO_SUPPRESS_AFTER_MS);
            monitor.tts.speaking = false;
            monitor.tts.queued = speakQueue.length;
            touchMonitor(monitor);
            if (speakQueue.length > 0) drainSpeak();
          }
        }
      };
      const interruptAssistant = (reason: string, publishButtonInterrupt = true) => {
        speakGeneration++;
        activeSpeakGeneration = speakGeneration;
        suppressTtsUntil = Date.now() + 1_500;
        suppressMicCaptureUntil = Math.max(suppressMicCaptureUntil, Date.now() + MIC_ECHO_SUPPRESS_AFTER_MS);
        const dropped = speakQueue.length;
        speakQueue.length = 0;
        speaking = false;
        assistantAudioSpeaking = false;
        monitor.tts.speaking = false;
        monitor.tts.queued = 0;
        monitor.directAudio.speaking = false;
        if (OPENAI_DIRECT_AUDIO) monitor.directAudio.detail = `interrupted: ${reason}`;
        touchMonitor(monitor);
        console.log(`[interrupt] ${reason}; dropped=${dropped}`);
        recordUiEvent(`Interrupt: ${reason}`, "warn");
        try {
          session.audio.stopAudio();
        } catch (err) {
          addMonitorError(monitor, "Audio stop failed", err);
        }
        if (publishButtonInterrupt && micRoom?.localParticipant) {
          micRoom.localParticipant
            .publishData(new TextEncoder().encode(reason), {
              reliable: true,
              topic: "guidance.button_interrupt",
            })
            .catch((err) => addMonitorError(monitor, "Button interrupt publish failed", err));
        }
      };
      const setMuted = (nextMuted: boolean, reason = "dashboard") => {
        if (muted === nextMuted) {
          monitor.muted = muted;
          touchMonitor(monitor);
          return;
        }
        muted = nextMuted;
        monitor.muted = muted;
        touchMonitor(monitor);
        if (muted) {
          interruptAssistant(`mute:${reason}`, false);
          publishAgentControl({ type: "mute", reason });
          recordUiEvent(`Muted agent output (${reason})`, "warn");
        } else {
          publishAgentControl({ type: "unmute", reason });
          recordUiEvent(`Unmuted agent output (${reason})`, "good");
        }
      };
      activeSessionControls.set(userId, {
        mute: (reason = "api") => setMuted(true, reason),
        unmute: (reason = "api") => setMuted(false, reason),
        toggleMute: (reason = "api") => {
          setMuted(!muted, reason);
          return muted;
        },
        isMuted: () => muted,
      });
      const queueSpeak = (text: string) => {
        const trimmed = text.trim();
        if (!trimmed) return;
        if (muted) {
          console.log(`[tts] suppressed while muted: ${JSON.stringify(trimmed)}`);
          monitor.tts.queued = 0;
          touchMonitor(monitor);
          return;
        }
        if (isSilentTtsPlaceholder(trimmed)) {
          console.log(`[tts] suppressed silent placeholder: ${JSON.stringify(trimmed)}`);
          recordUiEvent("Suppressed silent TTS placeholder", "info");
          return;
        }
        if (Date.now() < suppressTtsUntil) {
          console.log(`[tts] suppressed after button interrupt: ${JSON.stringify(trimmed)}`);
          recordUiEvent("Suppressed post-button TTS", "info");
          return;
        }
        const now = Date.now();
        const ttsKey = trimmed.toLowerCase().replace(/\s+/g, " ");
        if (ttsKey === lastQueuedTtsText && now - lastQueuedTtsAt < 4_000) {
          console.log(`[tts] suppressed duplicate payload: ${JSON.stringify(trimmed)}`);
          recordUiEvent("Suppressed duplicate TTS payload", "info");
          return;
        }
        lastQueuedTtsText = ttsKey;
        lastQueuedTtsAt = now;
        speakQueue.push(trimmed);
        monitor.tts.queued = speakQueue.length;
        touchMonitor(monitor);
        drainSpeak();
      };

      session.events.onButtonPress((button) => {
        interruptAssistant(`${button.buttonId}:${button.pressType}`);
      });

      micRoom.on(RoomEvent.DataReceived, (data, participant, _kind, topic) => {
        if (topic !== "guidance.tts") return;
        if (OPENAI_DIRECT_AUDIO) {
          console.log("[tts] ignored guidance.tts because OPENAI_DIRECT_AUDIO=1");
          return;
        }
        const text = new TextDecoder().decode(data);
        console.log(`[tts] received from ${participant?.identity ?? "?"} (${text.length} chars)`);
        if (isSilentTtsPlaceholder(text)) {
          console.log(`[tts] suppressed silent placeholder from ${participant?.identity ?? "?"}: ${JSON.stringify(text.trim())}`);
          monitor.tts.lastAt = Date.now();
          monitor.tts.lastText = "";
          touchMonitor(monitor);
          recordUiEvent("Silent TTS placeholder suppressed", "info");
          return;
        }
        if (muted) {
          console.log(`[tts] ignored while muted from ${participant?.identity ?? "?"}: ${JSON.stringify(text.trim())}`);
          monitor.tts.lastAt = Date.now();
          monitor.tts.lastText = "";
          monitor.tts.queued = 0;
          touchMonitor(monitor);
          return;
        }
        monitor.tts.messages++;
        monitor.tts.lastAt = Date.now();
        monitor.tts.lastText = text.trim();
        touchMonitor(monitor);
        recordUiEvent(`TTS message received from ${participant?.identity ?? "agent"}`, "info");
        queueSpeak(text);
      });

      micRoom.on(RoomEvent.TrackSubscribed, (track, _pub, participant) => {
        if (
          OPENAI_DIRECT_AUDIO &&
          participant.identity !== "glasses" &&
          participant.identity !== "mic-bridge" &&
          track.kind === TrackKind.KIND_AUDIO &&
          !monitor.directAudio.subscribed
        ) {
          console.log(`[openai-audio] subscribed to participant=${participant.identity}`);
          routeOpenAiAudioToMentra(track as RemoteAudioTrack, participant.identity, session, monitor, (isSpeaking) => {
            assistantAudioSpeaking = isSpeaking;
            if (!isSpeaking) {
              suppressMicCaptureUntil = Math.max(suppressMicCaptureUntil, Date.now() + MIC_ECHO_SUPPRESS_AFTER_MS);
            }
          });
          return;
        }

        // Glasses video stays on the original glasses track. The Python agent subscribes
        // directly to that track; this stream only confirms the first real frame arrived.
        if (ENABLE_VIDEO_STREAM && videoRequested && participant.identity === "glasses" && track.kind === TrackKind.KIND_VIDEO && !glassesVideoStream) {
          (async () => {
            // Capture our own stream handle so that if WiFi cycles and a newer observer
            // replaces glassesVideoStream, this observer's finally won't cancel the newer
            // one — it only acts while it still owns the global handle.
            let localStream: VideoStream | undefined;
            try {
              monitor.video.state = "pending";
              monitor.video.detail = "subscribed to glasses video";
              touchMonitor(monitor);
              monitor.video.forwarding = true;
              monitor.video.state = "active";
              monitor.video.detail = "glasses video flowing directly to agent";
              monitor.video.lastAt = Date.now();
              touchMonitor(monitor);
              recordUiEvent("Glasses video direct stream started", "good");
              localStream = new VideoStream(track as RemoteVideoTrack);
              glassesVideoStream = localStream;
              let observedFrames = 0;
              for await (const _evt of localStream) {
                if (!videoRequested) break;
                observedFrames++;
                monitor.video.frames = observedFrames;
                monitor.video.lastAt = Date.now();
                touchMonitor(monitor);
                if (observedFrames === 1 || observedFrames % 200 === 0) {
                  console.log(`[livekit] observed ${observedFrames} glasses video frames`);
                }
                if (observedFrames === VIDEO_READY_FRAMES) {
                  videoReady = true;
                  publishVideoStatus("ready", `${VIDEO_READY_FRAMES} direct glasses video frames observed`, currentVideoRequestId);
                }
              }
            } catch (err) {
              console.error("[livekit] video observation failed:", err);
              monitor.video.state = "error";
              monitor.video.detail = errMessage(err);
              addMonitorError(monitor, "Video observation failed", err);
              publishVideoStatus("error", errMessage(err), currentVideoRequestId);
            } finally {
              if (!videoRequested && glassesVideoStream === localStream) {
                await closeVideoForward("video off; waiting for agent request");
              }
            }
          })();
          return;
        }
      });
    } catch (err) {
      console.error("[livekit] mic-bridge setup failed:", err);
      monitor.micBridge.state = "error";
      monitor.micBridge.detail = errMessage(err);
      monitor.micBridge.lastAt = Date.now();
      addMonitorError(monitor, "Mic bridge setup failed", err);
    }

    let audioChunks = 0;
    session.events.onAudioChunk(async (chunk) => {
      audioChunks++;
      if (audioChunks === 1) console.log(`[audio] first mic chunk arrived (${chunk.arrayBuffer.byteLength}B @ ${chunk.sampleRate ?? 16000}Hz)`);
      monitor.audio.chunks = audioChunks;
      monitor.audio.bytes += chunk.arrayBuffer.byteLength;
      monitor.audio.sampleRate = chunk.sampleRate ?? 16000;
      monitor.audio.lastAt = Date.now();
      touchMonitor(monitor);
      if (audioChunks === 1) recordUiEvent("First mic audio chunk received", "good");
      if (!micAudioSource) return;
      if (assistantAudioSpeaking || Date.now() < suppressMicCaptureUntil) {
        suppressedEchoMicChunks++;
        if (suppressedEchoMicChunks === 1 || suppressedEchoMicChunks % 100 === 0) {
          console.log(`[mic-bridge] suppressed assistant echo chunks=${suppressedEchoMicChunks}`);
        }
        return;
      }
      suppressedEchoMicChunks = 0;
      try {
        const buf = Buffer.from(chunk.arrayBuffer);
        const samples = buf.byteLength >> 1;
        const pcm = new Int16Array(buf.buffer, buf.byteOffset, samples);
        await micAudioSource.captureFrame(new AudioFrame(pcm, chunk.sampleRate ?? 16000, 1, samples));
      } catch (err) {
        if (audioChunks % 200 === 0) console.error("[mic-bridge] captureFrame error:", err);
        monitor.audio.captureErrors++;
        touchMonitor(monitor);
      }
    });

    session.events.onDisconnected(async () => {
      activeSessions.delete(userId);
      activeSessionControls.delete(userId);
      console.log(`[session] disconnected ${sessionId} audioChunks=${audioChunks}`);
      monitor.connected = false;
      monitor.disconnectedAt = Date.now();
      monitor.rtmp.state = "disconnected";
      monitor.micBridge.state = "disconnected";
      monitor.video.state = "disconnected";
      monitor.tts.speaking = false;
      monitor.directAudio.speaking = false;
      monitor.directAudio.subscribed = false;
      monitor.directAudio.detail = "session disconnected";
      touchMonitor(monitor);
      recordUiEvent(`Session disconnected for ${userId}`, "warn");
      try { if (glassesVideoStream || monitor.video.forwarding) await session.camera.stopManagedStream(); } catch {}
      try { if (micAudioTrack) await micAudioTrack.close(); } catch (err) { console.error("[mic-bridge] audio close failed:", err); }
      try { if (glassesVideoStream) await glassesVideoStream.cancel(); } catch (err) { console.error("[livekit] glasses video stream close failed:", err); }
      try { if (micRoom) await micRoom.disconnect(); console.log("[livekit] mic-bridge disconnected"); } catch (err) { console.error("[mic-bridge] room disconnect failed:", err); }
      if (dispatchId) {
        try {
          await dispatchClient.deleteDispatch(dispatchId, roomName);
          console.log(`[livekit] deleted dispatch ${dispatchId}`);
        } catch (err) {
          console.error("[livekit] deleteDispatch failed:", err);
        }
      }
      if (ingressId) {
        try {
          await ingressClient.deleteIngress(ingressId);
          console.log(`[livekit] deleted ingress ${ingressId}`);
        } catch (err) {
          console.error("[livekit] deleteIngress failed:", err);
        }
      }
    });
  }
}

const app = new StreamApp({ packageName: PACKAGE_NAME, apiKey: API_KEY, port: PORT });
app.getExpressApp().get("/", (_req: Request, res: Response) => {
  res.setHeader("Content-Type", "text/html; charset=utf-8");
  res.send(dashboardHtml());
});
app.getExpressApp().get("/status", (_req: Request, res: Response) => res.json(dashboardStatus()));
app.getExpressApp().get("/health", (_req: Request, res: Response) => res.send("guidance: ok"));
app.getExpressApp().get("/mute", (_req: Request, res: Response) => {
  const control = currentSessionControl();
  if (!control) {
    res.status(503).json({ error: "no active session" });
    return;
  }
  control.mute("dashboard");
  res.json({ muted: true });
});
app.getExpressApp().get("/unmute", (_req: Request, res: Response) => {
  const control = currentSessionControl();
  if (!control) {
    res.status(503).json({ error: "no active session" });
    return;
  }
  control.unmute("dashboard");
  res.json({ muted: false });
});
app.getExpressApp().get("/mute-toggle", (_req: Request, res: Response) => {
  const control = currentSessionControl();
  if (!control) {
    res.status(503).json({ error: "no active session" });
    return;
  }
  res.json({ muted: control.toggleMute("dashboard") });
});

// 1-second 440 Hz sine MP3 at 16 kHz mono; cached under a stable id.
function buildTestToneMp3(): Buffer {
  const rate = 16000;
  const samples = rate; // 1 second
  const pcm = Buffer.alloc(samples * 2);
  for (let i = 0; i < samples; i++) {
    const v = Math.sin((2 * Math.PI * 440 * i) / rate) * 0.4 * 32767;
    pcm.writeInt16LE(Math.round(v), i * 2);
  }
  return pcm16ToMp3(pcm, rate);
}
audioCache.set("test-tone", { buf: buildTestToneMp3(), expires: Number.MAX_SAFE_INTEGER });

// 500 ms of silence used as a "pre-flush" at session start. Mentra Cloud carries
// a stuck audio request across sessions (we see "Received audio play response for
// unknown request ID" warnings), which blocks the first real playAudio of the new
// session. Sending a silent ping early absorbs that stale state. The bytes are
// already silent (zeros), so we play at full volume — playAudio with volume: 0
// appears to be skipped by the phone and the promise never resolves, leaving the
// queue blocked.
audioCache.set("silence", {
  buf: pcm16ToMp3(Buffer.alloc(16000 * 2 / 2), 16000),
  expires: Number.MAX_SAFE_INTEGER,
});

// Hit this from a browser while a session is live; plays a self-generated 1s sine
// tone via the same /audio/<id>.wav path the agent uses.
// Use ?track=0 (speaker, default) | 1 (app_audio) | 2 (tts) to test each output channel.
app.getExpressApp().get("/test-tone", (req: Request, res: Response) => {
  const session = activeSessions.values().next().value;
  if (!session) {
    res.status(503).send("no active session");
    return;
  }
  const trackId = Number(req.query.track ?? 0);
  const url = `${PUBLIC_BASE_URL}/audio/test-tone.mp3`;
  console.log(`[test-tone] requesting playAudio track=${trackId} url=${url}`);
  recordUiEvent(`Test tone queued on track ${trackId}`, "info");
  session.audio.playAudio({ audioUrl: url, volume: 1.0, stopOtherAudio: true, trackId })
    .then((r) => console.log(`[test-tone] track=${trackId} result:`, JSON.stringify(r)))
    .catch((e) => console.error(`[test-tone] track=${trackId} failed:`, e?.message ?? e));
  res.json({ url, trackId, queued: true });
});

// Streams a 3s 440 Hz tone via the new chunked /stream endpoint to verify the
// phone player accepts chunked MP3 before exercising the agent's voice path.
app.getExpressApp().get("/test-stream", (_req: Request, res: Response) => {
  const session = activeSessions.values().next().value;
  if (!session) {
    res.status(503).send("no active session");
    return;
  }
  const RATE = 24000;
  const KBPS = 64;
  const id = crypto.randomBytes(8).toString("hex");
  const enc = new StreamingMp3Encoder(id, RATE, KBPS);
  streams.set(id, enc);
  const url = `${PUBLIC_BASE_URL}/stream/${id}.mp3`;
  console.log(`[test-stream] opened ${url}`);
  recordUiEvent(`Chunked MP3 test stream opened (${id})`, "info");
  session.audio
    .playAudio({ audioUrl: url, stopOtherAudio: true, volume: 1.0 })
    .then((r) => console.log("[test-stream] result:", JSON.stringify(r)))
    .catch((e) => console.error("[test-stream] failed:", e?.message ?? e));
  // Push 100 ms of PCM every 100 ms for 3 s so the player must process chunks
  // as they arrive (instead of buffering everything before any download).
  let pushed = 0;
  const totalChunks = 30; // 30 * 100ms = 3s
  const samplesPerChunk = RATE / 10; // 100ms
  let phase = 0;
  const tick = setInterval(() => {
    const buf = new Int16Array(samplesPerChunk);
    for (let i = 0; i < samplesPerChunk; i++) {
      buf[i] = Math.round(Math.sin((2 * Math.PI * 440 * phase) / RATE) * 0.4 * 32767);
      phase++;
    }
    enc.pushPcm(buf);
    pushed++;
    if (pushed >= totalChunks) {
      clearInterval(tick);
      enc.end();
      recordUiEvent(`Chunked MP3 test stream ended (${id})`, "good");
    }
  }, 100);
  res.json({ url, queued: true });
});

// Plays a known-good public MP3 via playAudio. If this works but /test-tone (WAV)
// does not, the Mentra Live audio player only accepts MP3 (not PCM WAV).
app.getExpressApp().get("/test-mp3", (_req: Request, res: Response) => {
  const session = activeSessions.values().next().value;
  if (!session) {
    res.status(503).send("no active session");
    return;
  }
  const url = "https://samplelib.com/mp3/sample-3s.mp3";
  console.log(`[test-mp3] requesting playAudio for ${url}`);
  recordUiEvent("Public MP3 test queued", "info");
  session.audio.playAudio({ audioUrl: url, volume: 1.0, stopOtherAudio: true })
    .then((r) => console.log("[test-mp3] result:", JSON.stringify(r)))
    .catch((e) => console.error("[test-mp3] failed:", e?.message ?? e));
  res.json({ url, queued: true });
});

// Tests Mentra's built-in TTS path (ElevenLabs through the cloud). If this works
// but /test-tone does not, the speaker is fine and the issue is just track routing.
app.getExpressApp().get("/test-speak", (_req: Request, res: Response) => {
  const session = activeSessions.values().next().value;
  if (!session) {
    res.status(503).send("no active session");
    return;
  }
  console.log("[test-speak] requesting speak()");
  recordUiEvent("Mentra speak test queued", "info");
  session.audio.speak("Hello from the speaker test.", { volume: 1.0 })
    .then((r) => console.log("[test-speak] result:", JSON.stringify(r)))
    .catch((e) => console.error("[test-speak] failed:", e?.message ?? e));
  res.json({ queued: true });
});

app.getExpressApp().get("/audio/:id.mp3", (req: Request, res: Response) => {
  const id = String(req.params.id ?? "");
  const entry = audioCache.get(id);
  if (!entry || entry.expires < Date.now()) {
    res.status(404).send("not found");
    return;
  }
  res.set("Content-Type", "audio/mpeg");
  res.set("Content-Length", entry.buf.length.toString());
  res.send(entry.buf);
});

// Per-utterance chunked MP3 stream. The phone fetches once, our server holds the
// connection open and writes MP3 bytes as the agent's voice arrives.
app.getExpressApp().get("/stream/:id.mp3", (req: Request, res: Response) => {
  const id = String(req.params.id ?? "");
  const stream = streams.get(id);
  if (!stream) {
    res.status(404).end();
    return;
  }
  stream.addWriter(res);
});

app.getExpressApp().get("/viewer", async (req: Request, res: Response) => {
  const room = (req.query.room as string) || "guidance-akshay_sunkara_gmail_com";
  const identity = (req.query.identity as string) || `viewer-${Date.now()}`;
  const at = new AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET, { identity, ttl: "1h" });
  at.addGrant({ roomJoin: true, room, canSubscribe: true, canPublish: false, canPublishData: true });
  const token = await at.toJwt();
  const meetUrl = `https://meet.livekit.io/custom?liveKitUrl=${encodeURIComponent(LIVEKIT_URL)}&token=${token}`;
  if (req.query.format === "json") {
    res.json({ room, identity, token, liveKitUrl: LIVEKIT_URL, meetUrl });
  } else {
    res.redirect(meetUrl);
  }
});

// --- RTMP ingress diagnostics ---------------------------------------------
// Reads the LiveKit ingress state so we can tell whether the glasses' RTMP push
// is even reaching LiveKit. status meanings: INACTIVE = nothing has connected,
// BUFFERING/PUBLISHING = a stream IS arriving, ERROR = connected but failed.
const INGRESS_STATUS_LABELS = ["INACTIVE", "BUFFERING", "PUBLISHING", "ERROR", "COMPLETE"];

app.getExpressApp().get("/ingress-status", async (req: Request, res: Response) => {
  try {
    const roomName = (req.query.room as string) || currentMonitor()?.roomName;
    const ingresses = await ingressClient.listIngress(roomName ? { roomName } : {});
    res.json({
      roomName: roomName ?? null,
      count: ingresses.length,
      ingresses: ingresses.map((i) => ({
        ingressId: i.ingressId,
        name: i.name,
        roomName: i.roomName,
        url: i.url,
        streamKey: i.streamKey,
        statusCode: i.state?.status,
        status: i.state ? (INGRESS_STATUS_LABELS[i.state.status] ?? String(i.state.status)) : "no-state",
        error: i.state?.error || "",
        videoMime: i.state?.video?.mimeType || "",
        startedAt: i.state?.startedAt ? String(i.state.startedAt) : "",
      })),
    });
  } catch (err) {
    res.status(500).json({ error: errMessage(err) });
  }
});

// Creates a throwaway RTMP ingress (matching the session's config) so you can push
// a test stream with ffmpeg and confirm the LiveKit ingress works independent of the
// glasses. Returns the rtmp URL, a ready-to-run ffmpeg command, and a viewer link.
app.getExpressApp().get("/test-ingress", async (_req: Request, res: Response) => {
  try {
    const roomName = `ingress-test-${Date.now()}`;
    const ingress = await ingressClient.createIngress(IngressInput.RTMP_INPUT, {
      name: `ffmpeg-test-${Date.now()}`,
      roomName,
      participantIdentity: "ffmpeg-test",
      participantName: "ffmpeg-test",
      enableTranscoding: true,
    });
    const ingressBase = ingress.url.replace(/^rtmps?:\/\//, "");
    const rtmpUrl = `rtmp://${ingressBase}/${ingress.streamKey}`;
    const rtmpsUrl = `rtmps://${ingressBase}/${ingress.streamKey}`;
    const mkFfmpeg = (u: string) =>
      `ffmpeg -re -f lavfi -i testsrc=size=640x480:rate=15 -c:v libx264 -preset veryfast -tune zerolatency -profile:v baseline -pix_fmt yuv420p -b:v 500k -f flv "${u}"`;
    res.json({
      roomName,
      ingressId: ingress.ingressId,
      ingressUrl: ingress.url,
      rtmpUrl,
      rtmpsUrl,
      viewerUrl: `${PUBLIC_BASE_URL}/viewer?room=${encodeURIComponent(roomName)}`,
      statusUrl: `${PUBLIC_BASE_URL}/ingress-status?room=${encodeURIComponent(roomName)}`,
      deleteUrl: `${PUBLIC_BASE_URL}/test-ingress-delete?id=${ingress.ingressId}`,
      ffmpegPlainRtmp: mkFfmpeg(rtmpUrl),
      ffmpegRtmps: mkFfmpeg(rtmpsUrl),
    });
  } catch (err) {
    res.status(500).json({ error: errMessage(err) });
  }
});

app.getExpressApp().get("/test-ingress-delete", async (req: Request, res: Response) => {
  const id = String(req.query.id ?? "");
  if (!id) {
    res.status(400).json({ error: "missing id" });
    return;
  }
  try {
    await ingressClient.deleteIngress(id);
    res.json({ deleted: id });
  } catch (err) {
    res.status(500).json({ error: errMessage(err) });
  }
});

app.getExpressApp().get("/managed-urls", (_req: Request, res: Response) => {
  res.json(latestManagedUrls ?? { error: "no managed stream started yet this session" });
});

app.start();
console.log(`listening on :${PORT}`);
