import { useChatStore } from "@/stores/chatStore";

import { createStreamResponse } from "@/hooks/useStreamResponse";
import { storage } from "@/utils/storage";

const API_BASE_URL = (import.meta.env.VITE_API_BASE_URL || "").replace(/\/$/, "");
const PLAY_URL = `${API_BASE_URL}/rag/v3/voice/play`;
const STOP_URL = `${API_BASE_URL}/rag/v3/voice/stop`;

interface AudioMetaPayload {
  taskId?: string;
}

// 播放器单例 全局只播一条
let streamRef: ReturnType<typeof createStreamResponse> | null = null;
let mediaSourceRef: MediaSource | null = null;
let sourceBufferRef: SourceBuffer | null = null;
let audioElRef: HTMLAudioElement | null = null;
let objectUrlRef: string | null = null;
let taskIdRef: string | null = null;
let appendQueue: Uint8Array[] = [];
let appendPending = false;

function stopInternal() {
  const taskId = taskIdRef;
  if (taskId) {
    const token = storage.getToken();
    fetch(`${STOP_URL}?taskId=${encodeURIComponent(taskId)}`, {
      method: "POST",
      headers: token ? { Authorization: token } : undefined
    }).catch(() => null);
  }
  streamRef = null;
  taskIdRef = null;
  if (audioElRef) {
    audioElRef.pause();
    audioElRef.removeAttribute("src");
    audioElRef = null;
  }
  if (objectUrlRef) {
    URL.revokeObjectURL(objectUrlRef);
    objectUrlRef = null;
  }
  if (mediaSourceRef && mediaSourceRef.readyState !== "closed") {
    try {
      mediaSourceRef.endOfStream();
    } catch {
      // 流已关闭
    }
  }
  mediaSourceRef = null;

  sourceBufferRef = null;
  appendQueue = [];
  appendPending = false;
}

/**
 * MSE 流式播放 MP3 帧边收边喂 SourceBuffer（audio/mpeg）
 */
function playInternal(messageId: string) {
  stopInternal();

  if (!("MediaSource" in window)) {
    setPlaying(null);
    return;
  }
  if (!MediaSource.isTypeSupported("audio/mpeg")) {
    setPlaying(null);
    return;
  }

  let playStarted = false;
  let streamEnded = false;
  const mediaSource = new MediaSource();
  mediaSourceRef = mediaSource;
  const audio = new Audio();
  audioElRef = audio;

  const endStreamIfReady = () => {
    if (
      !streamEnded ||
      appendPending ||
      appendQueue.length > 0 ||
      !sourceBufferRef ||
      sourceBufferRef.updating ||
      mediaSource.readyState !== "open"
    ) {
      return;
    }
    try {
      mediaSource.endOfStream();
    } catch {
      // 流已关闭
    }
  };

  const flushAppendQueue = () => {
    if (appendPending || !sourceBufferRef || appendQueue.length === 0) return;
    const next = appendQueue.shift()!;
    appendPending = true;
    try {
      sourceBufferRef.appendBuffer(next);
      // 首帧入缓冲后开始播放
      if (!playStarted) {
        playStarted = true;
        audio.play().catch(() => {
          if (audioElRef === audio) {
            stopInternal();
            setPlaying(null);
          }
        });
      }
    } catch {
      appendPending = false;
    }
  };

  mediaSource.addEventListener("sourceopen", () => {
    try {
      sourceBufferRef = mediaSource.addSourceBuffer("audio/mpeg");
      // audio/mpeg 无带内时间戳 用 sequence 模式
      sourceBufferRef.mode = "sequence";
      sourceBufferRef.addEventListener("updateend", () => {
        appendPending = false;
        flushAppendQueue();
        endStreamIfReady();
      });
      // 补排待追加的帧
      flushAppendQueue();
      endStreamIfReady();
    } catch {
      setPlaying(null);
    }
  });

  audio.onerror = () => setPlaying(null);
  audio.onended = () => setPlaying(null);

  const handlers = {
    onEvent(event: string, payload: unknown) {
      if (streamRef !== stream) return;
      if (event === "audio-meta") {
        const meta = payload as AudioMetaPayload;
        taskIdRef = meta?.taskId ?? null;
      } else if (event === "audio") {
        const frame = payload as { base64?: string };
        if (!frame?.base64) return;
        const bytes = Uint8Array.from(atob(frame.base64), (c) => c.charCodeAt(0));
        appendQueue.push(bytes);
        flushAppendQueue();
      } else if (event === "done") {
        streamRef = null;
        taskIdRef = null;
        streamEnded = true;
        endStreamIfReady();
      }
    },
    onError() {
      if (streamRef !== stream) return;
      streamRef = null;
      taskIdRef = null;
      // 流异常结束 让已缓冲音频播完
      streamEnded = true;
      endStreamIfReady();
      setPlaying(null);
    }
  };

  const token = storage.getToken();
  const stream = createStreamResponse(
    {
      url: `${PLAY_URL}?messageId=${encodeURIComponent(messageId)}`,
      headers: token ? { Authorization: token } : undefined,
      retryCount: 0
    },
    handlers
  );
  streamRef = stream;

  if (objectUrlRef) {
    URL.revokeObjectURL(objectUrlRef);
  }
  objectUrlRef = URL.createObjectURL(mediaSource);
  audio.src = objectUrlRef;

  stream.start().catch(() => {
    if (streamRef === stream) {
      stopInternal();
      setPlaying(null);
    }
  });
  setPlaying(messageId);
}

function setPlaying(messageId: string | null) {
  useChatStore.setState({ playingMessageId: messageId });
}

/**
 * 停止当前语音播放
 */
export function stopVoicePlayback() {
  stopInternal();
  setPlaying(null);
}

/**
 * 消息语音播放
 */
export function useVoicePlayback() {
  const playingId = useChatStore((state) => state.playingMessageId);

  const togglePlay = (messageId: string) => {
    if (playingId === messageId) {
      stopInternal();
      setPlaying(null);
      return;
    }
    playInternal(messageId);
  };

  return { playingId, togglePlay };
}
