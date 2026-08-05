import { storage } from "@/utils/storage";
import audioWorkletUrl from "@/worklets/audioWorklet.ts?worker&url";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "";
const SAMPLE_RATE = 24_000;
const CHANNELS = 1;

export interface StreamAudioOptions {
  signal: AbortSignal;
  onPlaying: () => void;
}

interface DecodedAudioData {
  numberOfFrames: number;
  numberOfChannels: number;
  copyTo(destination: Float32Array, options: { planeIndex: number; format: string }): void;
  close(): void;
}

interface AudioDecoderLike {
  state: string;
  configure(config: { codec: string; sampleRate: number; numberOfChannels: number }): void;
  decode(chunk: unknown): void;
  flush(): Promise<void>;
  close(): void;
}

interface AudioDecoderConstructor {
  new (callbacks: {
    output: (audioData: DecodedAudioData) => void;
    error: (error: DOMException) => void;
  }): AudioDecoderLike;
  isConfigSupported?(config: {
    codec: string;
    sampleRate: number;
    numberOfChannels: number;
  }): Promise<{ supported?: boolean }>;
}

interface EncodedAudioChunkConstructor {
  new (init: {
    type: "key" | "delta";
    timestamp: number;
    data: BufferSource;
  }): unknown;
}

function streamUrl(messageId: string) {
  const params = new URLSearchParams({ messageId });
  return `${API_BASE_URL}/voice/tts/stream?${params.toString()}`;
}

function abortError() {
  return new DOMException("音频播放已取消", "AbortError");
}

function concatBytes(left: Uint8Array, right: Uint8Array) {
  const result = new Uint8Array(left.byteLength + right.byteLength);
  result.set(left, 0);
  result.set(right, left.byteLength);
  return result;
}

function waitForWorkletEnd(node: AudioWorkletNode, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const onMessage = (event: MessageEvent<{ type?: string }>) => {
      if (event.data?.type === "ended") {
        cleanup();
        resolve();
      }
    };
    const onAbort = () => {
      cleanup();
      reject(abortError());
    };
    const cleanup = () => {
      node.port.removeEventListener("message", onMessage);
      signal.removeEventListener("abort", onAbort);
    };
    node.port.addEventListener("message", onMessage);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * 将 Fetch 中任意边界的 Ogg 页重新组装，并提取完整的 Opus packet。
 * Ogg 自带页/packet framing，因此不把供应商回调简单拼成裸 Opus 流。
 */
class OggOpusDemuxer {
  private pending = new Uint8Array(0);
  private packet = new Uint8Array(0);
  private readonly onPacket: (packet: Uint8Array) => void;

  constructor(onPacket: (packet: Uint8Array) => void) {
    this.onPacket = onPacket;
  }

  push(chunk: Uint8Array) {
    this.pending = concatBytes(this.pending, chunk);
    let offset = 0;
    while (this.pending.byteLength - offset >= 27) {
      const segmentCount = this.pending[offset + 26];
      const headerLength = 27 + segmentCount;
      if (this.pending.byteLength - offset < headerLength) {
        break;
      }
      let bodyLength = 0;
      for (let index = 0; index < segmentCount; index += 1) {
        bodyLength += this.pending[offset + 27 + index];
      }
      const pageLength = headerLength + bodyLength;
      if (this.pending.byteLength - offset < pageLength) {
        break;
      }
      const bodyStart = offset + headerLength;
      let bodyOffset = bodyStart;
      for (let index = 0; index < segmentCount; index += 1) {
        const segmentLength = this.pending[offset + 27 + index];
        this.packet = concatBytes(this.packet, this.pending.subarray(bodyOffset, bodyOffset + segmentLength));
        bodyOffset += segmentLength;
        if (segmentLength < 255) {
          this.onPacket(this.packet);
          this.packet = new Uint8Array(0);
        }
      }
      offset += pageLength;
    }
    this.pending = this.pending.slice(offset);
  }
}

function getWebCodecs() {
  const decoder = (globalThis as typeof globalThis & { AudioDecoder?: AudioDecoderConstructor }).AudioDecoder;
  const chunk = (globalThis as typeof globalThis & { EncodedAudioChunk?: EncodedAudioChunkConstructor })
    .EncodedAudioChunk;
  return { decoder: decoder!, chunk: chunk! };
}

export async function streamMessageAudio(messageId: string, options: StreamAudioOptions) {
  const token = storage.getToken();
  const response = await fetch(streamUrl(messageId), {
    method: "GET",
    headers: token ? { Authorization: token } : undefined,
    signal: options.signal
  });
  if (!response.ok) {
    throw new Error(`语音请求失败（${response.status}）`);
  }
  if (!response.body) {
    throw new Error("语音请求失败");
  }

  const { decoder: AudioDecoderCtor, chunk: EncodedAudioChunkCtor } = getWebCodecs();
  const context = new AudioContext({ sampleRate: SAMPLE_RATE });
  const reader = response.body.getReader();
  let node: AudioWorkletNode | null = null;
  let decoder: AudioDecoderLike | null = null;
  let notifiedPlaying = false;
  let timestampUs = 0;
  let packetCount = 0;

  try {
    await context.audioWorklet.addModule(audioWorkletUrl);
    node = new AudioWorkletNode(context, "float-audio-processor", {
      numberOfInputs: 0,
      numberOfOutputs: 1,
      outputChannelCount: [CHANNELS],
      processorOptions: { channels: CHANNELS }
    });
    node.connect(context.destination);

    decoder = new AudioDecoderCtor({
      output: (audioData) => {
        try {
          const outputChannels = audioData.numberOfChannels;
          const frames = audioData.numberOfFrames;
          const planes = Array.from({ length: outputChannels }, () => new Float32Array(frames));
          planes.forEach((plane, planeIndex) => {
            audioData.copyTo(plane, { planeIndex, format: "f32-planar" });
          });
          const interleaved = new Float32Array(frames * outputChannels);
          for (let frame = 0; frame < frames; frame += 1) {
            for (let channel = 0; channel < outputChannels; channel += 1) {
              interleaved[frame * outputChannels + channel] = planes[channel][frame];
            }
          }
          const buffer = interleaved.buffer;
          node?.port.postMessage(
            { type: "chunk", buffer, channels: outputChannels },
            [buffer]
          );
          if (!notifiedPlaying) {
            notifiedPlaying = true;
            options.onPlaying();
            void context.resume();
          }
        } finally {
          audioData.close();
        }
      },
      error: () => {
        void reader.cancel().catch(() => undefined);
      }
    });
    decoder.configure({ codec: "opus", sampleRate: SAMPLE_RATE, numberOfChannels: CHANNELS });

    const demuxer = new OggOpusDemuxer((packet) => {
      if (packetCount < 2) {
        packetCount += 1;
        return;
      }
      const encoded = new Uint8Array(packet);
      decoder?.decode(new EncodedAudioChunkCtor({
        type: packetCount === 2 ? "key" : "delta",
        timestamp: timestampUs,
        data: encoded
      }));
      packetCount += 1;
      timestampUs += 20_000;
    });

    while (!options.signal.aborted) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      if (value && value.byteLength > 0) {
        demuxer.push(value);
      }
    }
    if (options.signal.aborted) {
      throw abortError();
    }
    await decoder.flush();

    node.port.postMessage({ type: "end" });
    await waitForWorkletEnd(node, options.signal);
  } finally {
    await reader.cancel().catch(() => undefined);
    if (decoder && decoder.state !== "closed") {
      decoder.close();
    }
    if (node) {
      node.port.postMessage({ type: "reset" });
      node.disconnect();
    }
    await context.close().catch(() => undefined);
  }
}
