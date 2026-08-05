interface AudioWorkletProcessorOptions {
  processorOptions?: {
    channels?: number;
  };
}

declare abstract class AudioWorkletProcessor {
  readonly port: MessagePort;

  protected constructor(options?: AudioWorkletProcessorOptions);
}

declare function registerProcessor(
  name: string,
  processorConstructor: new (options: AudioWorkletProcessorOptions) => AudioWorkletProcessor
): void;

type AudioMessage =
  | { type: "chunk"; buffer: ArrayBuffer; channels?: number }
  | { type: "end" }
  | { type: "reset" };

interface QueuedAudio {
  samples: Float32Array;
  channels: number;
}

class FloatAudioProcessor extends AudioWorkletProcessor {
  private channels: number;
  private queue: QueuedAudio[] = [];
  private readFrame = 0;
  private queuedFrames = 0;
  private ended = false;
  private endNotified = false;

  constructor(options: AudioWorkletProcessorOptions) {
    super(options);
    this.channels = Math.max(1, options.processorOptions?.channels ?? 1);

    this.port.onmessage = (event: MessageEvent<AudioMessage>) => {
      const message = event.data;
      if (message.type === "chunk") {
        const channels = Math.max(1, message.channels ?? this.channels);
        const samples = new Float32Array(message.buffer);
        if (samples.length === 0 || samples.length % channels !== 0) {
          return;
        }
        this.queue.push({ samples, channels });
        this.queuedFrames += samples.length / channels;
      } else if (message.type === "end") {
        this.ended = true;
      } else if (message.type === "reset") {
        this.queue = [];
        this.readFrame = 0;
        this.queuedFrames = 0;
        this.ended = false;
        this.endNotified = false;
      }
    };
  }

  process(_inputs: Float32Array[][], outputs: Float32Array[][]) {
    const output = outputs[0];
    const frameCount = output?.[0]?.length ?? 0;

    for (let frame = 0; frame < frameCount; frame += 1) {
      const current = this.queue[0];
      const offset = this.readFrame * (current?.channels ?? 1);
      for (let channel = 0; channel < output.length; channel += 1) {
        output[channel][frame] = current?.samples[offset + (channel % current.channels)] ?? 0;
      }
      if (current) {
        this.readFrame += 1;
        this.queuedFrames -= 1;
        if (this.readFrame >= current.samples.length / current.channels) {
          this.queue.shift();
          this.readFrame = 0;
        }
      }
    }

    if (this.ended && this.queuedFrames === 0 && !this.endNotified) {
      this.endNotified = true;
      this.port.postMessage({ type: "ended" });
    }

    return !this.ended || this.queuedFrames > 0;
  }
}

registerProcessor("float-audio-processor", FloatAudioProcessor);
