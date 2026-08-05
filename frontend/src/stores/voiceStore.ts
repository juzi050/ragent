import { toast } from "sonner";
import { create } from "zustand";

import { streamMessageAudio } from "@/services/voiceService";

type VoicePlaybackState = "idle" | "loading" | "playing" | "error";

interface VoiceState {
  playbackState: VoicePlaybackState;
  pendingMessageId: string | null;
  playingMessageId: string | null;
  playMessage: (messageId: string) => Promise<void>;
  stopPlayback: () => void;
}

let abortController: AbortController | null = null;
let playbackGeneration = 0;

function stopCurrentStream() {
  playbackGeneration += 1;
  abortController?.abort();
  abortController = null;
}

export const useVoiceStore = create<VoiceState>((set, get) => ({
  playbackState: "idle",
  pendingMessageId: null,
  playingMessageId: null,

  playMessage: async (messageId) => {
    if (get().playingMessageId === messageId) {
      get().stopPlayback();
      return;
    }

    stopCurrentStream();
    const controller = new AbortController();
    const generation = playbackGeneration;
    abortController = controller;
    set({
      playbackState: "loading",
      pendingMessageId: messageId,
      playingMessageId: null
    });

    try {
      await streamMessageAudio(messageId, {
        signal: controller.signal,
        onPlaying: () => {
          if (generation !== playbackGeneration) return;
          set({
            playbackState: "playing",
            pendingMessageId: null,
            playingMessageId: messageId
          });
        }
      });
      if (generation === playbackGeneration) {
        set({ playbackState: "idle", pendingMessageId: null, playingMessageId: null });
      }
    } catch (error) {
      if (controller.signal.aborted || generation !== playbackGeneration) {
        return;
      }
      set({ playbackState: "error", pendingMessageId: null, playingMessageId: null });
      toast.error((error as Error).message || "语音播放失败");
    } finally {
      if (abortController === controller) {
        abortController = null;
      }
    }
  },

  stopPlayback: () => {
    stopCurrentStream();
    set({ playbackState: "idle", pendingMessageId: null, playingMessageId: null });
  }
}));
