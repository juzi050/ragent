import { Loader2, Square, Volume2 } from "lucide-react";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { useVoiceStore } from "@/stores/voiceStore";

interface MessageSpeechButtonProps {
  messageId: string;
}

export function MessageSpeechButton({ messageId }: MessageSpeechButtonProps) {
  const pendingMessageId = useVoiceStore((state) => state.pendingMessageId);
  const playingMessageId = useVoiceStore((state) => state.playingMessageId);
  const playMessage = useVoiceStore((state) => state.playMessage);
  const stopPlayback = useVoiceStore((state) => state.stopPlayback);
  const isPending = pendingMessageId === messageId;
  const isPlaying = playingMessageId === messageId;

  const handleClick = () => {
    if (isPlaying || isPending) {
      stopPlayback();
      return;
    }
    void playMessage(messageId);
  };

  return (
    <Button
      type="button"
      variant="ghost"
      size="icon"
      onClick={handleClick}
      aria-label={isPlaying ? "停止朗读" : isPending ? "正在加载语音" : "朗读消息"}
      title={isPlaying ? "停止朗读" : "朗读消息"}
      className={cn(
        "h-7 w-7 rounded-md transition-colors hover:bg-[#F5F5F5]",
        isPlaying
          ? "bg-[#EAF1FF] text-[#2563EB] hover:bg-[#DBEAFE] hover:text-[#1D4ED8]"
          : "text-[#999999] hover:text-[#666666]",
        isPending && "cursor-wait"
      )}
    >
      {isPending ? (
        <Loader2 className="h-4 w-4 animate-spin" />
      ) : isPlaying ? (
        <Square className="h-3.5 w-3.5 fill-current" />
      ) : (
        <Volume2 className="h-4 w-4" />
      )}
    </Button>
  );
}
