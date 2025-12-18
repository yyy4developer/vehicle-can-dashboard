import { useState, useRef, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Video,
  VideoOff,
  Play,
  Pause,
  Maximize2,
  Volume2,
  VolumeX,
  RefreshCw,
  Camera,
} from "lucide-react";

interface VideoPlayerProps {
  vehicleId?: string;
}

type CameraKey = "front" | "rear" | "left" | "right";

const CAMERA_LABELS: Record<CameraKey, string> = {
  front: "フロント",
  rear: "リア",
  left: "左側",
  right: "右側",
};

export function VideoPlayer({ vehicleId = "VH001" }: VideoPlayerProps) {
  const [selectedCamera, setSelectedCamera] = useState<CameraKey>("front");
  const [isPlaying, setIsPlaying] = useState(false);
  const [isMuted, setIsMuted] = useState(true);
  const [isLoading, setIsLoading] = useState(false);
  const [hasError, setHasError] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const videoUrl = `/api/video/${selectedCamera}/stream?vehicle_id=${vehicleId}`;

  useEffect(() => {
    // Reset state when camera changes
    setIsLoading(true);
    setHasError(false);
    setIsPlaying(false);
  }, [selectedCamera]);

  const handlePlay = async () => {
    if (videoRef.current) {
      try {
        await videoRef.current.play();
        setIsPlaying(true);
      } catch (e) {
        console.error("Failed to play video:", e);
      }
    }
  };

  const handlePause = () => {
    if (videoRef.current) {
      videoRef.current.pause();
      setIsPlaying(false);
    }
  };

  const togglePlay = () => {
    if (isPlaying) {
      handlePause();
    } else {
      handlePlay();
    }
  };

  const toggleMute = () => {
    if (videoRef.current) {
      videoRef.current.muted = !isMuted;
      setIsMuted(!isMuted);
    }
  };

  const toggleFullscreen = async () => {
    if (!containerRef.current) return;

    if (!isFullscreen) {
      try {
        await containerRef.current.requestFullscreen();
        setIsFullscreen(true);
      } catch (e) {
        console.error("Failed to enter fullscreen:", e);
      }
    } else {
      try {
        await document.exitFullscreen();
        setIsFullscreen(false);
      } catch (e) {
        console.error("Failed to exit fullscreen:", e);
      }
    }
  };

  const handleRetry = () => {
    setHasError(false);
    setIsLoading(true);
    if (videoRef.current) {
      videoRef.current.load();
    }
  };

  const handleVideoLoaded = () => {
    setIsLoading(false);
    setHasError(false);
  };

  const handleVideoError = () => {
    setIsLoading(false);
    setHasError(true);
  };

  useEffect(() => {
    const handleFullscreenChange = () => {
      setIsFullscreen(!!document.fullscreenElement);
    };

    document.addEventListener("fullscreenchange", handleFullscreenChange);
    return () => {
      document.removeEventListener("fullscreenchange", handleFullscreenChange);
    };
  }, []);

  return (
    <Card className="lg:col-span-2">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <Video className="h-5 w-5" />
            カメラ映像
          </CardTitle>
          <Badge variant="secondary" className="flex items-center gap-1">
            <Camera className="h-3 w-3" />
            {CAMERA_LABELS[selectedCamera]}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Camera Selection */}
        <div className="flex gap-2 flex-wrap">
          {(Object.keys(CAMERA_LABELS) as CameraKey[]).map((camera) => (
            <Button
              key={camera}
              variant={selectedCamera === camera ? "default" : "outline"}
              size="sm"
              onClick={() => setSelectedCamera(camera)}
              className="min-w-20"
            >
              {CAMERA_LABELS[camera]}
            </Button>
          ))}
        </div>

        {/* Video Player Container */}
        <div
          ref={containerRef}
          className="relative aspect-video bg-black rounded-lg overflow-hidden group"
        >
          {/* Video Element */}
          <video
            ref={videoRef}
            src={videoUrl}
            className="w-full h-full object-contain"
            muted={isMuted}
            playsInline
            onLoadedData={handleVideoLoaded}
            onError={handleVideoError}
            onPlay={() => setIsPlaying(true)}
            onPause={() => setIsPlaying(false)}
            onEnded={() => setIsPlaying(false)}
          />

          {/* Loading Overlay */}
          {isLoading && (
            <div className="absolute inset-0 flex items-center justify-center bg-black/50">
              <RefreshCw className="h-8 w-8 text-white animate-spin" />
            </div>
          )}

          {/* Error Overlay */}
          {hasError && (
            <div className="absolute inset-0 flex flex-col items-center justify-center bg-muted/90 gap-3">
              <VideoOff className="h-12 w-12 text-muted-foreground/50" />
              <p className="text-sm text-muted-foreground">
                動画を読み込めませんでした
              </p>
              <Button variant="outline" size="sm" onClick={handleRetry}>
                <RefreshCw className="h-4 w-4 mr-2" />
                再試行
              </Button>
            </div>
          )}

          {/* Controls Overlay */}
          {!hasError && (
            <div className="absolute bottom-0 left-0 right-0 bg-gradient-to-t from-black/80 to-transparent p-4 opacity-0 group-hover:opacity-100 transition-opacity">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-8 w-8 text-white hover:bg-white/20"
                    onClick={togglePlay}
                    disabled={isLoading}
                  >
                    {isPlaying ? (
                      <Pause className="h-4 w-4" />
                    ) : (
                      <Play className="h-4 w-4" />
                    )}
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-8 w-8 text-white hover:bg-white/20"
                    onClick={toggleMute}
                  >
                    {isMuted ? (
                      <VolumeX className="h-4 w-4" />
                    ) : (
                      <Volume2 className="h-4 w-4" />
                    )}
                  </Button>
                </div>
                <Button
                  variant="ghost"
                  size="icon"
                  className="h-8 w-8 text-white hover:bg-white/20"
                  onClick={toggleFullscreen}
                >
                  <Maximize2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          )}

          {/* Play Button Overlay (when paused) */}
          {!isPlaying && !hasError && !isLoading && (
            <button
              className="absolute inset-0 flex items-center justify-center bg-black/20 hover:bg-black/30 transition-colors cursor-pointer"
              onClick={handlePlay}
            >
              <div className="h-16 w-16 rounded-full bg-white/90 flex items-center justify-center shadow-lg">
                <Play className="h-8 w-8 text-black ml-1" />
              </div>
            </button>
          )}
        </div>

        {/* Multi-Camera Grid (Small Previews) */}
        <div className="grid grid-cols-4 gap-2">
          {(Object.keys(CAMERA_LABELS) as CameraKey[]).map((camera) => (
            <button
              key={camera}
              className={`relative aspect-video rounded-md overflow-hidden border-2 transition-all ${
                selectedCamera === camera
                  ? "border-primary ring-2 ring-primary/20"
                  : "border-transparent opacity-60 hover:opacity-100"
              }`}
              onClick={() => setSelectedCamera(camera)}
            >
              <div className="absolute inset-0 bg-muted flex items-center justify-center">
                <Camera className="h-4 w-4 text-muted-foreground" />
              </div>
              <div className="absolute bottom-0 left-0 right-0 bg-black/60 text-white text-xs py-0.5 text-center">
                {CAMERA_LABELS[camera]}
              </div>
            </button>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

export function VideoPlayerSkeleton() {
  return (
    <Card className="lg:col-span-2">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <Skeleton className="h-6 w-32" />
          <Skeleton className="h-5 w-20" />
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex gap-2">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-8 w-20" />
          ))}
        </div>
        <Skeleton className="aspect-video rounded-lg" />
        <div className="grid grid-cols-4 gap-2">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="aspect-video rounded-md" />
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

