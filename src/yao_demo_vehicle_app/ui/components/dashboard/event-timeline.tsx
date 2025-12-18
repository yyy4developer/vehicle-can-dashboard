import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  AlertOctagon,
  Zap,
  RotateCcw,
  Clock,
  Gauge,
  Navigation,
} from "lucide-react";
import type { EventListOut, EventType } from "@/lib/api";

interface EventTimelineProps {
  data?: EventListOut;
  isLoading?: boolean;
  windowStart?: number;
  windowEnd?: number;
}

const eventConfig: Record<
  EventType,
  {
    label: string;
    icon: typeof AlertOctagon;
    color: string;
    bgColor: string;
  }
> = {
  hard_brake: {
    label: "Hard Brake",
    icon: AlertOctagon,
    color: "text-rose-500",
    bgColor: "bg-rose-500/10",
  },
  hard_acceleration: {
    label: "Hard Accel",
    icon: Zap,
    color: "text-amber-500",
    bgColor: "bg-amber-500/10",
  },
  sharp_turn: {
    label: "Sharp Turn",
    icon: RotateCcw,
    color: "text-purple-500",
    bgColor: "bg-purple-500/10",
  },
};

export function EventTimeline({ data, isLoading, windowStart, windowEnd }: EventTimelineProps) {
  if (isLoading) {
    return (
      <Card className="h-full">
        <CardHeader>
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <Clock className="h-5 w-5" />
            Event Timeline
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="flex items-start gap-3">
                <Skeleton className="h-8 w-8 rounded-full" />
                <div className="flex-1 space-y-2">
                  <Skeleton className="h-4 w-24" />
                  <Skeleton className="h-3 w-32" />
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  // Show all events - sorted by timestamp (newest first)
  const events = [...(data?.events ?? [])].sort((a, b) => 
    new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
  );

  return (
    <Card className="h-full flex flex-col">
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <Clock className="h-5 w-5" />
            Event Timeline
          </CardTitle>
          <Badge variant="secondary" className="font-mono">
            {data?.total ?? 0} events
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="flex-1 min-h-0">
        <ScrollArea className="h-[280px] pr-4">
          {events.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-muted-foreground">
              <AlertOctagon className="h-8 w-8 mb-2 opacity-50" />
              <p className="text-sm">No events detected</p>
            </div>
          ) : (
            <div className="space-y-3">
              {events.map((event, i) => {
                const config = eventConfig[event.event_type as EventType];
                const Icon = config?.icon ?? AlertOctagon;
                const time = new Date(event.timestamp).toLocaleTimeString(
                  "ja-JP",
                  {
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                  }
                );

                return (
                  <div
                    key={event.id || i}
                    className={`flex items-start gap-3 p-3 rounded-lg ${config?.bgColor ?? "bg-muted"} transition-colors hover:opacity-80`}
                  >
                    <div
                      className={`p-2 rounded-full bg-background ${config?.color}`}
                    >
                      <Icon className="h-4 w-4" />
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between gap-2">
                        <span className={`font-medium ${config?.color}`}>
                          {config?.label ?? event.event_type}
                        </span>
                        <span className="text-xs text-muted-foreground font-mono">
                          {time}
                        </span>
                      </div>
                      <div className="flex items-center gap-3 mt-1 text-xs text-muted-foreground">
                        {event.speed_kmh !== null && (
                          <span className="flex items-center gap-1">
                            <Gauge className="h-3 w-3" />
                            {event.speed_kmh?.toFixed(1)} km/h
                          </span>
                        )}
                        {event.steering_angle !== null && (
                          <span className="flex items-center gap-1">
                            <Navigation className="h-3 w-3" />
                            {event.steering_angle?.toFixed(0)}°
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

export function EventTimelineSkeleton() {
  return <EventTimeline isLoading />;
}

