import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Activity, CheckCircle2, AlertCircle, Radio } from "lucide-react";
import type { CANQualityOut } from "@/lib/api";

interface QualityViewProps {
  data?: CANQualityOut;
  isLoading?: boolean;
}

export function QualityView({ data, isLoading }: QualityViewProps) {
  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <Skeleton className="h-6 w-48" />
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="space-y-2">
                <div className="flex justify-between">
                  <Skeleton className="h-4 w-32" />
                  <Skeleton className="h-4 w-16" />
                </div>
                <Skeleton className="h-2 w-full" />
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  const overallHealth = (data?.overall_health ?? 1) * 100;
  const healthStatus =
    overallHealth >= 98
      ? "excellent"
      : overallHealth >= 95
        ? "good"
        : overallHealth >= 90
          ? "warning"
          : "critical";

  const statusConfig = {
    excellent: {
      label: "Excellent",
      color: "text-emerald-500",
      bgColor: "bg-emerald-500",
      icon: CheckCircle2,
    },
    good: {
      label: "Good",
      color: "text-blue-500",
      bgColor: "bg-blue-500",
      icon: CheckCircle2,
    },
    warning: {
      label: "Warning",
      color: "text-amber-500",
      bgColor: "bg-amber-500",
      icon: AlertCircle,
    },
    critical: {
      label: "Critical",
      color: "text-rose-500",
      bgColor: "bg-rose-500",
      icon: AlertCircle,
    },
  };

  const status = statusConfig[healthStatus];
  const StatusIcon = status.icon;

  return (
    <Card>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <Radio className="h-5 w-5" />
            CAN Quality
          </CardTitle>
          <Badge
            variant="outline"
            className={`${status.color} border-current`}
          >
            <StatusIcon className="h-3 w-3 mr-1" />
            {status.label}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Overall Health */}
        <div className="space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Overall Health</span>
            <span className={`font-semibold ${status.color}`}>
              {overallHealth.toFixed(1)}%
            </span>
          </div>
          <Progress value={overallHealth} className="h-2" />
        </div>

        {/* Per-Message Metrics */}
        <div className="space-y-3 pt-2">
          {data?.metrics?.map((metric, i) => {
            const successRate = (1 - (metric.missing_rate ?? 0)) * 100;
            const isHealthy = successRate >= 95;

            return (
              <div key={i} className="space-y-1.5">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <Activity
                      className={`h-3.5 w-3.5 ${isHealthy ? "text-emerald-500" : "text-amber-500"}`}
                    />
                    <span className="text-sm font-medium">
                      {metric.message_name}
                    </span>
                    <span className="text-xs text-muted-foreground font-mono">
                      0x{metric.arb_id.toString(16).toUpperCase()}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-xs">
                    <span className="text-muted-foreground">
                      {metric.message_count}/{metric.expected_count}
                    </span>
                    <span
                      className={
                        isHealthy ? "text-emerald-500" : "text-amber-500"
                      }
                    >
                      {successRate.toFixed(1)}%
                    </span>
                  </div>
                </div>
                <Progress
                  value={successRate}
                  className="h-1.5"
                />
              </div>
            );
          })}
        </div>

        {/* Period Info */}
        {data?.window_start && data?.window_end && (
          <div className="pt-2 border-t text-xs text-muted-foreground">
            Window: {new Date(data.window_start).toLocaleTimeString()} -{" "}
            {new Date(data.window_end).toLocaleTimeString()}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export function QualityViewSkeleton() {
  return <QualityView isLoading />;
}

