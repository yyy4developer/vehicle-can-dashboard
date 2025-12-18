import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Gauge,
  Activity,
  AlertTriangle,
  Navigation,
  Fuel,
  Timer,
} from "lucide-react";
import type { VehicleStatsSummaryOut } from "@/lib/api";

export interface CurrentValues {
  speed_kmh?: number;
  rpm?: number;
  throttle_pct?: number;
  brake_pressure?: number;
  steering_angle?: number;
}

interface StatsCardsProps {
  data?: VehicleStatsSummaryOut;
  currentValues?: CurrentValues;
  isLoading?: boolean;
}

export function StatsCards({ data, currentValues, isLoading }: StatsCardsProps) {
  // Use currentValues (from sliding window) when available, fallback to data (from API)
  const cards = [
    {
      title: "Current Speed",
      value: currentValues?.speed_kmh ?? data?.current_speed_kmh ?? 0,
      unit: "km/h",
      icon: Gauge,
      color: "text-emerald-500",
      bgColor: "bg-emerald-500/10",
    },
    {
      title: "RPM",
      value: currentValues?.rpm ?? data?.current_rpm ?? 0,
      unit: "rpm",
      icon: Activity,
      color: "text-blue-500",
      bgColor: "bg-blue-500/10",
    },
    {
      title: "Throttle",
      value: currentValues?.throttle_pct ?? data?.current_throttle_pct ?? 0,
      unit: "%",
      icon: Fuel,
      color: "text-amber-500",
      bgColor: "bg-amber-500/10",
    },
    {
      title: "Steering",
      value: currentValues?.steering_angle ?? data?.current_steering_angle ?? 0,
      unit: "°",
      icon: Navigation,
      color: "text-purple-500",
      bgColor: "bg-purple-500/10",
    },
    {
      title: "Events",
      value: data?.total_events ?? 0,
      unit: "today",
      icon: AlertTriangle,
      color: "text-rose-500",
      bgColor: "bg-rose-500/10",
    },
    {
      title: "Distance",
      value: data?.distance_km ?? 0,
      unit: "km",
      icon: Timer,
      color: "text-cyan-500",
      bgColor: "bg-cyan-500/10",
    },
  ];

  if (isLoading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        {cards.map((_, i) => (
          <Card key={i} className="relative overflow-hidden">
            <CardHeader className="flex flex-row items-center justify-between pb-2">
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-4 w-4 rounded" />
            </CardHeader>
            <CardContent>
              <Skeleton className="h-8 w-24" />
            </CardContent>
          </Card>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
      {cards.map((cardItem, i) => {
        const Icon = cardItem.icon;
        return (
          <Card key={i} className="relative overflow-hidden">
            <div
              className={`absolute inset-0 ${cardItem.bgColor} opacity-50`}
              style={{
                clipPath: "polygon(60% 0%, 100% 0%, 100% 100%, 40% 100%)",
              }}
            />
            <CardHeader className="flex flex-row items-center justify-between pb-2 relative">
              <CardTitle className="text-sm font-medium text-muted-foreground">
                {cardItem.title}
              </CardTitle>
              <Icon className={`h-4 w-4 ${cardItem.color}`} />
            </CardHeader>
            <CardContent className="relative">
              <div className="text-2xl font-bold">
                {typeof cardItem.value === "number"
                  ? cardItem.value.toLocaleString(undefined, {
                      maximumFractionDigits: 1,
                    })
                  : cardItem.value}
                <span className="text-sm font-normal text-muted-foreground ml-1">
                  {cardItem.unit}
                </span>
              </div>
            </CardContent>
          </Card>
        );
      })}
    </div>
  );
}

export function StatsCardsSkeleton() {
  return <StatsCards isLoading />;
}

