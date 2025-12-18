"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  Area,
  ComposedChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Bar,
  Cell,
} from "recharts";
import { useState, useMemo, useRef, useEffect } from "react";
import type { SignalTimeSeriesOut, EventListOut } from "@/lib/api";

export interface CurrentSignalValues {
  speed_kmh: number;
  rpm: number;
  throttle_pct: number;
  brake_pressure: number;
  steering_angle: number;
}

interface SignalChartProps {
  signalData?: SignalTimeSeriesOut;
  eventData?: EventListOut;
  isLoading?: boolean;
  onWindowChange?: (windowStart: number, windowEnd: number) => void;
  onCurrentValuesChange?: (values: CurrentSignalValues) => void;
}

const chartConfig: ChartConfig = {
  speed_kmh: {
    label: "Speed (km/h)",
    color: "hsl(var(--chart-1))",
  },
  rpm: {
    label: "RPM",
    color: "hsl(var(--chart-2))",
  },
  throttle_pct: {
    label: "Throttle (%)",
    color: "hsl(var(--chart-3))",
  },
  brake_pressure: {
    label: "Brake (%)",
    color: "hsl(var(--chart-4))",
  },
  steering_angle: {
    label: "Steering (°)",
    color: "hsl(var(--chart-5))",
  },
};

type SignalKey = keyof typeof chartConfig;

// Sliding window duration in milliseconds (8 minutes)
const WINDOW_DURATION_MS = 8 * 60 * 1000;

// Event colors
const EVENT_COLORS = {
  hard_brake: "#ef4444",      // Red
  hard_acceleration: "#f59e0b", // Amber
  sharp_turn: "#8b5cf6",      // Purple
};

interface ChartDataPoint {
  timestamp: number;
  time: string;
  speed_kmh: number;
  rpm: number;
  throttle_pct: number;
  brake_pressure: number;
  steering_angle: number;
  // Event marker fields
  eventType?: string;
  eventColor?: string;
  eventBar?: number; // Height for event bar (150 = full height when event exists)
}

export function SignalChart({
  signalData,
  eventData,
  isLoading,
  onWindowChange,
  onCurrentValuesChange,
}: SignalChartProps) {
  const [activeSignal, setActiveSignal] = useState<SignalKey>("speed_kmh");
  const [displayData, setDisplayData] = useState<ChartDataPoint[]>([]);
  
  // Use refs to maintain animation state - never changes
  const animationRef = useRef<{
    allData: ChartDataPoint[];
    windowStart: number;
    dataStart: number;
    dataEnd: number;
    initialized: boolean;
    intervalId: number | null;
  }>({
    allData: [],
    windowStart: 0,
    dataStart: 0,
    dataEnd: 0,
    initialized: false,
    intervalId: null,
  });
  
  // Store callbacks in ref to avoid dependency issues
  const onWindowChangeRef = useRef(onWindowChange);
  onWindowChangeRef.current = onWindowChange;
  
  const onCurrentValuesChangeRef = useRef(onCurrentValuesChange);
  onCurrentValuesChangeRef.current = onCurrentValuesChange;

  // Process signal data when it changes
  useEffect(() => {
    if (!signalData?.signals?.length) return;

    // Convert all signals to chart data points
    const allPoints: ChartDataPoint[] = signalData.signals.map((signal) => ({
      timestamp: new Date(signal.timestamp).getTime(),
      time: new Date(signal.timestamp).toLocaleTimeString("ja-JP", {
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      }),
      speed_kmh: signal.speed_kmh ?? 0,
      rpm: signal.rpm ? signal.rpm / 100 : 0,
      throttle_pct: signal.throttle_pct ?? 0,
      brake_pressure: signal.brake_pressure ?? 0,
      steering_angle: signal.steering_angle
        ? (signal.steering_angle + 200) / 4
        : 50,
    }));

    // Sort by timestamp
    allPoints.sort((a, b) => a.timestamp - b.timestamp);
    
    // Update animation ref
    const anim = animationRef.current;
    anim.allData = allPoints;
    
    if (allPoints.length > 0) {
      anim.dataStart = allPoints[0].timestamp;
      anim.dataEnd = allPoints[allPoints.length - 1].timestamp;
      
      // Initialize window start only once
      if (!anim.initialized) {
        anim.windowStart = allPoints[0].timestamp;
        anim.initialized = true;
      }
    }
  }, [signalData]);

  // Store event data in ref for use in animation
  const eventDataRef = useRef(eventData);
  eventDataRef.current = eventData;

  // Sliding window animation - runs once on mount
  useEffect(() => {
    const slideSpeed = 1000; // 1 second real time
    const timeStep = 1000;   // 1 second of data

    // Helper function to find events within the window and mark nearest data points
    const markEventsOnData = (data: ChartDataPoint[], windowStart: number, windowEnd: number): ChartDataPoint[] => {
      const events = eventDataRef.current?.events ?? [];
      
      // Get events in current window
      const windowEvents = events.filter((event) => {
        const ts = new Date(event.timestamp).getTime();
        return ts >= windowStart && ts <= windowEnd;
      });
      
      if (windowEvents.length === 0) return data;
      
      // For each event, find the closest data point and mark it
      const markedIndices = new Map<number, { type: string; color: string }>();
      
      windowEvents.forEach((event) => {
        const eventTs = new Date(event.timestamp).getTime();
        
        // Find closest data point
        let closestIndex = 0;
        let closestDiff = Infinity;
        
        data.forEach((point, index) => {
          const diff = Math.abs(point.timestamp - eventTs);
          if (diff < closestDiff) {
            closestDiff = diff;
            closestIndex = index;
          }
        });
        
        // Only mark if within 5 seconds tolerance
        if (closestDiff <= 5000) {
          markedIndices.set(closestIndex, {
            type: event.event_type,
            color: EVENT_COLORS[event.event_type as keyof typeof EVENT_COLORS] || "#8b5cf6",
          });
        }
      });
      
      // Apply markers to data
      return data.map((point, index) => {
        const marker = markedIndices.get(index);
        if (marker) {
          return {
            ...point,
            eventType: marker.type,
            eventColor: marker.color,
            eventBar: 200, // Full height bar for events
          };
        }
        return { ...point, eventBar: 0 };
      });
    };

    const updateDisplay = () => {
      const anim = animationRef.current;
      if (anim.allData.length === 0) return;

      const windowStart = anim.windowStart;
      const windowEnd = windowStart + WINDOW_DURATION_MS;

      // Filter data for current window
      const windowData = anim.allData.filter(
        (point) => point.timestamp >= windowStart && point.timestamp <= windowEnd
      );
      
      // Mark events on data points
      const markedData = markEventsOnData(windowData, windowStart, windowEnd);

      setDisplayData(markedData);
      onWindowChangeRef.current?.(windowStart, windowEnd);
      
      // Emit current values (last point in window)
      if (markedData.length > 0) {
        const lastPoint = markedData[markedData.length - 1];
        onCurrentValuesChangeRef.current?.({
          speed_kmh: lastPoint.speed_kmh,
          rpm: lastPoint.rpm * 100, // Convert back from display scale
          throttle_pct: lastPoint.throttle_pct,
          brake_pressure: lastPoint.brake_pressure,
          steering_angle: lastPoint.steering_angle,
        });
      }
    };

    const animate = () => {
      const anim = animationRef.current;
      const dataDuration = anim.dataEnd - anim.dataStart;

      // If data fits in window, show all
      if (dataDuration <= 0 || dataDuration <= WINDOW_DURATION_MS) {
        if (anim.allData.length > 0) {
          setDisplayData(anim.allData);
        }
        return;
      }

      // Advance window
      const maxStart = anim.dataEnd - WINDOW_DURATION_MS;
      let newStart = anim.windowStart + timeStep;

      if (newStart > maxStart) {
        newStart = anim.dataStart; // Loop back
      }

      anim.windowStart = newStart;
      updateDisplay();
    };

    // Initial display
    updateDisplay();

    // Start animation
    const intervalId = window.setInterval(animate, slideSpeed);
    animationRef.current.intervalId = intervalId;

    return () => {
      window.clearInterval(intervalId);
    };
  }, []); // Run once on mount - events accessed via ref

  // Get the date from the first signal in YYYY/MM/DD format
  const dataDate = useMemo(() => {
    if (!signalData?.signals?.length) return null;
    const firstSignal = signalData.signals[0];
    const date = new Date(firstSignal.timestamp);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, "0");
    const day = String(date.getDate()).padStart(2, "0");
    return `${year}/${month}/${day}`;
  }, [signalData]);

  // Count events in current window for display
  const eventCount = useMemo(() => {
    return displayData.filter((point) => point.eventType).length;
  }, [displayData]);

  if (isLoading) {
    return (
      <Card className="col-span-2">
        <CardHeader>
          <Skeleton className="h-6 w-48" />
        </CardHeader>
        <CardContent>
          <Skeleton className="h-[300px] w-full" />
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="col-span-2">
      <CardHeader className="flex flex-row items-center justify-between">
        <div className="flex items-center gap-3">
          <CardTitle className="text-lg font-semibold">
            Signal Time Series
          </CardTitle>
          {dataDate && (
            <span className="text-sm text-muted-foreground bg-muted px-2 py-1 rounded">
              {dataDate}
            </span>
          )}
        </div>
        <Tabs
          value={activeSignal}
          onValueChange={(v) => setActiveSignal(v as SignalKey)}
        >
          <TabsList className="grid grid-cols-5">
            <TabsTrigger value="speed_kmh" className="text-xs">
              Speed
            </TabsTrigger>
            <TabsTrigger value="rpm" className="text-xs">
              RPM
            </TabsTrigger>
            <TabsTrigger value="throttle_pct" className="text-xs">
              Throttle
            </TabsTrigger>
            <TabsTrigger value="brake_pressure" className="text-xs">
              Brake
            </TabsTrigger>
            <TabsTrigger value="steering_angle" className="text-xs">
              Steering
            </TabsTrigger>
          </TabsList>
        </Tabs>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="h-[300px] w-full">
          <ComposedChart 
            data={displayData} 
            margin={{ left: 0, right: 0, top: 20 }}
          >
            <defs>
              <linearGradient
                id={`gradient-${activeSignal}`}
                x1="0"
                y1="0"
                x2="0"
                y2="1"
              >
                <stop
                  offset="5%"
                  stopColor={chartConfig[activeSignal].color}
                  stopOpacity={0.4}
                />
                <stop
                  offset="95%"
                  stopColor={chartConfig[activeSignal].color}
                  stopOpacity={0}
                />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" className="stroke-muted" />
            <XAxis
              dataKey="time"
              tick={{ fontSize: 10 }}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 10 }}
              tickLine={false}
              axisLine={false}
              width={40}
              domain={[0, 200]}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(label) => `Time: ${label}`}
                />
              }
            />
            {/* Event bars - thin colored bars for events */}
            <Bar 
              dataKey="eventBar" 
              barSize={3}
              isAnimationActive={false}
            >
              {displayData.map((entry, index) => (
                <Cell 
                  key={`cell-${index}`} 
                  fill={entry.eventColor || "transparent"}
                  stroke={entry.eventColor || "transparent"}
                  strokeWidth={entry.eventType ? 2 : 0}
                  strokeDasharray={entry.eventType ? "5 3" : "0"}
                />
              ))}
            </Bar>
            <Area
              type="monotone"
              dataKey={activeSignal}
              stroke={chartConfig[activeSignal].color}
              fill={`url(#gradient-${activeSignal})`}
              strokeWidth={2}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ChartContainer>
        {eventCount > 0 && (
          <div className="text-xs text-muted-foreground mt-2">
            ⚠️ {eventCount} event(s) in current window
          </div>
        )}
      </CardContent>
    </Card>
  );
}

export function SignalChartSkeleton() {
  return <SignalChart isLoading />;
}

