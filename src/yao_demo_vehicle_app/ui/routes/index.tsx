import { createFileRoute } from "@tanstack/react-router";
import { Suspense, useEffect, useState, useCallback } from "react";
import { ErrorBoundary } from "react-error-boundary";
import Navbar from "@/components/apx/navbar";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
  StatsCards,
  StatsCardsSkeleton,
  SignalChart,
  SignalChartSkeleton,
  EventTimeline,
  EventTimelineSkeleton,
  QualityView,
  QualityViewSkeleton,
  VideoPlayer,
  VideoPlayerSkeleton,
} from "@/components/dashboard";
import {
  RefreshCw,
  Car,
  AlertTriangle,
} from "lucide-react";
import {
  getStatsSummary,
  getSignals,
  getEvents,
  getQuality,
} from "@/lib/api";
import type {
  VehicleStatsSummaryOut,
  SignalTimeSeriesOut,
  EventListOut,
  CANQualityOut,
  TimeRange,
} from "@/lib/api";

export const Route = createFileRoute("/")({
  component: () => <Dashboard />,
});

// ===========================================
// Time Range Mapping
// ===========================================

function mapTimeRange(range: TimeRangeType): TimeRange {
  switch (range) {
    case "10m":
      return "10m";
    case "1h":
      return "1h";
    case "today":
      return "today";
    default:
      return "10m";
  }
}

// ===========================================
// Dashboard Component
// ===========================================

type TimeRangeType = "10m" | "1h" | "today";

function Dashboard() {
  const [timeRange, setTimeRange] = useState<TimeRangeType>("10m");
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(new Date());
  const [isLoading, setIsLoading] = useState(true);

  // API data state
  const [stats, setStats] = useState<VehicleStatsSummaryOut | undefined>();
  const [signals, setSignals] = useState<SignalTimeSeriesOut | undefined>();
  const [events, setEvents] = useState<EventListOut | undefined>();
  const [quality, setQuality] = useState<CANQualityOut | undefined>();
  
  // Sliding window state for synchronizing signal chart and event timeline
  const [signalWindow, setSignalWindow] = useState<{ start: number; end: number }>({ start: 0, end: 0 });
  
  // Current signal values from sliding window (for real-time stats display)
  const [currentValues, setCurrentValues] = useState<{
    speed_kmh: number;
    rpm: number;
    throttle_pct: number;
    brake_pressure: number;
    steering_angle: number;
  } | undefined>();
  
  const handleWindowChange = useCallback((windowStart: number, windowEnd: number) => {
    setSignalWindow({ start: windowStart, end: windowEnd });
  }, []);
  
  const handleCurrentValuesChange = useCallback((values: {
    speed_kmh: number;
    rpm: number;
    throttle_pct: number;
    brake_pressure: number;
    steering_angle: number;
  }) => {
    setCurrentValues(values);
  }, []);

  // Load data from API
  const loadData = useCallback(async () => {
    try {
      const apiTimeRange = mapTimeRange(timeRange);
      
      const [statsRes, signalsRes, eventsRes, qualityRes] = await Promise.all([
        getStatsSummary({}).catch((e) => { console.error("Stats error:", e); return null; }),
        getSignals({ time_range: apiTimeRange }).catch((e) => { console.error("Signals error:", e); return null; }),
        getEvents({ time_range: apiTimeRange }).catch((e) => { console.error("Events error:", e); return null; }),
        getQuality({}).catch((e) => { console.error("Quality error:", e); return null; }),
      ]);

      // Extract data from AxiosResponse
      if (statsRes?.data) setStats(statsRes.data);
      if (signalsRes?.data) setSignals(signalsRes.data);
      if (eventsRes?.data) setEvents(eventsRes.data);
      if (qualityRes?.data) setQuality(qualityRes.data);
      
      setLastUpdate(new Date());
    } catch (error) {
      console.error("Failed to load data:", error);
    } finally {
      setIsLoading(false);
    }
  }, [timeRange]);

  // Initial load and auto-refresh
  useEffect(() => {
    loadData();
    // Refresh every 30 seconds (signals data is historical, no need for frequent updates)
    const interval = setInterval(loadData, 30000);
    return () => clearInterval(interval);
  }, [loadData]);

  const handleRefresh = async () => {
    setIsRefreshing(true);
    await loadData();
    setIsRefreshing(false);
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-background via-background to-muted/20">
      <Navbar />

      <main className="container mx-auto px-4 py-6 space-y-6">
        {/* Header */}
        <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
          <div className="flex items-center gap-3">
            <div className="p-2 rounded-lg bg-primary/10">
              <Car className="h-6 w-6 text-primary" />
            </div>
            <div>
              <h1 className="text-2xl font-bold tracking-tight">
                Vehicle Dashboard
              </h1>
              <p className="text-sm text-muted-foreground">
                CAN Data Monitoring • VH001
              </p>
            </div>
          </div>

          <div className="flex items-center gap-3">
            <Tabs
              value={timeRange}
              onValueChange={(v) => setTimeRange(v as TimeRangeType)}
            >
              <TabsList>
                <TabsTrigger value="10m">10 Min</TabsTrigger>
                <TabsTrigger value="1h">1 Hour</TabsTrigger>
                <TabsTrigger value="today">Today</TabsTrigger>
              </TabsList>
            </Tabs>

            <Button
              variant="outline"
              size="icon"
              onClick={handleRefresh}
              disabled={isRefreshing}
            >
              <RefreshCw
                className={`h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`}
              />
            </Button>
          </div>
        </div>

        {/* Stats Cards */}
        <ErrorBoundary
          fallback={
            <Card className="p-4 border-destructive">
              <div className="flex items-center gap-2 text-destructive">
                <AlertTriangle className="h-5 w-5" />
                <span>Failed to load statistics</span>
              </div>
            </Card>
          }
        >
          <Suspense fallback={<StatsCardsSkeleton />}>
            <StatsCards data={stats} currentValues={currentValues} />
          </Suspense>
        </ErrorBoundary>

        {/* Main Content Grid */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Signal Chart - spans 2 columns */}
          <ErrorBoundary
            fallback={
              <Card className="col-span-2 p-4 border-destructive">
                <div className="flex items-center gap-2 text-destructive">
                  <AlertTriangle className="h-5 w-5" />
                  <span>Failed to load chart</span>
                </div>
              </Card>
            }
          >
            <Suspense fallback={<SignalChartSkeleton />}>
              <SignalChart 
                signalData={signals} 
                eventData={events} 
                onWindowChange={handleWindowChange}
                onCurrentValuesChange={handleCurrentValuesChange}
              />
            </Suspense>
          </ErrorBoundary>

          {/* Event Timeline - synchronized with signal chart window */}
          <ErrorBoundary
            fallback={
              <Card className="p-4 border-destructive">
                <div className="flex items-center gap-2 text-destructive">
                  <AlertTriangle className="h-5 w-5" />
                  <span>Failed to load events</span>
                </div>
              </Card>
            }
          >
            <Suspense fallback={<EventTimelineSkeleton />}>
              <EventTimeline 
                data={events} 
                windowStart={signalWindow.start}
                windowEnd={signalWindow.end}
              />
            </Suspense>
          </ErrorBoundary>
        </div>

        {/* Bottom Row */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Video Player */}
          <ErrorBoundary
            fallback={
              <Card className="lg:col-span-2 p-4 border-destructive">
                <div className="flex items-center gap-2 text-destructive">
                  <AlertTriangle className="h-5 w-5" />
                  <span>動画プレーヤーの読み込みに失敗しました</span>
                </div>
              </Card>
            }
          >
            <Suspense fallback={<VideoPlayerSkeleton />}>
              <VideoPlayer vehicleId="VH001" />
            </Suspense>
          </ErrorBoundary>

          {/* Quality View */}
          <ErrorBoundary
            fallback={
              <Card className="p-4 border-destructive">
                <div className="flex items-center gap-2 text-destructive">
                  <AlertTriangle className="h-5 w-5" />
                  <span>Failed to load quality metrics</span>
                </div>
              </Card>
            }
          >
            <Suspense fallback={<QualityViewSkeleton />}>
              <QualityView data={quality} />
            </Suspense>
          </ErrorBoundary>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-between text-xs text-muted-foreground pt-4 border-t">
          <span>
            Last updated: {lastUpdate.toLocaleTimeString("ja-JP")}
          </span>
          <a
            href="https://github.com/databricks-solutions/apx"
            target="_blank"
            rel="noopener noreferrer"
            className="hover:text-foreground transition-colors"
          >
            Built with apx
          </a>
        </div>
      </main>
    </div>
  );
}
