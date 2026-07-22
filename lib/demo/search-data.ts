export type DemoScalar = string | number | boolean | null;

export interface DemoEvent {
  id: string;
  time: string;
  timeLabel: string;
  raw: string;
  fields: Record<string, DemoScalar>;
}

export interface DemoFieldValue {
  value: DemoScalar;
  count: number;
}

export interface DemoField {
  name: string;
  displayName: string;
  distinctCount: number;
  eventCount: number;
  selected: boolean;
  interesting: boolean;
  type: "string" | "number" | "boolean";
  values: DemoFieldValue[];
}

export interface DemoSavedSearch {
  id: string;
  name: string;
  description: string;
  query: string;
  earliest: string;
  latest: string;
  updatedAt: string;
  owner: string;
}

export interface DemoHistoryEntry {
  id: string;
  query: string;
  timeRange: string;
  earliest?: string;
  latest?: string;
  state: "Completed" | "Canceled" | "Failed";
  events: number;
  duration: string;
  ranAt: string;
}

export interface TimelinePoint {
  id: string;
  label: string;
  count: number;
  /** Transforming searches can return one numeric value per split-by series. */
  series?: Record<string, number>;
  /** Absolute boundaries are populated by the backend adapter. */
  earliest?: string;
  latest?: string;
}

function raw(fields: Record<string, DemoScalar>): string {
  return JSON.stringify(fields);
}

function makeEvent(id: string, time: string, fields: Record<string, DemoScalar>): DemoEvent {
  const date = new Date(time);
  return {
    id,
    time,
    timeLabel: new Intl.DateTimeFormat("en-US", {
      month: "numeric",
      day: "numeric",
      year: "2-digit",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
    }).format(date),
    raw: raw({ timestamp: time, ...fields }),
    fields: {
      _time: time,
      index: "gradethis",
      host: "api-prod-03",
      source: "/var/log/gradethis/app.json",
      sourcetype: "go:zap:json",
      environment: "production",
      service: "gradethis-api",
      ...fields,
    },
  };
}

export const DEMO_EVENTS: DemoEvent[] = [
  makeEvent("evt-10492", "2026-07-21T22:42:17.483Z", {
    level: "ERROR",
    logger: "submission-service",
    layer: "service",
    message: "Database query failed while loading submission",
    method: "POST",
    path: "/api/v1/submissions/grade",
    status: 500,
    duration_ms: 827,
    trace_id: "4b9f0f06d2cc47c89bd04ce9a7318fd1",
    submission_id: "sub_01J1QF8R3NZK5Y0PKQ4V4TQW6D",
    authorization: "[REDACTED]",
  }),
  makeEvent("evt-10491", "2026-07-21T22:41:54.219Z", {
    level: "WARN",
    logger: "request-middleware",
    layer: "transport",
    message: "Request completed above latency threshold",
    method: "GET",
    path: "/api/v1/courses/course_9/gradebook",
    status: 200,
    duration_ms: 1842,
    trace_id: "c1034628acf44f0e94392c359074fa39",
    user_id: "usr_8W4H20",
  }),
  makeEvent("evt-10490", "2026-07-21T22:40:31.907Z", {
    level: "ERROR",
    logger: "notification-worker",
    layer: "worker",
    message: "connection refused while delivering email notification",
    operation: "send-grade-released",
    retry_count: 3,
    queue: "notifications",
    trace_id: "06e5d19dd26b4ca19731202a0c59f21f",
  }),
  makeEvent("evt-10489", "2026-07-21T22:38:12.663Z", {
    level: "INFO",
    logger: "request-middleware",
    layer: "transport",
    message: "Request metrics",
    method: "POST",
    path: "/api/v1/submissions",
    status: 201,
    duration_ms: 94,
    bytes: 2841,
    trace_id: "15ad0c6f76f24d9e9453012ef34f8518",
  }),
  makeEvent("evt-10488", "2026-07-21T22:36:08.115Z", {
    level: "INFO",
    logger: "grading-service",
    layer: "service",
    message: "Submission queued for AI-assisted grading",
    course_id: "course_9",
    assignment_id: "asg_42",
    submission_id: "sub_01J1QF6MW8F5Y49NRN7K9B8P3J",
    trace_id: "739a2c6d01d14130af4d8f9a1b4a10f2",
  }),
  makeEvent("evt-10487", "2026-07-21T22:34:45.774Z", {
    level: "WARN",
    logger: "rate-limiter",
    layer: "middleware",
    message: "Client approaching request limit",
    method: "POST",
    path: "/api/v1/copilot/messages",
    status: 200,
    limit: 120,
    remaining: 7,
    trace_id: "58a8e96b170542249419706076788af4",
  }),
  makeEvent("evt-10486", "2026-07-21T22:31:27.402Z", {
    level: "ERROR",
    logger: "panic-recovery",
    layer: "transport",
    message: "Recovered panic: invalid memory address <request body omitted>",
    method: "PATCH",
    path: "/api/v1/rubrics/rub_18",
    status: 500,
    duration_ms: 35,
    trace_id: "fbc6d5bd307840099e94ef7554fcf903",
  }),
  makeEvent("evt-10485", "2026-07-21T22:29:02.950Z", {
    level: "INFO",
    logger: "realtime-hub",
    layer: "websocket",
    message: "Search progress subscriber connected",
    connection_id: "ws_9CQ7J2",
    channel: "grading-progress",
    trace_id: "6f57826f2153419a99462a44ab02fcde",
  }),
  makeEvent("evt-10484", "2026-07-21T22:26:18.006Z", {
    level: "WARN",
    logger: "session-service",
    layer: "service",
    message: "Stale session refresh token rejected",
    method: "POST",
    path: "/api/v1/auth/refresh",
    status: 401,
    duration_ms: 18,
    trace_id: "a5ae0434e4d0415aa47ad5f4d018772f",
  }),
  makeEvent("evt-10483", "2026-07-21T22:22:39.331Z", {
    level: "INFO",
    logger: "notification-worker",
    layer: "worker",
    message: "Grade release notification delivered",
    operation: "send-grade-released",
    retry_count: 0,
    duration_ms: 312,
    trace_id: "313a4c334a684a6a87e11caab061f4c2",
  }),
];

export const DEMO_FIELDS: DemoField[] = [
  {
    name: "host",
    displayName: "host",
    distinctCount: 3,
    eventCount: 12_846,
    selected: true,
    interesting: false,
    type: "string",
    values: [
      { value: "api-prod-03", count: 6942 },
      { value: "worker-prod-02", count: 3711 },
      { value: "api-prod-02", count: 2193 },
    ],
  },
  {
    name: "source",
    displayName: "source",
    distinctCount: 4,
    eventCount: 12_846,
    selected: true,
    interesting: false,
    type: "string",
    values: [
      { value: "/var/log/gradethis/app.json", count: 8420 },
      { value: "/var/log/gradethis/worker.json", count: 3204 },
      { value: "/var/log/gradethis/jobs.json", count: 1222 },
    ],
  },
  {
    name: "sourcetype",
    displayName: "sourcetype",
    distinctCount: 2,
    eventCount: 12_846,
    selected: true,
    interesting: false,
    type: "string",
    values: [
      { value: "go:zap:json", count: 12_101 },
      { value: "app:raw", count: 745 },
    ],
  },
  {
    name: "level",
    displayName: "level",
    distinctCount: 4,
    eventCount: 12_846,
    selected: true,
    interesting: false,
    type: "string",
    values: [
      { value: "INFO", count: 8917 },
      { value: "WARN", count: 2491 },
      { value: "ERROR", count: 1432 },
      { value: "DEBUG", count: 6 },
    ],
  },
  {
    name: "trace_id",
    displayName: "trace_id",
    distinctCount: 10_284,
    eventCount: 11_903,
    selected: true,
    interesting: false,
    type: "string",
    values: [
      { value: "4b9f0f06d2cc47c89bd04ce9a7318fd1", count: 18 },
      { value: "c1034628acf44f0e94392c359074fa39", count: 14 },
      { value: "15ad0c6f76f24d9e9453012ef34f8518", count: 12 },
    ],
  },
  {
    name: "path",
    displayName: "path",
    distinctCount: 42,
    eventCount: 9427,
    selected: false,
    interesting: true,
    type: "string",
    values: [
      { value: "/api/v1/submissions/grade", count: 1923 },
      { value: "/api/v1/submissions", count: 1438 },
      { value: "/api/v1/courses/course_9/gradebook", count: 1180 },
    ],
  },
  {
    name: "status",
    displayName: "status",
    distinctCount: 7,
    eventCount: 9427,
    selected: false,
    interesting: true,
    type: "number",
    values: [
      { value: 200, count: 6118 },
      { value: 201, count: 1331 },
      { value: 500, count: 812 },
      { value: 401, count: 604 },
    ],
  },
  {
    name: "method",
    displayName: "method",
    distinctCount: 5,
    eventCount: 9427,
    selected: false,
    interesting: true,
    type: "string",
    values: [
      { value: "GET", count: 5140 },
      { value: "POST", count: 3319 },
      { value: "PATCH", count: 626 },
    ],
  },
  {
    name: "duration_ms",
    displayName: "duration_ms",
    distinctCount: 1781,
    eventCount: 9427,
    selected: false,
    interesting: true,
    type: "number",
    values: [
      { value: 18, count: 144 },
      { value: 21, count: 137 },
      { value: 24, count: 129 },
    ],
  },
  {
    name: "logger",
    displayName: "logger",
    distinctCount: 18,
    eventCount: 12_846,
    selected: false,
    interesting: true,
    type: "string",
    values: [
      { value: "request-middleware", count: 6728 },
      { value: "submission-service", count: 1843 },
      { value: "notification-worker", count: 972 },
    ],
  },
  {
    name: "layer",
    displayName: "layer",
    distinctCount: 5,
    eventCount: 12_846,
    selected: false,
    interesting: true,
    type: "string",
    values: [
      { value: "transport", count: 7192 },
      { value: "service", count: 3861 },
      { value: "worker", count: 1193 },
    ],
  },
  {
    name: "service",
    displayName: "service",
    distinctCount: 3,
    eventCount: 12_846,
    selected: false,
    interesting: true,
    type: "string",
    values: [
      { value: "gradethis-api", count: 9731 },
      { value: "gradethis-worker", count: 2941 },
      { value: "gradethis-web", count: 174 },
    ],
  },
  {
    name: "environment",
    displayName: "environment",
    distinctCount: 2,
    eventCount: 12_846,
    selected: false,
    interesting: true,
    type: "string",
    values: [
      { value: "production", count: 11_983 },
      { value: "staging", count: 863 },
    ],
  },
  {
    name: "bytes",
    displayName: "bytes",
    distinctCount: 1420,
    eventCount: 9427,
    selected: false,
    interesting: true,
    type: "number",
    values: [
      { value: 2841, count: 82 },
      { value: 1150, count: 78 },
      { value: 842, count: 71 },
    ],
  },
];

export const DEMO_TIMELINE: TimelinePoint[] = Array.from({ length: 72 }, (_, index) => {
  const wave = Math.sin(index / 4.8) * 54 + Math.cos(index / 2.7) * 27;
  const spike = index === 18 ? 190 : index === 47 ? 265 : index > 58 && index < 64 ? 96 : 0;
  const count = Math.max(18, Math.round(108 + wave + spike + ((index * 29) % 41)));
  const minute = index * 20;
  const labelDate = new Date(Date.UTC(2026, 6, 21, 0, minute));
  return {
    id: `bucket-${index}`,
    label: new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    }).format(labelDate),
    count,
  };
});

export const DEMO_SAVED_SEARCHES: DemoSavedSearch[] = [
  {
    id: "saved-errors",
    name: "Production errors by service",
    description: "Errors and warnings across GradeThis production services.",
    query: "index=gradethis environment=production (level=ERROR OR level=WARN)\n| sort -_time",
    earliest: "-24h",
    latest: "now",
    updatedAt: "Today, 2:18 PM",
    owner: "admin",
  },
  {
    id: "saved-slow-routes",
    name: "Slow API routes",
    description: "Routes whose p95 latency exceeds 500 ms.",
    query:
      'index=gradethis message="Request metrics"\n| stats count p95(duration_ms) as p95_ms by path\n| where p95_ms > 500',
    earliest: "-7d",
    latest: "now",
    updatedAt: "Yesterday, 4:42 PM",
    owner: "admin",
  },
  {
    id: "saved-error-volume",
    name: "Error volume over time",
    description: "Five-minute error volume split by service.",
    query: "index=gradethis level=ERROR\n| timechart span=5m count by service",
    earliest: "-4h",
    latest: "now",
    updatedAt: "Jul 19, 11:06 AM",
    owner: "admin",
  },
];

export const DEMO_HISTORY: DemoHistoryEntry[] = [
  {
    id: "hist-901",
    query: "index=gradethis (level=ERROR OR status>=500) | sort -_time",
    timeRange: "Last 24 hours",
    state: "Completed",
    events: 12_846,
    duration: "1.82 s",
    ranAt: "Today, 3:42 PM",
  },
  {
    id: "hist-900",
    query: 'index=gradethis trace_id="4b9f0f06d2cc47c89bd04ce9a7318fd1" | sort _time',
    timeRange: "Last 60 minutes",
    state: "Completed",
    events: 18,
    duration: "384 ms",
    ranAt: "Today, 3:31 PM",
  },
  {
    id: "hist-899",
    query: "index=gradethis | transaction trace_id",
    timeRange: "Last 24 hours",
    state: "Failed",
    events: 0,
    duration: "22 ms",
    ranAt: "Today, 3:12 PM",
  },
  {
    id: "hist-898",
    query: 'index=gradethis message="Request metrics" | stats count by path, status',
    timeRange: "Last 7 days",
    state: "Canceled",
    events: 0,
    duration: "3.04 s",
    ranAt: "Today, 2:57 PM",
  },
];

export const DEMO_STATISTICS = [
  { level: "INFO", count: 8917, percent: "69.4%", avgDuration: 74.2 },
  { level: "WARN", count: 2491, percent: "19.4%", avgDuration: 438.7 },
  { level: "ERROR", count: 1432, percent: "11.1%", avgDuration: 682.3 },
  { level: "DEBUG", count: 6, percent: "0.1%", avgDuration: 12.1 },
];

export const DEMO_PATTERNS = [
  { signature: "Request metrics status=2** duration_ms=*", count: 6281, percent: 48.9 },
  { signature: "Submission * for grading", count: 2174, percent: 16.9 },
  { signature: "Request completed above latency threshold", count: 1608, percent: 12.5 },
  { signature: "Database query failed while *", count: 812, percent: 6.3 },
  { signature: "connection refused while delivering *", count: 391, percent: 3.0 },
];
