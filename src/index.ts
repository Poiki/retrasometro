import { config } from "./config";
import { DB } from "./db";
import { RenfeIngestor } from "./ingestor";
import { DashboardPresetCache } from "./dashboard-cache";
import { OpsPresetCache } from "./ops-cache";
import { startServer } from "./server";
import type { RunAnalytics } from "./analytics-worker";

const db = new DB(config.dbPath);
const worker = new Worker(new URL("./analytics-worker.ts", import.meta.url).href, { env: { ...process.env } as Record<string, string> });
const events = new EventTarget();
let sequence = 0;
const pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>();
const runAnalytics: RunAnalytics = (method, ...args) => new Promise((resolve, reject) => {
  const id = ++sequence;
  pending.set(id, { resolve: resolve as (value: unknown) => void, reject });
  worker.postMessage({ id, method, args });
});
worker.onmessage = (event: MessageEvent<{ id?: number; result?: unknown; error?: string; type?: string }>) => {
  if (event.data.type === "ready") { events.dispatchEvent(new Event("history")); return; }
  const job = pending.get(event.data.id!);
  if (job) {
    pending.delete(event.data.id!);
    if (event.data.error) job.reject(new Error(event.data.error));
    else job.resolve(event.data.result);
  }
};
worker.onerror = (error) => {
  console.error(`[analytics] ${error.message}`);
  for (const job of pending.values()) job.reject(new Error(error.message));
  process.exit(1);
};
const dashboardPresetCache = new DashboardPresetCache(db, (hours) => runAnalytics("dashboard", hours));
const opsPresetCache = new OpsPresetCache(db, (hours) => runAnalytics("ops", hours));
const ingestor = new RenfeIngestor(db, () => {
  events.dispatchEvent(new Event("update"));
  worker.postMessage({ method: "refresh", args: [] });
});

const server = startServer(db, ingestor, dashboardPresetCache, opsPresetCache, runAnalytics, events);
void ingestor.start().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[startup] ingestor init error ${message}`);
});

const shutdown = () => {
  console.log("[shutdown] stopping server");
  ingestor.stop();
  server.stop();
  worker.terminate();
  db.close();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
