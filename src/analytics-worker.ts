import { config } from "./config";
import { DB } from "./db";
import { DashboardPresetCache, type DashboardHistoricalCacheEntry } from "./dashboard-cache";
import { OpsPresetCache, type OpsCacheEntry } from "./ops-cache";
declare const self: Worker;

const queries = ["getHistoricalStats", "getHistoricalStatsCustom", "getTrainHistory", "getHistoryCoverage", "recoverObservationsFromSnapshots", "getCorridorMeta", "getObservationArchiveStorage"] as const;
type Query = (typeof queries)[number];
type Tasks = { [M in Query]: { args: Parameters<DB[M]>; result: ReturnType<DB[M]> } } & {
  dashboard: { args: [24 | 168 | 720]; result: DashboardHistoricalCacheEntry };
  ops: { args: [24 | 168 | 720]; result: OpsCacheEntry };
};
export type RunAnalytics = <M extends keyof Tasks>(method: M, ...args: Tasks[M]["args"]) => Promise<Tasks[M]["result"]>;

const db = new DB(config.dbPath);
const dashboard = new DashboardPresetCache(db);
const ops = new OpsPresetCache(db);
let queue = Promise.resolve();
let refreshQueued = false;

const refresh = () => {
  if (refreshQueued) return;
  refreshQueued = true;
  queue = queue.then(async () => {
    db.runOpsMaterializationTick();
    await dashboard.refreshDue();
    await ops.refreshDue();
    postMessage({ type: "ready" });
  }).catch((error) => {
    console.error(`[analytics] ${error instanceof Error ? error.message : String(error)}`);
  }).finally(() => { refreshQueued = false; });
};


self.onmessage = (event: MessageEvent<{ id: number; method: keyof Tasks | "refresh"; args: unknown[] }>) => {
  const { id, method, args } = event.data;
  if (method === "refresh") { refresh(); return; }
  queue = queue.then(async () => {
    try {
      let result: unknown;
      if (method === "dashboard") result = await dashboard.generate(args[0] as 24 | 168 | 720);
      else if (method === "ops") result = await ops.generate(args[0] as 24 | 168 | 720);
      else if (queries.includes(method)) {
        const query = db[method] as unknown as (...values: unknown[]) => unknown;
        result = query.apply(db, args);
        if (method === "recoverObservationsFromSnapshots" && Number(result) > 0) {
          db.bootstrapHourlyStatsFromObservations(Number(args[0]));
          refresh();
        }
      } else throw new Error("consulta desconocida");
      if (method === "dashboard" || method === "ops") postMessage({ type: "ready" });
      postMessage({ id, result });
    } catch (error) {
      postMessage({ id, error: error instanceof Error ? error.message : String(error) });
    }
  });
};

queue = queue.then(() => {
  const since = Math.floor(Date.now() / 1000) - config.recoveryLookbackHours * 3600;
  const recovered = db.recoverObservationsFromSnapshots(since);
  if (recovered > 0) db.bootstrapHourlyStatsFromObservations(since);
  const stateKey = "hourly_bootstrap_last_30d_v2";
  if (!db.getState(stateKey)) {
    db.bootstrapHourlyStatsFromObservations(Math.floor(Date.now() / 1000) - 30 * 86400);
    db.setState(stateKey, String(Math.floor(Date.now() / 1000)));
  }
});
refresh();
