import { config } from "./config";
import { DB } from "./db";
import { RenfeIngestor } from "./ingestor";
import { DashboardPresetCache } from "./dashboard-cache";
import { OPS_PRESET_HOURS, OpsPresetCache } from "./ops-cache";
import { startServer } from "./server";

const db = new DB(config.dbPath);
const dashboardPresetCache = new DashboardPresetCache(db);
const opsPresetCache = new OpsPresetCache(db);
const ingestor = new RenfeIngestor(db, async (currentEpoch) => {
  const opsProgress = db.runOpsMaterializationTick();
  const hasOpsChanges = opsProgress.processedArchiveRows > 0 || opsProgress.processedLiveRows > 0;
  if (opsProgress.processedArchiveRows > 0 || opsProgress.processedLiveRows > 0) {
    console.log(
      `[ops-materialize] status=${opsProgress.status} archive_rows=${opsProgress.processedArchiveRows} live_rows=${opsProgress.processedLiveRows}`,
    );
  }
  await dashboardPresetCache.refreshDue(currentEpoch);
  if (hasOpsChanges) {
    for (const hours of OPS_PRESET_HOURS) {
      await opsPresetCache.generate(hours);
    }
  } else {
    await opsPresetCache.refreshDue(currentEpoch);
  }
});

void Promise.resolve().then(async () => {
  const opsProgress = db.runOpsMaterializationTick();
  if (opsProgress.processedArchiveRows > 0 || opsProgress.processedLiveRows > 0) {
    console.log(
      `[ops-materialize] startup status=${opsProgress.status} archive_rows=${opsProgress.processedArchiveRows} live_rows=${opsProgress.processedLiveRows}`,
    );
  }
  await dashboardPresetCache.refreshDue();
  for (const hours of OPS_PRESET_HOURS) {
    await opsPresetCache.generate(hours);
  }
});

const server = startServer(db, ingestor, dashboardPresetCache, opsPresetCache);
void ingestor.start().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[startup] ingestor init error ${message}`);
});

const shutdown = () => {
  console.log("[shutdown] stopping server");
  ingestor.stop();
  server.stop();
  db.close();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
