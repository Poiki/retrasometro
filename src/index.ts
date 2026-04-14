import { config } from "./config";
import { DB } from "./db";
import { RenfeIngestor } from "./ingestor";
import { DashboardPresetCache } from "./dashboard-cache";
import { startServer } from "./server";

const db = new DB(config.dbPath);
const dashboardPresetCache = new DashboardPresetCache(db);
const ingestor = new RenfeIngestor(db, async (currentEpoch) => {
  await dashboardPresetCache.refreshDue(currentEpoch);
});

await ingestor.start();
const server = startServer(db, ingestor, dashboardPresetCache);

const shutdown = () => {
  console.log("[shutdown] stopping server");
  ingestor.stop();
  server.stop();
  db.close();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
