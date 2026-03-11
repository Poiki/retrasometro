import { config } from "./config";
import { DB } from "./db";
import { RenfeIngestor } from "./ingestor";
import { startServer } from "./server";

const db = new DB(config.dbPath);
const ingestor = new RenfeIngestor(db);

await ingestor.start();
const server = startServer(db, ingestor);

const shutdown = () => {
  console.log("[shutdown] stopping server");
  ingestor.stop();
  server.stop();
  db.close();
  process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
