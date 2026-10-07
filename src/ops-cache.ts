import { join } from "node:path";
import { config } from "./config";
import { DB } from "./db";
import { getDisplayRefreshInterval, writeJsonAtomic } from "./utils";

export const OPS_PRESET_HOURS = [24, 168, 720] as const;
type OpsPresetHours = (typeof OPS_PRESET_HOURS)[number];
export type OpsCacheEntry = ReturnType<DB["getOpsMetrics"]>;

const nowEpoch = () => Math.floor(Date.now() / 1000);

export class OpsPresetCache {
  constructor(private readonly db: DB, private readonly compute?: (hours: OpsPresetHours) => Promise<OpsCacheEntry>) {}

  async refreshDue(currentEpoch = nowEpoch()) {
    for (const hours of OPS_PRESET_HOURS) {
      const interval = getDisplayRefreshInterval(hours);
      const cached = await this.read(hours);
      if (!cached || currentEpoch - cached.generatedAtEpoch >= interval) {
        await this.generate(hours);
      }
    }
  }

  async generate(hours: OpsPresetHours): Promise<OpsCacheEntry> {
    if (this.compute) return this.compute(hours);
    const payload = this.db.getOpsMetrics(hours);
    const filePath = this.getFilePath(hours);
    await writeJsonAtomic(filePath, JSON.stringify(payload));
    console.log(`[ops-cache] ${hours}h actualizado`);
    return payload;
  }

  async read(hours: OpsPresetHours): Promise<OpsCacheEntry | null> {
    try {
      const file = Bun.file(this.getFilePath(hours));
      if (!(await file.exists())) {
        return null;
      }

      const payload = (await file.json()) as OpsCacheEntry;
      if (
        !payload ||
        payload.windowHours !== hours ||
        !payload.generatedAt ||
        !payload.generatedAtEpoch
      ) {
        return null;
      }

      return payload;
    } catch {
      return null;
    }
  }

  private getFilePath(hours: OpsPresetHours) {
    return join(config.displayCacheDir, `ops.${hours}h.json`);
  }

}
