import { dirname, join } from "node:path";
import { mkdir, rename, writeFile } from "node:fs/promises";
import { config } from "./config";
import { DB } from "./db";

export const OPS_PRESET_HOURS = [24, 168, 720] as const;
type OpsPresetHours = (typeof OPS_PRESET_HOURS)[number];
type OpsPayload = ReturnType<DB["getOpsMetrics"]>;

export interface OpsCacheEntry extends OpsPayload {}

const nowEpoch = () => Math.floor(Date.now() / 1000);

export class OpsPresetCache {
  private readonly db: DB;

  constructor(db: DB) {
    this.db = db;
  }

  isPresetHours(hours: number): hours is OpsPresetHours {
    return OPS_PRESET_HOURS.includes(hours as OpsPresetHours);
  }

  getRefreshIntervalSeconds(hours: OpsPresetHours): number {
    if (hours === 24) {
      return config.displayCache24Seconds;
    }

    if (hours === 168) {
      return config.displayCache168Seconds;
    }

    return config.displayCache720Seconds;
  }

  async refreshDue(currentEpoch = nowEpoch()) {
    for (const hours of OPS_PRESET_HOURS) {
      const interval = this.getRefreshIntervalSeconds(hours);
      const cached = await this.read(hours);
      if (!cached || currentEpoch - cached.generatedAtEpoch >= interval) {
        await this.generate(hours);
      }
    }
  }

  async generate(hours: OpsPresetHours): Promise<OpsCacheEntry> {
    const payload = this.db.getOpsMetrics(hours);
    const filePath = this.getFilePath(hours);
    await this.writeJsonAtomic(filePath, JSON.stringify(payload));
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

  private async writeJsonAtomic(filePath: string, content: string) {
    await mkdir(dirname(filePath), { recursive: true });
    const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
    await writeFile(tempPath, content, "utf8");
    await rename(tempPath, filePath);
  }
}
