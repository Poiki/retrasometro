import { dirname, join } from "node:path";
import { mkdir, rename, writeFile } from "node:fs/promises";
import { config } from "./config";
import { DB } from "./db";

export const DASHBOARD_PRESET_HOURS = [24, 168, 720] as const;

type PresetHours = (typeof DASHBOARD_PRESET_HOURS)[number];
type HistoricalPayload = ReturnType<DB["getHistoricalStats"]>;

export interface DashboardHistoricalCacheEntry {
  historyHours: PresetHours;
  generatedAt: string;
  generatedAtEpoch: number;
  historical: HistoricalPayload;
}

const nowEpoch = () => Math.floor(Date.now() / 1000);

export class DashboardPresetCache {
  private readonly db: DB;

  constructor(db: DB) {
    this.db = db;
  }

  isPresetHours(hours: number): hours is PresetHours {
    return DASHBOARD_PRESET_HOURS.includes(hours as PresetHours);
  }

  getRefreshIntervalSeconds(hours: PresetHours): number {
    if (hours === 24) {
      return config.displayCache24Seconds;
    }

    if (hours === 168) {
      return config.displayCache168Seconds;
    }

    return config.displayCache720Seconds;
  }

  async refreshDue(currentEpoch = nowEpoch()) {
    for (const hours of DASHBOARD_PRESET_HOURS) {
      const interval = this.getRefreshIntervalSeconds(hours);
      const cached = await this.read(hours);
      if (!cached || currentEpoch - cached.generatedAtEpoch >= interval) {
        await this.generate(hours, currentEpoch);
      }
    }
  }

  async generate(hours: PresetHours, generatedAtEpoch = nowEpoch()): Promise<DashboardHistoricalCacheEntry> {
    const historical = this.db.getHistoricalStats(hours);
    const entry: DashboardHistoricalCacheEntry = {
      historyHours: hours,
      generatedAt: new Date(generatedAtEpoch * 1000).toISOString(),
      generatedAtEpoch,
      historical,
    };

    const filePath = this.getFilePath(hours);
    await this.writeJsonAtomic(filePath, JSON.stringify(entry));
    console.log(`[display-cache] dashboard ${hours}h actualizado`);

    return entry;
  }

  async read(hours: PresetHours): Promise<DashboardHistoricalCacheEntry | null> {
    try {
      const file = Bun.file(this.getFilePath(hours));
      if (!(await file.exists())) {
        return null;
      }

      const payload = (await file.json()) as DashboardHistoricalCacheEntry;
      if (!payload || payload.historyHours !== hours || !payload.generatedAtEpoch || !payload.historical) {
        return null;
      }

      return payload;
    } catch {
      return null;
    }
  }

  private getFilePath(hours: PresetHours) {
    return join(config.displayCacheDir, `dashboard.${hours}h.json`);
  }

  private async writeJsonAtomic(filePath: string, content: string) {
    await mkdir(dirname(filePath), { recursive: true });
    const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
    await writeFile(tempPath, content, "utf8");
    await rename(tempPath, filePath);
  }
}
