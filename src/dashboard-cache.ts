import { join } from "node:path";
import { config } from "./config";
import { DB } from "./db";
import { getDisplayRefreshInterval, writeJsonAtomic } from "./utils";

export const DASHBOARD_PRESET_HOURS = [24, 168, 720] as const;

type PresetHours = (typeof DASHBOARD_PRESET_HOURS)[number];
type HistoricalPayload = ReturnType<DB["getHistoricalStats"]>;
type CorridorMetaPayload = ReturnType<DB["getCorridorMeta"]>;

export interface DashboardHistoricalCacheEntry {
  historyHours: PresetHours;
  generatedAt: string;
  generatedAtEpoch: number;
  historical: HistoricalPayload;
  corridorMeta?: CorridorMetaPayload;
}

const nowEpoch = () => Math.floor(Date.now() / 1000);

export class DashboardPresetCache {
  private readonly pending = new Map<PresetHours, Promise<DashboardHistoricalCacheEntry>>();

  constructor(private readonly db: DB, private readonly compute?: (hours: PresetHours) => Promise<DashboardHistoricalCacheEntry>) {}

  isPresetHours(hours: number): hours is PresetHours {
    return DASHBOARD_PRESET_HOURS.includes(hours as PresetHours);
  }

  async refreshDue(currentEpoch = nowEpoch()) {
    for (const hours of DASHBOARD_PRESET_HOURS) {
      const interval = getDisplayRefreshInterval(hours);
      const cached = await this.read(hours);
      if (!cached || currentEpoch - cached.generatedAtEpoch >= interval) {
        await this.generate(hours, currentEpoch);
      }
    }
  }

  async generate(hours: PresetHours, generatedAtEpoch = nowEpoch()): Promise<DashboardHistoricalCacheEntry> {
    if (this.compute) {
      const existing = this.pending.get(hours);
      if (existing) return existing;
      const job = this.compute(hours).finally(() => this.pending.delete(hours));
      this.pending.set(hours, job);
      return job;
    }
    const historical = this.db.getHistoricalStats(hours);
    const corridorMeta = this.db.getCorridorMeta();
    const entry: DashboardHistoricalCacheEntry = {
      historyHours: hours,
      generatedAt: new Date(generatedAtEpoch * 1000).toISOString(),
      generatedAtEpoch,
      historical,
      corridorMeta,
    };

    const filePath = this.getFilePath(hours);
    await writeJsonAtomic(filePath, JSON.stringify(entry));
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

}
