import { config } from "./config";

export interface ApiKeyRecord {
  key: string;
  createdAt: number;
  expiresAt: number;
  inFlight: boolean;
  lastStartedAt: number;
  lastCompletedAt: number;
  totalRequests: number;
}

type AcquireFailure = "missing" | "invalid" | "expired" | "in_flight" | "rate_limited";

export class ApiKeyManager {
  private readonly store = new Map<string, ApiKeyRecord>();

  private nowMs(): number {
    return Date.now();
  }

  issueKey() {
    this.cleanupExpired();

    const now = this.nowMs();
    const key = crypto.randomUUID();
    const record: ApiKeyRecord = {
      key,
      createdAt: now,
      expiresAt: now + config.apiKeyTtlSeconds * 1000,
      inFlight: false,
      lastStartedAt: 0,
      lastCompletedAt: 0,
      totalRequests: 0,
    };

    this.store.set(key, record);

    return {
      key,
      createdAt: record.createdAt,
      expiresAt: record.expiresAt,
      expiresInSeconds: config.apiKeyTtlSeconds,
      limits: {
        maxConcurrentRequests: 1,
        minIntervalMs: config.apiRateLimitMs,
      },
    };
  }

  getKeyFromRequest(request: Request): string | null {
    const key = request.headers.get("x-api-key")?.trim();
    return key && key.length > 0 ? key : null;
  }

  private validateRecord(key: string | null): { ok: true; record: ApiKeyRecord } | { ok: false; reason: AcquireFailure } {
    if (!key) {
      return { ok: false, reason: "missing" };
    }

    const record = this.store.get(key);
    if (!record) {
      return { ok: false, reason: "invalid" };
    }

    if (this.nowMs() > record.expiresAt) {
      this.store.delete(key);
      return { ok: false, reason: "expired" };
    }

    return { ok: true, record };
  }

  tryAcquire(key: string | null):
    | { ok: true; record: ApiKeyRecord; release: () => void }
    | { ok: false; reason: AcquireFailure } {
    const validated = this.validateRecord(key);
    if (!validated.ok) {
      return validated;
    }

    const { record } = validated;
    const now = this.nowMs();

    if (record.inFlight) {
      return { ok: false, reason: "in_flight" };
    }

    if (record.lastStartedAt > 0 && now - record.lastStartedAt < config.apiRateLimitMs) {
      return { ok: false, reason: "rate_limited" };
    }

    record.inFlight = true;
    record.lastStartedAt = now;
    record.totalRequests += 1;

    return {
      ok: true,
      record,
      release: () => {
        record.inFlight = false;
        record.lastCompletedAt = this.nowMs();
      },
    };
  }

  cleanupExpired() {
    const now = this.nowMs();

    for (const [key, record] of this.store.entries()) {
      if (record.expiresAt < now) {
        this.store.delete(key);
      }
    }
  }

  getStats() {
    this.cleanupExpired();

    return {
      activeKeys: this.store.size,
      ttlSeconds: config.apiKeyTtlSeconds,
      rateLimitMs: config.apiRateLimitMs,
      maxConcurrentRequests: 1,
    };
  }
}
