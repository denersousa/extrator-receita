'use strict';

class Watchdog {
    constructor({ intervalMs, timeoutMs, onTimeout }) {
        this.intervalMs = Math.max(1000, intervalMs);
        this.timeoutMs = Math.max(this.intervalMs * 2, timeoutMs);
        this.onTimeout = onTimeout;
        this.timer = null;
        this.lastProgressAt = Date.now();
        this.lastMeta = 'startup';
        this.triggered = false;
    }

    start() {
        if (this.timer) {
            return;
        }

        this.timer = setInterval(async () => {
            if (this.triggered) {
                return;
            }

            const idleMs = Date.now() - this.lastProgressAt;
            if (idleMs <= this.timeoutMs) {
                return;
            }

            this.triggered = true;
            const reason = `[watchdog] sem progresso por ${idleMs}ms (limite=${this.timeoutMs}ms, ultimoEvento=${this.lastMeta})`;
            console.error(reason);

            try {
                await this.onTimeout({ idleMs, timeoutMs: this.timeoutMs, lastMeta: this.lastMeta });
            } catch (error) {
                console.error(`[watchdog] erro no callback de timeout: ${error.message}`);
            }
        }, this.intervalMs);

        this.timer.unref?.();
    }

    beat(meta = 'progress') {
        this.lastProgressAt = Date.now();
        this.lastMeta = meta;
    }

    stop() {
        if (!this.timer) {
            return;
        }

        clearInterval(this.timer);
        this.timer = null;
    }
}

module.exports = { Watchdog };
