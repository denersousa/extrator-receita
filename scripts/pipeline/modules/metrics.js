const fsp = require('node:fs/promises');

class Metrics {
    constructor(statusFile, dbSizeLogEverySec) {
        this.statusFile = statusFile;
        this.dbSizeLogEverySec = dbSizeLogEverySec;
        this.startedAt = Date.now();
        this.lastBatchMs = 0;
        this.lastBatchProcessed = 0;
        this.lastBatchInserted = 0;
        this.lastBatchThroughput = 0;
        this.nextDbSizeAtMs = Date.now();
    }

    async write(state) {
        const elapsedSec = Math.max(1, Math.floor((Date.now() - this.startedAt) / 1000));
        const mem = process.memoryUsage();
        const status = {
            timestamp: new Date().toISOString(),
            ...state,
            elapsedSec,
            processedPerSec: Number((state.totalProcessed / elapsedSec).toFixed(2)),
            insertedPerSec: Number((state.totalInserted / elapsedSec).toFixed(2)),
            lastBatchMs: this.lastBatchMs,
            lastBatchProcessed: this.lastBatchProcessed,
            lastBatchInserted: this.lastBatchInserted,
            lastBatchThroughput: this.lastBatchThroughput,
            nodeMemoryMB: {
                rss: Number((mem.rss / 1024 / 1024).toFixed(2)),
                heapUsed: Number((mem.heapUsed / 1024 / 1024).toFixed(2)),
                heapTotal: Number((mem.heapTotal / 1024 / 1024).toFixed(2)),
            },
        };

        await fsp.writeFile(this.statusFile, JSON.stringify(status, null, 2), 'utf8');
    }

    shouldLogDbSize() {
        const now = Date.now();
        if (now < this.nextDbSizeAtMs) {
            return false;
        }

        this.nextDbSizeAtMs = now + (this.dbSizeLogEverySec * 1000);
        return true;
    }
}

module.exports = {
    Metrics,
};
