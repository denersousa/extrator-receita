const fsp = require('node:fs/promises');

class CheckpointStore {
    constructor(filePath) {
        this.filePath = filePath;
        this.state = {
            phase: 'empresas',
            fileIndex: 0,
            lineInFile: 0,
            currentFilePath: null,
            totalProcessed: 0,
            totalInserted: 0,
            totalSkipped: 0,
            updatedAt: new Date().toISOString(),
        };
    }

    async load() {
        const raw = await fsp.readFile(this.filePath, 'utf8').catch(() => null);
        if (!raw) {
            return this.state;
        }

        try {
            const parsed = JSON.parse(raw);
            this.state = { ...this.state, ...parsed };
        } catch {
            // keep defaults
        }

        return this.state;
    }

    async save(patch) {
        this.state = {
            ...this.state,
            ...patch,
            updatedAt: new Date().toISOString(),
        };

        await fsp.writeFile(this.filePath, JSON.stringify(this.state, null, 2), 'utf8');
    }

    async clear() {
        await fsp.unlink(this.filePath).catch(() => null);
    }
}

module.exports = { CheckpointStore };
