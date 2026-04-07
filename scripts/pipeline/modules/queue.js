'use strict';

/**
 * Semáforo para controlar a concorrência de operações COPY no PostgreSQL.
 * Garante que no máximo `maxConcurrent` operações rodem simultaneamente.
 * Operações excedentes aguardam ordenadamente na fila.
 */
class Semaphore {
    constructor(maxConcurrent) {
        this._max = Math.max(1, maxConcurrent);
        this._running = 0;
        this._queue = [];
    }

    /** Número de operações em execução no momento. */
    get running() {
        return this._running;
    }

    /** Número de operações aguardando na fila. */
    get waiting() {
        return this._queue.length;
    }

    /** Limite máximo de concorrência atual. */
    get maxConcurrent() {
        return this._max;
    }

    /** Ajusta o limite em tempo de execução (mínimo 1). */
    set maxConcurrent(value) {
        this._max = Math.max(1, value);
    }

    /**
     * Executa `task` respeitando o limite de concorrência.
     * Se o limite já foi atingido, aguarda na fila até uma vaga abrir.
     *
     * @param {string}           label - Identificador usado nos logs
     * @param {() => Promise<*>} task  - Função async a executar
     * @returns {Promise<*>} Valor retornado por `task`
     */
    async run(label, task) {
        const queuedAt = Date.now();

        if (this._running >= this._max) {
            console.log(
                `[copy:queue] "${label}" aguardando (fila: ${this._queue.length + 1}, rodando: ${this._running}/${this._max})`,
            );
            await new Promise((resolve) => this._queue.push(resolve));
        }

        this._running += 1;
        const startMs = Date.now();
        const waitedMs = startMs - queuedAt;

        if (waitedMs > 10) {
            console.log(
                `[copy:start] "${label}" iniciando (aguardou: ${waitedMs}ms, rodando: ${this._running}/${this._max})`,
            );
        } else {
            console.log(`[copy:start] "${label}" iniciando (rodando: ${this._running}/${this._max})`);
        }

        try {
            const result = await task();
            const elapsedMs = Date.now() - startMs;
            console.log(
                `[copy:done] "${label}" concluido em ${elapsedMs}ms (fila restante: ${this._queue.length})`,
            );
            return result;
        } finally {
            this._running -= 1;
            if (this._queue.length > 0) {
                const next = this._queue.shift();
                next();
            }
        }
    }
}

module.exports = { Semaphore };
