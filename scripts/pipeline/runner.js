const path = require('node:path');
const fsp = require('node:fs/promises');
const { config } = require('./modules/config');
const { CheckpointStore } = require('./modules/checkpoint');
const { parseEmpresa, parseEstabelecimento } = require('./modules/processor');
const { streamZipLines } = require('./modules/reader');
const { CopyWriter } = require('./modules/writer-copy');
const { Metrics } = require('./modules/metrics');
const { listZipFiles } = require('./modules/file-list');
const { Watchdog } = require('./modules/watchdog');

async function runPipeline() {
    const checkpoint = new CheckpointStore(config.checkpointFile);
    const state = await checkpoint.load();

    applyStartPhaseOverride(state);

    const metrics = new Metrics(config.statusFile, config.dbSizeLogEverySec);

    const writer = new CopyWriter({
        connectionString: process.env.DATABASE_URL,
        tmpDir: config.tmpDir,
        maxRetries: config.dbMaxRetries,
        baseRetryMs: config.dbBaseRetryMs,
        maxConcurrentCopy: config.dbMaxConcurrentCopy,
        poolMax: config.dbPoolMax,
        readyMaxAttempts: config.dbReadyMaxAttempts,
        importSessionSettings: {
            synchronousCommitOff: config.dbImportSynchronousCommitOff,
            workMemMB: config.dbImportWorkMemMB,
            maintenanceWorkMemMB: config.dbImportMaintenanceWorkMemMB,
            tempBuffersMB: config.dbImportTempBuffersMB,
            lockTimeoutMs: config.dbImportLockTimeoutMs,
            statementTimeoutMs: config.dbImportStatementTimeoutMs,
        },
    });

    await writer.init();
    const importJobId = await writer.createImportJob();

    const watchdog = new Watchdog({
        intervalMs: config.watchdogIntervalMs,
        timeoutMs: config.watchdogTimeoutMs,
        onTimeout: async ({ idleMs, timeoutMs, lastMeta }) => {
            state.watchdog = {
                timedOutAt: new Date().toISOString(),
                idleMs,
                timeoutMs,
                lastMeta,
            };
            await checkpoint.save(state).catch(() => null);
            await metrics.write(statusPayload(state)).catch(() => null);
            process.exitCode = 1;
            process.exit(1);
        },
    });
    watchdog.start();

    const empresas = await listZipFiles(
        config.dataDir,
        'empresas',
        'Empresas',
        config.empresasStartIndex,
        config.empresasEndIndex,
    );

    const estabelecimentos = await listZipFiles(
        config.dataDir,
        'estabelecimentos',
        'Estabelecimentos',
        config.estabelecimentosStartIndex,
        config.estabelecimentosEndIndex,
    );

    console.log(`[pipeline] empresas=${empresas.length}, estabelecimentos=${estabelecimentos.length}`);

    try {
        watchdog.beat('pipeline-start');
        if (state.phase === 'done') {
            console.log('[pipeline] Checkpoint ja indica processamento completo.');
            return;
        }

        if (!state.phase || state.phase === 'empresas') {
            state.phase = 'empresas';
            await runPhase({
                phase: 'empresas',
                files: empresas,
                parser: parseEmpresa,
                writerFn: (rows) => writer.writeEmpresas(rows),
                checkpoint,
                metrics,
                state,
                writer,
                watchdog,
                importJobId,
            });

            state.phase = 'estabelecimentos';
            state.fileIndex = 0;
            state.lineInFile = 0;
            await checkpoint.save(state);
        }

        if (state.phase === 'estabelecimentos') {
            await runPhase({
                phase: 'estabelecimentos',
                files: estabelecimentos,
                parser: parseEstabelecimento,
                writerFn: (rows) => writer.writeEstabelecimentos(rows),
                checkpoint,
                metrics,
                state,
                writer,
                watchdog,
                importJobId,
            });

            state.phase = 'done';
            state.fileIndex = 0;
            state.lineInFile = 0;
            await checkpoint.save(state);
            if (!config.keepCheckpointOnSuccess) {
                await checkpoint.clear();
            }

            await writer.finishImportJob(importJobId, {
                status: 'SUCCESS',
                totalProcessed: state.totalProcessed || 0,
                totalInserted: state.totalInserted || 0,
                totalSkipped: state.totalSkipped || 0,
                observacao: 'Pipeline finalizado com sucesso',
            });
        }
    } catch (error) {
        await writer.recordImportError(importJobId, {
            phase: state.phase,
            message: error.message,
            errorCode: error.code || null,
        }).catch(() => null);

        await writer.finishImportJob(importJobId, {
            status: 'FAILED',
            totalProcessed: state.totalProcessed || 0,
            totalInserted: state.totalInserted || 0,
            totalSkipped: state.totalSkipped || 0,
            observacao: error.message,
        }).catch(() => null);

        throw error;
    } finally {
        watchdog.stop();
        await metrics.write(statusPayload(state));
        await writer.close();
    }
}

function applyStartPhaseOverride(state) {
    const startPhase = normalizePhase(config.startPhase);
    if (!startPhase) {
        return;
    }

    if (state.phase !== startPhase || state.fileIndex !== 0 || state.lineInFile !== 0) {
        console.warn(`[pipeline] Forcando inicio pela fase=${startPhase}. Checkpoint atual sera ignorado para essa execucao.`);
    }

    state.phase = startPhase;
    state.fileIndex = 0;
    state.lineInFile = 0;
    state.currentFilePath = null;
}

async function runPhase({ phase, files, parser, writerFn, checkpoint, metrics, state, writer, watchdog, importJobId }) {
    console.log(`[fase] ${phase} iniciando no arquivo index=${state.fileIndex || 0}`);

    for (let fileIdx = state.fileIndex || 0; fileIdx < files.length; fileIdx += 1) {
        const file = files[fileIdx];
        const resumeLine = fileIdx === state.fileIndex ? (state.lineInFile || 0) : 0;

        console.log(`[arquivo] ${file.name} (retomar linha ${resumeLine})`);

        state.currentFilePath = file.path;
        await checkpoint.save(state);
        watchdog.beat(`phase:${phase}:file-start:${file.name}`);
        const importFileId = await writer.startImportFile(importJobId, {
            phase,
            fileName: file.name,
            filePath: file.path,
        });

        let fileStats = null;
        try {
            fileStats = await processZipFile({
                phase,
                file,
                resumeLine,
                parser,
                writerFn,
                state,
                checkpoint,
                metrics,
                initialBatchSize: config.initialBatchSize,
                minBatchSize: config.minBatchSize,
                maxBatchSize: config.maxBatchSize,
                watchdog,
            });

            await writer.finishImportFile(importFileId, {
                status: 'SUCCESS',
                processed: fileStats.processed,
                inserted: fileStats.inserted,
                skipped: fileStats.skipped,
            });
        } catch (error) {
            await writer.recordImportError(importJobId, {
                importFileId,
                phase,
                message: error.message,
                errorCode: error.code || null,
                payload: { file: file.name },
            }).catch(() => null);

            await writer.finishImportFile(importFileId, {
                status: 'FAILED',
                processed: fileStats?.processed || 0,
                inserted: fileStats?.inserted || 0,
                skipped: fileStats?.skipped || 0,
            }).catch(() => null);

            throw error;
        }

        console.log(`[resumo] ${file.name} processadas=${fileStats.processed}, inseridas=${fileStats.inserted}, descartadas=${fileStats.skipped}`);

        state.fileIndex = fileIdx + 1;
        state.lineInFile = 0;
        await checkpoint.save(state);
        await metrics.write(statusPayload(state));

        if (!config.safeMode) {
            await fsp.unlink(file.path).catch(() => null);
        }

        if (metrics.shouldLogDbSize()) {
            const bytes = await writer.getDatabaseSizeBytes().catch(() => null);
            if (bytes !== null) {
                console.log(`[db] tamanho atual ${formatGiB(bytes)} GiB`);
            }
        }

        watchdog.beat(`phase:${phase}:file-done:${file.name}`);
    }
}

async function processZipFile({
    phase,
    file,
    resumeLine,
    parser,
    writerFn,
    state,
    checkpoint,
    metrics,
    initialBatchSize,
    minBatchSize,
    maxBatchSize,
    watchdog,
}) {
    let currentBatchSize = initialBatchSize;
    let lastCheckpointAt = 0;

    let lineNo = 0;
    let processed = 0;
    let inserted = 0;
    let skipped = 0;

    let batchRows = [];
    let batchRawLines = 0;

    let lastLineHeartbeatAt = 0;

    async function flushBatch() {
        if (batchRows.length === 0 && batchRawLines === 0) {
            return;
        }

        const rowsToWrite = batchRows;
        const processedInBatch = batchRawLines;
        const endLine = lineNo;

        batchRows = [];
        batchRawLines = 0;

        watchdog.beat(`flush:start:${phase}:${file.name}:line=${endLine}`);
        const startMs = Date.now();
        const insertedInBatch = await writerFn(rowsToWrite);
        const batchMs = Date.now() - startMs;
        const skippedInBatch = processedInBatch - rowsToWrite.length;
        const throughput = Number((processedInBatch / Math.max(0.001, batchMs / 1000)).toFixed(2));

        processed += processedInBatch;
        inserted += insertedInBatch;
        skipped += skippedInBatch;

        state.totalProcessed = (state.totalProcessed || 0) + processedInBatch;
        state.totalInserted = (state.totalInserted || 0) + insertedInBatch;
        state.totalSkipped = (state.totalSkipped || 0) + skippedInBatch;
        state.lineInFile = endLine;

        metrics.lastBatchMs = batchMs;
        metrics.lastBatchProcessed = processedInBatch;
        metrics.lastBatchInserted = insertedInBatch;
        metrics.lastBatchThroughput = throughput;

        watchdog.beat(`flush:done:${phase}:${file.name}:line=${endLine}`);

        adjustBatchSize({
            result: {
                batchMs,
            },
            currentBatchSizeRef: {
                get value() { return currentBatchSize; },
                set value(v) { currentBatchSize = v; },
            },
            minBatchSize,
            maxBatchSize,
        });

        if ((processed - lastCheckpointAt) >= config.checkpointEveryLines) {
            await checkpoint.save(state);
            await metrics.write(statusPayload(state));
            logProgress({
                phase,
                file,
                processed,
                inserted,
                skipped,
                currentBatchSize,
                batchMs,
                batchRows: rowsToWrite.length,
                batchProcessed: processedInBatch,
                batchInserted: insertedInBatch,
                batchThroughput: throughput,
            });
            lastCheckpointAt = processed;
        }
    }

    await streamZipLines(file.path, async (line) => {
        lineNo += 1;
        if (lineNo <= resumeLine) {
            return;
        }

        const parsed = parser(line);
        if (parsed) {
            batchRows.push(parsed);
        }
        batchRawLines += 1;

        if ((lineNo - lastLineHeartbeatAt) >= 10000) {
            lastLineHeartbeatAt = lineNo;
            watchdog.beat(`read:${phase}:${file.name}:line=${lineNo}`);
        }

        if (batchRows.length >= currentBatchSize) {
            await flushBatch();
        }
    });

    await flushBatch();

    await checkpoint.save(state);
    await metrics.write(statusPayload(state));

    return { processed, inserted, skipped };
}

function adjustBatchSize({ result, currentBatchSizeRef, minBatchSize, maxBatchSize }) {
    const current = currentBatchSizeRef.value;

    if (result.batchMs > 3500 && current > minBatchSize) {
        currentBatchSizeRef.value = Math.max(minBatchSize, Math.floor(current * 0.8));
        return;
    }

    if (result.batchMs < 1200 && current < maxBatchSize) {
        currentBatchSizeRef.value = Math.min(maxBatchSize, Math.floor(current * 1.15));
    }
}

function logProgress({
    phase,
    file,
    processed,
    inserted,
    skipped,
    currentBatchSize,
    batchMs,
    batchRows,
    batchProcessed,
    batchInserted,
    batchThroughput,
}) {
    const progress = `${processed.toLocaleString()} processadas / ${inserted.toLocaleString()} inseridas / ${skipped.toLocaleString()} descartadas`;
    const mem = process.memoryUsage();
    const rssMb = (mem.rss / 1024 / 1024).toFixed(1);

    console.log(
        `[progress] fase=${phase} arquivo=${file.name} ${progress} ` +
        `batchAtual=${currentBatchSize} ultimoBatchMs=${batchMs} ` +
        `batchLidas=${batchProcessed} batchValidas=${batchRows} batchInseridas=${batchInserted} ` +
        `batchThroughput=${batchThroughput}/s rssMB=${rssMb}`,
    );
}

function statusPayload(state) {
    return {
        phase: state.phase,
        fileIndex: state.fileIndex,
        lineInFile: state.lineInFile,
        currentFilePath: state.currentFilePath,
        totalProcessed: state.totalProcessed || 0,
        totalInserted: state.totalInserted || 0,
        totalSkipped: state.totalSkipped || 0,
    };
}

function formatGiB(bytes) {
    return (bytes / 1024 / 1024 / 1024).toFixed(2);
}

function normalizePhase(value) {
    const normalized = String(value || '').trim().toLowerCase();
    if (normalized === 'empresas' || normalized === 'estabelecimentos') {
        return normalized;
    }

    return '';
}

module.exports = {
    runPipeline,
};
