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
        ensureEmpresaForEstabelecimento: config.ensureEmpresaForEstabelecimento,
        estabelecimentoWriteMode: config.estabelecimentoWriteMode,
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
                processingBatchSize: config.processingBatchSize,
                insertBatchSize: config.insertBatchSize,
                initialBatchSize: config.initialBatchSize,
                minBatchSize: config.minBatchSize,
                maxBatchSize: config.maxBatchSize,
                batchScaleDownMs: config.batchScaleDownMs,
                batchScaleUpMs: config.batchScaleUpMs,
                batchScaleDownFactor: config.batchScaleDownFactor,
                batchScaleUpFactor: config.batchScaleUpFactor,
                batchScaleDownStreak: config.batchScaleDownStreak,
                batchScaleUpStreak: config.batchScaleUpStreak,
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
    processingBatchSize,
    insertBatchSize,
    initialBatchSize,
    minBatchSize,
    maxBatchSize,
    batchScaleDownMs,
    batchScaleUpMs,
    batchScaleDownFactor,
    batchScaleUpFactor,
    batchScaleDownStreak,
    batchScaleUpStreak,
    watchdog,
}) {
    const normalizedProcessingBatchSize = Number.isFinite(processingBatchSize) && processingBatchSize > 0
        ? Math.floor(processingBatchSize)
        : 2000;
    const normalizedInsertBatchSize = Number.isFinite(insertBatchSize) && insertBatchSize > 0
        ? Math.floor(insertBatchSize)
        : initialBatchSize;

    let currentBatchSize = normalizedInsertBatchSize;
    let lastCheckpointAt = 0;
    const adaptiveState = {
        slowStreak: 0,
        fastStreak: 0,
    };

    let lineNo = 0;
    let processed = 0;
    let inserted = 0;
    let skipped = 0;

    let currentProcessRows = [];
    let currentProcessRawLines = 0;
    let currentProcessEndLine = 0;
    const pendingProcessBatches = [];
    let pendingProcessedLines = 0;

    let lastLineHeartbeatAt = 0;
    const maxInFlightWrites = Math.max(1, config.dbMaxConcurrentCopy);
    const adaptiveBatchEnabled = Boolean(config.batchAdaptiveEnabled) && maxInFlightWrites === 1;
    const pendingWrites = new Set();
    const completedBatches = new Map();
    let nextBatchId = 1;
    let nextBatchToCommit = 1;
    let completionChain = Promise.resolve();

    async function commitCompletedBatches() {
        while (completedBatches.has(nextBatchToCommit)) {
            const batch = completedBatches.get(nextBatchToCommit);
            completedBatches.delete(nextBatchToCommit);

            processed += batch.processedInBatch;
            inserted += batch.insertedInBatch;
            skipped += batch.skippedInBatch;

            state.totalProcessed = (state.totalProcessed || 0) + batch.processedInBatch;
            state.totalInserted = (state.totalInserted || 0) + batch.insertedInBatch;
            state.totalSkipped = (state.totalSkipped || 0) + batch.skippedInBatch;
            state.lineInFile = batch.endLine;

            metrics.lastBatchMs = batch.batchMs;
            metrics.lastBatchProcessed = batch.processedInBatch;
            metrics.lastBatchInserted = batch.insertedInBatch;
            metrics.lastBatchThroughput = batch.throughput;

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
                    batchMs: batch.batchMs,
                    batchRows: batch.rowsToWrite,
                    batchProcessed: batch.processedInBatch,
                    batchInserted: batch.insertedInBatch,
                    batchThroughput: batch.throughput,
                });
                lastCheckpointAt = processed;
            }

            nextBatchToCommit += 1;
        }
    }

    async function flushBatch(force = false) {
        if (pendingProcessBatches.length === 0) {
            return;
        }

        if (!force && pendingProcessedLines < currentBatchSize) {
            return;
        }

        let rowsToWrite = [];
        let processedInBatch = 0;
        let endLine = 0;

        while (pendingProcessBatches.length > 0 && (force || processedInBatch < currentBatchSize)) {
            const batch = pendingProcessBatches.shift();
            rowsToWrite = rowsToWrite.concat(batch.rows);
            processedInBatch += batch.rawLines;
            endLine = batch.endLine;
            pendingProcessedLines -= batch.rawLines;
        }

        if (processedInBatch === 0) {
            return;
        }
        const batchId = nextBatchId;
        nextBatchId += 1;

        const writePromise = (async () => {
            watchdog.beat(`flush:start:${phase}:${file.name}:line=${endLine}`);
            const startMs = Date.now();
            const insertedInBatch = await writerFn(rowsToWrite);
            const batchMs = Date.now() - startMs;
            const skippedInBatch = processedInBatch - rowsToWrite.length;
            const throughput = Number((processedInBatch / Math.max(0.001, batchMs / 1000)).toFixed(2));

            watchdog.beat(`flush:done:${phase}:${file.name}:line=${endLine}`);

            if (adaptiveBatchEnabled) {
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
                    scaleDownMs: batchScaleDownMs,
                    scaleUpMs: batchScaleUpMs,
                    scaleDownFactor: batchScaleDownFactor,
                    scaleUpFactor: batchScaleUpFactor,
                    scaleDownStreak: batchScaleDownStreak,
                    scaleUpStreak: batchScaleUpStreak,
                    adaptiveState,
                });
            }

            completionChain = completionChain.then(async () => {
                completedBatches.set(batchId, {
                    endLine,
                    processedInBatch,
                    insertedInBatch,
                    skippedInBatch,
                    batchMs,
                    throughput,
                    rowsToWrite: rowsToWrite.length,
                });
                await commitCompletedBatches();
            });

            await completionChain;
        })();

        pendingWrites.add(writePromise);
        writePromise.finally(() => {
            pendingWrites.delete(writePromise);
        });

        if (pendingWrites.size >= maxInFlightWrites) {
            await Promise.race(pendingWrites);
        }
    }

    function pushProcessedChunk() {
        if (currentProcessRawLines === 0) {
            return;
        }

        pendingProcessBatches.push({
            rows: currentProcessRows,
            rawLines: currentProcessRawLines,
            endLine: currentProcessEndLine,
        });
        pendingProcessedLines += currentProcessRawLines;

        currentProcessRows = [];
        currentProcessRawLines = 0;
        currentProcessEndLine = 0;
    }

    try {
        await streamZipLines(file.path, async (line) => {
            lineNo += 1;
            if (lineNo <= resumeLine) {
                return;
            }

            const parsed = parser(line);
            if (parsed) {
                currentProcessRows.push(parsed);
            }
            currentProcessRawLines += 1;
            currentProcessEndLine = lineNo;

            if ((lineNo - lastLineHeartbeatAt) >= 10000) {
                lastLineHeartbeatAt = lineNo;
                watchdog.beat(`read:${phase}:${file.name}:line=${lineNo}`);
            }

            if (currentProcessRawLines >= normalizedProcessingBatchSize) {
                pushProcessedChunk();
                await flushBatch();
            }
        });

        pushProcessedChunk();
        await flushBatch(true);

        if (pendingWrites.size > 0) {
            await Promise.all(pendingWrites);
        }
        await completionChain;
    } catch (error) {
        if (pendingWrites.size > 0) {
            await Promise.allSettled(pendingWrites);
        }
        throw error;
    }

    await checkpoint.save(state);
    await metrics.write(statusPayload(state));

    return { processed, inserted, skipped };
}

function adjustBatchSize({
    result,
    currentBatchSizeRef,
    minBatchSize,
    maxBatchSize,
    scaleDownMs,
    scaleUpMs,
    scaleDownFactor,
    scaleUpFactor,
    scaleDownStreak,
    scaleUpStreak,
    adaptiveState,
}) {
    const current = currentBatchSizeRef.value;
    const downFactor = Number.isFinite(scaleDownFactor) && scaleDownFactor > 0 && scaleDownFactor < 1
        ? scaleDownFactor
        : 0.8;
    const upFactor = Number.isFinite(scaleUpFactor) && scaleUpFactor > 1
        ? scaleUpFactor
        : 1.15;
    const downMs = Number.isFinite(scaleDownMs) && scaleDownMs > 0
        ? scaleDownMs
        : 3500;
    const upMs = Number.isFinite(scaleUpMs) && scaleUpMs > 0
        ? scaleUpMs
        : 1200;
    const downStreakTarget = Number.isFinite(scaleDownStreak) && scaleDownStreak > 0
        ? Math.floor(scaleDownStreak)
        : 1;
    const upStreakTarget = Number.isFinite(scaleUpStreak) && scaleUpStreak > 0
        ? Math.floor(scaleUpStreak)
        : 1;

    if (result.batchMs > downMs) {
        adaptiveState.slowStreak += 1;
        adaptiveState.fastStreak = 0;
    } else if (result.batchMs < upMs) {
        adaptiveState.fastStreak += 1;
        adaptiveState.slowStreak = 0;
    } else {
        adaptiveState.fastStreak = 0;
        adaptiveState.slowStreak = 0;
    }

    if (adaptiveState.slowStreak >= downStreakTarget && current > minBatchSize) {
        currentBatchSizeRef.value = Math.max(minBatchSize, Math.floor(current * downFactor));
        adaptiveState.slowStreak = 0;
        adaptiveState.fastStreak = 0;
        return;
    }

    if (adaptiveState.fastStreak >= upStreakTarget && current < maxBatchSize) {
        currentBatchSizeRef.value = Math.min(maxBatchSize, Math.floor(current * upFactor));
        adaptiveState.slowStreak = 0;
        adaptiveState.fastStreak = 0;
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
