const path = require('node:path');
const { loadEnvFile, intEnv, boolEnv, stringEnv } = require('./env');

const BASE_DIR = path.resolve(__dirname, '..', '..', '..');
loadEnvFile(path.join(BASE_DIR, '.env'));

const config = {
    baseDir: BASE_DIR,
    dataDir: path.join(BASE_DIR, 'data'),
    tmpDir: path.join(BASE_DIR, 'scripts', 'pipeline', 'tmp'),
    checkpointFile: path.join(BASE_DIR, '.pipeline-checkpoint.json'),
    statusFile: path.join(BASE_DIR, '.pipeline-status.json'),

    safeMode: boolEnv('PIPELINE_SAFE_MODE', true),
    minBatchSize: intEnv('PIPELINE_BATCH_MIN', 500),
    maxBatchSize: intEnv('PIPELINE_BATCH_MAX', 2000),
    initialBatchSize: intEnv('PIPELINE_BATCH_INITIAL', 1000),
    processingBatchSize: intEnv('PIPELINE_PROCESSING_BATCH_SIZE', intEnv('PIPELINE_BATCH_INITIAL', 1000)),
    insertBatchSize: intEnv('PIPELINE_INSERT_BATCH_SIZE', intEnv('PIPELINE_BATCH_INITIAL', 1000)),
    batchScaleDownMs: intEnv('PIPELINE_BATCH_SCALE_DOWN_MS', 12000),
    batchScaleUpMs: intEnv('PIPELINE_BATCH_SCALE_UP_MS', 4500),
    batchScaleDownFactor: Number(stringEnv('PIPELINE_BATCH_SCALE_DOWN_FACTOR', '0.85')),
    batchScaleUpFactor: Number(stringEnv('PIPELINE_BATCH_SCALE_UP_FACTOR', '1.20')),
    batchScaleDownStreak: intEnv('PIPELINE_BATCH_SCALE_DOWN_STREAK', 2),
    batchScaleUpStreak: intEnv('PIPELINE_BATCH_SCALE_UP_STREAK', 2),
    batchAdaptiveEnabled: boolEnv('PIPELINE_BATCH_ADAPTIVE_ENABLED', true),
    checkpointEveryLines: intEnv('PIPELINE_CHECKPOINT_EVERY_LINES', 5000),

    dbMaxRetries: intEnv('PIPELINE_DB_MAX_RETRIES', 10),
    dbBaseRetryMs: intEnv('PIPELINE_DB_BASE_RETRY_DELAY_MS', 1000),
    dbReadyMaxAttempts: intEnv('PIPELINE_DB_READY_MAX_ATTEMPTS', 60),
    dbMaxConcurrentCopy: intEnv('PIPELINE_DB_MAX_CONCURRENT_COPY', 1),
    dbPoolMax: intEnv('PIPELINE_DB_POOL_MAX', 8),
    dbImportSynchronousCommitOff: boolEnv('PIPELINE_DB_IMPORT_SYNC_COMMIT_OFF', true),
    dbImportWorkMemMB: intEnv('PIPELINE_DB_IMPORT_WORK_MEM_MB', 64),
    dbImportMaintenanceWorkMemMB: intEnv('PIPELINE_DB_IMPORT_MAINTENANCE_WORK_MEM_MB', 1024),
    dbImportTempBuffersMB: intEnv('PIPELINE_DB_IMPORT_TEMP_BUFFERS_MB', 64),
    dbImportLockTimeoutMs: intEnv('PIPELINE_DB_IMPORT_LOCK_TIMEOUT_MS', 15000),
    dbImportStatementTimeoutMs: intEnv('PIPELINE_DB_IMPORT_STATEMENT_TIMEOUT_MS', 0),
    ensureEmpresaForEstabelecimento: boolEnv('PIPELINE_ESTABELECIMENTO_ENSURE_EMPRESA', true),
    estabelecimentoWriteMode: stringEnv('PIPELINE_ESTABELECIMENTO_WRITE_MODE', 'upsert'),

    watchdogIntervalMs: intEnv('PIPELINE_WATCHDOG_INTERVAL_MS', 30000),
    watchdogTimeoutMs: intEnv('PIPELINE_WATCHDOG_TIMEOUT_MS', 180000),

    keepCheckpointOnSuccess: boolEnv('PIPELINE_KEEP_CHECKPOINT_ON_SUCCESS', false),
    dbSizeLogEverySec: intEnv('PIPELINE_DB_SIZE_LOG_EVERY_SEC', 180),
    startPhase: stringEnv('PIPELINE_START_PHASE', ''),

    empresasStartIndex: intEnv('PIPELINE_EMPRESAS_START_INDEX', 1),
    empresasEndIndex: intEnv('PIPELINE_EMPRESAS_END_INDEX', 9),
    estabelecimentosStartIndex: intEnv('PIPELINE_ESTABELECIMENTOS_START_INDEX', 0),
    estabelecimentosEndIndex: intEnv('PIPELINE_ESTABELECIMENTOS_END_INDEX', 9),
};

module.exports = { config };
