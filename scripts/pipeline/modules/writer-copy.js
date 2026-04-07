const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const { pipeline } = require('node:stream/promises');
const { Pool } = require('pg');
const { from: copyFrom } = require('pg-copy-streams');
const { Semaphore } = require('./queue');

const EMPRESA_COLUMNS = [
    'cnpjBase',
    'razaoSocial',
    'naturezaJuridica',
    'qualificacaoResponsavel',
    'capitalSocial',
    'porte',
    'enteFederativo',
];

const ESTABELECIMENTO_COLUMNS = [
    'cnpjBase',
    'cnpjOrdem',
    'cnpjDv',
    'cnpjCompleto',
    'matrizFilial',
    'nomeFantasia',
    'situacaoCadastral',
    'dataSituacao',
    'motivoSituacao',
    'nomeCidadeExterior',
    'pais',
    'dataInicioAtividade',
    'cnaePrincipal',
    'cnaeSecundarios',
    'tipoLogradouro',
    'logradouro',
    'numero',
    'complemento',
    'bairro',
    'cep',
    'uf',
    'municipio',
    'ddd1',
    'telefone1',
    'ddd2',
    'telefone2',
    'dddFax',
    'fax',
    'email',
    'situacaoEspecial',
    'dataSituacaoEspecial',
];

class CopyWriter {
    constructor({
        connectionString,
        tmpDir,
        maxRetries,
        baseRetryMs,
        maxConcurrentCopy = 1,
        poolMax = 8,
        readyMaxAttempts = 60,
        importSessionSettings = {},
    }) {
        this.pool = new Pool({
            connectionString,
            max: Math.max(3, Math.max(poolMax, maxConcurrentCopy * 2 + 1)),
            idleTimeoutMillis: 60000,
            connectionTimeoutMillis: 10000,
            keepAlive: true,
            maxUses: 20000,
        });
        this.pool.on('error', (error) => {
            console.warn(`[db] erro em conexao ociosa do pool: ${error.message}`);
        });

        this.tmpDir = tmpDir;
        this.maxRetries = maxRetries;
        this.baseRetryMs = baseRetryMs;
        this.readyMaxAttempts = readyMaxAttempts;
        this.importSessionSettings = {
            synchronousCommitOff: Boolean(importSessionSettings.synchronousCommitOff),
            workMemMB: Number(importSessionSettings.workMemMB || 0),
            maintenanceWorkMemMB: Number(importSessionSettings.maintenanceWorkMemMB || 0),
            tempBuffersMB: Number(importSessionSettings.tempBuffersMB || 0),
            lockTimeoutMs: Number(importSessionSettings.lockTimeoutMs || 0),
            statementTimeoutMs: Number(importSessionSettings.statementTimeoutMs || 0),
        };

        // Semáforo: limita COPYs simultâneos para não sobrecarregar o PostgreSQL
        this.copySemaphore = new Semaphore(maxConcurrentCopy);

        // Contadores de saúde para backpressure no runner
        this.consecutiveFailures = 0;
        this.totalTransientErrors = 0;
    }

    async init() {
        await fsp.mkdir(this.tmpDir, { recursive: true });
        await this.waitUntilReady();
    }

    async close() {
        await this.pool.end();
    }

    async getDatabaseSizeBytes() {
        const result = await this.pool.query('SELECT pg_database_size(current_database()) as size');
        return Number(result.rows[0].size);
    }

    async writeEmpresas(rows) {
        if (rows.length === 0) {
            return 0;
        }

        const label = `empresa/${rows.length}rows`;
        return this.copySemaphore.run(label, () =>
            this.execWithRetry(() => this.copyAndMerge('empresa', EMPRESA_COLUMNS, rows)),
        );
    }

    async writeEstabelecimentos(rows) {
        if (rows.length === 0) {
            return 0;
        }

        const label = `estabelecimento/${rows.length}rows`;
        return this.copySemaphore.run(label, () =>
            this.execWithRetry(() => this.copyAndMerge('estabelecimento', ESTABELECIMENTO_COLUMNS, rows)),
        );
    }

    async createImportJob() {
        const result = await this.pool.query(
            `INSERT INTO receita.import_job (status, "startedAt") VALUES ('RUNNING', now()) RETURNING id`,
        );
        return Number(result.rows[0].id);
    }

    async finishImportJob(importJobId, totals) {
        await this.pool.query(
            `
            UPDATE receita.import_job
               SET status = $2,
                   "endedAt" = now(),
                   "totalProcessed" = $3,
                   "totalInserted" = $4,
                   "totalSkipped" = $5,
                   observacao = $6
             WHERE id = $1
            `,
            [
                importJobId,
                totals.status,
                String(totals.totalProcessed || 0),
                String(totals.totalInserted || 0),
                String(totals.totalSkipped || 0),
                totals.observacao || null,
            ],
        );
    }

    async startImportFile(importJobId, payload) {
        const result = await this.pool.query(
            `
            INSERT INTO receita.import_file (
                "importJobId", phase, "fileName", "filePath", status, "startedAt"
            )
            VALUES ($1, $2, $3, $4, 'RUNNING', now())
            RETURNING id
            `,
            [importJobId, payload.phase, payload.fileName, payload.filePath || null],
        );

        return Number(result.rows[0].id);
    }

    async finishImportFile(importFileId, payload) {
        await this.pool.query(
            `
            UPDATE receita.import_file
               SET status = $2,
                   "endedAt" = now(),
                   processed = $3,
                   inserted = $4,
                   skipped = $5
             WHERE id = $1
            `,
            [
                importFileId,
                payload.status,
                String(payload.processed || 0),
                String(payload.inserted || 0),
                String(payload.skipped || 0),
            ],
        );
    }

    async recordImportError(importJobId, payload) {
        await this.pool.query(
            `
            INSERT INTO receita.import_error (
                "importJobId", "importFileId", phase, "lineNumber", "errorCode", message, payload
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            `,
            [
                importJobId,
                payload.importFileId || null,
                payload.phase || null,
                payload.lineNumber || null,
                payload.errorCode || null,
                payload.message,
                payload.payload ? JSON.stringify(payload.payload) : null,
            ],
        );
    }

    async execWithRetry(task) {
        let attempt = 0;
        let hadError = false;

        // Penalidade progressiva: se operações anteriores falharam, espera antes de tentar
        if (this.consecutiveFailures > 0) {
            const penaltyMs = Math.min(this.baseRetryMs * this.consecutiveFailures * 2, 30000);
            console.warn(
                `[copy] ${this.consecutiveFailures} falha(s) consecutiva(s) anterior(es). Aguardando ${penaltyMs}ms antes de iniciar.`,
            );
            await sleep(penaltyMs);
        }

        while (true) {
            try {
                const result = await task();

                // Sucesso: reduz contador de falhas consecutivas gradualmente
                if (!hadError) {
                    this.consecutiveFailures = Math.max(0, this.consecutiveFailures - 1);
                }

                return result;
            } catch (error) {
                if (!this.isTransient(error) || attempt >= this.maxRetries) {
                    if (!hadError) {
                        this.consecutiveFailures += 1;
                        hadError = true;
                    }
                    throw error;
                }

                attempt += 1;
                if (!hadError) {
                    this.consecutiveFailures += 1;
                    hadError = true;
                }
                this.totalTransientErrors += 1;

                const waitMs = this.baseRetryMs * (2 ** (attempt - 1));
                console.warn(
                    `[copy] Falha transiente. Tentativa ${attempt}/${this.maxRetries}. ` +
                    `Falhas consecutivas: ${this.consecutiveFailures}. ` +
                    `Retry em ${waitMs}ms. Erro: ${error.message}`,
                );
                await sleep(waitMs);
            }
        }
    }

    async copyAndMerge(entity, columns, rows) {
        const client = await this.pool.connect();
        let clientStreamError = null;
        const onClientError = (error) => {
            clientStreamError = error;
        };
        client.on('error', onClientError);

        const tempTable = entity === 'empresa' ? 'tmp_empresa_ingest' : 'tmp_estabelecimento_ingest';
        const csvPath = await this.writeCsvFile(entity, columns, rows);

        try {
            await client.query('BEGIN');
            await this.applyImportSessionSettings(client);
            await client.query(`DROP TABLE IF EXISTS ${tempTable}`);
            await client.query(`CREATE TEMP TABLE ${tempTable} (LIKE ${entity === 'empresa' ? 'receita.empresa' : 'receita.estabelecimento'} INCLUDING DEFAULTS)`);

            const copyCommand = this.buildCopyCommand(tempTable, columns);
            const copyStream = client.query(copyFrom(copyCommand));
            await pipeline(fs.createReadStream(csvPath), copyStream);

            if (clientStreamError) {
                throw clientStreamError;
            }

            let insertedCount = 0;
            if (entity === 'empresa') {
                insertedCount = await this.mergeEmpresa(client, tempTable);
            } else {
                await this.ensureEmpresasForEstabelecimentos(client, tempTable);
                insertedCount = await this.mergeEstabelecimento(client, tempTable);
            }

            await client.query('COMMIT');
            return insertedCount;
        } catch (error) {
            await client.query('ROLLBACK').catch(() => null);
            throw error;
        } finally {
            client.removeListener('error', onClientError);
            client.release();
            await fsp.unlink(csvPath).catch(() => null);
        }
    }

    async applyImportSessionSettings(client) {
        if (this.importSessionSettings.synchronousCommitOff) {
            await client.query('SET LOCAL synchronous_commit = OFF');
        }

        if (this.importSessionSettings.workMemMB > 0) {
            await client.query(`SET LOCAL work_mem = '${this.importSessionSettings.workMemMB}MB'`);
        }

        if (this.importSessionSettings.maintenanceWorkMemMB > 0) {
            await client.query(`SET LOCAL maintenance_work_mem = '${this.importSessionSettings.maintenanceWorkMemMB}MB'`);
        }

        if (this.importSessionSettings.tempBuffersMB > 0) {
            await client.query(`SET LOCAL temp_buffers = '${this.importSessionSettings.tempBuffersMB}MB'`);
        }

        if (this.importSessionSettings.lockTimeoutMs > 0) {
            await client.query(`SET LOCAL lock_timeout = '${this.importSessionSettings.lockTimeoutMs}ms'`);
        }

        if (this.importSessionSettings.statementTimeoutMs > 0) {
            await client.query(`SET LOCAL statement_timeout = '${this.importSessionSettings.statementTimeoutMs}ms'`);
        }
    }

    async waitUntilReady() {
        for (let attempt = 1; attempt <= this.readyMaxAttempts; attempt += 1) {
            try {
                await this.pool.query('SELECT 1');
                if (attempt > 1) {
                    console.log(`[db] conexao restabelecida apos ${attempt} tentativa(s)`);
                }
                return;
            } catch (error) {
                if (attempt >= this.readyMaxAttempts) {
                    throw new Error(`PostgreSQL indisponivel apos ${this.readyMaxAttempts} tentativas: ${error.message}`);
                }

                const waitMs = Math.min(this.baseRetryMs * attempt, 10000);
                console.warn(`[db] aguardando disponibilidade do PostgreSQL (${attempt}/${this.readyMaxAttempts}) em ${waitMs}ms: ${error.message}`);
                await sleep(waitMs);
            }
        }
    }

    buildCopyCommand(tempTable, columns) {
        const quotedCols = columns.map((c) => `"${c}"`).join(', ');
        return `COPY ${tempTable} (${quotedCols}) FROM STDIN WITH (FORMAT csv, DELIMITER ';', QUOTE '"', ESCAPE '"', NULL '')`;
    }

    async mergeEmpresa(client, tempTable) {
        const result = await client.query(`
            INSERT INTO receita.empresa (
                "cnpjBase",
                "razaoSocial",
                "naturezaJuridica",
                "qualificacaoResponsavel",
                "capitalSocial",
                "porte",
                "enteFederativo"
            )
            SELECT
                "cnpjBase",
                "razaoSocial",
                "naturezaJuridica",
                "qualificacaoResponsavel",
                "capitalSocial",
                "porte",
                "enteFederativo"
            FROM ${tempTable}
            WHERE "cnpjBase" IS NOT NULL
            ON CONFLICT ("cnpjBase") DO NOTHING
        `);

        return result.rowCount;
    }

    async ensureEmpresasForEstabelecimentos(client, tempTable) {
        await client.query(`
            INSERT INTO receita.empresa ("cnpjBase")
            SELECT DISTINCT "cnpjBase"
            FROM ${tempTable}
            WHERE "cnpjBase" IS NOT NULL
            ON CONFLICT ("cnpjBase") DO NOTHING
        `);
    }

    async mergeEstabelecimento(client, tempTable) {
        const result = await client.query(`
            INSERT INTO receita.estabelecimento (
                "cnpjBase", "cnpjOrdem", "cnpjDv", "cnpjCompleto", "matrizFilial", "nomeFantasia",
                "situacaoCadastral", "dataSituacao", "motivoSituacao", "nomeCidadeExterior", "pais", "dataInicioAtividade",
                "cnaePrincipal", "cnaeSecundarios", "tipoLogradouro", "logradouro", "numero", "complemento", "bairro", "cep",
                "uf", "municipio", "ddd1", "telefone1", "ddd2", "telefone2", "dddFax", "fax", "email", "situacaoEspecial", "dataSituacaoEspecial",
                "hashRegistro", "dataAtualizacaoReceita"
            )
            SELECT
                "cnpjBase", "cnpjOrdem", "cnpjDv", "cnpjCompleto", "matrizFilial", "nomeFantasia",
                "situacaoCadastral", "dataSituacao", "motivoSituacao", "nomeCidadeExterior", "pais", "dataInicioAtividade",
                "cnaePrincipal", "cnaeSecundarios", "tipoLogradouro", "logradouro", "numero", "complemento", "bairro", "cep",
                "uf", "municipio", "ddd1", "telefone1", "ddd2", "telefone2", "dddFax", "fax", "email", "situacaoEspecial", "dataSituacaoEspecial",
                md5(concat_ws('|',
                    coalesce("cnpjBase", ''), coalesce("cnpjOrdem", ''), coalesce("cnpjDv", ''), coalesce("cnpjCompleto", ''), coalesce("matrizFilial", ''),
                    coalesce("nomeFantasia", ''), coalesce("situacaoCadastral", ''), coalesce("dataSituacao", ''), coalesce("motivoSituacao", ''),
                    coalesce("nomeCidadeExterior", ''), coalesce("pais", ''), coalesce("dataInicioAtividade", ''), coalesce("cnaePrincipal", ''),
                    coalesce("cnaeSecundarios", ''), coalesce("tipoLogradouro", ''), coalesce("logradouro", ''), coalesce("numero", ''),
                    coalesce("complemento", ''), coalesce("bairro", ''), coalesce("cep", ''), coalesce("uf", ''), coalesce("municipio", ''),
                    coalesce("ddd1", ''), coalesce("telefone1", ''), coalesce("ddd2", ''), coalesce("telefone2", ''), coalesce("dddFax", ''),
                    coalesce("fax", ''), coalesce("email", ''), coalesce("situacaoEspecial", ''), coalesce("dataSituacaoEspecial", '')
                )),
                now()
            FROM ${tempTable}
            WHERE "cnpjCompleto" IS NOT NULL
            ON CONFLICT ("cnpjCompleto") DO UPDATE
               SET "cnpjBase" = EXCLUDED."cnpjBase",
                   "cnpjOrdem" = EXCLUDED."cnpjOrdem",
                   "cnpjDv" = EXCLUDED."cnpjDv",
                   "matrizFilial" = EXCLUDED."matrizFilial",
                   "nomeFantasia" = EXCLUDED."nomeFantasia",
                   "situacaoCadastral" = EXCLUDED."situacaoCadastral",
                   "dataSituacao" = EXCLUDED."dataSituacao",
                   "motivoSituacao" = EXCLUDED."motivoSituacao",
                   "nomeCidadeExterior" = EXCLUDED."nomeCidadeExterior",
                   "pais" = EXCLUDED."pais",
                   "dataInicioAtividade" = EXCLUDED."dataInicioAtividade",
                   "cnaePrincipal" = EXCLUDED."cnaePrincipal",
                   "cnaeSecundarios" = EXCLUDED."cnaeSecundarios",
                   "tipoLogradouro" = EXCLUDED."tipoLogradouro",
                   "logradouro" = EXCLUDED."logradouro",
                   "numero" = EXCLUDED."numero",
                   "complemento" = EXCLUDED."complemento",
                   "bairro" = EXCLUDED."bairro",
                   "cep" = EXCLUDED."cep",
                   "uf" = EXCLUDED."uf",
                   "municipio" = EXCLUDED."municipio",
                   "ddd1" = EXCLUDED."ddd1",
                   "telefone1" = EXCLUDED."telefone1",
                   "ddd2" = EXCLUDED."ddd2",
                   "telefone2" = EXCLUDED."telefone2",
                   "dddFax" = EXCLUDED."dddFax",
                   "fax" = EXCLUDED."fax",
                   "email" = EXCLUDED."email",
                   "situacaoEspecial" = EXCLUDED."situacaoEspecial",
                   "dataSituacaoEspecial" = EXCLUDED."dataSituacaoEspecial",
                   "hashRegistro" = EXCLUDED."hashRegistro",
                   "dataAtualizacaoReceita" = now()
             WHERE receita.estabelecimento."hashRegistro" IS DISTINCT FROM EXCLUDED."hashRegistro"
        `);

        return result.rowCount;
    }

    async writeCsvFile(entity, columns, rows) {
        const filePath = path.join(this.tmpDir, `${entity}-${Date.now()}-${Math.random().toString(16).slice(2)}.csv`);

        const stream = fs.createWriteStream(filePath, { encoding: 'utf8' });
        for (const row of rows) {
            const values = columns.map((col) => toCsvValue(row[col]));
            const ok = stream.write(`${values.join(';')}\n`);
            if (!ok) {
                await once(stream, 'drain');
            }
        }

        await new Promise((resolve, reject) => {
            stream.end((error) => {
                if (error) {
                    reject(error);
                    return;
                }

                resolve();
            });
        });

        return filePath;
    }

    isTransient(error) {
        const message = String(error && error.message ? error.message : '').toLowerCase();
        const code = String(error && error.code ? error.code : '').toUpperCase();

        return (
            code === 'ECONNRESET' ||
            code === '57P01' ||
            code === '57P02' ||
            code === '57P03' ||
            code === '08006' ||
            code === '08001' ||
            message.includes('can\'t reach database server') ||
            message.includes('connection terminated') ||
            message.includes('connection reset') ||
            message.includes('econnreset') ||
            message.includes('timeout')
        );
    }
}

function toCsvValue(value) {
    if (value === null || value === undefined) {
        return '';
    }

    const text = String(value);
    const escaped = text.replace(/"/g, '""');
    return `"${escaped}"`;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function once(emitter, event) {
    return new Promise((resolve, reject) => {
        const onEvent = () => {
            cleanup();
            resolve();
        };

        const onError = (error) => {
            cleanup();
            reject(error);
        };

        const cleanup = () => {
            emitter.removeListener(event, onEvent);
            emitter.removeListener('error', onError);
        };

        emitter.on(event, onEvent);
        emitter.on('error', onError);
    });
}

module.exports = {
    CopyWriter,
};
