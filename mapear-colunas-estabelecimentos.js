const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const https = require('node:https');
const readline = require('node:readline');
const dns = require('node:dns/promises');
const { pipeline } = require('node:stream/promises');
const yauzl = require('yauzl');

const ZIP_URL = 'https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos0.zip';
const EXECUTION_MODE_DIAGNOSTIC = 'diagnostic';
const EXECUTION_MODE_PROCESSING = 'processing';

const DEFAULT_TIMEOUT_MS = 10 * 60 * 1000;
const DEFAULT_MAX_DOWNLOAD_ATTEMPTS = 3;
const DEFAULT_BACKOFF_BASE_MS = 1_000;
const DEFAULT_BACKOFF_MAX_MS = 30_000;
const PRECHECK_HEAD_DEFAULT = true;

const CONFIG = {
    executionMode: parseExecutionMode(process.argv, process.env.EXECUTION_MODE || process.env.RUN_MODE),
    primaryUrl: process.env.RFB_ZIP_URL || ZIP_URL,
    fallbackUrl: process.env.RFB_ZIP_FALLBACK_URL || '',
    fallbackLocalPath: process.env.RFB_ZIP_LOCAL_PATH || '',
    timeoutMs: parsePositiveInt(process.env.DOWNLOAD_TIMEOUT_MS, DEFAULT_TIMEOUT_MS),
    maxAttempts: parsePositiveInt(process.env.DOWNLOAD_MAX_ATTEMPTS, DEFAULT_MAX_DOWNLOAD_ATTEMPTS),
    backoffBaseMs: parsePositiveInt(process.env.DOWNLOAD_BACKOFF_BASE_MS, DEFAULT_BACKOFF_BASE_MS),
    backoffMaxMs: parsePositiveInt(process.env.DOWNLOAD_BACKOFF_MAX_MS, DEFAULT_BACKOFF_MAX_MS),
    precheckHead: parseBoolean(process.env.DOWNLOAD_PRECHECK_HEAD, PRECHECK_HEAD_DEFAULT),
};

const HTTPS_AGENT = new https.Agent({ keepAlive: true });

async function main() {
    log(`Modo de execucao: ${CONFIG.executionMode}`);
    log(`Configuracao ativa: timeout=${CONFIG.timeoutMs}ms, tentativas=${CONFIG.maxAttempts}, backoffBase=${CONFIG.backoffBaseMs}ms, backoffMax=${CONFIG.backoffMaxMs}ms, precheckHead=${CONFIG.precheckHead}`);

    if (CONFIG.executionMode === EXECUTION_MODE_DIAGNOSTIC) {
        await runDiagnosticMode();
        return;
    }

    await runProcessingMode();
}

async function runDiagnosticMode() {
    log('Iniciando modo diagnostico: teste de DNS e HEAD nas fontes de URL.');

    const urlSources = buildSources().filter((source) => source.kind === 'url');
    if (urlSources.length === 0) {
        throw new Error('Modo diagnostico exige ao menos uma fonte de URL configurada.');
    }

    const failures = [];
    let successCount = 0;

    for (const source of urlSources) {
        try {
            log(`Diagnostico da fonte: ${source.describe()}`);
            await probeUrl(source.value, CONFIG.timeoutMs);
            successCount += 1;
            log(`Fonte OK: ${source.describe()}`);
        } catch (error) {
            failures.push({ source, error });
            log(`${source.describe()} falhou no diagnostico: ${classifyConnectionError(error)} - ${error.message}`);
        }
    }

    if (successCount === 0) {
        throw buildExternalConnectivityError(failures);
    }

    log(`Diagnostico concluido com sucesso. Fontes acessiveis: ${successCount}/${urlSources.length}`);
}

async function runProcessingMode() {
    const tempDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'rfb-cnpj-'));
    const zipPath = path.join(tempDir, 'Estabelecimentos0.zip');

    try {
        log('1) Iniciando processo de obtencao do ZIP...');
        const source = await obtainZipFromSources(zipPath);
        log(`Arquivo obtido com sucesso via ${source.describe()}`);

        log('2) Lendo primeira linha do CSV dentro do ZIP...');
        const firstLine = await readFirstLineFromZip(zipPath);

        if (!firstLine) {
            throw new Error('A primeira linha do arquivo CSV veio vazia.');
        }

        const columns = firstLine.split(';');

        console.log(`\nTotal de colunas encontradas: ${columns.length}`);
        console.log('Valores por posicao:');

        columns.forEach((value, index) => {
            console.log(`Coluna ${index}: ${value}`);
        });
    } finally {
        await safeCleanup(tempDir);
    }
}

function buildSources() {
    const sources = [
        createSource('url', CONFIG.primaryUrl, 'URL principal'),
    ];

    if (CONFIG.fallbackUrl) {
        sources.push(createSource('url', CONFIG.fallbackUrl, 'URL alternativa'));
    }

    if (CONFIG.fallbackLocalPath) {
        sources.push(createSource('local', CONFIG.fallbackLocalPath, 'arquivo local'));
    }

    return sources;
}

function createSource(kind, value, label) {
    return {
        kind,
        value,
        label,
        describe() {
            return `${this.label} (${this.value})`;
        },
    };
}

async function obtainZipFromSources(destinationPath) {
    const sources = buildSources();
    const failures = [];

    for (const source of sources) {
        try {
            log(`Tentando obter ZIP via ${source.describe()}`);

            if (source.kind === 'url') {
                await downloadFileWithRetry(source.value, destinationPath, {
                    maxAttempts: CONFIG.maxAttempts,
                    timeoutMs: CONFIG.timeoutMs,
                    backoffBaseMs: CONFIG.backoffBaseMs,
                    backoffMaxMs: CONFIG.backoffMaxMs,
                    precheckHead: CONFIG.precheckHead,
                });
            } else {
                await copyLocalFile(source.value, destinationPath);
            }

            return source;
        } catch (error) {
            failures.push({ source, error });
            log(`${source.describe()} falhou: ${error.message}`);
        }
    }

    if (isExternalConnectivityFailure(failures, sources)) {
        throw buildExternalConnectivityError(failures);
    }

    const details = failures.map((item) => `${item.source.describe()} falhou: ${item.error.message}`).join(' | ');
    throw new Error(`Nenhuma fonte conseguiu fornecer o ZIP. Detalhes: ${details}`);
}

async function copyLocalFile(sourcePath, destinationPath) {
    const stat = await fsp.stat(sourcePath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error('Arquivo local de fallback nao encontrado ou invalido.');
    }

    await pipeline(
        fs.createReadStream(sourcePath),
        fs.createWriteStream(destinationPath),
    );
}

async function downloadFileWithRetry(url, destinationPath, options) {
    let lastError;

    for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
        try {
            log(`Tentativa ${attempt}/${options.maxAttempts} para ${url}`);

            if (options.precheckHead) {
                await probeUrl(url, options.timeoutMs);
            }

            await downloadFile(url, destinationPath, options.timeoutMs);
            return;
        } catch (error) {
            lastError = error;
            const classification = classifyConnectionError(error);
            log(`Falha na tentativa ${attempt}/${options.maxAttempts}: ${classification} - ${error.message}`);

            if (attempt < options.maxAttempts) {
                const waitMs = calculateBackoffMs(attempt, options.backoffBaseMs, options.backoffMaxMs);
                log(`Aguardando ${waitMs}ms antes da proxima tentativa...`);
                await sleep(waitMs);
            }
        }
    }

    const wrapped = new Error(`Nao foi possivel baixar o arquivo apos ${options.maxAttempts} tentativas: ${lastError.message}`, { cause: lastError });
    if (lastError && lastError.code) wrapped.code = lastError.code;
    throw wrapped;
}

async function probeUrl(url, timeoutMs) {
    const parsed = new URL(url);
    log(`Diagnostico DNS para ${parsed.hostname}`);

    try {
        const addresses = await dns.lookup(parsed.hostname, { all: true });
        const rendered = addresses.map((item) => `${item.address}/IPv${item.family}`).join(', ');
        log(`DNS OK: ${rendered || 'sem enderecos retornados'}`);
    } catch (error) {
        const wrapped = new Error(`Falha DNS (${parsed.hostname}): ${error.message}`, { cause: error });
        if (error && error.code) wrapped.code = error.code;
        throw wrapped;
    }

    await headRequest(url, timeoutMs);
}

function headRequest(url, timeoutMs) {
    return new Promise((resolve, reject) => {
        const request = https.request(url, {
            method: 'HEAD',
            timeout: timeoutMs,
            agent: HTTPS_AGENT,
            headers: {
                'User-Agent': 'extrator-receita/1.0',
            },
        }, (response) => {
            const { statusCode } = response;
            response.resume();

            if (statusCode >= 300 && statusCode < 400 && response.headers.location) {
                log(`HEAD recebeu redirecionamento para ${response.headers.location}`);
                headRequest(response.headers.location, timeoutMs).then(resolve).catch(reject);
                return;
            }

            if (statusCode !== 200) {
                reject(new Error(`HEAD retornou HTTP ${statusCode}`));
                return;
            }

            log(`HEAD OK (HTTP ${statusCode}) para ${url}`);
            resolve();
        });

        request.on('timeout', () => {
            const error = new Error('Timeout no HEAD da URL.');
            error.code = 'ETIMEDOUT';
            request.destroy(error);
        });

        request.on('error', reject);
        request.end();
    });
}

function downloadFile(url, destinationPath, timeoutMs) {
    return new Promise((resolve, reject) => {
        const file = fs.createWriteStream(destinationPath);

        const request = https.get(url, {
            timeout: timeoutMs,
            agent: HTTPS_AGENT,
            headers: {
                'User-Agent': 'extrator-receita/1.0',
            },
        }, (response) => {
            if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
                log(`GET recebeu redirecionamento para ${response.headers.location}`);
                file.close();
                fs.unlink(destinationPath, () => {
                    downloadFile(response.headers.location, destinationPath, timeoutMs).then(resolve).catch(reject);
                });
                return;
            }

            if (response.statusCode !== 200) {
                file.close();
                fs.unlink(destinationPath, () => {
                    reject(new Error(`Falha no download. HTTP ${response.statusCode}`));
                });
                return;
            }

            response.pipe(file);

            file.on('finish', () => {
                file.close(resolve);
            });
        });

        request.on('error', (error) => {
            file.close();
            fs.unlink(destinationPath, () => reject(error));
        });

        file.on('error', (error) => {
            file.close();
            fs.unlink(destinationPath, () => reject(error));
        });

        request.setTimeout(timeoutMs, () => {
            const error = new Error('Timeout no download do arquivo ZIP.');
            error.code = 'ETIMEDOUT';
            request.destroy(error);
        });
    });
}

function readFirstLineFromZip(zipPath) {
    return new Promise((resolve, reject) => {
        yauzl.open(zipPath, { lazyEntries: true }, (openErr, zipFile) => {
            if (openErr) {
                reject(openErr);
                return;
            }

            let resolved = false;

            const fail = (error) => {
                if (resolved) return;
                resolved = true;
                zipFile.close();
                reject(error);
            };

            zipFile.readEntry();

            zipFile.on('entry', (entry) => {
                if (/\/$/.test(entry.fileName)) {
                    zipFile.readEntry();
                    return;
                }

                zipFile.openReadStream(entry, (streamErr, readStream) => {
                    if (streamErr) {
                        fail(streamErr);
                        return;
                    }

                    const rl = readline.createInterface({
                        input: readStream,
                        crlfDelay: Infinity,
                    });

                    let gotLine = false;

                    rl.on('line', (line) => {
                        if (gotLine || resolved) return;
                        gotLine = true;
                        resolved = true;

                        rl.close();
                        readStream.destroy();
                        zipFile.close();

                        resolve(line.replace(/^\uFEFF/, ''));
                    });

                    rl.once('close', () => {
                        if (!gotLine && !resolved) {
                            zipFile.readEntry();
                        }
                    });

                    rl.once('error', fail);
                    readStream.once('error', fail);
                });
            });

            zipFile.once('end', () => {
                if (!resolved) {
                    fail(new Error('Nao foi possivel encontrar uma linha valida no ZIP.'));
                }
            });

            zipFile.once('error', fail);
        });
    });
}

async function safeCleanup(tempDir) {
    try {
        await fsp.rm(tempDir, { recursive: true, force: true });
    } catch {
        // Ignora falhas de limpeza temporaria.
    }
}

function isExternalConnectivityFailure(failures, sources) {
    const urlSourcesCount = sources.filter((source) => source.kind === 'url').length;
    const urlFailures = failures.filter((item) => item.source.kind === 'url');

    if (urlSourcesCount === 0 || urlFailures.length !== urlSourcesCount) {
        return false;
    }

    return urlFailures.every((item) => isConnectivityError(item.error));
}

function buildExternalConnectivityError(failures) {
    const details = failures.map((item) => `${item.source.describe()} falhou: ${item.error.message}`).join(' | ');
    const error = new Error(`Bloqueio de conectividade externa detectado. Detalhes: ${details}`);
    error.code = 'EXTERNAL_CONNECTIVITY_BLOCKED';
    return error;
}

function isConnectivityError(error) {
    const code = error && error.code ? String(error.code) : '';
    if (['ENOTFOUND', 'EAI_AGAIN', 'ECONNREFUSED', 'ECONNRESET', 'EHOSTUNREACH', 'ETIMEDOUT', 'EPROTO', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE'].includes(code)) {
        return true;
    }

    const message = error && error.message ? String(error.message) : '';
    return /timeout|timedout|dns|enotfound|eai_again|econnrefused|ehostunreach|econnreset/i.test(message);
}

function calculateBackoffMs(attempt, baseMs, maxMs) {
    const exponential = Math.min(maxMs, baseMs * (2 ** (attempt - 1)));
    const jitter = Math.floor(Math.random() * Math.max(100, Math.floor(exponential * 0.2)));
    return Math.min(maxMs, exponential + jitter);
}

function classifyConnectionError(error) {
    const code = error && error.code ? String(error.code) : '';

    if (code === 'ENOTFOUND' || code === 'EAI_AGAIN') return 'falha de DNS';
    if (code === 'ECONNREFUSED' || code === 'ECONNRESET' || code === 'EHOSTUNREACH') return 'bloqueio ou indisponibilidade de rede';
    if (code === 'ETIMEDOUT') return 'timeout de conexao';
    if (code === 'EPROTO' || code === 'UNABLE_TO_VERIFY_LEAF_SIGNATURE') return 'problema de TLS/certificado';

    return 'falha de conectividade';
}

function parseExecutionMode(argv, envModeValue) {
    const modeArg = argv.find((arg) => arg.startsWith('--mode='));
    const raw = modeArg ? modeArg.split('=')[1] : envModeValue;

    if (!raw) return EXECUTION_MODE_PROCESSING;

    const normalized = String(raw).trim().toLowerCase();

    if (normalized === 'diagnostic' || normalized === 'diagnostico') {
        return EXECUTION_MODE_DIAGNOSTIC;
    }

    if (normalized === 'processing' || normalized === 'processamento') {
        return EXECUTION_MODE_PROCESSING;
    }

    return EXECUTION_MODE_PROCESSING;
}

function parsePositiveInt(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
    return parsed;
}

function parseBoolean(value, fallback) {
    if (typeof value !== 'string') return fallback;

    const normalized = value.trim().toLowerCase();
    if (normalized === 'true' || normalized === '1') return true;
    if (normalized === 'false' || normalized === '0') return false;

    return fallback;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function log(message) {
    console.log(`[${new Date().toISOString()}] ${message}`);
}

main().catch((error) => {
    console.error('Erro durante a execucao:', error.message);

    if (error.code === 'EXTERNAL_CONNECTIVITY_BLOCKED') {
        console.error('Recomendacao: execute em um ambiente de nuvem com egress HTTPS liberado (ex.: Railway, GCP Cloud Run, VM com NAT/proxy).');
        console.error('Alternativas imediatas: configure HTTPS_PROXY/HTTP_PROXY ou use RFB_ZIP_LOCAL_PATH para fallback local.');
    }

    process.exitCode = 1;
});
