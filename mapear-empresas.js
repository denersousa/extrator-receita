const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const readline = require('node:readline');
const yauzl = require('yauzl');

const ZIP_PATH = './Empresas0.zip';

async function main() {
    const absoluteZipPath = path.resolve(process.cwd(), ZIP_PATH);

    await assertZipExists(absoluteZipPath);

    const result = await processEmpresasFromZip(absoluteZipPath);

    if (!result.sample) {
        throw new Error('Nenhuma linha valida foi encontrada no arquivo de empresas.');
    }

    console.log('Exemplo de objeto processado:');
    console.log(JSON.stringify(result.sample, null, 2));
    console.log(`Total de linhas processadas: ${result.processedCount}`);
}

async function assertZipExists(zipPath) {
    const stat = await fsp.stat(zipPath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error(`Arquivo ZIP nao encontrado em: ${zipPath}`);
    }
}

function processEmpresasFromZip(zipPath) {
    return new Promise((resolve, reject) => {
        yauzl.open(zipPath, { lazyEntries: true }, (openErr, zipFile) => {
            if (openErr) {
                reject(openErr);
                return;
            }

            let resolved = false;
            let processedCount = 0;
            let sample = null;

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

                    rl.on('line', (line) => {
                        if (resolved) return;

                        const cleanLine = line.replace(/^\uFEFF/, '');
                        if (!cleanLine.trim()) {
                            return;
                        }

                        const empresa = transformEmpresaLine(cleanLine);
                        processedCount += 1;

                        if (!sample) {
                            sample = empresa;
                        }
                    });

                    rl.once('close', () => {
                        if (resolved) return;

                        // Processa apenas a primeira entrada valida do ZIP e encerra.
                        resolved = true;
                        zipFile.close();
                        resolve({ sample, processedCount });
                    });

                    rl.once('error', fail);
                    readStream.once('error', fail);
                });
            });

            zipFile.once('end', () => {
                if (!resolved) {
                    resolved = true;
                    resolve({ sample, processedCount });
                }
            });

            zipFile.once('error', fail);
        });
    });
}

function transformEmpresaLine(line) {
    const columns = line.split(';');

    return {
        cnpjBase: normalizeField(columns[0]),
        razaoSocial: normalizeField(columns[1]),
        naturezaJuridica: normalizeField(columns[2]),
        qualificacaoResponsavel: normalizeField(columns[3]),
        capitalSocial: parseCapitalSocial(columns[4]),
        porte: normalizeField(columns[5]),
        enteFederativo: normalizeField(columns[6]),
    };
}

function normalizeField(value) {
    if (value === undefined || value === null) {
        return null;
    }

    const trimmed = String(value).trim().replace(/^"|"$/g, '');
    return trimmed === '' ? null : trimmed;
}

function parseCapitalSocial(value) {
    const normalized = normalizeField(value);
    if (normalized === null) {
        return null;
    }

    const asFloat = Number.parseFloat(normalized.replace(',', '.'));
    return Number.isNaN(asFloat) ? null : asFloat;
}

main().catch((error) => {
    console.error('Erro durante a execucao:', error.message);
    process.exitCode = 1;
});
