const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const readline = require('node:readline');
const yauzl = require('yauzl');

const ZIP_PATH = 'data/estabelecimentos/Estabelecimentos0.zip';

async function main() {
    const absoluteZipPath = path.resolve(__dirname, '..', ZIP_PATH);

    await assertZipExists(absoluteZipPath);

    const lines = await readFirstLinesFromZip(absoluteZipPath, 20);
    if (lines.length === 0) {
        throw new Error('Nao foi encontrada nenhuma linha valida no CSV.');
    }

    lines.forEach((line, index) => {
        const columns = line.split(';');
        const cnpj = buildCnpjCompleto(columns[0], columns[1], columns[2]);
        const nomeFantasia = normalizeField(columns[4]);
        const telefone = pickTelefone(columns[21], columns[22], columns[23], columns[24]);
        const email = normalizeField(columns[27]);

        console.log(`Linha ${index + 1}:`);
        console.log(`CNPJ: ${cnpj ?? 'N/A'}`);
        console.log(`Nome: ${nomeFantasia ?? 'N/A'}`);
        console.log(`Telefone: ${telefone ?? 'N/A'}`);
        console.log(`Email: ${email ?? 'N/A'}`);
        console.log('');
    });
}

async function assertZipExists(zipPath) {
    const stat = await fsp.stat(zipPath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error(`Arquivo ZIP nao encontrado em: ${zipPath}`);
    }
}

function readFirstLinesFromZip(zipPath, maxLines) {
    return new Promise((resolve, reject) => {
        yauzl.open(zipPath, { lazyEntries: true }, (openErr, zipFile) => {
            if (openErr) {
                reject(openErr);
                return;
            }

            let resolved = false;
            const lines = [];

            const fail = (error) => {
                if (resolved) return;
                resolved = true;
                zipFile.close();
                reject(error);
            };

            zipFile.readEntry();

            zipFile.on('entry', (entry) => {
                // Ignora diretorios e segue para a proxima entrada do ZIP.
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

                        lines.push(cleanLine);

                        if (lines.length >= maxLines) {
                            resolved = true;
                            rl.close();
                            readStream.destroy();
                            zipFile.close();
                            resolve(lines);
                        }
                    });

                    rl.once('close', () => {
                        if (!resolved) {
                            // Arquivo vazio: tenta a proxima entrada dentro do ZIP.
                            zipFile.readEntry();
                        }
                    });

                    rl.once('error', fail);
                    readStream.once('error', fail);
                });
            });

            zipFile.once('end', () => {
                if (!resolved) {
                    resolved = true;
                    resolve(lines);
                }
            });

            zipFile.once('error', fail);
        });
    });
}

function normalizeField(value) {
    if (value === undefined || value === null) {
        return null;
    }

    const trimmed = String(value).trim().replace(/^"|"$/g, '');
    return trimmed === '' ? null : trimmed;
}

function buildCnpjCompleto(base, ordem, dv) {
    const cnpjBase = normalizeField(base);
    const cnpjOrdem = normalizeField(ordem);
    const cnpjDv = normalizeField(dv);

    if (!cnpjBase || !cnpjOrdem || !cnpjDv) {
        return null;
    }

    return `${cnpjBase.padStart(8, '0')}${cnpjOrdem.padStart(4, '0')}${cnpjDv.padStart(2, '0')}`;
}

function pickTelefone(ddd1, telefone1, ddd2, telefone2) {
    const d1 = normalizeField(ddd1);
    const t1 = normalizeField(telefone1);
    if (d1 && t1) {
        return `(${d1}) ${t1}`;
    }

    const d2 = normalizeField(ddd2);
    const t2 = normalizeField(telefone2);
    if (d2 && t2) {
        return `(${d2}) ${t2}`;
    }

    return null;
}

main().catch((error) => {
    console.error('Erro durante a execucao:', error.message);
    process.exitCode = 1;
});
