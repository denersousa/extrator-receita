const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const readline = require('node:readline');
const yauzl = require('yauzl');

const ZIP_PATH = './Empresas0.zip';

async function main() {
    const absoluteZipPath = path.resolve(process.cwd(), ZIP_PATH);

    await assertZipExists(absoluteZipPath);

    const firstLine = await readFirstLineFromZip(absoluteZipPath);
    if (!firstLine) {
        throw new Error('A primeira linha do CSV esta vazia.');
    }

    const columns = firstLine.split(';');

    console.log(`Total de colunas encontradas: ${columns.length}`);
    columns.forEach((value, index) => {
        console.log(`Coluna ${index}: ${value}`);
    });
}

async function assertZipExists(zipPath) {
    const stat = await fsp.stat(zipPath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error(`Arquivo ZIP nao encontrado em: ${zipPath}`);
    }
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

main().catch((error) => {
    console.error('Erro durante a execucao:', error.message);
    process.exitCode = 1;
});
