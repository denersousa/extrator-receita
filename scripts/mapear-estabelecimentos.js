const fsp = require('node:fs/promises');
const path = require('node:path');
const readline = require('node:readline');
const yauzl = require('yauzl');

const BASE_DIR = path.resolve(__dirname, '..');
const ZIP_PATH = path.join('data', 'estabelecimentos', 'Estabelecimentos0.zip');

async function main() {
    const absoluteZipPath = path.join(BASE_DIR, ZIP_PATH);
    console.log(`[diagnostico] Processando arquivo: ${absoluteZipPath}`);

    await assertZipExists(absoluteZipPath);

    console.log('Abrindo arquivo ZIP');
    const firstLine = await readFirstLineFromZip(absoluteZipPath);

    if (!firstLine) {
        throw new Error('A primeira linha do CSV esta vazia.');
    }

    console.log('Lendo primeira linha');
    const columns = firstLine.split(';');

    console.log(`Total de colunas encontradas: ${columns.length}`);
    console.log('');

    columns.forEach((value, index) => {
        console.log(`Coluna ${index}: ${value}`);
    });

    console.log('Mapeamento concluido');
}

async function assertZipExists(zipPath) {
    const stat = await fsp.stat(zipPath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error(`Arquivo nao encontrado: ${zipPath}`);
    }

    console.log(`[diagnostico] Arquivo encontrado: ${zipPath}`);
}

function readFirstLineFromZip(zipPath) {
    return new Promise((resolve, reject) => {
        yauzl.open(zipPath, { lazyEntries: true }, (openErr, zipFile) => {
            if (openErr) {
                reject(openErr);
                return;
            }

            let done = false;

            const fail = (error) => {
                if (done) return;
                done = true;
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
                        if (done) return;
                        done = true;

                        rl.close();
                        readStream.destroy();
                        zipFile.close();

                        resolve(line.replace(/^\uFEFF/, ''));
                    });

                    rl.once('close', () => {
                        if (!done) {
                            zipFile.readEntry();
                        }
                    });

                    rl.once('error', fail);
                    readStream.once('error', fail);
                });
            });

            zipFile.once('end', () => {
                if (!done) {
                    done = true;
                    reject(new Error('Nao foi possivel encontrar uma linha valida no ZIP.'));
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
