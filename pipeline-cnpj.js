const fsp = require('node:fs/promises');
const path = require('node:path');
const readline = require('node:readline');
const yauzl = require('yauzl');

async function main() {
    const empresas = ['./Empresas0.zip'];
    const estabelecimentos = ['./Estabelecimentos0.zip'];

    for (const arquivo of empresas) {
        const resultado = await processarEmpresas(arquivo);
        logResumo('empresas', arquivo, resultado);
        await deletarArquivo(arquivo);
    }

    for (const arquivo of estabelecimentos) {
        const resultado = await processarEstabelecimentos(arquivo);
        logResumo('estabelecimentos', arquivo, resultado);
        await deletarArquivo(arquivo);
    }
}

async function processarEmpresas(caminhoZip) {
    const absoluteZipPath = path.resolve(process.cwd(), caminhoZip);
    await assertZipExists(absoluteZipPath);

    let processedCount = 0;
    let sample = null;

    await streamZipLines(absoluteZipPath, (line) => {
        const registro = transformarLinhaEmpresa(line);

        // Ponto de extensao para persistencia futura (nao implementada agora).
        prepararEmpresaParaPersistencia(registro);

        processedCount += 1;
        if (!sample) {
            sample = registro;
        }
    });

    if (!sample) {
        throw new Error(`Nenhuma linha valida encontrada em ${caminhoZip}`);
    }

    return { processedCount, sample };
}

async function processarEstabelecimentos(caminhoZip) {
    const absoluteZipPath = path.resolve(process.cwd(), caminhoZip);
    await assertZipExists(absoluteZipPath);

    let processedCount = 0;
    let sample = null;

    await streamZipLines(absoluteZipPath, (line) => {
        const registro = transformarLinhaEstabelecimento(line);

        // Ponto de extensao para persistencia futura (nao implementada agora).
        prepararEstabelecimentoParaPersistencia(registro);

        processedCount += 1;
        if (!sample) {
            sample = registro;
        }
    });

    if (!sample) {
        throw new Error(`Nenhuma linha valida encontrada em ${caminhoZip}`);
    }

    return { processedCount, sample };
}

async function streamZipLines(zipPath, onLine) {
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

                        const cleanLine = line.replace(/^\uFEFF/, '');
                        if (!cleanLine.trim()) {
                            return;
                        }

                        try {
                            onLine(cleanLine);
                        } catch (error) {
                            fail(error);
                        }
                    });

                    rl.once('close', () => {
                        if (done) return;
                        done = true;
                        zipFile.close();
                        resolve();
                    });

                    rl.once('error', fail);
                    readStream.once('error', fail);
                });
            });

            zipFile.once('end', () => {
                if (!done) {
                    done = true;
                    resolve();
                }
            });

            zipFile.once('error', fail);
        });
    });
}

function transformarLinhaEmpresa(line) {
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

function transformarLinhaEstabelecimento(line) {
    const columns = line.split(';');

    const cnpjBase = normalizeField(columns[0]);
    const cnpjOrdem = normalizeField(columns[1]);
    const cnpjDv = normalizeField(columns[2]);

    return {
        cnpjBase,
        cnpjOrdem,
        cnpjDv,
        cnpjCompleto: montarCnpjCompleto(cnpjBase, cnpjOrdem, cnpjDv),
        nomeFantasia: normalizeField(columns[4]),
        situacaoCadastral: normalizeField(columns[5]),
        cnaePrincipal: normalizeField(columns[11]),
        endereco: {
            tipoLogradouro: normalizeField(columns[13]),
            logradouro: normalizeField(columns[14]),
            numero: normalizeField(columns[15]),
            complemento: normalizeField(columns[16]),
            bairro: normalizeField(columns[17]),
            cep: normalizeField(columns[18]),
            uf: normalizeField(columns[19]),
            municipioCodigo: normalizeField(columns[20]),
        },
        contato: {
            ddd1: normalizeField(columns[21]),
            telefone1: normalizeField(columns[22]),
            ddd2: normalizeField(columns[23]),
            telefone2: normalizeField(columns[24]),
            dddFax: normalizeField(columns[25]),
            fax: normalizeField(columns[26]),
            email: normalizeField(columns[27]),
        },
    };
}

function montarCnpjCompleto(cnpjBase, cnpjOrdem, cnpjDv) {
    if (!cnpjBase || !cnpjOrdem || !cnpjDv) {
        return null;
    }

    return `${cnpjBase.padStart(8, '0')}${cnpjOrdem.padStart(4, '0')}${cnpjDv.padStart(2, '0')}`;
}

function parseCapitalSocial(value) {
    const normalized = normalizeField(value);
    if (normalized === null) {
        return null;
    }

    const asFloat = Number.parseFloat(normalized.replace(',', '.'));
    return Number.isNaN(asFloat) ? null : asFloat;
}

function normalizeField(value) {
    if (value === undefined || value === null) {
        return null;
    }

    const trimmed = String(value).trim().replace(/^"|"$/g, '');
    return trimmed === '' ? null : trimmed;
}

function prepararEmpresaParaPersistencia(registro) {
    return registro;
}

function prepararEstabelecimentoParaPersistencia(registro) {
    return registro;
}

async function assertZipExists(zipPath) {
    const stat = await fsp.stat(zipPath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error(`Arquivo ZIP nao encontrado em: ${zipPath}`);
    }
}

async function deletarArquivo(caminho) {
    const absolutePath = path.resolve(process.cwd(), caminho);
    await fsp.unlink(absolutePath);
    console.log(`[cleanup] Arquivo removido: ${caminho}`);
}

function logResumo(tipo, caminho, resultado) {
    console.log(`\n[${tipo}] Arquivo processado: ${caminho}`);
    console.log(`[${tipo}] Total de linhas: ${resultado.processedCount}`);
    console.log(`[${tipo}] Exemplo de registro:`);
    console.log(JSON.stringify(resultado.sample, null, 2));
}

main().catch((error) => {
    console.error('Erro durante a execucao:', error.message);
    process.exitCode = 1;
});
