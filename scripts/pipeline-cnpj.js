const fsp = require('node:fs/promises');
const path = require('node:path');
const readline = require('node:readline');
const { PrismaClient } = require('@prisma/client');
const yauzl = require('yauzl');

const BASE_DIR = path.resolve(__dirname, '..');
const prisma = new PrismaClient();
const BATCH_SIZE = 1000;
const SAFE_MODE = true;
const RUN_PIPELINE = process.env.RUN_PIPELINE === 'true';

async function main() {
    if (!RUN_PIPELINE) {
        console.log('Pipeline desativado');
        console.log('Pipeline desativado. Defina RUN_PIPELINE=true para executar.');
        process.exit(0);
    }

    console.log('Executando pipeline em producao');

    const empresas = [
        path.join(BASE_DIR, 'data', 'empresas', 'Empresas0.zip'),
    ];
    const estabelecimentos = [
        path.join(BASE_DIR, 'data', 'estabelecimentos', 'Estabelecimentos0.zip'),
    ];

    try {
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
    } finally {
        await prisma.$disconnect();
    }
}

async function processarEmpresas(caminhoZip) {
    const absoluteZipPath = path.resolve(caminhoZip);
    console.log(`[diagnostico] Processando arquivo: ${absoluteZipPath}`);
    await assertZipExists(absoluteZipPath);

    let processedCount = 0;
    let insertedCount = 0;
    let skippedCount = 0;
    let sample = null;
    let batch = [];

    try {
        await streamZipLines(absoluteZipPath, async (line) => {
            const registro = prepararEmpresaParaPersistencia(transformarLinhaEmpresa(line));
            if (!registro) {
                skippedCount += 1;
                return;
            }

            batch.push(registro);
            processedCount += 1;

            if (!sample) {
                sample = registro;
            }

            if (batch.length >= BATCH_SIZE) {
                insertedCount += await inserirBatchEmpresas(batch);
                batch = [];
            }
        });

        if (batch.length > 0) {
            insertedCount += await inserirBatchEmpresas(batch);
        }
    } catch (error) {
        throw new Error(`Falha no processamento de empresas (${caminhoZip}): ${error.message}`);
    }

    if (!sample) {
        throw new Error(`Nenhuma linha valida encontrada em ${caminhoZip}`);
    }

    return { processedCount, insertedCount, skippedCount, sample };
}

async function processarEstabelecimentos(caminhoZip) {
    const absoluteZipPath = path.resolve(caminhoZip);
    console.log(`[diagnostico] Processando arquivo: ${absoluteZipPath}`);
    await assertZipExists(absoluteZipPath);

    let processedCount = 0;
    let insertedCount = 0;
    let skippedCount = 0;
    let sample = null;
    let batch = [];

    try {
        await streamZipLines(absoluteZipPath, async (line) => {
            const registro = prepararEstabelecimentoParaPersistencia(transformarLinhaEstabelecimento(line));
            if (!registro) {
                skippedCount += 1;
                return;
            }

            batch.push(registro);
            processedCount += 1;

            if (!sample) {
                sample = registro;
            }

            if (batch.length >= BATCH_SIZE) {
                insertedCount += await inserirBatchEstabelecimentos(batch);
                batch = [];
            }
        });

        if (batch.length > 0) {
            insertedCount += await inserirBatchEstabelecimentos(batch);
        }
    } catch (error) {
        throw new Error(`Falha no processamento de estabelecimentos (${caminhoZip}): ${error.message}`);
    }

    if (!sample) {
        throw new Error(`Nenhuma linha valida encontrada em ${caminhoZip}`);
    }

    return { processedCount, insertedCount, skippedCount, sample };
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

                    let lineChain = Promise.resolve();

                    rl.on('line', (line) => {
                        if (done) return;

                        const cleanLine = line.replace(/^\uFEFF/, '');
                        if (!cleanLine.trim()) {
                            return;
                        }

                        rl.pause();
                        lineChain = lineChain
                            .then(() => onLine(cleanLine))
                            .then(() => {
                                if (!done) {
                                    rl.resume();
                                }
                            })
                            .catch(fail);
                    });

                    rl.once('close', () => {
                        if (done) return;

                        lineChain
                            .then(() => {
                                if (done) return;
                                done = true;
                                zipFile.close();
                                resolve();
                            })
                            .catch(fail);
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
        matrizFilial: normalizeField(columns[3]),
        nomeFantasia: normalizeField(columns[4]),
        situacaoCadastral: normalizeField(columns[5]),
        dataSituacao: normalizeField(columns[6]),
        motivoSituacao: normalizeField(columns[7]),
        nomeCidadeExterior: normalizeField(columns[8]),
        pais: normalizeField(columns[9]),
        dataInicioAtividade: normalizeField(columns[10]),
        cnaePrincipal: normalizeField(columns[11]),
        cnaeSecundarios: normalizeField(columns[12]),
        tipoLogradouro: normalizeField(columns[13]),
        logradouro: normalizeField(columns[14]),
        numero: normalizeField(columns[15]),
        complemento: normalizeField(columns[16]),
        bairro: normalizeField(columns[17]),
        cep: normalizeField(columns[18]),
        uf: normalizeField(columns[19]),
        municipio: normalizeField(columns[20]),
        ddd1: normalizeField(columns[21]),
        telefone1: normalizeField(columns[22]),
        ddd2: normalizeField(columns[23]),
        telefone2: normalizeField(columns[24]),
        dddFax: normalizeField(columns[25]),
        fax: normalizeField(columns[26]),
        email: normalizeField(columns[27]),
        situacaoEspecial: normalizeField(columns[28]),
        dataSituacaoEspecial: normalizeField(columns[29]),
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
    if (!registro.cnpjBase) {
        return null;
    }

    return registro;
}

function prepararEstabelecimentoParaPersistencia(registro) {
    if (!registro.cnpjBase || !registro.cnpjOrdem || !registro.cnpjDv || !registro.cnpjCompleto) {
        return null;
    }

    if (!registro.matrizFilial || !registro.situacaoCadastral || !registro.cnaePrincipal) {
        return null;
    }

    return registro;
}

async function inserirBatchEmpresas(batch) {
    try {
        const result = await prisma.empresa.createMany({
            data: batch,
            skipDuplicates: true,
        });
        console.log(`Inseridos ${result.count} registros em empresa`);
        return result.count;
    } catch (error) {
        throw new Error(`Erro ao inserir lote de empresas: ${error.message}`);
    }
}

async function inserirBatchEstabelecimentos(batch) {
    try {
        const result = await prisma.estabelecimento.createMany({
            data: batch,
            skipDuplicates: true,
        });
        console.log(`Inseridos ${result.count} registros em estabelecimento`);
        return result.count;
    } catch (error) {
        throw new Error(`Erro ao inserir lote de estabelecimentos: ${error.message}`);
    }
}

async function assertZipExists(zipPath) {
    const stat = await fsp.stat(zipPath).catch(() => null);
    if (!stat || !stat.isFile()) {
        throw new Error(`Arquivo nao encontrado: ${zipPath}`);
    }

    console.log(`[diagnostico] Arquivo encontrado: ${zipPath}`);
}

async function deletarArquivo(caminho) {
    const absolutePath = path.resolve(caminho);

    if (SAFE_MODE === true) {
        console.log(`[cleanup] SAFE MODE ativo - arquivo nao removido: ${absolutePath}`);
        return;
    }

    await fsp.unlink(absolutePath);
    console.log(`[cleanup] Arquivo removido: ${absolutePath}`);
}

function logResumo(tipo, caminho, resultado) {
    console.log(`\n[${tipo}] Arquivo processado: ${caminho}`);
    console.log(`[${tipo}] Total de linhas validas: ${resultado.processedCount}`);
    console.log(`[${tipo}] Total inserido: ${resultado.insertedCount}`);
    console.log(`[${tipo}] Total descartado: ${resultado.skippedCount}`);
    console.log(`[${tipo}] Exemplo de registro:`);
    console.log(JSON.stringify(resultado.sample, null, 2));
}

main().catch((error) => {
    console.error('Erro durante a execucao:', error.message);
    process.exitCode = 1;
});
