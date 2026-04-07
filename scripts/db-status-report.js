const path = require('node:path');
const { Client } = require('pg');
const { loadEnvFile } = require('./pipeline/modules/env');

const BASE_DIR = path.resolve(__dirname, '..');
loadEnvFile(path.join(BASE_DIR, '.env'));

async function querySingleValue(client, sql) {
    const result = await client.query(sql);
    const firstRow = result.rows[0] || {};
    const firstKey = Object.keys(firstRow)[0];
    return firstKey ? firstRow[firstKey] : null;
}

async function main() {
    if (!process.env.DATABASE_URL) {
        throw new Error('DATABASE_URL nao definido no ambiente/.env');
    }

    const client = new Client({ connectionString: process.env.DATABASE_URL });
    await client.connect();

    try {
        const dbMeta = (await client.query(`
            SELECT
                current_database() AS database,
                current_user AS user_name,
                now() AS server_time
        `)).rows[0];

        const tableExists = (await client.query(`
            SELECT
                to_regclass('receita.empresa') IS NOT NULL AS empresa,
                to_regclass('receita.estabelecimento') IS NOT NULL AS estabelecimento
        `)).rows[0];

        const estimatedRows = (await client.query(`
            SELECT relname AS table_name, COALESCE(n_live_tup, 0)::bigint AS estimated_rows
            FROM pg_stat_user_tables
            WHERE schemaname = 'receita' AND relname IN ('empresa', 'estabelecimento')
            ORDER BY relname
        `)).rows;

        const estimatedEmpresas = Number(estimatedRows.find((row) => row.table_name === 'empresa')?.estimated_rows || 0);
        const estimatedEstabelecimentos = Number(estimatedRows.find((row) => row.table_name === 'estabelecimento')?.estimated_rows || 0);

        const empresasComEstabelecimento = Number(await querySingleValue(
            client,
            'SELECT COUNT(DISTINCT "cnpjBase")::bigint AS total FROM receita.estabelecimento',
        ));

        const situacaoTop = (await client.query(`
            SELECT "situacaoCadastral", COUNT(*)::bigint AS total
            FROM receita.estabelecimento
            GROUP BY "situacaoCadastral"
            ORDER BY total DESC
            LIMIT 5
        `)).rows;

        const amostraJoin = (await client.query(`
            SELECT
                emp."cnpjBase",
                emp."razaoSocial",
                est."cnpjCompleto",
                est."nomeFantasia",
                est."uf",
                est."municipio"
            FROM receita.empresa emp
            JOIN receita.estabelecimento est ON est."cnpjBase" = emp."cnpjBase"
            ORDER BY emp."cnpjBase" ASC, est."cnpjCompleto" ASC
            LIMIT 5
        `)).rows;

        const report = {
            generatedAt: new Date().toISOString(),
            database: {
                name: dbMeta.database,
                user: dbMeta.user_name,
                serverTime: dbMeta.server_time,
            },
            tables: {
                empresaExists: tableExists.empresa,
                estabelecimentoExists: tableExists.estabelecimento,
            },
            totals: {
                empresasEstimadas: estimatedEmpresas,
                estabelecimentosEstimados: estimatedEstabelecimentos,
                empresasComEstabelecimento,
            },
            topSituacaoCadastral: situacaoTop,
            sampleEmpresaEstabelecimento: amostraJoin,
            integrationHints: [
                'Use cnpjBase para joins Empresa -> Estabelecimento.',
                'Use cnpjCompleto como identificador unico de estabelecimento.',
                'Campos de contato (telefone/email) podem ser nulos; trate no projeto.',
            ],
        };

        console.log(JSON.stringify(report, null, 2));
    } finally {
        await client.end();
    }
}

main().catch((error) => {
    console.error(`[db-status-report] ${error.message || error}`);
    process.exitCode = 1;
});