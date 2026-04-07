const { PrismaClient } = require('@prisma/client');
const fs = require('node:fs');
const path = require('node:path');

// Carrega .env manualmente
const envPath = path.join(__dirname, '..', '.env');
const raw = fs.readFileSync(envPath, 'utf8');
for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    const value = trimmed.slice(eq + 1).trim().replace(/^"|"$/g, '');
    if (!(key in process.env)) process.env[key] = value;
}

console.log('DATABASE_URL:', process.env.DATABASE_URL);

const prisma = new PrismaClient();
prisma.$queryRawUnsafe('SELECT COUNT(*) as total FROM "Empresa"')
    .then((r) => {
        console.log('Conexao OK. Total de empresas no banco:', r[0].total.toString());
    })
    .catch((e) => {
        console.error('Erro na conexao:', e.message);
    })
    .finally(() => prisma.$disconnect());
