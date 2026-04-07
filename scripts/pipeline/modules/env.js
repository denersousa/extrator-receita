const fs = require('node:fs');

function loadEnvFile(envPath) {
    try {
        const raw = fs.readFileSync(envPath, 'utf8');
        for (const line of raw.split(/\r?\n/)) {
            const trimmed = line.trim();
            if (!trimmed || trimmed.startsWith('#')) {
                continue;
            }

            const eqIndex = trimmed.indexOf('=');
            if (eqIndex <= 0) {
                continue;
            }

            const key = trimmed.slice(0, eqIndex).trim();
            const value = trimmed.slice(eqIndex + 1).trim().replace(/^"|"$/g, '');
            if (!(key in process.env)) {
                process.env[key] = value;
            }
        }
    } catch {
        // optional .env
    }
}

function intEnv(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === null || raw === '') {
        return fallback;
    }

    const parsed = Number.parseInt(String(raw), 10);
    return Number.isNaN(parsed) ? fallback : parsed;
}

function boolEnv(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === null || raw === '') {
        return fallback;
    }

    const normalized = String(raw).trim().toLowerCase();
    if (normalized === '1' || normalized === 'true' || normalized === 'yes') {
        return true;
    }

    if (normalized === '0' || normalized === 'false' || normalized === 'no') {
        return false;
    }

    return fallback;
}

function stringEnv(name, fallback) {
    const raw = process.env[name];
    if (raw === undefined || raw === null) {
        return fallback;
    }

    const normalized = String(raw).trim();
    return normalized === '' ? fallback : normalized;
}

module.exports = {
    loadEnvFile,
    intEnv,
    boolEnv,
    stringEnv,
};
