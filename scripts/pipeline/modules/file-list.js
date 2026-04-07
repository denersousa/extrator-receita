const fsp = require('node:fs/promises');
const path = require('node:path');

async function listZipFiles(dataDir, folder, prefix, startIndex, endIndex) {
    const dirPath = path.join(dataDir, folder);
    const entries = await fsp.readdir(dirPath, { withFileTypes: true });

    const files = entries
        .filter((entry) => entry.isFile())
        .map((entry) => entry.name)
        .map((name) => {
            if (!name.startsWith(prefix) || !name.toLowerCase().endsWith('.zip')) {
                return null;
            }

            const idxText = name.slice(prefix.length, -4);
            if (!/^\d+$/.test(idxText)) {
                return null;
            }

            const index = Number.parseInt(idxText, 10);
            if (index < startIndex || index > endIndex) {
                return null;
            }

            return {
                index,
                name,
                path: path.join(dirPath, name),
            };
        })
        .filter((item) => item !== null)
        .sort((a, b) => a.index - b.index);

    return files;
}

module.exports = {
    listZipFiles,
};
