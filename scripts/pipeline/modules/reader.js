const yauzl = require('yauzl');
const readline = require('node:readline');

async function streamZipLines(zipPath, onLine) {
    return new Promise((resolve, reject) => {
        yauzl.open(zipPath, { lazyEntries: true }, (openErr, zipFile) => {
            if (openErr) {
                reject(openErr);
                return;
            }

            let done = false;
            let lineChain = Promise.resolve();

            const fail = (error) => {
                if (done) {
                    return;
                }

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
                        if (done) {
                            return;
                        }

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
                        lineChain
                            .then(() => {
                                if (!done) {
                                    done = true;
                                    zipFile.close();
                                    resolve();
                                }
                            })
                            .catch(fail);
                    });

                    rl.once('error', fail);
                    readStream.once('error', fail);
                });
            });

            zipFile.once('error', fail);
        });
    });
}

module.exports = {
    streamZipLines,
};
