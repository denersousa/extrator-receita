const { runPipeline } = require('./pipeline/runner');

runPipeline().catch((error) => {
    console.error('Erro durante a execucao:', error.message);
    process.exitCode = 1;
});
