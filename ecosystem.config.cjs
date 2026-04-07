module.exports = {
    apps: [
        {
            name: 'pipeline-cnpj',
            script: 'scripts/pipeline-cnpj.js',
            cwd: __dirname,
            instances: 1,
            exec_mode: 'fork',
            autorestart: true,
            watch: false,
            max_memory_restart: '4G',
            restart_delay: 5000,
            env: {
                NODE_ENV: 'production',
            },
            out_file: 'logs/pipeline-out.log',
            error_file: 'logs/pipeline-error.log',
            merge_logs: true,
            time: true,
        },
    ],
};
