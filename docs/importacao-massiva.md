# Importacao Massiva CNPJ (Producao)

## 1) Infra Docker/PostgreSQL (WSL2)

- O banco usa volume nomeado Docker (`postgres_data`) para evitar I/O lento de bind no filesystem do Windows.
- Foi adicionado `shm_size: 1gb`, `ulimits.nofile`, `stop_grace_period` e limite de CPU/RAM no container.
- Logs rotacionam com `max-size`/`max-file` para evitar crescimento infinito.

Subir o banco:

```bash
docker compose up -d postgres
docker compose ps
docker compose logs -f postgres
```

Monitorar recursos do container:

```bash
docker stats extrator-postgres
```

## 2) Postgres tuning para carga massiva

Arquivo ativo: `postgres/postgresql.conf`.

Parametros principais ajustados:
- `shared_buffers = 3GB`
- `work_mem = 32MB`
- `maintenance_work_mem = 1GB`
- `wal_buffers = 32MB`
- `checkpoint_timeout = 20min`
- `max_wal_size = 8GB`
- `wal_compression = on`

Observacao importante:
- `synchronous_commit` ficou `on` globalmente (mais seguro).
- Durante importacao, o pipeline aplica `SET LOCAL synchronous_commit = OFF` por transacao para acelerar sem deixar o cluster permanentemente em modo menos duravel.

Aplicar alteracoes de config:

```bash
docker compose restart postgres
docker compose exec -T postgres psql -U postgres -d DealRadar -c "SHOW shared_buffers;"
```

## 3) Pipeline robusto (COPY + retry + watchdog)

O pipeline usa `COPY FROM STDIN` (via `pg-copy-streams`) com merge em tabela alvo.

Recursos implementados:
- Retry exponencial para erros transientes de conexao.
- Controle de concorrencia de COPY por semaforo (`PIPELINE_DB_MAX_CONCURRENT_COPY`).
- Checkpoint por linha (`.pipeline-checkpoint.json`) para retomada automatica.
- Watchdog de travamento: se nao houver progresso por `PIPELINE_WATCHDOG_TIMEOUT_MS`, o processo encerra com erro para reinicio externo (PM2).
- Metricas por batch com throughput, tempo e memoria em `.pipeline-status.json`.

Variaveis recomendadas em `.env` para inicio estavel:

```dotenv
PIPELINE_BATCH_INITIAL=1000
PIPELINE_BATCH_MIN=500
PIPELINE_BATCH_MAX=2000

PIPELINE_DB_MAX_CONCURRENT_COPY=1
PIPELINE_DB_POOL_MAX=8
PIPELINE_DB_MAX_RETRIES=10
PIPELINE_DB_BASE_RETRY_DELAY_MS=1000
PIPELINE_DB_READY_MAX_ATTEMPTS=60

PIPELINE_DB_IMPORT_SYNC_COMMIT_OFF=true
PIPELINE_DB_IMPORT_WORK_MEM_MB=64
PIPELINE_DB_IMPORT_MAINTENANCE_WORK_MEM_MB=1024
PIPELINE_DB_IMPORT_TEMP_BUFFERS_MB=64
PIPELINE_DB_IMPORT_LOCK_TIMEOUT_MS=15000
PIPELINE_DB_IMPORT_STATEMENT_TIMEOUT_MS=0

PIPELINE_WATCHDOG_INTERVAL_MS=30000
PIPELINE_WATCHDOG_TIMEOUT_MS=180000
PIPELINE_CHECKPOINT_EVERY_LINES=5000
```

## 4) Fluxo recomendado de importacao

1. Derrubar indices secundarios (mantendo integridade essencial):

```bash
npm run db:import:drop-indexes
```

2. Rodar pipeline:

```bash
npm run pipeline
```

3. Recriar indices secundarios e atualizar estatisticas:

```bash
npm run db:import:create-indexes
npm run db:import:analyze
```

## 5) Monitoramento operacional

Status do pipeline (Node):
- arquivo `.pipeline-status.json`
- campos: `processedPerSec`, `insertedPerSec`, `lastBatchMs`, `lastBatchThroughput`, `nodeMemoryMB`

Status do PostgreSQL:

```bash
npm run db:metrics
docker stats extrator-postgres
```

Logs de banco e pipeline:

```bash
docker compose logs -f postgres
npm run pipeline:pm2:logs
```

## 6) Escalabilidade segura

- Comece com `PIPELINE_DB_MAX_CONCURRENT_COPY=1`.
- Aumente para `2` apenas se:
  - CPU do Postgres < 85% sustentado,
  - sem reinicios OOM,
  - sem aumento forte de lock contention/WAL pressure.
- Se houver instabilidade: reduza concorrencia, reduza batch max, aumente timeout do watchdog.
