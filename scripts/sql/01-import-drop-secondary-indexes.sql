-- Use este script antes da importacao massiva para reduzir custo de escrita.
-- Mantemos PK/UNIQUE/FK para integridade. Derrubamos apenas indices secundarios.

BEGIN;

DROP INDEX IF EXISTS receita."empresa_razaoSocial_idx";
DROP INDEX IF EXISTS receita."empresa_naturezaJuridica_idx";
DROP INDEX IF EXISTS receita."estabelecimento_cnpjBase_idx";
DROP INDEX IF EXISTS receita."estabelecimento_cnaePrincipal_idx";
DROP INDEX IF EXISTS receita."estabelecimento_uf_idx";
DROP INDEX IF EXISTS receita."estabelecimento_municipio_idx";

COMMIT;
