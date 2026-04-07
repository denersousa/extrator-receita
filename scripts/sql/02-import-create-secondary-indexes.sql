-- Recria indices secundarios apos finalizar a importacao massiva.
-- CREATE INDEX CONCURRENTLY minimiza bloqueios de escrita/leitura.

CREATE INDEX CONCURRENTLY IF NOT EXISTS "empresa_razaoSocial_idx"
  ON receita.empresa ("razaoSocial");

CREATE INDEX CONCURRENTLY IF NOT EXISTS "empresa_naturezaJuridica_idx"
  ON receita.empresa ("naturezaJuridica");

CREATE INDEX CONCURRENTLY IF NOT EXISTS "estabelecimento_cnpjBase_idx"
  ON receita.estabelecimento ("cnpjBase");

CREATE INDEX CONCURRENTLY IF NOT EXISTS "estabelecimento_cnaePrincipal_idx"
  ON receita.estabelecimento ("cnaePrincipal");

CREATE INDEX CONCURRENTLY IF NOT EXISTS "estabelecimento_uf_idx"
  ON receita.estabelecimento ("uf");

CREATE INDEX CONCURRENTLY IF NOT EXISTS "estabelecimento_municipio_idx"
  ON receita.estabelecimento ("municipio");
