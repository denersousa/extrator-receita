-- CreateTable
CREATE TABLE "Empresa" (
    "id" SERIAL NOT NULL,
    "cnpjBase" TEXT NOT NULL,
    "razaoSocial" TEXT,
    "naturezaJuridica" TEXT,
    "qualificacaoResponsavel" TEXT,
    "capitalSocial" DECIMAL(65,30),
    "porte" TEXT,
    "enteFederativo" TEXT,

    CONSTRAINT "Empresa_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "Estabelecimento" (
    "id" SERIAL NOT NULL,
    "cnpjBase" TEXT NOT NULL,
    "cnpjOrdem" TEXT NOT NULL,
    "cnpjDv" TEXT NOT NULL,
    "cnpjCompleto" TEXT NOT NULL,
    "matrizFilial" TEXT NOT NULL,
    "nomeFantasia" TEXT,
    "situacaoCadastral" TEXT NOT NULL,
    "dataSituacao" TEXT,
    "motivoSituacao" TEXT,
    "nomeCidadeExterior" TEXT,
    "pais" TEXT,
    "dataInicioAtividade" TEXT,
    "cnaePrincipal" TEXT NOT NULL,
    "cnaeSecundarios" TEXT,
    "tipoLogradouro" TEXT,
    "logradouro" TEXT,
    "numero" TEXT,
    "complemento" TEXT,
    "bairro" TEXT,
    "cep" TEXT,
    "uf" TEXT,
    "municipio" TEXT,
    "ddd1" TEXT,
    "telefone1" TEXT,
    "ddd2" TEXT,
    "telefone2" TEXT,
    "dddFax" TEXT,
    "fax" TEXT,
    "email" TEXT,
    "situacaoEspecial" TEXT,
    "dataSituacaoEspecial" TEXT,

    CONSTRAINT "Estabelecimento_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Empresa_cnpjBase_key" ON "Empresa"("cnpjBase");

-- CreateIndex
CREATE INDEX "Empresa_razaoSocial_idx" ON "Empresa"("razaoSocial");

-- CreateIndex
CREATE INDEX "Empresa_naturezaJuridica_idx" ON "Empresa"("naturezaJuridica");

-- CreateIndex
CREATE UNIQUE INDEX "Estabelecimento_cnpjCompleto_key" ON "Estabelecimento"("cnpjCompleto");

-- CreateIndex
CREATE INDEX "Estabelecimento_cnpjBase_idx" ON "Estabelecimento"("cnpjBase");

-- CreateIndex
CREATE INDEX "Estabelecimento_cnaePrincipal_idx" ON "Estabelecimento"("cnaePrincipal");

-- CreateIndex
CREATE INDEX "Estabelecimento_uf_idx" ON "Estabelecimento"("uf");

-- CreateIndex
CREATE INDEX "Estabelecimento_municipio_idx" ON "Estabelecimento"("municipio");

-- AddForeignKey
ALTER TABLE "Estabelecimento" ADD CONSTRAINT "Estabelecimento_cnpjBase_fkey" FOREIGN KEY ("cnpjBase") REFERENCES "Empresa"("cnpjBase") ON DELETE RESTRICT ON UPDATE CASCADE;
