function normalizeField(value) {
    if (value === undefined || value === null) {
        return null;
    }

    const trimmed = String(value).trim().replace(/^"|"$/g, '');
    // PostgreSQL rejeita byte nulo (0x00) em colunas text durante COPY.
    const sanitized = trimmed.replace(/\u0000/g, '');
    return sanitized === '' ? null : sanitized;
}

function parseCapitalSocial(value) {
    const normalized = normalizeField(value);
    if (normalized === null) {
        return null;
    }

    const asFloat = Number.parseFloat(normalized.replace(',', '.'));
    return Number.isNaN(asFloat) ? null : asFloat;
}

function montarCnpjCompleto(cnpjBase, cnpjOrdem, cnpjDv) {
    if (!cnpjBase || !cnpjOrdem || !cnpjDv) {
        return null;
    }

    return `${cnpjBase.padStart(8, '0')}${cnpjOrdem.padStart(4, '0')}${cnpjDv.padStart(2, '0')}`;
}

function parseEmpresa(line) {
    const columns = line.split(';');

    const row = {
        cnpjBase: normalizeField(columns[0]),
        razaoSocial: normalizeField(columns[1]),
        naturezaJuridica: normalizeField(columns[2]),
        qualificacaoResponsavel: normalizeField(columns[3]),
        capitalSocial: parseCapitalSocial(columns[4]),
        porte: normalizeField(columns[5]),
        enteFederativo: normalizeField(columns[6]),
    };

    if (!row.cnpjBase) {
        return null;
    }

    return row;
}

function parseEstabelecimento(line) {
    const columns = line.split(';');

    const cnpjBase = normalizeField(columns[0]);
    const cnpjOrdem = normalizeField(columns[1]);
    const cnpjDv = normalizeField(columns[2]);

    const row = {
        cnpjBase,
        cnpjOrdem,
        cnpjDv,
        cnpjCompleto: montarCnpjCompleto(cnpjBase, cnpjOrdem, cnpjDv),
        matrizFilial: normalizeField(columns[3]),
        nomeFantasia: normalizeField(columns[4]),
        situacaoCadastral: normalizeField(columns[5]),
        dataSituacao: normalizeField(columns[6]),
        motivoSituacao: normalizeField(columns[7]),
        nomeCidadeExterior: normalizeField(columns[8]),
        pais: normalizeField(columns[9]),
        dataInicioAtividade: normalizeField(columns[10]),
        cnaePrincipal: normalizeField(columns[11]),
        cnaeSecundarios: normalizeField(columns[12]),
        tipoLogradouro: normalizeField(columns[13]),
        logradouro: normalizeField(columns[14]),
        numero: normalizeField(columns[15]),
        complemento: normalizeField(columns[16]),
        bairro: normalizeField(columns[17]),
        cep: normalizeField(columns[18]),
        uf: normalizeField(columns[19]),
        municipio: normalizeField(columns[20]),
        ddd1: normalizeField(columns[21]),
        telefone1: normalizeField(columns[22]),
        ddd2: normalizeField(columns[23]),
        telefone2: normalizeField(columns[24]),
        dddFax: normalizeField(columns[25]),
        fax: normalizeField(columns[26]),
        email: normalizeField(columns[27]),
        situacaoEspecial: normalizeField(columns[28]),
        dataSituacaoEspecial: normalizeField(columns[29]),
    };

    if (!row.cnpjBase || !row.cnpjOrdem || !row.cnpjDv || !row.cnpjCompleto) {
        return null;
    }

    if (!row.matrizFilial || !row.situacaoCadastral || !row.cnaePrincipal) {
        return null;
    }

    return row;
}

module.exports = {
    parseEmpresa,
    parseEstabelecimento,
};
