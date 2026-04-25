function doPost(e) {
  try {
    const body = JSON.parse(((e || {}).postData || {}).contents || "{}");
    const action = String(body.action || "").trim();

    Logger.log("WEBAPP doPost action = " + action);
    Logger.log("WEBAPP doPost body = " + JSON.stringify(body));

    if (action === "processar_recebimentos_v1") {
      const result = processarRecebimentosV1_(body);
      return jsonOutput_(result);
    }

    if (action === "processar_pagamentos_v1") {
      const result = processarPagamentosV1_(body);
      return jsonOutput_(result);
    }

    if (action === "processar_compras_v1") {
      const result = processarComprasV1_(body);
      return jsonOutput_(result);
    }

    if (action === "associar_pendencia_recebimentos") {
      const result = associarPendenciaRecebimentos_(body);
      return jsonOutput_(result);
    }

    if (action === "associar_pendencia_pagamentos") {
      const result = associarPendenciaPagamentos_(body);
      return jsonOutput_(result);
    }

    if (action === "confirmar_lote_recebimentos") {
      const result = confirmarLoteRecebimentos_(body);
      return jsonOutput_(result);
    }

    if (action === "confirmar_lote_pagamentos") {
      const result = confirmarLotePagamentos_(body);
      return jsonOutput_(result);
    }

    if (action === "confirmar_lote_compras") {
      const result = confirmarLoteCompras_(body);
      return jsonOutput_(result);
    }

    if (action === "listar_clientes_oficiais_recebimentos") {
      const result = listarClientesOficiaisRecebimentos_(body);
      return jsonOutput_(result);
    }

    if (action === "listar_planos_pagamentos") {
      const result = listarPlanosPagamentos_(body);
      return jsonOutput_(result);
    }

    if (action === "resolver_cliente_oficial_recebimentos") {
      const result = resolverClienteOficialRecebimentos_(body);
      return jsonOutput_(result);
    }

    if (action === "resolver_plano_pagamentos") {
      const result = resolverPlanoPagamentos_(body);
      return jsonOutput_(result);
    }

    if (action === "resolver_produto_compra") {
      const result = resolverProdutoCompraAction_(body);
      return jsonOutput_(result);
    }

    if (action === "garantir_mapa_pagamentos") {
      const result = garantirInfraPagamentos_();
      return jsonOutput_(result);
    }

    if (action === "garantir_mapa_compras") {
      const result = garantirInfraCompras_();
      return jsonOutput_(result);
    }

    return jsonOutput_({
      ok: false,
      message: "Ação inválida."
    });

  } catch (err) {
    Logger.log("WEBAPP erro = " + err);
    return jsonOutput_({
      ok: false,
      message: "Erro no Apps Script: " + err
    });
  }
}

function processarRecebimentosV1_(payload) {
  const origem = String(payload.origem || "").toLowerCase().trim();
  const periodo = payload.periodo || {};
  const telegram = payload.telegram || {};
  const meta = payload.message_meta || {};

  Logger.log("WEBAPP processarRecebimentosV1_ origem = " + origem);
  Logger.log("WEBAPP processarRecebimentosV1_ periodo = " + JSON.stringify(periodo));
  Logger.log("WEBAPP processarRecebimentosV1_ telegram = " + JSON.stringify({
    chat_id: telegram.chat_id || null,
    has_document: !!telegram.has_document,
    document_text_length: String(telegram.document_text || "").length,
    has_document_json: !!telegram.document_json
  }));
  Logger.log("WEBAPP processarRecebimentosV1_ meta = " + JSON.stringify(meta));

  if (!origem) {
    return {
      ok: false,
      message: "Origem não informada."
    };
  }

  if (origem === "inter") {
    return processarInterV1_(periodo, telegram, meta);
  }

  if (origem === "itau") {
    return processarItauV1_(periodo, telegram, meta);
  }

  return {
    ok: false,
    message: 'Origem inválida. Use "inter" ou "itau".'
  };
}

function processarInterV1_(periodo, telegram, meta) {
  Logger.log("WEBAPP processarInterV1_ periodo = " + JSON.stringify(periodo));
  return preVisualizarInterRecebimentos_(periodo, telegram, meta);
}

function processarItauV1_(periodo, telegram, meta) {
  Logger.log("WEBAPP processarItauV1_ periodo = " + JSON.stringify(periodo));
  Logger.log("WEBAPP processarItauV1_ label = " + formatPeriodoLabel_(periodo));
  Logger.log("WEBAPP processarItauV1_ dias = " + getPeriodoDias_(periodo));
  Logger.log("WEBAPP processarItauV1_ has_document = " + !!(telegram && telegram.has_document));
  Logger.log("WEBAPP processarItauV1_ document_text_length = " + String((telegram && telegram.document_text) || "").length);
  Logger.log("WEBAPP processarItauV1_ has_document_json = " + !!(telegram && telegram.document_json));

  const resp = preVisualizarItauRecebimentos_(periodo, telegram, meta);

  Logger.log("WEBAPP processarItauV1_ resp.periodo = " + JSON.stringify(resp && resp.periodo));
  Logger.log("WEBAPP processarItauV1_ resp.ok = " + !!(resp && resp.ok));

  return resp;
}

function getPeriodoDias_(periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();
  const valor = Number(periodo && periodo.valor);

  if (tipo === "dias" && isFinite(valor) && valor > 0) {
    return valor;
  }

  const label = String((periodo && periodo.label) || "").toLowerCase().trim();
  const m = label.match(/(\d+)/);
  if (m && Number(m[1]) > 0) {
    return Number(m[1]);
  }

  return 1;
}

function formatPeriodoLabel_(periodo) {
  const label = String((periodo && periodo.label) || "").trim();
  if (label) return label;

  const tipo = String((periodo && periodo.tipo) || "").trim();
  const valor = Number(periodo && periodo.valor);

  if (tipo === "dias" && isFinite(valor) && valor > 0) {
    if (valor === 1) return "hoje";
    return "últimos " + valor + " dias";
  }

  if (
    tipo === "datas_especificas" &&
    Array.isArray(periodo && periodo.datas) &&
    periodo.datas.length
  ) {
    return periodo.datas.join(", ");
  }

  return "hoje";
}

function jsonOutput_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
