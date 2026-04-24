/************************************************************
 * INTER REVIEW / RECEBIMENTOS REVIEW
 * - pré-visualização do lote
 * - associação manual
 * - confirmação do lote
 * - suporte a INTER e ITAÚ
 * - 3 camadas:
 *   1) LOG_PIX
 *   2) COMPROVANTES_CLIENTES
 *   3) GC
 * - com tratamento de ambiguo_no_gc
 * - com suporte a force_duplicate
 ************************************************************/

/**
 * =========================================================
 * PRÉ-VISUALIZAÇÃO INTER
 * =========================================================
 */
function preVisualizarInterRecebimentos_(periodo, telegram, meta) {
  const mensagens = listarMensagensInterPorPeriodo_(periodo);

  const itens_prontos = [];
  const pendencias_associacao = [];
  const ignorados = [];
  const duplicados = [];
  const ja_processados = [];

  let seqPronto = 1;
  let seqPend = 1;

  if (!mensagens.length) {
    return {
      ok: true,
      modo: "pre_visualizacao",
      origem: "inter",
      periodo: normalizarPeriodoRetornoInter_(periodo),
      itens_prontos: [],
      pendencias_associacao: [],
      ignorados: [],
      duplicados: [],
      ja_processados: [],
      message: "Nenhum e-mail do Inter encontrado para o período informado."
    };
  }

  for (const item of mensagens) {
    const msg = item.message;

    try {
      if (jaProcessouMensagemInter_(msg)) {
        ja_processados.push({
          message_id: String(msg.getId() || ""),
          assunto_email: safeSubjectInter_(msg),
          remetente: msg.getFrom()
        });
        continue;
      }

      const assunto = safeSubjectInter_(msg);
      const corpo = limparTextoInter_(msg.getPlainBody() || "");

      const assuntoNorm = gcNormInter_(assunto);
      const corpoNorm = gcNormInter_(corpo);

      const parecePixRecebido =
        assuntoNorm.includes("pagamento pix recebido") ||
        corpoNorm.includes("voce recebeu um pix");

      if (!parecePixRecebido) {
        ignorados.push({
          motivo: "nao_parece_pix",
          nome_extraido: "",
          data_pagamento: Utilities.formatDate(
            msg.getDate(),
            Session.getScriptTimeZone(),
            "yyyy-MM-dd"
          ),
          valor: null,
          assunto_email: assunto
        });
        continue;
      }

      const extraido = analisarEmailPixInterComOpenAI_(msg);

      if (!extraido || !extraido.eh_pix) {
        ignorados.push({
          motivo: "nao_eh_pix",
          nome_extraido: "",
          data_pagamento: Utilities.formatDate(
            msg.getDate(),
            Session.getScriptTimeZone(),
            "yyyy-MM-dd"
          ),
          valor: null,
          assunto_email: assunto
        });
        continue;
      }

      if (!extraido.tipo_pix || gcNormInter_(extraido.tipo_pix) !== "recebido") {
        ignorados.push({
          motivo: "nao_eh_pix_recebido",
          nome_extraido: extraido.nome_pagador || "",
          data_pagamento: Utilities.formatDate(
            msg.getDate(),
            Session.getScriptTimeZone(),
            "yyyy-MM-dd"
          ),
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          assunto_email: assunto
        });
        continue;
      }

      extraido.data_pagamento = Utilities.formatDate(
        msg.getDate(),
        Session.getScriptTimeZone(),
        "yyyy-MM-dd"
      );

      extraido.nome_pagador = limparNomeExtraidoInter_(extraido.nome_pagador || "");

      if (deveIgnorarPagadorInter_(extraido.nome_pagador || "")) {
        ignorados.push({
          motivo: "transferencia_interna",
          nome_extraido: extraido.nome_pagador || "",
          data_pagamento: extraido.data_pagamento,
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          assunto_email: assunto
        });
        continue;
      }

      const chaveDuplicidade = montarChaveDuplicidadeInter_(extraido, msg);

      if (existeRegistroDriveInter_(chaveDuplicidade.fileName)) {
        duplicados.push({
          motivo: "duplicado_drive_log",
          nome_extraido: extraido.nome_pagador || "",
          data_pagamento: extraido.data_pagamento,
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          assunto_email: assunto,
          message_id: String(msg.getId() || ""),
          remetente: msg.getFrom(),
          id_transacao: extraido.id_transacao || "",
          banco_extraido: extraido.banco_email || ""
        });
        continue;
      }

      const clienteInfo = resolverClienteOficialInter_(extraido.nome_pagador || "");

      if (!clienteInfo || !clienteInfo.cliente) {
        pendencias_associacao.push({
          id_local: "P" + seqPend++,
          nome_extraido: extraido.nome_pagador || "",
          data_pagamento: extraido.data_pagamento,
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          forma: "PIX",
          conta_oficial: INTER_CONTA_OFICIAL,
          banco_extraido: extraido.banco_email || "",
          id_transacao: extraido.id_transacao || "",
          assunto_email: assunto,
          remetente: msg.getFrom(),
          message_id: String(msg.getId() || ""),
          status: "pendente_associacao"
        });
        continue;
      }

      const dupGC = existeDuplicataGCInter_(clienteInfo.cliente, extraido);

      if (dupGC.ok && dupGC.duplicado) {
        duplicados.push({
          motivo: "duplicado_gc",
          nome_extraido: extraido.nome_pagador || "",
          cliente_oficial: clienteInfo.cliente,
          data_pagamento: extraido.data_pagamento,
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          assunto_email: assunto,
          remetente: msg.getFrom(),
          message_id: String(msg.getId() || ""),
          id_transacao: extraido.id_transacao || "",
          banco_extraido: extraido.banco_email || "",
          gc_recebimento_id: dupGC.recebimento_id || null,
          gc_recebimento_codigo: dupGC.recebimento_codigo || null
        });
        continue;
      }

      if (dupGC.ok && dupGC.ambiguo) {
        pendencias_associacao.push({
          id_local: "P" + seqPend++,
          nome_extraido: extraido.nome_pagador || "",
          cliente_sugerido: clienteInfo.cliente,
          data_pagamento: extraido.data_pagamento,
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          forma: "PIX",
          conta_oficial: INTER_CONTA_OFICIAL,
          banco_extraido: extraido.banco_email || "",
          id_transacao: extraido.id_transacao || "",
          assunto_email: assunto,
          remetente: msg.getFrom(),
          message_id: String(msg.getId() || ""),
          status: "pendente_gc_ambiguo",
          erro:
            "Ambiguidade no GC: múltiplos recebimentos já existentes com mesma data e valor para a cliente sugerida.",
          gc_ambiguo: true,
          gc_matches: dupGC.matches || []
        });
        continue;
      }

      itens_prontos.push({
        id_local: "I" + seqPronto++,
        cliente_oficial: clienteInfo.cliente,
        nome_extraido: extraido.nome_pagador || "",
        data_pagamento: extraido.data_pagamento,
        valor: extraido.valor != null ? Number(extraido.valor) : null,
        forma: "PIX",
        conta_oficial: INTER_CONTA_OFICIAL,
        banco_extraido: extraido.banco_email || "",
        id_transacao: extraido.id_transacao || "",
        assunto_email: assunto,
        remetente: msg.getFrom(),
        message_id: String(msg.getId() || ""),
        status: "pronto"
      });

    } catch (e) {
      pendencias_associacao.push({
        id_local: "P" + seqPend++,
        nome_extraido: "",
        data_pagamento: "",
        valor: null,
        forma: "PIX",
        conta_oficial: INTER_CONTA_OFICIAL,
        banco_extraido: "",
        id_transacao: "",
        assunto_email: safeSubjectInter_(msg),
        remetente: msg.getFrom(),
        message_id: String(msg.getId() || ""),
        status: "pendente_associacao",
        erro: String(e)
      });
    }
  }

  return {
    ok: true,
    modo: "pre_visualizacao",
    origem: "inter",
    periodo: normalizarPeriodoRetornoInter_(periodo),
    itens_prontos: itens_prontos,
    pendencias_associacao: pendencias_associacao,
    ignorados: ignorados,
    duplicados: duplicados,
    ja_processados: ja_processados,
    message: "Pré-visualização do lote de recebimentos gerada com sucesso."
  };
}

/**
 * =========================================================
 * DISPATCHERS POR ORIGEM
 * =========================================================
 */
function associarPendenciaRecebimentos_(payload) {
  const origem = String(payload.origem || "").toLowerCase().trim();

  if (origem === "inter") {
    return associarPendenciaRecebimentosInter_(payload);
  }

  if (origem === "itau") {
    return associarPendenciaRecebimentosItau_(payload);
  }

  return {
    ok: false,
    message: 'Origem inválida para associação. Use "inter" ou "itau".'
  };
}

function confirmarLoteRecebimentos_(payload) {
  const origem = String(payload.origem || "").toLowerCase().trim();

  if (origem === "inter") {
    return confirmarLoteRecebimentosInter_(payload);
  }

  if (origem === "itau") {
    return confirmarLoteRecebimentosItau_(payload);
  }

  return {
    ok: false,
    message: 'Origem inválida para confirmação. Use "inter" ou "itau".'
  };
}

function listarClientesOficiaisRecebimentos_(payload) {
  const origem = String((payload && payload.origem) || "inter").toLowerCase().trim();

  if (origem === "inter") {
    return listarClientesOficiaisRecebimentosInter_(payload);
  }

  if (origem === "itau") {
    return listarClientesOficiaisRecebimentosItau_(payload);
  }

  return {
    ok: false,
    message: 'Origem inválida para listagem. Use "inter" ou "itau".'
  };
}

function resolverClienteOficialRecebimentos_(payload) {
  const origem = String((payload && payload.origem) || "inter").toLowerCase().trim();

  if (origem === "inter") {
    return resolverClienteOficialRecebimentosInter_(payload);
  }

  if (origem === "itau") {
    return resolverClienteOficialRecebimentosItau_(payload);
  }

  return {
    ok: false,
    message: 'Origem inválida para resolução. Use "inter" ou "itau".'
  };
}

/**
 * =========================================================
 * ASSOCIAÇÃO MANUAL - INTER
 * =========================================================
 */
function associarPendenciaRecebimentosInter_(payload) {
  const nomeExtraido = String(payload.nome_extraido || "").trim();
  const clienteOficial = String(payload.cliente_oficial || "").trim();

  if (!nomeExtraido || !clienteOficial) {
    return {
      ok: false,
      message: "Nome extraído e cliente oficial são obrigatórios."
    };
  }

  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(INTER_ABA_MAPA_CLIENTES);

  if (!sh) {
    return {
      ok: false,
      message: 'Aba "' + INTER_ABA_MAPA_CLIENTES + '" não encontrada.'
    };
  }

  const rowInfo = encontrarLinhaClienteOficialInter_(sh, clienteOficial);
  if (!rowInfo) {
    return {
      ok: false,
      message: 'Cliente oficial não encontrado no MAPA_CLIENTES: "' + clienteOficial + '"'
    };
  }

  const colVazia = encontrarPrimeiraColunaNomeBaseVaziaInter_(sh, rowInfo.row);
  if (!colVazia) {
    return {
      ok: false,
      message: 'Não encontrei nome_base vazio para "' + clienteOficial + '"'
    };
  }

  sh.getRange(rowInfo.row, colVazia).setValue(nomeExtraido);

  return {
    ok: true,
    message: 'Associação salva: ' + nomeExtraido + " -> " + clienteOficial
  };
}

/**
 * =========================================================
 * ASSOCIAÇÃO MANUAL - ITAÚ
 * =========================================================
 */
function associarPendenciaRecebimentosItau_(payload) {
  const nomeExtraido = String(payload.nome_extraido || "").trim();
  const clienteOficial = String(payload.cliente_oficial || "").trim();

  if (!nomeExtraido || !clienteOficial) {
    return {
      ok: false,
      message: "Nome extraído e cliente oficial são obrigatórios."
    };
  }

  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(INTER_ABA_MAPA_CLIENTES);

  if (!sh) {
    return {
      ok: false,
      message: 'Aba "' + INTER_ABA_MAPA_CLIENTES + '" não encontrada.'
    };
  }

  const rowInfo = encontrarLinhaClienteOficialInter_(sh, clienteOficial);
  if (!rowInfo) {
    return {
      ok: false,
      message: 'Cliente oficial não encontrado no MAPA_CLIENTES: "' + clienteOficial + '"'
    };
  }

  const colVazia = encontrarPrimeiraColunaNomeBaseVaziaInter_(sh, rowInfo.row);
  if (!colVazia) {
    return {
      ok: false,
      message: 'Não encontrei nome_base vazio para "' + clienteOficial + '"'
    };
  }

  sh.getRange(rowInfo.row, colVazia).setValue(nomeExtraido);

  return {
    ok: true,
    message: 'Associação salva (Itaú): ' + nomeExtraido + " -> " + clienteOficial
  };
}

/**
 * =========================================================
 * CONFIRMAÇÃO DO LOTE - INTER
 * =========================================================
 */
function confirmarLoteRecebimentosInter_(payload) {
  const itens = Array.isArray(payload.itens) ? payload.itens : [];

  if (!itens.length) {
    return {
      ok: false,
      message: "Nenhum item informado para confirmação."
    };
  }

  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(INTER_ABA_CENTRAL);
  if (!sh) {
    return {
      ok: false,
      message: 'Aba "' + INTER_ABA_CENTRAL + '" não encontrada.'
    };
  }

  const resultados = [];
  let processados = 0;
  let duplicadosLog = 0;
  let duplicadosGC = 0;
  let ambiguosGC = 0;
  let confirmadosGC = 0;
  let forcados = 0;
  let erros = 0;

  for (const item of itens) {
    try {
      const extraido = {
        nome_pagador: item.nome_extraido || "",
        data_pagamento: item.data_pagamento || "",
        valor: item.valor != null ? Number(item.valor) : null,
        id_transacao: item.id_transacao || "",
        banco_email: item.banco_extraido || "",
        tipo_pix: "recebido",
        eh_pix: true
      };

      const fakeMsg = {
        getId: function() {
          return String(item.message_id || "");
        },
        getDate: function() {
          const iso = String(item.data_pagamento || "") + "T12:00:00";
          return new Date(iso);
        }
      };

      const isForced = !!item.force_duplicate;
      const chaveDuplicidade = montarChaveDuplicidadeInter_(extraido, fakeMsg);

      if (!isForced && existeRegistroDriveInter_(chaveDuplicidade.fileName)) {
        duplicadosLog += 1;
        resultados.push({
          ok: false,
          tipo: "duplicado_log",
          cliente_oficial: item.cliente_oficial || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Duplicado no LOG_PIX/Drive."
        });
        continue;
      }

      const dupGC = existeDuplicataGCInter_(item.cliente_oficial || "", extraido);

      if (!isForced && dupGC.ok && dupGC.ambiguo) {
        ambiguosGC += 1;
        resultados.push({
          ok: false,
          tipo: "ambiguo_gc",
          cliente_oficial: item.cliente_oficial || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Ambiguidade no GC: múltiplos recebimentos iguais para a mesma cliente/data/valor.",
          matches: dupGC.matches || []
        });
        continue;
      }

      if (!isForced && dupGC.ok && dupGC.duplicado) {
        duplicadosGC += 1;
        resultados.push({
          ok: false,
          tipo: "duplicado_gc",
          cliente_oficial: item.cliente_oficial || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Duplicado no GC.",
          gc_recebimento_id: dupGC.recebimento_id || null,
          gc_recebimento_codigo: dupGC.recebimento_codigo || null
        });
        continue;
      }

      const linhaDestino = getPrimeiraLinhaLivreInter_(sh);
      if (!linhaDestino) {
        erros += 1;
        resultados.push({
          ok: false,
          tipo: "erro_sem_linha",
          cliente_oficial: item.cliente_oficial || "",
          valor: item.valor || null,
          erro: "Sem linha livre para continuar."
        });
        continue;
      }

      const idComprovante = gerarIdComprovanteManualInter_(item);

      preencherLinhaCapturaRecebimentosGenerica_(sh, linhaDestino, {
        conta_oficial: item.conta_oficial || INTER_CONTA_OFICIAL,
        cliente_oficial: item.cliente_oficial,
        data_pagamento: item.data_pagamento || "",
        valor: item.valor,
        forma: item.forma || "PIX",
        nome_extraido: item.nome_extraido || "",
        assunto_email: item.assunto_email || "",
        remetente: item.remetente || "",
        id_transacao: item.id_transacao || "",
        id_comprovante: idComprovante,
        motivo_cliente: isForced
          ? "duplicata liberada manualmente via Telegram"
          : "confirmado via Telegram",
        score_cliente: "",
        banco_extraido: item.banco_extraido || ""
      });

      const registroJson = {
        origem: "inter",
        banco: "Inter Empresas",
        cliente_oficial: item.cliente_oficial || "",
        nome_extraido: item.nome_extraido || "",
        data: item.data_pagamento || "",
        valor: item.valor != null ? Number(item.valor) : null,
        tipo: "recebido",
        id_transacao: item.id_transacao || null,
        id_comprovante: idComprovante,
        subject: item.assunto_email || "",
        from: item.remetente || "",
        message_id: String(item.message_id || ""),
        processado_em: new Date().toISOString(),
        tipo_registro: "confirmacao_lote_telegram",
        status: isForced ? "processado_forcado" : "processado",
        duplicata_liberada_por_usuario: isForced,
        duplicate_source: item.duplicate_source || null,
        duplicate_reference: item.duplicate_reference || null
      };

      gravarRegistroInterNoDrive_(chaveDuplicidade.fileName, registroJson);

      if (item.message_id) {
        gravarRegistroInterNoDrive_(
          "INTER_MSG_ONLY__" + sanitizeFileNameInter_(String(item.message_id || "")) + ".json",
          {
            tipo_registro: "message_id",
            status: isForced ? "processado_forcado" : "processado",
            duplicata_liberada_por_usuario: isForced,
            id_comprovante: idComprovante,
            message_id: String(item.message_id || ""),
            subject: item.assunto_email || "",
            from: item.remetente || "",
            id_transacao: item.id_transacao || null,
            tipo_pix: "recebido",
            data_pagamento: item.data_pagamento || "",
            valor: item.valor != null ? Number(item.valor) : null,
            nome_pagador: item.nome_extraido || "",
            cliente_oficial: item.cliente_oficial || "",
            processado_em: new Date().toISOString()
          }
        );
      }

      salvarJsonClienteInter_(
        item.cliente_oficial || "",
        {
          data_pagamento: item.data_pagamento || "",
          valor: item.valor != null ? Number(item.valor) : null,
          nome_pagador: item.nome_extraido || "",
          id_transacao: item.id_transacao || "",
          banco_email: item.banco_extraido || ""
        },
        {
          getSubject: function() {
            return item.assunto_email || "";
          }
        },
        idComprovante,
        registroJson
      );

      marcarMensagemInterComoProcessadaSePossivel_(item.message_id);

      const validacaoGC = existeDuplicataGCInter_(item.cliente_oficial || "", extraido);
      const entrouNoGC = !!(validacaoGC.ok && validacaoGC.duplicado);

      if (entrouNoGC) confirmadosGC += 1;
      if (isForced) forcados += 1;
      processados += 1;

      resultados.push({
        ok: true,
        tipo: isForced ? "processado_forcado" : "processado",
        cliente_oficial: item.cliente_oficial || "",
        data_pagamento: item.data_pagamento || "",
        valor: item.valor || null,
        id_comprovante: idComprovante,
        confirmado_no_gc: entrouNoGC,
        force_duplicate: isForced
      });

    } catch (e) {
      erros += 1;
      resultados.push({
        ok: false,
        tipo: "erro",
        cliente_oficial: item.cliente_oficial || "",
        valor: item.valor || null,
        erro: String(e)
      });
    }
  }

  return montarResumoConfirmacaoRecebimentos_(
    "inter",
    resultados,
    processados,
    duplicadosLog,
    duplicadosGC,
    ambiguosGC,
    forcados,
    confirmadosGC,
    erros
  );
}

/**
 * =========================================================
 * CONFIRMAÇÃO DO LOTE - ITAÚ
 * =========================================================
 */
function confirmarLoteRecebimentosItau_(payload) {
  const itens = Array.isArray(payload.itens) ? payload.itens : [];

  if (!itens.length) {
    return {
      ok: false,
      message: "Nenhum item informado para confirmação."
    };
  }

  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(INTER_ABA_CENTRAL);
  if (!sh) {
    return {
      ok: false,
      message: 'Aba "' + INTER_ABA_CENTRAL + '" não encontrada.'
    };
  }

  const resultados = [];
  let processados = 0;
  let duplicadosLog = 0;
  let duplicadosGC = 0;
  let ambiguosGC = 0;
  let confirmadosGC = 0;
  let forcados = 0;
  let erros = 0;

  for (const item of itens) {
    try {
      const lanc = {
        nome_pagador: item.nome_extraido || "",
        data_pagamento: item.data_pagamento || "",
        valor: item.valor != null ? Number(item.valor) : null,
        cpf_cnpj: item.cpf_cnpj || "",
        linha_resumo: item.linha_origem || ""
      };

      const isForced = !!item.force_duplicate;
      const chaveDuplicidade = montarChaveDuplicidadeItau_(lanc);

      if (!isForced && existeRegistroDriveItau_(chaveDuplicidade.fileName)) {
        duplicadosLog += 1;
        resultados.push({
          ok: false,
          tipo: "duplicado_log",
          cliente_oficial: item.cliente_oficial || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Duplicado no LOG_PIX/Drive."
        });
        continue;
      }

      const dupGC = existeDuplicataGCItau_(item.cliente_oficial || "", lanc);

      if (!isForced && dupGC.ok && dupGC.ambiguo) {
        ambiguosGC += 1;
        resultados.push({
          ok: false,
          tipo: "ambiguo_gc",
          cliente_oficial: item.cliente_oficial || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Ambiguidade no GC: múltiplos recebimentos iguais para a mesma cliente/data/valor.",
          matches: dupGC.matches || []
        });
        continue;
      }

      if (!isForced && dupGC.ok && dupGC.duplicado) {
        duplicadosGC += 1;
        resultados.push({
          ok: false,
          tipo: "duplicado_gc",
          cliente_oficial: item.cliente_oficial || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Duplicado no GC.",
          gc_recebimento_id: dupGC.recebimento_id || null,
          gc_recebimento_codigo: dupGC.recebimento_codigo || null
        });
        continue;
      }

      const linhaDestino = getPrimeiraLinhaLivreInter_(sh);
      if (!linhaDestino) {
        erros += 1;
        resultados.push({
          ok: false,
          tipo: "erro_sem_linha",
          cliente_oficial: item.cliente_oficial || "",
          valor: item.valor || null,
          erro: "Sem linha livre para continuar."
        });
        continue;
      }

      const idComprovante = gerarIdComprovanteManualItau_(item);

      preencherLinhaCapturaRecebimentosGenerica_(sh, linhaDestino, {
        conta_oficial: item.conta_oficial || ITAU_CONTA_OFICIAL,
        cliente_oficial: item.cliente_oficial,
        data_pagamento: item.data_pagamento || "",
        valor: item.valor,
        forma: item.forma || "PIX",
        nome_extraido: item.nome_extraido || "",
        assunto_email: item.assunto_email || "Extrato Itaú PDF",
        remetente: item.remetente || "Telegram PDF",
        id_transacao: item.id_transacao || "",
        id_comprovante: idComprovante,
        motivo_cliente: isForced
          ? "duplicata liberada manualmente via Telegram"
          : "confirmado via Telegram",
        score_cliente: "",
        banco_extraido: item.banco_extraido || "Itaú Empresas"
      });

      const registroJson = {
        origem: "itau",
        banco: "Itaú Empresas",
        cliente_oficial: item.cliente_oficial || "",
        nome_extraido: item.nome_extraido || "",
        data: item.data_pagamento || "",
        valor: item.valor != null ? Number(item.valor) : null,
        cpf_cnpj: item.cpf_cnpj || "",
        tipo: "recebido",
        id_transacao: item.id_transacao || null,
        id_comprovante: idComprovante,
        subject: item.assunto_email || "Extrato Itaú PDF",
        from: item.remetente || "Telegram PDF",
        message_id: String(item.message_id || ""),
        linha_origem: item.linha_origem || "",
        processado_em: new Date().toISOString(),
        tipo_registro: "confirmacao_lote_telegram",
        status: isForced ? "processado_forcado" : "processado",
        duplicata_liberada_por_usuario: isForced,
        duplicate_source: item.duplicate_source || null,
        duplicate_reference: item.duplicate_reference || null
      };

      gravarRegistroItauNoDriveSafe_(chaveDuplicidade.fileName, registroJson);

      if (item.message_id) {
        gravarRegistroItauNoDriveSafe_(
          "ITAU_MSG_ONLY__" + sanitizeFileNameInter_(String(item.message_id || "")) + ".json",
          {
            tipo_registro: "message_id",
            status: isForced ? "processado_forcado" : "processado",
            duplicata_liberada_por_usuario: isForced,
            id_comprovante: idComprovante,
            message_id: String(item.message_id || ""),
            subject: item.assunto_email || "Extrato Itaú PDF",
            from: item.remetente || "Telegram PDF",
            id_transacao: item.id_transacao || null,
            tipo_pix: "recebido",
            data_pagamento: item.data_pagamento || "",
            valor: item.valor != null ? Number(item.valor) : null,
            nome_pagador: item.nome_extraido || "",
            cliente_oficial: item.cliente_oficial || "",
            processado_em: new Date().toISOString()
          }
        );
      }

      salvarJsonClienteItauSafe_(
        item.cliente_oficial || "",
        {
          data_pagamento: item.data_pagamento || "",
          valor: item.valor != null ? Number(item.valor) : null,
          nome_pagador: item.nome_extraido || "",
          cpf_cnpj: item.cpf_cnpj || "",
          id_transacao: item.id_transacao || "",
          banco_email: item.banco_extraido || "Itaú Empresas",
          linha_resumo: item.linha_origem || ""
        },
        idComprovante,
        registroJson
      );

      const validacaoGC = existeDuplicataGCItau_(item.cliente_oficial || "", lanc);
      const entrouNoGC = !!(validacaoGC.ok && validacaoGC.duplicado);

      if (entrouNoGC) confirmadosGC += 1;
      if (isForced) forcados += 1;
      processados += 1;

      resultados.push({
        ok: true,
        tipo: isForced ? "processado_forcado" : "processado",
        cliente_oficial: item.cliente_oficial || "",
        data_pagamento: item.data_pagamento || "",
        valor: item.valor || null,
        id_comprovante: idComprovante,
        confirmado_no_gc: entrouNoGC,
        force_duplicate: isForced
      });

    } catch (e) {
      erros += 1;
      resultados.push({
        ok: false,
        tipo: "erro",
        cliente_oficial: item.cliente_oficial || "",
        valor: item.valor || null,
        erro: String(e)
      });
    }
  }

  return montarResumoConfirmacaoRecebimentos_(
    "itau",
    resultados,
    processados,
    duplicadosLog,
    duplicadosGC,
    ambiguosGC,
    forcados,
    confirmadosGC,
    erros
  );
}

/**
 * =========================================================
 * CLIENTES OFICIAIS / RESOLUÇÃO FLEXÍVEL
 * =========================================================
 */
function listarClientesOficiaisRecebimentosInter_(payload) {
  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(INTER_ABA_MAPA_CLIENTES);

  if (!sh) {
    return {
      ok: false,
      message: 'Aba "' + INTER_ABA_MAPA_CLIENTES + '" não encontrada.'
    };
  }

  const lastRow = sh.getLastRow();
  if (lastRow < 2) {
    return { ok: true, clientes: [] };
  }

  const vals = sh.getRange(2, 11, lastRow - 1, 1).getDisplayValues();
  const clientes = [];
  const seen = {};

  for (let i = 0; i < vals.length; i++) {
    const nome = String(vals[i][0] || "").trim();
    const key = gcNormInter_(nome);
    if (!nome || !key) continue;
    if (seen[key]) continue;
    seen[key] = true;
    clientes.push(nome);
  }

  clientes.sort();

  return {
    ok: true,
    clientes: clientes
  };
}

function listarClientesOficiaisRecebimentosItau_(payload) {
  return listarClientesOficiaisRecebimentosInter_(payload);
}

function resolverClienteOficialRecebimentosInter_(payload) {
  const nomeFalado = String((payload && payload.nome_falado) || "").trim();

  if (!nomeFalado) {
    return {
      ok: false,
      message: "nome_falado não informado."
    };
  }

  const listaResp = listarClientesOficiaisRecebimentosInter_({ origem: "inter" });
  if (!listaResp.ok) return listaResp;

  const clientes = Array.isArray(listaResp.clientes) ? listaResp.clientes : [];
  if (!clientes.length) {
    return {
      ok: false,
      message: "Nenhum cliente oficial encontrado no MAPA_CLIENTES."
    };
  }

  const alvo = gcNormInter_(nomeFalado);

  for (let i = 0; i < clientes.length; i++) {
    if (gcNormInter_(clientes[i]) === alvo) {
      return {
        ok: true,
        encontrado: true,
        cliente_oficial: clientes[i],
        score: 1,
        motivo: "match_exato"
      };
    }
  }

  for (let j = 0; j < clientes.length; j++) {
    const cNorm = gcNormInter_(clientes[j]);
    if (cNorm.indexOf(alvo) >= 0 || alvo.indexOf(cNorm) >= 0) {
      return {
        ok: true,
        encontrado: true,
        cliente_oficial: clientes[j],
        score: 0.94,
        motivo: "match_contem"
      };
    }
  }

  let melhor = null;
  let melhorScore = 0;

  for (let k = 0; k < clientes.length; k++) {
    const cand = clientes[k];
    const score = scoreClienteOficialFaladoInter_(nomeFalado, cand);
    if (score > melhorScore) {
      melhorScore = score;
      melhor = cand;
    }
  }

  if (melhor && melhorScore >= 0.72) {
    return {
      ok: true,
      encontrado: true,
      cliente_oficial: melhor,
      score: melhorScore,
      motivo: "match_aproximado"
    };
  }

  return {
    ok: true,
    encontrado: false,
    cliente_oficial: "",
    score: melhorScore,
    motivo: "nao_encontrado"
  };
}

function resolverClienteOficialRecebimentosItau_(payload) {
  return resolverClienteOficialRecebimentosInter_(payload);
}

function scoreClienteOficialFaladoInter_(falado, candidato) {
  const a = tokensClienteOficialInter_(falado);
  const b = tokensClienteOficialInter_(candidato);

  if (!a.length || !b.length) return 0;

  let inter = 0;
  const mapa = {};
  for (let i = 0; i < b.length; i++) mapa[b[i]] = true;
  for (let j = 0; j < a.length; j++) {
    if (mapa[a[j]]) inter++;
  }

  const uniaoObj = {};
  for (let x = 0; x < a.length; x++) uniaoObj[a[x]] = true;
  for (let y = 0; y < b.length; y++) uniaoObj[b[y]] = true;

  const uniao = Object.keys(uniaoObj).length || 1;
  let score = inter / uniao;

  if (a[0] && b[0] && a[0] === b[0]) score += 0.25;
  if (gcNormInter_(falado) === gcNormInter_(candidato)) score = 1;

  return Math.min(1, score);
}

function tokensClienteOficialInter_(s) {
  return gcNormInter_(s)
    .split(/[^a-z0-9]+/)
    .map(function(x) { return x.trim(); })
    .filter(Boolean);
}

/**
 * =========================================================
 * HELPERS
 * =========================================================
 */
function normalizarPeriodoRetornoInter_(periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();

  if (tipo === "datas_especificas") {
    return {
      tipo: "datas_especificas",
      datas: Array.isArray(periodo.datas) ? periodo.datas : [],
      label: String(periodo.label || "datas específicas")
    };
  }

  return {
    tipo: "dias",
    valor: getPeriodoDias_(periodo),
    label: formatPeriodoLabel_(periodo)
  };
}

function encontrarLinhaClienteOficialInter_(sh, clienteOficial) {
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return null;

  const vals = sh.getRange(2, 11, lastRow - 1, 1).getDisplayValues();
  const alvo = gcNormInter_(clienteOficial);

  for (let i = 0; i < vals.length; i++) {
    const atual = gcNormInter_(vals[i][0] || "");
    if (atual === alvo) {
      return { row: i + 2 };
    }
  }

  return null;
}

function encontrarPrimeiraColunaNomeBaseVaziaInter_(sh, rowNumber) {
  const vals = sh.getRange(rowNumber, 1, 1, 10).getDisplayValues()[0];

  for (let i = 0; i < vals.length; i++) {
    if (!String(vals[i] || "").trim()) {
      return i + 1;
    }
  }

  return null;
}

function gerarIdComprovanteManualInter_(item) {
  const data = String(item.data_pagamento || "").replace(/-/g, "");
  const valor = Number(item.valor || 0).toFixed(2).replace(".", "");
  const msgId = String(item.message_id || "MSG").replace(/[^a-zA-Z0-9]/g, "").slice(-10) || "MSG";
  return "INTERTG_" + data + "_" + valor + "_" + msgId;
}

function gerarIdComprovanteManualItau_(item) {
  const data = String(item.data_pagamento || "").replace(/-/g, "");
  const valor = Number(item.valor || 0).toFixed(2).replace(".", "");
  const msgId = String(item.message_id || "PDF").replace(/[^a-zA-Z0-9]/g, "").slice(-10) || "PDF";
  return "ITAUTG_" + data + "_" + valor + "_" + msgId;
}

function preencherLinhaCapturaRecebimentosGenerica_(sh, c, dados) {
  if (dados.cliente_oficial) sh.getRange(c.nome).setValue(dados.cliente_oficial);
  if (dados.data_pagamento) sh.getRange(c.data).setValue(dados.data_pagamento);
  if (dados.valor != null && isFinite(Number(dados.valor))) sh.getRange(c.valor).setValue(Number(dados.valor));

  sh.getRange(c.forma).setValue(dados.forma || "PIX");
  sh.getRange(c.conta).setValue(dados.conta_oficial || "");

  const nota = [
    "Origem: " + (dados.conta_oficial || ""),
    "Assunto: " + (dados.assunto_email || ""),
    "Remetente: " + (dados.remetente || ""),
    "Nome extraído: " + (dados.nome_extraido || ""),
    "Cliente resolvido: " + (dados.cliente_oficial || "não identificado"),
    "Motivo cliente: " + (dados.motivo_cliente || ""),
    "Score cliente: " + (dados.score_cliente != null ? dados.score_cliente : ""),
    "Conta resolvida: " + (dados.conta_oficial || ""),
    "Banco extraído: " + (dados.banco_extraido || ""),
    "ID transação: " + (dados.id_transacao || ""),
    "ID comprovante: " + (dados.id_comprovante || "")
  ].join("\n");

  sh.getRange(c.nome).setNote(nota);
}

function montarResumoConfirmacaoRecebimentos_(
  origem,
  resultados,
  processados,
  duplicadosLog,
  duplicadosGC,
  ambiguosGC,
  forcados,
  confirmadosGC,
  erros
) {
  const nome = String(origem || "").toUpperCase();

  const linhas = [];
  linhas.push("Lote de recebimentos confirmado (" + nome + ").");
  linhas.push("");
  linhas.push("Processados: " + processados);
  linhas.push("Duplicados no LOG_PIX: " + duplicadosLog);
  linhas.push("Duplicados no GC: " + duplicadosGC);
  linhas.push("Ambíguos no GC: " + ambiguosGC);
  linhas.push("Forçados manualmente: " + forcados);
  linhas.push("Confirmados no GC: " + confirmadosGC);
  linhas.push("Erros: " + erros);

  if (resultados.length) {
    linhas.push("", "Resumo:");
    resultados.forEach(function(r, idx) {
      if (r.ok) {
        linhas.push(
          (idx + 1) + ". " +
          (r.force_duplicate ? "OK FORÇADO" : "OK") + " | " +
          r.cliente_oficial + " | " +
          r.data_pagamento + " | " +
          formatMoneySimple_(r.valor) +
          (r.confirmado_no_gc ? " | GC OK" : " | GC pendente")
        );
      } else {
        linhas.push(
          (idx + 1) + ". " +
          String(r.tipo || "ERRO").toUpperCase() + " | " +
          (r.cliente_oficial || "?") + " | " +
          (r.erro || "falha")
        );
      }
    });
  }

  return {
    ok: erros === 0,
    resultados: resultados,
    totais: {
      processados: processados,
      duplicados_log: duplicadosLog,
      duplicados_gc: duplicadosGC,
      ambiguos_gc: ambiguosGC,
      forcados: forcados,
      confirmados_gc: confirmadosGC,
      erros: erros
    },
    message: linhas.join("\n")
  };
}

function gravarRegistroItauNoDriveSafe_(fileName, jsonObj) {
  if (typeof gravarRegistroItauNoDrive_ === "function") {
    return gravarRegistroItauNoDrive_(fileName, jsonObj);
  }

  if (typeof DriveApp === "undefined") {
    throw new Error("DriveApp indisponível.");
  }

  const folder = DriveApp.getFolderById(ITAU_DRIVE_FOLDER_ID_LOG);
  const files = folder.getFilesByName(fileName);
  while (files.hasNext()) {
    files.next().setTrashed(true);
  }

  return folder.createFile(fileName, JSON.stringify(jsonObj, null, 2), MimeType.PLAIN_TEXT);
}

function salvarJsonClienteItauSafe_(clienteOficial, extraido, idComprovante, registroJson) {
  if (typeof salvarJsonClienteItau_ === "function") {
    return salvarJsonClienteItau_(clienteOficial, extraido, idComprovante, registroJson);
  }

  if (typeof salvarJsonClienteInter_ === "function") {
    return salvarJsonClienteInter_(
      clienteOficial,
      extraido,
      {
        getSubject: function() {
          return registroJson && registroJson.subject ? registroJson.subject : "Extrato Itaú PDF";
        }
      },
      idComprovante,
      registroJson
    );
  }

  return null;
}

function marcarMensagemInterComoProcessadaSePossivel_(messageId) {
  try {
    if (!messageId) return;

    if (typeof GmailApp === "undefined" || typeof GmailApp.getMessageById !== "function") {
      return;
    }

    const msg = GmailApp.getMessageById(String(messageId));
    if (!msg) return;

    const thread = msg.getThread();
    if (thread) {
      marcarThreadComoProcessadoInter_(thread);
    }

  } catch (e) {
    Logger.log("Falha ao marcar mensagem como processada: " + e);
  }
}

function formatMoneySimple_(v) {
  const n = Number(v);
  if (!isFinite(n)) return "R$ ?";
  return "R$ " + n.toFixed(2);
}