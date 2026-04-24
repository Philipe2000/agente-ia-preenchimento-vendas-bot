/************************************************************
 * ITAU CORE - RECEBIMENTOS
 * MODO NOVO:
 * - lê telegram.document_json gerado pela IA no Node
 * - usa lancamentos estruturados
 * - não depende mais de parser manual do PDF
 * - mantém mesmo padrão do Inter para:
 *   - MAPA_CLIENTES
 *   - duplicata em Drive
 *   - duplicata no GC
 *   - comprovantes por cliente
 ************************************************************/

const ITAU_CONTA_OFICIAL = "Itaú Empresas";

// por enquanto reaproveita as mesmas pastas do Inter
const ITAU_DRIVE_FOLDER_ID_LOG = INTER_DRIVE_FOLDER_ID_LOG;
const ITAU_DRIVE_FOLDER_ID_COMPROVANTES = INTER_DRIVE_FOLDER_ID_COMPROVANTES;

/************************************************************
 * ENTRADA PRINCIPAL
 ************************************************************/
function preVisualizarItauRecebimentos_(periodo, telegram, meta) {
  Logger.log("ITAU CORE periodo recebido = " + JSON.stringify(periodo));
  Logger.log("ITAU CORE document_json = " + JSON.stringify((telegram && telegram.document_json) || null));

  const docJson = (telegram && telegram.document_json) || null;
  const banco = String((docJson && docJson.banco) || "").toLowerCase().trim();
  const extratoDetectado = !!(docJson && docJson.extrato_detectado);

  if (!docJson) {
    return {
      ok: false,
      message: "Não recebi o JSON estruturado do PDF do Itaú."
    };
  }

  if (!extratoDetectado) {
    return {
      ok: false,
      message: "A IA não confirmou que o arquivo é um extrato bancário válido."
    };
  }

  if (banco && banco !== "itau") {
    return {
      ok: false,
      message: 'O arquivo analisado não foi identificado como extrato Itaú. Banco detectado: "' + banco + '"'
    };
  }

  const lancamentos = Array.isArray(docJson.lancamentos)
    ? docJson.lancamentos.map(normalizarLancamentoItauJson_)
    : [];

  Logger.log("ITAU CORE total lancamentos json = " + lancamentos.length);
  Logger.log("ITAU CORE lancamentos json = " + JSON.stringify(lancamentos.slice(0, 20)));

  const recebidos = filtrarLancamentosItauRecebidos_(lancamentos);
  const recebidosNoPeriodo = filtrarLancamentosItauPorPeriodo_(recebidos, periodo);

  Logger.log("ITAU CORE total recebidos = " + recebidos.length);
  Logger.log("ITAU CORE total recebidosNoPeriodo = " + recebidosNoPeriodo.length);

  const itens_prontos = [];
  const pendencias_associacao = [];
  const ignorados = [];
  const duplicados = [];
  const ja_processados = [];

  let seqPronto = 1;
  let seqPend = 1;

  for (const lanc of recebidosNoPeriodo) {
    try {
      if (deveIgnorarPagadorItau_(lanc.nome_pagador || "")) {
        ignorados.push({
          motivo: "transferencia_interna",
          nome_extraido: lanc.nome_pagador || "",
          data_pagamento: lanc.data_pagamento || "",
          valor: lanc.valor != null ? Number(lanc.valor) : null,
          linha_origem: lanc.linha_resumo || ""
        });
        continue;
      }

      const chaveDuplicidade = montarChaveDuplicidadeItau_(lanc);

      if (existeRegistroDriveItau_(chaveDuplicidade.fileName)) {
        duplicados.push({
          motivo: "duplicado_drive_log",
          nome_extraido: lanc.nome_pagador || "",
          data_pagamento: lanc.data_pagamento || "",
          valor: lanc.valor != null ? Number(lanc.valor) : null,
          cpf_cnpj: lanc.cpf_cnpj || "",
          banco_extraido: "Itaú Empresas",
          linha_origem: lanc.linha_resumo || ""
        });
        continue;
      }

      const clienteInfo = resolverClienteOficialItau_(lanc.nome_pagador || "");

      if (!clienteInfo || !clienteInfo.cliente) {
        pendencias_associacao.push({
          id_local: "P" + seqPend++,
          nome_extraido: lanc.nome_pagador || "",
          data_pagamento: lanc.data_pagamento || "",
          valor: lanc.valor != null ? Number(lanc.valor) : null,
          forma: "PIX",
          conta_oficial: ITAU_CONTA_OFICIAL,
          banco_extraido: "Itaú Empresas",
          cpf_cnpj: lanc.cpf_cnpj || "",
          id_transacao: "",
          assunto_email: "Extrato Itaú PDF",
          remetente: "Telegram PDF",
          message_id: String((meta && meta.message_id) || ""),
          linha_origem: lanc.linha_resumo || "",
          status: "pendente_associacao"
        });
        continue;
      }

      const dupGC = existeDuplicataGCItau_(clienteInfo.cliente, lanc);

      if (dupGC.ok && dupGC.duplicado) {
        duplicados.push({
          motivo: "duplicado_gc",
          nome_extraido: lanc.nome_pagador || "",
          cliente_oficial: clienteInfo.cliente,
          data_pagamento: lanc.data_pagamento || "",
          valor: lanc.valor != null ? Number(lanc.valor) : null,
          cpf_cnpj: lanc.cpf_cnpj || "",
          banco_extraido: "Itaú Empresas",
          gc_recebimento_id: dupGC.recebimento_id || null,
          gc_recebimento_codigo: dupGC.recebimento_codigo || null,
          linha_origem: lanc.linha_resumo || ""
        });
        continue;
      }

      if (dupGC.ok && dupGC.ambiguo) {
        pendencias_associacao.push({
          id_local: "P" + seqPend++,
          nome_extraido: lanc.nome_pagador || "",
          cliente_sugerido: clienteInfo.cliente,
          data_pagamento: lanc.data_pagamento || "",
          valor: lanc.valor != null ? Number(lanc.valor) : null,
          forma: "PIX",
          conta_oficial: ITAU_CONTA_OFICIAL,
          banco_extraido: "Itaú Empresas",
          cpf_cnpj: lanc.cpf_cnpj || "",
          id_transacao: "",
          assunto_email: "Extrato Itaú PDF",
          remetente: "Telegram PDF",
          message_id: String((meta && meta.message_id) || ""),
          linha_origem: lanc.linha_resumo || "",
          status: "pendente_gc_ambiguo",
          erro: "Ambiguidade no GC: múltiplos recebimentos iguais para a mesma cliente/data/valor.",
          gc_ambiguo: true,
          gc_matches: dupGC.matches || []
        });
        continue;
      }

      itens_prontos.push({
        id_local: "I" + seqPronto++,
        cliente_oficial: clienteInfo.cliente,
        nome_extraido: lanc.nome_pagador || "",
        data_pagamento: lanc.data_pagamento || "",
        valor: lanc.valor != null ? Number(lanc.valor) : null,
        forma: "PIX",
        conta_oficial: ITAU_CONTA_OFICIAL,
        banco_extraido: "Itaú Empresas",
        cpf_cnpj: lanc.cpf_cnpj || "",
        id_transacao: "",
        assunto_email: "Extrato Itaú PDF",
        remetente: "Telegram PDF",
        message_id: String((meta && meta.message_id) || ""),
        linha_origem: lanc.linha_resumo || "",
        status: "pronto"
      });

    } catch (e) {
      pendencias_associacao.push({
        id_local: "P" + seqPend++,
        nome_extraido: lanc.nome_pagador || "",
        data_pagamento: lanc.data_pagamento || "",
        valor: lanc.valor != null ? Number(lanc.valor) : null,
        forma: "PIX",
        conta_oficial: ITAU_CONTA_OFICIAL,
        banco_extraido: "Itaú Empresas",
        cpf_cnpj: lanc.cpf_cnpj || "",
        id_transacao: "",
        assunto_email: "Extrato Itaú PDF",
        remetente: "Telegram PDF",
        message_id: String((meta && meta.message_id) || ""),
        linha_origem: lanc.linha_resumo || "",
        status: "pendente_associacao",
        erro: String(e)
      });
    }
  }

  const resposta = {
    ok: true,
    modo: "pre_visualizacao",
    origem: "itau",
    periodo: normalizarPeriodoRetornoItau_(periodo),
    itens_prontos: itens_prontos,
    pendencias_associacao: pendencias_associacao,
    ignorados: ignorados,
    duplicados: duplicados,
    ja_processados: ja_processados,
    message: "Pré-visualização do lote Itaú gerada com sucesso."
  };

  Logger.log("ITAU CORE resposta.periodo = " + JSON.stringify(resposta.periodo));
  return resposta;
}

/************************************************************
 * NORMALIZAÇÃO DO JSON VINDO DA IA
 ************************************************************/
function normalizarLancamentoItauJson_(item) {
  return {
    tipo: String((item && item.tipo) || "").toLowerCase().trim(),
    data_pagamento: normalizarDataIsoItauCore_((item && item.data_pagamento) || ""),
    nome_pagador: limparNomeExtraidoInter_((item && item.nome_pagador) || ""),
    cpf_cnpj: String((item && item.cpf_cnpj) || "").trim(),
    valor: item && item.valor != null ? Number(item.valor) : null,
    linha_resumo: String((item && item.linha_resumo) || "").trim()
  };
}

function normalizarDataIsoItauCore_(value) {
  const s = String(value || "").trim();
  if (!s) return "";

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    return s;
  }

  const br = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (br) {
    return br[3] + "-" + br[2] + "-" + br[1];
  }

  return "";
}

/************************************************************
 * FILTRO DE LANÇAMENTOS
 ************************************************************/
function filtrarLancamentosItauRecebidos_(lancamentos) {
  return (lancamentos || []).filter(function(l) {
    return (
      l &&
      String(l.tipo || "").toLowerCase().trim() === "pix_recebido" &&
      String(l.data_pagamento || "").trim() &&
      isFinite(Number(l.valor)) &&
      Number(l.valor) > 0
    );
  });
}

function filtrarLancamentosItauPorPeriodo_(lancamentos, periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();

  if (tipo === "datas_especificas") {
    const datas = {};
    (periodo.datas || []).forEach(function(d) {
      datas[String(d)] = true;
    });

    return (lancamentos || []).filter(function(l) {
      return !!datas[String(l.data_pagamento || "")];
    });
  }

  const dias = getPeriodoDias_(periodo);
  const janela = montarJanelaDatasInterComDias_(dias);

  return (lancamentos || []).filter(function(l) {
    return !!janela[String(l.data_pagamento || "")];
  });
}

function normalizarPeriodoRetornoItau_(periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();

  if (tipo === "datas_especificas") {
    const datas = Array.isArray(periodo && periodo.datas) ? periodo.datas : [];
    return {
      tipo: "datas_especificas",
      datas: datas,
      label: formatPeriodoLabel_(periodo)
    };
  }

  const dias = getPeriodoDias_(periodo);
  return {
    tipo: "dias",
    valor: dias,
    label: formatPeriodoLabel_({
      tipo: "dias",
      valor: dias,
      label: String((periodo && periodo.label) || "").trim()
    })
  };
}

/************************************************************
 * MAPA CLIENTES
 ************************************************************/
function resolverClienteOficialItau_(nomeExtraido) {
  return resolverClienteOficialInter_(nomeExtraido);
}

function deveIgnorarPagadorItau_(nomePagador) {
  return deveIgnorarPagadorInter_(nomePagador);
}

/************************************************************
 * GC - CONTA ITAÚ
 ************************************************************/
function getContaBancariaIdItauGC_() {
  const cache = CacheService.getScriptCache();
  const cacheKey = "GC_CONTA_ID_ITAU_EMPRESAS";
  const cached = cache.get(cacheKey);
  if (cached) return String(cached);

  const id = getContaBancariaIdPorNomeGC_(ITAU_CONTA_OFICIAL);
  cache.put(cacheKey, String(id), 21600);
  return String(id);
}

function existeDuplicataGCItau_(clienteOficial, extraido) {
  try {
    if (typeof mapClienteParaId_ !== "function" || typeof jreq_ !== "function") {
      return {
        ok: false,
        duplicado: false,
        ambiguo: false,
        motivo: "gc_api_indisponivel"
      };
    }

    const cid = mapClienteParaId_(clienteOficial);
    const data = String(extraido.data_pagamento || "").trim();
    const valor = Number(extraido.valor);
    const contaBancariaIdItau = getContaBancariaIdItauGC_();

    if (!cid || !data || !isFinite(valor)) {
      return {
        ok: false,
        duplicado: false,
        ambiguo: false,
        motivo: "dados_invalidos"
      };
    }

    const lista = jreq_("GET", "/recebimentos", {
      params: {
        cliente_id: cid,
        conta_bancaria_id: contaBancariaIdItau,
        data_inicio: data,
        data_fim: data,
        valor_inicio: valor,
        valor_fim: valor,
        liquidado: "pg",
        limit: 200,
        pagina: 1
      }
    });

    const itens = Array.isArray(lista) ? lista : [];

    const matches = itens.filter(function(r) {
      const valorCand = Number(r.valor_total || r.valor || 0);

      const datasCand = [
        r.data_liquidacao,
        r.data_competencia,
        r.data_vencimento
      ]
        .map(function(x) { return String(x || "").slice(0, 10); })
        .filter(Boolean);

      const mesmaData = datasCand.indexOf(data) >= 0;
      const mesmoValor = Math.abs(valorCand - valor) < 0.01;
      const mesmaConta =
        String(r.conta_bancaria_id || "") === String(contaBancariaIdItau);

      return mesmaData && mesmoValor && mesmaConta;
    });

    if (matches.length === 0) {
      return {
        ok: true,
        duplicado: false,
        ambiguo: false,
        motivo: "sem_match"
      };
    }

    if (matches.length === 1) {
      return {
        ok: true,
        duplicado: true,
        ambiguo: false,
        motivo: "match_unico",
        recebimento_id: matches[0].id || null,
        recebimento_codigo: matches[0].codigo || null
      };
    }

    return {
      ok: true,
      duplicado: false,
      ambiguo: true,
      motivo: "multiplos_matches_mesmo_dia_valor",
      matches: matches.slice(0, 10)
    };

  } catch (e) {
    Logger.log("Falha ao verificar duplicata no GC (Itaú): " + e);
    return {
      ok: false,
      duplicado: false,
      ambiguo: false,
      motivo: String(e)
    };
  }
}

/************************************************************
 * DRIVE / LOG / COMPROVANTES
 ************************************************************/
function montarChaveDuplicidadeItau_(lanc) {
  const base = [
    "ITAU",
    String(lanc.data_pagamento || ""),
    Number(lanc.valor || 0).toFixed(2),
    gcNormInter_(lanc.nome_pagador || ""),
    gcNormInter_(lanc.cpf_cnpj || "")
  ].join("__");

  const chave = "ITAU_PDF__" + sanitizeFileNameInter_(base);

  return {
    chave: chave,
    fileName: chave + ".json"
  };
}

function lerRegistroDriveItau_(fileName) {
  const folder = DriveApp.getFolderById(ITAU_DRIVE_FOLDER_ID_LOG);
  const files = folder.getFilesByName(fileName);
  if (!files.hasNext()) return null;

  const file = files.next();
  const text = file.getBlob().getDataAsString();

  try {
    return JSON.parse(text);
  } catch (e) {
    return {
      _arquivo_legado: true,
      raw_text: text
    };
  }
}

function existeRegistroDriveItau_(fileName) {
  const reg = lerRegistroDriveItau_(fileName);
  if (!reg) return false;

  if (reg._arquivo_legado) return true;

  const status = String(reg.status || "processado").trim();
  return ["processado", "duplicado-drive", "duplicado-gc", "processado_forcado"].indexOf(status) >= 0;
}

function gravarRegistroItauNoDrive_(fileName, obj) {
  const folder = DriveApp.getFolderById(ITAU_DRIVE_FOLDER_ID_LOG);
  const files = folder.getFilesByName(fileName);
  const payload = JSON.stringify(obj, null, 2);

  if (files.hasNext()) {
    files.next().setContent(payload);
    return;
  }

  folder.createFile(fileName, payload, MimeType.PLAIN_TEXT);
}

function salvarJsonClienteItau_(clienteOficial, lanc, idComprovante, registroBase) {
  const pastaMes = garantirEstruturaPastaClienteItau_(clienteOficial, lanc.data_pagamento);
  const nomeArquivo = montarNomeArquivoJsonItau_(clienteOficial, lanc, idComprovante);

  const payload = Object.assign({}, registroBase, {
    cliente_pasta: clienteOficial,
    origem_pdf: "Extrato Itaú PDF"
  });

  const files = pastaMes.getFilesByName(nomeArquivo);
  const content = JSON.stringify(payload, null, 2);

  if (files.hasNext()) {
    files.next().setContent(content);
    return;
  }

  pastaMes.createFile(nomeArquivo, content, MimeType.PLAIN_TEXT);
}

function garantirEstruturaPastaClienteItau_(clienteOficial, dataIso) {
  const raiz = DriveApp.getFolderById(ITAU_DRIVE_FOLDER_ID_COMPROVANTES);
  const nomeCliente = sanitizeFolderNameInter_(clienteOficial);
  const data = new Date(dataIso + "T12:00:00");

  const ano = Utilities.formatDate(data, Session.getScriptTimeZone(), "yyyy");
  const mes = nomeMesPtInter_(data);

  const pastaCliente = getOrCreateSubFolderInter_(raiz, nomeCliente);
  const pastaAno = getOrCreateSubFolderInter_(pastaCliente, ano);
  const pastaMes = getOrCreateSubFolderInter_(pastaAno, mes);

  return pastaMes;
}

function montarNomeArquivoJsonItau_(clienteOficial, lanc, idComprovante) {
  return [
    sanitizeFileNameInter_(clienteOficial),
    lanc.data_pagamento,
    Number(lanc.valor).toFixed(2),
    sanitizeFileNameInter_(idComprovante)
  ].join("__") + ".json";
}