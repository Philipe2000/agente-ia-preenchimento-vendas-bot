/************************************************************
 * INTER CORE - RECEBIMENTOS
 * Atualizado para:
 * - conta_bancaria_id automático via GC
 * - log com status
 * - duplicata GC com conta bancária
 * - base para reprocessamento futuro
 ************************************************************/

const RECEB_PLANILHA_ID = "11IMG566GZByCTvuKQ4LM32QTY16ZhFJMXBY4hDiLir4";

const INTER_OPENAI_MODEL = "gpt-4.1-mini";
const INTER_JANELA_DIAS = 3;

const INTER_ABA_MAPA_CLIENTES = "MAPA_CLIENTES";
const INTER_ABA_CENTRAL = "Central de Controle API";

const INTER_DRIVE_FOLDER_ID_LOG = "1qJPNdrUQPb-GTVtLlr1UhQ-7DgUMF4Y2";
const INTER_DRIVE_FOLDER_ID_COMPROVANTES = "1oH3DVP6WfNjOz1xQrWJ5Qz0VP7Tf1BF7";

const INTER_CONTA_OFICIAL = "Inter Empresas";

const INTER_GMAIL_LABEL_PROCESSADO = "pix-processado";
const INTER_GMAIL_LABEL_DUPLICADO = "pix-duplicado";
const INTER_GMAIL_LABEL_ERRO = "pix-erro";
const INTER_GMAIL_LABEL_IGNORADO = "pix-ignorado";

const INTER_EXCECOES_PAGADOR_IGNORAR = [
  "lp comercio",
  "lp comercio de cosmeticos",
  "lp comercio de cosmeticos ltda",
  "lp comercio de cosm"
];

const INTER_MAX_BASES = 10;

const INTER_CELLS_CAPTURA = [
  { nome: "C11", data: "E11", valor: "G11", forma: "I11", conta: "K11" },
  { nome: "C15", data: "E15", valor: "G15", forma: "I15", conta: "K15" },
  { nome: "C19", data: "E19", valor: "G19", forma: "I19", conta: "K19" },
  { nome: "C23", data: "E23", valor: "G23", forma: "I23", conta: "K23" },
  { nome: "C27", data: "E27", valor: "G27", forma: "I27", conta: "K27" },
  { nome: "C31", data: "E31", valor: "G31", forma: "I31", conta: "K31" },
  { nome: "C35", data: "E35", valor: "G35", forma: "I35", conta: "K35" },
  { nome: "C39", data: "E39", valor: "G39", forma: "I39", conta: "K39" }
];

/************************************************************
 * TESTE MANUAL OPCIONAL
 ************************************************************/
function captarDadosInterHoje() {
  const result = processarInterRecebimentos_(INTER_JANELA_DIAS);
  notifyInter_(result.message, "Inter");
}

/************************************************************
 * FUNÇÃO PRINCIPAL NOVA
 ************************************************************/
function processarInterRecebimentos_(dias) {
  dias = Number(dias);
  if (!isFinite(dias) || dias <= 0) dias = 1;

  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(INTER_ABA_CENTRAL);
  if (!sh) {
    throw new Error('Aba "' + INTER_ABA_CENTRAL + '" não encontrada.');
  }

  garantirLabelInter_(INTER_GMAIL_LABEL_PROCESSADO);
  garantirLabelInter_(INTER_GMAIL_LABEL_DUPLICADO);
  garantirLabelInter_(INTER_GMAIL_LABEL_ERRO);
  garantirLabelInter_(INTER_GMAIL_LABEL_IGNORADO);

  const mensagens = listarMensagensInterJanelaComDias_(dias);

  if (!mensagens.length) {
    return {
      ok: true,
      origem: "inter",
      periodo_dias: dias,
      processados: 0,
      duplicados: 0,
      ignorados: 0,
      erros: 0,
      ja_processados: 0,
      relatorio: [],
      message: "Nenhum e-mail do Inter encontrado na janela de " + dias + " dia(s)."
    };
  }

  let processados = 0;
  let duplicados = 0;
  let ignorados = 0;
  let erros = 0;
  let jaProcessados = 0;

  const relatorio = [];

  for (const item of mensagens) {
    const thread = item.thread;
    const msg = item.message;

    try {
      if (jaProcessouMensagemInter_(msg)) {
        jaProcessados += 1;
        continue;
      }

      const linhaDestino = getPrimeiraLinhaLivreInter_(sh);
      if (!linhaDestino) {
        relatorio.push("Sem linha livre para continuar.");
        break;
      }

      const assunto = safeSubjectInter_(msg);
      const corpo = limparTextoInter_(msg.getPlainBody() || "");

      const assuntoNorm = gcNormInter_(assunto);
      const corpoNorm = gcNormInter_(corpo);

      const parecePixRecebido =
        assuntoNorm.includes("pagamento pix recebido") ||
        corpoNorm.includes("voce recebeu um pix");

      if (!parecePixRecebido) {
        ignorados += 1;
        gravarRegistroMensagemInterNoDrive_(msg, null, null, "ignorado-nao-parece-pix", null);
        marcarThreadComoIgnoradoInter_(thread);
        continue;
      }

      const extraido = analisarEmailPixInterComOpenAI_(msg);

      if (!extraido || !extraido.eh_pix) {
        ignorados += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, null, "ignorado-nao-eh-pix", null);
        marcarThreadComoIgnoradoInter_(thread);
        relatorio.push("Ignorado (não parece PIX): " + assunto);
        continue;
      }

      if (!extraido.tipo_pix || gcNormInter_(extraido.tipo_pix) !== "recebido") {
        ignorados += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, null, "ignorado-nao-recebido", null);
        marcarThreadComoIgnoradoInter_(thread);
        relatorio.push("Ignorado (não é PIX recebido): " + assunto);
        continue;
      }

      extraido.data_pagamento = Utilities.formatDate(
        msg.getDate(),
        Session.getScriptTimeZone(),
        "yyyy-MM-dd"
      );

      extraido.nome_pagador = limparNomeExtraidoInter_(extraido.nome_pagador || "");

      if (deveIgnorarPagadorInter_(extraido.nome_pagador || "")) {
        ignorados += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, null, "ignorado-transferencia-interna", null);
        marcarThreadComoIgnoradoInter_(thread);
        relatorio.push("Ignorado (transferência interna): " + (extraido.nome_pagador || assunto));
        continue;
      }

      const chaveDuplicidade = montarChaveDuplicidadeInter_(extraido, msg);

      if (existeRegistroDriveInter_(chaveDuplicidade.fileName)) {
        duplicados += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, null, "duplicado-drive", null);
        marcarThreadComoDuplicadoInter_(thread);
        relatorio.push("Duplicado no Drive: " + chaveDuplicidade.chave);
        continue;
      }

      const clienteInfo = resolverClienteOficialInter_(extraido.nome_pagador || "");

      if (!clienteInfo || !clienteInfo.cliente) {
        erros += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, null, "erro-cliente-nao-definido", null);
        marcarThreadComoErroInter_(thread);
        relatorio.push("Cliente não definido: " + (extraido.nome_pagador || assunto));
        continue;
      }

      const dupGC = existeDuplicataGCInter_(clienteInfo.cliente, extraido);

      if (dupGC.ok && dupGC.ambiguo) {
        erros += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, clienteInfo, "gc-ambiguo", null);
        marcarThreadComoErroInter_(thread);
        relatorio.push(
          "GC ambíguo: " +
          clienteInfo.cliente + " | " +
          extraido.data_pagamento + " | " +
          formatarMoedaInter_(extraido.valor)
        );
        continue;
      }

      if (dupGC.ok && dupGC.duplicado) {
        duplicados += 1;
        gravarRegistroMensagemInterNoDrive_(msg, extraido, clienteInfo, "duplicado-gc", null);
        marcarThreadComoDuplicadoInter_(thread);
        relatorio.push(
          "Duplicado no GC: " +
          clienteInfo.cliente + " | " +
          extraido.data_pagamento + " | " +
          formatarMoedaInter_(extraido.valor)
        );
        continue;
      }

      const idComprovante = gerarIdComprovanteInter_(msg, extraido);

      preencherLinhaCapturaInter_(sh, linhaDestino, {
        cliente_oficial: clienteInfo.cliente,
        data_pagamento: extraido.data_pagamento || "",
        valor: extraido.valor,
        forma: "PIX",
        conta_oficial: INTER_CONTA_OFICIAL,
        nome_extraido: extraido.nome_pagador || "",
        assunto_email: assunto,
        remetente: msg.getFrom(),
        id_transacao: extraido.id_transacao || "",
        id_comprovante: idComprovante,
        motivo_cliente: clienteInfo.motivo || "",
        score_cliente: clienteInfo.score,
        banco_extraido: extraido.banco_email || ""
      });

      const registroJson = {
        origem: "inter",
        banco: "Inter Empresas",
        cliente_oficial: clienteInfo.cliente,
        nome_extraido: extraido.nome_pagador || "",
        data: extraido.data_pagamento || "",
        valor: extraido.valor != null ? Number(extraido.valor) : null,
        tipo: "recebido",
        id_transacao: extraido.id_transacao || null,
        id_comprovante: idComprovante,
        subject: assunto,
        from: msg.getFrom(),
        message_id: String(msg.getId() || ""),
        processado_em: new Date().toISOString(),
        status: "processado"
      };

      gravarRegistroInterNoDrive_(chaveDuplicidade.fileName, registroJson);
      gravarRegistroMensagemInterNoDrive_(msg, extraido, clienteInfo, "processado", idComprovante);
      salvarJsonClienteInter_(clienteInfo.cliente, extraido, msg, idComprovante, registroJson);

      marcarThreadComoProcessadoInter_(thread);

      processados += 1;
      relatorio.push(
        "OK | " +
        clienteInfo.cliente + " | " +
        extraido.data_pagamento + " | " +
        formatarMoedaInter_(extraido.valor)
      );

      Utilities.sleep(60);

    } catch (e) {
      erros += 1;
      try { marcarThreadComoErroInter_(thread); } catch (err) {}
      relatorio.push("Erro: " + safeSubjectInter_(msg) + " => " + e);
    }
  }

  return {
    ok: true,
    origem: "inter",
    periodo_dias: dias,
    processados: processados,
    duplicados: duplicados,
    ignorados: ignorados,
    erros: erros,
    ja_processados: jaProcessados,
    relatorio: relatorio,
    message: montarResumoInterProcessado_({
      dias: dias,
      processados: processados,
      duplicados: duplicados,
      ignorados: ignorados,
      erros: erros,
      jaProcessados: jaProcessados,
      relatorio: relatorio
    })
  };
}

function montarResumoInterProcessado_(ctx) {
  const linhas = [
    "Recebimentos Inter processados.",
    "",
    "Período: últimos " + ctx.dias + " dia(s)",
    "Processados: " + ctx.processados,
    "Duplicados: " + ctx.duplicados,
    "Ignorados: " + ctx.ignorados,
    "Já processados: " + ctx.jaProcessados,
    "Erros: " + ctx.erros
  ];

  if (ctx.relatorio && ctx.relatorio.length) {
    linhas.push("", "Resumo:");
    linhas.push(ctx.relatorio.slice(0, 20).join("\n"));
  }

  return linhas.join("\n");
}

/************************************************************
 * COMPATIBILIDADE
 ************************************************************/
function listarMensagensInterJanela_() {
  return listarMensagensInterJanelaComDias_(INTER_JANELA_DIAS);
}

/************************************************************
 * GMAIL
 ************************************************************/
function listarMensagensInterJanelaComDias_(dias) {
  const tz = Session.getScriptTimeZone();
  const hoje = new Date();
  const inicio = new Date(hoje);
  inicio.setDate(inicio.getDate() - (dias - 1));

  const amanha = new Date(hoje.getTime() + 24 * 60 * 60 * 1000);

  const afterStr = Utilities.formatDate(inicio, tz, "yyyy/MM/dd");
  const beforeStr = Utilities.formatDate(amanha, tz, "yyyy/MM/dd");

  const datasPermitidas = montarJanelaDatasInterComDias_(dias);

  const query = [
    "after:" + afterStr,
    "before:" + beforeStr,
    "from:no-reply@inter.co",
    '(subject:"Pagamento Pix recebido" OR "Você recebeu um Pix")'
  ].join(" ");

  const threads = GmailApp.search(query, 0, 200);
  const out = [];

  for (const thread of threads) {
    const msgs = thread.getMessages();

    for (const msg of msgs) {
      const dataMsg = Utilities.formatDate(msg.getDate(), tz, "yyyy-MM-dd");
      if (!datasPermitidas[dataMsg]) continue;

      out.push({
        thread: thread,
        message: msg
      });
    }
  }

  out.sort((a, b) => a.message.getDate().getTime() - b.message.getDate().getTime());
  return out;
}

function montarJanelaDatasInterComDias_(dias) {
  const out = {};
  const hoje = new Date();

  for (let i = 0; i < dias; i++) {
    const d = new Date(hoje);
    d.setDate(d.getDate() - i);

    const iso = Utilities.formatDate(
      d,
      Session.getScriptTimeZone(),
      "yyyy-MM-dd"
    );

    out[iso] = true;
  }

  return out;
}

function montarJanelaDatasInter_(dias) {
  return montarJanelaDatasInterComDias_(dias);
}

function marcarThreadComoProcessadoInter_(thread) {
  aplicarLabelExclusivaInter_(thread, INTER_GMAIL_LABEL_PROCESSADO);
}

function marcarThreadComoDuplicadoInter_(thread) {
  aplicarLabelExclusivaInter_(thread, INTER_GMAIL_LABEL_DUPLICADO);
}

function marcarThreadComoErroInter_(thread) {
  aplicarLabelExclusivaInter_(thread, INTER_GMAIL_LABEL_ERRO);
}

function marcarThreadComoIgnoradoInter_(thread) {
  aplicarLabelExclusivaInter_(thread, INTER_GMAIL_LABEL_IGNORADO);
}

function aplicarLabelExclusivaInter_(thread, nomeLabel) {
  const labels = [
    INTER_GMAIL_LABEL_PROCESSADO,
    INTER_GMAIL_LABEL_DUPLICADO,
    INTER_GMAIL_LABEL_ERRO,
    INTER_GMAIL_LABEL_IGNORADO
  ];

  for (const l of labels) {
    try {
      const lbl = garantirLabelInter_(l);
      thread.removeLabel(lbl);
    } catch (e) {}
  }

  thread.addLabel(garantirLabelInter_(nomeLabel));
}

function garantirLabelInter_(nome) {
  return GmailApp.getUserLabelByName(nome) || GmailApp.createLabel(nome);
}

/************************************************************
 * OPENAI
 ************************************************************/
function analisarEmailPixInterComOpenAI_(msg) {
  const apiKey = getScriptPropOrThrowInter_("OPENAI_API_KEY");

  const assunto = safeSubjectInter_(msg);
  const remetente = msg.getFrom();
  const corpoTexto = limparTextoInter_(msg.getPlainBody() || "");
  const anexos = msg.getAttachments({ includeInlineImages: false, includeAttachments: true }) || [];

  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      eh_pix: { type: "boolean" },
      tipo_pix: { type: ["string", "null"] },
      nome_pagador: { type: ["string", "null"] },
      valor: { type: ["number", "null"] },
      banco_email: { type: ["string", "null"] },
      id_transacao: { type: ["string", "null"] },
      confianca: { type: ["number", "null"] },
      observacoes: { type: ["string", "null"] }
    },
    required: [
      "eh_pix",
      "tipo_pix",
      "nome_pagador",
      "valor",
      "banco_email",
      "id_transacao",
      "confianca",
      "observacoes"
    ]
  };

  const prompt = [
    "Analise o e-mail abaixo e, se houver, também o anexo.",
    "Seu objetivo é identificar se o conteúdo trata de um PIX bancário brasileiro.",
    "Retorne APENAS JSON válido, seguindo exatamente o schema.",
    "",
    "Regras:",
    "- eh_pix = true somente se realmente houver indicação suficiente de PIX.",
    '- tipo_pix deve ser "recebido" ou "realizado" quando identificável. Se não souber, null.',
    "- nome_pagador = nome da pessoa pagadora/origem. Em caso de PIX recebido, priorize quem pagou.",
    "- valor = número decimal sem símbolo de moeda.",
    "- banco_email = nome do banco ou instituição identificado no e-mail/comprovante.",
    "- id_transacao = id da transação PIX, e2e id, identificador, ou equivalente, se aparecer.",
    "- confianca = número entre 0 e 1.",
    "- observacoes = comentário curto.",
    "",
    "Importante:",
    "- Se for claramente um aviso de PIX recebido, marque tipo_pix = recebido.",
    "- Não invente dados."
  ].join("\n");

  const textoEmail = [
    "ASSUNTO:",
    assunto,
    "",
    "REMETENTE:",
    remetente,
    "",
    "CORPO DO E-MAIL:",
    corpoTexto
  ].join("\n");

  const contentParts = [
    { type: "input_text", text: prompt },
    { type: "input_text", text: textoEmail }
  ];

  const anexoUtil = escolherAnexoSuportadoInter_(anexos);
  if (anexoUtil) {
    const blob = anexoUtil.copyBlob();
    const mime = String(blob.getContentType() || "application/octet-stream").toLowerCase();
    const base64 = Utilities.base64Encode(blob.getBytes());
    const dataUrl = "data:" + mime + ";base64," + base64;

    if (mime === "application/pdf") {
      contentParts.push({
        type: "input_file",
        filename: anexoUtil.getName(),
        file_data: dataUrl
      });
    } else if (
      mime === "image/jpeg" ||
      mime === "image/png" ||
      mime === "image/gif" ||
      mime === "image/webp"
    ) {
      contentParts.push({
        type: "input_image",
        image_url: dataUrl
      });
    }
  }

  const payload = {
    model: INTER_OPENAI_MODEL,
    input: [
      {
        role: "user",
        content: contentParts
      }
    ],
    text: {
      format: {
        type: "json_schema",
        name: "captura_pix_email_inter",
        schema: schema,
        strict: true
      }
    }
  };

  const resp = UrlFetchApp.fetch("https://api.openai.com/v1/responses", {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + apiKey
    },
    muteHttpExceptions: true,
    payload: JSON.stringify(payload)
  });

  const code = resp.getResponseCode();
  const body = resp.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error("OpenAI HTTP " + code + ": " + body);
  }

  const js = JSON.parse(body);
  const texto = extrairTextoOpenAIInter_(js);

  if (!texto) {
    throw new Error("A OpenAI respondeu sem texto utilizável.");
  }

  let out;
  try {
    out = JSON.parse(texto);
  } catch (e) {
    throw new Error("Resposta da OpenAI não veio como JSON válido: " + texto);
  }

  if (out.valor != null) out.valor = Number(out.valor);

  return out;
}

function escolherAnexoSuportadoInter_(anexos) {
  for (const a of anexos) {
    const mime = String(a.getContentType() || "").toLowerCase();

    if (
      mime === "application/pdf" ||
      mime === "image/jpeg" ||
      mime === "image/png" ||
      mime === "image/gif" ||
      mime === "image/webp"
    ) {
      return a;
    }
  }
  return null;
}

function extrairTextoOpenAIInter_(js) {
  if (js.output_text) return String(js.output_text).trim();

  if (Array.isArray(js.output)) {
    for (const item of js.output) {
      if (!item || !Array.isArray(item.content)) continue;
      for (const c of item.content) {
        if (c.type === "output_text" && c.text) {
          return String(c.text).trim();
        }
      }
    }
  }

  return "";
}

/************************************************************
 * MAPA_CLIENTES
 ************************************************************/
function listarMapaClientesInter_() {
  const sh = SpreadsheetApp.openById(RECEB_PLANILHA_ID).getSheetByName(INTER_ABA_MAPA_CLIENTES);
  if (!sh) throw new Error('Aba "' + INTER_ABA_MAPA_CLIENTES + '" não encontrada.');

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const vals = sh.getRange(2, 1, lastRow - 1, 13).getDisplayValues();
  const out = [];

  for (let i = 0; i < vals.length; i++) {
    const row = vals[i];

    const bases = [];
    for (let c = 0; c < INTER_MAX_BASES; c++) {
      bases.push(String(row[c] || "").trim());
    }

    const cliente = String(row[10] || "").trim();
    const bancoPreferido = String(row[11] || "").trim();
    const ativo = gcNormInter_(row[12] || "");

    if (!cliente) continue;
    if (ativo && !["sim", "s", "ativo", "1", "true"].includes(ativo)) continue;

    out.push({
      rowNumber: i + 2,
      nome_bases: bases,
      cliente_oficial: cliente,
      banco_preferido: bancoPreferido
    });
  }

  return out;
}

function resolverClienteOficialInter_(nomeExtraido) {
  const nomeLimpo = limparNomeExtraidoInter_(nomeExtraido);
  const nomeNorm = gcNormInter_(nomeLimpo);

  if (!nomeNorm) {
    return { cliente: "", score: 0, motivo: "Nome extraído vazio" };
  }

  const mapa = listarMapaClientesInter_();
  if (!mapa.length) {
    return { cliente: "", score: 0, motivo: "Nenhum cliente encontrado no MAPA_CLIENTES" };
  }

  const exato = encontrarMatchExatoInter_(nomeNorm, mapa);
  if (exato) return exato;

  const candidatos = [];
  for (const item of mapa) {
    for (let i = 0; i < item.nome_bases.length; i++) {
      adicionarCandidatoInter_(candidatos, nomeNorm, item.cliente_oficial, item.nome_bases[i], "nome_base_" + (i + 1));
    }
    adicionarCandidatoInter_(candidatos, nomeNorm, item.cliente_oficial, item.cliente_oficial, "cliente_oficial");
  }

  candidatos.sort((a, b) => b.score - a.score);

  const top1 = candidatos[0];
  const top2 = candidatos[1];

  if (!top1 || top1.score < 0.50) {
    return {
      cliente: "",
      score: top1 ? top1.score : 0,
      motivo: 'Sem correspondência forte para "' + nomeLimpo + '"'
    };
  }

  if (
    top2 &&
    top1.cliente !== top2.cliente &&
    Math.abs(top1.score - top2.score) < 0.06 &&
    top2.score >= 0.50
  ) {
    const preferidoCurto = tentarPreferirNomeCurtoInter_(nomeLimpo, [top1, top2]);
    if (preferidoCurto) return preferidoCurto;

    return {
      cliente: "",
      score: top1.score,
      motivo: 'Ambíguo entre "' + top1.cliente + '" e "' + top2.cliente + '"'
    };
  }

  return {
    cliente: top1.cliente,
    score: Math.min(0.99, top1.score),
    motivo: 'Correspondência por ' + top1.origem + ' com "' + top1.comparadoCom + '"'
  };
}

function encontrarMatchExatoInter_(nomeNorm, mapa) {
  const exatos = [];

  for (const item of mapa) {
    for (let i = 0; i < item.nome_bases.length; i++) {
      const baseNorm = gcNormInter_(limparNomeExtraidoInter_(item.nome_bases[i] || ""));
      if (baseNorm && baseNorm === nomeNorm) {
        exatos.push({
          cliente: item.cliente_oficial,
          score: 0.999,
          motivo: 'Match exato por nome_base_' + (i + 1) + ' com "' + item.nome_bases[i] + '"'
        });
      }
    }

    const clienteNorm = gcNormInter_(limparNomeExtraidoInter_(item.cliente_oficial || ""));
    if (clienteNorm && clienteNorm === nomeNorm) {
      exatos.push({
        cliente: item.cliente_oficial,
        score: 0.999,
        motivo: 'Match exato por cliente_oficial com "' + item.cliente_oficial + '"'
      });
    }
  }

  if (exatos.length === 1) return exatos[0];
  if (exatos.length > 1) {
    const curto = exatos.find(x => tokensNomeInter_(gcNormInter_(x.cliente)).length === 1);
    return curto || {
      cliente: "",
      score: 0.999,
      motivo: "Match exato ambíguo entre clientes diferentes"
    };
  }

  return null;
}

function adicionarCandidatoInter_(candidatos, nomeNorm, clienteOficial, baseComparacao, origem) {
  const baseNorm = gcNormInter_(limparNomeExtraidoInter_(baseComparacao || ""));
  if (!baseNorm) return;

  const score = scoreNomeClienteInter_(nomeNorm, baseNorm, clienteOficial);
  const bonus = origem.indexOf("nome_base_") === 0 ? 0.10 : 0;

  candidatos.push({
    cliente: clienteOficial,
    score: Math.min(0.99, score + bonus),
    origem: origem,
    comparadoCom: baseComparacao
  });
}

function scoreNomeClienteInter_(nomeBusca, nomeCliente, clienteOficial) {
  if (!nomeBusca || !nomeCliente) return 0;

  if (nomeBusca === nomeCliente) return 1.00;
  if (nomeCliente.indexOf(nomeBusca) >= 0) return 0.96;
  if (nomeBusca.indexOf(nomeCliente) >= 0) return 0.94;

  const buscaTokens = tokensNomeInter_(nomeBusca);
  const clienteTokens = tokensNomeInter_(nomeCliente);
  const oficialTokens = tokensNomeInter_(gcNormInter_(clienteOficial || ""));

  if (!buscaTokens.length || !clienteTokens.length) return 0;

  const primeiroBusca = buscaTokens[0];
  const primeiroCliente = clienteTokens[0];

  let score = 0;

  if (primeiroBusca && primeiroBusca === primeiroCliente) score += 0.55;
  if (clienteTokens.includes(primeiroBusca)) score += 0.20;

  const inter = intersecTokensInter_(buscaTokens, clienteTokens).length;
  const uniao = uniaoTokensInter_(buscaTokens, clienteTokens).length;
  const jaccard = uniao ? inter / uniao : 0;
  score += jaccard * 0.40;

  if (inter >= 2) score += 0.12;
  if (primeiroBusca && primeiroBusca.length >= 4 && nomeCliente.startsWith(primeiroBusca)) score += 0.08;

  if (buscaTokens.length >= 2 && oficialTokens.length >= 2) {
    const sobrenomeBusca = buscaTokens[1];
    const sobrenomeOficial = oficialTokens[1];
    if (sobrenomeBusca && sobrenomeOficial && sobrenomeBusca !== sobrenomeOficial) {
      score -= 0.25;
    }
  }

  if (oficialTokens.length === 1 && buscaTokens[0] === oficialTokens[0]) {
    score += 0.10;
  }

  return Math.max(0, Math.min(0.99, score));
}

function tentarPreferirNomeCurtoInter_(nomeExtraido, tops) {
  const primeiro = tokensNomeInter_(gcNormInter_(nomeExtraido))[0] || "";
  if (!primeiro) return null;

  for (const t of tops) {
    const toks = tokensNomeInter_(gcNormInter_(t.cliente));
    if (toks.length === 1 && toks[0] === primeiro) {
      return {
        cliente: t.cliente,
        score: t.score,
        motivo: 'Preferência pelo nome curto "' + t.cliente + '" diante de conflito de complemento'
      };
    }
  }
  return null;
}

/************************************************************
 * PLANILHA
 ************************************************************/
function getPrimeiraLinhaLivreInter_(sh) {
  for (const c of INTER_CELLS_CAPTURA) {
    const nome = String(sh.getRange(c.nome).getValue() || "").trim();
    const valor = String(sh.getRange(c.valor).getValue() || "").trim();
    if (!nome && !valor) return c;
  }
  return null;
}

function preencherLinhaCapturaInter_(sh, c, dados) {
  if (dados.cliente_oficial) sh.getRange(c.nome).setValue(dados.cliente_oficial);
  if (dados.data_pagamento) sh.getRange(c.data).setValue(dados.data_pagamento);
  if (dados.valor != null && isFinite(Number(dados.valor))) sh.getRange(c.valor).setValue(Number(dados.valor));

  sh.getRange(c.forma).setValue("PIX");
  sh.getRange(c.conta).setValue(INTER_CONTA_OFICIAL);

  const nota = [
    "Origem: Inter Gmail",
    "Assunto: " + (dados.assunto_email || ""),
    "Remetente: " + (dados.remetente || ""),
    "Nome extraído: " + (dados.nome_extraido || ""),
    "Cliente resolvido: " + (dados.cliente_oficial || "não identificado"),
    "Motivo cliente: " + (dados.motivo_cliente || ""),
    "Score cliente: " + (dados.score_cliente != null ? dados.score_cliente : ""),
    "Conta resolvida: " + INTER_CONTA_OFICIAL,
    "Banco extraído: " + (dados.banco_extraido || ""),
    "ID transação: " + (dados.id_transacao || ""),
    "ID comprovante: " + (dados.id_comprovante || "")
  ].join("\n");

  sh.getRange(c.nome).setNote(nota);
}

/************************************************************
 * GC - CONTA BANCÁRIA AUTOMÁTICA
 ************************************************************/
function listarContasBancariasGC_() {
  const cache = CacheService.getScriptCache();
  const cacheKey = "GC_CONTAS_BANCARIAS_JSON";
  const cached = cache.get(cacheKey);

  if (cached) {
    try {
      return JSON.parse(cached);
    } catch (e) {}
  }

  const lista = jreq_("GET", "/contas_bancarias", {
    params: {
      limit: 200,
      pagina: 1
    }
  });

  const out = Array.isArray(lista) ? lista : [];
  cache.put(cacheKey, JSON.stringify(out), 21600);

  return out;
}

function getContaBancariaIdPorNomeGC_(nomeConta) {
  const alvo = gcNormInter_(nomeConta);
  if (!alvo) throw new Error("Nome da conta bancária vazio.");

  const contas = listarContasBancariasGC_();
  if (!contas.length) {
    throw new Error("Nenhuma conta bancária retornada pela API do GC.");
  }

  let exata = contas.find(function(c) {
    return gcNormInter_(c && c.nome || "") === alvo;
  });
  if (exata && exata.id) return String(exata.id);

  let parcial = contas.find(function(c) {
    const n = gcNormInter_(c && c.nome || "");
    return n.indexOf(alvo) >= 0 || alvo.indexOf(n) >= 0;
  });
  if (parcial && parcial.id) return String(parcial.id);

  throw new Error('Conta bancária não encontrada no GC: "' + nomeConta + '"');
}

function getContaBancariaIdInterGC_() {
  const cache = CacheService.getScriptCache();
  const cacheKey = "GC_CONTA_ID_INTER_EMPRESAS";
  const cached = cache.get(cacheKey);
  if (cached) return String(cached);

  const id = getContaBancariaIdPorNomeGC_(INTER_CONTA_OFICIAL);
  cache.put(cacheKey, String(id), 21600);
  return String(id);
}

/************************************************************
 * DUPLICATA GC
 ************************************************************/
function existeDuplicataGCInter_(clienteOficial, extraido) {
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
    const contaBancariaIdInter = getContaBancariaIdInterGC_();

    if (!cid || !data || !isFinite(valor)) {
      return {
        ok: false,
        duplicado: false,
        ambiguo: false,
        motivo: "cliente_id_data_ou_valor_invalidos"
      };
    }

    const lista = jreq_("GET", "/recebimentos", {
      params: {
        cliente_id: cid,
        conta_bancaria_id: contaBancariaIdInter,
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
      const mesmaConta = String(r.conta_bancaria_id || "") === String(contaBancariaIdInter);

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
      quantidade_matches: matches.length,
      matches: matches.slice(0, 10).map(function(r) {
        return {
          id: r.id || null,
          codigo: r.codigo || null,
          descricao: r.descricao || "",
          valor: r.valor_total || r.valor || null,
          data_liquidacao: r.data_liquidacao || null,
          data_competencia: r.data_competencia || null,
          data_vencimento: r.data_vencimento || null,
          conta_bancaria_id: r.conta_bancaria_id || null,
          nome_conta_bancaria: r.nome_conta_bancaria || null
        };
      })
    };

  } catch (e) {
    Logger.log("Falha ao verificar duplicata no GC (Inter): " + e);
    return {
      ok: false,
      duplicado: false,
      ambiguo: false,
      motivo: String(e)
    };
  }
}

/************************************************************
 * DRIVE / LOG / STATUS
 ************************************************************/
function montarChaveDuplicidadeInter_(extraido, msg) {
  const id = String(extraido.id_transacao || "").trim();
  if (id) {
    const chave = "INTER_ID__" + sanitizeFileNameInter_(id);
    return { chave: chave, fileName: chave + ".json" };
  }

  const fallback = [
    "INTER",
    Utilities.formatDate(msg.getDate(), Session.getScriptTimeZone(), "yyyy-MM-dd"),
    formatValorChaveInter_(extraido.valor),
    gcNormInter_(extraido.nome_pagador || ""),
    String(msg.getId() || "")
  ].join("__");

  const chave = "INTER_MSG__" + sanitizeFileNameInter_(fallback);
  return { chave: chave, fileName: chave + ".json" };
}

function montarChaveMensagemInter_(msg) {
  const id = String(msg.getId() || "").trim();
  return "INTER_MSG_ONLY__" + sanitizeFileNameInter_(id) + ".json";
}

function jaProcessouMensagemInter_(msg) {
  return existeRegistroDriveInter_(montarChaveMensagemInter_(msg));
}

function lerRegistroDriveInter_(fileName) {
  const folder = DriveApp.getFolderById(INTER_DRIVE_FOLDER_ID_LOG);
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

function isStatusBloqueanteInter_(status) {
  return [
    "processado",
    "duplicado-drive",
    "duplicado-gc"
  ].indexOf(String(status || "").trim()) >= 0;
}

function existeRegistroDriveInter_(fileName) {
  const reg = lerRegistroDriveInter_(fileName);
  if (!reg) return false;

  if (reg._arquivo_legado) {
    return true;
  }

  const status = String(reg.status || "processado").trim();
  return isStatusBloqueanteInter_(status);
}

function gravarRegistroMensagemInterNoDrive_(msg, extraido, clienteInfo, status, idComprovante) {
  const fileName = montarChaveMensagemInter_(msg);

  gravarRegistroInterNoDrive_(fileName, {
    tipo_registro: "message_id",
    status: status || "processado",
    id_comprovante: idComprovante || null,
    message_id: String(msg.getId() || ""),
    subject: safeSubjectInter_(msg),
    from: msg.getFrom(),
    date: msg.getDate() ? msg.getDate().toISOString() : null,
    id_transacao: extraido && extraido.id_transacao ? extraido.id_transacao : null,
    tipo_pix: extraido && extraido.tipo_pix ? extraido.tipo_pix : null,
    data_pagamento: extraido && extraido.data_pagamento ? extraido.data_pagamento : null,
    valor: extraido && extraido.valor != null ? Number(extraido.valor) : null,
    nome_pagador: extraido && extraido.nome_pagador ? extraido.nome_pagador : null,
    cliente_oficial: clienteInfo && clienteInfo.cliente ? clienteInfo.cliente : null,
    processado_em: new Date().toISOString()
  });
}

function gravarRegistroInterNoDrive_(fileName, obj) {
  const folder = DriveApp.getFolderById(INTER_DRIVE_FOLDER_ID_LOG);
  const files = folder.getFilesByName(fileName);

  const payload = JSON.stringify(obj, null, 2);

  if (files.hasNext()) {
    const file = files.next();
    file.setContent(payload);
    return;
  }

  folder.createFile(fileName, payload, MimeType.PLAIN_TEXT);
}

function marcarRegistroDriveComoCanceladoManualGCInter_(fileName, motivo) {
  const atual = lerRegistroDriveInter_(fileName);
  if (!atual) return false;

  atual.status = "cancelado_manual_gc";
  atual.cancelado_manual_gc_em = new Date().toISOString();
  atual.cancelado_manual_gc_motivo = String(motivo || "").trim();

  gravarRegistroInterNoDrive_(fileName, atual);
  return true;
}

function gerarIdComprovanteInter_(msg, extraido) {
  const dt = msg.getDate();
  const dataHora = Utilities.formatDate(dt, Session.getScriptTimeZone(), "yyyyMMdd_HHmmss");
  const valor = extraido && extraido.valor != null ? Number(extraido.valor).toFixed(2).replace(".", "") : "000";
  const msgId = String(msg.getId() || "").replace(/[^a-zA-Z0-9]/g, "").slice(-10) || "MSG";
  const prefixo = extraido && extraido.id_transacao ? "INTERTX" : "INTER";
  return prefixo + "_" + dataHora + "_" + valor + "_" + msgId;
}

function salvarJsonClienteInter_(clienteOficial, extraido, msg, idComprovante, registroBase) {
  const pastaMes = garantirEstruturaPastaClienteInter_(clienteOficial, extraido.data_pagamento);
  const nomeArquivo = montarNomeArquivoJsonInter_(clienteOficial, extraido, idComprovante);

  const payload = Object.assign({}, registroBase, {
    cliente_pasta: clienteOficial,
    nome_arquivo_origem: safeSubjectInter_(msg)
  });

  const files = pastaMes.getFilesByName(nomeArquivo);
  const content = JSON.stringify(payload, null, 2);

  if (files.hasNext()) {
    files.next().setContent(content);
    return;
  }

  pastaMes.createFile(nomeArquivo, content, MimeType.PLAIN_TEXT);
}

function garantirEstruturaPastaClienteInter_(clienteOficial, dataIso) {
  const raiz = DriveApp.getFolderById(INTER_DRIVE_FOLDER_ID_COMPROVANTES);
  const nomeCliente = sanitizeFolderNameInter_(clienteOficial);
  const data = new Date(dataIso + "T12:00:00");

  const ano = Utilities.formatDate(data, Session.getScriptTimeZone(), "yyyy");
  const mes = nomeMesPtInter_(data);

  const pastaCliente = getOrCreateSubFolderInter_(raiz, nomeCliente);
  const pastaAno = getOrCreateSubFolderInter_(pastaCliente, ano);
  const pastaMes = getOrCreateSubFolderInter_(pastaAno, mes);

  return pastaMes;
}

function getOrCreateSubFolderInter_(parent, nome) {
  const it = parent.getFoldersByName(nome);
  if (it.hasNext()) return it.next();
  return parent.createFolder(nome);
}

function montarNomeArquivoJsonInter_(clienteOficial, extraido, idComprovante) {
  return [
    sanitizeFileNameInter_(clienteOficial),
    extraido.data_pagamento,
    Number(extraido.valor).toFixed(2),
    sanitizeFileNameInter_(idComprovante)
  ].join("__") + ".json";
}

/************************************************************
 * UTILITÁRIOS
 ************************************************************/
function getScriptPropOrThrowInter_(key) {
  const v = PropertiesService.getScriptProperties().getProperty(key);
  if (!v) throw new Error("Falta Script Property: " + key);
  return v;
}

function notifyInter_(msg, title) {
  try {
    SpreadsheetApp.getUi().alert(title || "Inter", msg, SpreadsheetApp.getUi().ButtonSet.OK);
  } catch (e) {
    Logger.log((title || "Inter") + ": " + msg);
  }
}

function gcNormInter_(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function limparTextoInter_(s) {
  return String(s || "")
    .replace(/\r/g, "")
    .replace(/[ \t]+/g, " ")
    .replace(/\n{3,}/g, "\n\n")
    .trim()
    .substring(0, 15000);
}

function limparNomeExtraidoInter_(nome) {
  let s = String(nome || "").trim();
  s = s.replace(/\b\d{2}\/\d{2}\b/g, " ");
  s = s.replace(/\s+/g, " ").trim();
  return s;
}

function safeSubjectInter_(msg) {
  try {
    return String(msg.getSubject() || "").trim();
  } catch (e) {
    return "";
  }
}

function formatarMoedaInter_(v) {
  const n = Number(v);
  if (!isFinite(n)) return "";
  return "R$ " + n.toFixed(2);
}

function formatValorChaveInter_(v) {
  const n = Number(v);
  if (!isFinite(n)) return "";
  return n.toFixed(2);
}

function tokensNomeInter_(s) {
  return String(s || "")
    .split(/[^a-z0-9]+/)
    .map(x => x.trim())
    .filter(Boolean);
}

function intersecTokensInter_(a, b) {
  const mb = {};
  b.forEach(x => mb[x] = true);
  return a.filter(x => mb[x]);
}

function uniaoTokensInter_(a, b) {
  const m = {};
  a.forEach(x => m[x] = true);
  b.forEach(x => m[x] = true);
  return Object.keys(m);
}

function sanitizeFileNameInter_(s) {
  return String(s || "")
    .replace(/[\\\/:*?"<>|#%{}~&]/g, "_")
    .replace(/\s+/g, "_")
    .substring(0, 180);
}

function sanitizeFolderNameInter_(s) {
  return String(s || "")
    .replace(/[\\\/:*?"<>|#%{}~&]/g, "_")
    .replace(/\s+/g, " ")
    .trim()
    .substring(0, 120);
}

function nomeMesPtInter_(data) {
  const meses = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
  ];
  return meses[data.getMonth()];
}

function deveIgnorarPagadorInter_(nomePagador) {
  const n = gcNormInter_(nomePagador);
  if (!n) return false;
  return INTER_EXCECOES_PAGADOR_IGNORAR.some(function(x) {
    return n.includes(gcNormInter_(x));
  });
}

function listarMensagensInterPorPeriodo_(periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();

  if (tipo === "datas_especificas") {
    const datas = Array.isArray(periodo.datas) ? periodo.datas : [];
    return listarMensagensInterDatasEspecificas_(datas);
  }

  const dias = getPeriodoDias_(periodo);
  return listarMensagensInterJanelaComDias_(dias);
}

function listarMensagensInterDatasEspecificas_(datasIso) {
  const tz = Session.getScriptTimeZone();
  const datas = Array.isArray(datasIso) ? datasIso.filter(Boolean) : [];

  if (!datas.length) return [];

  const sorted = datas.slice().sort();
  const primeira = sorted[0];
  const ultima = sorted[sorted.length - 1];

  const inicio = new Date(primeira + "T00:00:00");
  const fim = new Date(ultima + "T00:00:00");
  fim.setDate(fim.getDate() + 1);

  const afterStr = Utilities.formatDate(inicio, tz, "yyyy/MM/dd");
  const beforeStr = Utilities.formatDate(fim, tz, "yyyy/MM/dd");

  const datasPermitidas = {};
  datas.forEach(function(d) { datasPermitidas[String(d)] = true; });

  const query = [
    "after:" + afterStr,
    "before:" + beforeStr,
    "from:no-reply@inter.co",
    '(subject:"Pagamento Pix recebido" OR "Você recebeu um Pix")'
  ].join(" ");

  const threads = GmailApp.search(query, 0, 200);
  const out = [];

  for (const thread of threads) {
    const msgs = thread.getMessages();

    for (const msg of msgs) {
      const dataMsg = Utilities.formatDate(msg.getDate(), tz, "yyyy-MM-dd");
      if (!datasPermitidas[dataMsg]) continue;

      out.push({
        thread: thread,
        message: msg
      });
    }
  }

  out.sort(function(a, b) {
    return a.message.getDate().getTime() - b.message.getDate().getTime();
  });

  return out;
}