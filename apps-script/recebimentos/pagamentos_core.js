/************************************************************
 * PAGAMENTOS CORE
 * Primeira fase:
 * - Inter via Gmail
 * - pré-visualização / associação / confirmação
 * - preenchimento da área "Contas a Pagar" na Central de Controle API
 * - logs de duplicidade em Drive + verificação no GC
 * - MAPA_PAGAMENTOS para aliases -> plano de contas
 ************************************************************/

const PAY_ABA_MAPA = "MAPA_PAGAMENTOS";
const PAY_ABA_CENTRAL = INTER_ABA_CENTRAL;

const PAY_INTER_CONTA_OFICIAL = "Inter Empresas";
const PAY_ITAU_CONTA_OFICIAL = "Itaú Empresas";
const PAY_FORMA_PADRAO = "PIX";
const PAY_QUITADO_PADRAO = "Sim";
const PAY_PLANO_SALARIO_PHILIPE = "Salario Philipe";
const PAY_PLANO_SALARIO_LUCCA = "Salario Lucca";
const PAY_PLANO_ENTREGA_MOTOBOY = "Entrega Motoboy";

const PAY_ROW_START = 93;
const PAY_ROW_END = 120;
const PAY_STEP = 3;

const PAY_INTER_OPENAI_MODEL = "gpt-4.1";
const PAY_LOG_FOLDER_ID = INTER_DRIVE_FOLDER_ID_LOG;

const PAY_ENDPOINT_PLANOS_1 = "/api/planos_contas";
const PAY_ENDPOINT_PLANOS_2 = "/planos_contas";
const PAY_ENDPOINT_FORMAS_1 = "/api/formas_pagamentos";
const PAY_ENDPOINT_FORMAS_2 = "/formas_pagamentos";
const PAY_ENDPOINT_CONTAS_1 = "/api/contas_bancarias";
const PAY_ENDPOINT_CONTAS_2 = "/contas_bancarias";
const PAY_ENDPOINT_PAGAMENTOS_1 = "/api/pagamentos";
const PAY_ENDPOINT_PAGAMENTOS_2 = "/pagamentos";

const PAY_GMAIL_LABEL_PROCESSADO = "pay-processado";
const PAY_GMAIL_LABEL_DUPLICADO = "pay-duplicado";
const PAY_GMAIL_LABEL_ERRO = "pay-erro";
const PAY_GMAIL_LABEL_IGNORADO = "pay-ignorado";
const PAY_GMAIL_QUERY_EXTRATO_INTER = "from:no-reply@inter.co has:attachment newer_than:180d";
const PAY_EXTRATO_CACHE_PREFIX = "PAYINTER_EXTRATO_PARSE_V2__";
const PAY_EXTRATO_CACHE_TYPE = "inter_pdf_pagamentos_periodo_v2";
const PAY_INTER_EXTRATO_DRIVE_FOLDER_ID = "1oJCd0cfQeU5Z5v0UaYvI1Cjr9H7psagD";

const PAY_IGNORE_NAMES = [
  "lp comercio",
  "lp comercio de cosmeticos",
  "lp comercio de cosmeticos ltda",
  "lp comercio de cosm"
];

const PAY_DIRECT_PLAN_RULES = [
  { match: "philipe", plano: PAY_PLANO_SALARIO_PHILIPE },
  { match: "lucca", plano: PAY_PLANO_SALARIO_LUCCA }
];

const PAY_MOTOBOY_HINTS = [
  "99",
  "99app",
  "99 app",
  "99pay",
  "99 pay"
];

const PAY_PHILIPE_PERSONAL_RULES = [
  {
    motivo: "alimentacao",
    hints: [
      "ifood",
      "i food",
      "mcdonalds",
      "mc donalds",
      "burger king",
      "bk",
      "subway",
      "habibs",
      "giraffas",
      "restaurante",
      "lanchonete",
      "hamburgueria",
      "pizzaria",
      "padaria",
      "cafeteria",
      "sorveteria",
      "churrascaria",
      "temakeria",
      "sushi",
      "acai",
      "acougue"
    ]
  },
  {
    motivo: "combustivel",
    hints: [
      "posto",
      "combustivel",
      "gasolina",
      "etanol",
      "shell",
      "ipiranga",
      "petrobras",
      "ale combustiveis",
      "br mania"
    ]
  },
  {
    motivo: "cinema",
    hints: [
      "cinema",
      "cinemark",
      "cinepolis",
      "cinesystem",
      "uci"
    ]
  },
  {
    motivo: "shopping",
    hints: [
      "shopping",
      "mall"
    ]
  },
  {
    motivo: "estacionamento",
    hints: [
      "estacionamento",
      "estapar",
      "indigo",
      "zona azul",
      "park"
    ]
  }
];

const PAY_CNPJ_HINTS = [
  "ltda",
  "eireli",
  "mei",
  "me ",
  "mei ",
  "sa ",
  "s a",
  "s/a",
  "comercio",
  "comércio",
  "industria",
  "indústria",
  "distribuidora",
  "importadora"
];

function garantirMapaPagamentosSheet_() {
  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  let sh = ss.getSheetByName(PAY_ABA_MAPA);

  if (!sh) {
    sh = ss.insertSheet(PAY_ABA_MAPA);
  }

  const headers = [
    "nome_base_1",
    "nome_base_2",
    "nome_base_3",
    "nome_base_4",
    "nome_base_5",
    "nome_base_6",
    "nome_base_7",
    "nome_base_8",
    "nome_base_9",
    "nome_base_10",
    "plano_contas",
    "acao",
    "ativo",
    "observacao"
  ];

  sh.getRange(1, 1, 1, headers.length).setValues([headers]);
  sh.setFrozenRows(1);
  sh.getRange(1, 1, 1, headers.length).setFontWeight("bold");

  if (sh.getMaxColumns() < headers.length) {
    sh.insertColumnsAfter(sh.getMaxColumns(), headers.length - sh.getMaxColumns());
  }

  return sh;
}

function garantirInfraPagamentos_() {
  garantirMapaPagamentosSheet_();
  return {
    ok: true,
    message: 'Aba "' + PAY_ABA_MAPA + '" garantida com sucesso.'
  };
}

function garantirMapaPagamentos() {
  return garantirInfraPagamentos_();
}

function listarMapaPagamentos_() {
  const sh = garantirMapaPagamentosSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const vals = sh.getRange(2, 1, lastRow - 1, 14).getDisplayValues();
  const out = [];

  for (let i = 0; i < vals.length; i++) {
    const row = vals[i];
    const aliases = [];

    for (let c = 0; c < 10; c++) {
      aliases.push(String(row[c] || "").trim());
    }

    const plano = String(row[10] || "").trim();
    const acao = payNorm_(row[11] || "") || "usar";
    const ativo = payNorm_(row[12] || "");
    const observacao = String(row[13] || "").trim();

    if (!plano && !aliases.some(Boolean)) continue;
    if (ativo && !["sim", "s", "ativo", "1", "true"].includes(ativo)) continue;

    out.push({
      rowNumber: i + 2,
      nome_bases: aliases.filter(Boolean),
      plano_contas: plano,
      acao: acao || "usar",
      observacao: observacao
    });
  }

  return out;
}

function payUpsertAliasNoMapa_(nomeExtraido, planoContas, acao, observacao) {
  const nome = String(nomeExtraido || "").trim();
  const plano = String(planoContas || "").trim();
  if (!nome || !plano) {
    return {
      ok: false,
      message: "nomeExtraido e planoContas são obrigatórios."
    };
  }

  const sh = garantirMapaPagamentosSheet_();
  const mapa = listarMapaPagamentos_();
  const planoNorm = payNorm_(plano);

  let rowInfo = mapa.find(function(item) {
    return payNorm_(item.plano_contas) === planoNorm;
  });

  if (!rowInfo) {
    const row = Math.max(2, sh.getLastRow() + 1);
    sh.getRange(row, 11).setValue(plano);
    sh.getRange(row, 12).setValue(String(acao || "usar"));
    sh.getRange(row, 13).setValue("Sim");
    if (observacao) sh.getRange(row, 14).setValue(String(observacao));
    sh.getRange(row, 1).setValue(nome);

    return {
      ok: true,
      message: "Associação criada no MAPA_PAGAMENTOS.",
      row: row
    };
  }

  const vals = sh.getRange(rowInfo.rowNumber, 1, 1, 10).getDisplayValues()[0];
  const nomeNorm = payNorm_(nome);

  for (let i = 0; i < vals.length; i++) {
    if (payNorm_(vals[i] || "") === nomeNorm) {
      return {
        ok: true,
        message: "Associação já existia no MAPA_PAGAMENTOS.",
        row: rowInfo.rowNumber
      };
    }
  }

  for (let j = 0; j < vals.length; j++) {
    if (!String(vals[j] || "").trim()) {
      sh.getRange(rowInfo.rowNumber, j + 1).setValue(nome);
      if (acao) sh.getRange(rowInfo.rowNumber, 12).setValue(String(acao));
      if (observacao && !String(sh.getRange(rowInfo.rowNumber, 14).getValue() || "").trim()) {
        sh.getRange(rowInfo.rowNumber, 14).setValue(String(observacao));
      }

      return {
        ok: true,
        message: "Associação salva no MAPA_PAGAMENTOS.",
        row: rowInfo.rowNumber
      };
    }
  }

  return {
    ok: false,
    message: 'Não encontrei alias vazio para o plano "' + plano + '" no MAPA_PAGAMENTOS.'
  };
}

function payListAllWithFallback_(endpoint1, endpoint2, cacheKey) {
  const cache = CacheService.getScriptCache();
  const cached = cache.get(cacheKey);
  if (cached) {
    try {
      return JSON.parse(cached);
    } catch (e) {}
  }

  let lista = [];
  try {
    lista = payListAll_(endpoint1);
  } catch (e) {
    const msg = String(e || "");
    if (msg.indexOf("HTTP 404") < 0 || !endpoint2) throw e;
    lista = payListAll_(endpoint2);
  }

  cache.put(cacheKey, JSON.stringify(lista), 21600);
  return lista;
}

function payListAll_(endpoint) {
  const out = [];
  let pagina = 1;
  const limit = 100;

  while (true) {
    const raw = jreq_("GET", endpoint, {
      params: {
        pagina: pagina,
        limit: limit
      }
    });

    const arr = payNormalizeList_(raw);
    if (!arr.length) break;

    out.push.apply(out, arr);
    if (arr.length < limit) break;

    pagina += 1;
    Utilities.sleep(120);
  }

  return out;
}

function payNormalizeList_(raw) {
  const base = Array.isArray(raw)
    ? raw
    : raw && typeof raw === "object" && Array.isArray(raw.data)
    ? raw.data
    : [];

  return base.map(payUnwrapGcItem_).filter(Boolean);
}

function payUnwrapGcItem_(item) {
  return (
    item && item.FormasPagamento ||
    item && item.formasPagamento ||
    item && item.FormaPagamento ||
    item && item.PlanoConta ||
    item && item.PlanosConta ||
    item && item.ContasBancaria ||
    item && item.ContaBancaria ||
    item && item.contaBancaria ||
    item && item.Pagamento ||
    item && item.pagamento ||
    item
  );
}

function payListarPlanosContasGC_() {
  return payListAllWithFallback_(
    PAY_ENDPOINT_PLANOS_1,
    PAY_ENDPOINT_PLANOS_2,
    "GC_PAY_PLANOS_CONTAS_JSON"
  );
}

function payListarContasBancariasGC_() {
  return payListAllWithFallback_(
    PAY_ENDPOINT_CONTAS_1,
    PAY_ENDPOINT_CONTAS_2,
    "GC_PAY_CONTAS_BANCARIAS_JSON"
  );
}

function payListarFormasPagamentoGC_() {
  return payListAllWithFallback_(
    PAY_ENDPOINT_FORMAS_1,
    PAY_ENDPOINT_FORMAS_2,
    "GC_PAY_FORMAS_PAGAMENTO_JSON"
  );
}

function payGetContaBancariaIdPorNome_(nomeConta) {
  const alvo = payNorm_(nomeConta);
  if (!alvo) throw new Error("Nome da conta bancária vazio.");

  const contas = payListarContasBancariasGC_();
  if (!contas.length) {
    throw new Error("Nenhuma conta bancária retornada pela API do GC.");
  }

  let exata = contas.find(function(c) {
    return payNorm_(c && (c.nome || c.descricao || c.banco) || "") === alvo;
  });
  if (exata && exata.id) return String(exata.id);

  let parcial = contas.find(function(c) {
    const n = payNorm_(c && (c.nome || c.descricao || c.banco) || "");
    return n.indexOf(alvo) >= 0 || alvo.indexOf(n) >= 0;
  });
  if (parcial && parcial.id) return String(parcial.id);

  throw new Error('Conta bancária não encontrada no GC: "' + nomeConta + '"');
}

function payGetFormaPagamentoIdPix_() {
  const formas = payListarFormasPagamentoGC_();
  const alvo = "pix";

  for (let i = 0; i < formas.length; i++) {
    const nome = payNorm_(formas[i].nome || "");
    const desc = payNorm_(formas[i].descricao || "");
    if (nome === alvo || desc === alvo) return String(formas[i].id);
  }

  for (let j = 0; j < formas.length; j++) {
    const nome2 = payNorm_(formas[j].nome || "");
    const desc2 = payNorm_(formas[j].descricao || "");
    if (nome2.indexOf(alvo) >= 0 || desc2.indexOf(alvo) >= 0) {
      return String(formas[j].id);
    }
  }

  throw new Error('Forma de pagamento "PIX" não encontrada no GC.');
}

function payListarNomesPlanosGC_() {
  const planos = payListarPlanosContasGC_();
  const out = [];
  const seen = {};

  for (let i = 0; i < planos.length; i++) {
    const nome = String(
      planos[i].nome || planos[i].descricao || planos[i].titulo || ""
    ).trim();
    const key = payNorm_(nome);

    if (!nome || !key || seen[key]) continue;
    seen[key] = true;
    out.push(nome);
  }

  out.sort();
  return out;
}

function payEscolherAnexoPdfExtratoInter_(anexos) {
  const pdfs = [];

  for (let i = 0; i < (anexos || []).length; i++) {
    const anexo = anexos[i];
    const mime = String(anexo.getContentType() || "").toLowerCase();
    const nome = String(anexo.getName() || "").toLowerCase();

    if (mime === "application/pdf" || /\.pdf$/i.test(nome)) {
      pdfs.push(anexo);
    }
  }

  if (!pdfs.length) return null;

  for (let j = 0; j < pdfs.length; j++) {
    if (payNorm_(pdfs[j].getName() || "").indexOf("extrato") >= 0) {
      return pdfs[j];
    }
  }

  return pdfs[0];
}

function payPareceEmailExtratoInter_(msg, anexo) {
  const assunto = payNorm_(safeSubjectInter_(msg));
  const corpo = payNorm_(limparTextoInter_(msg.getPlainBody() || ""));
  const nome = payNorm_((anexo && anexo.getName()) || "");
  const texto = [assunto, corpo, nome].join(" ").trim();

  return (
    nome.indexOf("extrato") >= 0 ||
    texto.indexOf("extrato") >= 0 ||
    texto.indexOf("saldo total") >= 0 ||
    texto.indexOf("saldo disponivel") >= 0 ||
    texto.indexOf("periodo") >= 0
  );
}

function payLerTextoAnexoInter_(anexo) {
  const blob = anexo.copyBlob();
  let texto = "";

  try {
    texto = blob.getDataAsString("UTF-8");
  } catch (e) {}

  if (!texto || texto.indexOf("\uFFFD") >= 0) {
    try {
      texto = blob.getDataAsString("ISO-8859-1");
    } catch (e) {}
  }

  return String(texto || "")
    .replace(/^\uFEFF/, "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
}

function payGetBlobFromSource_(source) {
  if (!source) {
    throw new Error("Fonte do extrato vazia.");
  }

  if (typeof source.copyBlob === "function") {
    return source.copyBlob();
  }

  if (typeof source.getBlob === "function") {
    return source.getBlob();
  }

  throw new Error("Fonte do extrato sem método de blob compatível.");
}

function payExtrairPeriodoDoNomeArquivoInter_(texto) {
  const raw = String(texto || "");
  const m = raw.match(
    /(\d{2})[-_/](\d{2})[-_/](\d{4}).{0,20}?(?:\ba\b|ate|até).{0,20}?(\d{2})[-_/](\d{2})[-_/](\d{4})/i
  );

  if (!m) return null;

  return {
    inicio: [m[3], m[2], m[1]].join("-"),
    fim: [m[6], m[5], m[4]].join("-")
  };
}

function payScoreCandidatoExtratoInter_(periodoArquivo, datasPermitidas, msgDate) {
  const datas = Object.keys(datasPermitidas || {}).filter(Boolean).sort();
  const baseTime = msgDate && msgDate.getTime ? msgDate.getTime() : 0;

  if (!datas.length) return 500000000000 + baseTime;

  if (!periodoArquivo || !periodoArquivo.inicio || !periodoArquivo.fim) {
    return 250000000000 + baseTime;
  }

  let cobertura = 0;
  for (let i = 0; i < datas.length; i++) {
    if (datas[i] >= periodoArquivo.inicio && datas[i] <= periodoArquivo.fim) {
      cobertura += 1;
    }
  }

  if (!cobertura) return -1;

  const cobreTudo = cobertura === datas.length;
  return (cobreTudo ? 900000000000 : 600000000000) + (cobertura * 1000000) + baseTime;
}

function payListarCandidatosExtratoInterDrive_(periodo) {
  const datasPermitidas = payMontarJanelaDatasPorPeriodo_(periodo);
  const out = [];

  try {
    const folder = DriveApp.getFolderById(PAY_INTER_EXTRATO_DRIVE_FOLDER_ID);
    const files = folder.getFiles();

    while (files.hasNext()) {
      const file = files.next();
      const nome = String(file.getName() || "");
      const mime = String(file.getMimeType() || "").toLowerCase();

      if (mime !== "application/pdf" && !/\.pdf$/i.test(nome)) continue;
      if (payNorm_(nome).indexOf("extrato") < 0) continue;

      const periodoArquivo = payExtrairPeriodoDoNomeArquivoInter_(nome);
      const score = payScoreCandidatoExtratoInter_(
        periodoArquivo,
        datasPermitidas,
        file.getLastUpdated()
      );

      if (score < 0) continue;

      out.push({
        source_kind: "drive_folder_pdf",
        file: file,
        attachment: file,
        periodo_arquivo: periodoArquivo,
        score: score,
        message: null,
        assunto_email: "[Drive] " + nome,
        remetente: "Drive",
        message_id: "drive:" + String(file.getId() || ""),
        attachment_name: nome
      });
    }
  } catch (e) {
    Logger.log("Falha ao listar extratos do Inter na pasta do Drive: " + e);
  }

  out.sort(function(a, b) {
    return Number(b.score || 0) - Number(a.score || 0);
  });

  return out;
}

function payListarCandidatosExtratoInter_(periodo) {
  const datasPermitidas = payMontarJanelaDatasPorPeriodo_(periodo);
  const threads = GmailApp.search(PAY_GMAIL_QUERY_EXTRATO_INTER, 0, 80);
  const out = [];

  for (let t = 0; t < threads.length; t++) {
    const msgs = threads[t].getMessages();

    for (let m = 0; m < msgs.length; m++) {
      const msg = msgs[m];
      const anexos = msg.getAttachments({
        includeInlineImages: false,
        includeAttachments: true
      }) || [];

      const anexoPdf = payEscolherAnexoPdfExtratoInter_(anexos);
      if (!anexoPdf) continue;
      if (!payPareceEmailExtratoInter_(msg, anexoPdf)) continue;

      const periodoArquivo = payExtrairPeriodoDoNomeArquivoInter_(
        String(anexoPdf.getName() || "") + " " + safeSubjectInter_(msg)
      );
      const score = payScoreCandidatoExtratoInter_(
        periodoArquivo,
        datasPermitidas,
        msg.getDate()
      );

      if (score < 0) continue;

      out.push({
        source_kind: "gmail_pdf",
        message: msg,
        attachment: anexoPdf,
        periodo_arquivo: periodoArquivo,
        score: score,
        assunto_email: safeSubjectInter_(msg),
        remetente: msg.getFrom(),
        message_id: String(msg.getId() || ""),
        attachment_name: String(anexoPdf.getName() || "")
      });
    }
  }

  out.sort(function(a, b) {
    return Number(b.score || 0) - Number(a.score || 0);
  });

  return out;
}

function payDigestHex_(texto) {
  const digest = Utilities.computeDigest(
    Utilities.DigestAlgorithm.MD5,
    String(texto || ""),
    Utilities.Charset.UTF_8
  );

  return digest
    .map(function(b) {
      const n = b < 0 ? b + 256 : b;
      const hx = n.toString(16);
      return hx.length === 1 ? "0" + hx : hx;
    })
    .join("");
}

function payBuildPdfExtratoCacheFileName_(msg, anexo, periodo) {
  const chave = payDigestHex_(
    [
      String(msg && msg.getId ? msg.getId() : ""),
      String(anexo && anexo.getName ? anexo.getName() : ""),
      JSON.stringify(payNormalizarPeriodoRetorno_(periodo || {}))
    ].join("__")
  );

  return PAY_EXTRATO_CACHE_PREFIX + sanitizeFileNameInter_(chave) + ".json";
}

function payDescreverDatasPermitidasPrompt_(datasPermitidas) {
  const datas = Object.keys(datasPermitidas || {}).filter(Boolean).sort();

  if (!datas.length) {
    return "Considere apenas o dia de hoje no fuso horário do projeto.";
  }

  if (datas.length <= 14) {
    return "Considere somente estas datas ISO: " + datas.join(", ") + ".";
  }

  return (
    "Considere somente o intervalo de " +
    datas[0] +
    " até " +
    datas[datas.length - 1] +
    "."
  );
}

function payAnalisarExtratoInterPdfComOpenAI_(anexoPdf, datasPermitidas) {
  const apiKey = getScriptPropOrThrowInter_("OPENAI_API_KEY");
  const blob = payGetBlobFromSource_(anexoPdf);
  const nomeArquivo = String(anexoPdf.getName() || "extrato_inter.pdf");
  const mime = String(blob.getContentType() || "application/pdf").toLowerCase();
  const base64 = Utilities.base64Encode(blob.getBytes());
  const dataUrl = "data:" + mime + ";base64," + base64;

  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      extrato_detectado: { type: "boolean" },
      banco: { type: ["string", "null"] },
      periodo_inicial: { type: ["string", "null"] },
      periodo_final: { type: ["string", "null"] },
      pagamentos: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            tipo_pagamento: { type: ["string", "null"] },
            data_pagamento: { type: ["string", "null"] },
            nome_favorecido: { type: ["string", "null"] },
            descricao_pagamento: { type: ["string", "null"] },
            cpf_cnpj: { type: ["string", "null"] },
            valor: { type: ["number", "null"] },
            id_transacao: { type: ["string", "null"] },
            linha_resumo: { type: ["string", "null"] }
          },
          required: [
            "tipo_pagamento",
            "data_pagamento",
            "nome_favorecido",
            "descricao_pagamento",
            "cpf_cnpj",
            "valor",
            "id_transacao",
            "linha_resumo"
          ]
        }
      },
      observacoes: { type: ["string", "null"] }
    },
    required: [
      "extrato_detectado",
      "banco",
      "periodo_inicial",
      "periodo_final",
      "pagamentos",
      "observacoes"
    ]
  };

  const prompt = [
    "Analise o PDF anexado.",
    "Ele deve ser um extrato bancário do Banco Inter.",
    payDescreverDatasPermitidasPrompt_(datasPermitidas),
    "",
    "Extraia somente SAÍDAS efetivas de dinheiro da conta.",
    "Considere como pagamentos válidos apenas:",
    '- "Compra no debito"',
    '- "Pix enviado"',
    '- "Pix QR-Code" somente se for claramente um pagamento/saída',
    "",
    "Ignore totalmente:",
    '- "Pix recebido"',
    '- "Estorno"',
    "- saldo do dia, saldo total, cabeçalhos, rodapés e linhas informativas",
    "- compras ou pagamentos que foram claramente revertidos por estorno no próprio extrato",
    "",
    "Regras de saída:",
    "- Retorne APENAS JSON válido seguindo o schema.",
    "- Se não for um extrato do Banco Inter, use extrato_detectado=false e pagamentos=[].",
    '- tipo_pagamento deve ser "debito_compra", "pix_enviado" ou null.',
    "- data_pagamento deve vir em formato ISO YYYY-MM-DD.",
    "- valor deve ser número positivo, sem sinal e sem símbolo.",
    "- nome_favorecido deve ser o melhor nome possível do estabelecimento ou beneficiário.",
    "- descricao_pagamento deve preservar a descrição original útil da linha.",
    "- cpf_cnpj pode ser null se não aparecer.",
    "- id_transacao pode ser null se não aparecer.",
    "- linha_resumo deve ser uma versão curta da linha original do extrato.",
    "",
    "Padrões comuns no Inter:",
    '- Compra no debito: "No estabelecimento ..."',
    '- Pix enviado: "Cp :...-NOME"',
    '- Pix recebido: "Cp :...-NOME"'
  ].join("\n");

  const payload = {
    model: PAY_INTER_OPENAI_MODEL,
    input: [
      {
        role: "user",
        content: [
          { type: "input_text", text: prompt },
          {
            type: "input_file",
            filename: nomeArquivo,
            file_data: dataUrl
          }
        ]
      }
    ],
    text: {
      format: {
        type: "json_schema",
        name: "captura_pagamentos_extrato_inter_pdf",
        strict: true,
        schema: schema
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
    throw new Error("OpenAI HTTP " + code + " ao analisar PDF do Inter: " + body);
  }

  const js = JSON.parse(body);
  const texto = extrairTextoOpenAIInter_(js);
  if (!texto) {
    throw new Error("A OpenAI respondeu sem texto utilizável para o extrato do Inter.");
  }

  const out = JSON.parse(texto);
  out.pagamentos = Array.isArray(out.pagamentos) ? out.pagamentos : [];

  out.pagamentos = out.pagamentos
    .map(function(item) {
      return {
        tipo_pagamento: payNormalizarTipoPagamentoInter_(item.tipo_pagamento || "") || null,
        data_pagamento: String(item.data_pagamento || "").trim() || null,
        nome_favorecido: String(item.nome_favorecido || "").trim() || null,
        descricao_pagamento: String(item.descricao_pagamento || "").trim() || null,
        cpf_cnpj: String(item.cpf_cnpj || "").trim() || null,
        valor: item.valor != null ? Math.abs(Number(item.valor)) : null,
        id_transacao: String(item.id_transacao || "").trim() || null,
        linha_resumo: String(item.linha_resumo || "").trim() || null
      };
    })
    .filter(function(item) {
      return (
        item &&
        item.data_pagamento &&
        item.valor != null &&
        isFinite(Number(item.valor)) &&
        Number(item.valor) > 0 &&
        ["debito_compra", "pix_enviado"].indexOf(
          payNormalizarTipoPagamentoInter_(item.tipo_pagamento || "")
        ) >= 0
      );
    });

  return out;
}

function payValorCentavosExtratoTxt_(texto) {
  const digitos = String(texto || "").replace(/\D/g, "");
  if (!digitos) return null;

  const inteiro = Number(digitos);
  if (!isFinite(inteiro)) return null;
  return Number((inteiro / 100).toFixed(2));
}

function payTipoLinhaExtratoTxt_(linha) {
  const labels = [
    { label: "EST COMPRA CARTAO", tipo: "estorno_compra" },
    { label: "COMPRA CARTAO", tipo: "debito_compra" },
    { label: "PIX ENVIADO", tipo: "pix_enviado" },
    { label: "PIX RECEBIDO", tipo: "pix_recebido" }
  ];

  for (let i = 0; i < labels.length; i++) {
    const idx = linha.indexOf(labels[i].label);
    if (idx >= 0) {
      return {
        label: labels[i].label,
        tipo: labels[i].tipo,
        index: idx
      };
    }
  }

  return null;
}

function payDataLinhaExtratoTxt_(linha) {
  const m = String(linha || "").match(/S(\d{2})(\d{2})(\d{4})(\d{2})(\d{2})(\d{4})/);
  if (!m) return null;
  return [m[3], m[2], m[1]].join("-");
}

function payDescricaoTipoTxt_(tipo) {
  if (tipo === "debito_compra") return "Compra no débito";
  if (tipo === "pix_enviado") return "Pix enviado";
  if (tipo === "pix_recebido") return "Pix recebido";
  if (tipo === "estorno_compra") return "Estorno compra cartão";
  return String(tipo || "").trim();
}

function payAnalisarExtratoInterTxt_(anexoTxt, datasPermitidas) {
  const texto = payLerTextoAnexoInter_(anexoTxt);
  const linhas = texto.split("\n");
  const pagamentosBrutos = [];
  const estornos = {};

  for (let i = 0; i < linhas.length; i++) {
    const linha = String(linhas[i] || "").replace(/\u0000/g, "").trim();
    if (!linha || linha.indexOf("DPV00") < 0) continue;

    const tipoInfo = payTipoLinhaExtratoTxt_(linha);
    if (!tipoInfo) continue;

    const dataPagamento = payDataLinhaExtratoTxt_(linha);
    if (!dataPagamento || !datasPermitidas[dataPagamento]) continue;

    const prefixo = linha.slice(0, tipoInfo.index);
    const valorMatch = prefixo.match(/(\d{4,18})([DC])(\d{7})\s*$/);
    if (!valorMatch) continue;

    const valor = payValorCentavosExtratoTxt_(valorMatch[1]);
    const dc = valorMatch[2];
    const sufixo = linha.slice(tipoInfo.index + tipoInfo.label.length).trim();
    const idTransacao = String((sufixo.split(/\s+/)[0] || "")).trim();

    if (valor == null || !isFinite(valor) || valor <= 0) continue;

    if (tipoInfo.tipo === "pix_recebido") continue;

    if (tipoInfo.tipo === "estorno_compra") {
      const chaveEstorno = [
        "debito_compra",
        dataPagamento,
        payFormatValorChave_(valor)
      ].join("__");
      estornos[chaveEstorno] = Number(estornos[chaveEstorno] || 0) + 1;
      continue;
    }

    if (dc !== "D") continue;

    pagamentosBrutos.push({
      tipo_pagamento: tipoInfo.tipo,
      data_pagamento: dataPagamento,
      nome_favorecido: "",
      descricao_pagamento: payDescricaoTipoTxt_(tipoInfo.tipo),
      cpf_cnpj: null,
      valor: valor,
      id_transacao: idTransacao || null,
      linha_resumo: (tipoInfo.label + (sufixo ? " " + sufixo : "")).trim()
    });
  }

  const pagamentos = [];
  for (let j = 0; j < pagamentosBrutos.length; j++) {
    const item = pagamentosBrutos[j];
    const chave = [
      item.tipo_pagamento,
      item.data_pagamento,
      payFormatValorChave_(item.valor)
    ].join("__");

    if (item.tipo_pagamento === "debito_compra" && Number(estornos[chave] || 0) > 0) {
      estornos[chave] -= 1;
      continue;
    }

    pagamentos.push(item);
  }

  return {
    extrato_detectado:
      texto.indexOf("BANCO INTER S.A.") >= 0 || texto.indexOf("BANCO INTER S A") >= 0,
    banco: "Inter",
    periodo_inicial: null,
    periodo_final: null,
    pagamentos: pagamentos,
    observacoes: "Extrato Inter TXT analisado por parser determinístico."
  };
}

function payObterPagamentosDoMelhorExtratoInter_(periodo) {
  const candidatosDrive = payListarCandidatosExtratoInterDrive_(periodo);
  const candidatosGmail = payListarCandidatosExtratoInter_(periodo);
  const candidatos = candidatosDrive.length ? candidatosDrive : candidatosGmail;

  if (!candidatos.length) {
    return {
      ok: false,
      encontrado: false,
      message: "Nenhum extrato PDF do Inter encontrado nem na pasta do Drive nem no Gmail."
    };
  }

  const candidato = candidatos[0];
  const cacheFileName = payBuildPdfExtratoCacheFileName_(
    candidato.message,
    candidato.attachment,
    periodo
  );

  let parsed = payLerRegistroDrive_(cacheFileName);
  if (!parsed || parsed.cache_type !== PAY_EXTRATO_CACHE_TYPE) {
    parsed = payAnalisarExtratoInterPdfComOpenAI_(
      candidato.attachment,
      payMontarJanelaDatasPorPeriodo_(periodo)
    );
    parsed.cache_type = PAY_EXTRATO_CACHE_TYPE;
    parsed.cached_at = new Date().toISOString();
    payGravarRegistroNoDrive_(cacheFileName, parsed);
  }

  return {
    ok: true,
    encontrado: true,
    source_kind: candidato.source_kind || "pdf_extrato",
    message_id: String(candidato.message_id || ""),
    assunto_email: candidato.assunto_email || "[Extrato Inter PDF]",
    remetente: candidato.remetente || "",
    attachment_name: candidato.attachment_name || String(candidato.attachment.getName() || ""),
    periodo_arquivo: candidato.periodo_arquivo || null,
    pagamentos: Array.isArray(parsed.pagamentos) ? parsed.pagamentos : [],
    extrato_detectado: !!parsed.extrato_detectado,
    banco: parsed.banco || "Inter",
    observacoes: parsed.observacoes || "",
    origem_extrato: candidatosDrive.length ? "drive_folder_pdf" : "gmail_pdf",
    source_candidate_found: true
  };
}

function payBuildSyntheticTransactionId_(pagamento, sourceTag) {
  const base = [
    String(sourceTag || "inter"),
    String(pagamento.data_pagamento || ""),
    payFormatValorChave_(pagamento.valor),
    payNorm_(pagamento.tipo_pagamento || ""),
    payNorm_(pagamento.nome_favorecido || ""),
    payNorm_(pagamento.descricao_pagamento || "")
  ].join("__");

  return "PAYAUTO_" + payDigestHex_(base).slice(0, 24);
}

function payNormalizarTipoPagamentoInter_(tipo) {
  const t = payNorm_(tipo || "");

  if (
    [
      "debito_compra",
      "compra no debito",
      "compra no debito:",
      "compra debito",
      "debito compra"
    ].indexOf(t) >= 0
  ) {
    return "debito_compra";
  }

  if (
    [
      "pix_enviado",
      "pix enviado",
      "pix_realizado",
      "pix realizado",
      "pix_qrcode",
      "pix qrcode",
      "pix qr-code",
      "pix qr code"
    ].indexOf(t) >= 0
  ) {
    return "pix_enviado";
  }

  return t;
}

function payListarMensagensInterPagamentosPorPeriodo_(periodo) {
  garantirLabelInter_(PAY_GMAIL_LABEL_PROCESSADO);
  garantirLabelInter_(PAY_GMAIL_LABEL_DUPLICADO);
  garantirLabelInter_(PAY_GMAIL_LABEL_ERRO);
  garantirLabelInter_(PAY_GMAIL_LABEL_IGNORADO);

  const tz = Session.getScriptTimeZone();
  const datasPermitidas = payMontarJanelaDatasPorPeriodo_(periodo);
  const range = payBuildSearchWindow_(datasPermitidas);
  const afterStr = Utilities.formatDate(range.inicio, tz, "yyyy/MM/dd");
  const beforeStr = Utilities.formatDate(range.fimExclusivo, tz, "yyyy/MM/dd");

  const query = [
    "after:" + afterStr,
    "before:" + beforeStr,
    "from:no-reply@inter.co"
  ].join(" ");

  const threads = GmailApp.search(query, 0, 250);
  const out = [];

  for (let t = 0; t < threads.length; t++) {
    const thread = threads[t];
    const msgs = thread.getMessages();

    for (let m = 0; m < msgs.length; m++) {
      const msg = msgs[m];
      const dataMsg = Utilities.formatDate(msg.getDate(), tz, "yyyy-MM-dd");
      if (!datasPermitidas[dataMsg]) continue;

      if (!payPareceEmailPagamentoInter_(msg)) continue;

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

function payPareceEmailPagamentoInter_(msg) {
  const assunto = payNorm_(safeSubjectInter_(msg));
  const corpo = payNorm_(limparTextoInter_(msg.getPlainBody() || ""));
  const texto = assunto + " " + corpo;

  return (
    texto.indexOf("pix") >= 0 ||
    texto.indexOf("qrcode") >= 0 ||
    texto.indexOf("qr code") >= 0 ||
    texto.indexOf("debito") >= 0 ||
    texto.indexOf("débito") >= 0 ||
    texto.indexOf("pagamento") >= 0 ||
    texto.indexOf("compra") >= 0
  );
}

function payAnalisarEmailPagamentoInterComOpenAI_(msg) {
  const apiKey = getScriptPropOrThrowInter_("OPENAI_API_KEY");

  const assunto = safeSubjectInter_(msg);
  const remetente = msg.getFrom();
  const corpoTexto = limparTextoInter_(msg.getPlainBody() || "");
  const anexos = msg.getAttachments({ includeInlineImages: false, includeAttachments: true }) || [];

  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      eh_pagamento: { type: "boolean" },
      tipo_pagamento: { type: ["string", "null"] },
      nome_favorecido: { type: ["string", "null"] },
      cpf_cnpj: { type: ["string", "null"] },
      valor: { type: ["number", "null"] },
      descricao_pagamento: { type: ["string", "null"] },
      banco_email: { type: ["string", "null"] },
      id_transacao: { type: ["string", "null"] },
      confianca: { type: ["number", "null"] },
      observacoes: { type: ["string", "null"] }
    },
    required: [
      "eh_pagamento",
      "tipo_pagamento",
      "nome_favorecido",
      "cpf_cnpj",
      "valor",
      "descricao_pagamento",
      "banco_email",
      "id_transacao",
      "confianca",
      "observacoes"
    ]
  };

  const prompt = [
    "Analise o e-mail abaixo e, se houver, também o anexo.",
    "Seu objetivo é identificar pagamentos realizados pela empresa.",
    "Considere como válidos:",
    "- PIX enviado / PIX realizado",
    "- PIX QR-CODE",
    "- compra no débito",
    "",
    "Regras obrigatórias:",
    "- Retorne APENAS JSON válido seguindo o schema.",
    '- tipo_pagamento pode ser "pix_realizado", "pix_qrcode", "debito_compra" ou null.',
    "- nome_favorecido deve ser o melhor nome possível de quem recebeu o valor.",
    "- descricao_pagamento deve ser um resumo curto e útil do pagamento.",
    "- valor deve ser número decimal sem símbolo.",
    "- cpf_cnpj pode ser null se não aparecer.",
    "- eh_pagamento = true somente se o e-mail realmente indicar saída/pagamento.",
    "- Não trate PIX recebido como pagamento.",
    "",
    "Importante:",
    "- Para compra no débito, trate como pagamento válido.",
    "- Se houver dúvida, prefira não inventar.",
    "- Foque no favorecido/estabelecimento e no valor."
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
    model: PAY_INTER_OPENAI_MODEL,
    input: [
      {
        role: "user",
        content: contentParts
      }
    ],
    text: {
      format: {
        type: "json_schema",
        name: "captura_pagamento_email_inter",
        strict: true,
        schema: schema
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

function processarPagamentosV1_(payload) {
  const origem = String(payload.origem || "").toLowerCase().trim();
  const periodo = payload.periodo || {};

  if (!origem) {
    return {
      ok: false,
      message: "Origem não informada para pagamentos."
    };
  }

  if (origem === "inter") {
    return preVisualizarPagamentosInter_(periodo, payload.telegram || {}, payload.message_meta || {});
  }

  if (origem === "itau") {
    return {
      ok: false,
      message: "Pagamentos Itaú ainda não foram implementados nesta primeira fase."
    };
  }

  return {
    ok: false,
    message: 'Origem inválida. Use "inter" ou "itau".'
  };
}

function payProcessarPagamentoInterParaPrevia_(state, pagamento, meta) {
  const fakeMsg = {
    getId: function() {
      return String(meta.message_id || "");
    },
    getDate: function() {
      return new Date(String(pagamento.data_pagamento || "") + "T12:00:00");
    }
  };

  const regra = payAplicarRegrasEspeciais_(pagamento);
  if (regra.acao === "ignorar") {
    state.ignorados.push({
      motivo: regra.motivo || "ignorado_por_regra",
      nome_extraido: pagamento.nome_favorecido || "",
      data_pagamento: pagamento.data_pagamento,
      valor: pagamento.valor,
      assunto_email: meta.assunto_email || "",
      source_kind: meta.source_kind || "email_pagamento"
    });
    return;
  }

  const chaveDuplicidade = payMontarChaveDuplicidadeInter_(pagamento, fakeMsg);
  if (payExisteRegistroDrive_(chaveDuplicidade.fileName)) {
    state.duplicados.push({
      id_local: "D" + state.seqDup++,
      motivo: "duplicado_drive_log",
      nome_extraido: pagamento.nome_favorecido || "",
      descricao_extraida: pagamento.descricao_pagamento || "",
      data_pagamento: pagamento.data_pagamento,
      valor: pagamento.valor,
      assunto_email: meta.assunto_email || "",
      remetente: meta.remetente || "",
      message_id: String(meta.message_id || ""),
      id_transacao: pagamento.id_transacao || "",
      banco_extraido: pagamento.banco_email || "",
      source_kind: meta.source_kind || "email_pagamento",
      attachment_name: meta.attachment_name || ""
    });
    return;
  }

  const planoInfo = regra.plano
    ? {
        plano_contas: regra.plano,
        score: 1,
        motivo: regra.motivo || "regra_direta",
        acao: "usar"
      }
    : payResolverPlanoPagamentoInter_(pagamento);

  if (planoInfo.acao === "ignorar") {
    state.ignorados.push({
      motivo: planoInfo.motivo || "ignorado_por_plano",
      nome_extraido: pagamento.nome_favorecido || "",
      data_pagamento: pagamento.data_pagamento,
      valor: pagamento.valor,
      assunto_email: meta.assunto_email || "",
      source_kind: meta.source_kind || "email_pagamento"
    });
    return;
  }

  if (!planoInfo.plano_contas) {
    state.pendencias_associacao.push({
      id_local: "P" + state.seqPend++,
      nome_extraido: pagamento.nome_favorecido || pagamento.descricao_pagamento || "",
      descricao_extraida: pagamento.descricao_pagamento || "",
      data_pagamento: pagamento.data_pagamento,
      data_compensacao: pagamento.data_pagamento,
      vencimento: pagamento.data_pagamento,
      valor: pagamento.valor,
      forma: PAY_FORMA_PADRAO,
      conta_oficial: PAY_INTER_CONTA_OFICIAL,
      banco_extraido: pagamento.banco_email || "",
      cpf_cnpj: pagamento.cpf_cnpj || "",
      id_transacao: pagamento.id_transacao || "",
      assunto_email: meta.assunto_email || "",
      remetente: meta.remetente || "",
      message_id: String(meta.message_id || ""),
      source_kind: meta.source_kind || "email_pagamento",
      attachment_name: meta.attachment_name || "",
      status: "pendente_associacao",
      erro: planoInfo.motivo || "Plano de contas não resolvido"
    });
    return;
  }

  const dupGC = payExisteDuplicataGCPagamento_(
    planoInfo.plano_contas,
    pagamento,
    PAY_INTER_CONTA_OFICIAL
  );

  if (dupGC.ok && dupGC.duplicado) {
    state.duplicados.push({
      id_local: "D" + state.seqDup++,
      motivo: "duplicado_gc",
      nome_extraido: pagamento.nome_favorecido || "",
      descricao_extraida: pagamento.descricao_pagamento || "",
      plano_contas: planoInfo.plano_contas,
      data_pagamento: pagamento.data_pagamento,
      valor: pagamento.valor,
      assunto_email: meta.assunto_email || "",
      remetente: meta.remetente || "",
      message_id: String(meta.message_id || ""),
      id_transacao: pagamento.id_transacao || "",
      banco_extraido: pagamento.banco_email || "",
      gc_pagamento_id: dupGC.pagamento_id || null,
      gc_pagamento_codigo: dupGC.pagamento_codigo || null,
      source_kind: meta.source_kind || "email_pagamento",
      attachment_name: meta.attachment_name || ""
    });
    return;
  }

  if (dupGC.ok && dupGC.ambiguo) {
    state.pendencias_associacao.push({
      id_local: "P" + state.seqPend++,
      nome_extraido: pagamento.nome_favorecido || pagamento.descricao_pagamento || "",
      descricao_extraida: pagamento.descricao_pagamento || "",
      plano_sugerido: planoInfo.plano_contas,
      data_pagamento: pagamento.data_pagamento,
      data_compensacao: pagamento.data_pagamento,
      vencimento: pagamento.data_pagamento,
      valor: pagamento.valor,
      forma: PAY_FORMA_PADRAO,
      conta_oficial: PAY_INTER_CONTA_OFICIAL,
      banco_extraido: pagamento.banco_email || "",
      cpf_cnpj: pagamento.cpf_cnpj || "",
      id_transacao: pagamento.id_transacao || "",
      assunto_email: meta.assunto_email || "",
      remetente: meta.remetente || "",
      message_id: String(meta.message_id || ""),
      source_kind: meta.source_kind || "email_pagamento",
      attachment_name: meta.attachment_name || "",
      status: "pendente_gc_ambiguo",
      erro: "Ambiguidade no GC: múltiplos pagamentos iguais para mesma data/valor/conta.",
      gc_ambiguo: true,
      gc_matches: dupGC.matches || []
    });
    return;
  }

  state.itens_prontos.push({
    id_local: "I" + state.seqPronto++,
    plano_contas: planoInfo.plano_contas,
    descricao_pagamento: planoInfo.plano_contas,
    nome_extraido: pagamento.nome_favorecido || pagamento.descricao_pagamento || "",
    descricao_extraida: pagamento.descricao_pagamento || "",
    data_pagamento: pagamento.data_pagamento,
    data_compensacao: pagamento.data_pagamento,
    vencimento: pagamento.data_pagamento,
    valor: pagamento.valor,
    forma: PAY_FORMA_PADRAO,
    conta_oficial: PAY_INTER_CONTA_OFICIAL,
    quitado: PAY_QUITADO_PADRAO,
    banco_extraido: pagamento.banco_email || "",
    cpf_cnpj: pagamento.cpf_cnpj || "",
    id_transacao: pagamento.id_transacao || "",
    assunto_email: meta.assunto_email || "",
    remetente: meta.remetente || "",
    message_id: String(meta.message_id || ""),
    source_kind: meta.source_kind || "email_pagamento",
    attachment_name: meta.attachment_name || "",
    status: "pronto"
  });
}

function preVisualizarPagamentosInter_(periodo, telegram, meta) {
  garantirMapaPagamentosSheet_();

  const itens_prontos = [];
  const pendencias_associacao = [];
  const ignorados = [];
  const duplicados = [];
  const ja_processados = [];
  const state = {
    itens_prontos: itens_prontos,
    pendencias_associacao: pendencias_associacao,
    ignorados: ignorados,
    duplicados: duplicados,
    seqPronto: 1,
    seqPend: 1,
    seqDup: 1
  };

  let extrato = {
    ok: false,
    encontrado: false,
    message: ""
  };

  try {
    extrato = payObterPagamentosDoMelhorExtratoInter_(periodo);
  } catch (e) {
    Logger.log("Falha ao analisar extrato PDF do Inter: " + e);
    extrato = {
      ok: false,
      encontrado: false,
      message: "Falha ao analisar o extrato PDF do Inter: " + e
    };
  }

  if (extrato.ok && extrato.encontrado && extrato.extrato_detectado) {
    for (let p = 0; p < extrato.pagamentos.length; p++) {
      const bruto = extrato.pagamentos[p];

      try {
        const pagamento = {
          nome_favorecido: payLimparNomePagamento_(bruto.nome_favorecido || ""),
          descricao_pagamento: String(
            bruto.descricao_pagamento || bruto.linha_resumo || ""
          ).trim(),
          cpf_cnpj: String(bruto.cpf_cnpj || "").trim(),
          valor: bruto.valor != null ? Number(bruto.valor) : null,
          id_transacao: String(bruto.id_transacao || "").trim(),
          banco_email: PAY_INTER_CONTA_OFICIAL,
          data_pagamento: String(bruto.data_pagamento || "").trim(),
          tipo_pagamento: payNormalizarTipoPagamentoInter_(bruto.tipo_pagamento || "")
        };

        if (!pagamento.id_transacao) {
          pagamento.id_transacao = payBuildSyntheticTransactionId_(
            pagamento,
            "inter_pdf_extrato"
          );
        }

        payProcessarPagamentoInterParaPrevia_(state, pagamento, {
          assunto_email:
            extrato.assunto_email || "[Extrato Inter PDF] " + (extrato.attachment_name || ""),
          remetente: extrato.remetente || "",
          message_id: extrato.message_id || "",
          attachment_name: extrato.attachment_name || "",
          source_kind: extrato.source_kind || "pdf_extrato"
        });

      } catch (e) {
        pendencias_associacao.push({
          id_local: "P" + state.seqPend++,
          nome_extraido: String(bruto && bruto.nome_favorecido || "").trim(),
          descricao_extraida: String(bruto && bruto.descricao_pagamento || "").trim(),
          data_pagamento: String(bruto && bruto.data_pagamento || "").trim(),
          data_compensacao: String(bruto && bruto.data_pagamento || "").trim(),
          vencimento: String(bruto && bruto.data_pagamento || "").trim(),
          valor: bruto && bruto.valor != null ? Number(bruto.valor) : null,
          forma: PAY_FORMA_PADRAO,
          conta_oficial: PAY_INTER_CONTA_OFICIAL,
          banco_extraido: PAY_INTER_CONTA_OFICIAL,
          cpf_cnpj: String(bruto && bruto.cpf_cnpj || "").trim(),
          id_transacao: String(bruto && bruto.id_transacao || "").trim(),
          assunto_email: extrato.assunto_email || "",
          remetente: extrato.remetente || "",
          message_id: String(extrato.message_id || ""),
          source_kind: extrato.source_kind || "pdf_extrato",
          attachment_name: extrato.attachment_name || "",
          status: "pendente_associacao",
          erro: String(e)
        });
      }
    }

    return {
      ok: true,
      modo: "pre_visualizacao",
      tipo_fluxo: "pagamentos",
      origem: "inter",
      fonte_dados: extrato.origem_extrato || "extrato_pdf_gmail",
      periodo: payNormalizarPeriodoRetorno_(periodo),
      itens_prontos: itens_prontos,
      pendencias_associacao: pendencias_associacao,
      ignorados: ignorados,
      duplicados: duplicados,
      ja_processados: ja_processados,
      attachment_name: extrato.attachment_name || "",
      observacoes_extrato: extrato.observacoes || "",
      message: extrato.pagamentos.length
        ? "Pré-visualização do lote de pagamentos gerada a partir do extrato PDF do Inter."
        : "Extrato PDF do Inter encontrado, mas sem pagamentos válidos no período informado."
    };
  }

  const mensagens = payListarMensagensInterPagamentosPorPeriodo_(periodo);

  if (!mensagens.length) {
    return {
      ok: true,
      modo: "pre_visualizacao",
      tipo_fluxo: "pagamentos",
      origem: "inter",
      periodo: payNormalizarPeriodoRetorno_(periodo),
      itens_prontos: [],
      pendencias_associacao: [],
      ignorados: [],
      duplicados: [],
      ja_processados: [],
      message:
        (extrato && extrato.message) ||
        "Nenhum extrato PDF do Inter foi encontrado na pasta do Drive, no Gmail, nem houve e-mail de pagamentos no período informado."
    };
  }

  for (let i = 0; i < mensagens.length; i++) {
    const item = mensagens[i];
    const msg = item.message;

    try {
      if (payJaProcessouMensagemInter_(msg)) {
        ja_processados.push({
          message_id: String(msg.getId() || ""),
          assunto_email: safeSubjectInter_(msg),
          remetente: msg.getFrom()
        });
        continue;
      }

      const assunto = safeSubjectInter_(msg);
      const extraido = payAnalisarEmailPagamentoInterComOpenAI_(msg);

      if (!extraido || !extraido.eh_pagamento) {
        ignorados.push({
          motivo: "nao_eh_pagamento",
          nome_extraido: "",
          data_pagamento: Utilities.formatDate(
            msg.getDate(),
            Session.getScriptTimeZone(),
            "yyyy-MM-dd"
          ),
          valor: null,
          assunto_email: assunto,
          source_kind: "email_pagamento"
        });
        continue;
      }

      const tipoPagamento = payNormalizarTipoPagamentoInter_(extraido.tipo_pagamento || "");
      if (["pix_enviado", "debito_compra"].indexOf(tipoPagamento) < 0) {
        ignorados.push({
          motivo: "tipo_nao_suportado",
          nome_extraido: extraido.nome_favorecido || "",
          data_pagamento: Utilities.formatDate(
            msg.getDate(),
            Session.getScriptTimeZone(),
            "yyyy-MM-dd"
          ),
          valor: extraido.valor != null ? Number(extraido.valor) : null,
          assunto_email: assunto,
          source_kind: "email_pagamento"
        });
        continue;
      }

      const pagamento = {
        nome_favorecido: payLimparNomePagamento_(extraido.nome_favorecido || ""),
        descricao_pagamento: String(extraido.descricao_pagamento || "").trim(),
        cpf_cnpj: String(extraido.cpf_cnpj || "").trim(),
        valor: extraido.valor != null ? Number(extraido.valor) : null,
        id_transacao: String(extraido.id_transacao || "").trim(),
        banco_email: String(extraido.banco_email || PAY_INTER_CONTA_OFICIAL).trim(),
        data_pagamento: Utilities.formatDate(
          msg.getDate(),
          Session.getScriptTimeZone(),
          "yyyy-MM-dd"
        ),
        tipo_pagamento: tipoPagamento
      };

      payProcessarPagamentoInterParaPrevia_(state, pagamento, {
        assunto_email: assunto,
        remetente: msg.getFrom(),
        message_id: String(msg.getId() || ""),
        attachment_name: "",
        source_kind: "email_pagamento"
      });

    } catch (e) {
      pendencias_associacao.push({
        id_local: "P" + state.seqPend++,
        nome_extraido: "",
        descricao_extraida: "",
        data_pagamento: "",
        data_compensacao: "",
        vencimento: "",
        valor: null,
        forma: PAY_FORMA_PADRAO,
        conta_oficial: PAY_INTER_CONTA_OFICIAL,
        banco_extraido: "",
        cpf_cnpj: "",
        id_transacao: "",
        assunto_email: safeSubjectInter_(msg),
        remetente: msg.getFrom(),
        message_id: String(msg.getId() || ""),
        source_kind: "email_pagamento",
        status: "pendente_associacao",
        erro: String(e)
      });
    }
  }

  return {
    ok: true,
    modo: "pre_visualizacao",
    tipo_fluxo: "pagamentos",
    origem: "inter",
    fonte_dados: "emails_notificacao_fallback",
    periodo: payNormalizarPeriodoRetorno_(periodo),
    itens_prontos: itens_prontos,
    pendencias_associacao: pendencias_associacao,
    ignorados: ignorados,
    duplicados: duplicados,
    ja_processados: ja_processados,
    attachment_name: extrato.attachment_name || "",
    observacoes_extrato: extrato.observacoes || "",
    message: extrato && extrato.encontrado
      ? "Usei o fallback de e-mails porque não consegui interpretar corretamente o extrato PDF do Inter."
      : "Pré-visualização do lote de pagamentos gerada com sucesso."
  };
}

function associarPendenciaPagamentos_(payload) {
  const origem = String(payload.origem || "").toLowerCase().trim();
  if (origem !== "inter") {
    return {
      ok: false,
      message: "Pagamentos Itaú ainda não foram implementados nesta primeira fase."
    };
  }

  const nomeExtraido = String(payload.nome_extraido || "").trim();
  const planoContas = String(payload.plano_contas || "").trim();

  if (!nomeExtraido || !planoContas) {
    return {
      ok: false,
      message: "Nome extraído e plano de contas são obrigatórios."
    };
  }

  return payUpsertAliasNoMapa_(nomeExtraido, planoContas, "usar", "associado via Telegram");
}

function confirmarLotePagamentos_(payload) {
  const origem = String(payload.origem || "").toLowerCase().trim();
  if (origem !== "inter") {
    return {
      ok: false,
      message: "Pagamentos Itaú ainda não foram implementados nesta primeira fase."
    };
  }

  return confirmarLotePagamentosInter_(payload);
}

function confirmarLotePagamentosInter_(payload) {
  const itens = Array.isArray(payload.itens) ? payload.itens : [];

  if (!itens.length) {
    return {
      ok: false,
      message: "Nenhum item informado para confirmação."
    };
  }

  const ss = SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  const sh = ss.getSheetByName(PAY_ABA_CENTRAL);
  if (!sh) {
    return {
      ok: false,
      message: 'Aba "' + PAY_ABA_CENTRAL + '" não encontrada.'
    };
  }

  const resultados = [];
  let processados = 0;
  let duplicadosLog = 0;
  let duplicadosGC = 0;
  let ambiguosGC = 0;
  let forcados = 0;
  let erros = 0;

  for (let i = 0; i < itens.length; i++) {
    const item = itens[i];

    try {
      const pagamento = {
        nome_favorecido: item.nome_extraido || "",
        descricao_pagamento: item.descricao_extraida || item.descricao_pagamento || "",
        cpf_cnpj: item.cpf_cnpj || "",
        valor: item.valor != null ? Number(item.valor) : null,
        id_transacao: item.id_transacao || "",
        banco_email: item.banco_extraido || PAY_INTER_CONTA_OFICIAL,
        data_pagamento: item.data_pagamento || ""
      };

      const fakeMsg = {
        getId: function() {
          return String(item.message_id || "");
        },
        getDate: function() {
          return new Date(String(item.data_pagamento || "") + "T12:00:00");
        }
      };

      const isForced = !!item.force_duplicate;
      const chaveDuplicidade = payMontarChaveDuplicidadeInter_(pagamento, fakeMsg);

      if (!isForced && payExisteRegistroDrive_(chaveDuplicidade.fileName)) {
        duplicadosLog += 1;
        resultados.push({
          ok: false,
          tipo: "duplicado_log",
          plano_contas: item.plano_contas || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Duplicado no LOG de pagamentos."
        });
        continue;
      }

      const dupGC = payExisteDuplicataGCPagamento_(
        item.plano_contas || "",
        pagamento,
        item.conta_oficial || PAY_INTER_CONTA_OFICIAL
      );

      if (!isForced && dupGC.ok && dupGC.ambiguo) {
        ambiguosGC += 1;
        resultados.push({
          ok: false,
          tipo: "ambiguo_gc",
          plano_contas: item.plano_contas || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Ambiguidade no GC: múltiplos pagamentos iguais para a mesma data/valor/conta."
        });
        continue;
      }

      if (!isForced && dupGC.ok && dupGC.duplicado) {
        duplicadosGC += 1;
        resultados.push({
          ok: false,
          tipo: "duplicado_gc",
          plano_contas: item.plano_contas || "",
          data_pagamento: item.data_pagamento || "",
          valor: item.valor || null,
          erro: "Duplicado no GC.",
          gc_pagamento_id: dupGC.pagamento_id || null,
          gc_pagamento_codigo: dupGC.pagamento_codigo || null
        });
        continue;
      }

      const bloco = payGetPrimeiroBlocoLivre_(sh);
      if (!bloco) {
        erros += 1;
        resultados.push({
          ok: false,
          tipo: "erro_sem_bloco",
          plano_contas: item.plano_contas || "",
          valor: item.valor || null,
          erro: "Sem bloco livre em Contas a Pagar."
        });
        continue;
      }

      payPreencherBlocoPagamento_(sh, bloco.baseRow, item);

      const registroJson = {
        origem: "inter",
        tipo_fluxo: "pagamentos",
        banco: item.conta_oficial || PAY_INTER_CONTA_OFICIAL,
        plano_contas: item.plano_contas || "",
        descricao_pagamento: item.descricao_pagamento || item.plano_contas || "",
        nome_extraido: item.nome_extraido || "",
        data: item.data_pagamento || "",
        valor: item.valor != null ? Number(item.valor) : null,
        cpf_cnpj: item.cpf_cnpj || "",
        id_transacao: item.id_transacao || null,
        subject: item.assunto_email || "",
        from: item.remetente || "",
        message_id: String(item.message_id || ""),
        processado_em: new Date().toISOString(),
        status: isForced ? "processado_forcado" : "processado",
        duplicata_liberada_por_usuario: isForced,
        duplicate_source: item.duplicate_source || null,
        duplicate_reference: item.duplicate_reference || null
      };

      payGravarRegistroNoDrive_(chaveDuplicidade.fileName, registroJson);

      if (item.message_id && item.source_kind !== "pdf_extrato") {
        payGravarRegistroNoDrive_(
          payMontarChaveMensagemInter_(fakeMsg),
          {
            tipo_registro: "message_id",
            status: isForced ? "processado_forcado" : "processado",
            message_id: String(item.message_id || ""),
            plano_contas: item.plano_contas || "",
            valor: item.valor != null ? Number(item.valor) : null,
            data_pagamento: item.data_pagamento || "",
            nome_favorecido: item.nome_extraido || "",
            processado_em: new Date().toISOString()
          }
        );
      }

      if (item.message_id && item.source_kind !== "pdf_extrato") {
        marcarMensagemInterComoProcessadaSePossivel_(item.message_id);
      }

      if (isForced) forcados += 1;
      processados += 1;

      resultados.push({
        ok: true,
        tipo: isForced ? "processado_forcado" : "processado",
        plano_contas: item.plano_contas || "",
        data_pagamento: item.data_pagamento || "",
        valor: item.valor || null,
        base_row: bloco.baseRow,
        force_duplicate: isForced
      });

    } catch (e) {
      erros += 1;
      resultados.push({
        ok: false,
        tipo: "erro",
        plano_contas: item.plano_contas || "",
        valor: item.valor || null,
        erro: String(e)
      });
    }
  }

  return payMontarResumoConfirmacao_(
    resultados,
    processados,
    duplicadosLog,
    duplicadosGC,
    ambiguosGC,
    forcados,
    erros
  );
}

function listarPlanosPagamentos_(payload) {
  return {
    ok: true,
    planos: payListarNomesPlanosGC_()
  };
}

function resolverPlanoPagamentos_(payload) {
  const nomeFalado = String((payload && payload.nome_falado) || "").trim();
  if (!nomeFalado) {
    return {
      ok: false,
      message: "nome_falado não informado."
    };
  }

  const planos = payListarNomesPlanosGC_();
  if (!planos.length) {
    return {
      ok: false,
      message: "Nenhum plano de contas encontrado no GC."
    };
  }

  const alvo = payNorm_(nomeFalado);

  for (let i = 0; i < planos.length; i++) {
    if (payNorm_(planos[i]) === alvo) {
      return {
        ok: true,
        encontrado: true,
        plano_contas: planos[i],
        score: 1,
        motivo: "match_exato"
      };
    }
  }

  for (let j = 0; j < planos.length; j++) {
    const pNorm = payNorm_(planos[j]);
    if (pNorm.indexOf(alvo) >= 0 || alvo.indexOf(pNorm) >= 0) {
      return {
        ok: true,
        encontrado: true,
        plano_contas: planos[j],
        score: 0.94,
        motivo: "match_contem"
      };
    }
  }

  let melhor = null;
  let melhorScore = 0;

  for (let k = 0; k < planos.length; k++) {
    const score = payScoreTexto_(nomeFalado, planos[k]);
    if (score > melhorScore) {
      melhorScore = score;
      melhor = planos[k];
    }
  }

  if (melhor && melhorScore >= 0.74) {
    return {
      ok: true,
      encontrado: true,
      plano_contas: melhor,
      score: melhorScore,
      motivo: "match_aproximado"
    };
  }

  return {
    ok: true,
    encontrado: false,
    plano_contas: "",
    score: melhorScore,
    motivo: "nao_encontrado"
  };
}

function payResolverPlanoPagamentoInter_(pagamento) {
  const textoBusca = payMelhorTextoBuscaPlano_(pagamento);
  const textoNorm = payNorm_(textoBusca);

  if (!textoNorm) {
    return {
      plano_contas: "",
      score: 0,
      motivo: "Texto de pagamento vazio"
    };
  }

  const mapa = listarMapaPagamentos_();

  for (let i = 0; i < mapa.length; i++) {
    const item = mapa[i];
    const aliases = item.nome_bases || [];

    for (let j = 0; j < aliases.length; j++) {
      const aliasNorm = payNorm_(aliases[j] || "");
      if (!aliasNorm) continue;

      if (aliasNorm === textoNorm || aliasNorm.indexOf(textoNorm) >= 0 || textoNorm.indexOf(aliasNorm) >= 0) {
        if (item.acao === "ignorar") {
          return {
            acao: "ignorar",
            motivo: 'Regra do MAPA_PAGAMENTOS para "' + item.plano_contas + '"'
          };
        }

        return {
          plano_contas: item.plano_contas,
          score: 0.98,
          motivo: 'MAPA_PAGAMENTOS com alias "' + aliases[j] + '"'
        };
      }
    }
  }

  const planos = payListarNomesPlanosGC_();

  for (let k = 0; k < planos.length; k++) {
    if (payNorm_(planos[k]) === textoNorm) {
      return {
        plano_contas: planos[k],
        score: 1,
        motivo: "match_exato_plano_gc"
      };
    }
  }

  let melhor = null;
  let melhorScore = 0;
  for (let x = 0; x < planos.length; x++) {
    const score = payScoreTexto_(textoBusca, planos[x]);
    if (score > melhorScore) {
      melhor = planos[x];
      melhorScore = score;
    }
  }

  if (melhor && melhorScore >= 0.84) {
    return {
      plano_contas: melhor,
      score: melhorScore,
      motivo: "fuzzy_plano_gc"
    };
  }

  return {
    plano_contas: "",
    score: melhorScore,
    motivo: 'Plano não resolvido para "' + textoBusca + '"'
  };
}

function payAplicarRegrasEspeciais_(pagamento) {
  const nome = payNorm_(pagamento.nome_favorecido || "");
  const descricao = payNorm_(pagamento.descricao_pagamento || "");
  const texto = [nome, descricao].join(" ").trim();

  for (let i = 0; i < PAY_IGNORE_NAMES.length; i++) {
    if (texto.indexOf(payNorm_(PAY_IGNORE_NAMES[i])) >= 0) {
      return {
        acao: "ignorar",
        motivo: "transferencia_interna_lp_comercio"
      };
    }
  }

  for (let j = 0; j < PAY_DIRECT_PLAN_RULES.length; j++) {
    if (texto.indexOf(payNorm_(PAY_DIRECT_PLAN_RULES[j].match)) >= 0) {
      return {
        acao: "usar",
        plano: PAY_DIRECT_PLAN_RULES[j].plano,
        motivo: "regra_direta_salario"
      };
    }
  }

  const planoDespesaPessoal = payResolverPlanoDespesaPessoal_(pagamento);
  if (planoDespesaPessoal) {
    return planoDespesaPessoal;
  }

  if (payEhEntregaMotoboy_(pagamento)) {
    return {
      acao: "usar",
      plano: PAY_PLANO_ENTREGA_MOTOBOY,
      motivo: "regra_direta_entrega_motoboy"
    };
  }

  if (Number(pagamento.valor || 0) > 3000 && payPareceCnpj_(pagamento)) {
    return {
      acao: "ignorar",
      motivo: "compra_cnpj_acima_3000"
    };
  }

  return {
    acao: "seguir"
  };
}

function payPareceCnpj_(pagamento) {
  const cpfCnpj = String(pagamento.cpf_cnpj || "").replace(/\D/g, "");
  if (cpfCnpj.length === 14) return true;

  const texto = payNorm_(
    (pagamento.nome_favorecido || "") + " " + (pagamento.descricao_pagamento || "")
  );

  for (let i = 0; i < PAY_CNPJ_HINTS.length; i++) {
    if (texto.indexOf(payNorm_(PAY_CNPJ_HINTS[i])) >= 0) return true;
  }

  return false;
}

function payResolverPlanoDespesaPessoal_(pagamento) {
  const texto = payNorm_(
    (pagamento.nome_favorecido || "") + " " + (pagamento.descricao_pagamento || "")
  );

  for (let i = 0; i < PAY_PHILIPE_PERSONAL_RULES.length; i++) {
    const regra = PAY_PHILIPE_PERSONAL_RULES[i];
    for (let j = 0; j < regra.hints.length; j++) {
      if (texto.indexOf(payNorm_(regra.hints[j])) >= 0) {
        return {
          acao: "usar",
          plano: PAY_PLANO_SALARIO_PHILIPE,
          motivo: "regra_despesa_pessoal_" + regra.motivo
        };
      }
    }
  }

  return null;
}

function payEhEntregaMotoboy_(pagamento) {
  const texto = payNorm_(
    (pagamento.nome_favorecido || "") + " " + (pagamento.descricao_pagamento || "")
  );

  if (texto.indexOf("uber") >= 0) return true;

  for (let i = 0; i < PAY_MOTOBOY_HINTS.length; i++) {
    const hint = payNorm_(PAY_MOTOBOY_HINTS[i]);
    if (hint === "99") {
      if (/(^|[^a-z0-9])99([^a-z0-9]|$)/.test(texto)) return true;
      continue;
    }

    if (texto.indexOf(hint) >= 0) return true;
  }

  return false;
}

function payExisteDuplicataGCPagamento_(planoContas, pagamento, contaOficial) {
  try {
    const contaId = payGetContaBancariaIdPorNome_(contaOficial);
    const formaId = payGetFormaPagamentoIdPix_();
    const data = String(pagamento.data_pagamento || "").trim();
    const valor = Number(pagamento.valor);

    if (!data || !isFinite(valor)) {
      return {
        ok: false,
        duplicado: false,
        ambiguo: false,
        motivo: "data_ou_valor_invalidos"
      };
    }

    let lista = [];
    try {
      lista = payNormalizeList_(
        jreq_("GET", PAY_ENDPOINT_PAGAMENTOS_1, {
          params: {
            pagina: 1,
            limit: 200,
            conta_bancaria_id: contaId,
            forma_pagamento_id: formaId,
            data_inicio: data,
            data_fim: data,
            valor_inicio: valor,
            valor_fim: valor
          }
        })
      );
    } catch (e) {
      const msg = String(e || "");
      if (msg.indexOf("HTTP 404") < 0) throw e;
      lista = payNormalizeList_(
        jreq_("GET", PAY_ENDPOINT_PAGAMENTOS_2, {
          params: {
            pagina: 1,
            limit: 200,
            conta_bancaria_id: contaId,
            forma_pagamento_id: formaId,
            data_inicio: data,
            data_fim: data,
            valor_inicio: valor,
            valor_fim: valor
          }
        })
      );
    }

    const planoNorm = payNorm_(planoContas || "");
    const itens = Array.isArray(lista) ? lista : [];

    const matches = itens.filter(function(item) {
      const r = payUnwrapGcItem_(item);
      const contaIdCand = String(
        r.conta_bancaria_id || r.conta_id || r.id_conta_bancaria || ""
      );
      const formaIdCand = String(
        r.forma_pagamento_id || r.id_forma_pagamento || ""
      );
      const valorCand = Number(r.valor_total || r.valor || 0);
      const planoNomeCand = payNorm_(
        r.nome_plano_conta || r.plano_contas_nome || r.plano_contas || r.descricao || ""
      );
      const descCand = payNorm_(r.descricao || "");

      const datasCand = [
        r.data_competencia,
        r.data_compensacao,
        r.data_pagamento,
        r.data_vencimento
      ]
        .map(function(x) { return String(x || "").slice(0, 10); })
        .filter(Boolean);

      const mesmaData = datasCand.indexOf(data) >= 0;
      const mesmoValor = Math.abs(valorCand - valor) < 0.01;
      const mesmaConta = !contaId || contaIdCand === String(contaId);
      const mesmaForma = !formaIdCand || formaIdCand === String(formaId);
      const mesmoPlano =
        !planoNorm ||
        planoNomeCand === planoNorm ||
        (descCand && descCand.indexOf(planoNorm) >= 0) ||
        (descCand && planoNorm.indexOf(descCand) >= 0);

      return mesmaData && mesmoValor && mesmaConta && mesmaForma && mesmoPlano;
    });

    if (!matches.length) {
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
        pagamento_id: matches[0].id || null,
        pagamento_codigo: matches[0].codigo || null
      };
    }

    return {
      ok: true,
      duplicado: false,
      ambiguo: true,
      motivo: "multiplos_matches",
      matches: matches.slice(0, 10).map(function(r) {
        return {
          id: r.id || null,
          codigo: r.codigo || null,
          descricao: r.descricao || "",
          valor: r.valor_total || r.valor || null,
          data_competencia: r.data_competencia || null,
          data_compensacao: r.data_compensacao || null,
          data_pagamento: r.data_pagamento || null,
          data_vencimento: r.data_vencimento || null,
          conta_bancaria_id: r.conta_bancaria_id || null,
          nome_conta_bancaria: r.nome_conta_bancaria || null
        };
      })
    };

  } catch (e) {
    Logger.log("Falha ao verificar duplicata no GC (Pagamentos Inter): " + e);
    return {
      ok: false,
      duplicado: false,
      ambiguo: false,
      motivo: String(e)
    };
  }
}

function payGetPrimeiroBlocoLivre_(sh) {
  for (let r = PAY_ROW_START; r <= PAY_ROW_END; r += PAY_STEP) {
    const vals = sh.getRange(r, 4, 3, 6).getDisplayValues();
    const flat = [].concat(vals[0], vals[1], vals[2]).join(" ").trim();
    if (!flat) return { baseRow: r };
  }

  return null;
}

function payPreencherBlocoPagamento_(sh, baseRow, item) {
  sh.getRange("D" + baseRow).setValue(item.descricao_pagamento || item.plano_contas || "");
  sh.getRange("G" + baseRow).setValue(item.vencimento || item.data_pagamento || "");

  if (item.valor != null && isFinite(Number(item.valor))) {
    sh.getRange("I" + baseRow).setValue(Number(item.valor));
    sh.getRange("I" + baseRow).setNumberFormat("0.00");
  }

  sh.getRange("D" + (baseRow + 1)).setValue(item.plano_contas || "");
  sh.getRange("G" + (baseRow + 1)).setValue(item.conta_oficial || PAY_INTER_CONTA_OFICIAL);
  sh.getRange("I" + (baseRow + 1)).setValue(item.data_compensacao || item.data_pagamento || "");

  sh.getRange("D" + (baseRow + 2)).setValue(item.forma || PAY_FORMA_PADRAO);
  sh.getRange("G" + (baseRow + 2)).setValue(item.quitado || PAY_QUITADO_PADRAO);

  const origemNota =
    item.source_kind === "pdf_extrato"
      ? "Extrato Inter PDF / Gmail / Telegram"
      : "Inter Gmail / Telegram";

  const nota = [
    "Origem: " + origemNota,
    "Nome extraído: " + (item.nome_extraido || ""),
    "Descrição extraída: " + (item.descricao_extraida || ""),
    "Plano de contas: " + (item.plano_contas || ""),
    "Conta: " + (item.conta_oficial || PAY_INTER_CONTA_OFICIAL),
    "Forma: " + (item.forma || PAY_FORMA_PADRAO),
    "Quitado: " + (item.quitado || PAY_QUITADO_PADRAO),
    "Data compensação: " + (item.data_compensacao || item.data_pagamento || ""),
    "CPF/CNPJ: " + (item.cpf_cnpj || ""),
    "Banco extraído: " + (item.banco_extraido || ""),
    "ID transação: " + (item.id_transacao || ""),
    "Message ID: " + (item.message_id || ""),
    "Anexo PDF: " + (item.attachment_name || "")
  ].join("\n");

  sh.getRange("D" + baseRow).setNote(nota);
}

function payMontarChaveDuplicidadeInter_(pagamento, msg) {
  const id = String(pagamento.id_transacao || "").trim();
  if (id) {
    const chave = "PAYINTER_ID__" + sanitizeFileNameInter_(id);
    return { chave: chave, fileName: chave + ".json" };
  }

  const fallback = [
    "PAYINTER",
    String(pagamento.data_pagamento || ""),
    payFormatValorChave_(pagamento.valor),
    payNorm_(pagamento.nome_favorecido || pagamento.descricao_pagamento || ""),
    String(msg.getId() || "")
  ].join("__");

  const chave = "PAYINTER_MSG__" + sanitizeFileNameInter_(fallback);
  return { chave: chave, fileName: chave + ".json" };
}

function payMontarChaveMensagemInter_(msg) {
  const id = String(msg.getId() || "").trim();
  return "PAYINTER_MSG_ONLY__" + sanitizeFileNameInter_(id) + ".json";
}

function payJaProcessouMensagemInter_(msg) {
  return payExisteRegistroDrive_(payMontarChaveMensagemInter_(msg));
}

function payLerRegistroDrive_(fileName) {
  const folder = DriveApp.getFolderById(PAY_LOG_FOLDER_ID);
  const files = folder.getFilesByName(fileName);
  if (!files.hasNext()) return null;

  const file = files.next();
  const text = file.getBlob().getDataAsString();

  try {
    return JSON.parse(text);
  } catch (e) {
    return { raw_text: text };
  }
}

function payExisteRegistroDrive_(fileName) {
  const reg = payLerRegistroDrive_(fileName);
  if (!reg) return false;

  const status = String(reg.status || "processado").trim();
  return [
    "processado",
    "duplicado_drive_log",
    "duplicado_gc",
    "processado_forcado"
  ].indexOf(status) >= 0;
}

function payGravarRegistroNoDrive_(fileName, obj) {
  const folder = DriveApp.getFolderById(PAY_LOG_FOLDER_ID);
  const files = folder.getFilesByName(fileName);
  const payload = JSON.stringify(obj, null, 2);

  if (files.hasNext()) {
    files.next().setContent(payload);
    return;
  }

  folder.createFile(fileName, payload, MimeType.PLAIN_TEXT);
}

function payMontarJanelaDatasPorPeriodo_(periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();
  const out = {};

  if (tipo === "datas_especificas") {
    const datas = Array.isArray(periodo && periodo.datas) ? periodo.datas : [];
    for (let i = 0; i < datas.length; i++) {
      if (datas[i]) out[String(datas[i])] = true;
    }
    return out;
  }

  const dias = payGetPeriodoDias_(periodo);
  const hoje = new Date();

  for (let j = 0; j < dias; j++) {
    const d = new Date(hoje);
    d.setDate(d.getDate() - j);
    const iso = Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
    out[iso] = true;
  }

  return out;
}

function payBuildSearchWindow_(datasPermitidas) {
  const keys = Object.keys(datasPermitidas || {}).filter(Boolean).sort();
  const hoje = new Date();

  if (!keys.length) {
    const inicioFallback = new Date(hoje);
    inicioFallback.setHours(0, 0, 0, 0);

    const fimFallback = new Date(inicioFallback);
    fimFallback.setDate(fimFallback.getDate() + 1);

    return {
      inicio: inicioFallback,
      fimExclusivo: fimFallback
    };
  }

  const primeiro = new Date(keys[0] + "T00:00:00");
  const ultimo = new Date(keys[keys.length - 1] + "T00:00:00");
  const fimExclusivo = new Date(ultimo);
  fimExclusivo.setDate(fimExclusivo.getDate() + 1);

  return {
    inicio: primeiro,
    fimExclusivo: fimExclusivo
  };
}

function payNormalizarPeriodoRetorno_(periodo) {
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
    valor: payGetPeriodoDias_(periodo),
    label: formatPeriodoLabel_(periodo)
  };
}

function payGetPeriodoDias_(periodo) {
  const tipo = String((periodo && periodo.tipo) || "").trim();
  const valor = Number(periodo && periodo.valor);

  if (tipo === "dias" && isFinite(valor) && valor > 0) return valor;
  return 1;
}

function payMontarResumoConfirmacao_(
  resultados,
  processados,
  duplicadosLog,
  duplicadosGC,
  ambiguosGC,
  forcados,
  erros
) {
  const linhas = [];
  linhas.push("Lote de pagamentos confirmado (INTER).");
  linhas.push("");
  linhas.push("Processados: " + processados);
  linhas.push("Duplicados no LOG: " + duplicadosLog);
  linhas.push("Duplicados no GC: " + duplicadosGC);
  linhas.push("Ambíguos no GC: " + ambiguosGC);
  linhas.push("Forçados manualmente: " + forcados);
  linhas.push("Erros: " + erros);

  if (resultados.length) {
    linhas.push("", "Resumo:");
    for (let i = 0; i < resultados.length; i++) {
      const r = resultados[i];

      if (r.ok) {
        linhas.push(
          (i + 1) + ". " +
          (r.force_duplicate ? "OK FORÇADO" : "OK") + " | " +
          (r.plano_contas || "?") + " | " +
          (r.data_pagamento || "?") + " | " +
          payFormatMoney_(r.valor) +
          (r.base_row ? " | bloco " + r.base_row : "")
        );
      } else {
        linhas.push(
          (i + 1) + ". " +
          String(r.tipo || "ERRO").toUpperCase() + " | " +
          (r.plano_contas || "?") + " | " +
          (r.erro || "falha")
        );
      }
    }
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
      erros: erros
    },
    message: linhas.join("\n")
  };
}

function payMelhorTextoBuscaPlano_(pagamento) {
  const nome = String(pagamento.nome_favorecido || "").trim();
  const descricao = String(pagamento.descricao_pagamento || "").trim();
  const texto = nome || descricao;
  const textoNorm = payNorm_(texto);

  if (
    [
      "compra cartao",
      "compra no debito",
      "pix enviado",
      "pix recebido",
      "estorno compra cartao"
    ].indexOf(textoNorm) >= 0
  ) {
    return "";
  }

  return texto;
}

function payLimparNomePagamento_(nome) {
  return String(nome || "")
    .replace(/\b\d{2}\/\d{2}\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function payNorm_(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function payTokens_(s) {
  return payNorm_(s)
    .split(/[^a-z0-9]+/)
    .map(function(x) { return x.trim(); })
    .filter(Boolean);
}

function payScoreTexto_(a, b) {
  const ta = payTokens_(a);
  const tb = payTokens_(b);

  if (!ta.length || !tb.length) return 0;
  if (payNorm_(a) === payNorm_(b)) return 1;

  let inter = 0;
  const mapa = {};
  for (let i = 0; i < tb.length; i++) mapa[tb[i]] = true;
  for (let j = 0; j < ta.length; j++) {
    if (mapa[ta[j]]) inter++;
  }

  const uniaoObj = {};
  for (let x = 0; x < ta.length; x++) uniaoObj[ta[x]] = true;
  for (let y = 0; y < tb.length; y++) uniaoObj[tb[y]] = true;

  const uniao = Object.keys(uniaoObj).length || 1;
  let score = inter / uniao;

  if (ta[0] && tb[0] && ta[0] === tb[0]) score += 0.22;
  if (payNorm_(a).indexOf(payNorm_(b)) >= 0 || payNorm_(b).indexOf(payNorm_(a)) >= 0) score += 0.18;

  return Math.min(1, score);
}

function payFormatMoney_(v) {
  const n = Number(v);
  if (!isFinite(n)) return "R$ ?";
  return "R$ " + n.toFixed(2);
}

function payFormatValorChave_(v) {
  const n = Number(v);
  if (!isFinite(n)) return "";
  return n.toFixed(2);
}
