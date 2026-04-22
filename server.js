const express = require("express");
const axios = require("axios");
const FormData = require("form-data");
const { isRecebimentosIntent } = require("./intents_recebimentos");
const { parseRecebimentosIntent } = require("./intents_recebimentos");
const { callRecebimentosWebApp } = require("./appscript_recebimentos");

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_TRANSCRIBE_MODEL = process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-transcribe";
const GOOGLE_APPS_SCRIPT_WEBAPP_URL = process.env.GOOGLE_APPS_SCRIPT_WEBAPP_URL || "";

/**
 * =========================================================
 * ESTADO EM MEMÓRIA
 * =========================================================
 */
const pendingBatches = new Map(); // vendas
const pendingRecebimentos = new Map(); // recebimentos

function savePendingRecebimentos(chatId, lote) {
  pendingRecebimentos.set(String(chatId), lote);
}

function getPendingRecebimentos(chatId) {
  return pendingRecebimentos.get(String(chatId)) || null;
}

function clearPendingRecebimentos(chatId) {
  pendingRecebimentos.delete(String(chatId));
}

/**
 * =========================================================
 * TELEGRAM
 * =========================================================
 */
function telegramApiUrl(method) {
  return `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;
}

function telegramFileUrl(filePath) {
  return `https://api.telegram.org/file/bot${TELEGRAM_BOT_TOKEN}/${filePath}`;
}

async function sendTelegramMessage(chatId, text) {
  return axios.post(telegramApiUrl("sendMessage"), {
    chat_id: chatId,
    text
  });
}

async function getTelegramFile(fileId) {
  const resp = await axios.get(telegramApiUrl("getFile"), {
    params: { file_id: fileId }
  });

  if (!resp.data?.ok || !resp.data?.result?.file_path) {
    throw new Error("Não foi possível obter o file_path do Telegram.");
  }

  return resp.data.result;
}

async function downloadTelegramFileBuffer(filePath) {
  const resp = await axios.get(telegramFileUrl(filePath), {
    responseType: "arraybuffer"
  });

  return Buffer.from(resp.data);
}

/**
 * =========================================================
 * OPENAI - TRANSCRIÇÃO
 * =========================================================
 */
async function transcribeAudioWithOpenAI(buffer, filename = "audio.ogg") {
  const form = new FormData();
  form.append("file", buffer, filename);
  form.append("model", OPENAI_TRANSCRIBE_MODEL);
  form.append("language", "pt");

  form.append(
    "prompt",
    [
      "Transcrição em português do Brasil.",
      "Contexto: assistente operacional de recebimentos e vendas via Telegram.",
      "Preserve com máxima fidelidade nomes próprios, datas, valores, códigos curtos e comandos.",
      "Comandos frequentes de recebimentos:",
      "preencha recebimentos de hoje",
      "preencha recebimentos dos ultimos 7 dias",
      "preencha recebimentos dia 18/04, 19/04, 20/04",
      "associar P1 Diergia",
      "associar Karolaine a Ricardo",
      "confirmar lote",
      "cancelar lote",
      "remover 5",
      "Datas podem aparecer como 18/04, 18-04, 19/04, 20/04.",
      "Se houver código como P1, P2, I1, I2, preserve exatamente.",
      "Se houver valor monetário, preserve os números com máxima fidelidade.",
      "Nomes frequentes de clientes e pessoas:",
      "Diergia, Ricardo, Sandro, Larissa, Raquel, Renata, Flávio, Fábio, Diege, Dieergia, Karolaine, Philipe, Izabel, Samara, Eliete, Edilene, Lidiane, Manu.",
      "Produtos frequentes:",
      "liga rosa, liga branca, castanho, loiro, vietnamita, castanho liga rosa, louro liga branca.",
      "Medidas frequentes:",
      "55cm, 60/65cm, 65/70cm, 70/75cm."
    ].join(" ")
  );

  const resp = await axios.post(
    "https://api.openai.com/v1/audio/transcriptions",
    form,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        ...form.getHeaders()
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity
    }
  );

  return resp.data?.text || "";
}

/**
 * =========================================================
 * UTIL GERAIS
 * =========================================================
 */
function normalizeText(text) {
  return String(text || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function isConfirmationText(text) {
  const t = normalizeText(text);

  return (
    t === "sim" ||
    t === "ok" ||
    t === "certo" ||
    t === "isso" ||
    t === "confirmar" ||
    t === "pode confirmar" ||
    t === "confirme" ||
    t === "confirmar sim" ||
    t === "confirme tudo" ||
    t === "confirmar duplicata"
  );
}

function isCancelText(text) {
  const t = normalizeText(text);

  return (
    t === "cancelar" ||
    t === "cancelar lote" ||
    t === "limpar" ||
    t === "limpar lote" ||
    t === "comecar de novo" ||
    t === "recomecar" ||
    t === "apagar lote"
  );
}

/**
 * =========================================================
 * RECEBIMENTOS - COMANDOS DE LOTE
 * =========================================================
 */
function limparClienteOficialFalado(text) {
  return String(text || "")
    .trim()
    .replace(/^[\s"'`.,;:!?-]+/, "")
    .replace(/[\s"'`.,;:!?-]+$/, "")
    .replace(/^(a|ao|aos|a\s+cliente|cliente)\s+/i, "")
    .trim();
}

function parseAssociarPendenciaCommand(text, lote = null) {
  const raw = String(text || "").trim();

  // Ex.: "associar P1 Ricardo"
  let m = raw.match(/^associar\s+(P\d+)\s+(.+)$/i);
  if (m) {
    return {
      pendenciaId: String(m[1]).toUpperCase(),
      clienteOficial: limparClienteOficialFalado(m[2])
    };
  }

  // Ex.: "associar Karolaine a Ricardo"
  m = raw.match(/^associar\s+(.+?)\s+(?:a|ao)\s+(.+)$/i);
  if (m && lote) {
    const nomeFalado = normalizeText(m[1]);
    const clienteOficial = limparClienteOficialFalado(m[2]);

    const pendencias = Array.isArray(lote?.pendencias_associacao)
      ? lote.pendencias_associacao
      : [];

    const found = pendencias.find(p => {
      const nomeExtraido = normalizeText(p.nome_extraido || "");
      return nomeExtraido.includes(nomeFalado) || nomeFalado.includes(nomeExtraido);
    });

    if (found) {
      return {
        pendenciaId: String(found.id_local).toUpperCase(),
        clienteOficial
      };
    }
  }

  return null;
}

function parseRemoverRecebimentoCommand(text) {
  const raw = String(text || "").trim();

  let m = raw.match(/^remover\s+(\d+)[\.\!\?]?$/i);
  if (m) {
    return { itemNumero: Number(m[1]) };
  }

  m = raw.match(/^remove(?:r)?\s+(?:item\s+)?(\d+)[\.\!\?]?$/i);
  if (m) {
    return { itemNumero: Number(m[1]) };
  }

  return null;
}

function isConfirmarRecebimentosCommand(text) {
  const t = normalizeText(String(text || "").replace(/[.!?]+$/g, ""));
  return (
    t === "confirmar lote" ||
    t === "confirma lote" ||
    t === "confirmar" ||
    t === "confirma"
  );
}

function isCancelarRecebimentosCommand(text) {
  const t = normalizeText(String(text || "").replace(/[.!?]+$/g, ""));
  return (
    t === "cancelar lote" ||
    t === "cancela lote" ||
    t === "cancelar" ||
    t === "cancela"
  );
}

function formatMoneyBRL(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "R$ ?";
  return `R$ ${n.toFixed(2).replace(".", ",")}`;
}

function summarizePendingRecebimentos(lote) {
  const prontos = Array.isArray(lote?.itens_prontos) ? lote.itens_prontos : [];
  const pendencias = Array.isArray(lote?.pendencias_associacao) ? lote.pendencias_associacao : [];
  const ignorados = Array.isArray(lote?.ignorados) ? lote.ignorados : [];
  const duplicados = Array.isArray(lote?.duplicados) ? lote.duplicados : [];
  const jaProcessados = Array.isArray(lote?.ja_processados) ? lote.ja_processados : [];

  const linhas = [];
  linhas.push(`Recebimentos ${String(lote?.origem || "").toUpperCase()} encontrados.`);
  linhas.push("");
  linhas.push(`Período: ${lote?.periodo?.label || "não informado"}`);
  linhas.push(`Prontos para preencher: ${prontos.length}`);
  linhas.push(`Pendências: ${pendencias.length}`);
  linhas.push(`Ignorados: ${ignorados.length}`);
  linhas.push(`Já processados: ${jaProcessados.length}`);
  linhas.push(`Duplicados: ${duplicados.length}`);

  if (prontos.length) {
    linhas.push("", "Prontos:");
    prontos.forEach((item, idx) => {
      linhas.push(
        `${idx + 1}. ${item.cliente_oficial} | ${item.data_pagamento} | ${formatMoneyBRL(item.valor)}`
      );
    });
  }

  if (pendencias.length) {
    linhas.push("", "Pendências:");
    pendencias.forEach((item, idx) => {
      linhas.push(
        `P${idx + 1}. ${item.nome_extraido} | ${item.data_pagamento} | ${formatMoneyBRL(item.valor)}`
      );
    });
  }

  linhas.push("", "Comandos:");
  linhas.push("- associar P1 NOME_DO_CLIENTE_OFICIAL");
  linhas.push("- ou: associar NOME_DA_PENDENCIA a NOME_DO_CLIENTE_OFICIAL");
  linhas.push("- remover 5");
  linhas.push("- confirmar lote");
  linhas.push("- cancelar lote");
  linhas.push("");
  linhas.push("Exemplos:");
  linhas.push("- associar P1 Diergia");
  linhas.push("- associar Karolaine a Ricardo");
  linhas.push("");
  linhas.push("Você pode usar qualquer cliente oficial do MAPA_CLIENTES.");

  return linhas.join("\n");
}

async function tryHandleRecebimentosPendingCommands(chatId, text) {
  const lote = getPendingRecebimentos(chatId);
  if (!lote) return false;

  const associar = parseAssociarPendenciaCommand(text, lote);
  if (associar) {
    const pendencias = Array.isArray(lote.pendencias_associacao) ? lote.pendencias_associacao : [];
    const idx = pendencias.findIndex(
      p => String(p.id_local || "").toUpperCase() === associar.pendenciaId
    );

    if (idx < 0) {
      await sendTelegramMessage(chatId, `Não encontrei a pendência ${associar.pendenciaId}.`);
      return true;
    }

    const pendencia = pendencias[idx];

    const payload = {
      action: "associar_pendencia_recebimentos",
      origem: lote.origem,
      pendencia_id: pendencia.id_local,
      nome_extraido: pendencia.nome_extraido,
      cliente_oficial: associar.clienteOficial
    };

    const result = await callRecebimentosWebApp(payload);
    console.log("Recebimentos associar result:", JSON.stringify(result));

    if (!result?.ok) {
      await sendTelegramMessage(
        chatId,
        result?.message || "Não consegui salvar a associação."
      );
      return true;
    }

    const itemPronto = {
      id_local: `I${(lote.itens_prontos?.length || 0) + 1}`,
      cliente_oficial: associar.clienteOficial,
      nome_extraido: pendencia.nome_extraido,
      data_pagamento: pendencia.data_pagamento,
      valor: pendencia.valor,
      forma: pendencia.forma || "PIX",
      conta_oficial: pendencia.conta_oficial || "Inter Empresas",
      banco_extraido: pendencia.banco_extraido || "",
      id_transacao: pendencia.id_transacao || null,
      assunto_email: pendencia.assunto_email || "",
      remetente: pendencia.remetente || "",
      message_id: pendencia.message_id || "",
      status: "pronto"
    };

    lote.pendencias_associacao.splice(idx, 1);
    lote.itens_prontos.push(itemPronto);
    lote.historico_comandos.push({
      tipo: "associacao",
      pendencia_id: associar.pendenciaId,
      cliente_oficial: associar.clienteOficial,
      em: new Date().toISOString()
    });

    savePendingRecebimentos(chatId, lote);

    await sendTelegramMessage(
      chatId,
      [
        "Associação salva:",
        `${pendencia.nome_extraido} -> ${associar.clienteOficial}`,
        "",
        summarizePendingRecebimentos(lote)
      ].join("\n")
    );
    return true;
  }

  const remover = parseRemoverRecebimentoCommand(text);
  if (remover) {
    const idx = remover.itemNumero - 1;
    const prontos = Array.isArray(lote.itens_prontos) ? lote.itens_prontos : [];

    if (idx < 0 || idx >= prontos.length) {
      await sendTelegramMessage(chatId, `Não encontrei o item ${remover.itemNumero}.`);
      return true;
    }

    const removido = prontos.splice(idx, 1)[0];

    lote.historico_comandos.push({
      tipo: "remocao",
      item_numero: remover.itemNumero,
      cliente_oficial: removido?.cliente_oficial || "",
      em: new Date().toISOString()
    });

    savePendingRecebimentos(chatId, lote);

    await sendTelegramMessage(
      chatId,
      [
        "Item removido do lote:",
        `${remover.itemNumero}. ${removido.cliente_oficial} | ${removido.data_pagamento} | ${formatMoneyBRL(removido.valor)}`,
        "",
        summarizePendingRecebimentos(lote)
      ].join("\n")
    );
    return true;
  }

  if (isCancelarRecebimentosCommand(text)) {
    clearPendingRecebimentos(chatId);
    await sendTelegramMessage(chatId, "Lote de recebimentos cancelado.");
    return true;
  }

  if (isConfirmarRecebimentosCommand(text)) {
    if ((lote.pendencias_associacao || []).length > 0) {
      await sendTelegramMessage(
        chatId,
        "Ainda existem pendências de associação. Resolva antes de confirmar o lote."
      );
      return true;
    }

    const payload = {
      action: "confirmar_lote_recebimentos",
      origem: lote.origem,
      itens: lote.itens_prontos || []
    };

    const result = await callRecebimentosWebApp(payload);
    console.log("Recebimentos confirmar result:", JSON.stringify(result));

    if (!result?.ok) {
      await sendTelegramMessage(
        chatId,
        result?.message || "Não consegui confirmar o lote de recebimentos."
      );
      return true;
    }

    clearPendingRecebimentos(chatId);
    await sendTelegramMessage(chatId, result?.message || "Lote confirmado com sucesso.");
    return true;
  }

  return false;
}

/**
 * =========================================================
 * RECEBIMENTOS - HANDLER PRINCIPAL
 * =========================================================
 */
async function handleRecebimentosMessage(ctx) {
  const { message, text, sendTelegramMessage, transcription } = ctx;
  const chatId = message.chat.id;

  const parsed = parseRecebimentosIntent(text, message);

  if (transcription) {
    await sendTelegramMessage(
      chatId,
      `Transcrição:\n"${transcription}"`
    );
  }

  await sendTelegramMessage(
    chatId,
    `Entendi. Vou processar recebimentos de ${parsed.origem.toUpperCase()} para ${parsed.periodo.label}.`
  );

  await sendTelegramMessage(
    chatId,
    "Montando a prévia dos recebimentos, aguarde um instante..."
  );

  const payload = {
    action: "processar_recebimentos_v1",
    origem: parsed.origem,
    periodo: parsed.periodo,
    telegram: {
      chat_id: chatId,
      has_document: !!message.document
    },
    message_meta: {
      message_id: message.message_id || null,
      date: message.date || null
    }
  };

  const result = await callRecebimentosWebApp(payload);
  console.log("Recebimentos result:", JSON.stringify(result));

  if (result?.modo === "pre_visualizacao") {
    const lote = {
      tipo: "recebimentos_lote_pendente",
      origem: result.origem || parsed.origem,
      periodo: result.periodo || parsed.periodo,
      criadoEm: new Date().toISOString(),
      resumoOrigem: {
        processados_detectados: (result.itens_prontos || []).length,
        ignorados: (result.ignorados || []).length,
        duplicados: (result.duplicados || []).length,
        ja_processados: (result.ja_processados || []).length,
        erros: (result.pendencias_associacao || []).length
      },
      itens_prontos: result.itens_prontos || [],
      pendencias_associacao: result.pendencias_associacao || [],
      ignorados: result.ignorados || [],
      duplicados: result.duplicados || [],
      ja_processados: result.ja_processados || [],
      historico_comandos: []
    };

    savePendingRecebimentos(chatId, lote);
    await sendTelegramMessage(chatId, summarizePendingRecebimentos(lote));
    return;
  }

  const resumo = result?.message || "Recebimentos processados.";
  await sendTelegramMessage(chatId, resumo);
}

/**
 * =========================================================
 * VENDAS - CÓDIGO ATUAL
 * =========================================================
 */
function looksLikeFreshOrderMessage(text) {
  const t = normalizeText(text);

  return (
    t.includes("comprou") ||
    t.includes("pedido") ||
    /\b\d+(?:[.,]\d+)?\s*(g|kg)\b/.test(t) ||
    /\br\$\s*\d+/.test(t) ||
    t.includes("valor") ||
    t.includes("vence") ||
    t.includes("vencimento") ||
    t.includes("no dia")
  );
}

function cloneExtraction(extraction) {
  return JSON.parse(JSON.stringify(extraction || { pedidos: [] }));
}

function savePendingBatch(chatId, extraction, meta = {}) {
  pendingBatches.set(String(chatId), {
    extraction,
    meta,
    createdAt: new Date().toISOString()
  });
}

function getPendingBatch(chatId) {
  return pendingBatches.get(String(chatId)) || null;
}

function clearPendingBatch(chatId) {
  pendingBatches.delete(String(chatId));
}

function parseBrazilianNumber(str) {
  if (str == null) return null;
  const cleaned = String(str).trim().replace(/\./g, "").replace(",", ".");
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function formatDateToIso(date) {
  const yyyy = date.getFullYear();
  const mm = String(date.getMonth() + 1).padStart(2, "0");
  const dd = String(date.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function normalizeDateValue(rawText) {
  const original = String(rawText || "").trim();
  if (!original) return null;

  const t = normalizeText(original);
  const now = new Date();

  if (t === "hoje") {
    return formatDateToIso(now);
  }

  if (t === "ontem") {
    const d = new Date(now);
    d.setDate(d.getDate() - 1);
    return formatDateToIso(d);
  }

  if (t === "antes de ontem") {
    const d = new Date(now);
    d.setDate(d.getDate() - 2);
    return formatDateToIso(d);
  }

  let m = original.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;

  m = original.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    const dd = String(m[1]).padStart(2, "0");
    const mm = String(m[2]).padStart(2, "0");
    const yyyy = String(m[3]);
    return `${yyyy}-${mm}-${dd}`;
  }

  m = original.match(/^(\d{1,2})\/(\d{1,2})$/);
  if (m) {
    const dd = String(m[1]).padStart(2, "0");
    const mm = String(m[2]).padStart(2, "0");
    const yyyy = String(now.getFullYear());
    return `${yyyy}-${mm}-${dd}`;
  }

  return original;
}

function normalizeSinglePedido(pedido) {
  const out = { ...(pedido || {}) };

  if (out.quantidade != null && Number.isFinite(Number(out.quantidade))) {
    out.quantidade = Number(out.quantidade);
  }

  if (out.valor_falado != null && Number.isFinite(Number(out.valor_falado))) {
    out.valor_falado = Number(out.valor_falado);
  }

  if (out.data_falada) out.data_falada = normalizeDateValue(out.data_falada);
  if (out.vencimento_falado) out.vencimento_falado = normalizeDateValue(out.vencimento_falado);

  return out;
}

function normalizeExtraction(extraction) {
  const pedidos = Array.isArray(extraction?.pedidos) ? extraction.pedidos : [];
  return {
    ...extraction,
    pedidos: pedidos.map(normalizeSinglePedido)
  };
}

function parsePaymentText(rawText) {
  const t = normalizeText(rawText);
  if (t.includes("pix")) return "PIX";
  if (t.includes("dinheiro")) return "Dinheiro à Vista";
  if (t.includes("a vista") || t.includes("avista")) return "Dinheiro à Vista";
  return null;
}

function inferGlobalContextFromText(text) {
  const original = String(text || "").trim();
  const normalized = normalizeText(original);

  const context = {
    cliente_falado: null,
    data_falada: null,
    vencimento_falado: null,
    forma_pagamento_falada: null
  };

  const payment = parsePaymentText(original);
  if (payment) {
    context.forma_pagamento_falada = payment;
  }

  const explicitDates = [];
  const reFull = /\b(\d{1,2}\/\d{1,2}\/\d{4})\b/g;
  let m;
  while ((m = reFull.exec(original)) !== null) explicitDates.push(normalizeDateValue(m[1]));

  const reShort = /\b(\d{1,2}\/\d{1,2})\b/g;
  while ((m = reShort.exec(original)) !== null) explicitDates.push(normalizeDateValue(m[1]));

  if (normalized.includes("antes de ontem")) explicitDates.push(normalizeDateValue("antes de ontem"));
  else if (normalized.includes("ontem")) explicitDates.push(normalizeDateValue("ontem"));
  else if (normalized.includes("hoje")) explicitDates.push(normalizeDateValue("hoje"));

  const uniqueDates = [...new Set(explicitDates.filter(Boolean))];
  if (uniqueDates.length === 1) context.data_falada = uniqueDates[0];

  const duePatterns = [
    /\bvence(?:\s+no)?\s+dia\s+(\d{1,2}\/\d{1,2}(?:\/\d{4})?)\b/i,
    /\bvencimento\s+(?:dia\s+)?(\d{1,2}\/\d{1,2}(?:\/\d{4})?)\b/i,
    /\bvence\s+(\d{1,2}\/\d{1,2}(?:\/\d{4})?)\b/i
  ];

  for (const re of duePatterns) {
    const match = original.match(re);
    if (match) {
      context.vencimento_falado = normalizeDateValue(match[1]);
      break;
    }
  }

  const clientePattern = original.match(/^\s*([A-Za-zÀ-ÿ'’\-]+)\s+comprou\b/i);
  if (clientePattern) {
    context.cliente_falado = String(clientePattern[1] || "").trim() || null;
  }

  return context;
}

function applyBatchContextDefaults(extraction, rawText) {
  const normalizedExtraction = normalizeExtraction(extraction);
  const pedidos = Array.isArray(normalizedExtraction?.pedidos) ? normalizedExtraction.pedidos : [];
  if (!pedidos.length) return normalizedExtraction;

  const globalContext = inferGlobalContextFromText(rawText);

  const filled = pedidos.map((pedido) => {
    const out = { ...pedido };

    if (!out.cliente_falado && globalContext.cliente_falado) out.cliente_falado = globalContext.cliente_falado;
    if (!out.data_falada && globalContext.data_falada) out.data_falada = globalContext.data_falada;
    if (!out.vencimento_falado && globalContext.vencimento_falado) out.vencimento_falado = globalContext.vencimento_falado;
    if (!out.forma_pagamento_falada && globalContext.forma_pagamento_falada) out.forma_pagamento_falada = globalContext.forma_pagamento_falada;

    return normalizeSinglePedido(out);
  });

  return {
    ...normalizedExtraction,
    pedidos: filled
  };
}

async function extractOrdersFromText(text) {
  const prompt = `
Você é um extrator de pedidos de vendas em português do Brasil.

Converta a mensagem do usuário em JSON.
A mensagem pode conter de 1 a 10 pedidos.
Pode haver pedidos do mesmo cliente ou de clientes diferentes na mesma mensagem.
Se não houver clareza suficiente, ainda tente extrair o máximo com cautela.

Regras:
- Se a quantidade vier em kg, preserve unidade = "kg".
- Se a quantidade vier em g/gramas, preserve unidade = "g".
- Se o usuário disser um valor por extenso, converta para número.
- Se o usuário disser data da compra como "18/04", preserve "18/04".
- Se o usuário disser "ontem", "antes de ontem" ou "hoje", preserve esse texto em data_falada.
- Se a forma de pagamento não for dita, retorne null.
- Se o vencimento não for dito, retorne null.
- Retorne SOMENTE JSON válido.

Formato exato:
{
  "pedidos": [
    {
      "cliente_falado": "string ou null",
      "produto_falado": "string ou null",
      "quantidade": number ou null,
      "unidade": "g|kg|un|null",
      "data_falada": "string ou null",
      "valor_falado": number ou null,
      "forma_pagamento_falada": "PIX|Dinheiro à Vista|string|null",
      "vencimento_falado": "string ou null",
      "observacoes": "string ou null"
    }
  ]
}

Mensagem:
${text}
`.trim();

  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    {
      model: "gpt-4.1-mini",
      messages: [
        {
          role: "system",
          content:
            "Você extrai pedidos comerciais e responde apenas JSON válido, sem markdown, sem comentários e sem texto extra."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      temperature: 0,
      response_format: { type: "json_object" }
    },
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`
      }
    }
  );

  const content = String(resp.data?.choices?.[0]?.message?.content || "{}").trim();

  try {
    const parsed = JSON.parse(content);
    return applyBatchContextDefaults(parsed, text);
  } catch (err) {
    return { pedidos: [], erro_parse: true, conteudo_bruto: content };
  }
}

function summarizeOrders(extraction) {
  const pedidos = Array.isArray(extraction?.pedidos) ? extraction.pedidos : [];

  if (!pedidos.length) return "Não consegui extrair pedidos com segurança ainda.";

  const linhas = pedidos.map((p, i) => {
    const qtd = p.quantidade != null ? String(p.quantidade) : "?";
    const unidade = p.unidade || "";
    const produto = p.produto_falado || "produto não identificado";
    const cliente = p.cliente_falado || "cliente não identificado";
    const data = p.data_falada ? ` | data: ${p.data_falada}` : "";
    const forma = p.forma_pagamento_falada ? ` | forma: ${p.forma_pagamento_falada}` : "";
    const venc = p.vencimento_falado ? ` | vencimento: ${p.vencimento_falado}` : "";
    const valor = p.valor_falado != null ? ` | valor: ${p.valor_falado}` : "";

    return `${i + 1}. ${qtd}${unidade} de ${produto} para ${cliente}${data}${forma}${venc}${valor}`;
  });

  return `Entendi estes pedidos:\n\n${linhas.join("\n")}\n\nPode confirmar?`;
}

function parseSimpleCorrection(text, pendingExtraction) {
  const pedidos = Array.isArray(pendingExtraction?.pedidos) ? pendingExtraction.pedidos : [];
  if (!pedidos.length) return null;

  const normalized = normalizeText(text);
  const itemIndexMatch = normalized.match(/\bitem\s+(\d+)\b/);
  const itemIndex = itemIndexMatch ? Number(itemIndexMatch[1]) : (pedidos.length === 1 ? 1 : null);

  const removeMatch =
    normalized.match(/\bremove(?:r)?\s+o\s+item\s+(\d+)\b/) ||
    normalized.match(/\bapaga(?:r)?\s+o\s+item\s+(\d+)\b/) ||
    normalized.match(/\bexclui(?:r)?\s+o\s+item\s+(\d+)\b/);

  if (removeMatch) {
    return {
      is_correction: true,
      action: "remove_item",
      item_index: Number(removeMatch[1]),
      fields: {},
      motivo: "fallback regex remove item"
    };
  }

  if (!itemIndex) return null;

  const qtyMatch =
    normalized.match(/\b(\d+(?:[.,]\d+)?)\s*(kg|g)\b/) ||
    normalized.match(/\bnao[, ]+e\s+(\d+(?:[.,]\d+)?)\s*(kg|g)\b/);

  if (qtyMatch) {
    return {
      is_correction: true,
      action: "update_quantity",
      item_index: itemIndex,
      fields: {
        quantidade: Number(String(qtyMatch[1]).replace(",", ".")),
        unidade: qtyMatch[2]
      },
      motivo: "fallback regex quantidade"
    };
  }

  const clientMatch =
    normalized.match(/\btroca\s+o\s+cliente\s+para\s+(.+)$/) ||
    normalized.match(/\bo\s+cliente\s+e\s+(.+)$/) ||
    normalized.match(/\bcliente\s+(.+)$/);

  if (clientMatch) {
    const cliente = String(clientMatch[1] || "").trim();
    if (cliente) {
      return {
        is_correction: true,
        action: "update_client",
        item_index: itemIndex,
        fields: {
          cliente_falado: cliente
        },
        motivo: "fallback regex cliente"
      };
    }
  }

  return null;
}

function detectBatchRewriteIntent(text) {
  const t = normalizeText(text);
  return t.includes("comprou") || t.includes("pedido") || t.includes("pedidos");
}

async function tryBuildBatchRewriteCorrection(text, pendingExtraction) {
  if (!detectBatchRewriteIntent(text)) return null;

  const extracted = await extractOrdersFromText(text);
  const pedidos = Array.isArray(extracted?.pedidos) ? extracted.pedidos : [];
  if (!pedidos.length) return null;

  const normalized = normalizeText(text);
  const itemIndexMatch = normalized.match(/\bitem\s+(\d+)\b/);
  const itemIndex = itemIndexMatch ? Number(itemIndexMatch[1]) : null;
  const currentCount = Array.isArray(pendingExtraction?.pedidos) ? pendingExtraction.pedidos.length : 0;

  if (pedidos.length > 1) {
    return {
      is_correction: true,
      action: "replace_batch",
      item_index: null,
      fields: {},
      replacement_extraction: extracted,
      motivo: "reescrita completa do lote"
    };
  }

  if (pedidos.length === 1 && itemIndex) {
    return {
      is_correction: true,
      action: "replace_item_fields",
      item_index: itemIndex,
      fields: {},
      replacement_pedido: pedidos[0],
      motivo: "reescrita completa do item"
    };
  }

  if (pedidos.length === 1 && currentCount === 1) {
    return {
      is_correction: true,
      action: "replace_batch",
      item_index: null,
      fields: {},
      replacement_extraction: extracted,
      motivo: "substituicao total de lote com um item"
    };
  }

  return null;
}

async function interpretCorrectionFromText(text, pendingExtraction) {
  const batchRewrite = await tryBuildBatchRewriteCorrection(text, pendingExtraction);
  if (batchRewrite) return batchRewrite;

  const fallback = parseSimpleCorrection(text, pendingExtraction);
  if (fallback) return fallback;

  return {
    is_correction: false,
    action: null,
    item_index: null,
    fields: {},
    motivo: "não interpretado"
  };
}

async function applyCorrectionToExtraction(extraction, correction) {
  const novo = cloneExtraction(extraction);
  const pedidos = Array.isArray(novo.pedidos) ? novo.pedidos : [];

  if (correction?.action === "replace_batch") {
    const replacement = normalizeExtraction(correction?.replacement_extraction || { pedidos: [] });
    const novosPedidos = Array.isArray(replacement?.pedidos) ? replacement.pedidos : [];

    if (!novosPedidos.length) {
      return {
        ok: false,
        message: "Não consegui entender o novo lote completo."
      };
    }

    return {
      ok: true,
      extraction: {
        ...novo,
        pedidos: novosPedidos
      },
      message: `Substituí o lote inteiro por ${novosPedidos.length} pedido(s).`
    };
  }

  const idx = Number(correction?.item_index || 0) - 1;
  if (idx < 0 || idx >= pedidos.length) {
    return {
      ok: false,
      message: "Não consegui identificar qual item corrigir."
    };
  }

  const item = pedidos[idx];
  const action = correction?.action;

  if (action === "update_quantity") {
    item.quantidade = Number(correction.fields.quantidade);
    item.unidade = correction?.fields?.unidade || item.unidade || "g";
    return { ok: true, extraction: novo, message: `Corrigi a quantidade do item ${idx + 1}.` };
  }

  if (action === "update_client") {
    item.cliente_falado = String(correction?.fields?.cliente_falado || "").trim();
    return { ok: true, extraction: novo, message: `Corrigi o cliente do item ${idx + 1}.` };
  }

  if (action === "remove_item") {
    pedidos.splice(idx, 1);
    return { ok: true, extraction: novo, message: `Removi o item ${idx + 1}.` };
  }

  return {
    ok: false,
    message: "Não reconheci a ação de correção."
  };
}

async function callGoogleAppsScript(payload) {
  if (!GOOGLE_APPS_SCRIPT_WEBAPP_URL) {
    throw new Error("GOOGLE_APPS_SCRIPT_WEBAPP_URL não configurada.");
  }

  const resp = await axios.post(GOOGLE_APPS_SCRIPT_WEBAPP_URL, payload, {
    headers: { "Content-Type": "application/json" },
    validateStatus: () => true
  });

  return resp.data;
}

function formatGoogleSuccessMessage(gsResp) {
  const resultados = Array.isArray(gsResp?.resultados) ? gsResp.resultados : [];
  const okResults = resultados.filter((r) => r && r.ok);

  if (!okResults.length) {
    return `Lote confirmado.\n\nResposta bruta do Google:\n${JSON.stringify(gsResp).slice(0, 3500)}`;
  }

  const linhas = okResults.map((r, i) => {
    const cliente = r.cliente_oficial || "cliente ?";
    const produto = r.produto_oficial || "produto ?";
    const qtdG = r.quantidade_gramas != null ? `${r.quantidade_gramas}g` : "?g";
    const qtdSheet = r.quantidade_sheet != null ? String(r.quantidade_sheet) : "?";
    const valor = r.valor != null ? `R$ ${r.valor}` : "sem valor";
    const bloco = r.base_row != null ? `bloco ${r.base_row}` : "bloco ?";
    return `${i + 1}. ${cliente} — ${produto} — ${qtdG} — sheet ${qtdSheet} — ${valor} — ${bloco}`;
  });

  return `Lote confirmado com sucesso.\n\n${linhas.join("\n")}`;
}

async function handlePotentialCorrection(chatId, incomingText, sourceLabel, pending) {
  if (!pending || isConfirmationText(incomingText) || isCancelText(incomingText)) return false;

  const correction = await interpretCorrectionFromText(incomingText, pending.extraction);
  if (!correction?.is_correction) return false;

  const applied = await applyCorrectionToExtraction(pending.extraction, correction);
  if (!applied.ok) {
    if (looksLikeFreshOrderMessage(incomingText)) return false;
    await sendTelegramMessage(chatId, applied.message);
    return true;
  }

  savePendingBatch(chatId, applied.extraction, {
    ...(pending.meta || {}),
    lastCorrectionText: incomingText,
    lastCorrectionSource: sourceLabel,
    duplicateAwaitingForce: false
  });

  const novoResumo = summarizeOrders(applied.extraction);

  await sendTelegramMessage(
    chatId,
    `${sourceLabel === "audio" ? `Transcrição da correção:\n"${incomingText}"\n\n` : ""}${applied.message}\n\n${novoResumo}`
  );

  return true;
}

/**
 * =========================================================
 * ROTAS
 * =========================================================
 */
app.get("/", (req, res) => {
  res.status(200).json({
    ok: true,
    service: "agente-ia-preenchimento-vendas-bot",
    status: "online"
  });
});

app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    status: "healthy"
  });
});

app.post("/telegram/webhook", async (req, res) => {
  res.status(200).json({ ok: true });

  try {
    const update = req.body || {};
    const msg = update.message || update.edited_message;
    if (!msg) return;

    const chatId = msg.chat?.id;
    if (!chatId) return;

    const text = (msg.text || msg.caption || "").trim();

    if (msg.document) {
      const isReceb = isRecebimentosIntent(text, msg);

      if (!isReceb) {
        await sendTelegramMessage(
          chatId,
          "Recebi um arquivo. Se isso for um extrato de recebimentos, me diga algo como: 'processa esse extrato' ou 'pega os últimos 3 dias'."
        );
        return;
      }

      await handleRecebimentosMessage({
        message: msg,
        text,
        sendTelegramMessage,
        getTelegramFile,
        downloadTelegramFileBuffer
      });

      return;
    }

    if (text) {
      const handledRecebimentosPending = await tryHandleRecebimentosPendingCommands(chatId, text);
      if (handledRecebimentosPending) return;
    }

    if (text && isCancelText(text)) {
      clearPendingBatch(chatId);
      await sendTelegramMessage(chatId, "Lote pendente cancelado. Pode mandar um novo pedido.");
      return;
    }

    if (text && isConfirmationText(text)) {
      const pending = getPendingBatch(chatId);

      if (!pending) {
        await sendTelegramMessage(chatId, "Não encontrei nenhum lote pendente para confirmar.");
        return;
      }

      const pedidos = Array.isArray(pending.extraction?.pedidos) ? pending.extraction.pedidos : [];

      const gsResp = await callGoogleAppsScript({
        action: "preencher_lote_v1",
        pedidos,
        meta: pending.meta || {},
        force_duplicate_confirmed: false
      });

      if (gsResp?.ok) {
        clearPendingBatch(chatId);
        await sendTelegramMessage(chatId, formatGoogleSuccessMessage(gsResp));
      } else {
        clearPendingBatch(chatId);
        await sendTelegramMessage(
          chatId,
          `O lote foi confirmado, mas houve falha ao enviar ao Google.\n\nResposta: ${JSON.stringify(gsResp).slice(0, 3500)}`
        );
      }

      return;
    }

    if (text) {
      if (isRecebimentosIntent(text, msg)) {
        await handleRecebimentosMessage({
          message: msg,
          text,
          sendTelegramMessage,
          getTelegramFile,
          downloadTelegramFileBuffer
        });
        return;
      }

      const pending = getPendingBatch(chatId);
      const handledCorrection = await handlePotentialCorrection(chatId, text, "text", pending);
      if (handledCorrection) return;

      const extraction = await extractOrdersFromText(text);
      const resumo = summarizeOrders(extraction);

      savePendingBatch(chatId, extraction, {
        source: "text",
        originalText: text,
        duplicateAwaitingForce: false
      });

      await sendTelegramMessage(chatId, resumo);
      return;
    }

    if (msg.voice || msg.audio) {
      await sendTelegramMessage(chatId, "Recebi seu áudio. Vou transcrever e analisar.");

      const fileId = msg.voice?.file_id || msg.audio?.file_id;
      const fileInfo = await getTelegramFile(fileId);
      const audioBuffer = await downloadTelegramFileBuffer(fileInfo.file_path);
      const transcription = await transcribeAudioWithOpenAI(audioBuffer, "audio.ogg");

      if (!transcription || !String(transcription).trim()) {
        await sendTelegramMessage(chatId, "Não consegui transcrever esse áudio. Se quiser, mande de novo ou envie texto.");
        return;
      }

      const handledRecebimentosPending = await tryHandleRecebimentosPendingCommands(chatId, transcription);
      if (handledRecebimentosPending) return;

      if (isRecebimentosIntent(transcription, msg)) {
        await handleRecebimentosMessage({
          message: msg,
          text: transcription,
          transcription,
          sendTelegramMessage,
          getTelegramFile,
          downloadTelegramFileBuffer
        });
        return;
      }

      const pending = getPendingBatch(chatId);
      const handledCorrection = await handlePotentialCorrection(chatId, transcription, "audio", pending);
      if (handledCorrection) return;

      const extraction = await extractOrdersFromText(transcription);

      savePendingBatch(chatId, extraction, {
        source: "audio",
        transcription,
        duplicateAwaitingForce: false
      });

      const resumo = summarizeOrders(extraction);

      await sendTelegramMessage(chatId, `Transcrição:\n"${transcription}"\n\n${resumo}`);
      return;
    }

    await sendTelegramMessage(chatId, "Envie texto, áudio ou PDF para eu processar.");
  } catch (error) {
    console.error("Erro no webhook completo:", error);
    console.error("Erro no webhook response data:", error.response?.data);
    console.error("Erro no webhook message:", error.message);

    try {
      const update = req.body || {};
      const msg = update.message || update.edited_message;
      const chatId = msg?.chat?.id;

      if (chatId) {
        await sendTelegramMessage(chatId, "Tive um erro ao processar sua mensagem.");
      }
    } catch (err2) {
      console.error("Erro ao enviar mensagem de falha:", err2.message || err2);
    }
  }
});

app.listen(PORT, () => {
  console.log(`Servidor online na porta ${PORT}`);
});
