const express = require("express");
const axios = require("axios");
const FormData = require("form-data");
const pdfParse = require("pdf-parse");
const {
  isRecebimentosIntent,
  parseRecebimentosIntent
} = require("./intents_recebimentos");
const {
  isPagamentosIntent,
  parsePagamentosIntent
} = require("./intents_pagamentos");
const { callRecebimentosWebApp } = require("./appscript_recebimentos");
const { callPagamentosWebApp } = require("./appscript_pagamentos");
const { callVendasWebApp } = require("./appscript_vendas");

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_TRANSCRIBE_MODEL =
  process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-transcribe";

/**
 * =========================================================
 * ESTADO EM MEMÓRIA
 * =========================================================
 */
const pendingBatches = new Map(); // vendas
const pendingRecebimentos = new Map(); // recebimentos
const pendingPagamentos = new Map(); // pagamentos
const lastPdfContextByChat = new Map();
const activePdfProcessingByChat = new Map();
const queuedItauCommandsByChat = new Map();

/**
 * =========================================================
 * RECEBIMENTOS - ESTADO
 * =========================================================
 */
function savePendingRecebimentos(chatId, lote) {
  pendingRecebimentos.set(String(chatId), lote);
}

function getPendingRecebimentos(chatId) {
  return pendingRecebimentos.get(String(chatId)) || null;
}

function clearPendingRecebimentos(chatId) {
  pendingRecebimentos.delete(String(chatId));
}

function savePendingPagamentos(chatId, lote) {
  pendingPagamentos.set(String(chatId), lote);
}

function getPendingPagamentos(chatId) {
  return pendingPagamentos.get(String(chatId)) || null;
}

function clearPendingPagamentos(chatId) {
  pendingPagamentos.delete(String(chatId));
}

function saveLastPdfContext(chatId, ctx) {
  lastPdfContextByChat.set(String(chatId), {
    ...ctx,
    savedAt: new Date().toISOString()
  });
}

function getLastPdfContext(chatId) {
  return lastPdfContextByChat.get(String(chatId)) || null;
}

function clearLastPdfContext(chatId) {
  lastPdfContextByChat.delete(String(chatId));
}

function markActivePdfProcessing(chatId, ctx = {}) {
  activePdfProcessingByChat.set(String(chatId), {
    ...ctx,
    startedAt: new Date().toISOString()
  });
}

function getActivePdfProcessing(chatId) {
  return activePdfProcessingByChat.get(String(chatId)) || null;
}

function clearActivePdfProcessing(chatId) {
  activePdfProcessingByChat.delete(String(chatId));
}

function getUsableActivePdfProcessing(chatId, maxMinutes = 5) {
  const ctx = getActivePdfProcessing(chatId);
  if (!ctx?.startedAt) return null;
  const startedAt = new Date(ctx.startedAt).getTime();
  if (!Number.isFinite(startedAt)) return null;
  const ageMs = Date.now() - startedAt;
  if (ageMs > maxMinutes * 60 * 1000) return null;
  return ctx;
}

function saveQueuedItauCommand(chatId, queued) {
  queuedItauCommandsByChat.set(String(chatId), {
    ...queued,
    queuedAt: new Date().toISOString()
  });
}

function popQueuedItauCommand(chatId) {
  const key = String(chatId);
  const queued = queuedItauCommandsByChat.get(key) || null;
  queuedItauCommandsByChat.delete(key);
  return queued;
}

function shouldWaitForNewItauPdf(chatId, lastPdf = null) {
  const active = getUsableActivePdfProcessing(chatId);
  if (!active) return false;
  if (!lastPdf?.savedAt) return true;

  const activeStarted = new Date(active.startedAt).getTime();
  const lastSaved = new Date(lastPdf.savedAt).getTime();
  if (!Number.isFinite(activeStarted)) return false;
  if (!Number.isFinite(lastSaved)) return true;

  return activeStarted >= lastSaved;
}

function isFreshLastPdfContext(ctx, maxMinutes = 30) {
  if (!ctx?.savedAt) return false;
  const savedAt = new Date(ctx.savedAt).getTime();
  if (!Number.isFinite(savedAt)) return false;
  const ageMs = Date.now() - savedAt;
  return ageMs <= maxMinutes * 60 * 1000;
}

function getUsableLastPdfContext(chatId, maxMinutes = 30) {
  const ctx = getLastPdfContext(chatId);
  if (!ctx) return null;
  if (!isFreshLastPdfContext(ctx, maxMinutes)) return null;
  return ctx;
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
  const mensagem = String(text || "");
  const partes = splitTelegramMessage_(mensagem);
  let lastResp = null;

  for (let i = 0; i < partes.length; i++) {
    lastResp = await axios.post(telegramApiUrl("sendMessage"), {
      chat_id: chatId,
      text: partes[i]
    });
  }

  return lastResp;
}

function splitTelegramMessage_(text, maxLen = 3500) {
  const mensagem = String(text || "");
  if (!mensagem) return [""];
  if (mensagem.length <= maxLen) return [mensagem];

  const partes = [];
  let restante = mensagem;

  while (restante.length > maxLen) {
    let corte = restante.lastIndexOf("\n", maxLen);
    if (corte < Math.floor(maxLen * 0.6)) {
      corte = restante.lastIndexOf(" ", maxLen);
    }
    if (corte < Math.floor(maxLen * 0.4)) {
      corte = maxLen;
    }

    partes.push(restante.slice(0, corte).trim());
    restante = restante.slice(corte).trim();
  }

  if (restante) partes.push(restante);
  return partes.filter(Boolean);
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
 * PDF - ITAÚ
 * =========================================================
 */
async function extractTextFromPdfBuffer(buffer) {
  const data = await pdfParse(buffer);
  return String(data?.text || "").trim();
}

function looksLikePdfDocument(message = {}) {
  const name = String(message?.document?.file_name || "").toLowerCase();
  const mime = String(message?.document?.mime_type || "").toLowerCase();
  return mime === "application/pdf" || name.endsWith(".pdf");
}

function looksLikeItauStatement(text) {
  const t = normalizeText(text);
  return (
    t.includes("itau") &&
    t.includes("lancamentos do periodo") &&
    t.includes("agencia") &&
    t.includes("conta") &&
    t.includes("pix recebido")
  );
}

function chooseRecebimentosOrigin({ text = "", message = {}, documentText = "" }) {
  const t = normalizeText(text);
  const pdfText = normalizeText(documentText);

  if (looksLikePdfDocument(message) && looksLikeItauStatement(pdfText)) {
    return "itau";
  }

  if (pdfText && looksLikeItauStatement(pdfText)) {
    return "itau";
  }

  if (t.includes("itau") || t.includes("extrato")) {
    return "itau";
  }

  return "inter";
}

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
      "liberar duplicata D1",
      "confirmar lote",
      "cancelar lote",
      "remover 5",
      "Datas podem aparecer como 18/04, 18-04, 19/04, 20/04.",
      "Se houver código como P1, P2, I1, I2, D1, D2, preserve exatamente.",
      "Se houver valor monetário, preserve os números com máxima fidelidade.",
      "Nomes frequentes de clientes e pessoas:",
      "Diergia, Ricardo, Sandro, Larissa, Raquel, Renata, Flávio, Fábio, Diege, Dieergia, Karolaine, Philipe, Izabel, Samara, Eliete, Edilene, Lidiane, Manu."
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

async function extrairRecebimentosItauDoPdfComIA(pdfBuffer, filename = "extrato_itau.pdf") {
  if (!OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY não configurada.");
  }

  const model = process.env.OPENAI_ITAU_PDF_MODEL || "gpt-5";
  const base64Pdf = pdfBuffer.toString("base64");
  const dataUrl = `data:application/pdf;base64,${base64Pdf}`;

  const schema = {
    type: "object",
    additionalProperties: false,
    properties: {
      banco: { type: "string" },
      extrato_detectado: { type: "boolean" },
      observacoes: { type: ["string", "null"] },
      lancamentos: {
        type: "array",
        items: {
          type: "object",
          additionalProperties: false,
          properties: {
            tipo: { type: "string" },
            data_pagamento: { type: ["string", "null"] },
            nome_pagador: { type: ["string", "null"] },
            cpf_cnpj: { type: ["string", "null"] },
            valor: { type: ["number", "null"] },
            linha_resumo: { type: ["string", "null"] }
          },
          required: [
            "tipo",
            "data_pagamento",
            "nome_pagador",
            "cpf_cnpj",
            "valor",
            "linha_resumo"
          ]
        }
      }
    },
    required: ["banco", "extrato_detectado", "observacoes", "lancamentos"]
  };

  const instructions = [
    "Analise este PDF de extrato bancário.",
    "Seu trabalho é identificar se o documento é um extrato do Itaú e extrair os principais lançamentos de entrada e saída úteis para automação.",
    "",
    "Regras obrigatórias:",
    "- Retorne SOMENTE JSON válido no schema pedido.",
    "- banco deve ser 'itau' se o extrato for do Itaú; caso contrário, use 'desconhecido'.",
    "- extrato_detectado = true somente se realmente for extrato bancário.",
    "- Inclua em lancamentos apenas movimentações relevantes para recebimentos e pagamentos.",
    "- Tipos válidos para `tipo`: 'pix_recebido', 'pix_enviado', 'pix_qrcode', 'debito_compra'.",
    "- Use 'pix_recebido' para entradas PIX.",
    "- Use 'pix_enviado' para transferências/pagamentos PIX de saída.",
    "- Use 'pix_qrcode' para pagamento via PIX QR-Code quando isso estiver claro.",
    "- Use 'debito_compra' para compra em débito/cartão/débito em conta quando o extrato indicar gasto/pagamento de saída.",
    "- Ignore saldo, rendimento, depósito em dinheiro, tarifas irrelevantes, cabeçalhos e linhas sem valor claro.",
    "- data_pagamento deve vir em formato ISO YYYY-MM-DD quando possível.",
    "- valor deve ser número decimal, sem símbolo.",
    "- nome_pagador deve ser o melhor nome possível da contraparte, favorecido ou estabelecimento.",
    "- cpf_cnpj pode ser null se não estiver claro.",
    "- linha_resumo deve guardar um resumo curto da linha/origem do extrato.",
    "",
    "Contexto importante:",
    "- O extrato pode estar visualmente quebrado.",
    "- Se houver ruído na tabela, ainda assim tente reconstruir os lançamentos.",
    "- Se houver dúvida, prefira incluir menos itens do que inventar.",
    "",
    "Exemplos de tipos válidos:",
    "- tipo: 'pix_recebido'",
    "- tipo: 'pix_enviado'",
    "- tipo: 'pix_qrcode'",
    "- tipo: 'debito_compra'"
  ].join("\n");

  const payload = {
    model,
    reasoning: {
      effort: "medium"
    },
    input: [
      {
        role: "user",
        content: [
          { type: "input_text", text: instructions },
          {
            type: "input_file",
            filename,
            file_data: dataUrl
          }
        ]
      }
    ],
    text: {
      format: {
        type: "json_schema",
        name: "itau_recebimentos_pdf",
        strict: true,
        schema
      }
    }
  };

  const resp = await axios.post(
    "https://api.openai.com/v1/responses",
    payload,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      timeout: 180000,
      maxBodyLength: Infinity,
      maxContentLength: Infinity
    }
  );

  const data = resp.data || {};
  const outputText = extractResponsesOutputText(data);

  if (!outputText) {
    throw new Error("A OpenAI não retornou texto utilizável para o PDF do Itaú.");
  }

  let parsed;
  try {
    parsed = JSON.parse(outputText);
  } catch (err) {
    throw new Error("A OpenAI retornou JSON inválido para o PDF do Itaú: " + outputText);
  }

  return normalizarExtracaoItauIA(parsed);
}

function extractResponsesOutputText(data) {
  if (data && data.output_text) {
    return String(data.output_text).trim();
  }

  if (Array.isArray(data?.output)) {
    for (const item of data.output) {
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

function normalizarExtracaoItauIA(parsed) {
  const banco = String(parsed?.banco || "").trim().toLowerCase();
  const extratoDetectado = !!parsed?.extrato_detectado;
  const observacoes = parsed?.observacoes == null ? null : String(parsed.observacoes);

  const lancamentos = Array.isArray(parsed?.lancamentos)
    ? parsed.lancamentos
        .map((item) => ({
          tipo: normalizarTipoLancamentoItauIA(item?.tipo),
          data_pagamento: normalizarDataIsoIA(item?.data_pagamento),
          nome_pagador: item?.nome_pagador == null ? "" : String(item.nome_pagador).trim(),
          cpf_cnpj: item?.cpf_cnpj == null ? "" : String(item.cpf_cnpj).trim(),
          valor: item?.valor == null ? null : Number(item.valor),
          linha_resumo: item?.linha_resumo == null ? "" : String(item.linha_resumo).trim()
        }))
        .filter(
          (item) =>
            ["pix_recebido", "pix_enviado", "debito_compra"].includes(item.tipo) &&
            item.data_pagamento &&
            Number.isFinite(item.valor) &&
            item.valor > 0
        )
    : [];

  return {
    banco,
    extrato_detectado: extratoDetectado,
    observacoes,
    lancamentos
  };
}

function normalizarTipoLancamentoItauIA(value) {
  const tipo = String(value || "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");

  if (!tipo) return "";

  if (
    [
      "pix_recebido",
      "pix recebido",
      "recebimento pix",
      "pix de entrada"
    ].includes(tipo)
  ) {
    return "pix_recebido";
  }

  if (
    [
      "pix_qrcode",
      "pix qrcode",
      "pix qr-code",
      "pix qr code",
      "pagamento qr code",
      "pagamento pix qr code",
      "pix via qr code"
    ].includes(tipo)
  ) {
    return "pix_enviado";
  }

  if (
    [
      "pix_enviado",
      "pix enviado",
      "pagamento pix",
      "transferencia pix",
      "transferência pix",
      "pix de saida",
      "pix de saída"
    ].includes(tipo)
  ) {
    return "pix_enviado";
  }

  if (
    [
      "debito_compra",
      "compra no debito",
      "compra no debito:",
      "compra debito",
      "debito compra",
      "compra em debito",
      "compra em débito",
      "debito"
    ].includes(tipo)
  ) {
    return "debito_compra";
  }

  return tipo;
}

function normalizarDataIsoIA(value) {
  const s = String(value || "").trim();
  if (!s) return "";

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    return s;
  }

  const br = s.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (br) {
    return `${br[3]}-${br[2]}-${br[1]}`;
  }

  return "";
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

function formatMoneyBRL(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "R$ ?";
  return `R$ ${n.toFixed(2).replace(".", ",")}`;
}

function uniqueStrings(values) {
  return [...new Set((values || []).filter(Boolean))];
}

function levenshtein(a, b) {
  const s = String(a || "");
  const t = String(b || "");
  const m = s.length;
  const n = t.length;

  if (!m) return n;
  if (!n) return m;

  const dp = Array.from({ length: m + 1 }, () => Array(n + 1).fill(0));

  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;

  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost
      );
    }
  }

  return dp[m][n];
}

function similarityScore(a, b) {
  const x = normalizeText(a);
  const y = normalizeText(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  if (x.includes(y) || y.includes(x)) return 0.94;

  const dist = levenshtein(x, y);
  const maxLen = Math.max(x.length, y.length);
  return maxLen ? 1 - dist / maxLen : 0;
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

function isDuplicateConfirmationText(text) {
  const t = normalizeText(text);
  return (
    t === "confirmar duplicata" ||
    t === "confirme duplicata" ||
    t === "forcar duplicata" ||
    t === "forçar duplicata"
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
 * RECEBIMENTOS - RESOLUÇÃO FLEXÍVEL DE CLIENTE
 * =========================================================
 */
const CLIENTE_ALIAS_MAP = {
  diege: "Diergia",
  diegia: "Diergia",
  dieergia: "Diergia",
  diergia: "Diergia",
  dierja: "Diergia",
  ricardo: "Ricardo",
  sandro: "Sandro"
};

function normalizeClienteOficialAlias(text) {
  const raw = String(text || "").trim();
  const t = normalizeText(raw);
  return CLIENTE_ALIAS_MAP[t] || raw;
}

function limparClienteOficialFalado(text) {
  const cleaned = String(text || "")
    .trim()
    .replace(/^[\s"'`.,;:!?-]+/, "")
    .replace(/[\s"'`.,;:!?-]+$/, "")
    .replace(/^(a|ao|aos|a\s+cliente|cliente)\s+/i, "")
    .trim();

  return normalizeClienteOficialAlias(cleaned);
}

function getKnownClientesOficiaisFromLote(lote) {
  const prontos = Array.isArray(lote?.itens_prontos) ? lote.itens_prontos : [];
  const fromProntos = prontos.map((item) => item.cliente_oficial);
  const fromAliases = Object.values(CLIENTE_ALIAS_MAP);
  return uniqueStrings([...fromProntos, ...fromAliases]);
}

function resolveClienteOficialFlex(clienteFalado, lote) {
  const raw = limparClienteOficialFalado(clienteFalado);
  const normalized = normalizeText(raw);
  if (!normalized) return raw;

  const candidates = getKnownClientesOficiaisFromLote(lote);
  if (!candidates.length) return raw;

  let best = null;
  let bestScore = 0;

  for (const candidate of candidates) {
    const score = similarityScore(normalized, candidate);
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
    }
  }

  if (best && bestScore >= 0.82) {
    return best;
  }

  return raw;
}

function hasMultipleAssociations(text) {
  const matches = normalizeText(text).match(/\bassociar\b/g);
  return Array.isArray(matches) && matches.length > 1;
}

/**
 * =========================================================
 * RECEBIMENTOS - APPS SCRIPT HELPERS
 * =========================================================
 */
async function resolveClienteOficialViaAppsScript(nomeFalado) {
  try {
    const resp = await callRecebimentosWebApp({
      action: "resolver_cliente_oficial_recebimentos",
      origem: "inter",
      nome_falado: nomeFalado
    });

    return resp;
  } catch (err) {
    console.error("Erro ao resolver cliente oficial via Apps Script:", err?.message || err);
    return {
      ok: false,
      encontrado: false,
      cliente_oficial: "",
      message: String(err?.message || err || "Falha ao consultar Apps Script")
    };
  }
}

/**
 * =========================================================
 * RECEBIMENTOS - COMANDOS DE LOTE
 * =========================================================
 */
function parseAssociarPendenciaCommand(text, lote = null) {
  const raw = String(text || "").trim();

  let m = raw.match(/^associar\s+(P\d+)\s+(.+)$/i);
  if (m) {
    return {
      pendenciaId: String(m[1]).toUpperCase(),
      clienteOficial: resolveClienteOficialFlex(m[2], lote)
    };
  }

  m = raw.match(/^associar\s+(.+?)\s+(?:a|ao)\s+(.+)$/i);
  if (m && lote) {
    const nomeFalado = normalizeText(m[1]);
    const clienteOficial = resolveClienteOficialFlex(m[2], lote);

    const pendencias = Array.isArray(lote?.pendencias_associacao)
      ? lote.pendencias_associacao
      : [];

    const found = pendencias.find((p) => {
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

  let m = raw.match(/^remover\s+(\d+)[.!?]?$/i);
  if (m) return { itemNumero: Number(m[1]) };

  m = raw.match(/^remove(?:r)?\s+(?:item\s+)?(\d+)[.!?]?$/i);
  if (m) return { itemNumero: Number(m[1]) };

  return null;
}

function parseLiberarDuplicataCommand(text) {
  const raw = String(text || "").trim();

  let m = raw.match(/^liberar\s+duplicata\s+(D\d+)[.!?]?$/i);
  if (m) {
    return { duplicataId: String(m[1]).toUpperCase() };
  }

  m = raw.match(/^libera(?:r)?\s+(D\d+)[.!?]?$/i);
  if (m) {
    return { duplicataId: String(m[1]).toUpperCase() };
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

function summarizePendingRecebimentos(lote) {
  const prontos = Array.isArray(lote?.itens_prontos) ? lote.itens_prontos : [];
  const pendencias = Array.isArray(lote?.pendencias_associacao)
    ? lote.pendencias_associacao
    : [];
  const ignorados = Array.isArray(lote?.ignorados) ? lote.ignorados : [];
  const duplicados = Array.isArray(lote?.duplicados) ? lote.duplicados : [];
  const jaProcessados = Array.isArray(lote?.ja_processados)
    ? lote.ja_processados
    : [];

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
      let extra = "";
      if (item.force_duplicate) extra = " | duplicata liberada";
      linhas.push(
        `${idx + 1}. ${item.cliente_oficial} | ${item.data_pagamento} | ${formatMoneyBRL(item.valor)}${extra}`
      );
    });
  }

  if (pendencias.length) {
    linhas.push("", "Pendências:");
    pendencias.forEach((item) => {
      let extra = "";
      if (item.gc_ambiguo) extra = " | GC ambíguo";
      else if (item.erro) extra = ` | ${item.erro}`;

      linhas.push(
        `${item.id_local}. ${item.nome_extraido} | ${item.data_pagamento} | ${formatMoneyBRL(item.valor)}${extra}`
      );
    });
  }

  if (duplicados.length) {
    linhas.push("", "Duplicados:");
    duplicados.forEach((item) => {
      const motivo = String(item.motivo || "");
      let origemDup = "duplicado";

      if (motivo === "duplicado_gc") origemDup = "GC";
      else if (motivo === "duplicado_drive_log") origemDup = "Drive log";

      const cliente = item.cliente_oficial || item.nome_extraido || "?";
      const data = item.data_pagamento || "?";
      const valor = formatMoneyBRL(item.valor);

      let extra = "";
      if (item.gc_recebimento_codigo || item.gc_recebimento_id) {
        extra =
          " | cód " + (item.gc_recebimento_codigo || "?") +
          " | id " + (item.gc_recebimento_id || "?");
      }

      linhas.push(
        `${item.id_local}. ${origemDup} | ${cliente} | ${data} | ${valor}${extra}`
      );
    });
  }

  if (ignorados.length) {
    linhas.push("", "Ignorados:");
    ignorados.slice(0, 8).forEach((item, idx) => {
      const nome = item.nome_extraido || item.descricao_extraida || "?";
      const data = item.data_pagamento || "?";
      const valor = formatMoneyBRL(item.valor);
      const motivo = item.motivo || "ignorado";
      linhas.push(`${idx + 1}. ${nome} | ${data} | ${valor} | ${motivo}`);
    });
    if (ignorados.length > 8) {
      linhas.push(`... e mais ${ignorados.length - 8} ignorado(s)`);
    }
  }

  linhas.push("", "Comandos:");
  linhas.push("- associar P1 NOME_DO_CLIENTE_OFICIAL");
  linhas.push("- ou: associar NOME_DA_PENDENCIA a NOME_DO_CLIENTE_OFICIAL");
  linhas.push("- liberar duplicata D1");
  linhas.push("- remover 5");
  linhas.push("- confirmar lote");
  linhas.push("- cancelar lote");
  linhas.push("");
  linhas.push("Exemplos:");
  linhas.push("- associar P1 Diergia");
  linhas.push("- associar Karolaine a Ricardo");
  linhas.push("- liberar duplicata D1");
  linhas.push("");
  linhas.push("Faça uma associação por vez.");
  linhas.push("Você pode usar qualquer cliente oficial do MAPA_CLIENTES.");

  return linhas.join("\n");
}

function buildForcedItemFromDuplicate(lote, duplicado) {
  return {
    id_local: `I${(lote.itens_prontos?.length || 0) + 1}`,
    cliente_oficial: duplicado.cliente_oficial || duplicado.nome_extraido || "",
    nome_extraido: duplicado.nome_extraido || duplicado.cliente_oficial || "",
    data_pagamento: duplicado.data_pagamento || "",
    valor: duplicado.valor,
    forma: "PIX",
    conta_oficial: lote.origem === "itau" ? "Itaú Empresas" : "Inter Empresas",
    banco_extraido: duplicado.banco_extraido || "",
    id_transacao: duplicado.id_transacao || null,
    assunto_email: duplicado.assunto_email || "",
    remetente: duplicado.remetente || "",
    message_id: duplicado.message_id || "",
    status: "pronto",
    force_duplicate: true,
    duplicate_source: duplicado.motivo || "duplicado_gc",
    duplicate_reference: {
      gc_recebimento_id: duplicado.gc_recebimento_id || null,
      gc_recebimento_codigo: duplicado.gc_recebimento_codigo || null
    }
  };
}

async function tryHandleRecebimentosPendingCommands(chatId, text) {
  const lote = getPendingRecebimentos(chatId);
  if (!lote) return false;

  if (hasMultipleAssociations(text)) {
    await sendTelegramMessage(
      chatId,
      "Faça uma associação por vez. Exemplo: associar Karolaine a Ricardo"
    );
    return true;
  }

  const liberarDuplicata = parseLiberarDuplicataCommand(text);
  if (liberarDuplicata) {
    const duplicados = Array.isArray(lote.duplicados) ? lote.duplicados : [];
    const idx = duplicados.findIndex(
      (d) => String(d.id_local || "").toUpperCase() === liberarDuplicata.duplicataId
    );

    if (idx < 0) {
      await sendTelegramMessage(chatId, `Não encontrei a duplicata ${liberarDuplicata.duplicataId}.`);
      return true;
    }

    const duplicado = duplicados[idx];
    const itemPronto = buildForcedItemFromDuplicate(lote, duplicado);

    lote.duplicados.splice(idx, 1);
    lote.itens_prontos.push(itemPronto);
    lote.historico_comandos.push({
      tipo: "liberacao_duplicata",
      duplicata_id: liberarDuplicata.duplicataId,
      cliente_oficial: itemPronto.cliente_oficial,
      em: new Date().toISOString()
    });

    savePendingRecebimentos(chatId, lote);

    await sendTelegramMessage(
      chatId,
      [
        "Duplicata liberada manualmente:",
        `${liberarDuplicata.duplicataId} -> ${itemPronto.cliente_oficial} | ${itemPronto.data_pagamento} | ${formatMoneyBRL(itemPronto.valor)}`,
        "",
        summarizePendingRecebimentos(lote)
      ].join("\n")
    );
    return true;
  }

  const associar = parseAssociarPendenciaCommand(text, lote);
  if (associar) {
    if (!associar.clienteOficial) {
      await sendTelegramMessage(chatId, "Não consegui identificar o cliente oficial.");
      return true;
    }

    const resolved = await resolveClienteOficialViaAppsScript(associar.clienteOficial);

    if (resolved?.ok && resolved?.encontrado && resolved?.cliente_oficial) {
      associar.clienteOficial = resolved.cliente_oficial;
    }

    const pendencias = Array.isArray(lote?.pendencias_associacao)
      ? lote.pendencias_associacao
      : [];
    const idx = pendencias.findIndex(
      (p) => String(p.id_local || "").toUpperCase() === associar.pendenciaId
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

    let result = await callRecebimentosWebApp(payload);
    console.log("Recebimentos associar result:", JSON.stringify(result));

    if (!result?.ok) {
      const secondTry = await resolveClienteOficialViaAppsScript(
        limparClienteOficialFalado(associar.clienteOficial)
      );

      if (secondTry?.ok && secondTry?.encontrado && secondTry?.cliente_oficial) {
        payload.cliente_oficial = secondTry.cliente_oficial;
        associar.clienteOficial = secondTry.cliente_oficial;

        result = await callRecebimentosWebApp(payload);
        console.log("Recebimentos associar retry result:", JSON.stringify(result));
      }
    }

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
      source_kind: pendencia.source_kind || "",
      attachment_name: pendencia.attachment_name || "",
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
 * RECEBIMENTOS - FLUXO ITAÚ
 * =========================================================
 */
function buildSyntheticMessageWithDocument(msg, fileName = "extrato_itau.pdf") {
  return {
    ...msg,
    document: {
      file_name: fileName,
      mime_type: "application/pdf"
    }
  };
}

async function flushQueuedItauCommandIfAny({
  chatId,
  sendTelegramMessage,
  documentText = "",
  documentJson = null,
  fileName = ""
}) {
  const queued = popQueuedItauCommand(chatId);
  if (!queued) return false;

  if (!documentJson?.extrato_detectado) {
    await sendTelegramMessage(
      chatId,
      "Recebi o PDF do Itaú, mas não consegui estruturar o extrato com segurança. Então não consegui continuar o comando automaticamente."
    );
    return true;
  }

  await sendTelegramMessage(
    chatId,
    "Terminei de preparar o PDF do Itaú. Vou continuar automaticamente o comando que você acabou de enviar."
  );

  const syntheticMsg = buildSyntheticMessageWithDocument(
    queued.message,
    fileName || queued?.message?.document?.file_name || "extrato_itau.pdf"
  );

  if (queued.kind === "pagamentos") {
    await handlePagamentosMessage({
      message: syntheticMsg,
      text: queued.text,
      transcription: queued.transcription,
      sendTelegramMessage,
      documentText,
      documentJson,
      documentFileName: fileName || "extrato_itau.pdf"
    });
    return true;
  }

  if (queued.kind === "recebimentos") {
    await handleRecebimentosMessage({
      message: syntheticMsg,
      text: queued.text,
      transcription: queued.transcription,
      sendTelegramMessage,
      documentText,
      documentJson
    });
    return true;
  }

  return false;
}

async function handleRecebimentosMessage(ctx) {
  const {
    message,
    text,
    sendTelegramMessage,
    transcription,
    documentText = "",
    documentJson = null
  } = ctx;

  const chatId = message.chat.id;
  const parsed = parseRecebimentosIntent(text, message);
  parsed.origem = chooseRecebimentosOrigin({
    text,
    message,
    documentText
  });

  if (transcription) {
    await sendTelegramMessage(chatId, `Transcrição:\n"${transcription}"`);
  }

  if (parsed.origem === "itau" && !documentJson) {
    saveQueuedItauCommand(chatId, {
      kind: "recebimentos",
      message,
      text,
      transcription: transcription || ""
    });

    await sendTelegramMessage(
      chatId,
      "Ainda não tenho um PDF do Itaú pronto para usar. Se você acabou de enviar o extrato, vou esperar um instante e continuar automaticamente. Se não, envie o PDF do extrato."
    );
    return;
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
      has_document: !!message.document || !!documentJson || !!documentText,
      document_text: documentText || "",
      document_json: documentJson
    },
    message_meta: {
      message_id: message.message_id || null,
      date: message.date || null
    }
  };

  const result = await callRecebimentosWebApp(payload);
  console.log("Recebimentos result:", JSON.stringify(result));

  if (result?.modo === "pre_visualizacao") {
    const duplicadosComId = (result.duplicados || []).map((d, idx) => ({
      ...d,
      id_local: `D${idx + 1}`
    }));

    const lote = {
      tipo: "recebimentos_lote_pendente",
      origem: result.origem || parsed.origem,
      periodo: result.periodo || parsed.periodo,
      criadoEm: new Date().toISOString(),
      resumoOrigem: {
        processados_detectados: (result.itens_prontos || []).length,
        ignorados: (result.ignorados || []).length,
        duplicados: duplicadosComId.length,
        ja_processados: (result.ja_processados || []).length,
        erros: (result.pendencias_associacao || []).length
      },
      itens_prontos: result.itens_prontos || [],
      pendencias_associacao: result.pendencias_associacao || [],
      ignorados: result.ignorados || [],
      duplicados: duplicadosComId,
      ja_processados: result.ja_processados || [],
      historico_comandos: []
    };

    savePendingRecebimentos(chatId, lote);
    await sendTelegramMessage(chatId, summarizePendingRecebimentos(lote));
    return;
  }

  if (!result?.ok && result?.modo !== "pre_visualizacao") {
    await sendTelegramMessage(
      chatId,
      result?.message || result?.error || "Não consegui processar os recebimentos."
    );
    return;
  }

  const resumo = result?.message || "Recebimentos processados.";
  await sendTelegramMessage(chatId, resumo);
}

/**
 * =========================================================
 * PAGAMENTOS - HELPERS
 * =========================================================
 */
function limparPlanoContasFalado(text) {
  return String(text || "")
    .trim()
    .replace(/^[\s"'`.,;:!?-]+/, "")
    .replace(/[\s"'`.,;:!?-]+$/, "")
    .replace(/^(o|a|plano|plano de contas)\s+/i, "")
    .trim();
}

async function resolvePlanoContasViaAppsScript(nomeFalado) {
  try {
    const resp = await callPagamentosWebApp({
      action: "resolver_plano_pagamentos",
      nome_falado: nomeFalado
    });

    return resp;
  } catch (err) {
    console.error("Erro ao resolver plano via Apps Script:", err?.message || err);
    return {
      ok: false,
      encontrado: false,
      plano_contas: "",
      message: String(err?.message || err || "Falha ao consultar Apps Script")
    };
  }
}

function parseAssociarPagamentoCommand(text, lote = null) {
  const raw = String(text || "").trim();

  let m = raw.match(/^associar\s+(P\d+)\s+(.+)$/i);
  if (m) {
    return {
      pendenciaId: String(m[1]).toUpperCase(),
      planoContas: limparPlanoContasFalado(m[2])
    };
  }

  m = raw.match(/^associar\s+(.+?)\s+(?:a|ao)\s+(.+)$/i);
  if (m && lote) {
    const nomeFalado = normalizeText(m[1]);
    const planoContas = limparPlanoContasFalado(m[2]);

    const pendencias = Array.isArray(lote?.pendencias_associacao)
      ? lote.pendencias_associacao
      : [];

    const found = pendencias.find((p) => {
      const nomeExtraido = normalizeText(
        p.nome_extraido || p.descricao_extraida || ""
      );
      return nomeExtraido.includes(nomeFalado) || nomeFalado.includes(nomeExtraido);
    });

    if (found) {
      return {
        pendenciaId: String(found.id_local).toUpperCase(),
        planoContas
      };
    }
  }

  return null;
}

function summarizePendingPagamentos(lote) {
  const prontos = Array.isArray(lote?.itens_prontos) ? lote.itens_prontos : [];
  const pendencias = Array.isArray(lote?.pendencias_associacao)
    ? lote.pendencias_associacao
    : [];
  const ignorados = Array.isArray(lote?.ignorados) ? lote.ignorados : [];
  const duplicados = Array.isArray(lote?.duplicados) ? lote.duplicados : [];
  const jaProcessados = Array.isArray(lote?.ja_processados)
    ? lote.ja_processados
    : [];

  const linhas = [];
  linhas.push(`Pagamentos ${String(lote?.origem || "").toUpperCase()} encontrados.`);
  linhas.push("");
  if (lote?.fonte_dados) {
    linhas.push(`Fonte: ${String(lote.fonte_dados)}`);
  }
  if (lote?.attachment_name) {
    linhas.push(`Extrato: ${String(lote.attachment_name)}`);
  }
  if (lote?.mensagem_origem) {
    linhas.push(`Observação: ${String(lote.mensagem_origem)}`);
  }
  if (lote?.observacoes_extrato) {
    linhas.push(`Leitura do extrato: ${String(lote.observacoes_extrato)}`);
  }
  linhas.push(`Período: ${lote?.periodo?.label || "não informado"}`);
  linhas.push(`Prontos para preencher: ${prontos.length}`);
  linhas.push(`Pendências: ${pendencias.length}`);
  linhas.push(`Ignorados: ${ignorados.length}`);
  linhas.push(`Já processados: ${jaProcessados.length}`);
  linhas.push(`Duplicados: ${duplicados.length}`);

  if (prontos.length) {
    linhas.push("", "Prontos:");
    prontos.forEach((item, idx) => {
      let extra = "";
      if (item.force_duplicate) extra = " | duplicata liberada";
      linhas.push(
        `${idx + 1}. ${item.plano_contas} | ${item.data_pagamento} | ${formatMoneyBRL(item.valor)}${extra}`
      );
    });
  }

  if (pendencias.length) {
    linhas.push("", "Pendências:");
    pendencias.forEach((item) => {
      let extra = "";
      if (item.gc_ambiguo) extra = " | GC ambíguo";
      else if (item.erro) extra = ` | ${item.erro}`;

      linhas.push(
        `${item.id_local}. ${item.nome_extraido || item.descricao_extraida || "?"} | ${item.data_pagamento || "?"} | ${formatMoneyBRL(item.valor)}${extra}`
      );
    });
  }

  if (duplicados.length) {
    linhas.push("", "Duplicados:");
    duplicados.forEach((item) => {
      const motivo = String(item.motivo || "");
      let origemDup = "duplicado";

      if (motivo === "duplicado_gc") origemDup = "GC";
      else if (motivo === "duplicado_drive_log") origemDup = "Drive log";

      const plano = item.plano_contas || item.nome_extraido || "?";
      const data = item.data_pagamento || "?";
      const valor = formatMoneyBRL(item.valor);

      let extra = "";
      if (item.gc_pagamento_codigo || item.gc_pagamento_id) {
        extra =
          " | cód " + (item.gc_pagamento_codigo || "?") +
          " | id " + (item.gc_pagamento_id || "?");
      }

      linhas.push(
        `${item.id_local}. ${origemDup} | ${plano} | ${data} | ${valor}${extra}`
      );
    });
  }

  linhas.push("", "Comandos:");
  linhas.push("- associar P1 NOME_DO_PLANO_DE_CONTAS");
  linhas.push("- ou: associar NOME_DA_PENDENCIA a NOME_DO_PLANO_DE_CONTAS");
  linhas.push("- liberar duplicata D1");
  linhas.push("- remover 1");
  linhas.push("- confirmar lote");
  linhas.push("- cancelar lote");
  linhas.push("");
  linhas.push("Exemplos:");
  linhas.push("- associar P1 Salario Philipe");
  linhas.push("- associar Mercado X a Compras Diversas");
  linhas.push("- liberar duplicata D1");

  return linhas.join("\n");
}

function buildForcedPagamentoFromDuplicate(lote, duplicado) {
  return {
    id_local: `I${(lote.itens_prontos?.length || 0) + 1}`,
    plano_contas: duplicado.plano_contas || duplicado.nome_extraido || "",
    descricao_pagamento:
      duplicado.plano_contas || duplicado.descricao_extraida || duplicado.nome_extraido || "",
    nome_extraido: duplicado.nome_extraido || "",
    descricao_extraida: duplicado.descricao_extraida || "",
    data_pagamento: duplicado.data_pagamento || "",
    data_compensacao: duplicado.data_pagamento || "",
    vencimento: duplicado.data_pagamento || "",
    valor: duplicado.valor,
    forma: "PIX",
    conta_oficial: lote.origem === "itau" ? "Itaú Empresas" : "Inter Empresas",
    quitado: "Sim",
    banco_extraido: duplicado.banco_extraido || "",
    cpf_cnpj: duplicado.cpf_cnpj || "",
    id_transacao: duplicado.id_transacao || null,
    assunto_email: duplicado.assunto_email || "",
    remetente: duplicado.remetente || "",
    message_id: duplicado.message_id || "",
    source_kind: duplicado.source_kind || "",
    attachment_name: duplicado.attachment_name || "",
    status: "pronto",
    force_duplicate: true,
    duplicate_source: duplicado.motivo || "duplicado_gc",
    duplicate_reference: {
      gc_pagamento_id: duplicado.gc_pagamento_id || null,
      gc_pagamento_codigo: duplicado.gc_pagamento_codigo || null
    }
  };
}

async function tryHandlePagamentosPendingCommands(chatId, text) {
  const lote = getPendingPagamentos(chatId);
  if (!lote) return false;

  if (hasMultipleAssociations(text)) {
    await sendTelegramMessage(
      chatId,
      "Faça uma associação por vez. Exemplo: associar Mercado X a Compras Diversas"
    );
    return true;
  }

  const liberarDuplicata = parseLiberarDuplicataCommand(text);
  if (liberarDuplicata) {
    const duplicados = Array.isArray(lote.duplicados) ? lote.duplicados : [];
    const idx = duplicados.findIndex(
      (d) => String(d.id_local || "").toUpperCase() === liberarDuplicata.duplicataId
    );

    if (idx < 0) {
      await sendTelegramMessage(chatId, `Não encontrei a duplicata ${liberarDuplicata.duplicataId}.`);
      return true;
    }

    const duplicado = duplicados[idx];
    const itemPronto = buildForcedPagamentoFromDuplicate(lote, duplicado);

    lote.duplicados.splice(idx, 1);
    lote.itens_prontos.push(itemPronto);
    lote.historico_comandos.push({
      tipo: "liberacao_duplicata",
      duplicata_id: liberarDuplicata.duplicataId,
      plano_contas: itemPronto.plano_contas,
      em: new Date().toISOString()
    });

    savePendingPagamentos(chatId, lote);

    await sendTelegramMessage(
      chatId,
      [
        "Duplicata liberada manualmente:",
        `${liberarDuplicata.duplicataId} -> ${itemPronto.plano_contas} | ${itemPronto.data_pagamento} | ${formatMoneyBRL(itemPronto.valor)}`,
        "",
        summarizePendingPagamentos(lote)
      ].join("\n")
    );
    return true;
  }

  const associar = parseAssociarPagamentoCommand(text, lote);
  if (associar) {
    if (!associar.planoContas) {
      await sendTelegramMessage(chatId, "Não consegui identificar o plano de contas.");
      return true;
    }

    const resolved = await resolvePlanoContasViaAppsScript(associar.planoContas);

    if (resolved?.ok && resolved?.encontrado && resolved?.plano_contas) {
      associar.planoContas = resolved.plano_contas;
    }

    const pendencias = Array.isArray(lote?.pendencias_associacao)
      ? lote.pendencias_associacao
      : [];
    const idx = pendencias.findIndex(
      (p) => String(p.id_local || "").toUpperCase() === associar.pendenciaId
    );

    if (idx < 0) {
      await sendTelegramMessage(chatId, `Não encontrei a pendência ${associar.pendenciaId}.`);
      return true;
    }

    const pendencia = pendencias[idx];
    const payload = {
      action: "associar_pendencia_pagamentos",
      origem: lote.origem,
      pendencia_id: pendencia.id_local,
      nome_extraido: pendencia.nome_extraido || pendencia.descricao_extraida || "",
      plano_contas: associar.planoContas
    };

    let result = await callPagamentosWebApp(payload);
    console.log("Pagamentos associar result:", JSON.stringify(result));

    if (!result?.ok) {
      const secondTry = await resolvePlanoContasViaAppsScript(
        limparPlanoContasFalado(associar.planoContas)
      );

      if (secondTry?.ok && secondTry?.encontrado && secondTry?.plano_contas) {
        payload.plano_contas = secondTry.plano_contas;
        associar.planoContas = secondTry.plano_contas;
        result = await callPagamentosWebApp(payload);
        console.log("Pagamentos associar retry result:", JSON.stringify(result));
      }
    }

    if (!result?.ok) {
      await sendTelegramMessage(
        chatId,
        result?.message || "Não consegui salvar a associação."
      );
      return true;
    }

    const itemPronto = {
      id_local: `I${(lote.itens_prontos?.length || 0) + 1}`,
      plano_contas: associar.planoContas,
      descricao_pagamento: associar.planoContas,
      nome_extraido: pendencia.nome_extraido || "",
      descricao_extraida: pendencia.descricao_extraida || "",
      data_pagamento: pendencia.data_pagamento,
      data_compensacao: pendencia.data_compensacao || pendencia.data_pagamento,
      vencimento: pendencia.vencimento || pendencia.data_pagamento,
      valor: pendencia.valor,
      forma: pendencia.forma || "PIX",
      conta_oficial: pendencia.conta_oficial || "Inter Empresas",
      quitado: "Sim",
      banco_extraido: pendencia.banco_extraido || "",
      cpf_cnpj: pendencia.cpf_cnpj || "",
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
      plano_contas: associar.planoContas,
      em: new Date().toISOString()
    });

    savePendingPagamentos(chatId, lote);

    await sendTelegramMessage(
      chatId,
      [
        "Associação salva:",
        `${pendencia.nome_extraido || pendencia.descricao_extraida || "pendência"} -> ${associar.planoContas}`,
        "",
        summarizePendingPagamentos(lote)
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
      plano_contas: removido?.plano_contas || "",
      em: new Date().toISOString()
    });

    savePendingPagamentos(chatId, lote);

    await sendTelegramMessage(
      chatId,
      [
        "Item removido do lote:",
        `${remover.itemNumero}. ${removido.plano_contas} | ${removido.data_pagamento} | ${formatMoneyBRL(removido.valor)}`,
        "",
        summarizePendingPagamentos(lote)
      ].join("\n")
    );
    return true;
  }

  if (isCancelarRecebimentosCommand(text)) {
    clearPendingPagamentos(chatId);
    await sendTelegramMessage(chatId, "Lote de pagamentos cancelado.");
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
      action: "confirmar_lote_pagamentos",
      origem: lote.origem,
      itens: lote.itens_prontos || []
    };

    const result = await callPagamentosWebApp(payload);
    console.log("Pagamentos confirmar result:", JSON.stringify(result));

    if (!result?.ok) {
      await sendTelegramMessage(
        chatId,
        result?.message || "Não consegui confirmar o lote de pagamentos."
      );
      return true;
    }

    clearPendingPagamentos(chatId);
    await sendTelegramMessage(chatId, result?.message || "Lote confirmado com sucesso.");
    return true;
  }

  return false;
}

async function handlePagamentosMessage(ctx) {
  const {
    message,
    text,
    sendTelegramMessage,
    transcription,
    documentText = "",
    documentJson = null,
    documentFileName = ""
  } = ctx;
  const chatId = message.chat.id;
  const parsed = parsePagamentosIntent(text, message);
  const lastPdf = getUsableLastPdfContext(chatId);
  const textoNormalizado = normalizeText(text);
  const origemExplicita =
    textoNormalizado.includes("inter") ||
    textoNormalizado.includes("itau") ||
    textoNormalizado.includes("itaú");

  if (!origemExplicita && !documentJson && lastPdf?.origem === "itau" && lastPdf?.documentJson) {
    parsed.origem = "itau";
  }

  if (transcription) {
    await sendTelegramMessage(chatId, `Transcrição:\n"${transcription}"`);
  }

  if (parsed.origem === "itau") {
    const shouldWaitForPdf =
      !documentJson && shouldWaitForNewItauPdf(chatId, lastPdf);

    if (shouldWaitForPdf) {
      saveQueuedItauCommand(chatId, {
        kind: "pagamentos",
        message,
        text,
        transcription: transcription || ""
      });

      await sendTelegramMessage(
        chatId,
        "Estou terminando de preparar o PDF do Itaú que você acabou de enviar. Assim que ele ficar pronto, continuo esse comando automaticamente."
      );
      return;
    }

    const pdfCtx = documentJson
      ? {
          documentText,
          documentJson,
          fileName: documentFileName || message?.document?.file_name || ""
        }
      : lastPdf?.origem === "itau"
      ? {
          documentText: lastPdf?.documentText || "",
          documentJson: lastPdf?.documentJson || null,
          fileName: lastPdf?.file_name || ""
        }
      : null;

    if (!pdfCtx?.documentJson) {
      saveQueuedItauCommand(chatId, {
        kind: "pagamentos",
        message,
        text,
        transcription: transcription || ""
      });

      await sendTelegramMessage(
        chatId,
        "Ainda não tenho um PDF do Itaú pronto para usar. Se você acabou de enviar o extrato, vou esperar um instante e continuar automaticamente. Se não, envie o PDF do extrato."
      );
      return;
    }

    await sendTelegramMessage(
      chatId,
      `Entendi. Vou processar pagamentos de ITAÚ para ${parsed.periodo.label}.`
    );

    await sendTelegramMessage(
      chatId,
      "Montando a prévia dos pagamentos, aguarde um instante..."
    );

    const result = await callPagamentosWebApp({
      action: "processar_pagamentos_v1",
      origem: parsed.origem,
      periodo: parsed.periodo,
      telegram: {
        chat_id: chatId,
        document_text: pdfCtx.documentText || "",
        document_json: pdfCtx.documentJson || null,
        document_file_name: pdfCtx.fileName || ""
      },
      message_meta: {
        message_id: message.message_id || null,
        date: message.date || null
      }
    });

    console.log("Pagamentos result:", JSON.stringify(result));

    if (result?.modo === "pre_visualizacao") {
      const duplicadosComId = (result.duplicados || []).map((d, idx) => ({
        ...d,
        id_local: `D${idx + 1}`
      }));

      const lote = {
        tipo: "pagamentos_lote_pendente",
        origem: result.origem || parsed.origem,
        fonte_dados: result.fonte_dados || "",
        attachment_name: result.attachment_name || "",
        mensagem_origem: result.message || "",
        observacoes_extrato: result.observacoes_extrato || "",
        periodo: result.periodo || parsed.periodo,
        criadoEm: new Date().toISOString(),
        resumoOrigem: {
          processados_detectados: (result.itens_prontos || []).length,
          ignorados: (result.ignorados || []).length,
          duplicados: duplicadosComId.length,
          ja_processados: (result.ja_processados || []).length,
          erros: (result.pendencias_associacao || []).length
        },
        itens_prontos: result.itens_prontos || [],
        pendencias_associacao: result.pendencias_associacao || [],
        ignorados: result.ignorados || [],
        duplicados: duplicadosComId,
        ja_processados: result.ja_processados || [],
        historico_comandos: []
      };

      savePendingPagamentos(chatId, lote);
      await sendTelegramMessage(chatId, summarizePendingPagamentos(lote));
      return;
    }

    if (!result?.ok && result?.modo !== "pre_visualizacao") {
      await sendTelegramMessage(
        chatId,
        result?.message || result?.error || "Não consegui processar os pagamentos."
      );
      return;
    }

    const resumo = result?.message || "Pagamentos processados.";
    await sendTelegramMessage(chatId, resumo);
    return;
  }

  await sendTelegramMessage(
    chatId,
    `Entendi. Vou processar pagamentos de ${parsed.origem.toUpperCase()} para ${parsed.periodo.label}.`
  );

  await sendTelegramMessage(
    chatId,
    "Montando a prévia dos pagamentos, aguarde um instante..."
  );

  const result = await callPagamentosWebApp({
    action: "processar_pagamentos_v1",
    origem: parsed.origem,
    periodo: parsed.periodo,
    telegram: {
      chat_id: chatId
    },
    message_meta: {
      message_id: message.message_id || null,
      date: message.date || null
    }
  });

  console.log("Pagamentos result:", JSON.stringify(result));

  if (result?.modo === "pre_visualizacao") {
    const duplicadosComId = (result.duplicados || []).map((d, idx) => ({
      ...d,
      id_local: `D${idx + 1}`
    }));

    const lote = {
      tipo: "pagamentos_lote_pendente",
      origem: result.origem || parsed.origem,
      fonte_dados: result.fonte_dados || "",
      attachment_name: result.attachment_name || "",
      mensagem_origem: result.message || "",
      observacoes_extrato: result.observacoes_extrato || "",
      periodo: result.periodo || parsed.periodo,
      criadoEm: new Date().toISOString(),
      resumoOrigem: {
        processados_detectados: (result.itens_prontos || []).length,
        ignorados: (result.ignorados || []).length,
        duplicados: duplicadosComId.length,
        ja_processados: (result.ja_processados || []).length,
        erros: (result.pendencias_associacao || []).length
      },
      itens_prontos: result.itens_prontos || [],
      pendencias_associacao: result.pendencias_associacao || [],
      ignorados: result.ignorados || [],
      duplicados: duplicadosComId,
      ja_processados: result.ja_processados || [],
      historico_comandos: []
    };

    savePendingPagamentos(chatId, lote);
    await sendTelegramMessage(chatId, summarizePendingPagamentos(lote));
    return;
  }

  if (!result?.ok && result?.modo !== "pre_visualizacao") {
    await sendTelegramMessage(
      chatId,
      result?.message || result?.error || "Não consegui processar os pagamentos."
    );
    return;
  }

  await sendTelegramMessage(chatId, result?.message || "Pagamentos processados.");
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

  if (t === "hoje") return formatDateToIso(now);
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
  if (payment) context.forma_pagamento_falada = payment;

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
      return { ok: false, message: "Não consegui entender o novo lote completo." };
    }

    return {
      ok: true,
      extraction: { ...novo, pedidos: novosPedidos },
      message: `Substituí o lote inteiro por ${novosPedidos.length} pedido(s).`
    };
  }

  if (correction?.action === "replace_item_fields") {
    const idxReplace = Number(correction?.item_index || 0) - 1;
    if (idxReplace < 0 || idxReplace >= pedidos.length) {
      return { ok: false, message: "Não consegui identificar qual item substituir." };
    }

    const replacement = normalizeSinglePedido(correction?.replacement_pedido || {});
    pedidos[idxReplace] = replacement;

    return {
      ok: true,
      extraction: novo,
      message: `Substituí o item ${idxReplace + 1} pelo novo conteúdo.`
    };
  }

  const idx = Number(correction?.item_index || 0) - 1;
  if (idx < 0 || idx >= pedidos.length) {
    return { ok: false, message: "Não consegui identificar qual item corrigir." };
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

  return { ok: false, message: "Não reconheci a ação de correção." };
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

function buildVendasPayloadFromExtraction(extraction, forceDuplicateConfirmed = false) {
  const pedidos = Array.isArray(extraction?.pedidos) ? extraction.pedidos : [];
  return {
    action: "preencher_lote_v1",
    pedidos,
    ...(forceDuplicateConfirmed ? { force_duplicate_confirmed: true } : {})
  };
}

async function confirmPendingSales(chatId, pending, forceDuplicateConfirmed = false) {
  const extraction = normalizeExtraction(pending?.extraction || { pedidos: [] });
  const pedidos = Array.isArray(extraction?.pedidos) ? extraction.pedidos : [];

  if (!pedidos.length) {
    clearPendingBatch(chatId);
    await sendTelegramMessage(chatId, "Não há pedidos pendentes válidos para confirmar.");
    return true;
  }

  const payload = buildVendasPayloadFromExtraction(extraction, forceDuplicateConfirmed);
  const gsResp = await callVendasWebApp(payload);

  if (gsResp?.possible_duplicate && !forceDuplicateConfirmed) {
    savePendingBatch(chatId, extraction, {
      ...(pending?.meta || {}),
      duplicateAwaitingForce: true
    });

    const duplicatas = Array.isArray(gsResp?.resultados)
      ? gsResp.resultados.filter((r) => r && r.possible_duplicate)
      : [];

    const linhas = [];
    linhas.push("Encontrei possível duplicata no Google.");
    linhas.push("Revise antes de forçar.");
    linhas.push("");

    duplicatas.slice(0, 10).forEach((r, idx) => {
      linhas.push(
        `${idx + 1}. ${r.cliente_oficial || "?"} | ${r.produto_oficial || "?"} | ${r.data_venda || "?"} | ${r.quantidade_gramas || "?"}g`
      );
    });

    linhas.push("");
    linhas.push("Se quiser gravar mesmo assim, responda: confirmar duplicata");
    linhas.push("Se quiser cancelar, responda: cancelar");

    await sendTelegramMessage(chatId, linhas.join("\n"));
    return true;
  }

  if (!gsResp?.ok) {
    await sendTelegramMessage(
      chatId,
      `O lote foi confirmado, mas houve falha ao enviar ao Google.\n\nResposta: ${JSON.stringify(gsResp).slice(0, 3500)}`
    );
    return true;
  }

  clearPendingBatch(chatId);
  await sendTelegramMessage(chatId, formatGoogleSuccessMessage(gsResp));
  return true;
}

async function tryHandlePendingSalesCommands(chatId, text) {
  const pending = getPendingBatch(chatId);
  if (!pending) return false;

  const duplicateAwaitingForce = !!pending?.meta?.duplicateAwaitingForce;

  if (isCancelText(text)) {
    clearPendingBatch(chatId);
    await sendTelegramMessage(chatId, "Lote de vendas cancelado.");
    return true;
  }

  if (duplicateAwaitingForce && isDuplicateConfirmationText(text)) {
    return confirmPendingSales(chatId, pending, true);
  }

  if (isConfirmationText(text)) {
    return confirmPendingSales(chatId, pending, false);
  }

  return false;
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

    // =====================================================
    // DOCUMENTO / PDF
    // =====================================================
    if (msg.document) {
      const explicitRecebimentosIntentInCaption = !!text && isRecebimentosIntent(text, msg);
      const explicitPagamentosIntentInCaption = !!text && isPagamentosIntent(text, msg);

      if (!looksLikePdfDocument(msg)) {
        await sendTelegramMessage(
          chatId,
          "Recebi um arquivo. Se isso for um extrato de recebimentos, envie em PDF."
        );
        return;
      }

      markActivePdfProcessing(chatId, {
        file_name: msg.document.file_name || "",
        message_id: msg.message_id || null
      });

      const fileId = msg.document.file_id;
        try {
          const fileInfo = await getTelegramFile(fileId);
          const pdfBuffer = await downloadTelegramFileBuffer(fileInfo.file_path);

          let documentText = "";
          let documentJson = null;

          try {
            documentText = await extractTextFromPdfBuffer(pdfBuffer);
          } catch (err) {
            console.error("Falha ao extrair texto do PDF:", err?.message || err);
          }

          const itauDetected =
            looksLikeItauStatement(documentText) || normalizeText(text).includes("itau");

          if (itauDetected) {
            saveLastPdfContext(chatId, {
              origem: "itau",
              file_name: msg.document.file_name || "",
              documentText,
              documentJson: null,
              processing: true
            });

            try {
              documentJson = await extrairRecebimentosItauDoPdfComIA(
                pdfBuffer,
                msg.document.file_name || "extrato_itau.pdf"
              );

              console.log("ITAU documentJson extraido pela IA:");
              console.log(JSON.stringify(documentJson, null, 2));
            } catch (err) {
              console.error("Falha ao estruturar PDF Itaú com IA:", err?.message || err);
            }

            saveLastPdfContext(chatId, {
              origem: "itau",
              file_name: msg.document.file_name || "",
              documentText,
              documentJson,
              processing: false
            });

            clearActivePdfProcessing(chatId);

            const flushedQueuedCommand = await flushQueuedItauCommandIfAny({
              chatId,
              sendTelegramMessage,
              documentText,
              documentJson,
              fileName: msg.document.file_name || "extrato_itau.pdf"
            });

            if (flushedQueuedCommand) {
              return;
            }

            if (!explicitRecebimentosIntentInCaption && !explicitPagamentosIntentInCaption) {
              if (documentJson?.extrato_detectado) {
                await sendTelegramMessage(
                  chatId,
                  "Recebi o PDF do Itaú e já deixei tudo pronto. Agora me diga o período, por exemplo:\n- preencher recebimentos Itaú hoje\n- preencher pagamentos Itaú hoje\n- preencher pagamentos Itaú últimos 7 dias"
                );
              } else {
                await sendTelegramMessage(
                  chatId,
                  "Recebi o PDF, mas não consegui estruturar o extrato do Itaú com segurança ainda. Tente enviar novamente ou mande outro PDF."
                );
              }
              return;
            }

            const syntheticMsg = buildSyntheticMessageWithDocument(
              msg,
              msg.document.file_name || "extrato_itau.pdf"
            );

            if (explicitPagamentosIntentInCaption) {
              await handlePagamentosMessage({
                message: syntheticMsg,
                text,
                sendTelegramMessage,
                documentText,
                documentJson,
                documentFileName: msg.document.file_name || "extrato_itau.pdf"
              });
            } else {
              await handleRecebimentosMessage({
                message: syntheticMsg,
                text,
                sendTelegramMessage,
                documentText,
                documentJson
              });
            }
            return;
          }

          clearActivePdfProcessing(chatId);

          const isReceb = explicitRecebimentosIntentInCaption || isRecebimentosIntent(text, msg);

          if (!isReceb) {
            await sendTelegramMessage(
              chatId,
              "Recebi um PDF. Se isso for um extrato, me diga algo como:\n- preencher recebimentos Itaú últimos 7 dias\n- preencher pagamentos Itaú hoje"
            );
            return;
          }

          await handleRecebimentosMessage({
            message: msg,
            text,
            sendTelegramMessage,
            documentText,
            documentJson
          });
          return;
        } finally {
          clearActivePdfProcessing(chatId);
        }
    }

    // =====================================================
    // ÁUDIO
    // =====================================================
    if (msg.voice || msg.audio) {
      await sendTelegramMessage(chatId, "Recebi seu áudio. Vou transcrever e analisar.");

      const fileId = msg.voice?.file_id || msg.audio?.file_id;
      const fileInfo = await getTelegramFile(fileId);
      const audioBuffer = await downloadTelegramFileBuffer(fileInfo.file_path);
      const transcription = await transcribeAudioWithOpenAI(audioBuffer, "audio.ogg");

      if (!transcription || !String(transcription).trim()) {
        await sendTelegramMessage(
          chatId,
          "Não consegui transcrever esse áudio. Mande de novo ou envie em texto."
        );
        return;
      }

      const handledRecebimentosPending = await tryHandleRecebimentosPendingCommands(chatId, transcription);
      if (handledRecebimentosPending) return;

      const handledPagamentosPending = await tryHandlePagamentosPendingCommands(chatId, transcription);
      if (handledPagamentosPending) return;

      if (isPagamentosIntent(transcription, msg)) {
        await handlePagamentosMessage({
          message: msg,
          text: transcription,
          transcription,
          sendTelegramMessage
        });
        return;
      }

      if (isRecebimentosIntent(transcription, msg)) {
        const lastPdf = getUsableLastPdfContext(chatId);

        const origemFromContext = chooseRecebimentosOrigin({
          text: transcription,
          message: lastPdf?.documentJson
            ? buildSyntheticMessageWithDocument(msg, lastPdf?.file_name || "extrato_itau.pdf")
            : msg,
          documentText: lastPdf?.documentText || ""
        });

        if (origemFromContext === "itau" && shouldWaitForNewItauPdf(chatId, lastPdf)) {
          saveQueuedItauCommand(chatId, {
            kind: "recebimentos",
            message: msg,
            text: transcription,
            transcription
          });

          await sendTelegramMessage(
            chatId,
            "Estou terminando de preparar o PDF do Itaú que você acabou de enviar. Assim que ele ficar pronto, continuo esse comando automaticamente."
          );
          return;
        }

        if (origemFromContext === "itau" && !lastPdf?.documentJson) {
          saveQueuedItauCommand(chatId, {
            kind: "recebimentos",
            message: msg,
            text: transcription,
            transcription
          });

          await sendTelegramMessage(
            chatId,
            `Transcrição:\n"${transcription}"\n\nAinda não tenho um PDF do Itaú pronto para usar. Se você acabou de enviar o extrato, vou esperar um instante e continuar automaticamente. Se não, envie o PDF do extrato.`
          );
          return;
        }

        await handleRecebimentosMessage({
          message: lastPdf?.documentJson
            ? buildSyntheticMessageWithDocument(msg, lastPdf?.file_name || "extrato_itau.pdf")
            : msg,
          text: transcription,
          transcription,
          sendTelegramMessage,
          documentText: lastPdf?.documentText || "",
          documentJson: lastPdf?.documentJson || null
        });
        return;
      }

      const handledSalesPending = await tryHandlePendingSalesCommands(chatId, transcription);
      if (handledSalesPending) return;

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

    // =====================================================
    // TEXTO
    // =====================================================
    if (text) {
      const handledRecebimentosPending = await tryHandleRecebimentosPendingCommands(chatId, text);
      if (handledRecebimentosPending) return;

      const handledPagamentosPending = await tryHandlePagamentosPendingCommands(chatId, text);
      if (handledPagamentosPending) return;

      if (isPagamentosIntent(text, msg)) {
        await handlePagamentosMessage({
          message: msg,
          text,
          sendTelegramMessage
        });
        return;
      }

      if (isRecebimentosIntent(text, msg)) {
        const lastPdf = getUsableLastPdfContext(chatId);

        const origemFromContext = chooseRecebimentosOrigin({
          text,
          message: lastPdf?.documentJson
            ? buildSyntheticMessageWithDocument(msg, lastPdf?.file_name || "extrato_itau.pdf")
            : msg,
          documentText: lastPdf?.documentText || ""
        });

        if (origemFromContext === "itau" && shouldWaitForNewItauPdf(chatId, lastPdf)) {
          saveQueuedItauCommand(chatId, {
            kind: "recebimentos",
            message: msg,
            text
          });

          await sendTelegramMessage(
            chatId,
            "Estou terminando de preparar o PDF do Itaú que você acabou de enviar. Assim que ele ficar pronto, continuo esse comando automaticamente."
          );
          return;
        }

        if (origemFromContext === "itau" && !lastPdf?.documentJson) {
          saveQueuedItauCommand(chatId, {
            kind: "recebimentos",
            message: msg,
            text
          });

          await sendTelegramMessage(
            chatId,
            "Ainda não tenho um PDF do Itaú pronto para usar. Se você acabou de enviar o extrato, vou esperar um instante e continuar automaticamente. Se não, envie o PDF do extrato."
          );
          return;
        }

        await handleRecebimentosMessage({
          message: lastPdf?.documentJson
            ? buildSyntheticMessageWithDocument(msg, lastPdf?.file_name || "extrato_itau.pdf")
            : msg,
          text,
          sendTelegramMessage,
          documentText: lastPdf?.documentText || "",
          documentJson: lastPdf?.documentJson || null
        });
        return;
      }

      const handledSalesPending = await tryHandlePendingSalesCommands(chatId, text);
      if (handledSalesPending) return;

      const pending = getPendingBatch(chatId);
      const handledCorrection = await handlePotentialCorrection(chatId, text, "text", pending);
      if (handledCorrection) return;

      const extraction = await extractOrdersFromText(text);

      savePendingBatch(chatId, extraction, {
        source: "text",
        duplicateAwaitingForce: false
      });

      const resumo = summarizeOrders(extraction);
      await sendTelegramMessage(chatId, resumo);
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
        const detalhe = String(error?.message || "").trim();
        await sendTelegramMessage(
          chatId,
          detalhe
            ? `Tive um erro ao processar sua mensagem.\n\nDetalhe: ${detalhe}`
            : "Tive um erro ao processar sua mensagem."
        );
      }
    } catch (err2) {
      console.error("Erro ao enviar mensagem de falha:", err2.message || err2);
    }
  }
});

app.listen(PORT, () => {
  console.log(`Servidor online na porta ${PORT}`);
});
