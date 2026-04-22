const express = require("express");
const axios = require("axios");
const FormData = require("form-data");
const { isRecebimentosIntent } = require("./intents_recebimentos");
const { handleRecebimentosMessage } = require("./recebimentos");

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const GOOGLE_APPS_SCRIPT_WEBAPP_URL = process.env.GOOGLE_APPS_SCRIPT_WEBAPP_URL || "";

const pendingBatches = new Map();

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

async function transcribeAudioWithOpenAI(buffer, filename = "audio.ogg") {
  const form = new FormData();
  form.append("file", buffer, filename);

  // Modelo mais forte de transcrição
  form.append("model", "gpt-4o-transcribe");

  // Idioma esperado
  form.append("language", "pt");

  // Vocabulário útil do seu negócio
  form.append(
    "prompt",
    [
      "Transcrição em português do Brasil.",
      "Nomes frequentes de clientes:",
      "Diergia, Larissa, Raquel, Ricardo, Renata, Flávio, Fábio, Diege, Dieergia.",
      "Produtos frequentes:",
      "liga rosa, liga branca, castanho, loiro, vietnamita, castanho liga rosa, louro liga branca.",
      "Medidas frequentes:",
      "55cm, 60/65cm, 65/70cm, 70/75cm.",
      "Se houver nomes próprios raros, tente preservar a forma fonética mais próxima possível."
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
  if (m) {
    return `${m[1]}-${m[2]}-${m[3]}`;
  }

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

  if (out.data_falada) {
    out.data_falada = normalizeDateValue(out.data_falada);
  }

  if (out.vencimento_falado) {
    out.vencimento_falado = normalizeDateValue(out.vencimento_falado);
  }

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
  while ((m = reFull.exec(original)) !== null) {
    explicitDates.push(normalizeDateValue(m[1]));
  }

  const reShort = /\b(\d{1,2}\/\d{1,2})\b/g;
  while ((m = reShort.exec(original)) !== null) {
    explicitDates.push(normalizeDateValue(m[1]));
  }

  if (normalized.includes("antes de ontem")) {
    explicitDates.push(normalizeDateValue("antes de ontem"));
  } else if (normalized.includes("ontem")) {
    explicitDates.push(normalizeDateValue("ontem"));
  } else if (normalized.includes("hoje")) {
    explicitDates.push(normalizeDateValue("hoje"));
  }

  const uniqueDates = [...new Set(explicitDates.filter(Boolean))];
  if (uniqueDates.length === 1) {
    context.data_falada = uniqueDates[0];
  }

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

  const clientePattern =
    original.match(/^\s*([A-Za-zÀ-ÿ'’\-]+)\s+comprou\b/i);

  if (clientePattern) {
    context.cliente_falado = String(clientePattern[1] || "").trim() || null;
  }

  return context;
}

function applyBatchContextDefaults(extraction, rawText) {
  const normalizedExtraction = normalizeExtraction(extraction);
  const pedidos = Array.isArray(normalizedExtraction?.pedidos)
    ? normalizedExtraction.pedidos
    : [];

  if (!pedidos.length) return normalizedExtraction;

  const globalContext = inferGlobalContextFromText(rawText);

  const filled = pedidos.map((pedido) => {
    const out = { ...pedido };

    if (!out.cliente_falado && globalContext.cliente_falado) {
      out.cliente_falado = globalContext.cliente_falado;
    }

    if (!out.data_falada && globalContext.data_falada) {
      out.data_falada = globalContext.data_falada;
    }

    if (!out.vencimento_falado && globalContext.vencimento_falado) {
      out.vencimento_falado = globalContext.vencimento_falado;
    }

    if (!out.forma_pagamento_falada && globalContext.forma_pagamento_falada) {
      out.forma_pagamento_falada = globalContext.forma_pagamento_falada;
    }

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
  Exemplo: "dois mil duzentos e cinquenta" => 2250
- Se o usuário disser data da compra como "18/04", preserve "18/04".
- Se o usuário disser "ontem", "antes de ontem" ou "hoje", preserve esse texto em data_falada.
- Se a forma de pagamento não for dita, retorne null.
- Se o vencimento não for dito, retorne null.
- Se um mesmo contexto geral de data, cliente, vencimento ou pagamento parecer valer para vários pedidos, ainda assim extraia pedido por pedido e preserve o máximo possível.
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
  console.log("Resposta bruta da OpenAI extractOrdersFromText:", content);

  try {
    const parsed = JSON.parse(content);
    return applyBatchContextDefaults(parsed, text);
  } catch (err) {
    return { pedidos: [], erro_parse: true, conteudo_bruto: content };
  }
}

function summarizeOrders(extraction) {
  const pedidos = Array.isArray(extraction?.pedidos) ? extraction.pedidos : [];

  if (!pedidos.length) {
    return "Não consegui extrair pedidos com segurança ainda.";
  }

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

  const qtyOnlyMatch = normalized.match(/\bnao[, ]+e\s+(\d+(?:[.,]\d+)?)\b/);
  if (qtyOnlyMatch) {
    const currentUnit = pedidos[itemIndex - 1]?.unidade || "g";
    return {
      is_correction: true,
      action: "update_quantity",
      item_index: itemIndex,
      fields: {
        quantidade: Number(String(qtyOnlyMatch[1]).replace(",", ".")),
        unidade: currentUnit
      },
      motivo: "fallback regex quantidade sem unidade"
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

  const valueMatch =
    normalized.match(/\bvalor\s+(?:e|é)?\s*(\d+(?:[.,]\d+)?)\b/) ||
    normalized.match(/\bpreco\s+(?:e|é)?\s*(\d+(?:[.,]\d+)?)\b/) ||
    normalized.match(/\bcusta\s+(\d+(?:[.,]\d+)?)\b/);

  if (valueMatch) {
    return {
      is_correction: true,
      action: "update_value",
      item_index: itemIndex,
      fields: {
        valor_falado: parseBrazilianNumber(valueMatch[1])
      },
      motivo: "fallback regex valor"
    };
  }

  const payment = parsePaymentText(text);
  if (payment) {
    return {
      is_correction: true,
      action: "update_payment",
      item_index: itemIndex,
      fields: {
        forma_pagamento_falada: payment
      },
      motivo: "fallback regex pagamento"
    };
  }

  const dueDate = normalizeDateValue(text);
  if (
    dueDate &&
    dueDate !== String(text).trim() &&
    (normalized.includes("vence") || normalized.includes("vencimento") || normalized.includes("dia"))
  ) {
    return {
      is_correction: true,
      action: "update_due_date",
      item_index: itemIndex,
      fields: {
        vencimento_falado: dueDate
      },
      motivo: "fallback regex vencimento"
    };
  }

  const productMatch =
    normalized.match(/\btroca\s+o\s+produto\s+para\s+(.+)$/) ||
    normalized.match(/\bo\s+produto\s+e\s+(.+)$/) ||
    normalized.match(/\bproduto\s+(.+)$/);

  if (productMatch) {
    const produto = String(productMatch[1] || "").trim();
    if (produto) {
      return {
        is_correction: true,
        action: "update_product",
        item_index: itemIndex,
        fields: {
          produto_falado: produto
        },
        motivo: "fallback regex produto"
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

  const pedidos = Array.isArray(pendingExtraction?.pedidos) ? pendingExtraction.pedidos : [];

  const prompt = `
Você é um interpretador de correções de um lote de pedidos já extraído.

Você receberá:
1. a mensagem nova do usuário
2. o lote atual em JSON

Sua tarefa:
- descobrir se a mensagem é uma correção do lote atual
- se for, retornar a ação estruturada
- se não for, retornar "is_correction": false

Ações permitidas:
- alterar quantidade de um item
- alterar cliente de um item
- remover um item
- alterar produto de um item
- alterar valor de um item
- alterar vencimento de um item
- alterar forma de pagamento de um item
- substituir vários campos de um item
- substituir o lote inteiro

Retorne SOMENTE JSON válido neste formato:

{
  "is_correction": true ou false,
  "action": "update_quantity|update_client|remove_item|update_product|update_value|update_due_date|update_payment|replace_item_fields|replace_batch|null",
  "item_index": number ou null,
  "fields": {
    "cliente_falado": "string ou null",
    "produto_falado": "string ou null",
    "quantidade": number ou null,
    "unidade": "g|kg|un|null",
    "valor_falado": number ou null,
    "forma_pagamento_falada": "PIX|Dinheiro à Vista|string|null",
    "vencimento_falado": "string ou null",
    "data_falada": "string ou null"
  },
  "motivo": "string"
}

Regras:
- item_index é baseado em 1
- se o usuário não disser item, e houver só 1 pedido, use item_index = 1
- se a mensagem reescrever o lote inteiro, use replace_batch
- se a mensagem reescrever só um item, use replace_item_fields
- se a mensagem parecer um pedido novo e não correção, retorne is_correction false
- para data_falada e vencimento_falado, preserve o texto que conseguir

Mensagem do usuário:
${text}

Lote atual:
${JSON.stringify({ pedidos }, null, 2)}
`.trim();

  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    {
      model: "gpt-4.1-mini",
      messages: [
        {
          role: "system",
          content:
            "Você interpreta correções de lote e responde apenas JSON válido, sem markdown, sem comentários e sem texto extra."
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

    if (parsed?.fields?.data_falada) {
      parsed.fields.data_falada = normalizeDateValue(parsed.fields.data_falada);
    }

    if (parsed?.fields?.vencimento_falado) {
      parsed.fields.vencimento_falado = normalizeDateValue(parsed.fields.vencimento_falado);
    }

    return parsed;
  } catch (err) {
    return {
      is_correction: false,
      action: null,
      item_index: null,
      fields: {},
      motivo: "Falha ao interpretar correção"
    };
  }
}

async function applyCorrectionToExtraction(extraction, correction) {
  const novo = cloneExtraction(extraction);
  const pedidos = Array.isArray(novo.pedidos) ? novo.pedidos : [];

  if (correction?.action === "replace_batch") {
    const replacement = normalizeExtraction(
      correction?.replacement_extraction || { pedidos: [] }
    );
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
    if (correction?.fields?.quantidade == null) {
      return { ok: false, message: "Não consegui entender a nova quantidade." };
    }

    item.quantidade = Number(correction.fields.quantidade);
    item.unidade = correction?.fields?.unidade || item.unidade || "g";

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi a quantidade do item ${idx + 1}.`
    };
  }

  if (action === "update_client") {
    const cliente = String(correction?.fields?.cliente_falado || "").trim();
    if (!cliente) {
      return { ok: false, message: "Não consegui entender o novo cliente." };
    }

    item.cliente_falado = cliente;

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi o cliente do item ${idx + 1}.`
    };
  }

  if (action === "update_product") {
    const produto = String(correction?.fields?.produto_falado || "").trim();
    if (!produto) {
      return { ok: false, message: "Não consegui entender o novo produto." };
    }

    item.produto_falado = produto;

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi o produto do item ${idx + 1}.`
    };
  }

  if (action === "update_value") {
    const valor = correction?.fields?.valor_falado;
    if (valor == null || !Number.isFinite(Number(valor))) {
      return { ok: false, message: "Não consegui entender o novo valor." };
    }

    item.valor_falado = Number(valor);

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi o valor do item ${idx + 1}.`
    };
  }

  if (action === "update_due_date") {
    const vencimento = String(correction?.fields?.vencimento_falado || "").trim();
    if (!vencimento) {
      return { ok: false, message: "Não consegui entender o novo vencimento." };
    }

    item.vencimento_falado = vencimento;

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi o vencimento do item ${idx + 1}.`
    };
  }

  if (action === "update_payment") {
    const forma = String(correction?.fields?.forma_pagamento_falada || "").trim();
    if (!forma) {
      return { ok: false, message: "Não consegui entender a nova forma de pagamento." };
    }

    item.forma_pagamento_falada = forma;

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi a forma de pagamento do item ${idx + 1}.`
    };
  }

  if (action === "remove_item") {
    pedidos.splice(idx, 1);

    return {
      ok: true,
      extraction: novo,
      message: `Removi o item ${idx + 1}.`
    };
  }

  if (action === "replace_item_fields") {
    const replacementPedido = correction?.replacement_pedido
      ? normalizeSinglePedido(correction.replacement_pedido)
      : null;

    if (!replacementPedido) {
      return {
        ok: false,
        message: "Não consegui entender a correção completa desse item."
      };
    }

    pedidos[idx] = {
      ...item,
      ...replacementPedido,
      cliente_falado: replacementPedido.cliente_falado || item.cliente_falado,
      produto_falado: replacementPedido.produto_falado || item.produto_falado,
      quantidade: replacementPedido.quantidade != null ? replacementPedido.quantidade : item.quantidade,
      unidade: replacementPedido.unidade || item.unidade,
      valor_falado: replacementPedido.valor_falado != null ? replacementPedido.valor_falado : item.valor_falado,
      forma_pagamento_falada: replacementPedido.forma_pagamento_falada || item.forma_pagamento_falada,
      vencimento_falado: replacementPedido.vencimento_falado || item.vencimento_falado,
      data_falada: replacementPedido.data_falada || item.data_falada
    };

    return {
      ok: true,
      extraction: novo,
      message: `Corrigi o item ${idx + 1} com a frase completa.`
    };
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
    headers: {
      "Content-Type": "application/json"
    },
    validateStatus: () => true
  });

  const contentType = resp.headers?.["content-type"] || "";
  const bodyPreview =
    typeof resp.data === "string"
      ? resp.data.slice(0, 500)
      : JSON.stringify(resp.data).slice(0, 500);

  console.log("Apps Script status:", resp.status);
  console.log("Apps Script content-type:", contentType);
  console.log("Apps Script preview:", bodyPreview);

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
    const linha = r.linha_item != null ? ` | linha ${r.linha_item}` : "";
    const forma = r.forma_pagamento || "PIX";
    const venc = r.vencimento || "?";
    const confianca =
      r.confianca_produto != null ? ` | conf. produto ${r.confianca_produto}` : "";

    return `${i + 1}. ${cliente} — ${produto} — ${qtdG} — sheet ${qtdSheet} — ${valor} — ${bloco}${linha} — ${forma} — venc. ${venc}${confianca}`;
  });

  return `Lote confirmado com sucesso.\n\n${linhas.join("\n")}`;
}

function formatDuplicateMessage(gsResp) {
  const dup = Array.isArray(gsResp?.resultados)
    ? gsResp.resultados.find((r) => r && r.possible_duplicate)
    : null;

  const primeira = dup && Array.isArray(dup.duplicatas) ? dup.duplicatas[0] : null;

  return [
    "Possível duplicata encontrada.",
    "",
    `Cliente: ${dup?.cliente_oficial || "?"}`,
    `Produto: ${dup?.produto_oficial || "?"}`,
    `Quantidade: ${dup?.quantidade_gramas || "?"}g`,
    `Data: ${dup?.data_venda || "?"}`,
    "",
    `Registro já existente: ${primeira ? JSON.stringify(primeira) : "não detalhado"}`,
    "",
    "Se quiser lançar mesmo assim, responda: confirmar duplicata"
  ].join("\n");
}

async function handlePotentialCorrection(chatId, incomingText, sourceLabel, pending) {
  if (!pending || isConfirmationText(incomingText) || isCancelText(incomingText)) {
    return false;
  }

  const correction = await interpretCorrectionFromText(incomingText, pending.extraction);

  if (!correction?.is_correction) {
    return false;
  }

  const applied = await applyCorrectionToExtraction(pending.extraction, correction);

  if (!applied.ok) {
    if (looksLikeFreshOrderMessage(incomingText)) {
      return false;
    }

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

    // ===== RECEBIMENTOS: PDF / DOCUMENTO =====
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

      const pedidos = Array.isArray(pending.extraction?.pedidos)
        ? pending.extraction.pedidos
        : [];

      const forceDuplicateConfirmed =
        normalizeText(text) === "confirmar duplicata" ||
        pending?.meta?.duplicateAwaitingForce === true;

      const gsResp = await callGoogleAppsScript({
        action: "preencher_lote_v1",
        pedidos,
        meta: pending.meta || {},
        force_duplicate_confirmed: forceDuplicateConfirmed
      });

      if (gsResp?.ok) {
        clearPendingBatch(chatId);
        await sendTelegramMessage(chatId, formatGoogleSuccessMessage(gsResp));
      } else if (gsResp?.possible_duplicate) {
        savePendingBatch(chatId, pending.extraction, {
          ...(pending.meta || {}),
          duplicateAwaitingForce: true
        });

        await sendTelegramMessage(chatId, formatDuplicateMessage(gsResp));
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
      // ===== RECEBIMENTOS: TEXTO =====
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

      // ===== RECEBIMENTOS: ÁUDIO =====
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
