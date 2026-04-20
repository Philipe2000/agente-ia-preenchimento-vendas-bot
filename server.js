const express = require("express");
const axios = require("axios");
const FormData = require("form-data");

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
  form.append("model", "gpt-4o-mini-transcribe");

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

async function extractOrdersFromText(text) {
  const prompt = `
Você é um extrator de pedidos de vendas em português do Brasil.

Converta a mensagem do usuário em JSON.
A mensagem pode conter de 1 a 10 pedidos.
Se não houver clareza suficiente, ainda tente extrair o máximo com cautela.

Regras:
- Se a quantidade vier em kg, preserve unidade = "kg".
- Se a quantidade vier em g/gramas, preserve unidade = "g".
- Se o usuário disser um valor por extenso, converta para número.
  Exemplo: "dois mil duzentos e cinquenta" => 2250
- Se o usuário disser vencimento por data, preserve em texto.
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
  console.log("Resposta bruta da OpenAI extractOrdersFromText:", content);

  try {
    return JSON.parse(content);
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

function cloneExtraction(extraction) {
  return JSON.parse(JSON.stringify(extraction || { pedidos: [] }));
}

function parseSimpleCorrection(text, pendingExtraction) {
  const pedidos = Array.isArray(pendingExtraction?.pedidos) ? pendingExtraction.pedidos : [];
  if (!pedidos.length) return null;

  const normalized = normalizeText(text);
  const itemIndexMatch = normalized.match(/\bitem\s+(\d+)\b/);
  const itemIndex = itemIndexMatch ? Number(itemIndexMatch[1]) : (pedidos.length === 1 ? 1 : null);

  const qtyMatch = normalized.match(/\b(\d+(?:[.,]\d+)?)\s*(kg|g)\b/);
  if (qtyMatch && itemIndex) {
    return {
      is_correction: true,
      action: "update_quantity",
      item_index: itemIndex,
      fields: {
        quantidade: Number(qtyMatch[1].replace(",", ".")),
        unidade: qtyMatch[2],
        cliente_falado: null
      },
      motivo: "fallback regex quantidade"
    };
  }

  const removeMatch =
    normalized.match(/\bremove(?:r)?\s+o\s+item\s+(\d+)\b/) ||
    normalized.match(/\bapaga(?:r)?\s+o\s+item\s+(\d+)\b/) ||
    normalized.match(/\bexclui(?:r)?\s+o\s+item\s+(\d+)\b/);

  if (removeMatch) {
    return {
      is_correction: true,
      action: "remove_item",
      item_index: Number(removeMatch[1]),
      fields: {
        quantidade: null,
        unidade: null,
        cliente_falado: null
      },
      motivo: "fallback regex remove item"
    };
  }

  const clientMatch =
    normalized.match(/\btroca\s+o\s+cliente\s+para\s+(.+)$/) ||
    normalized.match(/\bo\s+cliente\s+e\s+(.+)$/) ||
    normalized.match(/\bcliente\s+(.+)$/);

  if (clientMatch && itemIndex) {
    const cliente = String(clientMatch[1] || "").trim();
    if (cliente) {
      return {
        is_correction: true,
        action: "update_client",
        item_index: itemIndex,
        fields: {
          quantidade: null,
          unidade: null,
          cliente_falado: cliente
        },
        motivo: "fallback regex cliente"
      };
    }
  }

  return null;
}

async function interpretCorrectionFromText(text, pendingExtraction) {
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

Ações permitidas nesta V1.1:
- alterar quantidade de um item
- alterar cliente de um item
- remover um item

Retorne SOMENTE JSON válido neste formato:

{
  "is_correction": true ou false,
  "action": "update_quantity|update_client|remove_item|null",
  "item_index": number ou null,
  "fields": {
    "quantidade": number ou null,
    "unidade": "g|kg|un|null",
    "cliente_falado": "string ou null"
  },
  "motivo": "string"
}

Regras:
- item_index é baseado em 1
- se o usuário não disser item, e houver só 1 pedido, use item_index = 1
- para "não, é 300g", tente entender como correção de quantidade
- para "troca o cliente para Flávio", tente entender como correção de cliente
- para "remove o item 2", use remove_item
- se a mensagem parecer um pedido novo e não correção, retorne is_correction false

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
    return JSON.parse(content);
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

function applyCorrectionToExtraction(extraction, correction) {
  const novo = cloneExtraction(extraction);
  const pedidos = Array.isArray(novo.pedidos) ? novo.pedidos : [];
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

  if (action === "remove_item") {
    pedidos.splice(idx, 1);

    return {
      ok: true,
      extraction: novo,
      message: `Removi o item ${idx + 1}.`
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
    const forma = r.forma_pagamento || "PIX";
    const venc = r.vencimento || "?";
    const confianca =
      r.confianca_produto != null ? ` | conf. produto ${r.confianca_produto}` : "";

    return `${i + 1}. ${cliente} — ${produto} — ${qtdG} — sheet ${qtdSheet} — ${valor} — ${bloco} — ${forma} — venc. ${venc}${confianca}`;
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
      const pending = getPendingBatch(chatId);

      if (pending && !isConfirmationText(text)) {
        const correction = await interpretCorrectionFromText(text, pending.extraction);

        if (correction?.is_correction) {
          const applied = applyCorrectionToExtraction(pending.extraction, correction);

          if (applied.ok) {
            savePendingBatch(chatId, applied.extraction, {
              ...(pending.meta || {}),
              lastCorrectionText: text,
              duplicateAwaitingForce: false
            });

            const novoResumo = summarizeOrders(applied.extraction);

            await sendTelegramMessage(chatId, `${applied.message}\n\n${novoResumo}`);
            return;
          }

          await sendTelegramMessage(chatId, applied.message);
          return;
        }
      }

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

      if (!transcription) {
        await sendTelegramMessage(chatId, "Não consegui transcrever esse áudio.");
        return;
      }

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

    await sendTelegramMessage(chatId, "Envie texto ou áudio para eu processar.");
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
