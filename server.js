const express = require("express");
const axios = require("axios");

const app = express();
app.use(express.json({ limit: "20mb" }));

const PORT = process.env.PORT || 3000;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN || "";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const GOOGLE_APPS_SCRIPT_WEBAPP_URL = process.env.GOOGLE_APPS_SCRIPT_WEBAPP_URL || "";

function telegramApiUrl(method) {
  return `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`;
}

async function sendTelegramMessage(chatId, text) {
  return axios.post(telegramApiUrl("sendMessage"), {
    chat_id: chatId,
    text
  });
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

    const chatId = msg.chat && msg.chat.id;
    const text = (msg.text || msg.caption || "").trim();

    if (!chatId) return;

    if (text) {
      await sendTelegramMessage(
        chatId,
        `Recebi seu texto: "${text}"\n\nPróximo passo: vou ligar a IA e o preenchimento no Google Sheets.`
      );
      return;
    }

    if (msg.voice || msg.audio) {
      await sendTelegramMessage(
        chatId,
        "Recebi seu áudio. Próximo passo: vou ligar a transcrição com OpenAI."
      );
      return;
    }

    await sendTelegramMessage(
      chatId,
      "Envie texto ou áudio para eu processar."
    );
  } catch (error) {
    console.error("Erro no webhook:", error.response?.data || error.message || error);
  }
});

app.listen(PORT, () => {
  console.log(`Servidor online na porta ${PORT}`);
});
