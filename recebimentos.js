const { parseRecebimentosIntent } = require("./intents_recebimentos");
const { callRecebimentosWebApp } = require("./appscript_recebimentos");

async function handleRecebimentosMessage(ctx) {
  const { message, text, sendTelegramMessage } = ctx;
  const chatId = message.chat.id;

  const parsed = parseRecebimentosIntent(text, message);

  await sendTelegramMessage(
    chatId,
    `Entendi. Vou processar recebimentos de ${parsed.origem.toUpperCase()} para ${parsed.periodo.label}.`
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

  const resumo = result?.message || "Recebimentos processados.";
  await sendTelegramMessage(chatId, resumo);
}

module.exports = {
  handleRecebimentosMessage
};
