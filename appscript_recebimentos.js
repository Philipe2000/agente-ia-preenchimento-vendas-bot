const axios = require("axios");

const GOOGLE_APPS_SCRIPT_RECEBIMENTOS_WEBAPP_URL =
  process.env.GOOGLE_APPS_SCRIPT_RECEBIMENTOS_WEBAPP_URL || "";

async function callRecebimentosWebApp(payload) {
  if (!GOOGLE_APPS_SCRIPT_RECEBIMENTOS_WEBAPP_URL) {
    throw new Error("GOOGLE_APPS_SCRIPT_RECEBIMENTOS_WEBAPP_URL não configurada.");
  }

  const body = { ...payload };

  console.log("Recebimentos payload enviado ao Apps Script:");
  console.log(JSON.stringify(body, null, 2));

  const resp = await axios.post(GOOGLE_APPS_SCRIPT_RECEBIMENTOS_WEBAPP_URL, body, {
    headers: {
      "Content-Type": "application/json"
    },
    validateStatus: () => true,
    timeout: 120000
  });

  console.log("Recebimentos resposta do Apps Script:");
  console.log(JSON.stringify(resp.data, null, 2));

  return resp.data;
}

module.exports = {
  callRecebimentosWebApp
};
