const axios = require("axios");

const GOOGLE_APPS_SCRIPT_RECEBIMENTOS_URL =
  process.env.GOOGLE_APPS_SCRIPT_RECEBIMENTOS_URL || "";

async function callRecebimentosWebApp(payload) {
  if (!GOOGLE_APPS_SCRIPT_RECEBIMENTOS_URL) {
    throw new Error("GOOGLE_APPS_SCRIPT_RECEBIMENTOS_URL não configurada.");
  }

  const resp = await axios.post(
    GOOGLE_APPS_SCRIPT_RECEBIMENTOS_URL,
    payload,
    {
      headers: {
        "Content-Type": "application/json"
      },
      timeout: 120000,
      validateStatus: () => true
    }
  );

  return resp.data;
}

module.exports = {
  callRecebimentosWebApp
};
