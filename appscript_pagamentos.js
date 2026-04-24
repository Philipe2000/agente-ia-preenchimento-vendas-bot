const axios = require("axios");

const GOOGLE_APPS_SCRIPT_PAGAMENTOS_WEBAPP_URL =
  process.env.GOOGLE_APPS_SCRIPT_RECEBIMENTOS_WEBAPP_URL ||
  process.env.GOOGLE_APPS_SCRIPT_PAGAMENTOS_WEBAPP_URL ||
  "";

async function callPagamentosWebApp(payload) {
  if (!GOOGLE_APPS_SCRIPT_PAGAMENTOS_WEBAPP_URL) {
    throw new Error(
      "GOOGLE_APPS_SCRIPT_PAGAMENTOS_WEBAPP_URL não configurada."
    );
  }

  const body = { ...payload };

  console.log("Pagamentos payload enviado ao Apps Script:");
  console.log(JSON.stringify(body, null, 2));

  const resp = await axios.post(GOOGLE_APPS_SCRIPT_PAGAMENTOS_WEBAPP_URL, body, {
    headers: {
      "Content-Type": "application/json"
    },
    validateStatus: () => true,
    timeout: 120000
  });

  console.log("Pagamentos resposta do Apps Script:");
  console.log(JSON.stringify(resp.data, null, 2));

  if (typeof resp.data === "string" && /<!doctype html|<html/i.test(resp.data)) {
    throw new Error(
      "A URL do Apps Script de pagamentos/recebimentos no Railway está apontando para uma página HTML do Google Drive, não para a web app JSON correta."
    );
  }

  return resp.data;
}

module.exports = {
  callPagamentosWebApp
};
