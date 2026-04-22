function normalizeText(s = "") {
  return String(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function isRecebimentosIntent(text = "", message = {}) {
  const t = normalizeText(text);

  if (message?.document) {
    const name = String(message.document.file_name || "").toLowerCase();
    const mime = String(message.document.mime_type || "").toLowerCase();

    if (mime === "application/pdf" || name.endsWith(".pdf")) {
      if (
        t.includes("extrato") ||
        t.includes("itau") ||
        t.includes("recebimento") ||
        !t
      ) {
        return true;
      }
    }
  }

  return [
    "recebimento",
    "recebimentos",
    "inter",
    "itau",
    "pix de hoje",
    "ultimos 3 dias",
    "ultimos 7 dias",
    "preencha recebimentos",
    "extrato"
  ].some(k => t.includes(k));
}

function parseRecebimentosIntent(text = "", message = {}) {
  const t = normalizeText(text);

  let origem = "inter";
  if (message?.document || t.includes("itau") || t.includes("extrato")) {
    origem = "itau";
  }

  let periodo = { tipo: "dias", valor: 1, label: "hoje" };

  if (t.includes("hoje")) {
    periodo = { tipo: "dias", valor: 1, label: "hoje" };
  } else if (t.includes("ontem")) {
    periodo = { tipo: "dias", valor: 1, label: "ontem", deslocamento: 1 };
  } else {
    const m = t.match(/ultimos?\s+(\d+)\s+dias?/);
    if (m) {
      periodo = { tipo: "dias", valor: Number(m[1]), label: `últimos ${m[1]} dias` };
    }
  }

  return { origem, periodo, texto_normalizado: t };
}

module.exports = {
  isRecebimentosIntent,
  parseRecebimentosIntent,
  normalizeText
};
