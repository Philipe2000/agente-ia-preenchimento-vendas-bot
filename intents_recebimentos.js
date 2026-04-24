function normalizeText(s = "") {
  return String(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function pad2(n) {
  return String(n).padStart(2, "0");
}

function currentYear() {
  return new Date().getFullYear();
}

function toIsoDate(day, month, year) {
  return `${year}-${pad2(month)}-${pad2(day)}`;
}

function extractSpecificDates(text = "") {
  const raw = String(text || "");
  const matches = [...raw.matchAll(/\b(\d{1,2})[\/\-](\d{1,2})(?:[\/\-](\d{4}))?\b/g)];

  if (!matches.length) return [];

  const yearNow = currentYear();
  const out = [];

  for (const m of matches) {
    const day = Number(m[1]);
    const month = Number(m[2]);
    const year = m[3] ? Number(m[3]) : yearNow;

    if (!day || !month) continue;
    if (day < 1 || day > 31) continue;
    if (month < 1 || month > 12) continue;

    out.push({
      iso: toIsoDate(day, month, year),
      label: `${pad2(day)}/${pad2(month)}`
    });
  }

  const unique = [];
  const seen = new Set();

  for (const item of out) {
    if (!seen.has(item.iso)) {
      seen.add(item.iso);
      unique.push(item);
    }
  }

  return unique;
}

function isRecebimentosIntent(text = "", message = {}) {
  const t = normalizeText(text);

  if (
    t.includes("pagamento") ||
    t.includes("pagamentos") ||
    t.includes("contas a pagar") ||
    t.includes("preencher pagamentos")
  ) {
    return false;
  }

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

  if (extractSpecificDates(text).length > 0 && t.includes("receb")) {
    return true;
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

  const specificDates = extractSpecificDates(text);

  if (specificDates.length > 0) {
    periodo = {
      tipo: "datas_especificas",
      datas: specificDates.map(x => x.iso),
      label: specificDates.map(x => x.label).join(", ")
    };
  } else if (t.includes("hoje")) {
    periodo = { tipo: "dias", valor: 1, label: "hoje" };
  } else if (t.includes("ontem")) {
    periodo = { tipo: "dias", valor: 1, label: "ontem", deslocamento: 1 };
  } else {
    const m = t.match(/ultimos?\s+(\d+)\s+dias?/);
    if (m) {
      periodo = {
        tipo: "dias",
        valor: Number(m[1]),
        label: `últimos ${m[1]} dias`
      };
    }
  }

  return { origem, periodo, texto_normalizado: t };
}

module.exports = {
  isRecebimentosIntent,
  parseRecebimentosIntent,
  normalizeText
};
