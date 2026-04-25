function normalizeText(s = "") {
  return String(s)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

function isComprasIntent(text = "", message = {}) {
  const t = normalizeText(text);

  if (!t) return false;

  return [
    "compra",
    "compras",
    "pedido ao fornecedor",
    "pedido para fornecedor",
    "pedido de compra",
    "preencher compras",
    "lancar compras",
    "lançar compras",
    "registrar compras",
    "setor de compras",
    "fornecedor"
  ].some((k) => t.includes(k));
}

function parseComprasIntent(text = "", message = {}) {
  const t = normalizeText(text);

  return {
    texto_normalizado: t,
    origem: message?.photo ? "imagem" : "texto"
  };
}

module.exports = {
  isComprasIntent,
  parseComprasIntent
};
