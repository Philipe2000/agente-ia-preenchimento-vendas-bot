const COMPRAS_SHEET_NAME_ = "Central de Controle API";
const MAPA_COMPRAS_SHEET_NAME_ = "MAPA_COMPRAS";
const COMPRAS_BASE_ROWS_ = [130, 137, 144, 151, 158];
const COMPRAS_LINES_PER_REG_ = 6;

const COMPRA_DEFAULTS_ = {
  fornecedor: "CH - SP",
  situacao: "Em São Paulo",
  quitar_pagamento: "Sim",
  conta_bancaria: "Itau"
};

function comprasGetSpreadsheet_() {
  if (typeof RECEB_PLANILHA_ID !== "undefined" && RECEB_PLANILHA_ID) {
    return SpreadsheetApp.openById(RECEB_PLANILHA_ID);
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    throw new Error("Planilha Central de Controle API não encontrada no contexto atual.");
  }
  return ss;
}

function comprasNorm_(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function comprasPad2_(n) {
  return String(n).padStart(2, "0");
}

function comprasTodayIso_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");
}

function comprasIsoToBr_(iso) {
  const s = String(iso || "").trim();
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return s;
  return m[3] + "/" + m[2] + "/" + m[1];
}

function comprasParseDateIso_(v) {
  if (Object.prototype.toString.call(v) === "[object Date]" && !isNaN(v.getTime())) {
    return Utilities.formatDate(v, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }

  const s = String(v || "").trim();
  if (!s) return comprasTodayIso_();

  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return m[3] + "-" + comprasPad2_(m[2]) + "-" + comprasPad2_(m[1]);

  m = s.match(/^(\d{1,2})\/(\d{1,2})$/);
  if (m) {
    const year = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy");
    return year + "-" + comprasPad2_(m[2]) + "-" + comprasPad2_(m[1]);
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  return comprasTodayIso_();
}

function comprasParseNumber_(v) {
  if (typeof v === "number") return v;
  const s = String(v || "").trim().replace(/\./g, "").replace(",", ".");
  const n = Number(s);
  return isFinite(n) ? n : NaN;
}

function comprasFormatMoneyBr_(v) {
  const n = Number(v);
  if (!isFinite(n)) return "R$ ?";
  return "R$ " + n.toFixed(2).replace(".", ",");
}

function comprasTokenize_(s) {
  return comprasNorm_(s)
    .split(/[^a-z0-9]+/)
    .map(function(tok) { return tok.trim(); })
    .filter(Boolean);
}

function comprasScoreProduto_(falado, oficial) {
  const a = comprasNorm_(falado);
  const b = comprasNorm_(oficial);
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.indexOf(b) >= 0 || b.indexOf(a) >= 0) return 0.94;

  const ta = comprasTokenize_(a);
  const tb = comprasTokenize_(b);
  const setB = {};
  tb.forEach(function(tok) { setB[tok] = true; });

  let common = 0;
  ta.forEach(function(tok) {
    if (setB[tok]) common++;
  });

  const tokenScore = common / Math.max(ta.length, tb.length, 1);

  let faixaBonus = 0;
  const faixas = ["50/55", "60/65", "65/70", "70/75", "70cm", "75cm"];
  faixas.forEach(function(faixa) {
    if (a.indexOf(comprasNorm_(faixa)) >= 0 && b.indexOf(comprasNorm_(faixa)) >= 0) {
      faixaBonus = Math.max(faixaBonus, 0.18);
    }
  });

  return Math.min(1, tokenScore + faixaBonus);
}

function comprasListAll_(path, paramsBase) {
  const out = [];
  let pagina = 1;
  const limit = 100;

  while (true) {
    const lista = jreq_("GET", path, {
      params: Object.assign({}, paramsBase || {}, {
        pagina: pagina,
        limit: limit
      })
    });

    const arr = Array.isArray(lista) ? lista : [];
    if (!arr.length) break;

    out.push.apply(out, arr);

    if (arr.length < limit) break;
    pagina += 1;
    Utilities.sleep(120);
  }

  return out;
}

function comprasProdutoNomeOficial_(item) {
  return String(
    item && (
      item.nome ||
      item.nome_produto ||
      item.descricao ||
      item.titulo
    ) || ""
  ).trim();
}

function comprasFornecedorNomeOficial_(item) {
  return String(
    item && (
      item.nome ||
      item.razao_social ||
      item.descricao ||
      item.titulo
    ) || ""
  ).trim();
}

function comprasSituacaoNomeOficial_(item) {
  return String(
    item && (
      item.nome ||
      item.descricao ||
      item.titulo
    ) || ""
  ).trim();
}

function comprasDefaultPriceSeeds_() {
  return {
    "cabelo castanho liga rosa 50/55cm": 1500,
    "cabelo castanho liga rosa 60/65cm": 2290,
    "cabelo castanho liga rosa 70/75cm": 2690,
    "cabelo loiro liga branca 65/70 cm": 4100,
    "cabelo loiro liga branca 65/70cm": 4100,
    "cabelo loiro liga branca 70/75 cm": 4400,
    "cabelo loiro liga branca 70/75cm": 4400,
    "cabelo vietnamita liga rosa 70cm": 4000,
    "cabelo vietnamita liga rosa 75cm": 4500
  };
}

function garantirInfraCompras_() {
  const ss = comprasGetSpreadsheet_();
  let sh = ss.getSheetByName(MAPA_COMPRAS_SHEET_NAME_);

  if (!sh) {
    sh = ss.insertSheet(MAPA_COMPRAS_SHEET_NAME_);
  }

  const headers = [
    "produto_gc",
    "produto_norm",
    "preco_padrao",
    "origem_preco",
    "ativo",
    "atualizado_em",
    "observacao"
  ];

  sh.getRange(1, 1, 1, headers.length).setValues([headers]);

  const lastRow = Math.max(sh.getLastRow(), 1);
  const existingRows = lastRow > 1
    ? sh.getRange(2, 1, lastRow - 1, headers.length).getValues()
    : [];

  const existingByNorm = {};
  existingRows.forEach(function(row, idx) {
    const produto = String(row[0] || "").trim();
    const norm = comprasNorm_(produto);
    if (!norm) return;
    existingByNorm[norm] = {
      rowNumber: idx + 2,
      produto: produto,
      preco: row[2],
      origem_preco: row[3],
      ativo: row[4],
      atualizado_em: row[5],
      observacao: row[6]
    };
  });

  const produtosRaw = comprasListAll_("/produtos", {});
  const produtos = produtosRaw
    .map(function(item) {
      return item && (item.Produto || item.produto || item);
    })
    .filter(Boolean);

  const seeds = comprasDefaultPriceSeeds_();
  const finalRows = [];

  produtos.forEach(function(prod) {
    const nome = comprasProdutoNomeOficial_(prod);
    if (!nome) return;

    const norm = comprasNorm_(nome);
    const existing = existingByNorm[norm] || {};
    const seededPrice = Object.prototype.hasOwnProperty.call(seeds, norm)
      ? seeds[norm]
      : "";
    const preco = existing.preco !== "" && existing.preco !== null && existing.preco !== undefined
      ? existing.preco
      : seededPrice;
    const origemPreco = existing.origem_preco || (seededPrice ? "seed_inicial" : "");
    const ativo = existing.ativo || "Sim";
    const atualizadoEm = existing.atualizado_em || "";
    const observacao = existing.observacao || "";

    finalRows.push([
      nome,
      norm,
      preco,
      origemPreco,
      ativo,
      atualizadoEm,
      observacao
    ]);
  });

  if (finalRows.length) {
    sh.getRange(2, 1, sh.getMaxRows() - 1, headers.length).clearContent();
    sh.getRange(2, 1, finalRows.length, headers.length).setValues(finalRows);
  }

  return {
    ok: true,
    sheet_name: MAPA_COMPRAS_SHEET_NAME_,
    produtos: finalRows.length
  };
}

function comprasMapaRows_() {
  garantirInfraCompras_();

  const ss = comprasGetSpreadsheet_();
  const sh = ss.getSheetByName(MAPA_COMPRAS_SHEET_NAME_);
  const lastRow = sh.getLastRow();
  if (lastRow <= 1) return [];

  const values = sh.getRange(2, 1, lastRow - 1, 7).getValues();
  return values.map(function(row, idx) {
    return {
      rowNumber: idx + 2,
      produto_gc: String(row[0] || "").trim(),
      produto_norm: String(row[1] || "").trim(),
      preco_padrao: row[2],
      origem_preco: String(row[3] || "").trim(),
      ativo: String(row[4] || "").trim(),
      atualizado_em: row[5],
      observacao: String(row[6] || "").trim()
    };
  }).filter(function(item) {
    return !!item.produto_gc;
  });
}

function comprasSalvarPrecoMapa_(produtoOficial, preco, origem) {
  const mapa = comprasMapaRows_();
  const norm = comprasNorm_(produtoOficial);
  const row = mapa.find(function(item) {
    return item.produto_norm === norm;
  });
  if (!row) return false;

  const ss = comprasGetSpreadsheet_();
  const sh = ss.getSheetByName(MAPA_COMPRAS_SHEET_NAME_);
  sh.getRange(row.rowNumber, 3).setValue(Number(preco));
  sh.getRange(row.rowNumber, 4).setValue(String(origem || "manual"));
  sh.getRange(row.rowNumber, 6).setValue(new Date());
  return true;
}

function resolverProdutoCompra_(produtoFalado) {
  const falado = String(produtoFalado || "").trim();
  if (!falado) {
    return { ok: false, motivo: "Produto vazio." };
  }

  const mapa = comprasMapaRows_();
  if (!mapa.length) {
    return { ok: false, motivo: "MAPA_COMPRAS vazio." };
  }

  const alvo = comprasNorm_(falado);

  let exact = mapa.find(function(item) {
    return item.produto_norm === alvo;
  });
  if (exact) {
    return {
      ok: true,
      produto_oficial: exact.produto_gc,
      preco_padrao: comprasParseNumber_(exact.preco_padrao),
      origem_preco: exact.origem_preco || ""
    };
  }

  let contains = mapa.find(function(item) {
    return item.produto_norm.indexOf(alvo) >= 0 || alvo.indexOf(item.produto_norm) >= 0;
  });
  if (contains) {
    return {
      ok: true,
      produto_oficial: contains.produto_gc,
      preco_padrao: comprasParseNumber_(contains.preco_padrao),
      origem_preco: contains.origem_preco || "",
      confianca: 0.9
    };
  }

  let best = null;
  let bestScore = 0;

  mapa.forEach(function(item) {
    const score = comprasScoreProduto_(falado, item.produto_gc);
    if (score > bestScore) {
      best = item;
      bestScore = score;
    }
  });

  if (best && bestScore >= 0.56) {
    return {
      ok: true,
      produto_oficial: best.produto_gc,
      preco_padrao: comprasParseNumber_(best.preco_padrao),
      origem_preco: best.origem_preco || "",
      confianca: bestScore
    };
  }

  return {
    ok: false,
    motivo: 'Produto não resolvido para "' + falado + '".'
  };
}

function resolverFornecedorCompra_(nomeFalado) {
  const nome = String(nomeFalado || "").trim();
  if (!nome) {
    return { ok: false, encontrado: false, fornecedor: "" };
  }

  const lista = comprasListAll_("/fornecedores", {
    nome: nome
  });

  const alvo = comprasNorm_(nome);
  let escolhido = lista.find(function(item) {
    return comprasNorm_(comprasFornecedorNomeOficial_(item)) === alvo;
  });

  if (!escolhido) {
    escolhido = lista.find(function(item) {
      const nm = comprasNorm_(comprasFornecedorNomeOficial_(item));
      return nm.indexOf(alvo) >= 0 || alvo.indexOf(nm) >= 0;
    });
  }

  if (!escolhido) {
    return { ok: false, encontrado: false, fornecedor: nome };
  }

  return {
    ok: true,
    encontrado: true,
    fornecedor: comprasFornecedorNomeOficial_(escolhido)
  };
}

function processarComprasV1_(payload) {
  garantirInfraCompras_();

  const compra = payload.compra || {};
  const defaults = compra.defaults || {};
  const itens = Array.isArray(compra.itens) ? compra.itens : [];

  const dataEmissaoIso = comprasParseDateIso_(defaults.data_emissao || "");
  const vencimentoIso = comprasParseDateIso_(defaults.vencimento || "");

  const fornecedorRaw = String(defaults.fornecedor || COMPRA_DEFAULTS_.fornecedor).trim();
  const situacao = String(defaults.situacao || COMPRA_DEFAULTS_.situacao).trim();
  const quitarPagamento = String(defaults.quitar_pagamento || COMPRA_DEFAULTS_.quitar_pagamento).trim();
  const contaBancaria = String(defaults.conta_bancaria || COMPRA_DEFAULTS_.conta_bancaria).trim();

  const fornecedorResolved = resolverFornecedorCompra_(fornecedorRaw);
  const fornecedor = fornecedorResolved.encontrado
    ? fornecedorResolved.fornecedor
    : fornecedorRaw;

  const itensProntos = [];
  const pendencias = [];

  itens.forEach(function(item, idx) {
    const idLocal = String(item.id_local || ("C" + (idx + 1))).toUpperCase();
    const produtoFalado = String(item.produto_falado || "").trim();
    const quantidade = isFinite(Number(item.quantidade)) && Number(item.quantidade) > 0
      ? Number(item.quantidade)
      : 1;
    const precoFalado = isFinite(Number(item.preco_unitario_falado)) && Number(item.preco_unitario_falado) > 0
      ? Number(item.preco_unitario_falado)
      : NaN;

    const produtoResolved = resolverProdutoCompra_(produtoFalado);

    if (!produtoResolved.ok) {
      pendencias.push({
        id_local: idLocal,
        produto_falado: produtoFalado,
        quantidade: quantidade,
        erro: produtoResolved.motivo || "Produto não resolvido."
      });
      return;
    }

    const precoMapa = comprasParseNumber_(produtoResolved.preco_padrao);
    const precoUnitario = isFinite(precoFalado)
      ? precoFalado
      : (isFinite(precoMapa) && precoMapa > 0 ? precoMapa : NaN);

    if (!isFinite(precoUnitario) || precoUnitario <= 0) {
      pendencias.push({
        id_local: idLocal,
        produto_falado: produtoFalado,
        produto_oficial: produtoResolved.produto_oficial,
        quantidade: quantidade,
        erro: 'Preço não encontrado no MAPA_COMPRAS para "' + produtoResolved.produto_oficial + '".'
      });
      return;
    }

    itensProntos.push({
      id_local: idLocal,
      produto_falado: produtoFalado,
      produto_oficial: produtoResolved.produto_oficial,
      quantidade: quantidade,
      valor_unitario: Number(precoUnitario),
      valor_total: Number(precoUnitario) * Number(quantidade),
      persistir_preco: isFinite(precoFalado) && precoFalado > 0,
      observacoes: String(item.observacoes || "").trim()
    });
  });

  return {
    ok: true,
    modo: "pre_visualizacao",
    tipo_fluxo: "compras",
    defaults: {
      fornecedor: fornecedor,
      situacao: situacao,
      data_emissao: dataEmissaoIso,
      vencimento: vencimentoIso,
      quitar_pagamento: quitarPagamento,
      conta_bancaria: contaBancaria
    },
    itens_prontos: itensProntos,
    pendencias_associacao: pendencias,
    message: "Pré-visualização do lote de compras gerada com sucesso."
  };
}

function resolverProdutoCompraAction_(payload) {
  const nomeFalado = String(payload.nome_falado || "").trim();
  const resp = resolverProdutoCompra_(nomeFalado);
  return {
    ok: !!resp.ok,
    encontrado: !!resp.ok,
    produto_oficial: resp.produto_oficial || "",
    preco_padrao: resp.preco_padrao || "",
    message: resp.motivo || ""
  };
}

function encontrarBlocoLivreCompras_() {
  const ss = comprasGetSpreadsheet_();
  const sh = ss.getSheetByName(COMPRAS_SHEET_NAME_) || ss.getActiveSheet();

  for (let i = 0; i < COMPRAS_BASE_ROWS_.length; i++) {
    const base = COMPRAS_BASE_ROWS_[i];
    const ranges = [
      "D" + base,
      "G" + base,
      "I" + base,
      "K" + base,
      "K" + (base + 4),
      "K" + (base + 5)
    ];

    for (let j = 1; j <= COMPRAS_LINES_PER_REG_; j++) {
      ranges.push("D" + (base + j));
      ranges.push("G" + (base + j));
      ranges.push("I" + (base + j));
    }

    const anyValue = ranges.some(function(a1) {
      const val = sh.getRange(a1).getDisplayValue();
      return String(val || "").trim() !== "";
    });

    if (!anyValue) {
      return {
        ok: true,
        base_row: base,
        sheet: sh
      };
    }
  }

  return {
    ok: false,
    message: "Sem bloco livre em Compras."
  };
}

function confirmarLoteCompras_(payload) {
  const defaults = payload.defaults || {};
  const itens = Array.isArray(payload.itens) ? payload.itens : [];

  if (!itens.length) {
    return {
      ok: false,
      message: "Não há itens válidos para confirmar compras."
    };
  }

  if (itens.length > COMPRAS_LINES_PER_REG_) {
    return {
      ok: false,
      message: "O setor de compras suporta até 6 itens por bloco no momento."
    };
  }

  const bloco = encontrarBlocoLivreCompras_();
  if (!bloco.ok) {
    return {
      ok: false,
      message: bloco.message || "Sem bloco livre em Compras."
    };
  }

  const sh = bloco.sheet;
  const base = bloco.base_row;

  const dataEmissaoIso = comprasParseDateIso_(defaults.data_emissao || "");
  const vencimentoIso = comprasParseDateIso_(defaults.vencimento || "");

  sh.getRange("D" + base).setValue(String(defaults.fornecedor || COMPRA_DEFAULTS_.fornecedor));
  sh.getRange("G" + base).setValue(String(defaults.situacao || COMPRA_DEFAULTS_.situacao));
  sh.getRange("I" + base).setValue(comprasIsoToBr_(dataEmissaoIso));
  sh.getRange("K" + base).setValue(comprasIsoToBr_(vencimentoIso));
  sh.getRange("K" + (base + 4)).setValue(String(defaults.quitar_pagamento || COMPRA_DEFAULTS_.quitar_pagamento));
  sh.getRange("K" + (base + 5)).setValue(String(defaults.conta_bancaria || COMPRA_DEFAULTS_.conta_bancaria));

  itens.forEach(function(item, idx) {
    const row = base + idx + 1;
    sh.getRange("D" + row).setValue(item.produto_oficial || item.produto_falado || "");
    sh.getRange("G" + row).setValue(Number(item.quantidade || 1));
    sh.getRange("I" + row).setValue(Number(item.valor_unitario || 0));

    if (Number(item.valor_unitario || 0) > 0) {
      comprasSalvarPrecoMapa_(
        item.produto_oficial || item.produto_falado || "",
        Number(item.valor_unitario || 0),
        item.persistir_preco ? "usuario_telegram" : "confirmacao_compra"
      );
    }
  });

  const linhas = [];
  linhas.push("Lote de compras confirmado.");
  linhas.push("");
  linhas.push("Bloco: " + base);
  linhas.push("Fornecedor: " + String(defaults.fornecedor || COMPRA_DEFAULTS_.fornecedor));
  linhas.push("Itens: " + itens.length);
  linhas.push("");
  itens.forEach(function(item, idx) {
    linhas.push(
      (idx + 1) + ". " +
      (item.produto_oficial || item.produto_falado || "?") +
      " | qtd " + Number(item.quantidade || 1) +
      " | unit " + comprasFormatMoneyBr_(item.valor_unitario) +
      " | total " + comprasFormatMoneyBr_(item.valor_total)
    );
  });

  return {
    ok: true,
    base_row: base,
    message: linhas.join("\n")
  };
}
