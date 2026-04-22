const PV1_SPREADSHEET_ID = "11IMG566GZByCTvuKQ4LM32QTY16ZhFJMXBY4hDiLir4";
const PV1_DRIVE_REGISTRO_VENDAS_FOLDER_ID = "1he9x9YMog0MZtImMKZUVtEU0ErOz68SY";

function doGet(e) {
  return ContentService
    .createTextOutput(JSON.stringify({
      ok: true,
      service: "AgenteIAPreenchimentovendasaudiotexto",
      status: "online"
    }))
    .setMimeType(ContentService.MimeType.JSON);
}

function doPost(e) {
  try {
    const body = e && e.postData && e.postData.contents
      ? JSON.parse(e.postData.contents)
      : {};

    const action = String(body.action || "").trim();

    if (action === "preencher_lote_v1") {
      return pv1JsonOutput_(pv1PreencherLoteV1_(body));
    }

    if (action === "preview_lote_v1") {
      return pv1JsonOutput_(pv1PreviewLoteV1_(body));
    }

    if (action === "delete_preenchimentos_v1") {
      return pv1JsonOutput_(pv1DeletePreenchimentosV1_(body));
    }

    return pv1JsonOutput_({
      ok: false,
      error: "Ação não reconhecida",
      actionRecebida: action,
      bodyRecebido: body
    });
  } catch (err) {
    return pv1JsonOutput_({
      ok: false,
      error: String(err)
    });
  }
}

/*********************************************************
 * PREENCHIMENTO REAL
 *********************************************************/

function pv1PreencherLoteV1_(body) {
  const pedidos = Array.isArray(body.pedidos) ? body.pedidos : [];
  if (!pedidos.length) {
    return { ok: false, error: "Nenhum pedido recebido" };
  }

  const forcarDuplicata = body && body.force_duplicate_confirmed === true;
  const sh = pv1GetCentralSheet_();
  const resultados = [];
  const grupos = [];

  for (let i = 0; i < pedidos.length; i++) {
    const pedido = pedidos[i];

    try {
      const resolvido = pv1ResolverPedidoPreview_(pedido);

      if (!resolvido.ok) {
        resultados.push(resolvido);
        continue;
      }

      const duplicatas = pv1BuscarDuplicatasRegistro_({
        cliente_oficial: resolvido.cliente_oficial,
        produto_oficial: resolvido.produto_oficial,
        quantidade_gramas: resolvido.quantidade_gramas,
        data_venda: resolvido.data_venda
      });

      if (duplicatas.length && !forcarDuplicata) {
        resultados.push({
          ok: false,
          possible_duplicate: true,
          erro: "Possível duplicata encontrada",
          cliente_oficial: resolvido.cliente_oficial,
          produto_oficial: resolvido.produto_oficial,
          quantidade_gramas: resolvido.quantidade_gramas,
          data_venda: resolvido.data_venda,
          duplicatas: duplicatas
        });
        continue;
      }

      const grupoExistente = grupos.find(function(g) {
        return (
          g.cliente_oficial === resolvido.cliente_oficial &&
          g.data_venda === resolvido.data_venda &&
          g.vencimento === resolvido.vencimento &&
          g.forma_pagamento === resolvido.forma_pagamento &&
          g.itens.length < 4
        );
      });

      if (grupoExistente) {
        grupoExistente.itens.push(resolvido);
      } else {
        grupos.push({
          cliente_oficial: resolvido.cliente_oficial,
          data_venda: resolvido.data_venda,
          vencimento: resolvido.vencimento,
          forma_pagamento: resolvido.forma_pagamento,
          itens: [resolvido]
        });
      }

    } catch (err) {
      resultados.push({
        ok: false,
        erro: String(err)
      });
    }
  }

  for (let g = 0; g < grupos.length; g++) {
    const grupo = grupos[g];
    const bloco = pv1EncontrarProximoBlocoLivre_(sh);

    if (!bloco) {
      grupo.itens.forEach(function(item) {
        resultados.push({
          ok: false,
          erro: "Sem bloco livre na aba Central de Controle API",
          cliente_oficial: item.cliente_oficial,
          produto_oficial: item.produto_oficial
        });
      });
      continue;
    }

    pv1PreencherGrupoNoBloco_(sh, bloco.baseRow, grupo);

    grupo.itens.forEach(function(item, idx) {
      const row = bloco.baseRow + idx;

      const registro = pv1SalvarRegistroVendaDrive_({
        cliente_oficial: item.cliente_oficial,
        cliente_falado: item.cliente_falado,
        produto_oficial: item.produto_oficial,
        produto_falado: item.produto_falado,
        quantidade_gramas: item.quantidade_gramas,
        quantidade_sheet: item.quantidade_sheet,
        data_venda: item.data_venda,
        vencimento: item.vencimento,
        forma_pagamento: item.forma_pagamento,
        valor: item.valor,
        origem: "telegram_v1",
        observacoes: item.pedido_original && item.pedido_original.observacoes
          ? item.pedido_original.observacoes
          : null,
        base_row: bloco.baseRow,
        linha_item: row
      });

      pv1AnexarRegistroNaNotaLinha_(sh, row, registro);

      resultados.push({
        ok: true,
        cliente_oficial: item.cliente_oficial,
        produto_oficial: item.produto_oficial,
        quantidade_gramas: item.quantidade_gramas,
        quantidade_sheet: item.quantidade_sheet,
        valor: item.valor,
        data_venda: item.data_venda,
        vencimento: item.vencimento,
        forma_pagamento: item.forma_pagamento,
        base_row: bloco.baseRow,
        linha_item: row,
        confianca_cliente: item.confianca_cliente,
        confianca_produto: item.confianca_produto,
        sugestao_cliente: item.sugestao_cliente || null,
        registro_drive: registro
      });
    });
  }

  const hasPossibleDuplicate = resultados.some(function(r) {
    return r && r.possible_duplicate;
  });

  return {
    ok: resultados.some(function(r) { return r.ok; }),
    possible_duplicate: hasPossibleDuplicate,
    totalPedidos: pedidos.length,
    totalSucesso: resultados.filter(function(r) { return r.ok; }).length,
    totalFalha: resultados.filter(function(r) { return !r.ok; }).length,
    resultados: resultados
  };
}

/*********************************************************
 * PRÉVIA OFICIAL
 *********************************************************/

function pv1PreviewLoteV1_(body) {
  const pedidos = Array.isArray(body.pedidos) ? body.pedidos : [];
  if (!pedidos.length) {
    return { ok: false, error: "Nenhum pedido recebido" };
  }

  const resultados = [];

  for (let i = 0; i < pedidos.length; i++) {
    const pedido = pedidos[i];

    try {
      const resolvido = pv1ResolverPedidoPreview_(pedido);
      resultados.push(resolvido);
    } catch (err) {
      resultados.push({
        ok: false,
        erro: String(err)
      });
    }
  }

  return {
    ok: resultados.some(function(r) { return r.ok; }),
    totalPedidos: pedidos.length,
    totalSucesso: resultados.filter(function(r) { return r.ok; }).length,
    totalFalha: resultados.filter(function(r) { return !r.ok; }).length,
    resultados: resultados
  };
}

function pv1ResolverPedidoPreview_(pedido) {
  const clienteResolvido = pv1ResolverClienteComIA_(pedido.cliente_falado || "");
  const produtoResolvido = pv1ResolverProdutoComIA_(pedido.produto_falado || "");
  const dataVenda = pv1ResolverData_(pedido.data_falada || null);
  const quantidadeGramas = pv1ConverterQuantidadeParaGramas_(
    pedido.quantidade,
    pedido.unidade
  );
  const quantidadeSheet = pv1ConverterGramasParaValorSheet_(quantidadeGramas);
  const formaPagamento = pv1ResolverFormaPagamento_(pedido.forma_pagamento_falada || null);
  const vencimento = pv1ResolverVencimento_(pedido.vencimento_falado || null, dataVenda);
  const valor = pedido.valor_falado != null && isFinite(Number(pedido.valor_falado))
    ? Number(pedido.valor_falado)
    : null;

  if (!clienteResolvido.ok) {
    return {
      ok: false,
      erro: "Cliente não resolvido",
      cliente_falado: pedido.cliente_falado || null,
      detalhe: clienteResolvido
    };
  }

  if (!produtoResolvido.ok) {
    return {
      ok: false,
      erro: "Produto não resolvido",
      produto_falado: pedido.produto_falado || null,
      detalhe: produtoResolvido
    };
  }

  if (!quantidadeGramas || !isFinite(quantidadeGramas) || quantidadeGramas <= 0) {
    return {
      ok: false,
      erro: "Quantidade inválida",
      quantidade: pedido.quantidade,
      unidade: pedido.unidade
    };
  }

  return {
    ok: true,
    pedido_original: pedido,
    cliente_oficial: clienteResolvido.cliente_oficial,
    cliente_falado: pedido.cliente_falado || null,
    produto_oficial: produtoResolvido.produto_oficial,
    produto_falado: pedido.produto_falado || null,
    quantidade_gramas: quantidadeGramas,
    quantidade_sheet: quantidadeSheet,
    valor: valor,
    data_venda: dataVenda,
    vencimento: vencimento,
    forma_pagamento: formaPagamento,
    confianca_cliente: clienteResolvido.confianca || null,
    confianca_produto: produtoResolvido.confianca || null,
    sugestao_cliente: clienteResolvido.sugestao_texto || null
  };
}

/*********************************************************
 * REMOÇÃO DE PREENCHIMENTOS
 *********************************************************/

function pv1DeletePreenchimentosV1_(body) {
  const sh = pv1GetCentralSheet_();
  const modo = String(body.mode || "").trim();
  const indices = Array.isArray(body.indices) ? body.indices.map(Number).filter(Boolean) : [];

  let basesParaApagar = [];

  if (modo === "all") {
    basesParaApagar = pv1ListarBlocosPreenchidos_(sh).map(function(x) { return x.baseRow; });
  } else if (modo === "last") {
    const preenchidos = pv1ListarBlocosPreenchidos_(sh);
    if (preenchidos.length) {
      basesParaApagar = [preenchidos[preenchidos.length - 1].baseRow];
    }
  } else if (modo === "specific") {
    const preenchidos = pv1ListarBlocosPreenchidos_(sh);
    basesParaApagar = indices
      .map(function(n) {
        const item = preenchidos.find(function(x) { return x.index === n; });
        return item ? item.baseRow : null;
      })
      .filter(Boolean);
  }

  if (!basesParaApagar.length) {
    return {
      ok: false,
      error: "Nenhum preenchimento encontrado para remover."
    };
  }

  const resultados = [];

  for (let i = 0; i < basesParaApagar.length; i++) {
    const baseRow = basesParaApagar[i];

    const cliente = String(sh.getRange("C" + baseRow).getDisplayValue() || "").trim();
    const data = String(sh.getRange("E" + baseRow).getDisplayValue() || "").trim();

    const produtos = [];
    const arquivosRemovidos = [];

    for (let j = 0; j < 4; j++) {
      const row = baseRow + j;
      const produto = String(sh.getRange("G" + row).getDisplayValue() || "").trim();
      if (produto) produtos.push(produto);

      const removidosLinha = pv1RemoverLogsDaLinhaPorNota_(sh, row);
      for (let k = 0; k < removidosLinha.length; k++) {
        arquivosRemovidos.push(removidosLinha[k]);
      }
    }

    pv1LimparBloco_(sh, baseRow);

    resultados.push({
      ok: true,
      base_row: baseRow,
      cliente: cliente || null,
      data: data || null,
      produtos: produtos,
      arquivos_removidos: arquivosRemovidos
    });
  }

  return {
    ok: true,
    totalRemovido: resultados.length,
    resultados: resultados
  };
}

/*********************************************************
 * BLOCO / PLANILHA
 *********************************************************/

function pv1GetSpreadsheet_() {
  return SpreadsheetApp.openById(PV1_SPREADSHEET_ID);
}

function pv1GetCentralSheet_() {
  const ss = pv1GetSpreadsheet_();
  const sh = ss.getSheetByName("Central de Controle API");
  if (!sh) throw new Error('Aba "Central de Controle API" não encontrada.');
  return sh;
}

function pv1GetMapaClientesSheet_() {
  const ss = pv1GetSpreadsheet_();
  const sh = ss.getSheetByName("MAPA_CLIENTES");
  if (!sh) throw new Error('Aba "MAPA_CLIENTES" não encontrada.');
  return sh;
}

function pv1GetBaseRows_() {
  return [50, 54, 58, 62, 66, 70, 74, 78, 82];
}

function pv1EncontrarProximoBlocoLivre_(sh) {
  const bases = pv1GetBaseRows_();

  for (let i = 0; i < bases.length; i++) {
    const baseRow = bases[i];
    const cliente = String(sh.getRange("C" + baseRow).getDisplayValue() || "").trim();

    let ocupado = !!cliente;
    for (let j = 0; j < 4; j++) {
      const r = baseRow + j;
      const produto = String(sh.getRange("G" + r).getDisplayValue() || "").trim();
      const quant = String(sh.getRange("I" + r).getDisplayValue() || "").trim();
      if (produto || quant) {
        ocupado = true;
        break;
      }
    }

    if (!ocupado) {
      return { baseRow: baseRow };
    }
  }

  return null;
}

function pv1PreencherGrupoNoBloco_(sh, baseRow, grupo) {
  const itens = Array.isArray(grupo.itens) ? grupo.itens : [];
  if (!itens.length) return;

  sh.getRange("C" + baseRow).setValue(grupo.cliente_oficial);
  sh.getRange("E" + baseRow).setValue(grupo.data_venda);
  sh.getRange("M" + baseRow).setValue(grupo.vencimento);
  sh.getRange("O" + baseRow).setValue(grupo.forma_pagamento);

  for (let i = 0; i < itens.length; i++) {
    const row = baseRow + i;
    const item = itens[i];

    sh.getRange("G" + row).setValue(item.produto_oficial);
    sh.getRange("I" + row).setValue(item.quantidade_sheet);
    sh.getRange("I" + row).setNumberFormat("0.000");

    if (item.valor != null && isFinite(item.valor)) {
      sh.getRange("K" + row).setValue(Number(item.valor));
      sh.getRange("K" + row).setNumberFormat("0.00");
    } else {
      sh.getRange("K" + row).clearContent();
    }

    const nota = [
      "Origem: Telegram V1",
      "Cliente oficial: " + item.cliente_oficial,
      "Produto oficial: " + item.produto_oficial,
      "Quantidade (g): " + item.quantidade_gramas,
      "Quantidade (sheet): " + item.quantidade_sheet,
      "Valor: " + (item.valor != null ? item.valor : ""),
      "Data: " + item.data_venda,
      "Vencimento: " + item.vencimento,
      "Forma de pagamento: " + item.forma_pagamento,
      "Pedido original: " + JSON.stringify(item.pedido_original || {})
    ].join("\n");

    sh.getRange("G" + row).setNote(nota);
  }
}

function pv1AnexarRegistroNaNotaLinha_(sh, row, registro) {
  const cell = sh.getRange("G" + row);
  const notaAtual = String(cell.getNote() || "").trim();

  const extra = [
    "Registro Drive ID: " + (registro.id_venda || ""),
    "Registro Drive File ID: " + (registro.file_id || ""),
    "Registro Drive Nome: " + (registro.nome_arquivo || "")
  ].join("\n");

  cell.setNote(notaAtual ? (notaAtual + "\n" + extra) : extra);
}

function pv1ListarBlocosPreenchidos_(sh) {
  const bases = pv1GetBaseRows_();
  const out = [];

  for (let i = 0; i < bases.length; i++) {
    const baseRow = bases[i];
    const cliente = String(sh.getRange("C" + baseRow).getDisplayValue() || "").trim();

    let ocupado = !!cliente;
    for (let j = 0; j < 4; j++) {
      const row = baseRow + j;
      const produto = String(sh.getRange("G" + row).getDisplayValue() || "").trim();
      const quant = String(sh.getRange("I" + row).getDisplayValue() || "").trim();
      if (produto || quant) {
        ocupado = true;
        break;
      }
    }

    if (ocupado) {
      out.push({
        index: out.length + 1,
        baseRow: baseRow
      });
    }
  }

  return out;
}

function pv1LimparBloco_(sh, baseRow) {
  sh.getRange("C" + baseRow).clearContent();
  sh.getRange("E" + baseRow).clearContent();
  sh.getRange("M" + baseRow).clearContent();
  sh.getRange("O" + baseRow).clearContent();

  for (let i = 0; i < 4; i++) {
    const row = baseRow + i;
    sh.getRange("G" + row).clearContent().clearNote();
    sh.getRange("I" + row).clearContent();
    sh.getRange("K" + row).clearContent();
  }
}

/*********************************************************
 * LOGS DRIVE
 *********************************************************/

function pv1SalvarRegistroVendaDrive_(dados) {
  const root = DriveApp.getFolderById(PV1_DRIVE_REGISTRO_VENDAS_FOLDER_ID);
  const data = new Date(dados.data_venda + "T12:00:00");

  const cliente = pv1SanitizeFileName_(dados.cliente_oficial || "SEM_CLIENTE");
  const ano = Utilities.formatDate(data, Session.getScriptTimeZone(), "yyyy");
  const mes = pv1NomeMesPt_(data);

  const pastaCliente = pv1GetOrCreateSubFolder_(root, cliente);
  const pastaAno = pv1GetOrCreateSubFolder_(pastaCliente, ano);
  const pastaMes = pv1GetOrCreateSubFolder_(pastaAno, mes);

  const idVenda = "PV1_" + Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyyMMdd_HHmmss_SSS");
  const nomeArquivo = [
    cliente,
    dados.data_venda,
    String(dados.quantidade_gramas) + "g",
    pv1SanitizeFileName_(dados.produto_oficial || "SEM_PRODUTO"),
    idVenda
  ].join("__") + ".json";

  const payload = {
    id_venda: idVenda,
    data_venda: dados.data_venda,
    cliente_oficial: dados.cliente_oficial,
    cliente_falado: dados.cliente_falado,
    produto_oficial: dados.produto_oficial,
    produto_falado: dados.produto_falado,
    quantidade_gramas: dados.quantidade_gramas,
    quantidade_sheet: dados.quantidade_sheet,
    valor: dados.valor,
    origem: dados.origem,
    observacoes: dados.observacoes,
    base_row: dados.base_row,
    linha_item: dados.linha_item || null,
    forma_pagamento: dados.forma_pagamento || "PIX",
    vencimento: dados.vencimento || null,
    criado_em: new Date().toISOString()
  };

  const file = pastaMes.createFile(
    nomeArquivo,
    JSON.stringify(payload, null, 2),
    MimeType.PLAIN_TEXT
  );

  return {
    id_venda: idVenda,
    nome_arquivo: nomeArquivo,
    file_id: file.getId()
  };
}

function pv1RemoverLogsDaLinhaPorNota_(sh, row) {
  const note = String(sh.getRange("G" + row).getNote() || "");
  const removidos = [];

  const matches = [...note.matchAll(/Registro Drive File ID:\s*([A-Za-z0-9\-_]+)/g)];
  for (let i = 0; i < matches.length; i++) {
    const fileId = String(matches[i][1] || "").trim();
    if (!fileId) continue;

    try {
      const file = DriveApp.getFileById(fileId);
      removidos.push({
        file_id: fileId,
        nome_arquivo: file.getName()
      });
      file.setTrashed(true);
    } catch (err) {
      removidos.push({
        file_id: fileId,
        nome_arquivo: null,
        erro: String(err)
      });
    }
  }

  return removidos;
}

function pv1BuscarDuplicatasRegistro_(filtro) {
  const root = DriveApp.getFolderById(PV1_DRIVE_REGISTRO_VENDAS_FOLDER_ID);
  const cliente = pv1SanitizeFileName_(filtro.cliente_oficial || "");
  if (!cliente) return [];

  const ano = String(filtro.data_venda || "").slice(0, 4);
  if (!ano) return [];

  const dataBase = new Date(filtro.data_venda + "T12:00:00");
  const mes = pv1NomeMesPt_(dataBase);

  const pastaCliente = pv1FindSubFolderByName_(root, cliente);
  if (!pastaCliente) return [];

  const pastaAno = pv1FindSubFolderByName_(pastaCliente, ano);
  if (!pastaAno) return [];

  const pastaMes = pv1FindSubFolderByName_(pastaAno, mes);
  if (!pastaMes) return [];

  const files = pastaMes.getFiles();
  const duplicatas = [];

  while (files.hasNext()) {
    const file = files.next();

    try {
      const raw = file.getBlob().getDataAsString();
      const json = JSON.parse(raw);

      const mesmoCliente =
        pv1Norm_(json.cliente_oficial || "") === pv1Norm_(filtro.cliente_oficial || "");
      const mesmoProduto =
        pv1Norm_(json.produto_oficial || "") === pv1Norm_(filtro.produto_oficial || "");
      const mesmaQuantidade =
        Number(json.quantidade_gramas || 0) === Number(filtro.quantidade_gramas || 0);
      const mesmaData =
        String(json.data_venda || "") === String(filtro.data_venda || "");

      if (mesmoCliente && mesmoProduto && mesmaQuantidade && mesmaData) {
        duplicatas.push({
          id_venda: json.id_venda || null,
          cliente_oficial: json.cliente_oficial || null,
          produto_oficial: json.produto_oficial || null,
          quantidade_gramas: json.quantidade_gramas || null,
          data_venda: json.data_venda || null,
          valor: json.valor || null,
          file_id: file.getId(),
          nome_arquivo: file.getName()
        });
      }
    } catch (err) {}
  }

  return duplicatas;
}

function pv1GetOrCreateSubFolder_(parent, nome) {
  const it = parent.getFoldersByName(nome);
  if (it.hasNext()) return it.next();
  return parent.createFolder(nome);
}

function pv1FindSubFolderByName_(parent, nome) {
  const it = parent.getFoldersByName(nome);
  if (it.hasNext()) return it.next();
  return null;
}

/*********************************************************
 * CLIENTE COM IA
 *********************************************************/

function pv1ListarMapaClientes_() {
  const sh = pv1GetMapaClientesSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const vals = sh.getRange(2, 1, lastRow - 1, 13).getDisplayValues();
  const out = [];

  for (let i = 0; i < vals.length; i++) {
    const row = vals[i];
    const bases = [];

    for (let c = 0; c < 10; c++) {
      bases.push(String(row[c] || "").trim());
    }

    const cliente = String(row[10] || "").trim();
    const ativo = pv1Norm_(row[12] || "");

    if (!cliente) continue;
    if (ativo && !["sim", "s", "ativo", "1", "true"].includes(ativo)) continue;

    out.push({
      rowNumber: i + 2,
      nome_bases: bases.filter(Boolean),
      cliente_oficial: cliente
    });
  }

  return out;
}

function pv1ResolverClienteComIA_(nomeFalado) {
  const nomeLimpo = String(nomeFalado || "").trim();
  const nomeNorm = pv1Norm_(nomeLimpo);

  if (!nomeNorm) {
    return { ok: false, motivo: "Nome do cliente vazio" };
  }

  const mapa = pv1ListarMapaClientes_();
  if (!mapa.length) {
    return { ok: false, motivo: "MAPA_CLIENTES vazio" };
  }

  for (let i = 0; i < mapa.length; i++) {
    const item = mapa[i];
    for (let j = 0; j < item.nome_bases.length; j++) {
      const base = item.nome_bases[j];
      if (pv1Norm_(base) === nomeNorm) {
        return {
          ok: true,
          cliente_oficial: item.cliente_oficial,
          confianca: 1,
          motivo: "match exato nome_base"
        };
      }
    }
  }

  for (let i = 0; i < mapa.length; i++) {
    const item = mapa[i];
    if (pv1Norm_(item.cliente_oficial) === nomeNorm) {
      return {
        ok: true,
        cliente_oficial: item.cliente_oficial,
        confianca: 0.99,
        motivo: "match exato cliente_oficial"
      };
    }
  }

  const candidatos = [];
  for (let i = 0; i < mapa.length; i++) {
    const item = mapa[i];
    const comparacoes = item.nome_bases.concat([item.cliente_oficial]).filter(Boolean);

    for (let j = 0; j < comparacoes.length; j++) {
      const comp = comparacoes[j];
      const score = pv1ScoreTexto_(nomeNorm, pv1Norm_(comp));
      candidatos.push({
        cliente_oficial: item.cliente_oficial,
        comparado_com: comp,
        score: score
      });
    }
  }

  candidatos.sort(function(a, b) { return b.score - a.score; });

  const top1 = candidatos[0];

  if (top1 && top1.score >= 0.90) {
    return {
      ok: true,
      cliente_oficial: top1.cliente_oficial,
      confianca: top1.score,
      motivo: "fuzzy alto"
    };
  }

  const respIA = pv1ResolverClienteIA_(nomeLimpo, mapa);

  if (!respIA || respIA.ok !== true) {
    return {
      ok: false,
      motivo: respIA && respIA.motivo ? respIA.motivo : "IA não conseguiu resolver cliente",
      resposta_ia: respIA || null
    };
  }

  let escolhido = null;

  if (respIA.cliente_index != null && isFinite(Number(respIA.cliente_index))) {
    const idx = Number(respIA.cliente_index) - 1;
    if (idx >= 0 && idx < mapa.length) {
      escolhido = mapa[idx].cliente_oficial;
    }
  }

  if (!escolhido && respIA.cliente_oficial) {
    const achado = mapa.find(function(x) {
      return pv1Norm_(x.cliente_oficial) === pv1Norm_(respIA.cliente_oficial);
    });
    escolhido = achado ? achado.cliente_oficial : null;
  }

  if (!escolhido) {
    return {
      ok: false,
      motivo: "IA retornou cliente fora da lista oficial",
      resposta_ia: respIA
    };
  }

  const confianca = respIA.confianca != null ? Number(respIA.confianca) : null;
  if (confianca != null && confianca < 0.60) {
    return {
      ok: false,
      motivo: "Cliente ambíguo com baixa confiança",
      resposta_ia: respIA
    };
  }

  return {
    ok: true,
    cliente_oficial: escolhido,
    confianca: confianca,
    sugestao_texto: confianca != null && confianca < 0.85 ? ("Você quis dizer " + escolhido + "?") : null,
    motivo: respIA.motivo || "IA"
  };
}

function pv1ResolverClienteIA_(nomeFalado, mapa) {
  const linhas = mapa.map(function(item, i) {
    return [
      (i + 1) + ". Cliente oficial: " + item.cliente_oficial,
      "Aliases: " + (item.nome_bases.length ? item.nome_bases.join(" | ") : "(sem aliases)")
    ].join("\n");
  }).join("\n\n");

  const prompt = [
    "Você é um resolvedor de cliente oficial.",
    "Escolha o cliente oficial mais provável a partir da lista fornecida.",
    "Considere erros de fala, transcrição, nomes próprios raros, sílabas trocadas e grafias aproximadas.",
    "RETORNE o cliente oficial exatamente como está na lista.",
    "Também retorne o índice do cliente escolhido.",
    "Se houver cliente suficientemente provável, retorne ok=true.",
    "Se estiver ambíguo ou fraco, retorne ok=false.",
    "",
    "Retorne SOMENTE JSON válido neste formato:",
    "{",
    '  "ok": true ou false,',
    '  "cliente_index": number ou null,',
    '  "cliente_oficial": "string ou null",',
    '  "confianca": number,',
    '  "motivo": "string"',
    "}",
    "",
    "Nome falado:",
    nomeFalado,
    "",
    "Lista oficial de clientes:",
    linhas
  ].join("\n");

  return pv1OpenAIJson_(prompt);
}

/*********************************************************
 * PRODUTO COM IA
 *********************************************************/

function pv1ListarProdutosColunaQ_() {
  const sh = pv1GetMapaClientesSheet_();
  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const vals = sh.getRange(2, 17, lastRow - 1, 1).getDisplayValues();
  const unicos = {};
  const out = [];

  vals.forEach(function(r) {
    const nome = String(r[0] || "").trim();
    if (!nome) return;
    const key = pv1Norm_(nome);
    if (unicos[key]) return;
    unicos[key] = true;
    out.push(nome);
  });

  return out;
}

function pv1ResolverProdutoComIA_(produtoFalado) {
  const produto = String(produtoFalado || "").trim();
  if (!produto) {
    return { ok: false, motivo: "Produto vazio" };
  }

  const produtos = pv1ListarProdutosColunaQ_();
  if (!produtos.length) {
    return { ok: false, motivo: "Lista de produtos da coluna Q vazia" };
  }

  const prompt = [
    "Você é um resolvedor de produto oficial.",
    "Escolha o produto oficial mais provável a partir da lista fornecida.",
    "Considere erros de fala, transcrição, palavras coladas, números por extenso, cm, faixas como 60/65cm.",
    "RETORNE o produto exatamente como está na lista oficial.",
    "Também retorne o índice do item escolhido.",
    "Se houver produto suficientemente provável, retorne ok=true.",
    "Se estiver ambíguo ou fraco, retorne ok=false.",
    "",
    "Retorne SOMENTE JSON válido neste formato:",
    "{",
    '  "ok": true ou false,',
    '  "produto_index": number ou null,',
    '  "produto_oficial": "string ou null",',
    '  "confianca": number,',
    '  "motivo": "string"',
    "}",
    "",
    "Produto falado:",
    produto,
    "",
    "Lista de produtos oficiais:",
    produtos.map(function(p, i) { return (i + 1) + ". " + p; }).join("\n")
  ].join("\n");

  const resp = pv1OpenAIJson_(prompt);

  if (!resp || resp.ok !== true) {
    return {
      ok: false,
      motivo: resp && resp.motivo ? resp.motivo : "IA não conseguiu resolver",
      resposta_ia: resp || null
    };
  }

  let escolhido = null;

  if (resp.produto_index != null && isFinite(Number(resp.produto_index))) {
    const idx = Number(resp.produto_index) - 1;
    if (idx >= 0 && idx < produtos.length) {
      escolhido = produtos[idx];
    }
  }

  if (!escolhido && resp.produto_oficial) {
    escolhido = produtos.find(function(p) {
      return pv1Norm_(p) === pv1Norm_(resp.produto_oficial);
    }) || null;
  }

  if (!escolhido) {
    return {
      ok: false,
      motivo: "IA retornou produto fora da lista oficial",
      resposta_ia: resp
    };
  }

  return {
    ok: true,
    produto_oficial: escolhido,
    confianca: resp.confianca != null ? Number(resp.confianca) : null,
    motivo: resp.motivo || "IA"
  };
}

/*********************************************************
 * OPENAI
 *********************************************************/

function pv1OpenAIJson_(prompt) {
  const apiKey = PropertiesService.getScriptProperties().getProperty("OPENAI_API_KEY");
  if (!apiKey) {
    throw new Error("Falta Script Property OPENAI_API_KEY");
  }

  const resp = UrlFetchApp.fetch("https://api.openai.com/v1/chat/completions", {
    method: "post",
    contentType: "application/json",
    muteHttpExceptions: true,
    headers: {
      Authorization: "Bearer " + apiKey
    },
    payload: JSON.stringify({
      model: "gpt-4.1-mini",
      messages: [
        {
          role: "system",
          content: "Responda apenas JSON válido, sem markdown, sem comentário, sem texto extra."
        },
        {
          role: "user",
          content: prompt
        }
      ],
      temperature: 0,
      response_format: { type: "json_object" }
    })
  });

  const code = resp.getResponseCode();
  const text = resp.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error("Erro OpenAI: HTTP " + code + " => " + text);
  }

  const json = JSON.parse(text);
  const content = json.choices && json.choices[0] && json.choices[0].message
    ? json.choices[0].message.content
    : "{}";

  return JSON.parse(content);
}

/*********************************************************
 * DATA / QUANTIDADE / PAGAMENTO
 *********************************************************/

function pv1ResolverData_(dataFalada) {
  const sOriginal = String(dataFalada || "").trim();
  const s = pv1Norm_(sOriginal);

  const tz = Session.getScriptTimeZone();
  const hoje = new Date();

  if (!s) {
    return Utilities.formatDate(hoje, tz, "yyyy-MM-dd");
  }

  if (s === "hoje") {
    return Utilities.formatDate(hoje, tz, "yyyy-MM-dd");
  }

  if (s === "ontem") {
    const d = new Date(hoje);
    d.setDate(d.getDate() - 1);
    return Utilities.formatDate(d, tz, "yyyy-MM-dd");
  }

  if (s === "antes de ontem") {
    const d = new Date(hoje);
    d.setDate(d.getDate() - 2);
    return Utilities.formatDate(d, tz, "yyyy-MM-dd");
  }

  if (s === "amanha") {
    const d = new Date(hoje);
    d.setDate(d.getDate() + 1);
    return Utilities.formatDate(d, tz, "yyyy-MM-dd");
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(sOriginal)) {
    return sOriginal;
  }

  let m = sOriginal.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    return m[3] + "-" + String(m[2]).padStart(2, "0") + "-" + String(m[1]).padStart(2, "0");
  }

  m = sOriginal.match(/^(\d{1,2})\/(\d{1,2})$/);
  if (m) {
    const ano = Utilities.formatDate(hoje, tz, "yyyy");
    return ano + "-" + String(m[2]).padStart(2, "0") + "-" + String(m[1]).padStart(2, "0");
  }

  return Utilities.formatDate(hoje, tz, "yyyy-MM-dd");
}

function pv1ConverterQuantidadeParaGramas_(quantidade, unidade) {
  const q = Number(quantidade);
  const u = pv1Norm_(unidade || "g");

  if (!isFinite(q) || q <= 0) return null;
  if (u === "kg") return q * 1000;
  return q;
}

function pv1ConverterGramasParaValorSheet_(quantidadeGramas) {
  const g = Number(quantidadeGramas);
  if (!isFinite(g) || g <= 0) return null;
  return g / 1000;
}

function pv1ResolverFormaPagamento_(formaFalado) {
  const s = pv1Norm_(formaFalado || "");
  if (!s) return "PIX";
  if (s.indexOf("pix") >= 0) return "PIX";
  if (s.indexOf("dinheiro") >= 0) return "Dinheiro à Vista";
  if (s.indexOf("a vista") >= 0 || s.indexOf("avista") >= 0) return "Dinheiro à Vista";
  return "PIX";
}

function pv1ResolverVencimento_(vencimentoFalado, dataVenda) {
  const s = String(vencimentoFalado || "").trim();

  if (!s) {
    return pv1SomarDiasDataIso_(dataVenda, 15);
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) {
    return m[3] + "-" + String(m[2]).padStart(2, "0") + "-" + String(m[1]).padStart(2, "0");
  }

  return pv1SomarDiasDataIso_(dataVenda, 15);
}

function pv1SomarDiasDataIso_(dataIso, dias) {
  const d = new Date(dataIso + "T12:00:00");
  d.setDate(d.getDate() + dias);
  return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
}

/*********************************************************
 * HELPERS
 *********************************************************/

function pv1JsonOutput_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

function pv1Norm_(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function pv1Tokens_(s) {
  return pv1Norm_(s).split(/[^a-z0-9]+/).filter(Boolean);
}

function pv1ScoreTexto_(a, b) {
  if (!a || !b) return 0;
  if (a === b) return 1;
  if (a.indexOf(b) >= 0 || b.indexOf(a) >= 0) return 0.92;

  const ta = pv1Tokens_(a);
  const tb = pv1Tokens_(b);
  if (!ta.length || !tb.length) return 0;

  const mapa = {};
  tb.forEach(function(x) { mapa[x] = true; });

  const inter = ta.filter(function(x) { return mapa[x]; }).length;
  const uniao = Object.keys(
    ta.concat(tb).reduce(function(acc, x) {
      acc[x] = true;
      return acc;
    }, {})
  ).length;

  const jaccard = uniao ? inter / uniao : 0;
  let score = jaccard * 0.7;

  if (ta[0] && tb[0] && ta[0] === tb[0]) score += 0.15;
  if (inter >= 2) score += 0.12;

  return Math.min(0.99, score);
}

function pv1SanitizeFileName_(s) {
  return String(s || "")
    .replace(/[\\\/:*?"<>|#%{}~&]/g, "_")
    .replace(/\s+/g, "_")
    .substring(0, 180);
}

function pv1NomeMesPt_(data) {
  const meses = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
  ];
  return meses[data.getMonth()];
}

/*********************************************************
 * TESTES
 *********************************************************/

function pv1TestePreviewLocal() {
  const body = {
    pedidos: [
      {
        cliente_falado: "Diege",
        produto_falado: "louro liga branca 65/70",
        quantidade: 100,
        unidade: "g",
        data_falada: "18/04",
        valor_falado: 5600,
        forma_pagamento_falada: null,
        vencimento_falado: null,
        observacoes: "teste preview"
      }
    ]
  };

  Logger.log(JSON.stringify(pv1PreviewLoteV1_(body), null, 2));
}

function pv1TesteDeleteLastLocal() {
  Logger.log(JSON.stringify(pv1DeletePreenchimentosV1_({ mode: "last" }), null, 2));
}

function pv1TestePreencherLoteLocal() {
  const body = {
    pedidos: [
      {
        cliente_falado: "Ricardo",
        produto_falado: "loiro liga branca",
        quantidade: 200,
        unidade: "g",
        data_falada: "18/04",
        valor_falado: 5500,
        forma_pagamento_falada: null,
        vencimento_falado: null,
        observacoes: "teste item 1"
      },
      {
        cliente_falado: "Ricardo",
        produto_falado: "castanho liga rosa 65",
        quantidade: 500,
        unidade: "g",
        data_falada: "18/04",
        valor_falado: 3000,
        forma_pagamento_falada: null,
        vencimento_falado: null,
        observacoes: "teste item 2"
      }
    ]
  };

  Logger.log(JSON.stringify(pv1PreencherLoteV1_(body), null, 2));
}
