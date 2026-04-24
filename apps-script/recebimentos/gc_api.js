const PROIB = {
  id: true,
  codigo: true,
  hash: true,
  hash_parcelamento: true,
  hash_contrato: true
};

function isTransientFetchError_(e) {
  const msg = String(e && (e.message || e)) || "";
  return (
    msg.includes("Endereço indisponível") ||
    msg.includes("Address unavailable") ||
    msg.includes("Service invoked too many times") ||
    msg.includes("Exception: Service") ||
    msg.includes("timed out") ||
    msg.includes("Timeout") ||
    msg.includes("Internal error") ||
    msg.includes("backendError") ||
    msg.includes("DNS") ||
    msg.includes("socket")
  );
}

function fetchWithRetry_(url, fetchOpt, tentativas) {
  tentativas = tentativas || 4;
  let lastErr;

  for (let i = 0; i < tentativas; i++) {
    try {
      return UrlFetchApp.fetch(url, fetchOpt);
    } catch (e) {
      lastErr = e;
      if (!isTransientFetchError_(e) || i === tentativas - 1) throw e;
      Utilities.sleep(500 * (i + 1));
    }
  }

  throw lastErr;
}

function getGcPropWithFallback_(primaryKey, legacyKey) {
  const props = PropertiesService.getScriptProperties();
  return (
    props.getProperty(primaryKey) ||
    props.getProperty(legacyKey) ||
    ""
  ).trim();
}

function getConfig_() {
  const API = getGcPropWithFallback_("GC_API_BASE", "API_BASE").replace(/\/+$/, "");
  const access = getGcPropWithFallback_("GC_ACCESS_TOKEN", "ACCESS_TOKEN");
  const secret = getGcPropWithFallback_("GC_SECRET_ACCESS_TOKEN", "SECRET_ACCESS_TOKEN");

  if (!API || !access || !secret) {
    throw new Error(
      "Faltam Script Properties. Use GC_API_BASE / GC_ACCESS_TOKEN / GC_SECRET_ACCESS_TOKEN"
    );
  }

  const HEAD = {
    "Content-Type": "application/json",
    "access-token": access,
    "secret-access-token": secret
  };

  return { API, HEAD };
}

function jreq_(method, path, opt) {
  opt = opt || {};

  const conf = getConfig_();
  let url = conf.API + path;

  if (opt.params) {
    const qs = Object.keys(opt.params)
      .filter(function(k) {
        return opt.params[k] !== undefined && opt.params[k] !== null && opt.params[k] !== "";
      })
      .map(function(k) {
        return encodeURIComponent(k) + "=" + encodeURIComponent(String(opt.params[k]));
      })
      .join("&");

    if (qs) url += (url.indexOf("?") >= 0 ? "&" : "?") + qs;
  }

  const fetchOpt = {
    method: method,
    headers: conf.HEAD,
    muteHttpExceptions: true
  };

  if (opt.json) {
    fetchOpt.payload = JSON.stringify(opt.json);
  }

  const resp = fetchWithRetry_(url, fetchOpt, 4);

  const code = resp.getResponseCode();
  const text = resp.getContentText() || "";

  if (code < 200 || code >= 300) {
    throw new Error("HTTP " + code + " em " + path + ": " + text);
  }

  let js;
  try {
    js = JSON.parse(text);
  } catch (e) {
    const inicio = text.slice(0, 400).replace(/\s+/g, " ").trim();
    throw new Error("Resposta inválida (não-JSON) em " + path + ". Início: " + inicio);
  }

  return (js && typeof js === "object" && "data" in js) ? js.data : js;
}

function gcNormKey_(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function mapClienteParaId_(nome) {
  const key = gcNormKey_(nome);
  if (!key) throw new Error("Nome do cliente vazio.");

  const cache = CacheService.getScriptCache();
  const cacheKey = "GC_REC_CLIENTE_ID_BYNAME_" + key;

  const cached = cache.get(cacheKey);
  if (cached) return String(cached);

  const lista = jreq_("GET", "/clientes", {
    params: {
      nome: nome,
      ativo: "1",
      limit: 100
    }
  });

  if (!Array.isArray(lista) || !lista.length) {
    throw new Error('Cliente não encontrado na API: "' + nome + '"');
  }

  let escolhido = lista.find(function(c) {
    return gcNormKey_(c && c.nome || "") === key;
  });

  if (!escolhido) {
    escolhido = lista.find(function(c) {
      const n = gcNormKey_(c && c.nome || "");
      return n.indexOf(key) >= 0 || key.indexOf(n) >= 0;
    });
  }

  if (!escolhido) escolhido = lista[0];

  if (!escolhido || !escolhido.id) {
    throw new Error('Resposta da API não trouxe "id" para o cliente "' + nome + '".');
  }

  cache.put(cacheKey, String(escolhido.id), 21600);
  return String(escolhido.id);
}