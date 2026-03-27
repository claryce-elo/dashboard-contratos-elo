"""
Cliente SIGA - Extrai dados de contratos e alunos do sistema Activesoft.

Estrategia:
  1. /assinatura_eletronica/contratos/ - lista de contratos com status de assinatura
  2. /api/v1/turma/ - turmas 2026 (para cruzar quem NAO tem contrato)
  3. Aluno na turma mas sem contrato = "Sem Contrato"
"""

import re
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

SIGA_URL = "https://siga02.activesoft.com.br"

UNIDADES = [
    {"pk": 2, "codigo": "BV", "nome": "Boa Viagem"},
    {"pk": 3, "codigo": "CD", "nome": "Candeias"},
    {"pk": 4, "codigo": "JG", "nome": "Janga"},
    {"pk": 5, "codigo": "CDR", "nome": "Cordeiro"},
]

UNIDADE_MAP = {u["codigo"]: u["nome"] for u in UNIDADES}

CONTRATOS_URL = f"{SIGA_URL}/assinatura_eletronica/contratos/"


def _extract_csrf(html):
    m = re.search(r'name=["\']csrfmiddlewaretoken["\'].*?value=["\']([^"\']+)["\']', html)
    if m:
        return m.group(1)
    m = re.search(r'value=["\']([^"\']+)["\'].*?name=["\']csrfmiddlewaretoken["\']', html)
    return m.group(1) if m else None


def _login(instituicao, login, senha, unidade):
    """Autentica no SIGA e retorna (sessao, erro_msg)."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/html, */*",
    })
    try:
        r = s.get(f"{SIGA_URL}/login/", timeout=15)
    except Exception as e:
        return None, f"Nao conseguiu acessar {SIGA_URL}/login/: {e}"

    csrf = s.cookies.get("csrftoken", "") or _extract_csrf(r.text) or ""
    if not csrf:
        return None, "CSRF token nao encontrado"

    r = s.post(f"{SIGA_URL}/login/", data={
        "csrfmiddlewaretoken": csrf,
        "codigo": instituicao,
        "login": login,
        "senha": senha,
    }, headers={"Referer": f"{SIGA_URL}/login/"}, allow_redirects=True, timeout=15)

    if "/login/unidade" not in r.url:
        return None, f"Login falhou (redirecionou para {r.url}). Verifique credenciais."

    csrf2 = s.cookies.get("csrftoken", csrf)
    s.post(f"{SIGA_URL}/login/unidade/", data={
        "csrfmiddlewaretoken": csrf2,
        "unidade": str(unidade["pk"]),
    }, headers={"Referer": f"{SIGA_URL}/login/unidade/"}, allow_redirects=True, timeout=15)

    return s, None


def _is_json(response):
    ct = response.headers.get("content-type", "")
    if "application/json" in ct:
        return True
    try:
        response.json()
        return True
    except Exception:
        return False


# =========================================================================
# EXTRACAO DE CONTRATOS
# =========================================================================

def _explorar_pagina_contratos(session):
    """Acessa a pagina de contratos e analisa profundamente o HTML e JS."""
    info = {"url": CONTRATOS_URL, "status": None, "tipo": None, "erro": None}

    try:
        r = session.get(CONTRATOS_URL, timeout=30)
        info["status"] = r.status_code

        if r.status_code != 200:
            info["erro"] = f"Status {r.status_code}"
            return info

        ct = r.headers.get("content-type", "")
        info["tipo"] = ct

        if "application/json" in ct:
            info["dados"] = r.json()
            return info

        html = r.text
        info["html_size"] = len(html)

        # 1. Encontrar arquivos JS carregados pela pagina
        js_srcs = re.findall(r'<script[^>]*src=["\']([^"\']+)["\']', html)
        info["js_files"] = js_srcs

        # 2. URLs encontradas no HTML inline
        api_urls = set()
        for pattern in [
            r'["\'](/api/v[^"\']+)["\']',
            r'["\'](/assinatura[^"\']+)["\']',
            r'["\'](https?://[^"\']*contrat[^"\']*)["\']',
        ]:
            api_urls.update(re.findall(pattern, html))
        info["api_urls_html"] = list(api_urls)

        # 3. Inline scripts - mostrar conteudo resumido
        scripts_inline = re.findall(r'<script(?:\s[^>]*)?>([^<]+)</script>', html, re.DOTALL)
        info["scripts_inline"] = [s.strip()[:200] for s in scripts_inline if s.strip() and len(s.strip()) > 10]

        # 4. Amostra do HTML body (sem tags de estilo/head)
        body_match = re.search(r'<body[^>]*>(.*)</body>', html, re.DOTALL)
        if body_match:
            body = body_match.group(1)
            # Remover scripts e styles inline
            body_clean = re.sub(r'<script[^>]*>.*?</script>', '', body, flags=re.DOTALL)
            body_clean = re.sub(r'<style[^>]*>.*?</style>', '', body_clean, flags=re.DOTALL)
            body_clean = re.sub(r'\s+', ' ', body_clean).strip()
            info["body_amostra"] = body_clean[:500]

        # 5. IDs e classes relevantes (hints de framework)
        divs_id = re.findall(r'<div[^>]*id=["\']([^"\']+)["\']', html)
        info["div_ids"] = divs_id

        # 6. Vasculhar arquivos JS externos para encontrar endpoints
        js_api_urls = set()
        for js_src in js_srcs:
            if "jquery" in js_src.lower() or "bootstrap" in js_src.lower():
                continue
            full_url = js_src if js_src.startswith("http") else f"{SIGA_URL}{js_src}"
            try:
                r_js = session.get(full_url, timeout=15)
                if r_js.status_code == 200 and len(r_js.text) > 50:
                    js_text = r_js.text
                    # Procurar URLs de API no JS
                    for p in [
                        r'["\'](/api/v[^"\']{3,60})["\']',
                        r'["\'](/assinatura_eletronica/[^"\']{3,80})["\']',
                        r'url\s*[=:]\s*["\']([^"\']{5,80})["\']',
                        r'endpoint\s*[=:]\s*["\']([^"\']{5,80})["\']',
                        r'baseURL\s*[=:]\s*["\']([^"\']{5,80})["\']',
                    ]:
                        js_api_urls.update(re.findall(p, js_text))

                    # Procurar referencias a contratos/competencia
                    contrato_refs = re.findall(
                        r'.{0,40}(?:contrat|competencia|assinatura).{0,60}',
                        js_text, re.IGNORECASE
                    )
                    if contrato_refs:
                        info.setdefault("js_contrato_refs", []).extend(
                            [ref.strip()[:120] for ref in contrato_refs[:5]]
                        )
            except Exception:
                continue

        info["js_api_urls"] = list(js_api_urls)

    except Exception as e:
        info["erro"] = str(e)

    return info


def _explorar_endpoints_contratos(session, competencia="2026"):
    """Testa todos os endpoints descobertos nos JS da pagina de contratos.

    Endpoints encontrados nos JS:
      - baseURL + /situacao_contratos/
      - /api/v1/assinatura_eletronica/dashboard/documentos/indicador/
      - /api/v1/turmas/ (com 's')
    """
    resultados = []

    # Endpoints reais extraidos do JS do SPA
    endpoints = [
        # Situacao dos contratos (encontrado no JS: this.baseURL + "/situacao_contratos/")
        "/api/v1/assinatura_eletronica/situacao_contratos/",
        "/api/v1/assinatura_eletronica/contratos/situacao_contratos/",
        "/assinatura_eletronica/contratos/situacao_contratos/",
        "/assinatura_eletronica/situacao_contratos/",
        # Dashboard indicadores (encontrado no JS)
        "/api/v1/assinatura_eletronica/dashboard/documentos/indicador/",
        # Documentos/contratos
        "/api/v1/assinatura_eletronica/documentos/",
        "/api/v1/assinatura_eletronica/contratos/",
        "/api/v1/assinatura_eletronica/contratos/documentos/",
        "/api/v1/assinatura_eletronica/",
        # Turmas com 's' (encontrado no JS, diferente de /turma/)
        "/api/v1/turmas/",
    ]

    params_variantes = [
        {"competencia": competencia},
        {"competencia": competencia, "limit": 500, "offset": 0},
        {"ano": competencia},
        {"periodo": competencia},
        {"limit": 500, "offset": 0},
        {},
    ]

    for endpoint in endpoints:
        for params in params_variantes:
            try:
                r = session.get(f"{SIGA_URL}{endpoint}", params=params, timeout=20,
                    headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
                if r.status_code == 200 and _is_json(r):
                    data = r.json()
                    info = {
                        "endpoint": endpoint,
                        "params": params,
                        "tipo": type(data).__name__,
                    }
                    if isinstance(data, list):
                        info["count"] = len(data)
                        if data:
                            info["campos"] = list(data[0].keys()) if isinstance(data[0], dict) else []
                            info["amostra"] = data[0]
                    elif isinstance(data, dict):
                        info["keys"] = list(data.keys())
                        if "results" in data:
                            info["count"] = data.get("count", len(data["results"]))
                            if data["results"]:
                                info["campos"] = list(data["results"][0].keys()) if isinstance(data["results"][0], dict) else []
                                info["amostra"] = data["results"][0]
                        elif "count" in data:
                            info["count"] = data["count"]
                        else:
                            info["amostra"] = {k: v for k, v in list(data.items())[:8]}
                    resultados.append(info)
                    break  # Encontrou params que funcionam, nao testar mais
            except Exception:
                continue

    return resultados


def _parsear_contratos_html(session, competencia="2026"):
    """Tenta extrair contratos do HTML da pagina com filtros."""
    registros = []
    csrf = session.cookies.get("csrftoken", "")

    # Tentar GET e POST com diferentes parametros de filtro
    tentativas = [
        ("GET", {"competencia": competencia}),
        ("GET", {"competencia": competencia, "status": "todos"}),
        ("GET", {"ano": competencia}),
        ("POST", {"csrfmiddlewaretoken": csrf, "competencia": competencia}),
        ("POST", {"csrfmiddlewaretoken": csrf, "competencia": competencia, "status": ""}),
    ]

    for method, params in tentativas:
        try:
            if method == "GET":
                r = session.get(CONTRATOS_URL, params=params, timeout=30)
            else:
                r = session.post(CONTRATOS_URL, data=params,
                    headers={"Referer": CONTRATOS_URL}, timeout=30)

            if r.status_code != 200:
                continue

            # Se retornou JSON
            if _is_json(r):
                data = r.json()
                if isinstance(data, list) and data:
                    return data
                if isinstance(data, dict):
                    for key in ["results", "contratos", "data", "rows"]:
                        if key in data and isinstance(data[key], list):
                            return data[key]

            # Se retornou HTML com tabela
            html = r.text
            rows = _parsear_tabela_contratos(html)
            if rows:
                return rows

        except Exception:
            continue

    return registros


def _parsear_tabela_contratos(html):
    """Parseia tabela HTML de contratos."""
    registros = []
    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)

    for table in tables:
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table, re.DOTALL)
        if len(rows) < 2:
            continue

        # Extrair headers
        header_cells = re.findall(r'<th[^>]*>(.*?)</th>', rows[0], re.DOTALL)
        headers = [re.sub(r'<[^>]+>', '', h).strip().lower() for h in header_cells]

        if not headers:
            continue

        # Mapear indices
        idx = {}
        for i, h in enumerate(headers):
            if "matric" in h or "matr" in h:
                idx["matricula"] = i
            elif "nome" in h or "aluno" in h:
                idx["nome"] = i
            elif "turma" in h:
                idx["turma"] = i
            elif "situa" in h:
                idx["situacao"] = i
            elif "status" in h or "assinatura" in h or "contrato" in h:
                idx["status"] = i

        if "nome" not in idx:
            continue

        # Parsear linhas de dados
        for row in rows[1:]:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]

            if len(cells) <= max(idx.values(), default=0):
                continue

            nome = cells[idx["nome"]] if "nome" in idx else ""
            matricula = cells[idx.get("matricula", -1)] if "matricula" in idx else ""
            turma = cells[idx.get("turma", -1)] if "turma" in idx else ""
            situacao = cells[idx.get("situacao", -1)] if "situacao" in idx else ""
            status_raw = cells[idx.get("status", -1)] if "status" in idx else ""

            if not nome:
                continue

            # Normalizar status
            status = "Outro"
            sl = status_raw.lower()
            if "assinado" in sl:
                status = "Assinado"
            elif "cancelad" in sl:
                status = "Cancelado"
            elif "aguardando" in sl:
                status = "Aguardando"

            registros.append({
                "matricula": matricula,
                "nome": nome,
                "turma": turma,
                "situacao": situacao,
                "status_contrato": status,
            })

    return registros


# =========================================================================
# EXTRACAO DE TURMAS (para cruzar quem NAO tem contrato)
# =========================================================================

def _extrair_turmas_2026(session):
    turmas = []
    offset = 0
    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/turma/", params={
                "limit": 500, "offset": offset,
            }, timeout=30)
            if r.status_code != 200 or not _is_json(r):
                break
            data = r.json()
            for t in data.get("results", []):
                periodo = str(t.get("periodo_sigla", "")).strip()
                if "2026" in periodo:
                    turmas.append(t)
            if not data.get("next"):
                break
            offset += 500
        except Exception:
            break
    return turmas


# =========================================================================
# ORQUESTRACAO
# =========================================================================

def extrair_contratos_unidade(instituicao, login, senha, unidade):
    """Extrai contratos de uma unidade via pagina de assinatura eletronica."""
    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        raise RuntimeError(f"Login falhou ({unidade['codigo']}): {erro}")

    registros = []

    # Passo 1: Tentar API de contratos
    endpoint, dados_api = _extrair_contratos_api(session)
    if dados_api:
        for c in dados_api:
            registros.append({
                "matricula": str(c.get("matricula", c.get("numero_matricula", ""))).strip(),
                "nome": c.get("nome", c.get("nome_aluno", c.get("aluno", ""))),
                "turma": c.get("turma", c.get("nome_turma", "")),
                "situacao": c.get("situacao", c.get("situacao_turma", "")),
                "unidade": unidade["codigo"],
                "status_contrato": _normalizar_status(c),
            })
        return registros

    # Passo 2: Tentar parsear HTML da pagina de contratos
    dados_html = _parsear_contratos_html(session)
    if dados_html:
        for c in dados_html:
            c["unidade"] = unidade["codigo"]
            registros.append(c)
        return registros

    # Passo 3: Se nada funcionou, retornar vazio com info
    return registros


def _normalizar_status(contrato):
    """Normaliza status de contrato a partir de campos variados."""
    for campo in ["status", "status_contrato", "situacao_contrato",
                   "assinatura", "status_assinatura"]:
        val = str(contrato.get(campo, "")).lower()
        if "assinado" in val:
            return "Assinado"
        if "cancelad" in val:
            return "Cancelado"
        if "aguardando" in val:
            return "Aguardando"
    return "Outro"


def extrair_tudo(instituicao, login, senha, progress_cb=None):
    """Extrai contratos de todas as unidades em paralelo."""
    resultado = {"contratos": [], "turmas": [], "erros": [], "timestamp": ""}

    def _proc(u):
        try:
            session, erro = _login(instituicao, login, senha, u)
            if not session:
                raise RuntimeError(erro)

            # Extrair contratos
            contratos = []
            endpoint, dados_api = _extrair_contratos_api(session)
            if dados_api:
                for c in dados_api:
                    contratos.append({
                        "matricula": str(c.get("matricula", c.get("numero_matricula", ""))).strip(),
                        "nome": c.get("nome", c.get("nome_aluno", c.get("aluno", ""))),
                        "turma": c.get("turma", c.get("nome_turma", "")),
                        "situacao": c.get("situacao", c.get("situacao_turma", "")),
                        "unidade": u["codigo"],
                        "status_contrato": _normalizar_status(c),
                    })
            else:
                dados_html = _parsear_contratos_html(session)
                for c in dados_html:
                    c["unidade"] = u["codigo"]
                    contratos.append(c)

            # Extrair turmas 2026
            turmas = _extrair_turmas_2026(session)

            return {
                "contratos": contratos,
                "turmas": turmas,
                "msg": f"{u['nome']}: {len(contratos)} contratos, {len(turmas)} turmas",
            }
        except Exception as e:
            msg = f"{u['codigo']}: {e}"
            resultado["erros"].append(msg)
            return {"contratos": [], "turmas": [], "msg": msg}

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_proc, u): u for u in UNIDADES}
        for f in as_completed(futures):
            res = f.result()
            resultado["contratos"].extend(res["contratos"])
            resultado["turmas"].extend(res["turmas"])
            if progress_cb:
                progress_cb(res["msg"])

    resultado["timestamp"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    return resultado


# =========================================================================
# DIAGNOSTICO
# =========================================================================

def testar_conexao(instituicao, login, senha):
    """Testa conexao, busca contratos e tenta obter dados do aluno."""
    log = []
    unidade = UNIDADES[0]

    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        log.append(f"ERRO: {erro}")
        return log
    log.append("Login: OK")

    # 1. Buscar contratos
    log.append("Buscando contratos 2026...")
    try:
        r = session.get(f"{SIGA_URL}/api/v1/assinatura_eletronica/", params={
            "competencia": "2026",
        }, timeout=60, headers={"Accept": "application/json"})

        if r.status_code == 200 and _is_json(r):
            data = r.json()
            contratos = data if isinstance(data, list) else data.get("results", [])
            log.append(f"  Contratos: {len(contratos)}")

            if contratos:
                # Mostrar TODOS os campos do primeiro registro (sem truncar)
                c = contratos[0]
                log.append(f"  Todos os campos ({len(c.keys())}):")
                for k, v in c.items():
                    val_str = json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v)
                    if len(val_str) > 100:
                        val_str = val_str[:100] + "..."
                    log.append(f"    {k}: {val_str}")

                # Contar por situacao
                situacoes = {}
                for ct in contratos:
                    sit = ct.get("situacao_assinatura_label", "?")
                    situacoes[sit] = situacoes.get(sit, 0) + 1
                log.append(f"  Por situacao: {json.dumps(situacoes, ensure_ascii=False)}")

                # 2. Tentar detalhe do primeiro contrato
                cid = c.get("id")
                log.append(f"  Testando detalhe /api/v1/assinatura_eletronica/{cid}/...")
                try:
                    r2 = session.get(f"{SIGA_URL}/api/v1/assinatura_eletronica/{cid}/",
                        timeout=15, headers={"Accept": "application/json"})
                    if r2.status_code == 200 and _is_json(r2):
                        detalhe = r2.json()
                        campos_extra = set(detalhe.keys()) - set(c.keys())
                        if campos_extra:
                            log.append(f"    Campos EXTRAS no detalhe: {list(campos_extra)}")
                            for ce in campos_extra:
                                val = detalhe[ce]
                                val_str = json.dumps(val, ensure_ascii=False) if isinstance(val, (dict, list)) else str(val)
                                log.append(f"      {ce}: {val_str[:150]}")
                        else:
                            log.append(f"    Detalhe: mesmos campos da lista")
                    else:
                        log.append(f"    Detalhe: status={r2.status_code}")
                except Exception as e:
                    log.append(f"    Detalhe: ERRO ({e})")

                # 3. Tentar endpoint de signatarios
                for sub in ["signatarios", "assinantes", "participantes", "aluno"]:
                    try:
                        r3 = session.get(f"{SIGA_URL}/api/v1/assinatura_eletronica/{cid}/{sub}/",
                            timeout=10, headers={"Accept": "application/json"})
                        if r3.status_code == 200 and _is_json(r3):
                            sig_data = r3.json()
                            log.append(f"    Sub-endpoint /{sub}/: OK")
                            if isinstance(sig_data, list) and sig_data:
                                log.append(f"      Campos: {list(sig_data[0].keys())}")
                                log.append(f"      Amostra: {json.dumps(sig_data[0], ensure_ascii=False)[:200]}")
                            elif isinstance(sig_data, dict):
                                log.append(f"      Keys: {list(sig_data.keys())}")
                    except Exception:
                        pass
        else:
            log.append(f"  Status: {r.status_code}")
    except Exception as e:
        log.append(f"  ERRO: {e}")

    # Turmas
    turmas = _extrair_turmas_2026(session)
    log.append(f"Turmas 2026: {len(turmas)}")

    return log
