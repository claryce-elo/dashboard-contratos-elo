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
    """Acessa a pagina de contratos e descobre como buscar dados."""
    info = {"url": CONTRATOS_URL, "status": None, "tipo": None, "dados": None, "erro": None}

    try:
        r = session.get(CONTRATOS_URL, timeout=30)
        info["status"] = r.status_code

        if r.status_code != 200:
            info["erro"] = f"Status {r.status_code}"
            return info

        ct = r.headers.get("content-type", "")
        info["tipo"] = ct

        # Se retornou JSON direto
        if "application/json" in ct:
            info["dados"] = r.json()
            return info

        # Se retornou HTML, analisar a pagina
        html = r.text
        info["html_size"] = len(html)

        # Procurar endpoints de API nos scripts
        api_urls = set()
        for pattern in [
            r'["\'](/api/v1/[^"\']+)["\']',
            r'["\'](/assinatura_eletronica/[^"\']+)["\']',
            r'url\s*[=:]\s*["\']([^"\']+contratos?[^"\']*)["\']',
            r'fetch\s*\(\s*["\']([^"\']+)["\']',
            r'ajax\s*\(\s*[{]?\s*url\s*:\s*["\']([^"\']+)["\']',
            r'\.get\s*\(\s*["\']([^"\']+)["\']',
            r'\.post\s*\(\s*["\']([^"\']+)["\']',
        ]:
            matches = re.findall(pattern, html)
            api_urls.update(matches)

        info["api_urls"] = list(api_urls)

        # Procurar formularios de filtro
        forms = re.findall(r'<form[^>]*>(.*?)</form>', html, re.DOTALL)
        selects = re.findall(r'<select[^>]*name=["\']([^"\']+)["\'][^>]*>', html)
        inputs = re.findall(r'<input[^>]*name=["\']([^"\']+)["\'][^>]*>', html)
        info["form_fields"] = {"selects": selects, "inputs": inputs}

        # Procurar dados inline (JSON em script tags)
        scripts = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
        for script in scripts:
            # Dados de contratos em variaveis JS
            for var_pattern in [
                r'(?:var|let|const)\s+\w*contrat\w*\s*=\s*(\[.*?\]);',
                r'(?:var|let|const)\s+\w*dados?\w*\s*=\s*(\[.*?\]);',
                r'data\s*:\s*(\[{.*?}\])',
            ]:
                m = re.search(var_pattern, script, re.DOTALL | re.IGNORECASE)
                if m:
                    try:
                        info["dados_inline"] = json.loads(m.group(1))[:3]
                    except Exception:
                        info["dados_inline_raw"] = m.group(1)[:300]

        # Procurar tabela HTML com dados de contratos
        tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
        if tables:
            info["tabelas"] = len(tables)
            # Tentar parsear primeira tabela
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tables[0], re.DOTALL)
            if rows:
                info["linhas_tabela"] = len(rows)
                # Headers
                headers = re.findall(r'<th[^>]*>(.*?)</th>', rows[0], re.DOTALL)
                headers = [re.sub(r'<[^>]+>', '', h).strip() for h in headers]
                info["headers_tabela"] = headers

    except Exception as e:
        info["erro"] = str(e)

    return info


def _extrair_contratos_api(session, competencia="2026"):
    """Tenta extrair contratos via API descoberta."""
    registros = []

    # Tentar endpoints comuns para contratos
    endpoints_candidatos = [
        f"/assinatura_eletronica/contratos/api/",
        f"/assinatura_eletronica/contratos/lista/",
        f"/assinatura_eletronica/contratos/json/",
        f"/assinatura_eletronica/api/contratos/",
        f"/api/v1/assinatura_eletronica/contratos/",
        f"/api/v1/contrato_assinatura/",
    ]

    for endpoint in endpoints_candidatos:
        try:
            # Tentar GET com filtro
            r = session.get(f"{SIGA_URL}{endpoint}", params={
                "competencia": competencia,
                "limit": 500, "offset": 0,
            }, timeout=30)

            if r.status_code == 200 and _is_json(r):
                data = r.json()
                if isinstance(data, list):
                    return endpoint, data
                elif isinstance(data, dict) and "results" in data:
                    return endpoint, data["results"]
        except Exception:
            pass

        try:
            # Tentar POST com filtro
            csrf = session.cookies.get("csrftoken", "")
            r = session.post(f"{SIGA_URL}{endpoint}", data={
                "csrfmiddlewaretoken": csrf,
                "competencia": competencia,
            }, headers={
                "Referer": CONTRATOS_URL,
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=30)

            if r.status_code == 200 and _is_json(r):
                data = r.json()
                if isinstance(data, list):
                    return endpoint, data
                elif isinstance(data, dict) and "results" in data:
                    return endpoint, data["results"]
        except Exception:
            pass

    return None, []


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
    """Testa conexao e explora pagina de contratos."""
    log = []
    unidade = UNIDADES[0]

    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        log.append(f"ERRO: {erro}")
        return log
    log.append("Login: OK")

    # Explorar pagina de contratos
    log.append(f"Acessando {CONTRATOS_URL}...")
    info = _explorar_pagina_contratos(session)
    log.append(f"  Status: {info.get('status')}")
    log.append(f"  Tipo: {info.get('tipo', 'N/A')}")

    if info.get("erro"):
        log.append(f"  ERRO: {info['erro']}")

    if info.get("html_size"):
        log.append(f"  HTML: {info['html_size']} bytes")

    if info.get("api_urls"):
        log.append(f"  URLs encontradas:")
        for url in info["api_urls"][:10]:
            log.append(f"    {url}")

    if info.get("form_fields"):
        ff = info["form_fields"]
        if ff.get("selects"):
            log.append(f"  Selects: {ff['selects']}")
        if ff.get("inputs"):
            log.append(f"  Inputs: {ff['inputs']}")

    if info.get("tabelas"):
        log.append(f"  Tabelas HTML: {info['tabelas']}")
        if info.get("linhas_tabela"):
            log.append(f"  Linhas: {info['linhas_tabela']}")
        if info.get("headers_tabela"):
            log.append(f"  Headers: {info['headers_tabela']}")

    if info.get("dados"):
        log.append(f"  JSON direto: {json.dumps(info['dados'], ensure_ascii=False)[:200]}")

    if info.get("dados_inline"):
        log.append(f"  Dados inline: {json.dumps(info['dados_inline'], ensure_ascii=False)[:200]}")
    elif info.get("dados_inline_raw"):
        log.append(f"  Dados inline (raw): {info['dados_inline_raw']}")

    # Tentar API de contratos
    log.append("Tentando endpoints de API...")
    endpoint, dados = _extrair_contratos_api(session)
    if endpoint:
        log.append(f"  Endpoint encontrado: {endpoint}")
        log.append(f"  Registros: {len(dados)}")
        if dados:
            log.append(f"  Campos: {list(dados[0].keys())}")
            log.append(f"  Amostra: {json.dumps(dados[0], ensure_ascii=False)[:200]}")
    else:
        log.append("  Nenhum endpoint API funcionou")

    # Tentar HTML
    log.append("Tentando parsear HTML...")
    dados_html = _parsear_contratos_html(session)
    if dados_html:
        log.append(f"  Contratos parseados: {len(dados_html)}")
        if dados_html:
            log.append(f"  Amostra: {json.dumps(dados_html[0], ensure_ascii=False)[:200]}")
    else:
        log.append("  Nenhum contrato encontrado no HTML")

    # Turmas
    turmas = _extrair_turmas_2026(session)
    log.append(f"Turmas 2026: {len(turmas)}")

    return log
