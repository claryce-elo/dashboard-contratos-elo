"""
Cliente SIGA - Extrai dados de contratos e alunos do sistema Activesoft.

Usa a API /api/v1/turma/ (confirmada) para buscar turmas 2026 e seus alunos.
Descobre dinamicamente os endpoints disponiveis para dados de alunos e contratos.
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


def _normalizar_matricula(mat):
    mat = str(mat).strip().replace(" ", "")
    if mat.startswith("1-"):
        mat = mat[2:]
    return mat


def _is_json(response):
    """Verifica se a resposta e JSON valido."""
    ct = response.headers.get("content-type", "")
    if "application/json" in ct:
        return True
    try:
        response.json()
        return True
    except Exception:
        return False


# =========================================================================
# DESCOBERTA DE ENDPOINTS
# =========================================================================

def _descobrir_endpoints(session):
    """Testa endpoints conhecidos do SIGA e retorna os que funcionam."""
    endpoints = {}

    # Endpoints candidatos para alunos/contratos
    candidatos = [
        ("turma", "/api/v1/turma/"),
        ("turma_detalhe", "/api/v1/turma/{id}/"),
        ("contratos", "/api/v1/contratos/"),
        ("contrato", "/api/v1/contrato/"),
        ("aluno", "/api/v1/aluno/"),
        ("alunos", "/api/v1/alunos/"),
        ("matricula", "/api/v1/matricula/"),
        ("matriculas", "/api/v1/matriculas/"),
        ("titulos", "/api/v1/titulos/"),
    ]

    for nome, path in candidatos:
        if "{id}" in path:
            continue  # Testar depois com ID real
        try:
            r = session.get(f"{SIGA_URL}{path}", params={"limit": 1, "offset": 0}, timeout=15)
            if r.status_code == 200 and _is_json(r):
                data = r.json()
                count = data.get("count", len(data.get("results", [])))
                endpoints[nome] = {"path": path, "count": count}
        except Exception:
            pass

    return endpoints


# =========================================================================
# EXTRACAO VIA API DE TURMAS
# =========================================================================

def _extrair_turmas_2026(session):
    """Busca turmas de 2026 via API."""
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


def _extrair_alunos_via_turma_detalhe(session, turma_id):
    """Tenta buscar alunos no detalhe da turma /api/v1/turma/{id}/."""
    try:
        r = session.get(f"{SIGA_URL}/api/v1/turma/{turma_id}/", timeout=30)
        if r.status_code != 200 or not _is_json(r):
            return None
        data = r.json()

        # Procurar lista de alunos em campos conhecidos
        for campo in ["alunos", "matriculas", "alunos_lista", "estudantes", "alunos_ativos_lista"]:
            if campo in data and isinstance(data[campo], list):
                return data[campo]

        return None
    except Exception:
        return None


def _extrair_alunos_via_titulos(session, turma_id):
    """Busca alunos via /api/v1/titulos/ filtrando por turma (endpoint confirmado no dashboard-livros)."""
    alunos = {}
    offset = 0

    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/titulos/", params={
                "limit": 500, "offset": offset,
                "turma": turma_id,
            }, timeout=60)
            if r.status_code != 200 or not _is_json(r):
                return None
            data = r.json()

            for titulo in data.get("results", []):
                mat = _normalizar_matricula(titulo.get("matricula", ""))
                if mat and mat not in alunos:
                    alunos[mat] = {
                        "matricula": mat,
                        "nome": titulo.get("nome_aluno", ""),
                        "situacao": titulo.get("situacao", ""),
                    }

            if not data.get("next"):
                break
            offset += 500
        except Exception:
            return None

    return list(alunos.values()) if alunos else None


def _extrair_relatorio_web_alunos(session, turma_ids):
    """Tenta extrair via relatorio web de alunos por turma."""
    registros = []

    url_rel = f"{SIGA_URL}/relatorio_web/aluno_turma/relacao_alunos_com_situacao_na_turma/"
    csrf = session.cookies.get("csrftoken", "")

    for turma_id in turma_ids:
        try:
            r = session.post(url_rel, data={
                "csrfmiddlewaretoken": csrf,
                "turma": [turma_id],
            }, headers={
                "Referer": f"{SIGA_URL}/relatorio_web/aluno_turma/",
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=30)

            if r.status_code == 200 and len(r.text) > 200:
                # Tentar parsear HTML com tabela de alunos
                alunos = _parsear_html_alunos(r.text)
                if alunos:
                    registros.extend(alunos)
        except Exception:
            continue

    return registros


def _parsear_html_alunos(html):
    """Parseia HTML do relatorio web para extrair alunos."""
    alunos = []
    # Procurar linhas de tabela com dados de alunos
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(cells) >= 3:
            # Limpar HTML tags
            cells = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
            # Tentar identificar matricula (numerico) e nome
            matricula = ""
            nome = ""
            situacao = ""
            for c in cells:
                if re.match(r'^\d[\d-]+$', c) and not matricula:
                    matricula = c
                elif len(c) > 3 and not c.isdigit() and not nome:
                    nome = c
                elif c in ("Cursando", "Cancelado", "Transferido", "Trancado"):
                    situacao = c
            if matricula and nome:
                alunos.append({
                    "matricula": _normalizar_matricula(matricula),
                    "nome": nome,
                    "situacao": situacao,
                })
    return alunos


# =========================================================================
# ORQUESTRACAO
# =========================================================================

def extrair_contratos_unidade(instituicao, login, senha, unidade):
    """Extrai alunos de uma unidade usando a API de turmas."""
    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        raise RuntimeError(f"Login falhou ({unidade['codigo']}): {erro}")

    # Buscar turmas 2026
    turmas = _extrair_turmas_2026(session)
    if not turmas:
        raise RuntimeError(f"Nenhuma turma 2026 encontrada para {unidade['codigo']}")

    registros = []

    # Estrategia 1: Detalhe da turma pode incluir alunos
    primeira_turma = turmas[0]
    alunos_teste = _extrair_alunos_via_turma_detalhe(session, primeira_turma["id"])

    if alunos_teste is not None:
        # Detalhe da turma funciona! Usar para todas
        for turma in turmas:
            turma_nome = turma.get("nome", "")
            alunos = _extrair_alunos_via_turma_detalhe(session, turma["id"])
            if not alunos:
                continue
            for a in alunos:
                mat = _normalizar_matricula(
                    a.get("matricula", a.get("numero_matricula", ""))
                )
                nome = a.get("nome", a.get("nome_aluno", ""))
                if not mat or not nome:
                    continue
                situacao = a.get("situacao", a.get("situacao_turma", ""))
                status = a.get("status_contrato", a.get("contrato_status", ""))

                registros.append({
                    "matricula": mat,
                    "nome": nome,
                    "turma": turma_nome,
                    "situacao": situacao,
                    "unidade": unidade["codigo"],
                    "status_contrato": status if status else "Sem Contrato",
                })
    else:
        # Estrategia 2: Usar dados basicos da turma (alunos_ativos, etc.)
        # A API de turmas ja tem contagem - vamos extrair o que tiver
        for turma in turmas:
            turma_nome = turma.get("nome", "")
            n_ativos = turma.get("alunos_ativos", 0) or 0

            # Tentar pegar alunos via campo da propria turma
            for campo in ["alunos", "matriculas", "alunos_lista"]:
                if campo in turma and isinstance(turma[campo], list):
                    for a in turma[campo]:
                        if isinstance(a, dict):
                            mat = _normalizar_matricula(a.get("matricula", ""))
                            nome = a.get("nome", a.get("nome_aluno", ""))
                        else:
                            continue
                        if not mat or not nome:
                            continue
                        registros.append({
                            "matricula": mat,
                            "nome": nome,
                            "turma": turma_nome,
                            "situacao": "",
                            "unidade": unidade["codigo"],
                            "status_contrato": "Sem Contrato",
                        })

    return registros


def extrair_tudo(instituicao, login, senha, progress_cb=None):
    """Extrai dados de todas as unidades em paralelo."""
    resultado = {"alunos": [], "erros": [], "timestamp": ""}

    def _proc(u):
        try:
            regs = extrair_contratos_unidade(instituicao, login, senha, u)
            return {"regs": regs, "msg": f"{u['nome']}: {len(regs)} registros"}
        except Exception as e:
            msg = f"{u['codigo']}: {e}"
            resultado["erros"].append(msg)
            return {"regs": [], "msg": msg}

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_proc, u): u for u in UNIDADES}
        for f in as_completed(futures):
            res = f.result()
            resultado["alunos"].extend(res["regs"])
            if progress_cb:
                progress_cb(res["msg"])

    resultado["timestamp"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    return resultado


def testar_conexao(instituicao, login, senha):
    """Testa conexao e retorna diagnostico detalhado."""
    log = []
    unidade = UNIDADES[0]

    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        log.append(f"ERRO: {erro}")
        return log
    log.append("Login: OK")

    # Descobrir endpoints
    log.append("Descobrindo endpoints...")
    endpoints = _descobrir_endpoints(session)
    for nome, info in endpoints.items():
        log.append(f"  /api/v1/{nome}/: OK (count={info['count']})")

    if not endpoints:
        log.append("  Nenhum endpoint encontrado!")

    # Buscar turmas 2026
    turmas_2026 = _extrair_turmas_2026(session)
    log.append(f"Turmas 2026: {len(turmas_2026)} encontradas")

    if turmas_2026:
        # Testar detalhe da primeira turma
        t = turmas_2026[0]
        log.append(f"Testando detalhe turma: {t.get('nome', t.get('id'))}...")

        try:
            r = session.get(f"{SIGA_URL}/api/v1/turma/{t['id']}/", timeout=15)
            if r.status_code == 200 and _is_json(r):
                data = r.json()
                campos = list(data.keys())
                log.append(f"  Campos: {', '.join(campos[:15])}")

                # Verificar se tem lista de alunos
                for campo in campos:
                    val = data[campo]
                    if isinstance(val, list) and len(val) > 0:
                        if isinstance(val[0], dict):
                            log.append(f"  Lista '{campo}': {len(val)} items, campos={list(val[0].keys())[:8]}")
                        else:
                            log.append(f"  Lista '{campo}': {len(val)} items (tipo: {type(val[0]).__name__})")
                    elif isinstance(val, (int, float)):
                        log.append(f"  {campo}: {val}")
            else:
                log.append(f"  Detalhe turma: status={r.status_code}")
        except Exception as e:
            log.append(f"  Detalhe turma: ERRO ({e})")

        # Mostrar amostra de campos da turma da lista
        campos_turma = list(t.keys())
        log.append(f"Campos turma (lista): {', '.join(campos_turma[:15])}")

    return log
