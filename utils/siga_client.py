"""
Cliente SIGA - Extrai dados de contratos e alunos do sistema Activesoft.

Tenta primeiro a API REST (/api/v1/contratos/), e como fallback usa o
relatorio web de contratos via scraping.
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


# =========================================================================
# EXTRACAO VIA API
# =========================================================================

def _extrair_contratos_api(session, unidade):
    """Tenta extrair contratos via /api/v1/contratos/."""
    registros = []
    offset = 0

    while True:
        params = {
            "limit": 500, "offset": offset,
            "periodo": "2026",
        }
        try:
            r = session.get(f"{SIGA_URL}/api/v1/contratos/", params=params, timeout=90)
            if r.status_code != 200:
                return None  # API nao disponivel, usar fallback
            data = r.json()
        except Exception:
            return None

        for c in data.get("results", []):
            status_raw = str(c.get("status", "") or c.get("situacao_contrato", "")).lower()
            if "assinado" in status_raw:
                status = "Assinado"
            elif "cancelad" in status_raw:
                status = "Cancelado"
            elif "aguardando" in status_raw:
                status = "Aguardando"
            else:
                status = "Outro"

            registros.append({
                "matricula": _normalizar_matricula(c.get("matricula", "")),
                "nome": c.get("nome_aluno", c.get("nome", "")),
                "turma": c.get("turma", c.get("nome_turma", "")),
                "situacao": c.get("situacao_turma", c.get("situacao", "")),
                "unidade": unidade["codigo"],
                "status_contrato": status,
            })

        if not data.get("next"):
            break
        offset += 500

    return registros


# =========================================================================
# EXTRACAO VIA RELATORIO WEB (FALLBACK)
# =========================================================================

def _extrair_turmas_2026(session):
    """Busca IDs das turmas de 2026."""
    turmas = []
    offset = 0
    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/turma/", params={
                "limit": 500, "offset": offset,
            }, timeout=30)
            if r.status_code != 200:
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


def _extrair_alunos_turma(session, turma_id):
    """Extrai alunos de uma turma especifica via API."""
    alunos = []
    offset = 0
    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/aluno/", params={
                "turma": turma_id, "limit": 500, "offset": offset,
            }, timeout=60)
            if r.status_code != 200:
                break
            data = r.json()
            for a in data.get("results", []):
                alunos.append({
                    "matricula": _normalizar_matricula(a.get("matricula", "")),
                    "nome": a.get("nome", a.get("nome_aluno", "")),
                    "situacao": a.get("situacao", a.get("situacao_turma", "")),
                })
            if not data.get("next"):
                break
            offset += 500
        except Exception:
            break
    return alunos


def _extrair_relatorio_contratos(session, unidade):
    """Tenta extrair via relatorio web de contratos."""
    registros = []

    # Buscar turmas 2026
    turmas = _extrair_turmas_2026(session)
    if not turmas:
        return registros

    # Para cada turma, buscar alunos
    for turma in turmas:
        turma_nome = turma.get("nome", turma.get("turma_nome", ""))
        turma_id = turma.get("id")
        alunos = _extrair_alunos_turma(session, turma_id)

        for aluno in alunos:
            registros.append({
                "matricula": aluno["matricula"],
                "nome": aluno["nome"],
                "turma": turma_nome,
                "situacao": aluno["situacao"],
                "unidade": unidade["codigo"],
                "status_contrato": "Sem Contrato",  # Default, sera cruzado depois
            })

    return registros


# =========================================================================
# ORQUESTRACAO
# =========================================================================

def extrair_contratos_unidade(instituicao, login, senha, unidade):
    """Extrai contratos de uma unidade, tentando API primeiro."""
    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        raise RuntimeError(f"Login falhou ({unidade['codigo']}): {erro}")

    # Tentar API de contratos primeiro
    registros = _extrair_contratos_api(session, unidade)

    if registros is None:
        # Fallback: relatorio web + alunos
        registros = _extrair_relatorio_contratos(session, unidade)

    return registros


def extrair_tudo(instituicao, login, senha, progress_cb=None):
    """Extrai contratos de todas as unidades em paralelo."""
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
    """Testa conexao e retorna diagnostico."""
    log = []
    unidade = UNIDADES[0]

    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        log.append(f"ERRO: {erro}")
        return log
    log.append("Login: OK")

    # Testar API de contratos
    try:
        r = session.get(f"{SIGA_URL}/api/v1/contratos/", params={
            "limit": 1, "offset": 0, "periodo": "2026",
        }, timeout=30)
        if r.status_code == 200:
            data = r.json()
            log.append(f"API contratos: count={data.get('count', '?')}")
        else:
            log.append(f"API contratos: status={r.status_code} (nao disponivel)")
    except Exception as e:
        log.append(f"API contratos: ERRO ({e})")

    # Testar API de turmas
    try:
        r = session.get(f"{SIGA_URL}/api/v1/turma/", params={
            "limit": 5, "offset": 0,
        }, timeout=30)
        if r.status_code == 200:
            data = r.json()
            log.append(f"API turmas: count={data.get('count', '?')}")
        else:
            log.append(f"API turmas: status={r.status_code}")
    except Exception as e:
        log.append(f"API turmas: ERRO ({e})")

    # Testar API de alunos
    try:
        r = session.get(f"{SIGA_URL}/api/v1/aluno/", params={
            "limit": 1, "offset": 0,
        }, timeout=30)
        if r.status_code == 200:
            data = r.json()
            log.append(f"API alunos: count={data.get('count', '?')}")
        else:
            log.append(f"API alunos: status={r.status_code}")
    except Exception as e:
        log.append(f"API alunos: ERRO ({e})")

    return log
