"""
Cliente SIGA - Extrai dados de contratos e alunos do sistema Activesoft.

Endpoints confirmados:
  /api/v1/turma/  (1394 turmas, 94 de 2026)
  /api/v1/alunos/ (7637 alunos)
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
    ct = response.headers.get("content-type", "")
    if "application/json" in ct:
        return True
    try:
        response.json()
        return True
    except Exception:
        return False


# =========================================================================
# EXTRACAO DE TURMAS
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


def _build_turma_map(turmas):
    """Cria mapa de turma_id -> info da turma."""
    m = {}
    for t in turmas:
        tid = t.get("id")
        if tid:
            m[tid] = {
                "nome": t.get("nome", t.get("nome_turma_completo", "")),
                "serie_nome": t.get("serie_nome", ""),
                "curso_nome": t.get("curso_nome", ""),
                "turno": t.get("turno", ""),
            }
    return m


# =========================================================================
# EXTRACAO DE ALUNOS
# =========================================================================

def _extrair_alunos_api(session):
    """Extrai todos os alunos via /api/v1/alunos/ (confirmado: 7637 registros)."""
    alunos = []
    offset = 0

    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/alunos/", params={
                "limit": 500, "offset": offset,
            }, timeout=90)
            if r.status_code != 200 or not _is_json(r):
                break
            data = r.json()

            results = data.get("results", [])
            if not results:
                break

            alunos.extend(results)

            if not data.get("next"):
                break
            offset += 500
        except Exception:
            break

    return alunos


def _normalizar_status_contrato(aluno):
    """Tenta extrair status de contrato de campos do aluno."""
    # Campos possiveis que podem conter info de contrato
    for campo in ["status_contrato", "contrato_status", "contrato",
                   "situacao_contrato", "assinatura_contrato", "contrato_assinado"]:
        val = aluno.get(campo, "")
        if val:
            val_lower = str(val).lower()
            if "assinado" in val_lower:
                return "Assinado"
            if "cancelad" in val_lower:
                return "Cancelado"
            if "aguardando" in val_lower:
                return "Aguardando"
            if val_lower not in ("", "none", "null"):
                return str(val)
    return "Sem Contrato"


def _extrair_turma_do_aluno(aluno):
    """Extrai nome da turma a partir dos dados do aluno."""
    # Tentar campos diretos
    for campo in ["turma", "nome_turma", "turma_nome", "turma_atual"]:
        val = aluno.get(campo, "")
        if val and str(val).strip():
            return str(val).strip()

    # Tentar turmas_vinculadas (formato do dashboard-livros)
    turmas = aluno.get("turmas_vinculadas", aluno.get("turmas", []))
    if isinstance(turmas, list) and turmas:
        for tv in turmas:
            tv_str = str(tv)
            if "2026" in tv_str:
                return tv_str
        return str(turmas[0])

    return ""


def _extrair_situacao(aluno):
    """Extrai situacao do aluno na turma."""
    for campo in ["situacao", "situacao_turma", "situacao_na_turma", "status"]:
        val = aluno.get(campo, "")
        if val and str(val).strip():
            return str(val).strip()
    return ""


def _extrair_nome(aluno):
    """Extrai nome do aluno."""
    for campo in ["nome", "nome_aluno", "nome_completo"]:
        val = aluno.get(campo, "")
        if val and str(val).strip():
            return str(val).strip()
    return ""


def _extrair_matricula(aluno):
    """Extrai matricula do aluno."""
    for campo in ["matricula", "numero_matricula", "codigo_matricula", "id"]:
        val = aluno.get(campo, "")
        if val:
            return _normalizar_matricula(str(val))
    return ""


# =========================================================================
# ORQUESTRACAO
# =========================================================================

def extrair_contratos_unidade(instituicao, login, senha, unidade):
    """Extrai alunos de uma unidade via API /api/v1/alunos/."""
    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        raise RuntimeError(f"Login falhou ({unidade['codigo']}): {erro}")

    # Buscar turmas 2026 para referencia
    turmas_2026 = _extrair_turmas_2026(session)
    turma_map = _build_turma_map(turmas_2026)
    turma_ids_2026 = set(turma_map.keys())

    # Buscar todos os alunos
    alunos_raw = _extrair_alunos_api(session)

    registros = []
    for aluno in alunos_raw:
        nome = _extrair_nome(aluno)
        matricula = _extrair_matricula(aluno)
        if not nome or not matricula:
            continue

        # Filtrar aluno teste
        if re.search(r"aluno\s*teste", nome, re.IGNORECASE):
            continue

        turma_str = _extrair_turma_do_aluno(aluno)
        situacao = _extrair_situacao(aluno)
        status = _normalizar_status_contrato(aluno)

        # Tentar associar a turma 2026
        turma_id = aluno.get("turma_id", aluno.get("turma", ""))
        if isinstance(turma_id, int) and turma_id in turma_ids_2026:
            info = turma_map[turma_id]
            turma_str = info["nome"] or turma_str

        registros.append({
            "matricula": matricula,
            "nome": nome,
            "turma": turma_str,
            "situacao": situacao,
            "unidade": unidade["codigo"],
            "status_contrato": status,
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


# =========================================================================
# DIAGNOSTICO
# =========================================================================

def _descobrir_endpoints(session):
    """Testa endpoints conhecidos do SIGA."""
    endpoints = {}
    candidatos = [
        ("turma", "/api/v1/turma/"),
        ("alunos", "/api/v1/alunos/"),
        ("contratos", "/api/v1/contratos/"),
        ("contrato", "/api/v1/contrato/"),
        ("titulos", "/api/v1/titulos/"),
        ("matriculas", "/api/v1/matriculas/"),
    ]
    for nome, path in candidatos:
        try:
            r = session.get(f"{SIGA_URL}{path}", params={"limit": 1, "offset": 0}, timeout=15)
            if r.status_code == 200 and _is_json(r):
                data = r.json()
                count = data.get("count", len(data.get("results", [])))
                endpoints[nome] = {"path": path, "count": count}
        except Exception:
            pass
    return endpoints


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

    # Turmas 2026
    turmas_2026 = _extrair_turmas_2026(session)
    log.append(f"Turmas 2026: {len(turmas_2026)} encontradas")

    # Amostra de campos da turma
    if turmas_2026:
        campos_turma = list(turmas_2026[0].keys())
        log.append(f"Campos turma (lista): {', '.join(campos_turma[:15])}")

    # Amostra de alunos - CAMPOS DISPONIVEIS
    log.append("Testando /api/v1/alunos/...")
    try:
        r = session.get(f"{SIGA_URL}/api/v1/alunos/", params={
            "limit": 2, "offset": 0,
        }, timeout=30)
        if r.status_code == 200 and _is_json(r):
            data = r.json()
            results = data.get("results", [])
            log.append(f"  Total alunos: {data.get('count', '?')}")
            if results:
                campos = list(results[0].keys())
                log.append(f"  Campos aluno: {', '.join(campos)}")
                # Mostrar amostra do primeiro aluno (sem dados sensiveis)
                amostra = results[0]
                for campo in campos:
                    val = amostra.get(campo)
                    if isinstance(val, (list, dict)):
                        log.append(f"    {campo}: {json.dumps(val, ensure_ascii=False)[:120]}")
                    elif val is not None and str(val).strip():
                        # Mascarar nome por privacidade
                        if "nome" in campo.lower():
                            log.append(f"    {campo}: (presente)")
                        else:
                            log.append(f"    {campo}: {str(val)[:80]}")
        else:
            log.append(f"  /api/v1/alunos/: status={r.status_code}")
    except Exception as e:
        log.append(f"  /api/v1/alunos/: ERRO ({e})")

    return log
