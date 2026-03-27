"""
Cliente SIGA - Extrai dados de contratos e alunos do sistema Activesoft.

Endpoints confirmados:
  /api/v1/turma/  (1394 turmas, 94 de 2026) - campos: id, nome, alunos_ativos, etc
  /api/v1/alunos/ (7637) - campos: id, nome, matricula, responsavel, data_nascimento, ativo

Estrategia: usar detalhe da turma /api/v1/turma/{id}/ para obter alunos por turma,
ja que /api/v1/alunos/ nao tem info de turma.
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
# EXTRACAO DE TURMAS 2026
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


# =========================================================================
# EXTRACAO DE ALUNOS POR TURMA
# =========================================================================

def _extrair_turma_detalhe(session, turma_id):
    """Busca detalhe completo de uma turma /api/v1/turma/{id}/."""
    try:
        r = session.get(f"{SIGA_URL}/api/v1/turma/{turma_id}/", timeout=30)
        if r.status_code == 200 and _is_json(r):
            return r.json()
    except Exception:
        pass
    return None


def _extrair_alunos_filtrados(session, turma_id):
    """Tenta /api/v1/alunos/?turma={id} para filtrar por turma."""
    alunos = []
    offset = 0
    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/alunos/", params={
                "limit": 500, "offset": offset, "turma": turma_id,
            }, timeout=60)
            if r.status_code != 200 or not _is_json(r):
                return None
            data = r.json()
            results = data.get("results", [])
            alunos.extend(results)
            if not data.get("next"):
                break
            offset += 500
        except Exception:
            return None
    return alunos if alunos else None


def _encontrar_alunos_em_detalhe(detalhe):
    """Procura lista de alunos dentro do detalhe da turma."""
    if not detalhe:
        return None

    # Procurar em campos que possam conter lista de alunos
    for campo in ["alunos", "alunos_lista", "matriculas", "matriculas_lista",
                   "alunos_ativos_lista", "estudantes", "alunos_por_situacao_na_turma"]:
        val = detalhe.get(campo)
        if isinstance(val, list) and len(val) > 0:
            # Verificar se sao dicts com dados de aluno
            if isinstance(val[0], dict):
                return val
        elif isinstance(val, dict):
            # Pode ser dict agrupado por situacao: {"Cursando": [...], ...}
            all_alunos = []
            for situacao, lista in val.items():
                if isinstance(lista, list):
                    for item in lista:
                        if isinstance(item, dict):
                            item["_situacao_turma"] = situacao
                            all_alunos.append(item)
                        elif isinstance(item, (int, str)):
                            all_alunos.append({
                                "id": item,
                                "_situacao_turma": situacao,
                            })
            if all_alunos:
                return all_alunos
    return None


def _extrair_info_aluno(aluno_data, alunos_base=None):
    """Extrai nome e matricula de um registro de aluno (detalhe ou API)."""
    nome = ""
    matricula = ""
    situacao = ""

    for campo in ["nome", "nome_aluno", "nome_completo", "aluno_nome"]:
        val = aluno_data.get(campo, "")
        if val and str(val).strip():
            nome = str(val).strip()
            break

    for campo in ["matricula", "numero_matricula", "codigo_matricula"]:
        val = aluno_data.get(campo, "")
        if val:
            matricula = _normalizar_matricula(str(val))
            break

    # Se nao tem nome, mas tem id, buscar no mapa de alunos base
    if not nome and alunos_base:
        aluno_id = aluno_data.get("id", aluno_data.get("aluno", aluno_data.get("aluno_id")))
        if aluno_id and aluno_id in alunos_base:
            info = alunos_base[aluno_id]
            nome = info.get("nome", "")
            if not matricula:
                matricula = info.get("matricula", "")

    situacao = aluno_data.get("_situacao_turma",
               aluno_data.get("situacao",
               aluno_data.get("situacao_turma",
               aluno_data.get("situacao_na_turma", ""))))

    return nome, matricula, str(situacao)


# =========================================================================
# MAPA BASE DE ALUNOS (para enriquecer dados)
# =========================================================================

def _carregar_alunos_base(session):
    """Carrega mapa id -> {nome, matricula} de /api/v1/alunos/."""
    mapa = {}
    offset = 0
    while True:
        try:
            r = session.get(f"{SIGA_URL}/api/v1/alunos/", params={
                "limit": 500, "offset": offset,
            }, timeout=90)
            if r.status_code != 200 or not _is_json(r):
                break
            data = r.json()
            for a in data.get("results", []):
                aid = a.get("id")
                if aid:
                    mapa[aid] = {
                        "nome": a.get("nome", ""),
                        "matricula": _normalizar_matricula(str(a.get("matricula", ""))),
                        "ativo": a.get("ativo", True),
                    }
            if not data.get("next"):
                break
            offset += 500
        except Exception:
            break
    return mapa


# =========================================================================
# ORQUESTRACAO
# =========================================================================

def extrair_contratos_unidade(instituicao, login, senha, unidade):
    """Extrai alunos por turma de uma unidade."""
    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        raise RuntimeError(f"Login falhou ({unidade['codigo']}): {erro}")

    # Buscar turmas 2026
    turmas = _extrair_turmas_2026(session)
    if not turmas:
        raise RuntimeError(f"Nenhuma turma 2026 encontrada para {unidade['codigo']}")

    # Carregar mapa base de alunos para enriquecer
    alunos_base = _carregar_alunos_base(session)

    registros = []
    metodo = None

    for turma in turmas:
        turma_id = turma.get("id")
        turma_nome = turma.get("nome", "")

        alunos_turma = None

        # Metodo 1: Detalhe da turma
        if metodo is None or metodo == "detalhe":
            detalhe = _extrair_turma_detalhe(session, turma_id)
            alunos_turma = _encontrar_alunos_em_detalhe(detalhe)
            if alunos_turma is not None:
                metodo = "detalhe"

        # Metodo 2: Filtrar /api/v1/alunos/?turma=X
        if alunos_turma is None and (metodo is None or metodo == "filtro"):
            alunos_turma = _extrair_alunos_filtrados(session, turma_id)
            if alunos_turma is not None:
                metodo = "filtro"

        if not alunos_turma:
            continue

        for aluno_data in alunos_turma:
            nome, matricula, situacao = _extrair_info_aluno(aluno_data, alunos_base)

            if not nome or not matricula:
                continue
            if re.search(r"aluno\s*teste", nome, re.IGNORECASE):
                continue

            registros.append({
                "matricula": matricula,
                "nome": nome,
                "turma": turma_nome,
                "situacao": situacao,
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


# =========================================================================
# DIAGNOSTICO
# =========================================================================

def _descobrir_endpoints(session):
    endpoints = {}
    candidatos = [
        ("turma", "/api/v1/turma/"),
        ("alunos", "/api/v1/alunos/"),
        ("titulos", "/api/v1/titulos/"),
        ("contratos", "/api/v1/contratos/"),
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

    if not turmas_2026:
        return log

    # Testar detalhe da primeira turma
    t = turmas_2026[0]
    log.append(f"Detalhe turma: {t.get('nome', '')}...")

    detalhe = _extrair_turma_detalhe(session, t["id"])
    if detalhe:
        campos = list(detalhe.keys())
        log.append(f"  Campos detalhe: {', '.join(campos)}")

        # Mostrar conteudo de cada campo relevante
        for campo in campos:
            val = detalhe[campo]
            if isinstance(val, list):
                if len(val) > 0 and isinstance(val[0], dict):
                    log.append(f"  {campo}: lista com {len(val)} dicts, keys={list(val[0].keys())[:10]}")
                elif len(val) > 0:
                    log.append(f"  {campo}: lista com {len(val)} items, tipo={type(val[0]).__name__}, amostra={str(val[:3])[:100]}")
                else:
                    log.append(f"  {campo}: lista vazia")
            elif isinstance(val, dict):
                log.append(f"  {campo}: dict keys={list(val.keys())[:10]}, amostra={json.dumps(val, ensure_ascii=False)[:150]}")
            elif val is not None and str(val).strip():
                log.append(f"  {campo}: {str(val)[:80]}")
    else:
        log.append("  Detalhe turma: falhou")

    # Testar filtro /api/v1/alunos/?turma=X
    log.append(f"Testando /api/v1/alunos/?turma={t['id']}...")
    try:
        r = session.get(f"{SIGA_URL}/api/v1/alunos/", params={
            "limit": 3, "offset": 0, "turma": t["id"],
        }, timeout=30)
        if r.status_code == 200 and _is_json(r):
            data = r.json()
            count = data.get("count", "?")
            log.append(f"  Resultado filtro: count={count}")
            results = data.get("results", [])
            if results:
                log.append(f"  Campos: {list(results[0].keys())}")
                # Verificar se filtragem funcionou (count < total)
                total = endpoints.get("alunos", {}).get("count", 0)
                if count == total or count == "?":
                    log.append(f"  AVISO: filtro por turma pode nao funcionar (count={count} vs total={total})")
        else:
            log.append(f"  Filtro: status={r.status_code}")
    except Exception as e:
        log.append(f"  Filtro: ERRO ({e})")

    return log
