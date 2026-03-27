"""
Cliente SIGA - Extrai dados de contratos e alunos do sistema Activesoft.

Limitacoes da API:
  /api/v1/turma/  - lista turmas com contagens (alunos_ativos, alunos_por_situacao_na_turma)
  /api/v1/alunos/ - so tem id/nome/matricula/ativo, SEM turma
  /api/v1/turma/{id}/ - detalhe NAO retorna JSON
  Filtro /api/v1/alunos/?turma=X NAO funciona

Estrategia: usar lista de turmas para resumo agregado e CSV upload para dados individuais.
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
    """Busca turmas de 2026 via API com todos os campos disponiveis."""
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


def _processar_turma(turma, unidade_codigo):
    """Converte dados da turma API em registro para o dashboard."""
    nome = turma.get("nome", turma.get("nome_turma_completo", ""))
    serie_nome = turma.get("serie_nome", "")
    curso_nome = turma.get("curso_nome", "")
    alunos_ativos = turma.get("alunos_ativos", 0) or 0
    quantidade_alunos = turma.get("quantidade_alunos", 0) or 0
    vagas_disponiveis = turma.get("vagas_disponiveis", 0) or 0

    # alunos_por_situacao_na_turma pode ser dict ou list
    situacao_data = turma.get("alunos_por_situacao_na_turma", {})

    return {
        "turma_id": turma.get("id"),
        "turma": nome,
        "serie_nome": serie_nome,
        "curso_nome": curso_nome,
        "turno": turma.get("turno", ""),
        "alunos_ativos": alunos_ativos,
        "quantidade_alunos": quantidade_alunos,
        "vagas_disponiveis": vagas_disponiveis,
        "alunos_por_situacao": situacao_data,
        "unidade": unidade_codigo,
    }


# =========================================================================
# ORQUESTRACAO
# =========================================================================

def extrair_turmas_unidade(instituicao, login, senha, unidade):
    """Extrai dados de turmas 2026 de uma unidade."""
    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        raise RuntimeError(f"Login falhou ({unidade['codigo']}): {erro}")

    turmas = _extrair_turmas_2026(session)
    return [_processar_turma(t, unidade["codigo"]) for t in turmas]


def extrair_tudo(instituicao, login, senha, progress_cb=None):
    """Extrai turmas de todas as unidades em paralelo."""
    resultado = {"turmas": [], "erros": [], "timestamp": ""}

    def _proc(u):
        try:
            regs = extrair_turmas_unidade(instituicao, login, senha, u)
            return {"regs": regs, "msg": f"{u['nome']}: {len(regs)} turmas"}
        except Exception as e:
            msg = f"{u['codigo']}: {e}"
            resultado["erros"].append(msg)
            return {"regs": [], "msg": msg}

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_proc, u): u for u in UNIDADES}
        for f in as_completed(futures):
            res = f.result()
            resultado["turmas"].extend(res["regs"])
            if progress_cb:
                progress_cb(res["msg"])

    resultado["timestamp"] = datetime.now().strftime("%d/%m/%Y %H:%M")
    return resultado


# =========================================================================
# DIAGNOSTICO
# =========================================================================

def testar_conexao(instituicao, login, senha):
    """Testa conexao e retorna diagnostico."""
    log = []
    unidade = UNIDADES[0]

    session, erro = _login(instituicao, login, senha, unidade)
    if not session:
        log.append(f"ERRO: {erro}")
        return log
    log.append("Login: OK")

    # Turmas 2026
    turmas_2026 = _extrair_turmas_2026(session)
    log.append(f"Turmas 2026: {len(turmas_2026)} encontradas")

    if turmas_2026:
        # Mostrar amostra de uma turma
        t = turmas_2026[0]
        log.append(f"Amostra turma: {t.get('nome', '')}")
        log.append(f"  alunos_ativos: {t.get('alunos_ativos', 0)}")
        log.append(f"  quantidade_alunos: {t.get('quantidade_alunos', 0)}")
        log.append(f"  serie_nome: {t.get('serie_nome', '')}")
        log.append(f"  curso_nome: {t.get('curso_nome', '')}")

        # Campo chave: alunos_por_situacao_na_turma
        situacao = t.get("alunos_por_situacao_na_turma")
        if situacao:
            log.append(f"  alunos_por_situacao_na_turma: {json.dumps(situacao, ensure_ascii=False)[:200]}")
        else:
            log.append("  alunos_por_situacao_na_turma: vazio")

        # Totais
        total_alunos = sum(t.get("alunos_ativos", 0) or 0 for t in turmas_2026)
        log.append(f"Total alunos ativos (todas turmas 2026): {total_alunos}")

    log.append("---")
    log.append("NOTA: API do SIGA nao fornece dados individuais por turma.")
    log.append("Para dados aluno a aluno, use Upload CSV na sidebar.")

    return log
