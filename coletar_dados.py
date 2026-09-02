#!/usr/bin/env python3
"""
Coleta de dados do SIGA para dashboard de contratos 2027.
Roda 2x/dia via cron. Salva snapshot em JSON.

Fontes por unidade:
- /api/v1/alunoturma/?periodo={id} → alunos matriculados 2027
- /api/v1/assinatura_eletronica/?periodo_sigla=2027 → contratos + status
"""

import json
import os
import time
import requests
from datetime import datetime
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────
BASE_URL = "https://siga02.activesoft.com.br"
SIGA_LOGIN_URL = "https://siga.activesoft.com.br"
try:
    import streamlit as st
    INSTITUICAO = st.secrets.get("SIGA_INSTITUICAO", "COLEGIOELO")
    LOGIN = st.secrets.get("SIGA_LOGIN", "claryce")
    SENHA = st.secrets.get("SIGA_SENHA", "Clary123@")
except Exception:
    INSTITUICAO = os.environ.get("SIGA_INSTITUICAO", "COLEGIOELO")
    LOGIN = os.environ.get("SIGA_LOGIN", "claryce")
    SENHA = os.environ.get("SIGA_SENHA", "Clary123@")
PERIODO_SIGLA = "2027"

UNIDADES = [
    {"pk": "2", "sigla": "BV", "nome": "Boa Viagem", "periodo_id": 112},
    {"pk": "3", "sigla": "CD", "nome": "Candeias", "periodo_id": 113},
    {"pk": "4", "sigla": "JG", "nome": "Janga", "periodo_id": 111},
    {"pk": "5", "sigla": "CDR", "nome": "Cordeiro", "periodo_id": 110},
]

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)


# ── Login ───────────────────────────────────────────────────────────
def fazer_login(unidade_pk):
    session = requests.Session()
    r = session.get(f"{SIGA_LOGIN_URL}/login/", timeout=15)
    csrf = session.cookies.get("csrftoken")
    session.post(
        f"{SIGA_LOGIN_URL}/login/",
        data={
            "codigo": INSTITUICAO, "login": LOGIN,
            "senha": SENHA, "csrfmiddlewaretoken": csrf,
        },
        headers={"Referer": f"{SIGA_LOGIN_URL}/login/"},
        allow_redirects=True, timeout=15,
    )
    csrf = session.cookies.get("csrftoken")
    r = session.post(
        f"{SIGA_LOGIN_URL}/login/unidade/",
        data={"unidade": unidade_pk, "csrfmiddlewaretoken": csrf},
        headers={"Referer": f"{SIGA_LOGIN_URL}/login/unidade/"},
        allow_redirects=False, timeout=15,
    )
    redirect_url = r.headers.get("Location", "")
    if redirect_url.startswith("/"):
        redirect_url = f"{SIGA_LOGIN_URL}{redirect_url}"
    if not redirect_url:
        print(f"  ERRO login pk={unidade_pk}: sem redirect", flush=True)
        return None
    session.get(redirect_url, allow_redirects=True, timeout=15)
    return session


# ── Paginação ───────────────────────────────────────────────────────
def buscar_todos(session, endpoint, params_base, label=""):
    todos = []
    offset = 0
    limit = 200
    while True:
        params = {**params_base, "limit": limit, "offset": offset}
        try:
            r = session.get(f"{BASE_URL}{endpoint}", params=params, timeout=60)
        except Exception as e:
            print(f"  ERRO {label} offset={offset}: {e}", flush=True)
            break
        if r.status_code != 200:
            print(f"  ERRO {label} status={r.status_code}", flush=True)
            break
        try:
            data = r.json()
        except Exception:
            break
        if isinstance(data, list):
            todos.extend(data)
            break
        results = data.get("results", [])
        todos.extend(results)
        total = data.get("count", 0)
        offset += limit
        if offset >= total:
            break
        time.sleep(0.2)
    return todos


# ── Coleta por unidade ──────────────────────────────────────────────
def coletar_unidade(unidade):
    pk = unidade["pk"]
    sigla = unidade["sigla"]
    periodo_id = unidade["periodo_id"]

    print(f"\n  {sigla}: login...", flush=True)
    session = fazer_login(pk)
    if not session:
        return None

    # 1. Alunos matriculados 2027 (filtro por periodo_id = rápido)
    print(f"  {sigla}: buscando alunos 2027...", flush=True)
    alunos_turma = buscar_todos(
        session, "/api/v1/alunoturma/",
        {"periodo": str(periodo_id)},
        f"{sigla} alunoturma",
    )
    # Filtrar apenas Cursando
    alunos_cursando = [
        at for at in alunos_turma
        if at.get("situacao_aluno_turma_nome") == "Cursando"
    ]
    print(f"  {sigla}: {len(alunos_cursando)} alunos cursando 2027 (de {len(alunos_turma)} total)", flush=True)

    # 2. Contratos 2027
    print(f"  {sigla}: buscando contratos 2027...", flush=True)
    contratos_raw = buscar_todos(
        session, "/api/v1/assinatura_eletronica/",
        {"periodo_sigla": PERIODO_SIGLA},
        f"{sigla} contratos",
    )
    print(f"  {sigla}: {len(contratos_raw)} contratos", flush=True)

    # Processar alunos
    alunos = []
    for at in alunos_cursando:
        alunos.append({
            "id_aluno_turma": at.get("id_aluno_turma"),
            "aluno_id": at.get("aluno"),
            "matricula": at.get("aluno_matricula"),
            "nome": at.get("aluno_nome"),
            "serie": at.get("serie_nome"),
            "curso": at.get("curso_nome"),
            "turma": at.get("turma_nome"),
            "turno": at.get("turno"),
            "responsavel": at.get("aluno_responsavel_nome"),
            "data_matricula": at.get("data_efetivacao_matricula"),
        })

    # Processar contratos
    aluno_turma_ids = {a["id_aluno_turma"] for a in alunos}
    contratos = []
    contratos_aguardando_ids = []
    for c in contratos_raw:
        info = {
            "id": c.get("id"),
            "aluno_turma_id": c.get("aluno_turma_id"),
            "aluno_id": c.get("aluno_id"),
            "matricula": c.get("aluno_matricula"),
            "nome": c.get("aluno_nome"),
            "serie": c.get("serie_nome"),
            "curso": c.get("curso_nome"),
            "turma": c.get("turma_nome"),
            "situacao_assinatura": c.get("situacao_assinatura_label"),
            "situacao_codigo": c.get("situacao_assinatura"),
            "titulo_contrato": c.get("titulo"),
            "data_criacao": c.get("data_criacao"),
            "data_assinatura": c.get("data_assinatura_confirmada"),
            "data_cancelamento": c.get("data_cancelamento"),
            "contrato_cancelado": c.get("contrato_cancelado_clicksign"),
            "signatarios": [],
        }
        contratos.append(info)
        # Coletar signatários apenas de contratos aguardando de alunos cursando
        if (c.get("situacao_assinatura_label") == "Aguardando assinatura"
                and c.get("aluno_turma_id") in aluno_turma_ids):
            contratos_aguardando_ids.append((info, c.get("id")))

    # 3. Buscar signatários dos contratos aguardando assinatura
    print(f"  {sigla}: buscando signatários de {len(contratos_aguardando_ids)} contratos...", flush=True)
    for i, (info, cid) in enumerate(contratos_aguardando_ids):
        try:
            r = session.get(
                f"{BASE_URL}/api/v1/assinatura_eletronica/documento/{cid}/signatarios/",
                timeout=15,
            )
            if r.status_code == 200:
                data = r.json()
                sigs = data.get("signatarios", [])
                info["signatarios"] = [
                    {
                        "tipo": s.get("tipo_signatario"),
                        "nome": s.get("nome_signatario"),
                        "assinou": s.get("data_hora_assinatura") is not None,
                        "data_assinatura": s.get("data_hora_assinatura"),
                    }
                    for s in sigs
                ]
        except Exception:
            pass
        time.sleep(0.05)
        if (i + 1) % 50 == 0:
            print(f"    Progresso signatários: {i+1}/{len(contratos_aguardando_ids)}", flush=True)
    print(f"  {sigla}: signatários coletados", flush=True)

    return {
        "unidade": sigla,
        "alunos": alunos,
        "contratos": contratos,
    }


# ── Main ────────────────────────────────────────────────────────────
def main():
    t0 = time.time()
    print(f"Coleta - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    resultado = {
        "periodo": PERIODO_SIGLA,
        "coleta_em": datetime.now().isoformat(),
        "unidades": {},
    }

    for unidade in UNIDADES:
        dados = coletar_unidade(unidade)
        if dados:
            resultado["unidades"][unidade["sigla"]] = dados

    # Salvar
    arquivo = DATA_DIR / "snapshot.json"
    with open(arquivo, "w", encoding="utf-8") as f:
        json.dump(resultado, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - t0
    print(f"\nSalvo: {arquivo} ({arquivo.stat().st_size / 1024:.1f} KB) em {elapsed:.0f}s", flush=True)

    # Resumo
    print(f"\n{'='*50}", flush=True)
    total_alunos = 0
    total_contratos = 0
    for sigla, dados in resultado["unidades"].items():
        na = len(dados["alunos"])
        nc = len(dados["contratos"])
        total_alunos += na
        total_contratos += nc
        print(f"  {sigla}: {na} alunos | {nc} contratos", flush=True)
    print(f"  TOTAL: {total_alunos} alunos | {total_contratos} contratos", flush=True)


if __name__ == "__main__":
    main()
