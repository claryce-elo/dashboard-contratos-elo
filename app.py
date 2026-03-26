"""
Dashboard de Contratos - Colegio ELO
Acompanhamento de assinaturas de contratos por turma.
Dados extraidos automaticamente do SIGA ou via upload de CSV.
"""

import streamlit as st
import pandas as pd
import json
import csv
import io
from pathlib import Path
from datetime import datetime

from utils.classificacao import (
    classificar_turma, normalizar_status, is_test_student,
    processar_csv_contratos, processar_csv_alunos,
    GRADE_ORDER, STATUS_CORES,
)
from utils.siga_client import UNIDADES, UNIDADE_MAP

st.set_page_config(
    page_title="Contratos - Colegio ELO",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
CACHE_FILE = DATA_DIR / "cache_contratos.json"
EDITS_FILE = DATA_DIR / "edits_contratos.json"


# =============================================================================
# PERSISTENCIA
# =============================================================================

def carregar_cache():
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    return None


def salvar_cache(dados):
    CACHE_FILE.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")


def carregar_edits():
    if EDITS_FILE.exists():
        return json.loads(EDITS_FILE.read_text(encoding="utf-8"))
    return {}


def salvar_edits(edits):
    EDITS_FILE.write_text(json.dumps(edits, ensure_ascii=False, indent=2), encoding="utf-8")


# =============================================================================
# PARSE CSV
# =============================================================================

def parse_csv_upload(uploaded_file):
    """Le CSV de um UploadedFile do Streamlit."""
    content = uploaded_file.getvalue().decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


# =============================================================================
# EXTRACAO SIGA
# =============================================================================

def atualizar_dados_siga():
    from utils.siga_client import extrair_tudo

    inst = st.secrets.get("SIGA_INSTITUICAO", "COLEGIOELO")
    login = st.secrets.get("SIGA_LOGIN", "")
    senha = st.secrets.get("SIGA_SENHA", "")

    if not login or not senha:
        st.error("Configure SIGA_LOGIN e SIGA_SENHA em .streamlit/secrets.toml")
        return None

    progress = st.progress(0, text="Conectando ao SIGA...")
    msgs = []

    def cb(msg):
        msgs.append(msg)
        progress.progress(min(len(msgs) / 8, 1.0), text=msg)

    resultado = extrair_tudo(inst, login, senha, progress_cb=cb)
    progress.empty()

    # Classificar turmas
    for a in resultado["alunos"]:
        cls = classificar_turma(a.get("turma", ""))
        a["categoria"] = cls["cat"]
        a["grupo"] = cls["grupo"]

    cache = {
        "alunos": resultado["alunos"],
        "erros": resultado["erros"],
        "timestamp": resultado["timestamp"],
        "fonte": "SIGA",
    }
    salvar_cache(cache)
    return cache


# =============================================================================
# METRICAS E TABELAS
# =============================================================================

def calcular_metricas(df):
    total = len(df)
    assinados = len(df[df["status_contrato"] == "Assinado"])
    aguardando = len(df[df["status_contrato"] == "Aguardando"])
    cancelados = len(df[df["status_contrato"] == "Cancelado"])
    sem_contrato = len(df[df["status_contrato"] == "Sem Contrato"])
    taxa = (assinados / total * 100) if total > 0 else 0
    return {
        "total": total, "assinados": assinados, "aguardando": aguardando,
        "cancelados": cancelados, "sem_contrato": sem_contrato, "taxa": taxa,
    }


def cor_taxa(pct):
    if pct >= 50:
        return "#27ae60"
    if pct >= 25:
        return "#f39c12"
    return "#e74c3c"


def badge_html(texto, status):
    cores = STATUS_CORES.get(status, STATUS_CORES["Outro"])
    return f'<span style="background:{cores["bg"]};color:{cores["cor"]};padding:3px 10px;border-radius:12px;font-size:0.8em;font-weight:600;">{texto}</span>'


# =============================================================================
# DASHBOARD
# =============================================================================

def main():
    # Header
    st.markdown("""
    <div style='background:linear-gradient(135deg,#2c3e50,#3498db);padding:20px 30px;
    border-radius:10px;margin-bottom:20px;'>
        <h1 style='color:white;margin:0;font-size:28px;'>Dashboard de Contratos - Colegio ELO</h1>
        <p style='color:rgba(255,255,255,0.8);margin:5px 0 0 0;'>
        Acompanhamento de assinaturas de contratos por turma</p>
    </div>
    """, unsafe_allow_html=True)

    # --- SIDEBAR ---
    with st.sidebar:
        st.header("Dados")

        # Botao SIGA
        if st.button("Atualizar do SIGA", use_container_width=True, type="primary"):
            cache = atualizar_dados_siga()
            if cache:
                n = len(cache.get("alunos", []))
                if n > 0:
                    st.success(f"{n} registros extraidos do SIGA")
                st.rerun()

        # Upload CSV Contratos
        st.divider()
        st.subheader("Upload Manual")
        file_contratos = st.file_uploader(
            "Relatorio de Contratos (CSV)",
            type=["csv"],
            key="upload_contratos",
            help="CSV exportado do SIGA com colunas: Turma, Matricula, Nome, Status do Contrato",
        )
        if file_contratos:
            rows = parse_csv_upload(file_contratos)
            alunos = processar_csv_contratos(rows)
            if alunos:
                cache = {
                    "alunos": alunos,
                    "erros": [],
                    "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M"),
                    "fonte": "CSV Upload",
                }
                salvar_cache(cache)
                # Limpar edicoes manuais ao carregar novos dados
                salvar_edits({})
                st.success(f"{len(alunos)} registros carregados")
                st.rerun()
            else:
                st.error("Nenhum registro encontrado no CSV")

        # Upload CSV Alunos (complementar)
        file_alunos = st.file_uploader(
            "Relatorio de Alunos (CSV)",
            type=["csv"],
            key="upload_alunos",
            help="Adiciona alunos sem contrato. Nao sobrescreve dados existentes.",
        )
        if file_alunos:
            cache = carregar_cache()
            if cache:
                rows = parse_csv_upload(file_alunos)
                novos = processar_csv_alunos(rows, cache.get("alunos", []))
                cache["alunos"].extend(novos)
                cache["timestamp"] = datetime.now().strftime("%d/%m/%Y %H:%M")
                salvar_cache(cache)
                st.success(f"{len(novos)} novos alunos adicionados como 'Sem Contrato'")
                st.rerun()
            else:
                st.warning("Carregue primeiro o relatorio de contratos")

        st.divider()

        # Info de ultima atualizacao
        cache = carregar_cache()
        if cache:
            st.info(f"Atualizado: {cache.get('timestamp', 'N/A')}")
            st.caption(f"Fonte: {cache.get('fonte', '?')}")
            erros = cache.get("erros", [])
            if erros:
                with st.expander("Erros"):
                    for e in erros:
                        st.caption(e)
        else:
            st.warning("Nenhum dado carregado")

        # Testar conexao
        if st.button("Testar conexao SIGA", use_container_width=True):
            from utils.siga_client import testar_conexao
            inst = st.secrets.get("SIGA_INSTITUICAO", "COLEGIOELO")
            login = st.secrets.get("SIGA_LOGIN", "")
            senha = st.secrets.get("SIGA_SENHA", "")
            if not login or not senha:
                st.error("Credenciais nao configuradas")
            else:
                with st.spinner("Testando..."):
                    log = testar_conexao(inst, login, senha)
                for linha in log:
                    if "ERRO" in linha or "FALHA" in linha:
                        st.error(linha)
                    elif "OK" in linha:
                        st.success(linha)
                    else:
                        st.info(linha)

    # --- CONTEUDO PRINCIPAL ---
    cache = carregar_cache()
    if not cache or not cache.get("alunos"):
        st.info("Clique em **Atualizar do SIGA** ou faca upload de CSV na barra lateral.")
        return

    # Aplicar edicoes manuais
    edits = carregar_edits()
    alunos = cache["alunos"]
    for a in alunos:
        mat = a.get("matricula", "")
        if mat in edits:
            a["status_contrato"] = edits[mat]["status"]
            a["_editado"] = True
        else:
            a["_editado"] = False

    df = pd.DataFrame(alunos)

    # --- FILTROS ---
    col_f1, col_f2, col_f3 = st.columns([1, 1, 2])
    with col_f1:
        unidades_disponiveis = sorted(df["unidade"].unique().tolist())
        nomes_unidades = ["Todas"] + [UNIDADE_MAP.get(u, u) for u in unidades_disponiveis]
        filtro_unidade = st.selectbox("Unidade", nomes_unidades)
    with col_f2:
        filtro_status = st.selectbox("Status", ["Todos", "Assinado", "Aguardando", "Cancelado", "Sem Contrato", "Outro"])
    with col_f3:
        busca = st.text_input("Buscar aluno", placeholder="Digite o nome...")

    # Aplicar filtros
    df_filtrado = df.copy()
    if filtro_unidade != "Todas":
        cod = next((u["codigo"] for u in UNIDADES if u["nome"] == filtro_unidade), filtro_unidade)
        df_filtrado = df_filtrado[df_filtrado["unidade"] == cod]
    if filtro_status != "Todos":
        df_filtrado = df_filtrado[df_filtrado["status_contrato"] == filtro_status]
    if busca:
        df_filtrado = df_filtrado[df_filtrado["nome"].str.contains(busca, case=False, na=False)]

    # --- CARDS RESUMO ---
    m = calcular_metricas(df_filtrado)
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    def metric_card(col, valor, label, cor):
        col.markdown(f"""
        <div style='background:white;border-radius:10px;padding:15px;text-align:center;
        box-shadow:0 2px 8px rgba(0,0,0,0.08);'>
            <div style='font-size:2em;font-weight:700;color:{cor};'>{valor}</div>
            <div style='font-size:0.85em;color:#7f8c8d;margin-top:4px;'>{label}</div>
        </div>""", unsafe_allow_html=True)

    metric_card(c1, m["total"], "Total de Alunos", "#2980b9")
    metric_card(c2, m["assinados"], "Assinados", "#27ae60")
    metric_card(c3, m["aguardando"], "Aguardando", "#f39c12")
    metric_card(c4, m["cancelados"], "Cancelados", "#e74c3c")
    metric_card(c5, m["sem_contrato"], "Sem Contrato", "#8e44ad")
    metric_card(c6, f"{m['taxa']:.1f}%", "Taxa de Assinatura", cor_taxa(m["taxa"]))

    st.markdown("<br>", unsafe_allow_html=True)

    # --- TABS ---
    tab_regular, tab_integral, tab_extra = st.tabs([
        "Turmas Regulares", "Integral/Complementar", "Extracurriculares"
    ])

    def render_categoria(container, df_cat, categoria):
        if df_cat.empty:
            container.info("Nenhum dado.")
            return

        # Agrupar por grupo
        grupos = df_cat.groupby("grupo")
        ordem = GRADE_ORDER if categoria == "regular" else None

        grupo_names = sorted(grupos.groups.keys(), key=lambda g: (
            GRADE_ORDER.index(g) if g in GRADE_ORDER else 999, g
        )) if ordem else sorted(grupos.groups.keys())

        for grupo_nome in grupo_names:
            df_grupo = grupos.get_group(grupo_nome)
            m_g = calcular_metricas(df_grupo)

            with container.expander(
                f"**{grupo_nome}** — {m_g['total']} alunos | {m_g['taxa']:.1f}% assinados",
                expanded=False,
            ):
                # Tabela por unidade
                unidades_grupo = sorted(df_grupo["unidade"].unique())
                for un in unidades_grupo:
                    df_un = df_grupo[df_grupo["unidade"] == un]
                    m_un = calcular_metricas(df_un)
                    un_nome = UNIDADE_MAP.get(un, un)

                    # Barra de progresso
                    pct = m_un["taxa"]
                    bar_cor = cor_taxa(pct)

                    st.markdown(f"""
                    <div style='display:flex;align-items:center;gap:12px;padding:8px 0;
                    border-bottom:1px solid #f1f2f6;'>
                        <div style='min-width:100px;font-weight:600;'>{un_nome}</div>
                        <div style='min-width:50px;text-align:center;'>{m_un['total']}</div>
                        <div>{badge_html(m_un['assinados'], 'Assinado')}</div>
                        <div>{badge_html(m_un['aguardando'], 'Aguardando')}</div>
                        <div>{badge_html(m_un['cancelados'], 'Cancelado')}</div>
                        <div>{badge_html(m_un['sem_contrato'], 'Sem Contrato')}</div>
                        <div style='font-weight:700;color:{bar_cor};min-width:55px;text-align:right;'>
                            {pct:.1f}%</div>
                        <div style='flex:1;max-width:120px;height:8px;background:#ecf0f1;
                        border-radius:4px;overflow:hidden;'>
                            <div style='height:100%;width:{pct}%;background:{bar_cor};
                            border-radius:4px;'></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                    # Tabela de alunos (dentro de sub-expander)
                    with st.expander(f"Alunos - {un_nome} ({m_un['total']})", expanded=False):
                        df_display = df_un[["nome", "matricula", "turma", "situacao", "status_contrato"]].copy()
                        df_display = df_display.sort_values("nome")
                        df_display.columns = ["Nome", "Matricula", "Turma", "Situacao", "Status"]

                        # Editar status
                        edited = st.data_editor(
                            df_display,
                            column_config={
                                "Status": st.column_config.SelectboxColumn(
                                    options=["Assinado", "Aguardando", "Cancelado", "Sem Contrato", "Outro"],
                                    required=True,
                                ),
                            },
                            hide_index=True,
                            use_container_width=True,
                            key=f"editor_{categoria}_{grupo_nome}_{un}",
                        )

                        # Detectar mudancas e salvar
                        if edited is not None:
                            for i, row in edited.iterrows():
                                mat = row["Matricula"]
                                novo_status = row["Status"]
                                # Comparar com dados originais
                                original = df_un[df_un["matricula"] == mat]
                                if not original.empty:
                                    status_orig = original.iloc[0]["status_contrato"]
                                    if novo_status != status_orig:
                                        edits[mat] = {
                                            "status": novo_status,
                                            "ts": datetime.now().isoformat(),
                                        }
                                        salvar_edits(edits)

    with tab_regular:
        df_reg = df_filtrado[df_filtrado["categoria"] == "regular"]
        render_categoria(tab_regular, df_reg, "regular")

    with tab_integral:
        df_int = df_filtrado[df_filtrado["categoria"] == "integral"]
        render_categoria(tab_integral, df_int, "integral")

    with tab_extra:
        df_ext = df_filtrado[df_filtrado["categoria"] == "extra"]
        render_categoria(tab_extra, df_ext, "extra")

    # --- RESULTADOS DA BUSCA ---
    if busca and not df_filtrado.empty:
        st.divider()
        st.subheader(f"Resultados da busca: {len(df_filtrado)} aluno(s)")
        st.dataframe(
            df_filtrado[["nome", "unidade", "turma", "grupo", "situacao", "status_contrato"]].sort_values("nome"),
            hide_index=True,
            use_container_width=True,
        )

    # --- EXPORTAR ---
    st.divider()
    col_exp1, col_exp2, col_exp3 = st.columns([1, 1, 4])
    with col_exp1:
        csv_data = df_filtrado[["nome", "matricula", "unidade", "grupo", "turma", "situacao", "status_contrato"]].to_csv(
            index=False, sep=";", encoding="utf-8-sig",
        )
        st.download_button(
            "Exportar CSV",
            csv_data,
            f"contratos_{datetime.now().strftime('%Y-%m-%d')}.csv",
            "text/csv",
            use_container_width=True,
        )
    with col_exp2:
        n_edits = len(edits)
        if n_edits > 0:
            if st.button(f"Limpar {n_edits} edicao(oes)", use_container_width=True):
                salvar_edits({})
                st.rerun()

    # Footer
    st.markdown("""
    <div style='text-align:center;color:#95a5a6;font-size:0.8em;padding:15px;
    border-top:1px solid #dcdde1;margin-top:20px;'>
        Dashboard de Contratos - Colegio ELO | Dados do ActiveSoft/SIGA
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
