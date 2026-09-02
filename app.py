import json
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Contratos 2027 - Colégio ELO",
    page_icon="📋",
    layout="wide",
)

DATA_FILE = Path(__file__).parent / "data" / "snapshot.json"

ORDEM_SEGMENTOS = [
    "Educação Infantil",
    "Ensino Fundamental I",
    "Ensino Fundamental II",
    "Ensino Médio",
]

STATUS_CORES = {
    "Assinado": "#2ecc71",
    "Aguardando assinatura": "#f39c12",
    "Sem contrato": "#e74c3c",
}


# ── Funções auxiliares ──────────────────────────────────────────────
def extrair_segmento(curso_nome):
    if not curso_nome:
        return "Outros"
    # O SIGA usa "Educação  Infantil" com 2 espaços
    nome = curso_nome.replace("  ", " ")
    for seg in ORDEM_SEGMENTOS:
        if seg in nome:
            return seg
    return "Outros"


@st.cache_data(ttl=300)
def carregar_dados():
    if not DATA_FILE.exists():
        return None
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def montar_dataframes(dados):
    alunos_all = []
    contratos_all = []

    for sigla, info in dados["unidades"].items():
        for a in info["alunos"]:
            rec = {**a, "unidade": sigla, "segmento": extrair_segmento(a.get("curso"))}
            alunos_all.append(rec)
        for c in info["contratos"]:
            rec = {**c, "unidade": sigla, "segmento": extrair_segmento(c.get("curso"))}
            contratos_all.append(rec)

    df_alunos = pd.DataFrame(alunos_all)
    df_contratos = pd.DataFrame(contratos_all)
    return df_alunos, df_contratos


def classificar_aluno(at_id, contratos_do_aluno):
    """Retorna o status consolidado do contrato para um aluno."""
    if not contratos_do_aluno:
        return "Sem contrato"

    # Filtrar apenas por situacao_assinatura (ignorar campo contrato_cancelado
    # pois no SIGA vem True para todos os registros)
    ativos = [
        c for c in contratos_do_aluno
        if c.get("situacao_assinatura") not in ("Contrato cancelado",)
    ]

    if not ativos:
        return "Sem contrato"

    if any(c["situacao_assinatura"] == "Assinado" for c in ativos):
        return "Assinado"

    if any(c["situacao_assinatura"] == "Aguardando assinatura" for c in ativos):
        return "Aguardando assinatura"

    return "Sem contrato"


def build_summary(df_alunos, df_contratos):
    # Agrupar contratos por aluno_turma_id
    contratos_por_at = {}
    for _, c in df_contratos.iterrows():
        at_id = c.get("aluno_turma_id")
        if at_id is not None:
            contratos_por_at.setdefault(at_id, []).append(c.to_dict())

    df = df_alunos.copy()
    df["status_contrato"] = df["id_aluno_turma"].apply(
        lambda at_id: classificar_aluno(at_id, contratos_por_at.get(at_id))
    )
    return df


# ── UI ──────────────────────────────────────────────────────────────
def main():
    st.title("📋 Dashboard de Contratos 2027")
    st.caption("Colégio ELO — Comparativo Matrículas x Contratos")

    dados = carregar_dados()
    if dados is None:
        st.error("Snapshot não encontrado. Execute `python coletar_dados.py` primeiro.")
        return

    coleta_em = dados.get("coleta_em", "")
    if coleta_em:
        try:
            dt = datetime.fromisoformat(coleta_em)
            st.caption(f"Última atualização: {dt.strftime('%d/%m/%Y %H:%M')}")
        except Exception:
            pass

    df_alunos, df_contratos = montar_dataframes(dados)

    if df_alunos.empty:
        st.warning("Nenhum aluno encontrado no snapshot.")
        return

    df_alunos = build_summary(df_alunos, df_contratos)

    # ── Filtros ─────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        unidades_disp = sorted(df_alunos["unidade"].unique())
        unidades_sel = st.multiselect("Unidade", unidades_disp, default=unidades_disp)
    with col_f2:
        segmentos_disp = sorted(
            df_alunos["segmento"].unique(),
            key=lambda s: ORDEM_SEGMENTOS.index(s) if s in ORDEM_SEGMENTOS else 99,
        )
        segmentos_sel = st.multiselect("Segmento", segmentos_disp, default=segmentos_disp)
    with col_f3:
        status_disp = ["Assinado", "Aguardando assinatura", "Sem contrato"]
        status_sel = st.multiselect("Status do Contrato", status_disp, default=status_disp)

    mask = (
        df_alunos["unidade"].isin(unidades_sel)
        & df_alunos["segmento"].isin(segmentos_sel)
        & df_alunos["status_contrato"].isin(status_sel)
    )
    df_filtrado = df_alunos[mask]

    # ── KPIs ────────────────────────────────────────────────────────
    st.markdown("---")
    total = len(df_filtrado)
    assinados = (df_filtrado["status_contrato"] == "Assinado").sum()
    aguardando = (df_filtrado["status_contrato"] == "Aguardando assinatura").sum()
    sem_contrato = (df_filtrado["status_contrato"] == "Sem contrato").sum()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Matriculados", total)
    c2.metric("Assinados", assinados, f"{assinados/total*100:.1f}%" if total else "0%")
    c3.metric("Aguardando Assinatura", aguardando, f"{aguardando/total*100:.1f}%" if total else "0%")
    c4.metric("Sem Contrato", sem_contrato, f"{sem_contrato/total*100:.1f}%" if total else "0%")

    # ── Tabs ────────────────────────────────────────────────────────
    st.markdown("---")
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Por Unidade", "📚 Por Segmento", "📋 Lista Detalhada", "✍️ Signatários Pendentes"
    ])

    def fazer_pivot(df, group_col):
        if df.empty:
            return pd.DataFrame()
        pivot = (
            df.groupby([group_col, "status_contrato"])
            .size()
            .unstack(fill_value=0)
        )
        for col in ["Assinado", "Aguardando assinatura", "Sem contrato"]:
            if col not in pivot.columns:
                pivot[col] = 0
        pivot = pivot[["Assinado", "Aguardando assinatura", "Sem contrato"]]
        pivot["Total"] = pivot.sum(axis=1)
        pivot["% Assinado"] = (pivot["Assinado"] / pivot["Total"] * 100).round(1)
        return pivot

    with tab1:
        st.subheader("Status dos Contratos por Unidade")
        pivot = fazer_pivot(df_filtrado, "unidade")
        if not pivot.empty:
            chart_cols = ["Assinado", "Aguardando assinatura", "Sem contrato"]
            st.bar_chart(pivot[chart_cols], color=["#2ecc71", "#f39c12", "#e74c3c"])
            st.dataframe(pivot, use_container_width=True)

    with tab2:
        st.subheader("Status dos Contratos por Segmento")
        pivot = fazer_pivot(df_filtrado, "segmento")
        if not pivot.empty:
            chart_cols = ["Assinado", "Aguardando assinatura", "Sem contrato"]
            st.bar_chart(pivot[chart_cols], color=["#2ecc71", "#f39c12", "#e74c3c"])
            st.dataframe(pivot, use_container_width=True)

    with tab3:
        st.subheader("Lista de Alunos")

        colunas_exibir = [
            "unidade", "matricula", "nome", "segmento",
            "serie", "turma", "responsavel", "status_contrato",
        ]
        colunas_presentes = [c for c in colunas_exibir if c in df_filtrado.columns]
        df_exibir = df_filtrado[colunas_presentes].sort_values(
            ["unidade", "segmento", "serie", "nome"]
        )

        busca = st.text_input("🔍 Buscar aluno por nome")
        if busca:
            df_exibir = df_exibir[
                df_exibir["nome"].str.contains(busca, case=False, na=False)
            ]

        def colorir_status(val):
            cor = STATUS_CORES.get(val, "")
            return f"background-color: {cor}30; color: {cor}" if cor else ""

        st.dataframe(
            df_exibir.style.map(colorir_status, subset=["status_contrato"]),
            use_container_width=True,
            height=600,
            column_config={
                "unidade": "Unidade",
                "matricula": "Matrícula",
                "nome": "Nome",
                "segmento": "Segmento",
                "serie": "Série",
                "turma": "Turma",
                "responsavel": "Responsável",
                "status_contrato": "Status Contrato",
            },
        )

        csv = df_exibir.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Baixar CSV",
            csv,
            f"contratos_2027_{datetime.now().strftime('%Y%m%d')}.csv",
            "text/csv",
        )

    with tab4:
        st.subheader("Signatários Pendentes")
        st.caption("Contratos aguardando assinatura — quem já assinou e quem falta")

        # Montar tabela de signatários — apenas o contrato MAIS RECENTE por aluno
        aluno_turma_ids_cursando = set(df_alunos["id_aluno_turma"].tolist())

        # Selecionar o contrato mais recente (maior id) por aluno_turma_id
        contratos_por_aluno = {}
        for _, c in df_contratos.iterrows():
            sigs = c.get("signatarios")
            if not sigs or not isinstance(sigs, list) or len(sigs) == 0:
                continue
            if c.get("aluno_turma_id") not in aluno_turma_ids_cursando:
                continue
            if c.get("unidade") not in unidades_sel:
                continue
            seg = c.get("segmento", "")
            if seg not in segmentos_sel:
                continue
            at_id = c.get("aluno_turma_id")
            cid = c.get("id", 0)
            if at_id not in contratos_por_aluno or cid > contratos_por_aluno[at_id].get("id", 0):
                contratos_por_aluno[at_id] = c.to_dict()

        sig_rows = []
        for at_id, c in contratos_por_aluno.items():
            for s in c.get("signatarios", []):
                sig_rows.append({
                    "unidade": c.get("unidade"),
                    "matricula": c.get("matricula"),
                    "aluno": c.get("nome"),
                    "serie": c.get("serie"),
                    "signatario": s.get("tipo", "").replace("_", " ").title(),
                    "nome_signatario": s.get("nome"),
                    "assinou": "Sim" if s.get("assinou") else "Pendente",
                })

        if sig_rows:
            df_sig = pd.DataFrame(sig_rows)

            # Resumo por tipo de signatário
            st.markdown("#### Resumo por tipo de signatário")
            pivot_sig = df_sig.groupby(["signatario", "assinou"]).size().unstack(fill_value=0)
            for col in ["Sim", "Pendente"]:
                if col not in pivot_sig.columns:
                    pivot_sig[col] = 0
            pivot_sig = pivot_sig[["Sim", "Pendente"]]
            pivot_sig["Total"] = pivot_sig.sum(axis=1)
            pivot_sig["% Assinado"] = (pivot_sig["Sim"] / pivot_sig["Total"] * 100).round(1)
            st.dataframe(pivot_sig, use_container_width=True)

            # Resumo por unidade x signatário
            st.markdown("#### Pendentes por Unidade e Signatário")
            pendentes = df_sig[df_sig["assinou"] == "Pendente"]
            if not pendentes.empty:
                pivot_uni_sig = (
                    pendentes.groupby(["unidade", "signatario"])
                    .size()
                    .unstack(fill_value=0)
                )
                st.dataframe(pivot_uni_sig, use_container_width=True)

            # Lista detalhada de pendentes
            st.markdown("#### Lista de Signatários Pendentes")
            busca_sig = st.text_input("🔍 Buscar por nome do aluno ou signatário", key="busca_sig")
            df_pendentes = df_sig[df_sig["assinou"] == "Pendente"].sort_values(
                ["unidade", "serie", "aluno", "signatario"]
            )
            if busca_sig:
                df_pendentes = df_pendentes[
                    df_pendentes["aluno"].str.contains(busca_sig, case=False, na=False)
                    | df_pendentes["nome_signatario"].str.contains(busca_sig, case=False, na=False)
                ]

            st.dataframe(
                df_pendentes,
                use_container_width=True,
                height=500,
                column_config={
                    "unidade": "Unidade",
                    "matricula": "Matrícula",
                    "aluno": "Aluno",
                    "serie": "Série",
                    "signatario": "Tipo Signatário",
                    "nome_signatario": "Nome do Signatário",
                    "assinou": "Status",
                },
            )

            csv_sig = df_pendentes.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Baixar Pendentes CSV",
                csv_sig,
                f"signatarios_pendentes_{datetime.now().strftime('%Y%m%d')}.csv",
                "text/csv",
            )
        else:
            st.info("Dados de signatários não disponíveis. Execute a coleta novamente.")


if __name__ == "__main__":
    main()
