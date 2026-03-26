"""
Classificacao de turmas e normalizacao de status de contrato.
Logica portada do dashboard_contratos.html original.
"""

import re

# Ordem de exibicao das turmas regulares
GRADE_ORDER = [
    "Infantil II", "Infantil III", "Infantil IV", "Infantil V",
    "1o Ano", "2o Ano", "3o Ano", "4o Ano", "5o Ano",
    "6o Ano", "7o Ano", "8o Ano", "9o Ano",
    "1a Serie - Medio", "2a Serie - Medio", "3a Serie - Medio",
    "Outros",
]

STATUS_CORES = {
    "Assinado": {"bg": "#d5f5e3", "cor": "#27ae60"},
    "Aguardando": {"bg": "#fef9e7", "cor": "#f39c12"},
    "Cancelado": {"bg": "#fdedec", "cor": "#e74c3c"},
    "Sem Contrato": {"bg": "#f4ecf7", "cor": "#8e44ad"},
    "Outro": {"bg": "#eaecee", "cor": "#7f8c8d"},
}


def detectar_unidade(texto):
    """Detecta unidade a partir de texto."""
    u = (texto or "").upper()
    if "BV" in u or "BOA VIAGEM" in u:
        return "BV"
    if ("CD" in u and "CDR" not in u) or "JABOAT" in u:
        return "CD"
    if "CDR" in u or "CORDEIRO" in u:
        return "CDR"
    if "JG" in u or "JANGA" in u or "PAULISTA" in u:
        return "JG"
    return "Outras"


def classificar_turma(turma):
    """Classifica turma em categoria e grupo.

    Retorna dict com 'cat' (regular/integral/extra) e 'grupo'.
    """
    tu = turma.upper()
    # Remover acentos para comparacao
    import unicodedata
    tu_norm = unicodedata.normalize("NFD", tu)
    tu_norm = re.sub(r"[\u0300-\u036f]", "", tu_norm)

    # Extracurriculares (esportes)
    esportes = [
        (["BALLET"], "Ballet"),
        (["BASQUETEBOL"], "Basquetebol"),
        (["FUTSAL"], "Futsal"),
        (["GINASTICA ART"], "Ginastica Artistica"),
        (["GINASTICA RIT"], "Ginastica Ritmica"),
        (["JUDO"], "Judo"),
        (["NATACAO", "NATAC"], "Natacao"),
        (["VOLEIBOL"], "Voleibol"),
    ]
    for keys, nome in esportes:
        if any(k in tu_norm for k in keys):
            return {"cat": "extra", "grupo": nome}

    if re.match(r"^SUB \d", tu):
        return {"cat": "extra", "grupo": "Futebol (Sub)"}

    # Integral / Complementar
    if "INTEGRAL" in tu_norm or "COMPLEMENTAR" in tu_norm:
        return {"cat": "integral", "grupo": "Integral/Complementar"}
    if re.match(r"^CD[\s-]", tu) or re.match(r"^CDR\s*-\s*INTEGRAL", tu):
        return {"cat": "integral", "grupo": "Integral/Complementar"}

    # Esportes gerais
    if re.match(r"^\d[MNT]\s", tu):
        return {"cat": "extra", "grupo": "Esportes Gerais"}

    # Ensino Medio
    sm = re.search(r"(\d)\s*[A ]*SERIE", tu_norm)
    if sm:
        return {"cat": "regular", "grupo": f"{sm.group(1)}a Serie - Medio"}

    # Infantil
    im = re.search(r"INFANTIL\s*(II|III|IV|V|2|3|4|5)\b", tu)
    if im:
        m = {"2": "II", "3": "III", "4": "IV", "5": "V",
             "II": "II", "III": "III", "IV": "IV", "V": "V"}
        return {"cat": "regular", "grupo": f"Infantil {m.get(im.group(1), im.group(1))}"}

    # Fundamental
    am = re.search(r"(\d)\s*[Ooºª ]*ANO", tu_norm)
    if am:
        return {"cat": "regular", "grupo": f"{am.group(1)}o Ano"}

    return {"cat": "regular", "grupo": "Outros"}


def normalizar_status(status):
    """Normaliza string de status do contrato."""
    s = (status or "").lower()
    if "assinado" in s:
        return "Assinado"
    if "cancelad" in s:
        return "Cancelado"
    if "aguardando" in s:
        return "Aguardando"
    if "sem contrato" in s:
        return "Sem Contrato"
    return "Outro"


def is_test_student(nome):
    return bool(re.search(r"aluno\s*teste", nome, re.IGNORECASE))


def processar_csv_contratos(rows):
    """Processa linhas de CSV de contratos."""
    alunos = []
    for row in rows:
        # Detectar formato pelo campo
        if "status_contrato" in row:
            # Formato pre-processado
            turma = row.get("turma", "")
            matricula = row.get("matricula", "")
            nome = row.get("nome", "")
            situacao = row.get("situacao", "")
            unidade = row.get("unidade", "Outras")
            status = row.get("status_contrato", "Outro")
        else:
            # Formato SIGA original
            turma = row.get("Turma", "")
            matricula = row.get("Matricula", row.get("Matrícula", ""))
            nome = row.get("Nome do Aluno", "")
            situacao = row.get("Situacao na Turma", row.get("Situação na Turma", ""))
            unidade = detectar_unidade(row.get("Unidade", ""))
            status = normalizar_status(row.get("Status do Contrato", ""))

        if not nome or is_test_student(nome):
            continue

        cls = classificar_turma(turma)
        alunos.append({
            "turma": turma,
            "matricula": matricula,
            "nome": nome,
            "situacao": situacao,
            "unidade": unidade,
            "status_contrato": status,
            "categoria": cls["cat"],
            "grupo": cls["grupo"],
        })
    return alunos


def processar_csv_alunos(rows, alunos_existentes):
    """Processa CSV de alunos e adiciona os que nao tem contrato."""
    existentes = {a["matricula"] for a in alunos_existentes if a.get("matricula")}
    novos = []

    for row in rows:
        turma = row.get("Turma", row.get("turma", ""))
        matricula = row.get("Matricula", row.get("Matrícula", row.get("matricula", "")))
        nome = row.get("Nome do Aluno", row.get("nome", ""))
        situacao = row.get("Situacao na Turma", row.get("Situação na Turma", row.get("situacao", "")))
        unidade = detectar_unidade(row.get("Unidade", row.get("unidade", "")))

        if not matricula or matricula in existentes:
            continue
        if not nome or is_test_student(nome):
            continue

        cls = classificar_turma(turma)
        novos.append({
            "turma": turma,
            "matricula": matricula,
            "nome": nome,
            "situacao": situacao,
            "unidade": unidade,
            "status_contrato": "Sem Contrato",
            "categoria": cls["cat"],
            "grupo": cls["grupo"],
        })
        existentes.add(matricula)

    return novos
