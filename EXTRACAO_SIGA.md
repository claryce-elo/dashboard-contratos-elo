# Extração de Dados do SIGA - Dashboard de Contratos

## Visão Geral

O dashboard coleta dados de duas fontes na API do SIGA Activesoft (`siga02.activesoft.com.br`):

1. **Alunos matriculados** — endpoint `/api/v1/alunoturma/`
2. **Contratos e assinaturas** — endpoint `/api/v1/assinatura_eletronica/`
3. **Signatários por contrato** — endpoint `/api/v1/assinatura_eletronica/documento/{id}/signatarios/`

---

## Autenticação (Login em 4 etapas)

O SIGA usa autenticação por sessão com CSRF token. O login é feito em 4 passos:

### Etapa 1 — Obter CSRF Token
```
GET https://siga.activesoft.com.br/login/
```
Capturar o cookie `csrftoken` da resposta.

### Etapa 2 — Enviar credenciais
```
POST https://siga.activesoft.com.br/login/
Content-Type: application/x-www-form-urlencoded

Campos:
  codigo=<INSTITUICAO>
  login=<USUARIO>
  senha=<SENHA>
  csrfmiddlewaretoken=<csrf_da_etapa_1>

Headers:
  Referer: https://siga.activesoft.com.br/login/

allow_redirects=True
```

### Etapa 3 — Selecionar unidade
```
POST https://siga.activesoft.com.br/login/unidade/
Content-Type: application/x-www-form-urlencoded

Campos:
  unidade=<PK_DA_UNIDADE>
  csrfmiddlewaretoken=<csrf_atualizado>

Headers:
  Referer: https://siga.activesoft.com.br/login/unidade/

allow_redirects=False  (importante!)
```
Capturar o header `Location` da resposta (redirect para siga02 com token).

### Etapa 4 — Seguir redirect
```
GET <URL_do_Location>
allow_redirects=True
```
Após isso, a sessão está autenticada com cookies `auth_jwt`, `csrftoken`, `sessionid`.

### Unidades disponíveis
| PK | Sigla | Nome |
|----|-------|------|
| 1  | —     | Matriz |
| 2  | BV    | Boa Viagem |
| 3  | CD    | Candeias |
| 4  | JG    | Janga |
| 5  | CDR   | Cordeiro |

**Importante:** Cada unidade requer login separado. Os dados retornados são específicos da unidade logada.

---

## Endpoint 1: Alunos Matriculados

```
GET /api/v1/alunoturma/?periodo={PERIODO_ID}&limit=200&offset=0
```

### Parâmetros
- `periodo` (int) — ID do período letivo. Cada unidade tem um ID diferente para o mesmo ano:

| Unidade | Período 2027 |
|---------|-------------|
| BV      | 112         |
| CD      | 113         |
| JG      | 111         |
| CDR     | 110         |

- `limit` / `offset` — paginação (máx 200 por página)

### Descobrir o ID do período
```
GET /api/v1/periodo/?limit=200
```
Procurar pelo registro com `sigla` contendo "2027".

### Resposta
```json
{
  "count": 881,
  "next": "...",
  "previous": null,
  "results": [
    {
      "id_aluno_turma": 150756,
      "aluno_matricula": "1-1114",
      "aluno_nome": "Nome do Aluno",
      "aluno_responsavel_nome": "Nome do Responsável",
      "situacao_aluno_turma_nome": "Cursando",
      "periodo_sigla": "2027",
      "serie_nome": "Infantil II",
      "curso_nome": "1- BV - Educação Infantil - Unidade Boa Viagem",
      "turma_nome": "Infantil II - Turma A Manhã - Boa Viagem",
      "turno": "M",
      "data_efetivacao_matricula": "2026-08-26T00:00:00",
      "aluno": 36988
    }
  ]
}
```

### Filtro importante
O endpoint retorna TODAS as situações (Cursando, Pré-Matriculado, Cancelado, etc). Para o dashboard, filtrar client-side por:
```python
situacao_aluno_turma_nome == "Cursando"
```

**Nota:** O parâmetro `periodo_sigla=2027` NÃO funciona como filtro neste endpoint. Usar `periodo={ID}` (numérico).

---

## Endpoint 2: Contratos (Assinatura Eletrônica)

```
GET /api/v1/assinatura_eletronica/?periodo_sigla=2027&limit=200&offset=0
```

### Parâmetros
- `periodo_sigla` — funciona aqui (string "2027")
- `limit` / `offset` — paginação

### Resposta
```json
{
  "count": 1852,
  "results": [
    {
      "id": 20621,
      "aluno_turma_id": 149065,
      "aluno_id": 18009,
      "aluno_nome": "Nome do Aluno",
      "aluno_matricula": "1-7978",
      "serie_nome": "5º Ano",
      "curso_nome": "1- BV - Ensino Fundamental I - Unidade Boa Viagem",
      "turma_nome": "5º Ano - Turma B Manhã - Boa Viagem",
      "situacao_aluno_turma": "Cursando",
      "situacao_assinatura_label": "Aguardando assinatura",
      "situacao_assinatura": 2,
      "titulo": "BV - ONLINE 1 - CONDICIONADO 2027 - REGULAR",
      "data_criacao": "2026-09-01T18:56:11.080000",
      "data_assinatura_confirmada": null,
      "data_cancelamento": null,
      "contratante": true,
      "contratado": true,
      "testemunha1": true,
      "testemunha2": true,
      "responsavel_secundario": false,
      "contrato_cancelado_clicksign": true,
      "servico_externo": "clicksign"
    }
  ]
}
```

### Status de assinatura possíveis
| Código | Label | Significado |
|--------|-------|-------------|
| 2 | Aguardando assinatura | Contrato gerado, pendente de assinaturas |
| — | Assinado eletronicamente | Todas as assinaturas concluídas |
| — | Contrato cancelado | Cancelado manualmente |
| — | Contrato cancelado (tempo limite) | Expirou sem todas as assinaturas |
| — | Contrato gerado | Gerado mas ainda não enviado |
| — | Falha ao enviar | Erro no envio para Clicksign |

### Observações
- O campo `contrato_cancelado_clicksign` vem `true` para TODOS os registros — não usar como indicador de cancelamento.
- Usar `situacao_assinatura_label` para determinar o status real.
- Um mesmo aluno pode ter múltiplos contratos (cancelado + novo). Considerar apenas o mais recente (maior `id`).
- Os campos `contratante`, `contratado`, `testemunha1`, `testemunha2` indicam se o signatário EXISTE no contrato, não se assinou.

---

## Endpoint 3: Signatários por Contrato

```
GET /api/v1/assinatura_eletronica/documento/{CONTRATO_ID}/signatarios/
```

Onde `CONTRATO_ID` é o campo `id` do contrato retornado no endpoint 2.

### Resposta
```json
{
  "signatarios": [
    {
      "id": 51708,
      "documento_id": 20621,
      "tipo_signatario": "contratante",
      "nome_signatario": "Nome do Responsável",
      "email_signatario": "email@exemplo.com",
      "telefone_signatario": "81 98765-4321",
      "cpf_signatario": "123.456.789-00",
      "data_hora_assinatura": null,
      "data_hora_email_enviado": "2026-09-01T18:56:00"
    },
    {
      "tipo_signatario": "contratado",
      "nome_signatario": "Instituto Elo Nacional...",
      "data_hora_assinatura": null
    },
    {
      "tipo_signatario": "testemunha1",
      "nome_signatario": "Nome da Testemunha",
      "data_hora_assinatura": "2026-09-02T08:00:00"
    }
  ]
}
```

### Tipos de signatário
- `contratante` — responsável financeiro do aluno
- `contratado` — Instituto ELO (a escola)
- `testemunha1` — primeira testemunha
- `testemunha2` — segunda testemunha
- `responsavel_secundario` — quando existe

### Como saber se assinou
- `data_hora_assinatura` = `null` → **pendente**
- `data_hora_assinatura` = datetime → **assinado**

---

## Cruzamento dos Dados

Para montar o dashboard, o cruzamento é feito pelo campo `aluno_turma_id`:
- `alunoturma.id_aluno_turma` = `assinatura_eletronica.aluno_turma_id`

### Classificação do aluno
1. Se não tem nenhum contrato vinculado → **Sem contrato**
2. Se todos os contratos são cancelados → **Sem contrato**
3. Se algum contrato está "Assinado eletronicamente" → **Assinado**
4. Se algum contrato está "Aguardando assinatura" → **Aguardando assinatura**

### Turmas regulares vs integral
Turmas com "Integral" no nome são classificadas separadamente. O dashboard filtra por padrão apenas turmas regulares.

---

## Fluxo Completo de Coleta

```
Para cada unidade (BV, CD, JG, CDR):
  1. Login no SIGA (4 etapas)
  2. GET /api/v1/alunoturma/?periodo={id} → paginar tudo
     → filtrar client-side: situacao_aluno_turma_nome == "Cursando"
  3. GET /api/v1/assinatura_eletronica/?periodo_sigla=2027 → paginar tudo
  4. Para cada contrato "Aguardando assinatura" de aluno cursando:
     GET /api/v1/assinatura_eletronica/documento/{id}/signatarios/
  5. Salvar snapshot em JSON
```

Tempo total: ~2 minutos para as 4 unidades (~385 alunos, ~300 contratos com signatários).

---

## Configuração de Credenciais

As credenciais são lidas de:
1. **Streamlit Cloud**: `st.secrets` (configurar em Settings → Secrets)
2. **Variáveis de ambiente**: `SIGA_INSTITUICAO`, `SIGA_LOGIN`, `SIGA_SENHA`
3. **Fallback**: valores padrão no código (apenas para desenvolvimento local)

### Formato do secrets.toml (Streamlit Cloud)
```toml
SIGA_INSTITUICAO = "COLEGIOELO"
SIGA_LOGIN = "<seu_usuario>"
SIGA_SENHA = "<sua_senha>"
```
