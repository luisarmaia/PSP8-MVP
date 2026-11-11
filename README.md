# Atrasos de voos e clima nos aeroportos brasileiros

**MVP – Pipeline de dados em nuvem** · Universidade de Brasília, Departamento de Engenharia de Produção
Autora: *[seu nome]* · Plataforma: Databricks Free Edition

> Status: **objetivo, coleta, bronze e silver concluídos; camada gold em validação**. As seções marcadas com 🚧 serão preenchidas nas próximas etapas.

---

## 1. Objetivo

### Problema
Atrasos e cancelamentos geram custo para passageiros, companhias aéreas e para o Judiciário. O "mau tempo" é uma das justificativas mais citadas para atrasos e costuma afastar a responsabilidade da companhia. Mas não se sabe quanto do atraso nos principais aeroportos brasileiros está de fato associado a condições meteorológicas medidas, e quanto ocorre com tempo bom e decorre de fatores operacionais ou de infraestrutura.

### Hipótese
Os atrasos se concentram em poucos aeroportos e faixas horárias, e as condições meteorológicas adversas medidas na **origem e no destino** explicam apenas uma parte deles: uma parcela relevante dos atrasos ocorre com tempo bom nos dois aeroportos.

### Decisões que o MVP apoia
- **Regulador e operadores de aeroporto:** onde e quando priorizar ações de capacidade e infraestrutura.
- **Consumidor e Judiciário:** quanto pesa o clima na pontualidade, uma referência para avaliar justificativas meteorológicas.

### Perguntas de negócio
| # | Pergunta |
|---|---|
| P1 | Quais aeroportos, faixas horárias e meses concentram as maiores taxas de atraso na chegada e de cancelamento? |
| P2 | Quanto aumentam as taxas de atraso e de cancelamento quando há tempo adverso **na origem** (no horário previsto de partida), **no destino** (no horário previsto de chegada) e **em ambos**, em comparação com tempo bom nos dois? |
| P3 | Que tipo de condição pesa mais: baixa visibilidade ou teto baixo, trovoada, chuva forte, ou vento e rajadas? |
| P4 | Que parcela dos atrasos pode ser associada ao tempo adverso (excesso em relação à taxa sob tempo bom), e quanto do atraso ocorre com tempo bom na origem e no destino? |
| P5 | A taxa de atraso cresce ao longo do dia mesmo com tempo bom, o que indicaria efeito cascata? |

### Definições
| Termo | Definição |
|---|---|
| Voo atrasado | Chegada real mais de **15 min** após a chegada prevista (padrão internacional). Análise de sensibilidade com **30 min** (critério da ANAC). |
| Tempo adverso | Em pelo menos uma observação METAR aplicável: visibilidade < 5 km, **ou** teto (BKN/OVC) < 1.500 pés, **ou** trovoada (TS), **ou** chuva forte (+RA, +SHRA, +TSRA), **ou** rajada ≥ 25 kt. |
| Observação aplicável | Último METAR/SPECI do aeroporto até o horário previsto (partida na origem, chegada no destino), com no máximo 90 min de defasagem. |
| Escopo | Etapas **domésticas de passageiros** (`Código Tipo Linha = N`) de **2024 e 2025** cuja origem **e** destino estão entre os 20 aeroportos com mais etapas domésticas, para haver METAR nas duas pontas. |

> Os limiares de tempo adverso partem dos mínimos de voo visual (5 km / 1.500 pés) e serão revisados na análise de qualidade, se necessário. Qualquer mudança será registrada aqui.

---

## 2. Fontes de dados

| Fonte | Publicador | Conteúdo | Formato | Licença / termos |
|---|---|---|---|---|
| **VRA – Voo Regular Ativo** | ANAC | Uma linha por etapa de voo: companhia, origem, destino, horários previstos e realizados, situação | CSV mensal, `;`, UTF-8, linha 1 = `Atualizado em` | Dados abertos governamentais (*licença a confirmar no dados.gov.br*) |
| **Aeródromos públicos** | ANAC | Cadastro de aeródromos: código OACI, nome, município, UF, coordenadas | CSV, `;`, Latin-1 | Idem |
| **METAR/SPECI** | Iowa Environmental Mesonet (Iowa State University), a partir de boletins do DECEA/REDEMET | Observações meteorológicas de aeroporto: visibilidade, nuvens, fenômenos, vento, rajada | CSV por aeroporto e ano, horários em UTC | Arquivo público sem restrição de acesso |

Portal da ANAC: <https://sistemas.anac.gov.br/dadosabertos/> → *Voos e operações aéreas* → *Voo Regular Ativo (VRA)*.
Arquivo METAR: <https://mesonet.agron.iastate.edu/request/download.phtml>.

**Data da coleta, volume e hash de cada arquivo:** registrados na tabela `bronze_manifesto_coleta`, gerada pelo notebook `01_bronze` a partir dos arquivos do Volume. Data do download manual: *[preencher]*.

### Achados que moldaram o escopo
- **O VRA não traz mais o motivo do atraso.** A coluna `Código Justificativa` existe, mas está vazia em 100% das linhas verificadas (jul/2026). A obrigação de informar o motivo (IAC 1504) foi revogada em 2020. Por isso o trabalho **mede** o clima em vez de usar a justificativa declarada.
- **Horários do VRA estão todos no horário de Brasília**, inclusive em aeroportos de outros fusos. Evidência: o tempo previsto GRU→Manaus é 3,9 h, que é a duração real do voo e só é coerente se a chegada estiver no horário de Brasília. Conversão para UTC: **+3 h** (o Brasil não adota horário de verão desde 2019).
- **Restringir às duas pontas reduz a amostra:** com os 10 maiores aeroportos, só 35% das etapas domésticas têm origem e destino no conjunto; com os **20 maiores, 65%**. Por isso o escopo usa 20.

---

## 3. Arquitetura

```
 Fontes (download manual)             Databricks Free Edition (Unity Catalog)
┌──────────────────────┐ upload  ┌──────────────────────────────────────────────────────┐
│ ANAC VRA (24 CSVs)   │ ──────▶ │ Volume workspace.mvp_voos.raw                        │
│ ANAC Aeródromos      │         │   │                                                  │
│ IEM METAR (2 CSVs)   │         │   ▼ 01_bronze   (texto, sem alterar valores)         │
└──────────────────────┘         │ bronze_vra · bronze_metar · bronze_aerodromos        │
                                 │ bronze_manifesto_coleta (linhagem)                   │
                                 │   ▼ 02_silver   (tipagem, limpeza, UTC, qualidade)   │
                                 │   ▼ 03_gold     (modelo estrela + clima O/D, catálogo)│
                                 │   ▼ 04_analise  (respostas P1–P5)             🚧     │
                                 └──────────────────────────────────────────────────────┘
```

**Por que a coleta é manual:** o Databricks Free Edition restringe o acesso de saída à internet a um conjunto de domínios confiáveis, o que bloqueia os portais da ANAC e do IEM. Os arquivos são baixados das fontes oficiais e enviados ao Volume sem nenhuma alteração; o notebook `01_bronze` registra fonte, URL, tamanho e hash de cada um, o que mantém a coleta rastreável.

### Modelo de dados
Esquema estrela com a **etapa de voo** como fato (`fato_voo`) e as dimensões `dim_tempo`, `dim_horario`, `dim_companhia`, `dim_aeroporto` (nos papéis de origem e destino) e `dim_condicao_meteorologica` (também nos papéis de origem e destino).

O clima de cada voo é a **última observação METAR até o horário previsto**, com no máximo 90 min de defasagem: na origem, em relação à partida prevista; no destino, em relação à chegada prevista. Sem observação na janela, a condição fica como "Sem observação" e o voo não entra nas comparações de clima.

- Diagrama e justificativa: [`catalogo/modelo_estrela.md`](catalogo/modelo_estrela.md)
- Catálogo de dados (descrição, domínio, nulos e linhagem de cada atributo): [`catalogo/catalogo_dados.md`](catalogo/catalogo_dados.md)

---

## 4. Como reproduzir

### 4.1 Coleta (download manual)
**VRA – 24 arquivos.** Em <https://sistemas.anac.gov.br/dadosabertos/> → *Voos e operações aéreas* → *Voo Regular Ativo (VRA)* → `2024` e `2025` → pasta de cada mês → `VRA_<ano><mês>.csv` (ex.: `VRA_20241.csv`). Baixe só o `.csv`.

**Aeródromos – 1 arquivo.** No mesmo portal → *Aerodromos* → *Aeródromos Públicos* → *Lista de aeródromos públicos* → arquivo `.csv`.

**METAR – 2 arquivos.** Em <https://mesonet.agron.iastate.edu/request/download.phtml?network=BR__ASOS>, uma vez para cada ano:

| Opção do formulário | Valor |
|---|---|
| Estações | SBGR, SBSP, SBBR, SBCF, SBKP, SBGL, SBRF, SBSV, SBRJ, SBPA, SBCT, SBFZ, SBBE, SBFL, SBGO, SBEG, SBVT, SBNF, SBMO, SBCY |
| Dados | *All Available* |
| Período | 2023-12-31 a 2025-01-01 (arquivo 2024) · 2024-12-31 a 2026-01-01 (arquivo 2025) |
| Fuso horário | UTC |
| Formato | *Comma Delimited (No DEBUG headers)*, com latitude/longitude e elevação |
| Valores ausentes | vazio (*blank/empty string*) |
| Tipos de relatório | *Routine* e *Specials* |
| Saída | salvar em arquivo |

Renomeie para `metar_2024.csv` e `metar_2025.csv`. O dia extra em cada ponta cobre voos perto da meia-noite e a conversão de horário de Brasília para UTC; as duplicidades entre os dois arquivos são removidas na camada silver.

Os 20 aeroportos são os de maior número de etapas domésticas de passageiros em 2024–2025, conferidos no notebook `02_silver`. A primeira lista, tirada da amostra de jul/2026, trazia SBPS (Porto Seguro) no lugar de SBNF (Navegantes); SBNF foi baixado à parte e SBPS ficou fora do escopo.

### 4.2 Databricks
1. Crie a conta em <https://www.databricks.com/learn/free-edition>.
2. **Workspace → Create → Git folder** e cole a URL deste repositório.
3. Rode `notebooks/00_setup` para criar o schema `mvp_voos` e o Volume `raw`.
4. **Catalog → workspace → mvp_voos → raw**: crie as pastas `vra`, `metar` e `aerodromos` (*Create directory*) e envie os arquivos para cada uma (*Upload to this volume*). Não renomeie os arquivos da ANAC: o notebook os encontra pelo nome.
5. Rode `00_setup` de novo (célula de conferência), tire o print para `evidencias/` e rode, em ordem, `01_bronze`, `02_silver` e `03_gold`.

---

## 5. Qualidade dos dados 🚧
## 6. Resultados e discussão 🚧
## 7. Autoavaliação 🚧

---

## Estrutura do repositório
| Caminho | Conteúdo |
|---|---|
| `README.md` | Este documento |
| `notebooks/` | Notebooks Databricks: setup, bronze, silver, gold, análise |
| `catalogo/` | Catálogo de dados e diagrama do modelo |
| `evidencias/` | Capturas de tela da execução na nuvem 🚧 |
| `LICENSE` | Licença do código (MIT); licenças dos dados na seção 2 |
