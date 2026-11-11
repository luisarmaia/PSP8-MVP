# Atrasos de voos e clima nos aeroportos brasileiros

**MVP – Pipeline de dados em nuvem** · Universidade de Brasília, Departamento de Engenharia de Produção
Autora: *[seu nome]* · Plataforma: Databricks Free Edition

> **Resultado em uma frase:** o tempo adverso na origem ou no destino aumenta o risco de atraso em 26% a 42%, mas só 7% de todos os atrasos podem ser associados diretamente a ele; dois terços acontecem com tempo bom nas duas pontas, e a taxa de atraso dobra entre a manhã e a noite mesmo sem chuva.

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

**Data da coleta, volume e hash de cada arquivo:** registrados na tabela `bronze_manifesto_coleta`, gerada pelo notebook `01_bronze` a partir dos arquivos do Volume. A persistência na nuvem está comprovada nas Figuras 1 e 2 de `evidencias.pdf`. Data do download manual: *[preencher]*.

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
                                 │   ▼ 04_analise  (respostas P1–P5, gráficos)          │
                                 └──────────────────────────────────────────────────────┘
```

**Por que a coleta é manual:** o Databricks Free Edition restringe o acesso de saída à internet a um conjunto de domínios confiáveis, o que bloqueia os portais da ANAC e do IEM. Os arquivos são baixados das fontes oficiais e enviados ao Volume sem nenhuma alteração; o notebook `01_bronze` registra fonte, URL, tamanho e hash de cada um, o que mantém a coleta rastreável.

### Modelo de dados
Esquema estrela com a **etapa de voo** como fato (`fato_voo`) e as dimensões `dim_tempo`, `dim_horario`, `dim_companhia`, `dim_aeroporto` (nos papéis de origem e destino) e `dim_condicao_meteorologica` (também nos papéis de origem e destino).

O clima de cada voo é a **última observação METAR até o horário previsto**, com no máximo 90 min de defasagem: na origem, em relação à partida prevista; no destino, em relação à chegada prevista. Sem observação na janela, a condição fica como "Sem observação" e o voo não entra nas comparações de clima.

- Diagrama e justificativa: [`catalogo/modelo_estrela.md`](catalogo/modelo_estrela.md)
- Catálogo de dados (descrição, domínio, nulos e linhagem de cada atributo): [`catalogo/catalogo_dados.md`](catalogo/catalogo_dados.md); domínio observado nos dados: Figura 9 de `evidencias.pdf`

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
5. Rode `00_setup` de novo (célula de conferência) e depois, em ordem, `01_bronze`, `02_silver`, `03_gold` e `04_analise`. Os gráficos ficam no Volume `workspace.mvp_voos.resultados`. As capturas de tela de cada etapa estão em `evidencias.pdf` (seção 8).

---

## 5. Qualidade dos dados

A qualidade foi medida atributo a atributo no notebook `02_silver` e registrada na tabela `silver_qualidade_dados` (uma linha por regra: dimensão, ocorrências, percentual e tratamento). Integridade e domínio do modelo final foram verificados no `03_gold` (tabela `gold_catalogo_dados`). Evidências: [`evidencias.pdf`](evidencias.pdf), Figuras 3 a 9.

### Voos (VRA): 1.992.831 etapas na bronze

| Dimensão | Achado | Ocorrências | Tratamento |
|---|---|---|---|
| Unicidade | Etapas duplicadas | 5 | Removidas |
| Atualidade | Etapas fora de 2024–2025 (sobras de meses vizinhos) | 33 | Removidas |
| Completude | Sem horário previsto (voos fora da grade) | 59.138 (2,97%) | Mantidas; sem atraso calculável |
| Completude | Sem horário real | 69.496 (3,49%) | Esperado: são **exatamente** os voos cancelados |
| Consistência | Voo realizado sem chegada real / cancelado com partida real | 0 / 0 | — |
| Consistência | Origem igual ao destino (retorno) | 7.979 (0,40%) | Excluídas do fato |
| Consistência | Chegada real antes da partida real | 3 | Fora das taxas |
| Acurácia | Atraso fora de [−2 h, +24 h] (erro de data) | 1.963 (0,10%) | Fora das taxas |
| Conformidade | Datas, códigos ICAO, situação e tipo de linha fora do padrão | 0 | — |
| Completude | **Código de justificativa preenchido** | **0** | Atributo descartado (obrigação revogada em 2020) |

Os horários estão todos no horário de Brasília: a duração prevista mediana é 3,92 h tanto em GRU→Manaus quanto em Manaus→GRU (Figura 3). Se os horários fossem locais, os dois sentidos difeririam em 2 h.

### Clima (METAR)

| Dimensão | Achado | Tratamento |
|---|---|---|
| Completude | **SBGL e SBRJ ausentes no primeiro download.** O problema foi detectado porque 26% dos voos ficaram sem observação; o METAR das duas estações foi baixado à parte. Depois disso, a cobertura passou a partir da lista de aeroportos, para que uma estação vazia apareça com 0% | Corrigido; após a correção, **1,4%** dos voos ficaram sem observação, espalhados entre todos os aeroportos (máximo de 2,4% em SBKP) |
| Completude | As outras 17 estações do primeiro download têm observação em ≥ 99,9% das horas de 2024–2025 (conferir SBGL, SBRJ e SBNF na tabela de cobertura) | — |
| Completude | **SBPA (Porto Alegre) sem observações em 24% das horas**: o aeroporto ficou fechado de 3 de maio a 21 de outubro de 2024 pelas enchentes no Rio Grande do Sul | Real, não é falha de dados; praticamente não houve voos no período (só 2% das etapas de SBPA ficaram sem observação) |
| Unicidade | Observações repetidas no mesmo aeroporto e horário (≈ 0,1%) | Mantida a mais severa |
| Acurácia | 2 observações com rajada > 80 kt | Mantidas (efeito desprezível) |
| Domínio | 15% das observações com algum fator adverso | Volume suficiente para as comparações |

### Aeroportos
43 dos 227 aeródromos com voos domésticos não constam da lista de aeródromos públicos da ANAC (aeródromos pequenos ou privados). Nenhum dos 20 aeroportos do escopo está nessa situação. Os 43 entram na `dim_aeroporto` como "não cadastrado".

### Resultado no modelo
- 1.600.871 etapas domésticas de passageiros; 7.243 com origem = destino removidas; **1.593.628 no fato**.
- **1.012.383 (63,5%) em escopo** (METAR nas duas pontas); 998.133 com observação válida nas duas pontas; 980.775 com atraso calculável.
- Integridade referencial: nenhuma chave do fato sem correspondência nas dimensões; `sk_voo` única (Figura 7).

---

## 6. Resultados e discussão

Todas as taxas de atraso referem-se a **chegada mais de 15 min após o previsto**, entre voos realizados. "Razão ajustada" compara voos do **mesmo aeroporto de origem, mês e faixa horária** (Mantel-Haenszel); ela separa o efeito do clima do fato de que chove mais justamente em meses e horários que já atrasam mais.

### P1 · Onde e quando o atraso se concentra
> Evidências: [`evidencias.pdf`](evidencias.pdf), Figuras 10 a 13 (atraso e cancelamento por aeroporto e por mês).

- **Aeroportos.** A taxa média dos 20 aeroportos é 15,3%. **Guarulhos (SBGR) lidera com 19,5%**; Brasília (SBBR) tem a menor, 10,8%. Os 5 aeroportos com mais atrasos (SBGR, SBSP, SBCF, SBKP, SBBR) concentram **60,3% dos atrasos com 54,9% das etapas**: a concentração existe, mas é moderada. O problema é sistêmico, não de um único aeroporto.
- **Meses.** O atraso varia mais no tempo do que entre aeroportos: de **10,7% em agosto de 2025** (estação seca) a **26,9% em dezembro de 2025** (alta temporada e verão chuvoso). O pico de cancelamentos foi em outubro de 2024 (3,4%); os dados deste MVP não explicam esse pico, que fica como ponto a investigar.
- **Horário.** A taxa sobe ao longo do dia: 8,0% na madrugada, 11,6% de manhã, 18,2% à tarde e 18,7% à noite (detalhado em P5).

### P2 · Tempo adverso na origem, no destino e em ambos
> Evidências: [`evidencias.pdf`](evidencias.pdf), Figuras 14 e 15 (atraso e cancelamento por cenário).

| Cenário | Taxa de atraso | Razão bruta | **Razão ajustada** | Ajustada (> 30 min) | Cancelamento | n |
|---|---|---|---|---|---|---|
| Bom nas duas pontas | 13,8% | 1,00 | 1,00 | 1,00 | 1,5% | 728.173 |
| Adverso só na origem | 19,4% | 1,41 | **1,29** | 1,40 | 2,1% (1,38×) | 112.886 |
| Adverso só no destino | 17,8% | 1,29 | **1,26** | 1,32 | 2,5% (1,69×) | 112.013 |
| Adverso nas duas pontas | 22,3% | 1,62 | **1,42** | 1,59 | 3,5% (2,33×) | 27.703 |

- O tempo adverso **aumenta o risco de atraso de forma clara e estatisticamente robusta**: os intervalos de confiança não se sobrepõem. Mas o efeito é moderado, de 26% a 42% a mais de risco, e não de multiplicação.
- **Parte do efeito bruto era confusão com sazonalidade e horário:** a razão cai de 1,41 para 1,29 (origem) e de 1,62 para 1,42 (ambos) quando se compara dentro do mesmo aeroporto, mês e faixa horária.
- **O efeito é maior nos atrasos longos** (razões de 1,32 a 1,59 para mais de 30 min): o clima pesa mais na cauda da distribuição do que no atraso típico.
- **Origem × destino:** o tempo adverso na origem atrasa um pouco mais, enquanto o tempo adverso no **destino cancela mais** (1,69× contra 1,38×). Isso é coerente com a operação: um destino abaixo dos mínimos de pouso leva a companhia a cancelar, e não só a esperar.
- A mediana do atraso de chegada é negativa em todos os cenários (−5 min com tempo bom): a maioria dos voos chega adiantada, o que indica folga na grade programada.

### P3 · Que condição pesa mais
> Evidências: [`evidencias.pdf`](evidencias.pdf), Figura 16 (risco por fator).

| Fator (em pelo menos uma ponta) | Razão ajustada | Taxa de atraso | Voos | Taxa quando é o único fator (n) |
|---|---|---|---|---|
| Chuva forte | **2,94** | 53,1% | 3.109 | 23,0% (61) |
| Rajada ≥ 25 kt | 2,15 | 36,5% | 9.482 | 31,7% (5.736) |
| Trovoada | 1,72 | 31,6% | 41.898 | 28,2% (27.520) |
| Visibilidade < 5 km | 1,64 | 22,5% | 59.182 | 16,8% (13.389) |
| Teto < 1.500 ft | 1,19 | 16,9% | 200.827 | 14,7% (152.132) |

- **Intensidade:** a chuva forte é o fator mais grave (risco quase 3× maior, com metade dos voos atrasada), mas é rara e quase nunca ocorre sozinha (61 casos): na prática, é o marcador de **tempestades**, em que se somam trovoada e rajada.
- **Frequência × intensidade:** a **trovoada** combina efeito alto (1,72×) com frequência relevante (42 mil voos) e mantém taxa elevada mesmo sozinha (28,2%). É o fator de maior peso operacional. O **teto baixo** é o mais frequente (201 mil voos), mas tem o menor efeito por voo (1,19×).
- Os fatores se sobrepõem (um voo pode ter vários), por isso as razões não devem ser somadas.

### P4 · Quanto do atraso pode ser associado ao clima
> Evidências: [`evidencias.pdf`](evidencias.pdf), Figura 17 (decomposição dos atrasos).

Dos **148.155 voos atrasados** em escopo:
- **67,6% aconteceram com tempo bom na origem e no destino;**
- **25,4%** aconteceram com tempo adverso, mas teriam atrasado mesmo com tempo bom (pela taxa de voos comparáveis);
- **7,0% (10.350 atrasos) são o excesso associado ao tempo adverso** no próprio aeroporto. Sem o ajuste por estratos, seriam 8,9%, ou seja, a comparação simples superestimaria o papel do clima.

**A hipótese se confirma:** o clima medido na origem e no destino explica uma fração pequena dos atrasos. Cerca de 93% deles ocorreriam com as mesmas taxas de dias de tempo bom.

**Ressalva importante:** o número mede o efeito **local e direto** do tempo nos dois aeroportos do voo. Uma tempestade em Guarulhos atrasa aeronaves que, horas depois, voam Brasília–Recife sob céu limpo; esse atraso propagado aparece aqui como "tempo bom". Os 7,0% são, portanto, um **limite inferior** do efeito total do clima na malha.

### P5 · Efeito cascata ao longo do dia
> Evidências: [`evidencias.pdf`](evidencias.pdf), Figura 18 (atraso por hora).

Mesmo com **tempo bom nas duas pontas**, a taxa de atraso sobe de **8,9% (partidas das 6h às 9h) para 17,3% (das 18h às 21h)**: quase o dobro (1,93×). A correlação entre hora e taxa, das 5h às 23h, é 0,82, e o pico é às 18h (19,2%). A curva com tempo adverso fica acima, mas segue o mesmo desenho.

O padrão é compatível com a **propagação de atrasos**: a aeronave e a tripulação de um voo da noite já fizeram várias etapas, e cada atraso se acumula. A ressalva é que o VRA não identifica a aeronave (matrícula), então não é possível seguir a rotação e separar a propagação do congestionamento nos horários de pico. O resultado é uma forte evidência, não uma prova.

### Discussão geral
1. **O atraso é sobretudo operacional e estrutural.** Dois terços dos atrasos acontecem com tempo bom nas duas pontas, e o excesso diretamente associado ao clima é de 7%. Os fatores que mais variam a taxa são o **mês** (de 10,7% a 26,9%) e a **hora do dia** (de 8,9% a 17,3% com tempo bom), não a meteorologia local.
2. **O clima importa, mas de forma concentrada:** tempestades (trovoada, chuva forte, rajada) elevam o risco de 2 a 3 vezes e respondem pelos atrasos longos e cancelamentos, sobretudo quando atingem o destino.
3. **Para o regulador e os aeroportos:** a maior alavanca está na gestão da grade no fim do dia e na alta temporada (folga entre etapas, capacidade nos horários de pico em SBGR e SBSP), e não em infraestrutura para tempo ruim, cujo ganho máximo direto é da ordem de 7% dos atrasos.
4. **Para o consumidor e o Judiciário:** a justificativa "condições meteorológicas" é plausível em voos com tempestade registrada na origem ou no destino, mas **não é a explicação típica de um atraso**. Como as companhias deixaram de informar o motivo no VRA, o cruzamento com o METAR, como feito aqui, é uma forma objetiva de verificar a alegação caso a caso.

---

## 7. Autoavaliação

**Quais perguntas foram respondidas?**
As cinco. P1, P2, P3 e P5 foram respondidas integralmente. P4 foi respondida com uma restrição explícita: mede o efeito local e direto do clima (nos dois aeroportos do voo) e não o efeito propagado pela malha, por isso o resultado é um limite inferior. Uma pergunta do plano original (comparar a justificativa declarada pela companhia com o clima medido) foi **abandonada antes da coleta**, porque o código de justificativa do VRA está vazio desde 2020; a hipótese foi reformulada para medir o clima diretamente.

**Que limitações dos dados condicionaram os resultados?**
- Ausência do motivo do atraso declarado pela companhia.
- Ausência da matrícula da aeronave no VRA, o que impede seguir a rotação e provar a propagação (P5).
- O METAR descreve o aeródromo em instantes discretos; não cobre o tempo em rota nem as restrições de fluxo do controle de tráfego aéreo (DECEA), que transmitem o efeito de uma tempestade para outros aeroportos.
- Restrição do escopo a voos entre os 20 maiores aeroportos (63,5% das etapas domésticas), para haver clima nas duas pontas.
- Fechamento de Porto Alegre por cerca de 5 meses em 2024, que altera a malha do Sul no período.

**Que decisões técnicas seriam diferentes?**
- **Verificar a cobertura a partir da lista de estações desde o início.** A falta de SBGL e SBRJ só foi percebida na junção (26% sem observação); uma conferência na camada bronze teria evitado o retrabalho.
- Definir a lista de aeroportos com os dados do próprio período (2024–2025) antes de baixar o METAR, e não com uma amostra de 2026 (SBPS × SBNF).
- Concentrar parâmetros repetidos (lista de aeroportos, limiares de tempo adverso) em um único arquivo de configuração, em vez de repeti-los nos notebooks.

**Que extensões tornariam o MVP uma solução de uso contínuo?**
- **Ingestão automática mensal** com Databricks Jobs (exige acesso de saída à internet, disponível fora do Free Edition ou com verificação de conta), e uma camada de testes de qualidade que bloqueie a carga quando uma estação ficar sem dados.
- Incorporar **restrições de fluxo do DECEA** e dados de **rotação de aeronaves** para medir o efeito propagado do clima.
- Ampliar para todos os aeroportos com METAR e adicionar previsão (TAF) para estimar o risco de atraso antes do voo.
- Painel para o consumidor consultar, por voo, se havia tempo adverso registrado na origem ou no destino.

**Viabilidade:** todo o pipeline roda no Databricks Free Edition, sem custo, com cerca de 400 MB de dados brutos e alguns minutos de processamento por execução.


---

## 8. Evidências

As capturas de tela da execução no Databricks e os gráficos dos resultados estão reunidos, com legendas, em **[`evidencias.pdf`](evidencias.pdf)**. As figuras são citadas pelo número ao longo deste documento.

| Figura | Conteúdo | Origem | Citada em |
|---|---|---|---|
| 1 | Arquivos brutos no Volume `workspace.mvp_voos.raw` | `00_setup`, célula de conferência | Seção 2 |
| 2 | Contagem de linhas das tabelas bronze | `01_bronze`, conferência | Seção 2 |
| 3 | Verificação do fuso dos horários do VRA | `02_silver` | Seção 5 |
| 4 | Conferência dos 20 aeroportos com METAR (ranking 2024–2025) | `02_silver` | Seção 5 |
| 5 | Cobertura temporal do METAR por aeroporto | `02_silver` | Seção 5 |
| 6 | Relatório de qualidade (`silver_qualidade_dados`) | `02_silver` | Seção 5 |
| 7 | Integridade referencial do modelo estrela | `03_gold`, seção 5 | Seção 5 |
| 8 | Cobertura da junção do clima e diagnóstico por aeroporto | `03_gold`, seção 6 | Seção 5 |
| 9 | Catálogo de dados com domínio observado (`gold_catalogo_dados`) | `03_gold`, seção 7 | Seção 3 |
| 10 | P1 · Taxa de atraso por aeroporto de origem | `04_analise` | Seção 6 |
| 11 | P1 · Taxa de cancelamento por aeroporto de origem | `04_analise` | Seção 6 |
| 12 | P1 · Taxa de atraso por mês | `04_analise` | Seção 6 |
| 13 | P1 · Taxa de cancelamento por mês | `04_analise` | Seção 6 |
| 14 | P2 · Taxa de atraso por cenário de clima | `04_analise` | Seção 6 |
| 15 | P2 · Taxa de cancelamento por cenário de clima | `04_analise` | Seção 6 |
| 16 | P3 · Risco de atraso por fator meteorológico | `04_analise` | Seção 6 |
| 17 | P4 · Decomposição dos atrasos | `04_analise` | Seção 6 |
| 18 | P5 · Taxa de atraso ao longo do dia | `04_analise` | Seção 6 |

---

## Estrutura do repositório
| Caminho | Conteúdo |
|---|---|
| `README.md` | Este documento |
| `notebooks/` | Notebooks Databricks: setup, bronze, silver, gold, análise |
| `catalogo/` | Catálogo de dados e diagrama do modelo |
| `evidencias.pdf` | Capturas de tela da execução na nuvem e gráficos dos resultados, com legendas (índice na seção 8) |
| `LICENSE` | Licença do código (MIT); licenças dos dados na seção 2 |
