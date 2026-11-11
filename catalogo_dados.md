# Catálogo de dados

Catálogo do modelo dimensional (camada gold) do schema `workspace.mvp_voos`. Gerado a partir do mesmo dicionário usado no notebook `03_gold`, que também:
- grava cada descrição como **comentário de coluna no Unity Catalog** (visível no Catalog Explorer);
- compara o domínio esperado com o **domínio observado** nos dados, na tabela `gold_catalogo_dados`.

**Linhagem geral:** fontes oficiais (ANAC VRA, ANAC Aeródromos, IEM METAR) → Volume `raw` → `01_bronze` (texto, sem alterar valores) → `02_silver` (tipagem, limpeza, regras R1–R10) → `03_gold` (modelo estrela, regras G1–G6). As regras estão descritas no início de cada notebook e no README.

Diagrama do modelo: [`modelo_estrela.md`](modelo_estrela.md).

## `fato_voo`

Grão: uma linha por etapa de voo doméstica de passageiros (2024–2025).

| Atributo | Tipo | Descrição | Domínio esperado | Nulos permitidos | Linhagem |
|---|---|---|---|---|---|
| `sk_voo` | texto | Chave da etapa: SHA-256 dos atributos de negócio | 64 caracteres hexadecimais | Não | silver_voo: hash (G5) |
| `sk_data` | numérico | Data da partida prevista (Brasília), AAAAMMDD → dim_tempo | 20240101 a 20251231 | Não | silver_voo.data_referencia_brt |
| `sk_hora_partida` | numérico | Hora da partida prevista (Brasília) → dim_horario | 0 a 23 | Não | silver_voo.partida_prevista_brt |
| `sk_companhia` | categórico | Código ICAO da companhia → dim_companhia | 3 letras (ex.: AZU, GLO, TAM) | Não | VRA: ICAO Empresa Aérea |
| `sk_aeroporto_origem` | categórico | Código ICAO do aeroporto de origem → dim_aeroporto | 4 caracteres | Não | VRA: ICAO Aeródromo Origem |
| `sk_aeroporto_destino` | categórico | Código ICAO do aeroporto de destino → dim_aeroporto | 4 caracteres | Não | VRA: ICAO Aeródromo Destino |
| `sk_condicao_origem` | numérico | Condição meteorológica na origem na partida prevista → dim_condicao_meteorologica | -1 a 31 | Não; -1 = sem observação | silver_metar, junção G3 |
| `sk_condicao_destino` | numérico | Condição meteorológica no destino na chegada prevista → dim_condicao_meteorologica | -1 a 31 | Não; -1 = sem observação | silver_metar, junção G3 |
| `numero_voo` | texto | Número do voo | 1 a 4 dígitos | Não | VRA: Número Voo |
| `codigo_di` | categórico | Código de autorização do voo (DI) | Dígito; 0 = voo regular | Não | VRA: Código Autorização (DI) |
| `situacao_voo` | categórico | Situação da etapa | REALIZADO, CANCELADO | Não | VRA: Situação Voo |
| `partida_prevista_brt` | data | Partida prevista, horário de Brasília | 2024–2025 | Sim: voo fora da grade prevista | VRA: Partida Prevista (R2) |
| `partida_real_brt` | data | Partida realizada, horário de Brasília | 2024–2025 (+1 dia) | Sim: voo cancelado | VRA: Partida Real (R2) |
| `chegada_prevista_brt` | data | Chegada prevista, horário de Brasília | 2024–2025 (+1 dia) | Sim: voo fora da grade prevista | VRA: Chegada Prevista (R2) |
| `chegada_real_brt` | data | Chegada realizada, horário de Brasília | 2024–2025 (+1 dia) | Sim: voo cancelado | VRA: Chegada Real (R2) |
| `partida_prevista_utc` | data | Partida prevista em UTC | Brasília + 3 h | Como a de Brasília | R3 |
| `chegada_prevista_utc` | data | Chegada prevista em UTC | Brasília + 3 h | Como a de Brasília | R3 |
| `realizado` | booleano | Etapa realizada | verdadeiro/falso | Não | situacao_voo |
| `cancelado` | booleano | Etapa cancelada | verdadeiro/falso | Não | situacao_voo |
| `atraso_partida_min` | numérico | Partida real − prevista, em minutos (negativo = adiantado) | -120 a 1440 | Sim: cancelado ou sem horário | R4 |
| `atraso_chegada_min` | numérico | Chegada real − prevista, em minutos (negativo = adiantado) | -120 a 1440 | Sim: cancelado ou sem horário | R4 |
| `atraso_valido` | booleano | Atraso de chegada calculável e plausível | verdadeiro/falso | Não | R5 |
| `atrasado_15` | booleano | Chegada com mais de 15 min de atraso | verdadeiro/falso | Sim: atraso não válido | R6 |
| `atrasado_30` | booleano | Chegada com mais de 30 min de atraso (sensibilidade) | verdadeiro/falso | Sim: atraso não válido | R6 |
| `em_escopo` | booleano | Origem e destino entre os 20 aeroportos com METAR | verdadeiro/falso | Não | G2 |
| `tempo_adverso_origem` | booleano | Tempo adverso na origem na partida prevista | verdadeiro/falso | Sim: sem observação | G3 |
| `tempo_adverso_destino` | booleano | Tempo adverso no destino na chegada prevista | verdadeiro/falso | Sim: sem observação | G3 |
| `cenario_clima` | categórico | Combinação de clima nas duas pontas | Bom nas duas pontas; Adverso só na origem; Adverso só no destino; Adverso nas duas pontas; Sem observação | Não | G3, G4 |
| `defasagem_obs_origem_min` | numérico | Minutos entre a observação usada e a partida prevista | 0 a 90 | Sim: sem observação | G3 |
| `defasagem_obs_destino_min` | numérico | Minutos entre a observação usada e a chegada prevista | 0 a 90 | Sim: sem observação | G3 |
| `visibilidade_origem_m` | numérico | Visibilidade na origem (m) | 0 a ~10.000 (9999 = 10 km ou mais) | Sim: sem observação ou não reportada | silver_metar.visibilidade_m |
| `teto_origem_ft` | numérico | Teto na origem (pés); nulo = sem camada BKN/OVC/VV | 0 a 30.000 | Sim: sem teto (céu sem cobertura significativa) | silver_metar.teto_ft |
| `rajada_origem_kt` | numérico | Rajada na origem (nós) | 15 a 80 | Sim: sem rajada reportada | silver_metar.rajada_kt |
| `fenomenos_origem` | texto | Fenômenos de tempo presente na origem (códigos METAR) | Ex.: RA, -RA, +TSRA, BR, FG, VCTS | Sim: nenhum fenômeno | silver_metar.fenomenos |
| `visibilidade_destino_m` | numérico | Visibilidade no destino (m) | 0 a ~10.000 | Sim: sem observação ou não reportada | silver_metar.visibilidade_m |
| `teto_destino_ft` | numérico | Teto no destino (pés) | 0 a 30.000 | Sim: sem teto | silver_metar.teto_ft |
| `rajada_destino_kt` | numérico | Rajada no destino (nós) | 15 a 80 | Sim: sem rajada reportada | silver_metar.rajada_kt |
| `fenomenos_destino` | texto | Fenômenos de tempo presente no destino | Códigos METAR | Sim: nenhum fenômeno | silver_metar.fenomenos |
| `arquivo_origem` | texto | Arquivo VRA de onde veio a etapa | VRA_AAAAM.csv | Não | bronze_vra._arquivo_origem |

## `dim_condicao_meteorologica`

Grão: uma linha por combinação de fatores adversos (32) + “Sem observação”.

| Atributo | Tipo | Descrição | Domínio esperado | Nulos permitidos | Linhagem |
|---|---|---|---|---|---|
| `sk_condicao` | numérico | Código da condição: soma dos pesos dos fatores adversos (1, 2, 4, 8, 16) | -1 a 31 | Não | Gerada (G4) |
| `descricao` | texto | Fatores adversos presentes | Tempo bom; combinações; Sem observação | Não | Gerada |
| `tempo_adverso` | booleano | Algum fator adverso presente | verdadeiro/falso | Sim: sem observação | Gerada |
| `fl_visibilidade_baixa` | booleano | Visibilidade < 5 km | verdadeiro/falso | Sim: sem observação | R9 |
| `fl_teto_baixo` | booleano | Teto < 1.500 ft | verdadeiro/falso | Sim: sem observação | R9 |
| `fl_trovoada` | booleano | Trovoada no aeródromo ou vizinhança (TS, VCTS) | verdadeiro/falso | Sim: sem observação | R9 |
| `fl_chuva_forte` | booleano | Chuva forte (+RA, +SHRA, +TSRA) | verdadeiro/falso | Sim: sem observação | R9 |
| `fl_rajada_forte` | booleano | Rajada ≥ 25 kt | verdadeiro/falso | Sim: sem observação | R9 |
| `qtd_fatores` | numérico | Quantidade de fatores adversos | 0 a 5 | Sim: sem observação | Gerada |

## `dim_aeroporto`

Grão: uma linha por aeródromo.

| Atributo | Tipo | Descrição | Domínio esperado | Nulos permitidos | Linhagem |
|---|---|---|---|---|---|
| `sk_aeroporto` | categórico | Código ICAO do aeródromo | 4 caracteres | Não | Aeródromos ANAC: Código OACI |
| `nome` | texto | Nome do aeródromo | Texto livre | Não | Aeródromos ANAC: Nome |
| `municipio` | texto | Município | Texto livre | Sim: não cadastrado | Aeródromos ANAC: Município |
| `uf` | categórico | Unidade da federação (nome por extenso) | 27 UFs | Sim: não cadastrado | Aeródromos ANAC: UF |
| `regiao` | categórico | Região geográfica | Norte, Nordeste, Centro-Oeste, Sudeste, Sul, Não informada | Não | Derivada da UF |
| `latitude` | numérico | Latitude (graus decimais) | -34 a 6 | Sim: não cadastrado | Aeródromos ANAC: LATGEOPOINT |
| `longitude` | numérico | Longitude (graus decimais) | -74 a -28 | Sim: não cadastrado | Aeródromos ANAC: LONGEOPOINT |
| `altitude_m` | numérico | Altitude (m) | 0 a 1.200 | Sim: não cadastrado | Aeródromos ANAC: Altitude |
| `tem_metar` | booleano | Está entre os 20 aeroportos com METAR | verdadeiro/falso | Não | Lista AEROPORTOS_METAR |

## `dim_companhia`

Grão: uma linha por companhia aérea.

| Atributo | Tipo | Descrição | Domínio esperado | Nulos permitidos | Linhagem |
|---|---|---|---|---|---|
| `sk_companhia` | categórico | Código ICAO da companhia | 3 letras | Não | VRA: ICAO Empresa Aérea |
| `nome` | texto | Nome comercial (principais companhias) | Texto; 'Não mapeado (código)' nas demais | Não | Mapeamento manual |
| `etapas_2024_2025` | numérico | Etapas da companhia no fato | ≥ 1 | Não | fato_voo |
| `participacao_pct` | numérico | Participação nas etapas do fato (%) | 0 a 100 | Não | fato_voo |

## `dim_tempo`

Grão: uma linha por dia de 2024–2025.

| Atributo | Tipo | Descrição | Domínio esperado | Nulos permitidos | Linhagem |
|---|---|---|---|---|---|
| `sk_data` | numérico | Data AAAAMMDD | 20240101 a 20251231 | Não | Gerada |
| `data` | data | Data | 2024-01-01 a 2025-12-31 | Não | Gerada |
| `ano` | numérico | Ano | 2024, 2025 | Não | Gerada |
| `trimestre` | numérico | Trimestre | 1 a 4 | Não | Gerada |
| `mes` | numérico | Mês | 1 a 12 | Não | Gerada |
| `ano_mes` | texto | Ano e mês | 2024-01 a 2025-12 | Não | Gerada |
| `dia` | numérico | Dia do mês | 1 a 31 | Não | Gerada |
| `dia_semana_num` | numérico | Dia da semana (1 = domingo) | 1 a 7 | Não | Gerada |
| `dia_semana` | categórico | Dia da semana | domingo a sábado | Não | Gerada |
| `fim_de_semana` | booleano | Sábado ou domingo | verdadeiro/falso | Não | Gerada |

## `dim_horario`

Grão: uma linha por hora do dia.

| Atributo | Tipo | Descrição | Domínio esperado | Nulos permitidos | Linhagem |
|---|---|---|---|---|---|
| `sk_hora` | numérico | Hora cheia (Brasília) | 0 a 23 | Não | Gerada |
| `faixa_horaria` | categórico | Faixa do dia | Madrugada, Manhã, Tarde, Noite | Não | Gerada |
| `ordem_faixa` | numérico | Ordem da faixa no dia | 1 a 4 | Não | Gerada |
