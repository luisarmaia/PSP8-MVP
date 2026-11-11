# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Camada gold: modelo estrela e clima na origem e no destino
# MAGIC
# MAGIC Monta o modelo dimensional usado nas análises. A etapa de voo é o fato; o clima entra
# MAGIC como uma dimensão de **condição meteorológica** usada em dois papéis: na **origem**, no
# MAGIC horário previsto de partida, e no **destino**, no horário previsto de chegada.
# MAGIC
# MAGIC ```
# MAGIC                dim_tempo      dim_horario
# MAGIC                     \           /
# MAGIC  dim_aeroporto ──(origem)  fato_voo  (destino)── dim_aeroporto
# MAGIC                     /           \
# MAGIC  dim_condicao_meteorologica      dim_companhia
# MAGIC   (papéis: origem e destino)
# MAGIC ```
# MAGIC
# MAGIC ### Regras de negócio
# MAGIC | # | Regra | Motivo |
# MAGIC |---|---|---|
# MAGIC | G1 | Fato = etapas **domésticas de passageiros** (`codigo_tipo_linha = 'N'`), 2024–2025, origem ≠ destino | Escopo do objetivo; origem = destino indica retorno, sem trecho real |
# MAGIC | G2 | `em_escopo` = origem **e** destino entre os 20 aeroportos com METAR | Só assim há clima nas duas pontas |
# MAGIC | G3 | Clima na origem = último METAR do aeroporto de origem **até** a partida prevista (UTC), com no máximo **90 min** de defasagem; no destino, o mesmo em relação à chegada prevista | Representa a condição conhecida no momento da operação; o limite evita usar observação antiga quando a estação falhou |
# MAGIC | G4 | Sem observação válida na janela → condição `-1` ("Sem observação") | Ausência explícita, não confundida com tempo bom |
# MAGIC | G5 | Duplicatas semânticas (mesmos valores após a conversão de datas) são removidas | Complementa a R7 da silver |
# MAGIC | G6 | Data e hora de referência = partida prevista no **horário de Brasília** | É como o passageiro e a ANAC enxergam o voo |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
import pandas as pd
import unicodedata

CATALOGO, SCHEMA = "workspace", "mvp_voos"
spark.conf.set("spark.sql.session.timeZone", "UTC")

AEROPORTOS_METAR = ["SBGR", "SBSP", "SBBR", "SBCF", "SBKP", "SBGL", "SBRF", "SBSV", "SBRJ", "SBPA",
                    "SBCT", "SBFZ", "SBBE", "SBFL", "SBGO", "SBEG", "SBVT", "SBPS", "SBMO", "SBCY"]
JANELA_MIN = 90

FLAGS = [  # (coluna em silver_metar, peso no código da condição, rótulo)
    ("fl_visibilidade_baixa", 1, "Visibilidade < 5 km"),
    ("fl_teto_baixo", 2, "Teto < 1.500 ft"),
    ("fl_trovoada", 4, "Trovoada"),
    ("fl_chuva_forte", 8, "Chuva forte"),
    ("fl_rajada_forte", 16, "Rajada ≥ 25 kt"),
]


def tabela(nome):
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def gravar(df, nome):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela(nome))
    print(f"{tabela(nome)}: {spark.table(tabela(nome)).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Etapas no escopo (G1, G5)

# COMMAND ----------

CHAVE = ["empresa_icao", "numero_voo", "codigo_di", "origem_icao", "destino_icao",
         "partida_prevista_brt", "partida_real_brt", "chegada_prevista_brt", "chegada_real_brt",
         "situacao_voo"]

voo_n = spark.table(tabela("silver_voo")).filter("codigo_tipo_linha = 'N'")
n_dom = voo_n.count()
voo_n = voo_n.filter("origem_icao <> destino_icao")
n_sem_retorno = voo_n.count()

voos = (voo_n
    .withColumn("sk_voo", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                                                    for c in CHAVE]), 256))
    .dropDuplicates(["sk_voo"])
    .withColumn("em_escopo", F.col("origem_icao").isin(AEROPORTOS_METAR)
                & F.col("destino_icao").isin(AEROPORTOS_METAR)))
voos.cache()
n_voos = voos.count()
n_escopo = voos.filter("em_escopo").count()
print(f"Etapas domésticas de passageiros:   {n_dom:,}")
print(f"  origem = destino (removidas):     {n_dom - n_sem_retorno:,}")
print(f"  duplicatas semânticas (removidas): {n_sem_retorno - n_voos:,}")
print(f"  no fato:                           {n_voos:,}")
print(f"  em escopo (clima nas 2 pontas):    {n_escopo:,} ({100 * n_escopo / n_voos:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Junção do clima (G3, G4)
# MAGIC Junção "as-of": para cada voo, a observação mais recente dentro da janela de 90 min.
# MAGIC Para evitar comparar cada voo com todas as observações do aeroporto, a junção usa como
# MAGIC chave o aeroporto e a **hora cheia**: cada voo procura nas 3 horas cheias que cobrem a
# MAGIC janela (a hora do evento e as 2 anteriores).

# COMMAND ----------

codigo_condicao = sum(F.col(c).cast("int") * peso for c, peso, _ in FLAGS)

metar = (spark.table(tabela("silver_metar"))
    .filter(F.col("aeroporto_icao").isin(AEROPORTOS_METAR))
    .select("aeroporto_icao", "observacao_utc", "visibilidade_m", "teto_ft", "rajada_kt",
            "fenomenos", codigo_condicao.alias("sk_condicao"))
    .withColumn("hora_cheia", F.date_trunc("hour", "observacao_utc")))


def clima_em(papel, col_aeroporto, col_instante):
    """Condição meteorológica no aeroporto `col_aeroporto` no instante `col_instante` (UTC)."""
    eventos = (voos.select("sk_voo", F.col(col_aeroporto).alias("aeroporto_icao"),
                           F.col(col_instante).alias("instante"))
               .filter(F.col("instante").isNotNull() & F.col("aeroporto_icao").isin(AEROPORTOS_METAR))
               .withColumn("hora_cheia", F.explode(F.array(*[
                   F.date_trunc("hour", F.col("instante") - F.expr(f"INTERVAL {h} HOURS")) for h in (0, 1, 2)]))))
    candidatos = (eventos.join(metar, ["aeroporto_icao", "hora_cheia"])
        .filter((F.col("observacao_utc") <= F.col("instante"))
                & (F.col("observacao_utc") >= F.col("instante") - F.expr(f"INTERVAL {JANELA_MIN} MINUTES"))))
    ultima = Window.partitionBy("sk_voo").orderBy(F.desc("observacao_utc"))
    return (candidatos.withColumn("_n", F.row_number().over(ultima)).filter("_n = 1")
        .select("sk_voo",
                F.col("sk_condicao").alias(f"sk_condicao_{papel}"),
                ((F.unix_timestamp("instante") - F.unix_timestamp("observacao_utc")) / 60)
                    .cast("int").alias(f"defasagem_obs_{papel}_min"),
                F.col("visibilidade_m").alias(f"visibilidade_{papel}_m"),
                F.col("teto_ft").alias(f"teto_{papel}_ft"),
                F.col("rajada_kt").alias(f"rajada_{papel}_kt"),
                F.col("fenomenos").alias(f"fenomenos_{papel}")))


clima_origem = clima_em("origem", "origem_icao", "partida_prevista_utc")
clima_destino = clima_em("destino", "destino_icao", "chegada_prevista_utc")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fato

# COMMAND ----------

adverso_o = F.col("sk_condicao_origem") > 0
adverso_d = F.col("sk_condicao_destino") > 0

fato_voo = (voos
    .join(clima_origem, "sk_voo", "left")
    .join(clima_destino, "sk_voo", "left")
    .fillna({"sk_condicao_origem": -1, "sk_condicao_destino": -1})
    .withColumn("tempo_adverso_origem", F.when(F.col("sk_condicao_origem") >= 0, adverso_o))
    .withColumn("tempo_adverso_destino", F.when(F.col("sk_condicao_destino") >= 0, adverso_d))
    .withColumn("cenario_clima", F.when((F.col("sk_condicao_origem") < 0) | (F.col("sk_condicao_destino") < 0), "Sem observação")
        .when(adverso_o & adverso_d, "Adverso nas duas pontas")
        .when(adverso_o, "Adverso só na origem")
        .when(adverso_d, "Adverso só no destino")
        .otherwise("Bom nas duas pontas"))
    .select(
        "sk_voo",
        F.date_format("data_referencia_brt", "yyyyMMdd").cast("int").alias("sk_data"),
        F.hour(F.coalesce("partida_prevista_brt", "partida_real_brt")).alias("sk_hora_partida"),
        F.col("empresa_icao").alias("sk_companhia"),
        F.col("origem_icao").alias("sk_aeroporto_origem"),
        F.col("destino_icao").alias("sk_aeroporto_destino"),
        "sk_condicao_origem", "sk_condicao_destino",
        "numero_voo", "codigo_di", "situacao_voo",
        "partida_prevista_brt", "partida_real_brt", "chegada_prevista_brt", "chegada_real_brt",
        "partida_prevista_utc", "chegada_prevista_utc",
        "realizado", "cancelado",
        "atraso_partida_min", "atraso_chegada_min", "atraso_valido", "atrasado_15", "atrasado_30",
        "em_escopo", "tempo_adverso_origem", "tempo_adverso_destino", "cenario_clima",
        "defasagem_obs_origem_min", "defasagem_obs_destino_min",
        "visibilidade_origem_m", "teto_origem_ft", "rajada_origem_kt", "fenomenos_origem",
        "visibilidade_destino_m", "teto_destino_ft", "rajada_destino_kt", "fenomenos_destino",
        F.col("_arquivo_origem").alias("arquivo_origem"),
    ))
gravar(fato_voo, "fato_voo")
voos.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensões

# COMMAND ----------

fato = spark.table(tabela("fato_voo"))

# --- dim_condicao_meteorologica: todas as 32 combinações + "Sem observação"
linhas = [{"sk_condicao": -1, "descricao": "Sem observação", "tempo_adverso": None,
           **{c: None for c, _, _ in FLAGS}, "qtd_fatores": None}]
for codigo in range(32):
    ativos = [(c, rotulo) for c, peso, rotulo in FLAGS if codigo & peso]
    linhas.append({"sk_condicao": codigo,
                   "descricao": " + ".join(r for _, r in ativos) or "Tempo bom",
                   "tempo_adverso": codigo > 0,
                   **{c: bool(codigo & peso) for c, peso, _ in FLAGS},
                   "qtd_fatores": len(ativos)})
esquema_condicao = ("sk_condicao int, descricao string, tempo_adverso boolean, "
                    + ", ".join(f"{c} boolean" for c, _, _ in FLAGS) + ", qtd_fatores int")
colunas_condicao = [c.split()[0] for c in esquema_condicao.split(", ")]
gravar(spark.createDataFrame([tuple(l[c] for c in colunas_condicao) for l in linhas], esquema_condicao),
       "dim_condicao_meteorologica")

# --- dim_tempo (calendário 2024–2025)
DIAS = {1: "domingo", 2: "segunda", 3: "terça", 4: "quarta", 5: "quinta", 6: "sexta", 7: "sábado"}
dias = F.create_map(*[x for k, v in DIAS.items() for x in (F.lit(k), F.lit(v))])
dim_tempo = (spark.sql("SELECT explode(sequence(DATE'2024-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS data")
    .select(F.date_format("data", "yyyyMMdd").cast("int").alias("sk_data"), "data",
            F.year("data").alias("ano"), F.quarter("data").alias("trimestre"),
            F.month("data").alias("mes"), F.date_format("data", "yyyy-MM").alias("ano_mes"),
            F.dayofmonth("data").alias("dia"), F.dayofweek("data").alias("dia_semana_num"),
            dias[F.dayofweek("data")].alias("dia_semana"),
            F.dayofweek("data").isin(1, 7).alias("fim_de_semana")))
gravar(dim_tempo, "dim_tempo")

# --- dim_horario (hora da partida prevista, horário de Brasília)
dim_horario = (spark.range(24).select(F.col("id").cast("int").alias("sk_hora"))
    .withColumn("faixa_horaria", F.when(F.col("sk_hora") < 6, "Madrugada (0h–5h)")
                .when(F.col("sk_hora") < 12, "Manhã (6h–11h)")
                .when(F.col("sk_hora") < 18, "Tarde (12h–17h)")
                .otherwise("Noite (18h–23h)"))
    .withColumn("ordem_faixa", (F.col("sk_hora") / 6).cast("int") + 1))
gravar(dim_horario, "dim_horario")

# --- dim_companhia (nomes das principais; demais ficam com o código)
NOMES = {"AZU": "Azul", "GLO": "Gol", "TAM": "LATAM Brasil", "PTB": "VoePass (Passaredo)",
         "ACN": "Azul Conecta"}
companhias = fato.groupBy(F.col("sk_companhia")).agg(F.count("*").alias("etapas_2024_2025")).toPandas()
companhias["nome"] = companhias["sk_companhia"].map(NOMES).fillna("Não mapeado (" + companhias["sk_companhia"] + ")")
companhias["participacao_pct"] = (100 * companhias["etapas_2024_2025"] / companhias["etapas_2024_2025"].sum()).round(2)
gravar(spark.createDataFrame(companhias[["sk_companhia", "nome", "etapas_2024_2025", "participacao_pct"]]),
       "dim_companhia")

# --- dim_aeroporto (cadastro ANAC + aeroportos do fato sem cadastro)
REGIOES = {
    "Norte": ["Acre", "Amapá", "Amazonas", "Pará", "Rondônia", "Roraima", "Tocantins"],
    "Nordeste": ["Alagoas", "Bahia", "Ceará", "Maranhão", "Paraíba", "Pernambuco", "Piauí",
                 "Rio Grande do Norte", "Sergipe"],
    "Centro-Oeste": ["Distrito Federal", "Goiás", "Mato Grosso", "Mato Grosso do Sul"],
    "Sudeste": ["Espírito Santo", "Minas Gerais", "Rio de Janeiro", "São Paulo"],
    "Sul": ["Paraná", "Rio Grande do Sul", "Santa Catarina"],
}


def chave(texto):
    if texto is None:
        return ""
    return "".join(c for c in unicodedata.normalize("NFKD", str(texto)) if not unicodedata.combining(c)).casefold().strip()


uf_regiao = {chave(uf): r for r, ufs in REGIOES.items() for uf in ufs}
aero = spark.table(tabela("silver_aeroporto")).toPandas()
usados = (fato.select(F.explode(F.array("sk_aeroporto_origem", "sk_aeroporto_destino")).alias("aeroporto_icao"))
          .distinct().toPandas())
faltantes = usados[~usados["aeroporto_icao"].isin(aero["aeroporto_icao"])].assign(nome="Não cadastrado na lista da ANAC")
aero = pd.concat([aero, faltantes], ignore_index=True)
aero["regiao"] = aero["uf"].map(lambda u: uf_regiao.get(chave(u), "Não informada"))
aero["tem_metar"] = aero["aeroporto_icao"].isin(AEROPORTOS_METAR)
dim_aeroporto = aero.rename(columns={"aeroporto_icao": "sk_aeroporto"})[
    ["sk_aeroporto", "nome", "municipio", "uf", "regiao", "latitude", "longitude", "altitude_m", "tem_metar"]]
gravar(spark.createDataFrame(dim_aeroporto.astype(object).where(pd.notnull(dim_aeroporto), None),
                             schema="sk_aeroporto string, nome string, municipio string, uf string, regiao string, "
                                    "latitude double, longitude double, altitude_m double, tem_metar boolean"),
       "dim_aeroporto")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Integridade do modelo
# MAGIC Toda chave do fato precisa existir na dimensão correspondente (integridade referencial),
# MAGIC e `sk_voo` precisa ser única.

# COMMAND ----------

checagens = {
    "sk_voo único": fato.count() - fato.select("sk_voo").distinct().count(),
    "sk_data sem dim_tempo": fato.join(spark.table(tabela("dim_tempo")), "sk_data", "left_anti").count(),
    "sk_hora_partida sem dim_horario": fato.join(spark.table(tabela("dim_horario")),
        fato.sk_hora_partida == F.col("sk_hora"), "left_anti").count(),
    "sk_companhia sem dim_companhia": fato.join(spark.table(tabela("dim_companhia")), "sk_companhia", "left_anti").count(),
}
dim_a = spark.table(tabela("dim_aeroporto"))
dim_c = spark.table(tabela("dim_condicao_meteorologica"))
for papel in ("origem", "destino"):
    checagens[f"aeroporto_{papel} sem dim_aeroporto"] = fato.join(
        dim_a, fato[f"sk_aeroporto_{papel}"] == dim_a.sk_aeroporto, "left_anti").count()
    checagens[f"condicao_{papel} sem dim_condicao"] = fato.join(
        dim_c, fato[f"sk_condicao_{papel}"] == dim_c.sk_condicao, "left_anti").count()
for regra, n in checagens.items():
    print(f"{'OK  ' if n == 0 else 'ERRO'} {regra}: {n}")
assert all(n == 0 for n in checagens.values()), "Falha de integridade no modelo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Cobertura da junção do clima
# MAGIC Quantos voos em escopo receberam observação nas duas pontas, e como se distribuem
# MAGIC entre os cenários de clima. Voos "Sem observação" ficam fora das comparações de P2–P4.

# COMMAND ----------

display(spark.sql(f"""
  SELECT cenario_clima, COUNT(*) AS etapas,
         ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct,
         ROUND(AVG(defasagem_obs_origem_min), 1) AS defasagem_media_origem_min,
         ROUND(AVG(defasagem_obs_destino_min), 1) AS defasagem_media_destino_min
  FROM {tabela('fato_voo')} WHERE em_escopo
  GROUP BY 1 ORDER BY etapas DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Catálogo de dados
# MAGIC Descrição, domínio esperado e linhagem de cada atributo do modelo. O mesmo dicionário
# MAGIC (1) grava as descrições como comentários no Unity Catalog, visíveis no Catalog Explorer, e
# MAGIC (2) é comparado com o **domínio observado** nos dados, na tabela `gold_catalogo_dados`.

# COMMAND ----------

S, V = "silver_voo", "silver_metar"
CAT = {  # tabela -> {coluna: (tipo, descrição, domínio esperado, nulos permitidos / significado, linhagem)}
 "fato_voo": {
  "sk_voo": ("texto", "Chave da etapa: SHA-256 dos atributos de negócio", "64 caracteres hexadecimais", "Não", f"{S}: hash (G5)"),
  "sk_data": ("numérico", "Data da partida prevista (Brasília), AAAAMMDD → dim_tempo", "20240101 a 20251231", "Não", f"{S}.data_referencia_brt"),
  "sk_hora_partida": ("numérico", "Hora da partida prevista (Brasília) → dim_horario", "0 a 23", "Não", f"{S}.partida_prevista_brt"),
  "sk_companhia": ("categórico", "Código ICAO da companhia → dim_companhia", "3 letras (ex.: AZU, GLO, TAM)", "Não", "VRA: ICAO Empresa Aérea"),
  "sk_aeroporto_origem": ("categórico", "Código ICAO do aeroporto de origem → dim_aeroporto", "4 caracteres", "Não", "VRA: ICAO Aeródromo Origem"),
  "sk_aeroporto_destino": ("categórico", "Código ICAO do aeroporto de destino → dim_aeroporto", "4 caracteres", "Não", "VRA: ICAO Aeródromo Destino"),
  "sk_condicao_origem": ("numérico", "Condição meteorológica na origem na partida prevista → dim_condicao_meteorologica", "-1 a 31", "Não; -1 = sem observação", f"{V}, junção G3"),
  "sk_condicao_destino": ("numérico", "Condição meteorológica no destino na chegada prevista → dim_condicao_meteorologica", "-1 a 31", "Não; -1 = sem observação", f"{V}, junção G3"),
  "numero_voo": ("texto", "Número do voo", "1 a 4 dígitos", "Não", "VRA: Número Voo"),
  "codigo_di": ("categórico", "Código de autorização do voo (DI)", "Dígito; 0 = voo regular", "Não", "VRA: Código Autorização (DI)"),
  "situacao_voo": ("categórico", "Situação da etapa", "REALIZADO, CANCELADO", "Não", "VRA: Situação Voo"),
  "partida_prevista_brt": ("data", "Partida prevista, horário de Brasília", "2024–2025", "Sim: voo fora da grade prevista", "VRA: Partida Prevista (R2)"),
  "partida_real_brt": ("data", "Partida realizada, horário de Brasília", "2024–2025 (+1 dia)", "Sim: voo cancelado", "VRA: Partida Real (R2)"),
  "chegada_prevista_brt": ("data", "Chegada prevista, horário de Brasília", "2024–2025 (+1 dia)", "Sim: voo fora da grade prevista", "VRA: Chegada Prevista (R2)"),
  "chegada_real_brt": ("data", "Chegada realizada, horário de Brasília", "2024–2025 (+1 dia)", "Sim: voo cancelado", "VRA: Chegada Real (R2)"),
  "partida_prevista_utc": ("data", "Partida prevista em UTC", "Brasília + 3 h", "Como a de Brasília", "R3"),
  "chegada_prevista_utc": ("data", "Chegada prevista em UTC", "Brasília + 3 h", "Como a de Brasília", "R3"),
  "realizado": ("booleano", "Etapa realizada", "verdadeiro/falso", "Não", "situacao_voo"),
  "cancelado": ("booleano", "Etapa cancelada", "verdadeiro/falso", "Não", "situacao_voo"),
  "atraso_partida_min": ("numérico", "Partida real − prevista, em minutos (negativo = adiantado)", "-120 a 1440", "Sim: cancelado ou sem horário", "R4"),
  "atraso_chegada_min": ("numérico", "Chegada real − prevista, em minutos (negativo = adiantado)", "-120 a 1440", "Sim: cancelado ou sem horário", "R4"),
  "atraso_valido": ("booleano", "Atraso de chegada calculável e plausível", "verdadeiro/falso", "Não", "R5"),
  "atrasado_15": ("booleano", "Chegada com mais de 15 min de atraso", "verdadeiro/falso", "Sim: atraso não válido", "R6"),
  "atrasado_30": ("booleano", "Chegada com mais de 30 min de atraso (sensibilidade)", "verdadeiro/falso", "Sim: atraso não válido", "R6"),
  "em_escopo": ("booleano", "Origem e destino entre os 20 aeroportos com METAR", "verdadeiro/falso", "Não", "G2"),
  "tempo_adverso_origem": ("booleano", "Tempo adverso na origem na partida prevista", "verdadeiro/falso", "Sim: sem observação", "G3"),
  "tempo_adverso_destino": ("booleano", "Tempo adverso no destino na chegada prevista", "verdadeiro/falso", "Sim: sem observação", "G3"),
  "cenario_clima": ("categórico", "Combinação de clima nas duas pontas", "Bom nas duas pontas; Adverso só na origem; Adverso só no destino; Adverso nas duas pontas; Sem observação", "Não", "G3, G4"),
  "defasagem_obs_origem_min": ("numérico", "Minutos entre a observação usada e a partida prevista", "0 a 90", "Sim: sem observação", "G3"),
  "defasagem_obs_destino_min": ("numérico", "Minutos entre a observação usada e a chegada prevista", "0 a 90", "Sim: sem observação", "G3"),
  "visibilidade_origem_m": ("numérico", "Visibilidade na origem (m)", "0 a ~10.000 (9999 = 10 km ou mais)", "Sim: sem observação ou não reportada", f"{V}.visibilidade_m"),
  "teto_origem_ft": ("numérico", "Teto na origem (pés); nulo = sem camada BKN/OVC/VV", "0 a 30.000", "Sim: sem teto (céu sem cobertura significativa)", f"{V}.teto_ft"),
  "rajada_origem_kt": ("numérico", "Rajada na origem (nós)", "15 a 80", "Sim: sem rajada reportada", f"{V}.rajada_kt"),
  "fenomenos_origem": ("texto", "Fenômenos de tempo presente na origem (códigos METAR)", "Ex.: RA, -RA, +TSRA, BR, FG, VCTS", "Sim: nenhum fenômeno", f"{V}.fenomenos"),
  "visibilidade_destino_m": ("numérico", "Visibilidade no destino (m)", "0 a ~10.000", "Sim: sem observação ou não reportada", f"{V}.visibilidade_m"),
  "teto_destino_ft": ("numérico", "Teto no destino (pés)", "0 a 30.000", "Sim: sem teto", f"{V}.teto_ft"),
  "rajada_destino_kt": ("numérico", "Rajada no destino (nós)", "15 a 80", "Sim: sem rajada reportada", f"{V}.rajada_kt"),
  "fenomenos_destino": ("texto", "Fenômenos de tempo presente no destino", "Códigos METAR", "Sim: nenhum fenômeno", f"{V}.fenomenos"),
  "arquivo_origem": ("texto", "Arquivo VRA de onde veio a etapa", "VRA_AAAAM.csv", "Não", "bronze_vra._arquivo_origem"),
 },
 "dim_condicao_meteorologica": {
  "sk_condicao": ("numérico", "Código da condição: soma dos pesos dos fatores adversos (1, 2, 4, 8, 16)", "-1 a 31", "Não", "Gerada (G4)"),
  "descricao": ("texto", "Fatores adversos presentes", "Tempo bom; combinações; Sem observação", "Não", "Gerada"),
  "tempo_adverso": ("booleano", "Algum fator adverso presente", "verdadeiro/falso", "Sim: sem observação", "Gerada"),
  "fl_visibilidade_baixa": ("booleano", "Visibilidade < 5 km", "verdadeiro/falso", "Sim: sem observação", "R9"),
  "fl_teto_baixo": ("booleano", "Teto < 1.500 ft", "verdadeiro/falso", "Sim: sem observação", "R9"),
  "fl_trovoada": ("booleano", "Trovoada no aeródromo ou vizinhança (TS, VCTS)", "verdadeiro/falso", "Sim: sem observação", "R9"),
  "fl_chuva_forte": ("booleano", "Chuva forte (+RA, +SHRA, +TSRA)", "verdadeiro/falso", "Sim: sem observação", "R9"),
  "fl_rajada_forte": ("booleano", "Rajada ≥ 25 kt", "verdadeiro/falso", "Sim: sem observação", "R9"),
  "qtd_fatores": ("numérico", "Quantidade de fatores adversos", "0 a 5", "Sim: sem observação", "Gerada"),
 },
 "dim_aeroporto": {
  "sk_aeroporto": ("categórico", "Código ICAO do aeródromo", "4 caracteres", "Não", "Aeródromos ANAC: Código OACI"),
  "nome": ("texto", "Nome do aeródromo", "Texto livre", "Não", "Aeródromos ANAC: Nome"),
  "municipio": ("texto", "Município", "Texto livre", "Sim: não cadastrado", "Aeródromos ANAC: Município"),
  "uf": ("categórico", "Unidade da federação (nome por extenso)", "27 UFs", "Sim: não cadastrado", "Aeródromos ANAC: UF"),
  "regiao": ("categórico", "Região geográfica", "Norte, Nordeste, Centro-Oeste, Sudeste, Sul, Não informada", "Não", "Derivada da UF"),
  "latitude": ("numérico", "Latitude (graus decimais)", "-34 a 6", "Sim: não cadastrado", "Aeródromos ANAC: LATGEOPOINT"),
  "longitude": ("numérico", "Longitude (graus decimais)", "-74 a -28", "Sim: não cadastrado", "Aeródromos ANAC: LONGEOPOINT"),
  "altitude_m": ("numérico", "Altitude (m)", "0 a 1.200", "Sim: não cadastrado", "Aeródromos ANAC: Altitude"),
  "tem_metar": ("booleano", "Está entre os 20 aeroportos com METAR", "verdadeiro/falso", "Não", "Lista AEROPORTOS_METAR"),
 },
 "dim_companhia": {
  "sk_companhia": ("categórico", "Código ICAO da companhia", "3 letras", "Não", "VRA: ICAO Empresa Aérea"),
  "nome": ("texto", "Nome comercial (principais companhias)", "Texto; 'Não mapeado (código)' nas demais", "Não", "Mapeamento manual"),
  "etapas_2024_2025": ("numérico", "Etapas da companhia no fato", "≥ 1", "Não", "fato_voo"),
  "participacao_pct": ("numérico", "Participação nas etapas do fato (%)", "0 a 100", "Não", "fato_voo"),
 },
 "dim_tempo": {
  "sk_data": ("numérico", "Data AAAAMMDD", "20240101 a 20251231", "Não", "Gerada"),
  "data": ("data", "Data", "2024-01-01 a 2025-12-31", "Não", "Gerada"),
  "ano": ("numérico", "Ano", "2024, 2025", "Não", "Gerada"),
  "trimestre": ("numérico", "Trimestre", "1 a 4", "Não", "Gerada"),
  "mes": ("numérico", "Mês", "1 a 12", "Não", "Gerada"),
  "ano_mes": ("texto", "Ano e mês", "2024-01 a 2025-12", "Não", "Gerada"),
  "dia": ("numérico", "Dia do mês", "1 a 31", "Não", "Gerada"),
  "dia_semana_num": ("numérico", "Dia da semana (1 = domingo)", "1 a 7", "Não", "Gerada"),
  "dia_semana": ("categórico", "Dia da semana", "domingo a sábado", "Não", "Gerada"),
  "fim_de_semana": ("booleano", "Sábado ou domingo", "verdadeiro/falso", "Não", "Gerada"),
 },
 "dim_horario": {
  "sk_hora": ("numérico", "Hora cheia (Brasília)", "0 a 23", "Não", "Gerada"),
  "faixa_horaria": ("categórico", "Faixa do dia", "Madrugada, Manhã, Tarde, Noite", "Não", "Gerada"),
  "ordem_faixa": ("numérico", "Ordem da faixa no dia", "1 a 4", "Não", "Gerada"),
 },
}

# COMMAND ----------

def aspas(texto):
    return texto.replace("'", "\\'")


registros = []
for nome_tabela, colunas in CAT.items():
    df = spark.table(tabela(nome_tabela))
    tipos = dict(df.dtypes)
    total = df.count()
    faltando = set(tipos) - set(colunas)
    assert not faltando, f"{nome_tabela}: colunas sem descrição no catálogo: {faltando}"
    for coluna, (tipo, descricao, dominio, nulos, linhagem) in colunas.items():
        try:  # comentário no Unity Catalog
            spark.sql(f"ALTER TABLE {tabela(nome_tabela)} ALTER COLUMN {coluna} COMMENT '{aspas(descricao)}'")
        except Exception as erro:
            print(f"  sem comentário em {nome_tabela}.{coluna}: {type(erro).__name__}")
        c = F.col(coluna)
        if tipo in ("numérico", "data"):
            r = df.agg(F.min(c).alias("a"), F.max(c).alias("b"), F.sum(c.isNull().cast("int")).alias("n")).first()
            observado = f"{r.a} a {r.b}"
        else:
            r = df.agg(F.countDistinct(c).alias("d"), F.sum(c.isNull().cast("int")).alias("n")).first()
            if r.d <= 8:
                valores = [str(x[0]) for x in df.select(c).distinct().orderBy(c).collect() if x[0] is not None]
                observado = ", ".join(valores)
            else:
                observado = f"{r.d:,} valores distintos"
        registros.append({"tabela": nome_tabela, "atributo": coluna, "tipo_logico": tipo,
                          "tipo_fisico": tipos[coluna], "descricao": descricao,
                          "dominio_esperado": dominio, "dominio_observado": observado,
                          "nulos_permitidos": nulos, "nulos_pct": round(100 * (r.n or 0) / total, 2) if total else 0.0,
                          "linhagem": linhagem})

catalogo = pd.DataFrame(registros)
gravar(spark.createDataFrame(catalogo), "gold_catalogo_dados")
display(spark.table(tabela("gold_catalogo_dados")))
