# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Camada silver: tipagem, limpeza e análise de qualidade
# MAGIC
# MAGIC Transforma as tabelas bronze (todas em texto) em tabelas tipadas e limpas, e mede
# MAGIC a qualidade de cada atributo. **Nenhuma linha é descartada em silêncio**: cada regra
# MAGIC aplicada é contada e registrada na tabela `silver_qualidade_dados`.
# MAGIC
# MAGIC | Tabela | Origem | Grão |
# MAGIC |---|---|---|
# MAGIC | `silver_voo` | `bronze_vra` | uma linha por etapa de voo (2024–2025) |
# MAGIC | `silver_metar` | `bronze_metar` | uma linha por aeroporto e horário de observação |
# MAGIC | `silver_aeroporto` | `bronze_aerodromos` | uma linha por aeródromo |
# MAGIC | `silver_qualidade_dados` | todas | uma linha por regra de qualidade verificada |
# MAGIC
# MAGIC ### Regras de negócio aplicadas
# MAGIC | # | Regra | Motivo |
# MAGIC |---|---|---|
# MAGIC | R1 | Textos `""`, `null`, `N/A` viram nulo | A ANAC grava ausência como texto literal |
# MAGIC | R2 | Datas aceitas em `AAAA-MM-DD HH:MM[:SS][.fração]` e `DD/MM/AAAA HH:MM[:SS]` | O formato muda entre arquivos |
# MAGIC | R3 | Horários do VRA tratados como **horário de Brasília**; UTC = Brasília + 3 h | Verificado nos dados (seção *Verificação do fuso*); Brasil sem horário de verão desde 2019 |
# MAGIC | R4 | Atraso = horário real − horário previsto, em minutos | Métrica central |
# MAGIC | R5 | Atraso fora de [−120, +1440] min é marcado como implausível e não entra nas taxas | Erros de data na origem (ex.: dia ou mês trocado) |
# MAGIC | R6 | Voo atrasado: atraso na **chegada** > 15 min (e > 30 min para sensibilidade) | Definição do objetivo |
# MAGIC | R7 | Duplicatas exatas de etapa são removidas | Mesma etapa publicada duas vezes inflaria as taxas |
# MAGIC | R8 | Período: partida prevista (ou real, se a prevista faltar) entre 01/01/2024 e 31/12/2025 | Os arquivos trazem algumas etapas do mês seguinte |
# MAGIC | R9 | METAR: visibilidade de milhas para metros; teto = base da camada mais baixa BKN, OVC ou VV | Unidades do IEM e definição aeronáutica de teto |
# MAGIC | R10 | METAR repetido no mesmo aeroporto e horário: fica uma observação (a mais severa) | Sobreposição entre os arquivos de 2024 e 2025 e boletins especiais |

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

CATALOGO, SCHEMA = "workspace", "mvp_voos"
spark.conf.set("spark.sql.session.timeZone", "UTC")  # timestamps sem conversão implícita


def tabela(nome):
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def gravar(df, nome, comentario):
    (df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela(nome)))
    spark.sql(f"COMMENT ON TABLE {tabela(nome)} IS '{comentario}'")
    print(f"{tabela(nome)}: {spark.table(tabela(nome)).count():,} linhas")


NULOS = ["", "null", "NULL", "N/A", "NA", "n/a"]


def limpo(coluna):
    """R1: texto aparado; marcadores de ausência viram nulo."""
    c = F.trim(F.col(coluna))
    return F.when(c.isin(NULOS), F.lit(None)).otherwise(c)


def data_hora(coluna):
    """R2: aceita os formatos de data encontrados nos arquivos da ANAC."""
    s = F.regexp_replace(limpo(coluna), r"\.\d+$", "")  # descarta fração de segundo
    return F.coalesce(*[F.try_to_timestamp(s, F.lit(fmt)) for fmt in (
        "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm", "dd/MM/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm")])


def minutos(fim, inicio):
    return (F.unix_timestamp(fim) - F.unix_timestamp(inicio)) / 60


qualidade = []  # cada item: uma regra verificada


def registrar(tabela_nome, atributo, dimensao, regra, n, total, tratamento):
    qualidade.append({
        "tabela": tabela_nome, "atributo": atributo, "dimensao": dimensao, "regra": regra,
        "ocorrencias": int(n), "total": int(total),
        "percentual": round(100.0 * n / total, 3) if total else 0.0,
        "tratamento": tratamento,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Voos (VRA)

# COMMAND ----------

bronze_vra = spark.table(tabela("bronze_vra"))
total_bronze = bronze_vra.count()

COLUNAS_NEGOCIO = ["empresa_icao", "numero_voo", "codigo_di", "codigo_tipo_linha",
                   "origem_icao", "destino_icao", "partida_prevista", "partida_real",
                   "chegada_prevista", "chegada_real", "situacao_voo", "codigo_justificativa"]

# R7: duplicatas exatas (todas as colunas de negócio iguais)
sem_duplicatas = bronze_vra.dropDuplicates(COLUNAS_NEGOCIO)
n_duplicatas = total_bronze - sem_duplicatas.count()

voo = (
    sem_duplicatas.select(
        limpo("empresa_icao").alias("empresa_icao"),
        limpo("numero_voo").alias("numero_voo"),
        limpo("codigo_di").alias("codigo_di"),
        limpo("codigo_tipo_linha").alias("codigo_tipo_linha"),
        F.upper(limpo("origem_icao")).alias("origem_icao"),
        F.upper(limpo("destino_icao")).alias("destino_icao"),
        data_hora("partida_prevista").alias("partida_prevista_brt"),
        data_hora("partida_real").alias("partida_real_brt"),
        data_hora("chegada_prevista").alias("chegada_prevista_brt"),
        data_hora("chegada_real").alias("chegada_real_brt"),
        F.upper(limpo("situacao_voo")).alias("situacao_voo"),
        limpo("codigo_justificativa").alias("codigo_justificativa"),
        F.col("_arquivo_origem"),
        # textos originais, para medir falhas de conversão de data
        limpo("partida_prevista").alias("_txt_partida_prevista"),
        limpo("partida_real").alias("_txt_partida_real"),
        limpo("chegada_prevista").alias("_txt_chegada_prevista"),
        limpo("chegada_real").alias("_txt_chegada_real"),
    )
    # R3: UTC = horário de Brasília + 3 h
    .withColumn("partida_prevista_utc", F.col("partida_prevista_brt") + F.expr("INTERVAL 3 HOURS"))
    .withColumn("partida_real_utc", F.col("partida_real_brt") + F.expr("INTERVAL 3 HOURS"))
    .withColumn("chegada_prevista_utc", F.col("chegada_prevista_brt") + F.expr("INTERVAL 3 HOURS"))
    .withColumn("chegada_real_utc", F.col("chegada_real_brt") + F.expr("INTERVAL 3 HOURS"))
    .withColumn("cancelado", F.col("situacao_voo") == "CANCELADO")
    .withColumn("realizado", F.col("situacao_voo") == "REALIZADO")
    # R4
    .withColumn("atraso_partida_min", minutos("partida_real_brt", "partida_prevista_brt"))
    .withColumn("atraso_chegada_min", minutos("chegada_real_brt", "chegada_prevista_brt"))
    # R5
    .withColumn("fl_atraso_implausivel",
                F.coalesce(~F.col("atraso_chegada_min").between(-120, 1440), F.lit(False)))
    .withColumn("fl_chegada_antes_partida",
                F.coalesce(F.col("chegada_real_brt") <= F.col("partida_real_brt"), F.lit(False)))
    # R6: só definido para voos realizados com atraso plausível
    .withColumn("atraso_valido", F.col("realizado") & F.col("atraso_chegada_min").isNotNull()
                & ~F.col("fl_atraso_implausivel") & ~F.col("fl_chegada_antes_partida"))
    .withColumn("atrasado_15", F.when(F.col("atraso_valido"), F.col("atraso_chegada_min") > 15))
    .withColumn("atrasado_30", F.when(F.col("atraso_valido"), F.col("atraso_chegada_min") > 30))
    # R8: data de referência da etapa
    .withColumn("data_referencia_brt",
                F.coalesce(F.col("partida_prevista_brt"), F.col("partida_real_brt")))
    .withColumn("id_etapa", F.sha2(F.concat_ws("|", "empresa_icao", "numero_voo", "origem_icao",
                                                "destino_icao", "_txt_partida_prevista",
                                                "_txt_partida_real"), 256))
)
voo.cache()
total = voo.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Qualidade dos voos
# MAGIC Métricas calculadas sobre as etapas já sem duplicatas, antes do filtro de período.

# COMMAND ----------

m = voo.agg(
    *[F.sum(F.col(f"_txt_{c}").isNull().cast("int")).alias(f"nulo_{c}")
      for c in ("partida_prevista", "partida_real", "chegada_prevista", "chegada_real")],
    *[F.sum((F.col(f"_txt_{c}").isNotNull() & F.col(f"{c}_brt").isNull()).cast("int")).alias(f"invalido_{c}")
      for c in ("partida_prevista", "partida_real", "chegada_prevista", "chegada_real")],
    F.sum(F.col("empresa_icao").isNull().cast("int")).alias("nulo_empresa"),
    F.sum((~F.col("origem_icao").rlike("^[A-Z0-9]{4}$")).cast("int")).alias("icao_origem_fora"),
    F.sum((~F.col("destino_icao").rlike("^[A-Z0-9]{4}$")).cast("int")).alias("icao_destino_fora"),
    F.sum((F.col("origem_icao") == F.col("destino_icao")).cast("int")).alias("origem_igual_destino"),
    F.sum((~F.col("situacao_voo").isin("REALIZADO", "CANCELADO")).cast("int")).alias("situacao_fora"),
    F.sum((~F.col("codigo_tipo_linha").isin("N", "C", "I", "G")).cast("int")).alias("tipo_linha_fora"),
    F.sum(F.col("realizado").cast("int")).alias("realizados"),
    F.sum((F.col("realizado") & F.col("chegada_real_brt").isNull()).cast("int")).alias("realizado_sem_chegada"),
    F.sum((F.col("cancelado") & F.col("partida_real_brt").isNotNull()).cast("int")).alias("cancelado_com_partida"),
    F.sum(F.col("fl_atraso_implausivel").cast("int")).alias("implausivel"),
    F.sum(F.col("fl_chegada_antes_partida").cast("int")).alias("chegada_antes"),
    F.sum(F.col("codigo_justificativa").isNotNull().cast("int")).alias("justificativa_preenchida"),
    F.sum(F.col("data_referencia_brt").isNull().cast("int")).alias("sem_data"),
    F.sum((~F.col("data_referencia_brt").between("2024-01-01", "2025-12-31 23:59:59")).cast("int")).alias("fora_periodo"),
).first().asDict()

T = "silver_voo"
registrar(T, "todas", "Unicidade", "Etapas duplicadas (todas as colunas iguais)", n_duplicatas, total_bronze, "Removidas (R7)")
for c in ("partida_prevista", "partida_real", "chegada_prevista", "chegada_real"):
    registrar(T, c, "Completude", f"{c} ausente", m[f"nulo_{c}"], total,
              "Mantida; esperado em cancelados (horário real)" if "real" in c else "Mantida; sem atraso calculável")
    registrar(T, c, "Conformidade", f"{c} em formato de data não reconhecido", m[f"invalido_{c}"], total, "Vira nulo")
registrar(T, "empresa_icao", "Completude", "Companhia ausente", m["nulo_empresa"], total, "Mantida")
registrar(T, "origem_icao", "Conformidade", "Código ICAO fora do padrão de 4 caracteres", m["icao_origem_fora"], total, "Mantida; fora do escopo de aeroportos")
registrar(T, "destino_icao", "Conformidade", "Código ICAO fora do padrão de 4 caracteres", m["icao_destino_fora"], total, "Mantida; fora do escopo de aeroportos")
registrar(T, "origem/destino", "Consistência", "Origem igual ao destino", m["origem_igual_destino"], total, "Mantida; avaliar se afeta o escopo")
registrar(T, "situacao_voo", "Conformidade", "Situação fora de {REALIZADO, CANCELADO}", m["situacao_fora"], total, "Mantida; não entra em taxas")
registrar(T, "codigo_tipo_linha", "Conformidade", "Tipo de linha fora de {N, C, I, G}", m["tipo_linha_fora"], total, "Mantida; fora do escopo (N)")
registrar(T, "chegada_real", "Consistência", "Voo realizado sem horário real de chegada", m["realizado_sem_chegada"], m["realizados"], "Sem atraso calculável")
registrar(T, "partida_real", "Consistência", "Voo cancelado com horário real de partida", m["cancelado_com_partida"], total, "Mantida; situação prevalece")
registrar(T, "atraso_chegada_min", "Acurácia", "Atraso fora de [-120, +1440] min", m["implausivel"], m["realizados"], "Fora das taxas de atraso (R5)")
registrar(T, "chegada_real", "Consistência", "Chegada real antes ou igual à partida real", m["chegada_antes"], m["realizados"], "Fora das taxas de atraso")
registrar(T, "codigo_justificativa", "Completude", "Justificativa do atraso preenchida", m["justificativa_preenchida"], total, "Atributo descartado: vazio desde 2020")
registrar(T, "data_referencia", "Atualidade", "Etapa fora de 2024–2025 ou sem data", m["fora_periodo"] + m["sem_data"], total, "Removida (R8)")

silver_voo = (voo
    .filter(F.col("data_referencia_brt").between("2024-01-01", "2025-12-31 23:59:59"))
    .withColumn("ano", F.year("data_referencia_brt"))
    .withColumn("mes", F.month("data_referencia_brt"))
    .drop("codigo_justificativa", "_txt_partida_prevista", "_txt_partida_real",
          "_txt_chegada_prevista", "_txt_chegada_real"))

gravar(silver_voo, "silver_voo", "Etapas de voo do VRA 2024-2025, tipadas, com atrasos em minutos e horários em Brasília e UTC")
voo.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificação do fuso dos horários (R3)
# MAGIC Se os horários estivessem no fuso local de cada aeroporto, o tempo previsto de voo
# MAGIC GRU→Manaus (−1 h) e Manaus→GRU seria muito diferente nos dois sentidos. Se ambos
# MAGIC estiverem no horário de Brasília, os dois ficam próximos da duração real (~4 h).

# COMMAND ----------

display(spark.sql(f"""
  SELECT origem_icao, destino_icao, COUNT(*) AS etapas,
         ROUND(PERCENTILE(TIMESTAMPDIFF(MINUTE, partida_prevista_brt, chegada_prevista_brt), 0.5) / 60, 2)
           AS duracao_prevista_mediana_h
  FROM {tabela('silver_voo')}
  WHERE (origem_icao, destino_icao) IN (('SBGR','SBEG'), ('SBEG','SBGR'), ('SBBR','SBRB'), ('SBRB','SBBR'))
  GROUP BY 1, 2 ORDER BY 1, 2
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência dos 20 aeroportos do METAR
# MAGIC A lista inicial do download do METAR veio da amostra de jul/2026. Conferida com 2024–2025,
# MAGIC SBNF (Navegantes) entrou no 20º lugar no lugar de SBPS (Porto Seguro); a lista abaixo já
# MAGIC está corrigida e o METAR de SBNF foi baixado à parte.

# COMMAND ----------

AEROPORTOS_METAR = ["SBGR", "SBSP", "SBBR", "SBCF", "SBKP", "SBGL", "SBRF", "SBSV", "SBRJ", "SBPA",
                    "SBCT", "SBFZ", "SBBE", "SBFL", "SBGO", "SBEG", "SBVT", "SBNF", "SBMO", "SBCY"]

ranking = (spark.table(tabela("silver_voo")).filter("codigo_tipo_linha = 'N'")
    .select(F.explode(F.array("origem_icao", "destino_icao")).alias("icao"))
    .groupBy("icao").count().orderBy(F.desc("count")).limit(20).toPandas())
ranking["posicao"] = range(1, len(ranking) + 1)
ranking["na_lista_metar"] = ranking["icao"].isin(AEROPORTOS_METAR)
display(ranking)
fora = sorted(set(AEROPORTOS_METAR) - set(ranking["icao"]))
print("Na lista do METAR mas fora do top 20 de 2024–2025:", fora or "nenhum")
print("No top 20 de 2024–2025 mas sem METAR:", sorted(set(ranking["icao"]) - set(AEROPORTOS_METAR)) or "nenhum")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Observações meteorológicas (METAR)

# COMMAND ----------

bronze_metar = spark.table(tabela("bronze_metar"))
total_metar_bronze = bronze_metar.count()


def num(coluna):
    return F.expr(f"try_cast(nullif(trim({coluna}), '') AS DOUBLE)")


camadas = [(F.trim(F.col(f"skyc{i}")), num(f"skyl{i}")) for i in range(1, 5)]
teto = F.least(*[F.when(cob.isin("BKN", "OVC", "VV"), alt) for cob, alt in camadas])

metar = (bronze_metar.select(
        F.upper(F.trim("station")).alias("aeroporto_icao"),
        F.try_to_timestamp(F.trim("valid"), F.lit("yyyy-MM-dd HH:mm")).alias("observacao_utc"),
        (num("vsby") * 1609.344).alias("visibilidade_m"),
        teto.alias("teto_ft"),
        num("sknt").alias("vento_kt"),
        num("gust").alias("rajada_kt"),
        num("drct").alias("vento_direcao_graus"),
        ((num("tmpf") - 32) * 5 / 9).alias("temperatura_c"),
        F.nullif(F.trim("wxcodes"), F.lit("")).alias("fenomenos"),
        F.trim("metar").alias("metar"),
    )
    # R9: indicadores de tempo adverso (limiares definidos no objetivo)
    .withColumn("fl_visibilidade_baixa", F.coalesce(F.col("visibilidade_m") < 5000, F.lit(False)))
    .withColumn("fl_teto_baixo", F.coalesce(F.col("teto_ft") < 1500, F.lit(False)))
    .withColumn("fl_trovoada", F.coalesce(F.col("fenomenos").rlike("TS"), F.lit(False)))
    .withColumn("fl_chuva_forte", F.coalesce(F.col("fenomenos").rlike(r"\+[A-Z]*RA"), F.lit(False)))
    .withColumn("fl_rajada_forte", F.coalesce(F.col("rajada_kt") >= 25, F.lit(False)))
    .withColumn("tempo_adverso", F.col("fl_visibilidade_baixa") | F.col("fl_teto_baixo")
                | F.col("fl_trovoada") | F.col("fl_chuva_forte") | F.col("fl_rajada_forte"))
    .withColumn("severidade", sum(F.col(c).cast("int") for c in (
        "fl_visibilidade_baixa", "fl_teto_baixo", "fl_trovoada", "fl_chuva_forte", "fl_rajada_forte")))
)
metar.cache()
total_metar = metar.count()

# R10: uma observação por aeroporto e horário, a mais severa
janela = Window.partitionBy("aeroporto_icao", "observacao_utc").orderBy(F.desc("severidade"), F.desc("metar"))
metar_unico = (metar.filter(F.col("observacao_utc").isNotNull())
               .withColumn("_n", F.row_number().over(janela)).filter("_n = 1").drop("_n"))
n_metar_unico = metar_unico.count()

mm = metar.agg(
    F.sum(F.col("observacao_utc").isNull().cast("int")).alias("sem_hora"),
    F.sum(F.col("visibilidade_m").isNull().cast("int")).alias("sem_vis"),
    F.sum(F.col("metar").isNull().cast("int")).alias("sem_metar"),
    F.sum((~F.col("aeroporto_icao").isin(AEROPORTOS_METAR)).cast("int")).alias("fora_lista"),
    F.sum((F.col("visibilidade_m") > 20000).cast("int")).alias("vis_alta"),
    F.sum((F.col("teto_ft") > 30000).cast("int")).alias("teto_alto"),
    F.sum((F.col("rajada_kt") > 80).cast("int")).alias("rajada_alta"),
    F.sum((F.col("rajada_kt") < F.col("vento_kt")).cast("int")).alias("rajada_menor"),
    F.sum(F.col("tempo_adverso").cast("int")).alias("adverso"),
).first().asDict()

T = "silver_metar"
registrar(T, "observacao_utc", "Conformidade", "Data/hora não reconhecida", mm["sem_hora"], total_metar_bronze, "Removida")
registrar(T, "aeroporto/observacao_utc", "Unicidade", "Mais de uma observação no mesmo aeroporto e horário",
          total_metar - mm["sem_hora"] - n_metar_unico, total_metar_bronze, "Mantida a mais severa (R10)")
registrar(T, "aeroporto_icao", "Conformidade", "Aeroporto fora da lista dos 20", mm["fora_lista"], total_metar, "Mantida; não entra no cruzamento")
registrar(T, "visibilidade_m", "Completude", "Visibilidade ausente", mm["sem_vis"], total_metar, "Tratada como sem restrição de visibilidade")
registrar(T, "metar", "Completude", "Texto METAR original ausente", mm["sem_metar"], total_metar, "Mantida")
registrar(T, "visibilidade_m", "Acurácia", "Visibilidade acima de 20 km", mm["vis_alta"], total_metar, "Mantida; não afeta o limiar de 5 km")
registrar(T, "teto_ft", "Acurácia", "Teto acima de 30.000 pés", mm["teto_alto"], total_metar, "Mantida; não afeta o limiar")
registrar(T, "rajada_kt", "Acurácia", "Rajada acima de 80 kt", mm["rajada_alta"], total_metar, "Verificar caso a caso")
registrar(T, "rajada_kt", "Consistência", "Rajada menor que o vento médio", mm["rajada_menor"], total_metar, "Mantida; não afeta o limiar")
registrar(T, "tempo_adverso", "Domínio", "Observações com tempo adverso", mm["adverso"], total_metar, "Informativo")

gravar(metar_unico.drop("severidade"), "silver_metar",
       "Observações METAR/SPECI tipadas, com visibilidade (m), teto (ft) e indicadores de tempo adverso")
metar.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cobertura temporal do METAR por aeroporto
# MAGIC Percentual das horas de 2024–2025 com pelo menos uma observação. Lacunas longas
# MAGIC significam voos sem clima conhecido; o limite de 90 min na junção (camada gold)
# MAGIC impede que uma observação antiga seja usada no lugar da que faltou.

# COMMAND ----------

horas_periodo = 731 * 24  # 2024 (bissexto) + 2025
# Parte da LISTA de aeroportos (e não das observações), para que uma estação sem
# nenhum dado apareça com cobertura 0% em vez de sumir do relatório.
spark.createDataFrame([(a,) for a in AEROPORTOS_METAR], "aeroporto_icao string") \
    .createOrReplaceTempView("lista_metar")
cobertura = spark.sql(f"""
  SELECT l.aeroporto_icao,
         COUNT(DISTINCT date_trunc('HOUR', m.observacao_utc)) AS horas_com_observacao,
         ROUND(100 * COUNT(DISTINCT date_trunc('HOUR', m.observacao_utc)) / {horas_periodo}, 1) AS cobertura_pct,
         ROUND(100 * AVG(CAST(m.tempo_adverso AS INT)), 1) AS pct_obs_tempo_adverso
  FROM lista_metar l
  LEFT JOIN {tabela('silver_metar')} m
    ON m.aeroporto_icao = l.aeroporto_icao
   AND m.observacao_utc >= '2024-01-01' AND m.observacao_utc < '2026-01-01'
  GROUP BY 1 ORDER BY cobertura_pct
""")
display(cobertura)
sem_dados = [l.aeroporto_icao for l in cobertura.collect() if l.horas_com_observacao == 0]
if sem_dados:
    print("ATENÇÃO: aeroportos da lista sem nenhuma observação METAR:", sem_dados)
for linha in cobertura.collect():
    registrar("silver_metar", f"cobertura {linha.aeroporto_icao}", "Completude",
              "Horas de 2024–2025 sem nenhuma observação", horas_periodo - linha.horas_com_observacao,
              horas_periodo, "Voos nessas horas ficam sem clima (junção limitada a 90 min)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aeroportos

# COMMAND ----------

bronze_aero = spark.table(tabela("bronze_aerodromos"))

aeroporto = (bronze_aero.select(
        F.upper(limpo("codigo_oaci")).alias("aeroporto_icao"),
        limpo("nome").alias("nome"),
        limpo("municipio").alias("municipio"),
        limpo("uf").alias("uf"),
        F.expr("try_cast(replace(trim(latgeopoint), ',', '.') AS DOUBLE)").alias("latitude"),
        F.expr("try_cast(replace(trim(longeopoint), ',', '.') AS DOUBLE)").alias("longitude"),
        F.expr("try_cast(replace(trim(altitude), ',', '.') AS DOUBLE)").alias("altitude_m"),
    ).dropDuplicates(["aeroporto_icao"]))

# Cobertura: os aeroportos usados nos voos estão no cadastro?
usados = (spark.table(tabela("silver_voo")).filter("codigo_tipo_linha = 'N'")
          .select(F.explode(F.array("origem_icao", "destino_icao")).alias("aeroporto_icao")).distinct())
n_usados = usados.count()
sem_cadastro = usados.join(aeroporto, "aeroporto_icao", "left_anti")
registrar("silver_aeroporto", "aeroporto_icao", "Consistência",
          "Aeroporto de voo doméstico sem cadastro na lista da ANAC", sem_cadastro.count(), n_usados,
          "Mantido na dimensão com nome 'não cadastrado'")
registrar("silver_aeroporto", "latitude/longitude", "Completude", "Coordenadas ausentes",
          aeroporto.filter("latitude IS NULL OR longitude IS NULL").count(), aeroporto.count(), "Mantido")
print("Aeroportos de voos domésticos sem cadastro:", [r.aeroporto_icao for r in sem_cadastro.collect()])

gravar(aeroporto, "silver_aeroporto", "Cadastro de aeródromos públicos da ANAC, tipado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Relatório de qualidade

# COMMAND ----------

import pandas as pd

relatorio = pd.DataFrame(qualidade)
spark.createDataFrame(relatorio).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(tabela("silver_qualidade_dados"))
display(spark.table(tabela("silver_qualidade_dados")).orderBy("tabela", "dimensao"))
