# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Análise: respostas às perguntas de negócio
# MAGIC
# MAGIC Responde P1–P5 sobre o modelo estrela da camada gold. Cada resposta tem uma tabela
# MAGIC gravada no schema (`resp_p1_*` … `resp_p5_*`), um gráfico salvo no Volume `resultados`
# MAGIC e os números usados na discussão do README.
# MAGIC
# MAGIC ### Populações
# MAGIC | Uso | Filtro |
# MAGIC |---|---|
# MAGIC | Taxa de atraso | voos **realizados** com atraso de chegada válido (`atraso_valido`) |
# MAGIC | Taxa de cancelamento | todas as etapas (realizadas + canceladas) |
# MAGIC | P1 | etapas com **origem** em um dos 20 aeroportos (não depende de clima) |
# MAGIC | P2–P5 | etapas **em escopo** (origem e destino com METAR) e com observação nas duas pontas |
# MAGIC
# MAGIC ### Método para separar o efeito do clima de outros fatores
# MAGIC O tempo adverso não acontece ao acaso: chove mais em certos meses, horários e aeroportos,
# MAGIC que também têm taxas de atraso diferentes. Comparar só "adverso × bom" misturaria esses
# MAGIC efeitos. Por isso, além da comparação direta, as respostas usam **estratos** de
# MAGIC *aeroporto de origem × mês × faixa horária* (≈ 1.900 estratos):
# MAGIC - **Razão de risco ajustada (Mantel-Haenszel)**: quantas vezes o atraso é mais provável com
# MAGIC   tempo adverso, comparando voos do mesmo aeroporto, mês e faixa horária (P2, P3).
# MAGIC - **Excesso de atrasos atribuível ao clima**: em cada estrato, atrasos observados com tempo
# MAGIC   adverso menos os que seriam esperados se esses voos tivessem a taxa dos voos com tempo
# MAGIC   bom no mesmo estrato (P4).
# MAGIC
# MAGIC Intervalos de confiança de 95% das taxas: método de Wilson.

# COMMAND ----------

import math
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.sql import functions as F

CATALOGO, SCHEMA = "workspace", "mvp_voos"
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.resultados "
          "COMMENT 'Gráficos e tabelas de resultado do MVP'")
PASTA_FIG = f"/Volumes/{CATALOGO}/{SCHEMA}/resultados"

AEROPORTOS_METAR = ["SBGR", "SBSP", "SBBR", "SBCF", "SBKP", "SBGL", "SBRF", "SBSV", "SBRJ", "SBPA",
                    "SBCT", "SBFZ", "SBBE", "SBFL", "SBGO", "SBEG", "SBVT", "SBNF", "SBMO", "SBCY"]


def tabela(nome):
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def salvar_tabela(pdf, nome):
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(tabela(nome))


# --- Estilo dos gráficos: fundo claro, eixos discretos, texto em cinza-escuro,
#     duas cores de série (azul = tempo bom/geral, laranja = tempo adverso).
AZUL, LARANJA, CINZA = "#2a78d6", "#eb6834", "#a8a7a2"
TEXTO, TEXTO_2, GRADE, FUNDO = "#0b0b0b", "#52514e", "#e6e5e0", "#fcfcfb"
plt.rcParams.update({
    "figure.facecolor": FUNDO, "axes.facecolor": FUNDO, "savefig.facecolor": FUNDO,
    "axes.edgecolor": GRADE, "axes.labelcolor": TEXTO_2, "xtick.color": TEXTO_2,
    "ytick.color": TEXTO_2, "text.color": TEXTO, "axes.grid": True, "grid.color": GRADE,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "axes.titlesize": 13, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "font.size": 10, "legend.frameon": False,
})


def finalizar(fig, ax, titulo, subtitulo, arquivo):
    ax.set_title(titulo, pad=24)
    ax.text(0, 1.02, subtitulo, transform=ax.transAxes, color=TEXTO_2, fontsize=9.5)
    fig.text(0.01, 0.005, "Fontes: ANAC (VRA, 2024–2025) e Iowa Environmental Mesonet (METAR). "
             "Atraso = chegada > 15 min após o previsto.", color=TEXTO_2, fontsize=8)
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(os.path.join(PASTA_FIG, arquivo), dpi=160)
    plt.show()


def wilson(atrasos, n, z=1.96):
    if n == 0:
        return (np.nan, np.nan)
    p = atrasos / n
    centro = (p + z * z / (2 * n)) / (1 + z * z / n)
    margem = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / (1 + z * z / n)
    return (centro - margem, centro + margem)


def br(valor, casas=1):
    """Número no padrão brasileiro: 1.234,5"""
    return f"{valor:,.{casas}f}".replace(",", "§").replace(".", ",").replace("§", ".")


def pct(x):
    return br(100 * x) + "%"


def eixo_br(ax, eixo="y", casas=0):
    from matplotlib.ticker import FuncFormatter
    fmt = FuncFormatter(lambda v, _: br(v, casas))
    (ax.yaxis if eixo == "y" else ax.xaxis).set_major_formatter(fmt)


fato = spark.table(tabela("fato_voo"))
condicao = spark.table(tabela("dim_condicao_meteorologica"))
tempo = spark.table(tabela("dim_tempo"))
horario = spark.table(tabela("dim_horario"))
aeroporto = spark.table(tabela("dim_aeroporto"))

base = (fato.join(tempo, "sk_data").join(horario, fato.sk_hora_partida == horario.sk_hora)
        .withColumn("atraso_int", F.col("atrasado_15").cast("int"))
        .withColumn("atraso30_int", F.col("atrasado_30").cast("int"))
        .withColumn("cancelado_int", F.col("cancelado").cast("int")))

# População de P2–P5
escopo = base.filter("em_escopo AND cenario_clima <> 'Sem observação'")
resumo = {}  # números-chave para a discussão

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 · Onde e quando o atraso se concentra
# MAGIC Quais aeroportos, faixas horárias e meses concentram as maiores taxas de atraso na chegada e de cancelamento?

# COMMAND ----------

p1_aero = (base.filter(F.col("sk_aeroporto_origem").isin(AEROPORTOS_METAR))
    .groupBy(F.col("sk_aeroporto_origem").alias("aeroporto"))
    .agg(F.count("*").alias("etapas"), F.sum("cancelado_int").alias("cancelados"),
         F.sum(F.col("atraso_valido").cast("int")).alias("voos_com_atraso_valido"),
         F.sum("atraso_int").alias("atrasados"))
    .join(aeroporto.select(F.col("sk_aeroporto").alias("aeroporto"), "nome", "uf"), "aeroporto", "left")
    .toPandas())
p1_aero["taxa_atraso"] = p1_aero["atrasados"] / p1_aero["voos_com_atraso_valido"]
p1_aero["taxa_cancelamento"] = p1_aero["cancelados"] / p1_aero["etapas"]
p1_aero["participacao_nos_atrasos"] = p1_aero["atrasados"] / p1_aero["atrasados"].sum()
p1_aero["participacao_nas_etapas"] = p1_aero["etapas"] / p1_aero["etapas"].sum()
p1_aero = p1_aero.sort_values("taxa_atraso", ascending=False).reset_index(drop=True)
salvar_tabela(p1_aero, "resp_p1_aeroporto")
display(p1_aero)

media = p1_aero["atrasados"].sum() / p1_aero["voos_com_atraso_valido"].sum()
fig, ax = plt.subplots(figsize=(9, 6.5))
d = p1_aero.sort_values("taxa_atraso")
cores = [LARANJA if t > media else AZUL for t in d["taxa_atraso"]]
ax.barh(d["aeroporto"], 100 * d["taxa_atraso"], color=cores, height=0.7)
for y, v in enumerate(d["taxa_atraso"]):
    ax.text(100 * v + 0.3, y, pct(v), va="center", fontsize=8.5, color=TEXTO_2)
ax.axvline(100 * media, color=TEXTO_2, lw=1, ls="--")
ax.text(100 * media, len(d) - 0.2, f" média {pct(media)}", color=TEXTO_2, fontsize=8.5)
ax.set_xlabel("Voos com chegada atrasada (%)")
eixo_br(ax, "x")
ax.grid(axis="y", visible=False)
finalizar(fig, ax, "P1 · Taxa de atraso na chegada por aeroporto de origem",
          "Laranja: acima da média dos 20 aeroportos · 2024–2025", "p1_atraso_por_aeroporto.png")

fig, ax = plt.subplots(figsize=(9, 6.5))
d = p1_aero.sort_values("taxa_cancelamento")
ax.barh(d["aeroporto"], 100 * d["taxa_cancelamento"], color=AZUL, height=0.7)
for y, v in enumerate(d["taxa_cancelamento"]):
    ax.text(100 * v + 0.05, y, pct(v), va="center", fontsize=8.5, color=TEXTO_2)
ax.set_xlabel("Etapas canceladas (%)")
eixo_br(ax, "x", 1)
ax.grid(axis="y", visible=False)
finalizar(fig, ax, "P1 · Taxa de cancelamento por aeroporto de origem", "2024–2025",
          "p1_cancelamento_por_aeroporto.png")

# Concentração: os 5 aeroportos com mais atrasos respondem por quanto do total?
top5 = p1_aero.sort_values("atrasados", ascending=False).head(5)
resumo["P1_taxa_atraso_media_20_aeroportos"] = pct(media)
resumo["P1_maior_taxa_atraso"] = f"{p1_aero.iloc[0].aeroporto} ({pct(p1_aero.iloc[0].taxa_atraso)})"
resumo["P1_menor_taxa_atraso"] = f"{p1_aero.iloc[-1].aeroporto} ({pct(p1_aero.iloc[-1].taxa_atraso)})"
resumo["P1_top5_aeroportos_por_volume_de_atrasos"] = ", ".join(top5["aeroporto"])
resumo["P1_top5_parcela_dos_atrasos_vs_das_etapas"] = (
    f"{pct(top5['participacao_nos_atrasos'].sum())} dos atrasos com "
    f"{pct(top5['participacao_nas_etapas'].sum())} das etapas")

# COMMAND ----------

p1_mes = (base.filter(F.col("sk_aeroporto_origem").isin(AEROPORTOS_METAR))
    .groupBy("ano_mes")
    .agg(F.count("*").alias("etapas"), F.sum("cancelado_int").alias("cancelados"),
         F.sum(F.col("atraso_valido").cast("int")).alias("voos_com_atraso_valido"),
         F.sum("atraso_int").alias("atrasados"))
    .orderBy("ano_mes").toPandas())
p1_mes["taxa_atraso"] = p1_mes["atrasados"] / p1_mes["voos_com_atraso_valido"]
p1_mes["taxa_cancelamento"] = p1_mes["cancelados"] / p1_mes["etapas"]
salvar_tabela(p1_mes, "resp_p1_mes")
display(p1_mes)

for coluna, rotulo, arquivo in (("taxa_atraso", "Voos com chegada atrasada (%)", "p1_atraso_por_mes.png"),
                                ("taxa_cancelamento", "Etapas canceladas (%)", "p1_cancelamento_por_mes.png")):
    fig, ax = plt.subplots(figsize=(10, 4.2))
    x = np.arange(len(p1_mes))
    ax.plot(x, 100 * p1_mes[coluna], color=AZUL, lw=2, marker="o", ms=4)
    for i in (p1_mes[coluna].idxmax(), p1_mes[coluna].idxmin()):
        ax.annotate(f"{p1_mes.ano_mes[i]}: {pct(p1_mes[coluna][i])}", (i, 100 * p1_mes[coluna][i]),
                    textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8.5, color=TEXTO_2)
    ax.set_xticks(x[::2], p1_mes["ano_mes"][::2], rotation=45, ha="right")
    ax.set_ylabel(rotulo)
    ax.set_ylim(0, 100 * p1_mes[coluna].max() * 1.2)  # folga para o rótulo do pico
    eixo_br(ax, "y", 1)
    finalizar(fig, ax, f"P1 · {rotulo.split(' (')[0]} por mês", "20 aeroportos de origem · 2024–2025", arquivo)

resumo["P1_mes_maior_atraso"] = f"{p1_mes.loc[p1_mes.taxa_atraso.idxmax(), 'ano_mes']} ({pct(p1_mes.taxa_atraso.max())})"
resumo["P1_mes_menor_atraso"] = f"{p1_mes.loc[p1_mes.taxa_atraso.idxmin(), 'ano_mes']} ({pct(p1_mes.taxa_atraso.min())})"
resumo["P1_mes_maior_cancelamento"] = f"{p1_mes.loc[p1_mes.taxa_cancelamento.idxmax(), 'ano_mes']} ({pct(p1_mes.taxa_cancelamento.max())})"

p1_faixa = (base.filter(F.col("sk_aeroporto_origem").isin(AEROPORTOS_METAR))
    .groupBy("ordem_faixa", "faixa_horaria")
    .agg(F.count("*").alias("etapas"), F.sum("cancelado_int").alias("cancelados"),
         F.sum(F.col("atraso_valido").cast("int")).alias("voos_com_atraso_valido"),
         F.sum("atraso_int").alias("atrasados"))
    .orderBy("ordem_faixa").toPandas())
p1_faixa["taxa_atraso"] = p1_faixa["atrasados"] / p1_faixa["voos_com_atraso_valido"]
p1_faixa["taxa_cancelamento"] = p1_faixa["cancelados"] / p1_faixa["etapas"]
salvar_tabela(p1_faixa, "resp_p1_faixa_horaria")
display(p1_faixa)
resumo["P1_atraso_por_faixa"] = "; ".join(f"{r.faixa_horaria} {pct(r.taxa_atraso)}" for r in p1_faixa.itertuples())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Estratos (base para P2–P4)
# MAGIC Contagens por estrato *aeroporto de origem × mês × faixa horária* e por condição.

# COMMAND ----------

ESTRATO = ["sk_aeroporto_origem", "ano_mes", "faixa_horaria"]


def razao_mh(estratos, exposto, nao_exposto="Bom nas duas pontas", y="atrasados", n="voos"):
    """Razão de risco de Mantel-Haenszel do grupo `exposto` contra `nao_exposto`."""
    e = estratos[estratos.grupo == exposto].set_index(ESTRATO)
    b = estratos[estratos.grupo == nao_exposto].set_index(ESTRATO)
    j = e[[y, n]].join(b[[y, n]], lsuffix="_e", rsuffix="_b", how="inner")
    j = j[(j[f"{n}_e"] > 0) & (j[f"{n}_b"] > 0)]
    tot = j[f"{n}_e"] + j[f"{n}_b"]
    num = (j[f"{y}_e"] * j[f"{n}_b"] / tot).sum()
    den = (j[f"{y}_b"] * j[f"{n}_e"] / tot).sum()
    return num / den if den else np.nan


estratos_cenario = (escopo.filter("atraso_valido")
    .groupBy(*ESTRATO, F.col("cenario_clima").alias("grupo"))
    .agg(F.count("*").alias("voos"), F.sum("atraso_int").alias("atrasados"),
         F.sum("atraso30_int").alias("atrasados_30"))
    .toPandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 · Efeito do tempo adverso na origem, no destino e em ambos
# MAGIC Quanto aumentam as taxas de atraso e de cancelamento com tempo adverso na origem (na partida
# MAGIC prevista), no destino (na chegada prevista) e em ambos, em relação a tempo bom nas duas pontas?

# COMMAND ----------

ORDEM = ["Bom nas duas pontas", "Adverso só na origem", "Adverso só no destino", "Adverso nas duas pontas"]

p2 = (escopo.groupBy("cenario_clima")
    .agg(F.count("*").alias("etapas"), F.sum("cancelado_int").alias("cancelados"),
         F.sum(F.col("atraso_valido").cast("int")).alias("voos"),
         F.sum("atraso_int").alias("atrasados"), F.sum("atraso30_int").alias("atrasados_30"),
         F.expr("percentile(CASE WHEN atraso_valido THEN atraso_chegada_min END, 0.5)").alias("atraso_mediano_min"))
    .toPandas().set_index("cenario_clima").loc[ORDEM].reset_index())
p2["taxa_atraso"] = p2.atrasados / p2.voos
p2["ic95_inf"], p2["ic95_sup"] = zip(*[wilson(a, n) for a, n in zip(p2.atrasados, p2.voos)])
p2["taxa_atraso_30"] = p2.atrasados_30 / p2.voos
p2["taxa_cancelamento"] = p2.cancelados / p2.etapas
bom = p2.iloc[0]
p2["razao_bruta"] = p2.taxa_atraso / bom.taxa_atraso
p2["razao_ajustada_mh"] = [1.0] + [razao_mh(estratos_cenario, c) for c in ORDEM[1:]]
p2["razao_ajustada_mh_30"] = [1.0] + [razao_mh(estratos_cenario, c, y="atrasados_30") for c in ORDEM[1:]]
p2["razao_cancelamento"] = p2.taxa_cancelamento / bom.taxa_cancelamento
salvar_tabela(p2, "resp_p2_cenario")
display(p2)

fig, ax = plt.subplots(figsize=(9, 4.8))
x = np.arange(len(p2))
ax.bar(x, 100 * p2.taxa_atraso, color=[AZUL] + [LARANJA] * 3, width=0.6)
ax.errorbar(x, 100 * p2.taxa_atraso, yerr=[100 * (p2.taxa_atraso - p2.ic95_inf), 100 * (p2.ic95_sup - p2.taxa_atraso)],
            fmt="none", ecolor=TEXTO_2, capsize=4, lw=1)
for i, r in p2.iterrows():
    rotulo = pct(r.taxa_atraso) + ("" if i == 0 else f"\n{br(r.razao_ajustada_mh, 2)}× (ajust.)")
    ax.text(i, 100 * r.ic95_sup + 0.6, rotulo, ha="center", va="bottom", fontsize=9, color=TEXTO)
ax.set_xticks(x, [c.replace(" nas ", "\nnas ").replace(" só ", "\nsó ") for c in p2.cenario_clima])
ax.set_ylabel("Voos com chegada atrasada (%)")
ax.set_ylim(0, 100 * p2.ic95_sup.max() * 1.35)
ax.grid(axis="x", visible=False)
finalizar(fig, ax, "P2 · Taxa de atraso por condição do tempo na origem e no destino",
          "Barras de erro: IC 95% · razão ajustada por aeroporto, mês e faixa horária (Mantel-Haenszel)",
          "p2_atraso_por_cenario.png")

fig, ax = plt.subplots(figsize=(9, 4.4))
ax.bar(x, 100 * p2.taxa_cancelamento, color=[AZUL] + [LARANJA] * 3, width=0.6)
for i, r in p2.iterrows():
    ax.text(i, 100 * r.taxa_cancelamento, f"{pct(r.taxa_cancelamento)}", ha="center", va="bottom", fontsize=9)
ax.set_xticks(x, [c.replace(" nas ", "\nnas ").replace(" só ", "\nsó ") for c in p2.cenario_clima])
ax.set_ylabel("Etapas canceladas (%)")
eixo_br(ax, "y", 1)
ax.grid(axis="x", visible=False)
finalizar(fig, ax, "P2 · Taxa de cancelamento por condição do tempo na origem e no destino",
          "Etapas em escopo · 2024–2025", "p2_cancelamento_por_cenario.png")

for r in p2.itertuples():
    resumo[f"P2_{r.cenario_clima}"] = (f"atraso {pct(r.taxa_atraso)} [IC95 {pct(r.ic95_inf)}–{pct(r.ic95_sup)}], "
        f"razão bruta {r.razao_bruta:.2f}, ajustada {r.razao_ajustada_mh:.2f} (>30 min: {r.razao_ajustada_mh_30:.2f}), "
        f"cancelamento {pct(r.taxa_cancelamento)} ({r.razao_cancelamento:.2f}×), mediana {r.atraso_mediano_min:.0f} min, "
        f"n={r.voos:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 · Que condição pesa mais
# MAGIC Visibilidade baixa ou teto baixo, trovoada, chuva forte, ou vento e rajadas? Para cada fator:
# MAGIC voos com o fator presente em **pelo menos uma ponta**, comparados a voos com tempo bom nas
# MAGIC duas pontas. Os fatores podem ocorrer juntos (trovoada costuma vir com chuva forte), então
# MAGIC também é mostrado o caso em que o fator aparece **sozinho**.

# COMMAND ----------

FATORES = [("fl_visibilidade_baixa", "Visibilidade < 5 km"), ("fl_teto_baixo", "Teto < 1.500 ft"),
           ("fl_trovoada", "Trovoada"), ("fl_chuva_forte", "Chuva forte"), ("fl_rajada_forte", "Rajada ≥ 25 kt")]
co = condicao.select(*[F.col(c).alias(f"{c}_o") for c in ["sk_condicao", "qtd_fatores"] + [f for f, _ in FATORES]])
cd = condicao.select(*[F.col(c).alias(f"{c}_d") for c in ["sk_condicao", "qtd_fatores"] + [f for f, _ in FATORES]])
com_fatores = (escopo.filter("atraso_valido")
    .join(co, escopo.sk_condicao_origem == F.col("sk_condicao_o"))
    .join(cd, escopo.sk_condicao_destino == F.col("sk_condicao_d")))

linhas, estratos_fator = [], []
for f, rotulo in FATORES:
    presente = F.col(f"{f}_o") | F.col(f"{f}_d")
    # "sozinho": é o único fator nas duas pontas somadas
    sozinho = presente & ((F.col("qtd_fatores_o") + F.col("qtd_fatores_d"))
                          == (F.col(f"{f}_o").cast("int") + F.col(f"{f}_d").cast("int")))
    g = (com_fatores.withColumn("grupo", F.when(F.col("cenario_clima") == "Bom nas duas pontas", "Bom nas duas pontas")
                                .when(presente, rotulo))
         .filter("grupo IS NOT NULL"))
    est = g.groupBy(*ESTRATO, "grupo").agg(F.count("*").alias("voos"), F.sum("atraso_int").alias("atrasados")).toPandas()
    a = g.filter(F.col("grupo") == rotulo).agg(F.count("*").alias("n"), F.sum("atraso_int").alias("y")).first()
    s = com_fatores.filter(sozinho).agg(F.count("*").alias("n"), F.sum("atraso_int").alias("y")).first()
    ic = wilson(a.y, a.n)
    linhas.append({"fator": rotulo, "voos": a.n, "atrasados": a.y, "taxa_atraso": a.y / a.n,
                   "ic95_inf": ic[0], "ic95_sup": ic[1], "razao_ajustada_mh": razao_mh(est, rotulo),
                   "voos_fator_sozinho": s.n, "taxa_atraso_fator_sozinho": (s.y / s.n) if s.n else np.nan})

p3 = pd.DataFrame(linhas).sort_values("razao_ajustada_mh", ascending=False).reset_index(drop=True)
salvar_tabela(p3, "resp_p3_fator")
display(p3)

fig, ax = plt.subplots(figsize=(9, 4.6))
d = p3.sort_values("razao_ajustada_mh")
ax.barh(d.fator, d.razao_ajustada_mh, color=LARANJA, height=0.6)
ax.axvline(1, color=TEXTO_2, lw=1)
ax.text(1, -0.75, " 1,0 = mesmo risco do tempo bom", color=TEXTO_2, fontsize=8.5, va="center")
for y, r in enumerate(d.itertuples()):
    ax.text(r.razao_ajustada_mh + 0.03, y, f"{br(r.razao_ajustada_mh, 2)}×  ({pct(r.taxa_atraso)}, n={br(r.voos, 0)})",
            va="center", fontsize=8.5, color=TEXTO_2)
ax.set_xlim(0, d.razao_ajustada_mh.max() * 1.45)
ax.set_ylim(-1.1, len(d) - 0.5)
eixo_br(ax, "x", 1)
ax.set_xlabel("Razão de risco de atraso ajustada vs. tempo bom nas duas pontas")
ax.grid(axis="y", visible=False)
finalizar(fig, ax, "P3 · Quanto cada condição aumenta o risco de atraso",
          "Fator presente em pelo menos uma ponta · ajuste por aeroporto, mês e faixa horária",
          "p3_risco_por_fator.png")

for r in p3.itertuples():
    resumo[f"P3_{r.fator}"] = (f"razão ajustada {r.razao_ajustada_mh:.2f}, atraso {pct(r.taxa_atraso)}, n={r.voos:,}; "
                               f"sozinho: {pct(r.taxa_atraso_fator_sozinho)} (n={r.voos_fator_sozinho:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 · Quanto do atraso pode ser associado ao clima
# MAGIC Que parcela dos atrasos pode ser associada ao tempo adverso (excesso em relação à taxa sob
# MAGIC tempo bom) e quanto do atraso ocorre com tempo bom na origem e no destino?

# COMMAND ----------

e = estratos_cenario.copy()
e["adverso"] = e.grupo != "Bom nas duas pontas"
por_estrato = e.groupby(ESTRATO + ["adverso"])[["voos", "atrasados"]].sum().unstack("adverso").fillna(0)
por_estrato.columns = [f"{m}_{'adv' if a else 'bom'}" for m, a in por_estrato.columns]
comparaveis = por_estrato[(por_estrato.voos_bom > 0) & (por_estrato.voos_adv > 0)].copy()
comparaveis["esperados_adv"] = comparaveis.voos_adv * comparaveis.atrasados_bom / comparaveis.voos_bom

total_atrasados = e.atrasados.sum()
atrasados_bom = e[~e.adverso].atrasados.sum()
atrasados_adv = e[e.adverso].atrasados.sum()
excesso = (comparaveis.atrasados_adv - comparaveis.esperados_adv).sum()
cobertura_excesso = comparaveis.atrasados_adv.sum() / atrasados_adv  # parte dos atrasos adversos em estratos comparáveis
# Versão bruta (sem estratos), para comparação
taxa_bom = atrasados_bom / e[~e.adverso].voos.sum()
excesso_bruto = atrasados_adv - e[e.adverso].voos.sum() * taxa_bom

p4 = pd.DataFrame([
    {"componente": "Atrasos com tempo bom nas duas pontas", "atrasos": atrasados_bom},
    {"componente": "Atrasos com tempo adverso que ocorreriam mesmo com tempo bom", "atrasos": atrasados_adv - excesso},
    {"componente": "Excesso de atrasos associado ao tempo adverso", "atrasos": excesso},
])
p4["participacao"] = p4.atrasos / total_atrasados
salvar_tabela(p4, "resp_p4_decomposicao")
display(p4)

fig, ax = plt.subplots(figsize=(10, 3.4))
esquerda = 0
for r, cor in zip(p4.itertuples(), (AZUL, CINZA, LARANJA)):
    largura = max(100 * r.participacao, 0)  # excesso negativo (sem efeito do clima) não é desenhado
    ax.barh([0], [largura], left=esquerda, color=cor, height=0.5, edgecolor=FUNDO, linewidth=2)
    if largura >= 8:   # cabe dentro da barra
        ax.text(esquerda + largura / 2, 0, pct(r.participacao), ha="center", va="center",
                color="white" if cor != CINZA else TEXTO, fontsize=11, fontweight="bold")
    else:              # fatia estreita: rótulo acima da barra
        ax.annotate(pct(r.participacao), (esquerda + largura / 2, 0.25), xytext=(0, 10),
                    textcoords="offset points", ha="center", fontsize=10, fontweight="bold",
                    arrowprops=dict(arrowstyle="-", color=TEXTO_2, lw=0.8))
    esquerda += largura
ax.set_xlim(0, 100)
ax.set_ylim(-0.4, 0.75)
ax.set_yticks([])
eixo_br(ax, "x")
ax.grid(False)
ax.legend([plt.Rectangle((0, 0), 1, 1, color=c) for c in (AZUL, CINZA, LARANJA)],
          ["Tempo bom nas duas pontas", "Tempo adverso, mas atrasaria mesmo assim",
           "Excesso associado ao tempo adverso"], loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3, fontsize=9)
finalizar(fig, ax, "P4 · De onde vêm os atrasos (parcela de todos os voos atrasados, %)",
          "Excesso = atrasos com tempo adverso − esperados pela taxa de tempo bom do mesmo aeroporto, mês e faixa horária",
          "p4_decomposicao_atrasos.png")

resumo["P4_total_voos_atrasados"] = f"{total_atrasados:,}"
resumo["P4_parcela_com_tempo_bom_nas_duas_pontas"] = pct(atrasados_bom / total_atrasados)
resumo["P4_excesso_associado_ao_clima"] = f"{excesso:,.0f} atrasos = {pct(excesso / total_atrasados)} do total"
resumo["P4_excesso_bruto_sem_estratos"] = f"{excesso_bruto:,.0f} = {pct(excesso_bruto / total_atrasados)} do total"
resumo["P4_atrasos_adversos_em_estratos_comparaveis"] = pct(cobertura_excesso)

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 · Efeito cascata ao longo do dia
# MAGIC A taxa de atraso cresce ao longo do dia mesmo com tempo bom? Se a resposta for sim, atrasos
# MAGIC acumulados nas primeiras etapas do dia se propagam para as seguintes, independentemente do clima.

# COMMAND ----------

p5 = (escopo.filter("atraso_valido")
    .withColumn("grupo", F.when(F.col("cenario_clima") == "Bom nas duas pontas", "Tempo bom nas duas pontas")
                .otherwise("Tempo adverso em alguma ponta"))
    .groupBy("sk_hora_partida", "grupo")
    .agg(F.count("*").alias("voos"), F.sum("atraso_int").alias("atrasados"),
         F.avg("atraso_partida_min").alias("atraso_partida_medio_min"))
    .orderBy("sk_hora_partida").toPandas())
p5["taxa_atraso"] = p5.atrasados / p5.voos
salvar_tabela(p5, "resp_p5_hora")
display(p5)

fig, ax = plt.subplots(figsize=(10, 4.6))
for grupo, cor in (("Tempo bom nas duas pontas", AZUL), ("Tempo adverso em alguma ponta", LARANJA)):
    d = p5[(p5.grupo == grupo) & (p5.voos >= 300)]  # horas com poucos voos ficam de fora
    ax.plot(d.sk_hora_partida, 100 * d.taxa_atraso, color=cor, lw=2, marker="o", ms=4, label=grupo)
    ult = d.iloc[-1]
    ax.text(ult.sk_hora_partida + 0.3, 100 * ult.taxa_atraso, grupo.replace("Tempo ", "").replace(" nas duas pontas", ""),
            color=TEXTO_2, fontsize=8.5, va="center")
ax.set_xticks(range(0, 24, 2), [f"{h}h" for h in range(0, 24, 2)])
ax.set_xlim(-0.5, 25.5)
ax.set_ylim(bottom=0)
ax.set_xlabel("Hora prevista de partida (horário de Brasília)")
ax.set_ylabel("Voos com chegada atrasada (%)")
eixo_br(ax, "y")
ax.legend(loc="upper left")
finalizar(fig, ax, "P5 · Taxa de atraso ao longo do dia",
          "Horas com pelo menos 300 voos no grupo · etapas em escopo, 2024–2025", "p5_atraso_por_hora.png")

bom_h = p5[(p5.grupo == "Tempo bom nas duas pontas") & (p5.voos >= 300)].set_index("sk_hora_partida")
manha = bom_h.loc[bom_h.index.isin(range(6, 10))]
noite = bom_h.loc[bom_h.index.isin(range(18, 22))]
taxa_manha = manha.atrasados.sum() / manha.voos.sum()
taxa_noite = noite.atrasados.sum() / noite.voos.sum()
correl = np.corrcoef(bom_h.loc[bom_h.index >= 5].index, bom_h.loc[bom_h.index >= 5].taxa_atraso)[0, 1]
resumo["P5_tempo_bom_6h_9h"] = pct(taxa_manha)
resumo["P5_tempo_bom_18h_21h"] = pct(taxa_noite)
resumo["P5_razao_noite_manha"] = f"{taxa_noite / taxa_manha:.2f}×"
resumo["P5_correlacao_hora_x_taxa_tempo_bom_5h_23h"] = f"{correl:.2f}"
resumo["P5_hora_pico_tempo_bom"] = f"{bom_h.taxa_atraso.idxmax()}h ({pct(bom_h.taxa_atraso.max())})"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo numérico
# MAGIC Copie a saída desta célula: ela traz os números usados na discussão do README.

# COMMAND ----------

n_escopo = escopo.count()
n_valido = escopo.filter("atraso_valido").count()
print(f"Etapas em escopo com observação nas duas pontas: {n_escopo:,} (com atraso válido: {n_valido:,})")
for chave, valor in resumo.items():
    print(f"{chave}: {valor}")
salvar_tabela(pd.DataFrame(list(resumo.items()), columns=["indicador", "valor"]), "resp_resumo")
print("\nGráficos salvos em", PASTA_FIG, ":", sorted(os.listdir(PASTA_FIG)))
