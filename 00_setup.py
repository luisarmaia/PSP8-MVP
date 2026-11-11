# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup do ambiente
# MAGIC
# MAGIC Cria o schema e o Volume que recebem os arquivos brutos da coleta.
# MAGIC
# MAGIC | Objeto | Nome | Uso |
# MAGIC |---|---|---|
# MAGIC | Catálogo | `workspace` | catálogo padrão do Databricks Free Edition |
# MAGIC | Schema | `mvp_voos` | todas as tabelas do MVP (bronze, silver, gold) |
# MAGIC | Volume | `raw` | arquivos brutos baixados das fontes (VRA, aeródromos, METAR) |
# MAGIC
# MAGIC Rode uma vez. Depois faça o upload do conteúdo de `data/raw/` para o Volume
# MAGIC (passo a passo no README, seção *Como reproduzir*).

# COMMAND ----------

CATALOGO = "workspace"
SCHEMA = "mvp_voos"
VOLUME = "raw"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA} "
          "COMMENT 'MVP UnB: atrasos de voos domésticos e condições meteorológicas'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{SCHEMA}.{VOLUME} "
          "COMMENT 'Arquivos brutos: VRA (ANAC), aeródromos (ANAC), METAR (IEM)'")

print(f"Volume pronto em /Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conferência após o upload
# MAGIC Lista o que já está no Volume. Rode de novo depois de enviar os arquivos.

# COMMAND ----------

import os

raiz = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
total = 0
for pasta, _, arquivos in sorted(os.walk(raiz)):
    for nome in sorted(arquivos):
        caminho = os.path.join(pasta, nome)
        total += os.path.getsize(caminho)
        print(f"{os.path.relpath(caminho, raiz):45s} {os.path.getsize(caminho)/1e6:8.1f} MB")
print(f"\nTotal: {total/1e6:.1f} MB")
