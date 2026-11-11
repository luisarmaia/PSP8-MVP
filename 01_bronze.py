# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Camada bronze
# MAGIC
# MAGIC Carrega os arquivos brutos do Volume em tabelas Delta **sem alterar valores**.
# MAGIC Todas as colunas ficam como texto; tipagem e limpeza acontecem na camada silver.
# MAGIC
# MAGIC Únicas transformações aplicadas aqui (técnicas, não de negócio):
# MAGIC 1. **Nomes de colunas** padronizados em `snake_case`, porque o Delta não aceita
# MAGIC    espaços e acentos e porque o cabeçalho do VRA muda entre anos
# MAGIC    (ex.: `Código DI` → `Código Autorização (DI)`).
# MAGIC 2. **Colunas de linhagem**: `_arquivo_origem`, `_atualizado_em` (data que a ANAC
# MAGIC    grava na linha 1 de cada arquivo) e `_ingerido_em`.
# MAGIC
# MAGIC | Tabela | Fonte | Grão |
# MAGIC |---|---|---|
# MAGIC | `bronze_vra` | ANAC – Voo Regular Ativo | uma linha por etapa de voo |
# MAGIC | `bronze_metar` | IEM – METAR/SPECI | uma linha por observação meteorológica |
# MAGIC | `bronze_aerodromos` | ANAC – Aeródromos públicos | uma linha por aeródromo |
# MAGIC | `bronze_manifesto_coleta` | arquivos do Volume | uma linha por arquivo bruto (fonte, URL, tamanho, hash) |

# COMMAND ----------

import io
import os
import re
import unicodedata
from datetime import datetime, timezone

import pandas as pd

CATALOGO, SCHEMA, VOLUME = "workspace", "mvp_voos", "raw"
RAIZ = f"/Volumes/{CATALOGO}/{SCHEMA}/{VOLUME}"
INGERIDO_EM = datetime.now(timezone.utc).isoformat(timespec="seconds")


def tabela(nome):
    return f"{CATALOGO}.{SCHEMA}.{nome}"


def normalizar(texto):
    sem_acento = "".join(c for c in unicodedata.normalize("NFKD", texto)
                         if not unicodedata.combining(c))
    return " ".join(sem_acento.split()).casefold()


def snake(texto):
    return re.sub(r"[^a-z0-9]+", "_", normalizar(texto)).strip("_")


def arquivos(padrao):
    """Busca recursiva no Volume: funciona com ou sem as subpastas da coleta."""
    regex = re.compile(padrao, re.IGNORECASE)
    achados = []
    for pasta, _, nomes in os.walk(RAIZ):
        achados += [os.path.join(pasta, n) for n in nomes if regex.fullmatch(n)]
    return sorted(achados)


def ler_csv_anac(caminho):
    """CSV da ANAC: ';', linha 1 opcional 'Atualizado em: AAAA-MM-DD', UTF-8 ou Latin-1."""
    bruto = open(caminho, "rb").read()
    for codificacao in ("utf-8-sig", "latin-1"):
        try:
            texto = bruto.decode(codificacao)
            break
        except UnicodeDecodeError:
            continue
    atualizado_em = None
    primeira, _, resto = texto.partition("\n")
    if normalizar(primeira).startswith("atualizado em"):
        atualizado_em = primeira.split(":", 1)[1].strip()
        texto = resto
    df = pd.read_csv(io.StringIO(texto), sep=";", dtype=str, keep_default_na=False)
    return df, atualizado_em


def gravar(pdf, nome):
    sdf = spark.createDataFrame(pdf.astype(str))
    sdf.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela(nome))
    print(f"{tabela(nome)}: {spark.table(tabela(nome)).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## VRA

# COMMAND ----------

# Nome normalizado do cabeçalho original -> nome canônico.
COLUNAS_VRA = {
    "icao empresa aerea": "empresa_icao",
    "numero voo": "numero_voo",
    "codigo di": "codigo_di",
    "codigo autorizacao (di)": "codigo_di",
    "codigo tipo linha": "codigo_tipo_linha",
    "icao aerodromo origem": "origem_icao",
    "icao aerodromo destino": "destino_icao",
    "partida prevista": "partida_prevista",
    "data partida prevista": "partida_prevista",
    "partida real": "partida_real",
    "data partida real": "partida_real",
    "chegada prevista": "chegada_prevista",
    "data chegada prevista": "chegada_prevista",
    "chegada real": "chegada_real",
    "data chegada real": "chegada_real",
    "situacao voo": "situacao_voo",
    "codigo justificativa": "codigo_justificativa",
}

ORDEM_VRA = list(dict.fromkeys(COLUNAS_VRA.values())) + ["_arquivo_origem", "_atualizado_em", "_ingerido_em"]

# Grava arquivo a arquivo (o 1º sobrescreve, os demais acrescentam) para não
# estourar a memória do Free Edition com ~2 milhões de linhas de uma vez.
for i, caminho in enumerate(arquivos(r"VRA_.*\.csv")):
    df, atualizado_em = ler_csv_anac(caminho)
    df.columns = [COLUNAS_VRA.get(normalizar(c), snake(c)) for c in df.columns]
    desconhecidas = set(df.columns) - set(COLUNAS_VRA.values())
    if desconhecidas:
        raise ValueError(f"colunas não mapeadas em {caminho}: {desconhecidas}")
    df["_arquivo_origem"] = os.path.relpath(caminho, RAIZ)
    df["_atualizado_em"] = atualizado_em or ""
    df["_ingerido_em"] = INGERIDO_EM
    df = df.reindex(columns=ORDEM_VRA, fill_value="")
    (spark.createDataFrame(df).write
        .mode("overwrite" if i == 0 else "append")
        .option("overwriteSchema", "true" if i == 0 else "false")
        .saveAsTable(tabela("bronze_vra")))
    print(f"{os.path.basename(caminho):20s} {len(df):>8,} linhas  atualizado em {atualizado_em}")

print(f"{tabela('bronze_vra')}: {spark.table(tabela('bronze_vra')).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## METAR

# COMMAND ----------

# Aceita o download manual (metar_2024.csv, metar_2025.csv) ou um arquivo por aeroporto e ano.
caminhos_metar = arquivos(r"(metar_.*|[A-Z]{4}_\d{4})\.csv")
print(f"{len(caminhos_metar)} arquivos METAR")

metar = (
    spark.read.option("header", True).option("inferSchema", False).csv(caminhos_metar)
    .selectExpr("*", "_metadata.file_name AS _arquivo_origem")
    .selectExpr("*", f"'{INGERIDO_EM}' AS _ingerido_em")
)
metar.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(tabela("bronze_metar"))
print(f"{tabela('bronze_metar')}: {spark.table(tabela('bronze_metar')).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aeródromos públicos

# COMMAND ----------

[caminho_aerodromos] = arquivos(r".*aerodromo.*\.csv")
aero, atualizado_em = ler_csv_anac(caminho_aerodromos)
aero.columns = [snake(c) for c in aero.columns]
aero["_arquivo_origem"] = os.path.relpath(caminho_aerodromos, RAIZ)
aero["_atualizado_em"] = atualizado_em or ""
aero["_ingerido_em"] = INGERIDO_EM
gravar(aero, "bronze_aerodromos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifesto da coleta (linhagem)
# MAGIC Registra cada arquivo bruto do Volume: fonte, URL de origem, tamanho, hash SHA-256
# MAGIC e data de envio ao Volume. Prova a persistência e permite verificar, no futuro,
# MAGIC se um arquivo foi trocado.

# COMMAND ----------

import hashlib

FONTES = [  # (padrão do nome do arquivo, fonte, URL de origem)
    (r"VRA_.*\.csv", "ANAC - Voo Regular Ativo (VRA)",
     "https://sistemas.anac.gov.br/dadosabertos/Voos e operações aéreas/Voo Regular Ativo (VRA)/"),
    (r"(metar_.*|[A-Z]{4}_\d{4})\.csv", "Iowa Environmental Mesonet - METAR/SPECI",
     "https://mesonet.agron.iastate.edu/request/download.phtml?network=BR__ASOS"),
    (r".*aerodromo.*\.csv", "ANAC - Lista de aeródromos públicos",
     "https://sistemas.anac.gov.br/dadosabertos/Aerodromos/Aeródromos Públicos/Lista de aeródromos públicos/"),
]

linhas = []
for padrao, fonte, url in FONTES:
    for caminho in arquivos(padrao):
        conteudo = open(caminho, "rb").read()
        linhas.append({
            "arquivo": os.path.relpath(caminho, RAIZ),
            "fonte": fonte,
            "url_origem": url,
            "bytes": str(len(conteudo)),
            "linhas_texto": str(conteudo.count(b"\n")),
            "sha256": hashlib.sha256(conteudo).hexdigest(),
            "enviado_ao_volume_em": datetime.fromtimestamp(
                os.path.getmtime(caminho), timezone.utc).isoformat(timespec="seconds"),
        })
manifesto = pd.DataFrame(linhas)
gravar(manifesto, "bronze_manifesto_coleta")
display(manifesto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência

# COMMAND ----------

display(spark.sql(f"""
  SELECT 'bronze_vra' AS tabela, COUNT(*) AS linhas FROM {tabela('bronze_vra')}
  UNION ALL SELECT 'bronze_metar', COUNT(*) FROM {tabela('bronze_metar')}
  UNION ALL SELECT 'bronze_aerodromos', COUNT(*) FROM {tabela('bronze_aerodromos')}
  UNION ALL SELECT 'bronze_manifesto_coleta', COUNT(*) FROM {tabela('bronze_manifesto_coleta')}
"""))
