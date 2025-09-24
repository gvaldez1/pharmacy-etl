from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from pathlib import Path


def build_spark(app_name="PharmacySparkJobs"):
    spark = (
        SparkSession.builder
        .master("local[10]")
        .appName(app_name)
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    return spark


# ---------- LOAD (Spark) ----------

def load_inputs_spark(
    spark: SparkSession,
    pharmacies_path: str,
    claims_dir: str,
    reverts_dir: str | None = None
):
    """
    Load pharmacies (CSV), claims (JSON) and reverts (JSON) using Spark.
    - claims_dir/reverts_dir are directories; recursive lookup is enabled.
    """
    # Pharmacies CSV
    df_ph = (
        spark.read.option("header", True)
        .csv(pharmacies_path)
        .select("npi", "chain")
        .dropna(subset=["npi"])
    )

    # Claims JSON (array or ndjson) from a directory (recursive)
    df_claims = (
        spark.read
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", True)
        .json(claims_dir)
        .select(
            "id",
            "npi",
            "ndc",
            "price",
            "quantity",
            "timestamp",
        )
        .dropna(subset=["id", "npi", "ndc", "price", "quantity"])
        .where(col("quantity") > 0)
    )

    # Filter claims to pharmacies dataset
    df_claims = df_claims.join(df_ph.select("npi"), "npi", "inner")

    # Reverts JSON (optional)
    if reverts_dir:
        df_reverts = (
            spark.read
            .option("multiLine", True)
            .option("mode", "PERMISSIVE")
            .option("recursiveFileLookup", True)
            .json(reverts_dir)
            .select("claim_id")
            .dropna(subset=["claim_id"])
        )
    else:
        df_reverts = spark.createDataFrame([], schema="claim_id string")

    return df_ph, df_claims, df_reverts


# ---------- RECOMMENDATION (Spark) ----------

def recommend_chains_for_each_drug(df_ph, df_claims, df_reverts, out_json_path: str):
    """
    For each ndc, recommend the top 2 chains with the lowest average unit price.
    Only NON-reverted claims are considered.
    Output format:
    [
      {"ndc": "...", "chain": [{"name": "health", "avg_price": 123.45}, {"name": "saint", "avg_price": 150.00}]},
      ...
    ]
    """
    # remove reverted claims
    df_valid = (
        df_claims.join(
            df_reverts,
            df_claims.id == df_reverts.claim_id,
            "left_anti"
        )
        .withColumn("unit_price", col("price") / col("quantity"))
        .join(df_ph, "npi", "inner")
    )

    # average unit price per (ndc, chain)
    df_chain_stats = df_valid.groupBy("ndc", "chain").agg(
        round(avg("unit_price"), 2).alias("avg_price")
    )

    # rank chains per ndc by lowest price, pick top 2
    w = Window.partitionBy("ndc").orderBy(asc("avg_price"), asc("chain"))

    df_ranked = df_chain_stats.withColumn(
        "rn", row_number().over(w)).where(col("rn") <= 2)

    df_out = (
        df_ranked.groupBy("ndc")
        .agg(collect_list(struct(col("chain").alias("name"), col("avg_price"))).alias("chain"))
        .orderBy("ndc")
    )

    rows = [row.asDict(recursive=True) for row in df_out.collect()]
    Path(out_json_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_json_path, "w") as f:
        json.dump(rows, f, indent=2)


# ---------- MOST COMMON QUANTITY (Spark) ----------

def most_common_quantity_per_drug(df_ph, df_claims, out_json_path: str):
    """
    For each ndc, return all most frequent quantities (ties included).
    Output:
    [
      {"ndc": "...", "most_prescribed_quantity": [ ... ]},
      ...
    ]
    """
    df_claims_f = df_claims.join(df_ph.select("npi"), "npi", "inner")

    df_claims_f = df_claims_f.where(col("quantity") > 0)

    freq = df_claims_f.groupBy("ndc", "quantity").agg(
        count("*").alias("count"))

    w = Window.partitionBy("ndc").orderBy(desc("count"))
    top = freq.withColumn("rk", dense_rank().over(w)).where(col("rk") <= 5)

    out_df = (
        top.groupBy("ndc")
        .agg(
            expr("sort_array(collect_list(struct(count, quantity)), false)").alias(
                "q_structs")
        )
        .withColumn(
            "most_prescribed_quantity",
            expr("transform(q_structs, x -> x.quantity)")
        )
        .drop("q_structs")
        .orderBy("ndc")
    )

    rows = [row.asDict(recursive=True) for row in out_df.collect()]
    Path(out_json_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_json_path, "w") as f:
        json.dump(rows, f, indent=2)
