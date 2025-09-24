
import argparse
import json
from pathlib import Path

from src.data_etl.utils import load_inputs
from src.data_etl.metrics import compute_metrics

try:
    from spark_jobs.jobs import (
        build_spark,
        load_inputs_spark,
        recommend_chains_for_each_drug,
        most_common_quantity_per_drug,
    )
except Exception:
    build_spark = None
    load_inputs_spark = None
    recommend_chains_for_each_drug = None
    most_common_quantity_per_drug = None


def generate_metrics(pharmacies_path_or_dir: str, claims_dir: str, reverts_dir: str, output_metrics_json: str):
    pharmacies, claims, reverts = load_inputs(
        pharmacies_path_or_dir=pharmacies_path_or_dir,
        claims_dir=claims_dir,
        reverts_dir=reverts_dir,
    )
    valid_npis = set(pharmacies.keys())
    metrics = compute_metrics(claims, reverts, valid_npis=valid_npis)

    output = Path(output_metrics_json)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w") as f:
        json.dump(metrics, f, indent=2)
    print(f"[Metrics] Wrote: {output} (rows={len(metrics)})")


def run_recommendations_and_quantities(
    pharmacies_file: str,
    claims_dir: str,
    reverts_dir: str,
    out_top2_chain: str,
    out_most_common_qty: str,
):
    if build_spark is None or load_inputs_spark is None:
        raise RuntimeError(
            "PySpark not available. Install requirements and try again.")

    spark = build_spark(app_name="PharmacySparkJobs")
    df_ph, df_claims, df_reverts = load_inputs_spark(
        spark,
        pharmacies_path=pharmacies_file,
        claims_dir=claims_dir,
        reverts_dir=reverts_dir,
    )

    recommend_chains_for_each_drug(
        df_ph, df_claims, df_reverts, out_top2_chain)
    most_common_quantity_per_drug(df_ph, df_claims, out_most_common_qty)

    spark.stop()
    print(f"[Spark] Wrote:\n - {out_top2_chain}\n - {out_most_common_qty}")


def parse_args():
    p = argparse.ArgumentParser(
        description="Pharmacy ETL — Pure Python metrics and PySpark analytics."
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # Metrics
    p_metrics = sub.add_parser(
        "metrics", help="Compute metrics per (npi, ndc) [no Spark]")
    p_metrics.add_argument("--pharmacies", required=True,
                           help="Path to pharmacies file or dir (e.g., data/pharmacies)")
    p_metrics.add_argument("--claims", required=True,
                           help="Directory with claims JSON files (e.g., data/claims)")
    p_metrics.add_argument("--reverts", required=True,
                           help="Directory with reverts JSON files (e.g., data/reverts)")
    p_metrics.add_argument(
        "--output", required=True, help="Output JSON path (e.g., output/metrics_by_npi_ndc.json)")

    # Spark analytics
    p_spark = sub.add_parser(
        "spark", help="Run recommendations and most-common quantities [PySpark]")
    p_spark.add_argument("--pharmacies", required=True,
                         help="Pharmacies CSV file (e.g., data/pharmacies-....csv)")
    p_spark.add_argument("--claims", required=True,
                         help="Directory with claims JSON files (e.g., data/claims)")
    p_spark.add_argument("--reverts", required=True,
                         help="Directory with reverts JSON files (e.g., data/reverts)")
    p_spark.add_argument("--output-top2-chain", required=True,
                         help="Output JSON path for chain recommendations")
    p_spark.add_argument("--output-most-common-qty", required=True,
                         help="Output JSON path for most common qty")

    return p.parse_args()


def main():
    args = parse_args()
    if args.cmd == "metrics":
        generate_metrics(
            pharmacies_path_or_dir=args.pharmacies,
            claims_dir=args.claims,
            reverts_dir=args.reverts,
            output_metrics_json=args.output,
        )
    elif args.cmd == "spark":
        run_recommendations_and_quantities(
            pharmacies_file=args.pharmacies,
            claims_dir=args.claims,
            reverts_dir=args.reverts,
            out_top2_chain=args.output_top2_chain,
            out_most_common_qty=args.output_most_common_qty,
        )


if __name__ == "__main__":
    main()
