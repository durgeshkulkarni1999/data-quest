# lambda_analytics/handler.py

import os
import json
import boto3
import pandas as pd

def main(event, context):
    # Environment
    bucket_name = os.environ["BUCKET"]
    series_key  = os.environ["SERIES_KEY"]
    pop_key     = os.environ["POP_KEY"]

    s3 = boto3.client("s3")

    # 1) Load the time-series CSV from S3
    series_obj = s3.get_object(Bucket=bucket_name, Key=series_key)
    df_series  = pd.read_csv(series_obj["Body"], sep="\t")

    # 2) Load the population JSON from S3
    pop_obj = s3.get_object(Bucket=bucket_name, Key=pop_key)
    payload = json.loads(pop_obj["Body"].read())
    df_pop  = pd.json_normalize(payload, record_path="data")

    # 3) Clean & coerce types
    # trim whitespace
    df_series = df_series.rename(columns=lambda c: c.strip().lower())
    df_pop    = df_pop.rename(columns=lambda c: c.strip().lower().replace(" ", "_"))
    # numeric types
    df_series = df_series.drop(columns=['footnote_codes'])
    df_series["year"]  = pd.to_numeric(df_series["year"], errors="coerce")
    df_series["value"] = pd.to_numeric(df_series["value"], errors="coerce")
    df_pop["year"]       = pd.to_numeric(df_pop["year"], errors="coerce")
    df_pop["population"] = pd.to_numeric(df_pop["population"], errors="coerce")

    # 4) Population stats [2013–2018] 
    pop_sub = df_pop.query("year >= 2013 and year <= 2018")
    pop_stats = pop_sub["population"].agg(["mean", "std"]).rename({
        "mean": "mean_population",
        "std":  "std_population"
    })
    print("Population 2013–2018 statistics:")
    print(pop_stats.to_dict())

    #5) Best-year per series
    yearly = (
        df_series
        .groupby(["series_id", "year"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "total_value"})
    )
    idx = yearly.groupby("series_id")["total_value"].idxmax()
    best_year_df = yearly.loc[idx].reset_index(drop=True) \
                          .rename(columns={"total_value": "value"})
    print(f" Best year per series ({len(best_year_df)} series):")
    # log first 5 as example
    for _, row in best_year_df.head(5).iterrows():
        print(f"   • {row.series_id}: {int(row.year)} → {row.value}")

    # PRS30006032 Q01 + Population join
    subset = (df_series[(df_series["series_id"] == "PRS30006032") & (df_series["period"] == "Q01")].copy())

    pop_small = df_pop[["year", "population"]]
    result = pd.merge(subset,
        pop_small,
        left_on="year",
        right_on="year",
        how="left"
    )
    report = result[["series_id", "year", "period", "value", "population"]]
    print("PRS30006032 Q01 with population:")
    # log all rows (usually one per year)
    for _, row in report.iterrows():
        print(f"   • Year {int(row.year)}: value={row.value}, population={int(row.population)}")

    return {
        "status": "analytics complete",
        "population_stats": pop_stats.to_dict(),
        "best_years_count": len(best_year_df),
        "prs30006032_rows": len(report)
    }
