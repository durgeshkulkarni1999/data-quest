# lambda_analytics/part3.py

import pandas as pd

def run_analytics(bucket_name: str, series_key: str, pop_key: str):
    """
    Loads the two datasets from S3 via Data Wrangler,
    performs the Part 3 analytics, and returns the three
    reports as DataFrames.
    """

    s3_path = f"s3://{bucket_name}"

    #Load BCL series CSV
    df_series = pd.s3.read_csv(f"{s3_path}/{series_key}", sep="\t")

    # --- Load population JSON via Wrangler ---
    df_pop = pd.s3.read_json(f"{s3_path}/{pop_key}")

    # --- Data cleaning ---
    df_series = df_series.rename(columns=lambda c: c.strip().lower())
    df_pop    = df_pop.rename(columns=lambda c: c.strip().lower().replace(" ", "_"))

    for df in (df_series, df_pop):
        str_cols = df.select_dtypes(include="object").columns
        for c in str_cols:
            df[c] = df[c].str.strip()

    df_series = df_series.dropna(subset=["series_id", "year", "period", "value"])
    df_series = df_series.drop(columns=["footnote_codes"], errors="ignore")
    df_series = df_series.drop_duplicates(
        subset=["series_id", "year", "period"],
        keep="last"
    ).reset_index(drop=True)

    df_pop = df_pop.dropna(subset=["year", "population"])
    df_pop["year"]       = df_pop["year"].astype(int)
    df_pop["population"] = df_pop["population"].astype(int)

    # --- Report 1: Population stats 2013–2018 ---
    pop_stats = (
        df_pop
        .query("year >= 2013 and year <= 2018")["population"]
        .agg(["mean", "std"])
        .rename({"mean": "mean_population", "std": "std_population"})
    )

    # --- Report 2: Best year per series ---
    yearly = (
        df_series
        .groupby(["series_id", "year"], as_index=False)["value"]
        .sum()
        .rename(columns={"value": "total_value"})
    )
    best_year = (
        yearly.loc[
            yearly.groupby("series_id")["total_value"].idxmax()
        ]
        .reset_index(drop=True)
        .rename(columns={"total_value": "value"})
    )

    # --- Report 3: PRS30006032 Q01 + population ---
    subset = df_series[
        (df_series["series_id"] == "PRS30006032") &
        (df_series["period"]    == "Q01")
    ].copy()

    report = (
        subset
        .merge(df_pop[["year", "population"]], on="year", how="left")
        .rename(columns={"population": "Population"})
        [["series_id", "year", "period", "value", "Population"]]
    )

    return pop_stats, best_year, report
