import os
from part1 import sync_pr_series
from part2 import fetch_and_store_population

def main(event, context):
    sync_pr_series(
        bucket_name=os.environ["BUCKET"],
        bls_url=os.environ["BLS_URL"]
    )
    fetch_and_store_population(
        bucket_name=os.environ["BUCKET"],
        api_url=os.environ["API_URL"]
    )
    return {"status": "ingest complete"}
