# lambda_ingest/part2.py

import boto3
import requests
import json

def fetch_and_store_population(bucket_name: str, api_url: str):
    """
    Fetch JSON from the given API URL and write it to S3 as population.json.
    """
    # 1) Fetch from the API
    headers = {
        "Accept": "application/json",
        "User-Agent": "Durgesh Kulkarni (durgesh.kulkarni99@gmail.com)"
    }
    resp = requests.get(api_url, headers=headers)
    resp.raise_for_status()          # abort on 4xx/5xx

    # 2) Parse & serialize
    payload = resp.json()            # native Python dict/list
    body    = json.dumps(payload)    # back to JSON text

    # 3) Upload to S3
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket      = bucket_name,
        Key         = "population.json",
        Body        = body,
        ContentType = "application/json"
    )
