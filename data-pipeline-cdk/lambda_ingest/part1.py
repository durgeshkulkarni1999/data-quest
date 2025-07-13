# lambda_ingest/part1.py

import boto3
import requests
import os
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def sync_pr_series(bucket_name: str, bls_url: str):
    """
    Mirror all files under bls_url into the root of the given S3 bucket,
    skipping duplicates, updating changed files, and deleting removed ones.
    """
    # Set up S3
    s3     = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)

    # List existing keys
    existing = {obj.key for obj in bucket.objects.all()}
    seen     = set()

    # Scrape the BLS index page for file hrefs
    headers = {
        "User-Agent": "Durgesh Kulkarni (durgesh.kulkarni99@gmail.com)"
    }
    resp = requests.get(bls_url, headers=headers)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    remote_files = [
        a["href"] for a in soup.find_all("a", href=True)
        if not a["href"].endswith("/") and a["href"] != "../"
    ]

    # Download & sync each file
    for href in remote_files:
        file_name = os.path.basename(href)
        seen.add(file_name)

        download_url = urljoin(bls_url, href)
        r = requests.get(download_url, headers=headers, stream=True)
        r.raise_for_status()
        data = r.content

        if file_name not in existing:
            bucket.put_object(Key=file_name, Body=data)
            print(f"Uploaded new: {file_name}")
        else:
            old = bucket.Object(file_name).get()["Body"].read()
            if old != data:
                bucket.put_object(Key=file_name, Body=data)
                print(f"Updated:      {file_name}")
            else:
                print(f"Skipped (no change): {file_name}")

    # Delete any S3 objects that were not seen upstream
    for stale_key in existing - seen:
        bucket.Object(stale_key).delete()
        print(f"Deleted stale: {stale_key}")