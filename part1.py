import boto3, requests, os
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BUCKET = "rearc-asessment"
DATA_SOURCE = "https://download.bls.gov/pub/time.series/pr/"
HEADERS = {"User-Agent": "Durgesh Kulkarni (durgesh.kulkarni99@gmail.com)"}

s3       = boto3.resource("s3")
bucket   = s3.Bucket(BUCKET)
existing = {obj.key for obj in bucket.objects.all()}
seen     = set()

# scrape the index
r    = requests.get(DATA_SOURCE, headers=HEADERS); r.raise_for_status()
soup = BeautifulSoup(r.text, "html.parser")
remote_files = [a["href"] for a in soup.find_all("a", href=True)
                if not a["href"].endswith("/") and a["href"] != "../"]

for href in remote_files:
    file_name = os.path.basename(href)             # <-- strip directories
    seen.add(file_name)

    remote_url = urljoin(DATA_SOURCE, href)
    resp = requests.get(remote_url, headers=HEADERS, stream=True)
    resp.raise_for_status()
    data = resp.content

    if file_name not in existing:
        bucket.put_object(Key=file_name, Body=data)
        print(f"Uploaded new: {file_name}")
    else:
        old = bucket.Object(file_name).get()["Body"].read()
        if old != data:
            bucket.put_object(Key=file_name, Body=data)
            print(f"Updated: {file_name}")
        else:
            print(f"Skipped (no change): {file_name}")

# delete anything left over that we didn’t see
for key in existing - seen:
    bucket.Object(key).delete()
    print(f"Deleted stale: {key}")
