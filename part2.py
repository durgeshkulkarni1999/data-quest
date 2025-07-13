import boto3
import requests

#Configurations
S3_BUCKET_NAME = "rearc-asessment"
DATA_SOURCE = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"

# Initialize S3 resource 
s3 = boto3.resource('s3')
bucket = s3.Bucket(S3_BUCKET_NAME)

# Request the data from API
r = requests.get(DATA_SOURCE)
data = r.text

# Upload the data to S3
bucket.put_object(Key="population.json", Body=data)