# pipeline_stack.py

from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as targets,
    aws_sqs as sqs,
)
from aws_cdk.aws_lambda_event_sources import SqsEventSource
from constructs import Construct

class PipelineStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # 1) Import your existing S3 bucket
        bucket = s3.Bucket.from_bucket_name(self, "DataBucket", "rearc-asessment")

        # 2) Create an SQS queue for population.json notifications
        queue = sqs.Queue(self, "PopulationQueue",
            visibility_timeout=Duration.seconds(300)
        )

        # 3) Ingest Lambda (Part 1 + Part 2)
        ingest_fn = lambda_.Function(self, "IngestFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.main",
            code=lambda_.Code.from_asset("lambda_ingest"),
            environment={
                "BUCKET": bucket.bucket_name,
                "BLS_URL": "https://download.bls.gov/pub/time.series/pr/",
                "API_URL": "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
            },
            timeout=Duration.minutes(5),
        )
        bucket.grant_read_write(ingest_fn)

        # 4) Schedule ingest_fn to run daily at midnight UTC
        events.Rule(self, "DailyIngestRule",
            schedule=events.Schedule.cron(hour="0", minute="0"),
            targets=[ targets.LambdaFunction(ingest_fn) ]
        )

        # 5) Fire an SQS message whenever population.json is created in S3
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(suffix="population.json")
        )

        # 6) Import your pre-published pandas + NumPy layer
        pandas_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, "PandasLayer",
            "arn:aws:lambda:us-east-2:916861644917:layer:pandas-numpy:2"
        )

        # 7) Analytics Lambda (Part 3) triggered by SQS
        analytics_fn = lambda_.Function(self, "AnalyticsFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="handler.main",
            # Exclude any local lib dirs—your code folder should only include your .py files
            code=lambda_.Code.from_asset(
                "lambda_analytics",
                exclude=[
                    "**/pandas*", "**/numpy*", "**/awswrangler*",
                    "**/boto3*", "**/botocore*", "**/__pycache__/*",
                    "**/*.pyc"
                ]
            ),
            layers=[pandas_layer],
            environment={
                "BUCKET":     bucket.bucket_name,
                "SERIES_KEY": "pr.data.0.Current",
                "POP_KEY":    "population.json"
            },
            timeout=Duration.minutes(5),
        )
        bucket.grant_read(analytics_fn)

        # 8) Wire the queue to trigger analytics_fn
        analytics_fn.add_event_source(
            SqsEventSource(queue, batch_size=1)
        )
