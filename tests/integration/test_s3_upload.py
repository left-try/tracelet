"""Opt-in integration contract; no live AWS account is needed."""

import importlib.util
import os
import unittest


@unittest.skipUnless(
    os.environ.get("TRACELET_S3_TEST_ENDPOINT") and importlib.util.find_spec("boto3"),
    "configure a local S3-compatible endpoint and optional boto3 extra",
)
class S3UploadIntegrationTests(unittest.TestCase):
    def test_local_s3_endpoint_receives_result_object(self):
        import asyncio
        import boto3

        from tests._support import public_symbol

        S3Sink = public_symbol("S3Sink")
        endpoint = os.environ["TRACELET_S3_TEST_ENDPOINT"]
        bucket = os.environ.get("TRACELET_S3_TEST_BUCKET", "tracelet-test")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        )
        if not any(item["Name"] == bucket for item in client.list_buckets()["Buckets"]):
            client.create_bucket(Bucket=bucket)
        sink = S3Sink(client=client, bucket=bucket, prefix="tracelet-tests")

        secret = "api_key=integration-secret-value"
        asyncio.run(sink.write(job_id="integration-record", record={
            "schema_version": 1, "score": True, "details": secret,
        }))

        response = client.get_object(Bucket=bucket, Key="tracelet-tests/integration-record.json")
        body = response["Body"].read().decode()
        self.assertIn('"score": true', body)
        self.assertNotIn("integration-secret-value", body)


if __name__ == "__main__":
    unittest.main()
