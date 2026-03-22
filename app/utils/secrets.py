import boto3
import json

def get_gcp_credentials() -> dict:
    client = boto3.client("secretsmanager")
    secret = client.get_secret_value(SecretId="music-metrics/gcp-service-account")
    return json.loads(secret["SecretString"])