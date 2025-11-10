import os
import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client("s3")

INGEST_BUCKET = os.environ.get("INGEST_BUCKET")
STAGE = os.environ.get("STAGE", "dev")
TABLE_NAME = os.environ.get("TABLE_NAME")  # si lo usas

def build_s3_key(body_json):
    """
    Estructura de clave:
    {stage}/ingesta/{tenant_id}/{uuid-or-generated}.json
    """
    tenant_id = body_json.get("tenant_id") or body_json.get("tenantId") or "unknown-tenant"
    file_uuid = body_json.get("uuid") or body_json.get("id") or str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    filename = f"{file_uuid}.json"
    key = f"{STAGE}/ingesta/{tenant_id}/{timestamp}-{filename}"
    return key

def lambda_handler(event, context):
    # Manejo del body según integración HTTP (APIGateway v1/v2)
    try:
        if isinstance(event.get("body"), str):
            body_text = event["body"]
            # API Gateway a veces manda body como JSON string o ya parseado
            try:
                body_json = json.loads(body_text)
            except Exception:
                body_json = {"raw": body_text}
        elif isinstance(event.get("body"), dict):
            body_json = event["body"]
        else:
            # Si no hay body, tratar el payload completo
            body_json = event

        # Construir key y subir a S3
        if not INGEST_BUCKET:
            raise RuntimeError("INGEST_BUCKET no está definido en las variables de entorno")

        s3_key = build_s3_key(body_json)
        s3.put_object(
            Bucket=INGEST_BUCKET,
            Key=s3_key,
            Body=json.dumps(body_json, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json"
        )

        response_body = {
            "message": "Comentario ingresado en bucket de ingesta",
            "bucket": INGEST_BUCKET,
            "key": s3_key
        }

        return {
            "statusCode": 201,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(response_body, ensure_ascii=False)
        }

    except Exception as e:
        err = {"error": str(e)}
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(err)
        }
