import json
import logging
import os
import uuid
from enum import StrEnum

import boto3
from pydantic import BaseModel
from pydantic import ValidationError
from pymongo import MongoClient

# Configure the root logger for AWS Lambda
logger = logging.getLogger()
for handler in logger.handlers:
    logger.removeHandler(handler)

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def get_mongo_client():
    mongodb_uri = os.getenv("MONGODB_URI")
    logger.info("Creating MongoDB client.")
    return MongoClient(mongodb_uri, uuidRepresentation="standard")


def get_knowledge_source_id_from_app(app_id: str) -> str:
    logger.info(f"Fetching source ID for app_id: {app_id}")

    # Connect to MongoDB and fetch the document
    try:
        client = get_mongo_client()
        db = client["chat"]
        apps = db.knowledge_apps

        # Convert app_id to a proper UUID format
        try:
            app_uuid = uuid.UUID(app_id)
        except ValueError as e:
            logger.error(f"Invalid app_id format: {app_id}. Error: {e}")
            raise

        # Get the app
        app = apps.find_one({"_id": app_uuid})
        if app and app.get("knowledge_sources"):
            logger.info(f"App found for app_id: {app_id}")
            return str(app["knowledge_sources"][0].oid)
        elif not app:
            logger.error(f"No app found for app_id: {app_id}")
            raise ValueError(f"No app found for app_id: {app_id}")
        else:
            logger.error(f"No knowledge sources found for app_id: {app_id}")
            raise ValueError(f"No knowledge sources found for app_id: {app_id}")
    finally:
        if client:
            client.close()


class DependsOnType(StrEnum):
    N_TO_N = "N_TO_N"
    SEQUENTIAL = "SEQUENTIAL"


class DependsOn(BaseModel):
    jobId: str
    type: DependsOnType

    def model_dump(self, **kwargs) -> dict:
        return {"jobId": self.jobId, "type": self.type.value}


class EnvVar(BaseModel):
    name: str
    value: str


class ContainerOverrides(BaseModel):
    command: list[str]
    environment: list[EnvVar]


def remove_special_characters(input_str: str) -> str:
    return "".join(e for e in input_str if e.isalnum())


class BatchManager:
    @staticmethod
    def submit_embed_job(
        job_name: str,
        job_queue: str,
        job_definition: str,
        container_overrides: ContainerOverrides,
        depends_on: list[DependsOn] = [],
    ) -> str:
        client = boto3.client("batch")
        try:
            response = client.submit_job(
                jobName=job_name,
                jobQueue=job_queue,
                jobDefinition=job_definition,
                containerOverrides={
                    "command": container_overrides.command,
                    "environment": [
                        {"name": env.name, "value": env.value}
                        for env in container_overrides.environment
                    ],
                },
                dependsOn=[{"jobId": dep.jobId, "type": dep.type.value} for dep in depends_on],
            )
            logger.info(f"Embed job '{job_name}' submitted successfully. ID: {response['jobId']}")
            return response["jobId"]
        except Exception as e:
            logger.error(
                f"Failed to submit embed job '{job_name}'. Error: {str(e)}",
                exc_info=True,
            )
            raise e


class EmbedJobEvent(BaseModel):
    name: str
    id: str


def handle_vendor_embed_request(event, context):
    try:
        logger.debug("Received event body: %s", event["body"])
        event = json.loads(event["body"])
        validated_event = EmbedJobEvent(**event)
        logger.info(
            f"Event validated successfully for embed job with name: '{validated_event.name}' and id: '{validated_event.id}'"
        )
    except json.JSONDecodeError as json_err:
        logger.error("JSON parsing error: %s", str(json_err), exc_info=True)
        return {
            "statusCode": 400,
            "body": json.dumps(f"JSON parsing error: {str(json_err)}"),
        }
    except ValidationError as validation_err:
        logger.error("Validation error: %s", str(validation_err), exc_info=True)
        return {
            "statusCode": 400,
            "body": json.dumps(f"Validation error: {str(validation_err)}"),
        }

    try:
        prepare_embed_job(validated_event.name, validated_event.id)

        return {
            "statusCode": 200,
            "body": json.dumps("Embed job submitted successfully"),
        }
    except Exception as e:
        logger.error("Error submitting embed job: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error submitting embed job: {str(e)}"),
        }


def prepare_embed_job(name: str, app_id: str) -> str:
    logger.debug(f"Preparing embed job for '{name}'")
    source_id = get_knowledge_source_id_from_app(app_id)
    logger.info(f"Source ID for '{name}': {source_id}")
    command = ["emb", "update-website", source_id, "--namespace", app_id]
    job_id = BatchManager.submit_embed_job(
        job_name=remove_special_characters(name),
        job_queue="website-processing-queue",
        job_definition="embedding-job",
        container_overrides=ContainerOverrides(
            command=command,
            environment=[],
        ),
        depends_on=[],
    )
    logger.info(f"Embed job for '{name}' prepared successfully. Job ID: {job_id}")
    return job_id