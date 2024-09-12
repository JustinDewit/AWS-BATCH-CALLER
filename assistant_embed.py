import json
import logging
import os
from enum import StrEnum

import boto3
from pydantic import BaseModel
from pydantic import ValidationError

client = boto3.client("batch")

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


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
    ) -> None:
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


class EmbedJobEvent(BaseModel):
    assistant_id: str
    knowledge_source_id: str


def handle_assistant_embed_request(event, context):
    try:
        logger.debug("Received event body: %s", event["body"])
        event = json.loads(event["body"])
        embed_event = EmbedJobEvent(**event)
        logger.info(f"Event validated successfully for assistant '{embed_event.assistant_id}'")
    except json.JSONDecodeError as json_err:
        logger.error("JSON parsing error: %s", str(json_err), exc_info=True)
        return {
            "statusCode": 400,
            "body": json.dumps(f"JSON parsing error: {str(json_err)}"),
        }
    except ValidationError as e:
        logger.error("Validation error for event: %s", str(e), exc_info=True)
        return {"statusCode": 400, "body": json.dumps(f"Validation error: {e}")}

    try:
        prepare_embed_job(
            embed_event.assistant_id,
            embed_event.knowledge_source_id,
        )

        return {"statusCode": 200, "body": json.dumps("Embed job submitted successfully")}
    except Exception as e:
        logger.error("Error submitting embed job: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error submitting embed job: {str(e)}"),
        }


def prepare_embed_job(
    assistant_id: str,
    knowledge_source_id: str,
) -> None:
    command = [
        "emb",
        "document",
        f"{knowledge_source_id}",
        "--namespace",
        f"{assistant_id}",
    ]

    logger.info(f"Final command for embed job: {command}")

    BatchManager.submit_embed_job(
        job_name=assistant_id,
        job_queue="website-processing-queue",
        job_definition=os.getenv("EMBED_JOB_DEFINITION"),
        container_overrides=ContainerOverrides(
            command=command,
            environment=[
                EnvVar(name="AWS_BUCKET", value=os.getenv("AWS_BUCKET")),
            ],
        ),
        depends_on=[],
    )
    logger.info(f"Embed job for assistant '{assistant_id}' prepared successfully.")