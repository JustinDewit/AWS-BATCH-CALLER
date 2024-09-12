import json
import logging
import os
from enum import StrEnum

import boto3
from pydantic import BaseModel
from pydantic import HttpUrl
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
    def submit_job(
        job_name: str,
        job_queue: str,
        job_definition: str,
        container_overrides: ContainerOverrides,
        depends_on: list[DependsOn] = [],
    ) -> str:
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
        logger.info(f"Job '{job_name}' submitted successfully. ID: {response['jobId']}")
        return response["jobId"]


class BatchJobEvent(BaseModel):
    assistant_id: str
    organization_name: str
    organization_id: str
    knowledge_source_name: str
    knowledge_source_id: str
    knowledge_source_uri: HttpUrl
    search_filter: str = ""
    negative_search_filter: str = ""


def handle_assistant_scrape_and_embed_request(event, context):
    try:
        logger.debug("Received event body: %s", event["body"])
        event = json.loads(event["body"])
        batch_event = BatchJobEvent(**event)
        logger.info(f"Event validated successfully for assistant '{batch_event.assistant_id}'")
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
        scrape_job_id = prepare_scrape_job(batch_event)
        prepare_embed_job(batch_event, scrape_job_id)

        return {"statusCode": 200, "body": json.dumps("Batch jobs submitted successfully")}
    except Exception as e:
        logger.error("Error submitting batch jobs: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error submitting batch jobs: {str(e)}"),
        }


def prepare_scrape_job(batch_event: BatchJobEvent) -> str:
    aws_prefix = f"{batch_event.organization_name}_{batch_event.organization_id}/{batch_event.assistant_id}/{batch_event.knowledge_source_name}"
    logger.info(f"AWS prefix set to: {aws_prefix}")
    command = [
        "page",
        f"{batch_event.organization_name}_{batch_event.organization_id}",
        str(batch_event.knowledge_source_uri),
    ]

    # Extend command with search_filter and negative_search_filter if provided
    if batch_event.search_filter:
        command.extend(["--lookup", batch_event.search_filter])
    if batch_event.negative_search_filter:
        command.extend(["--nlookup", batch_event.negative_search_filter])

    logger.info(f"Final command for scrape job: {command}")

    return BatchManager.submit_job(
        job_name=batch_event.assistant_id,
        job_queue="website-processing-queue",
        job_definition=os.getenv("SCRAPE_JOB_DEFINITION"),
        container_overrides=ContainerOverrides(
            command=command,
            environment=[
                EnvVar(name="AWS_PREFIX", value=aws_prefix),
                EnvVar(name="AWS_BUCKET", value=os.getenv("AWS_BUCKET")),
            ],
        ),
        depends_on=[],
    )


def prepare_embed_job(batch_event: BatchJobEvent, scrape_job_id: str) -> None:
    command = [
        "emb",
        "document",
        f"{batch_event.knowledge_source_id}",
        "--namespace",
        f"{batch_event.assistant_id}",
    ]

    logger.info(f"Final command for embed job: {command}")

    BatchManager.submit_job(
        job_name=batch_event.assistant_id,
        job_queue="website-processing-queue",
        job_definition=os.getenv("EMBED_JOB_DEFINITION"),
        container_overrides=ContainerOverrides(
            command=command,
            environment=[
                EnvVar(name="AWS_BUCKET", value=os.getenv("AWS_BUCKET")),
            ],
        ),
        depends_on=[DependsOn(jobId=scrape_job_id, type=DependsOnType.SEQUENTIAL)],
    )
    logger.info(f"Embed job for assistant '{batch_event.assistant_id}' prepared successfully.")