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
    def submit_scrape_job(
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
        logger.info(f"Job '{job_name}' submitted successfully. ID: {response['jobId']}")


class ScrapeJobEvent(BaseModel):
    assistant_id: str
    organization_name: str
    organization_id: str
    knowledge_source_name: str
    knowledge_source_uri: HttpUrl
    search_filter: str = ""
    negative_search_filter: str = ""


def handle_assistant_scrape_request(event, context):
    try:
        logger.debug("Received event body: %s", event["body"])
        event = json.loads(event["body"])
        scrape_event = ScrapeJobEvent(**event)
        logger.info(f"Event validated successfully for assistant '{scrape_event.assistant_id}'")
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
        prepare_scrape_job(
            scrape_event.assistant_id,
            scrape_event.organization_name,
            scrape_event.organization_id,
            scrape_event.knowledge_source_name,
            scrape_event.knowledge_source_uri,
            scrape_event.search_filter,
            scrape_event.negative_search_filter,
        )

        return {"statusCode": 200, "body": json.dumps("Job submitted successfully")}
    except Exception as e:
        logger.error("Error submitting job: %s", str(e), exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps(f"Error submitting job: {str(e)}"),
        }


def prepare_scrape_job(
    assistant_id: str,
    organization_name: str,
    organization_id: str,
    knowledge_source_name: str,
    knowledge_source_uri: str,
    search_filter: str = "",
    negative_search_filter: str = "",
) -> None:
    aws_prefix = f"{organization_name}_{organization_id}/{assistant_id}/{knowledge_source_name}"
    logger.info(f"AWS prefix set to: {aws_prefix}")
    command = [
        "page",
        f"{organization_name}_{organization_id}",
        str(knowledge_source_uri),
    ]

    # Extend command with search_filter and negative_search_filter if provided
    if search_filter:
        command.extend(["--lookup", search_filter])
    if negative_search_filter:
        command.extend(["--nlookup", negative_search_filter])

    logger.info(f"Final command for scrape job: {command}")

    BatchManager.submit_scrape_job(
        job_name=assistant_id,
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
    logger.info(f"Scrape job for assistant '{assistant_id}' prepared successfully.")