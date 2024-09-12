import json
import logging
from enum import StrEnum

import boto3
from pydantic import BaseModel
from pydantic import HttpUrl
from pydantic import ValidationError

client = boto3.client("batch")

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DependsOnType(StrEnum):
    N_TO_N = "N_TO_N"
    SEQUENTIAL = "SEQUENTIAL"


class DependsOn(BaseModel):
    # Model for job dependencies
    jobId: str
    type: DependsOnType

    def model_dump(self, **kwargs) -> dict:
        # Convert model to dictionary for AWS Batch submission
        return {"jobId": self.jobId, "type": self.type.value}


class EnvVar(BaseModel):
    # Model for environment variables
    name: str
    value: str


class ContainerOverrides(BaseModel):
    # Model for container override configurations
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
    name: str
    support_doc_urls: list[HttpUrl]
    search_filter: str = ""
    negative_search_filter: str = ""


def handle_vendor_scrape_request(event, context):
    try:
        logger.debug("Received event body: %s", event["body"])
        event = json.loads(event["body"])
        scrape_event = ScrapeJobEvent(**event)
        logger.info(f"Event validated successfully for job '{scrape_event.name}'")
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
            scrape_event.name,
            scrape_event.support_doc_urls,
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
    name: str,
    support_doc_urls: list[str],
    search_filter: str,
    negative_search_filter: str,
) -> None:
    logger.info(
        f"Preparing scrape job for '{name}' with URLs: {support_doc_urls}, filters: {search_filter}, {negative_search_filter}"
    )
    name = name.replace(" ", "")
    for url in support_doc_urls:
        command = [
            "page",
            name,
            str(url),  # Explicitly convert URL to string
        ]
        if search_filter:
            command.extend(["--lookup", search_filter])

        if negative_search_filter:
            command.extend(["--nlookup", negative_search_filter])

        BatchManager.submit_scrape_job(
            job_name=remove_special_characters(name),
            job_queue="website-processing-queue",
            job_definition="scraping-job",
            container_overrides=ContainerOverrides(
                command=command,
                environment=[
                    EnvVar(name="AWS_PREFIX", value=name),
                    EnvVar(name="SPIDER_DURATION", value="20m"),
                ],
            ),
            depends_on=[],
        )
    logger.info(f"Scrape job for '{name}' prepared successfully.")