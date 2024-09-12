import json
import logging
from enum import StrEnum

from assistant_embed import handle_assistant_embed_request
from assistant_scrape import handle_assistant_scrape_request
from assistant_scrape_and_embed import handle_assistant_scrape_and_embed_request
from vendor_embed import handle_vendor_embed_request
from vendor_scrape import handle_vendor_scrape_request

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Jobs(StrEnum):
    VENDOR_SCRAPE_JOB = "vendor_scrape"
    VENDOR_EMBED_JOB = "vendor_embed"
    ASSISTANT_SCRAPE_JOB = "assistant_scrape"
    ASSISTANT_EMBED_JOB = "assistant_embed"
    ASSISTANT_SCRAPE_AND_EMBED_JOB = "assistant_scrape_and_embed"


def route_request(event, context):
    logging.basicConfig(level=logging.DEBUG)
    try:
        body = json.loads(event["body"])
        request_type = body.get("request_type")

        logging.info(f"Received request type: {request_type}")

        if request_type == Jobs.VENDOR_SCRAPE_JOB.value:
            logging.info("Activating vendor scrape function")
            return handle_vendor_scrape_request(event, context)
        elif request_type == Jobs.VENDOR_EMBED_JOB.value:
            logging.info("Activating vendor embed function")
            return handle_vendor_embed_request(event, context)
        elif request_type == Jobs.ASSISTANT_SCRAPE_JOB.value:
            logging.info("Activating assistant scrape function")
            return handle_assistant_scrape_request(event, context)
        elif request_type == Jobs.ASSISTANT_EMBED_JOB.value:
            logging.info("Activating assistant embed function")
            return handle_assistant_embed_request(event, context)
        elif request_type == Jobs.ASSISTANT_SCRAPE_AND_EMBED_JOB.value:
            logging.info("Activating assistant scrape and embed function")
            return handle_assistant_scrape_and_embed_request(event, context)
        else:
            logging.warning(f"Invalid request type: {request_type}")
            return {"statusCode": 400, "body": json.dumps("Invalid request type")}
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps(f"An error occurred: {str(e)}")}