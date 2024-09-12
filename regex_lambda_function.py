import json

from pydantic import ValidationError
from pydantic_validator import UrlRulesModel
from regex_generator import generate_regex
from regex_generator import initialize_model


def lambda_handler(event, context):
    """
    Handles the AWS Lambda event.
    """

    print(f"Event data: {event}")  # Print the event data

    try:
        # Check if 'body' key exists in the event data
        if "body" in event:
            # Parse the body from the event data
            body = json.loads(event["body"])
        else:
            # Use the event data directly
            body = event

        # Validate the event data
        data = UrlRulesModel(**body)
    except ValidationError as e:
        # Handle the validation error
        print(f"Validation error: {e}")
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Invalid data format."}),
        }

    llm = initialize_model()
    if data.included_words or data.excluded_words:
        response = generate_regex(llm, data.included_words, data.excluded_words)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"response": response}),
        }
    else:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "No URL rules provided."}),
        }