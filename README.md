## Key Features

- AWS Batch job submission and management for scraping & embedding
- Lambda functions for handling different job types
- Integration with OpenAI's GPT model for regex generation

## Technologies Used

- Python
- AWS Lambda
- AWS Batch
- Boto3
- Pydantic
- LangChain
- OpenAI

## Project Structure

- `lambda_function.py`: Main entry point for AWS Lambda, routing requests to appropriate handlers
- `assistant_scrape.py`, `assistant_embed.py`, `vendor_scrape.py`, `vendor_embed.py`: Handlers for different job types
- `regex_generator.py`: Utilizes OpenAI's GPT model to generate regex patterns
- `pydantic_validator.py`: Defines Pydantic models for data validation
