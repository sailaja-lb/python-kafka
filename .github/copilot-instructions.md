# Python Kafka Project Setup Instructions

## Project Overview
This is a Python project with Kafka producer and consumer implementations for message streaming applications.

## Key Features
- Kafka Producer: Send messages to topics with error handling and retries
- Kafka Consumer: Consume and process messages with consumer groups
- Configuration management via environment variables
- Logging and error handling throughout

## Setup Steps Completed
- Project structure created with src/ package
- Producer and Consumer client implementations
- Configuration module with environment variable support
- Example usage scripts
- Requirements.txt with dependencies
- .env.example for configuration reference
- Comprehensive README with usage examples

## Dependencies
- kafka-python==2.0.2
- python-dotenv==1.0.0

## Next Steps for User
1. Navigate to project directory
2. Create virtual environment: `python -m venv venv`
3. Activate virtual environment
4. Install dependencies: `pip install -r requirements.txt`
5. Copy .env.example to .env and configure Kafka broker address
6. Ensure Kafka broker is running on configured address
7. Run producer: `python -m src.producer`
8. Run consumer: `python -m src.consumer`
