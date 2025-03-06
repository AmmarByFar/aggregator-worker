# The Aggregator

A news aggregation system that collects messages from various social media sources, processes them with an LLM to extract news information, and stores the results in Supabase.

## Features

- Collects messages from multiple sources:
  - Telegram channels
  - Twitter/X accounts
  - Facebook pages
- Uses LLM (via LangChain) to:
  - Determine if messages contain valid news
  - Extract structured information (title, content, location, etc.)
  - Categorize news items
- Stores processed news items in Supabase
- Runs as a Docker container for easy deployment and scaling

## Architecture

The system is designed as a worker that can be deployed in multiple instances, each potentially focusing on different data sources. The main components are:

1. **Data Sources**: Modules for collecting messages from different platforms
2. **LLM Processor**: Uses LangChain and OpenAI to analyze and extract information
3. **Storage**: Supabase client for storing processed news items

## Setup

1. Clone the repository
2. Copy `.env.example` to `.env` and fill in your API keys and configuration
3. Build the Docker image:
   ```
   docker build -t aggregator-worker .
   ```
4. Run the container:
   ```
   docker run -d --name aggregator-worker --env-file .env aggregator-worker
   ```

## Configuration

The system is configured through environment variables:

- **OpenAI**: API key for LLM processing
- **Supabase**: URL and key for data storage
- **Telegram**: API credentials and channels to monitor
- **Twitter/X**: API credentials and accounts to monitor
- **Facebook**: API credentials and pages to monitor
- **Worker**: ID, sources to use, and polling interval

See `.env.example` for all available configuration options.

## Development

### Prerequisites

- Python 3.11+
- Required packages (see `requirements.txt`)

### Local Setup

1. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Run the worker:
   ```
   python main.py
   ```

## License

MIT
