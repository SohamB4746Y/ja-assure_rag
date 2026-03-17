#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Load .env file if it exists
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

# Check if CEREBRAS_API_KEY is set
if [ -z "$CEREBRAS_API_KEY" ]; then
    echo "Error: CEREBRAS_API_KEY environment variable is not set"
    echo ""
    echo "Please create a .env file with your API key"
    exit 1
fi

echo ""
echo "=== JA Assure RAG API Server ==="
echo ""
echo "Starting FastAPI server on http://localhost:8000"
echo "Interactive docs: http://localhost:8000/docs"
echo ""
echo "Press CTRL+C to stop"
echo ""

# Run the API server
python api.py
