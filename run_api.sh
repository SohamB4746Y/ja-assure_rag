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

# Resolve a usable Python executable across environments
if [ -n "$VIRTUAL_ENV" ] && [ -x "$VIRTUAL_ENV/bin/python" ]; then
    PYTHON_CMD="$VIRTUAL_ENV/bin/python"
elif [ -x "venv/bin/python" ]; then
    PYTHON_CMD="venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "Error: No Python executable found"
    exit 1
fi

# Run the API server
"$PYTHON_CMD" api.py
