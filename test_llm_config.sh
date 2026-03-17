#!/bin/bash
# Test script to verify LLM endpoint configuration and error handling

echo "=========================================="
echo "LLM CONFIGURATION & ERROR HANDLING TEST"
echo "=========================================="
echo ""

# Start the API in background
echo "Starting API server..."
cd /Users/user57/Documents/ja-assure_rag
source venv/bin/activate

# Kill any existing process on port 8000
lsof -ti:8000 | xargs kill -9 2>/dev/null || true

# Start API in background
python -m uvicorn api:app --host 127.0.0.1 --port 8000 &
API_PID=$!
sleep 5

echo "API PID: $API_PID"
echo ""

# Test 1: Simple query that should work
echo "TEST 1: Simple query (should work with new model)"
echo "Query: What is the average underwriting turnaround time?"
curl -s -X POST "http://127.0.0.1:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"question":"What is the average underwriting turnaround time?"}' | python -m json.tool
echo ""

# Test 2: Health check
echo "TEST 2: Health check"
curl -s http://127.0.0.1:8000/health | python -m json.tool
echo ""

# Test 3: Query that uses LLM (should work or show real error)
echo "TEST 3: LLM-assisted query (error handling test)"
curl -s -X POST "http://127.0.0.1:8000/query" \
  -H "Content-Type: application/json" \
  -d '{"question":"Which proposals use Jaguar transit?"}' | python -m json.tool
echo ""

echo "Tests complete."
echo "If you see detailed error messages above, the error handling is working correctly."
echo ""

# Cleanup
kill $API_PID 2>/dev/null || true
