"""
Quick test script for JA Assure RAG API
Tests that the API starts, initializes, and handles a query correctly.
"""
import time
import subprocess
import requests
import sys
import signal
import os

API_URL = "http://localhost:8000"
STARTUP_WAIT = 35  # Wait for system initialization

def test_api():
    """Test the API by starting it, making a request, then stopping it."""
    
    print("Starting API server...")
    
    # Start the API in a subprocess
    env = os.environ.copy()
    env["HF_HUB_OFFLINE"] = "1"
    env["TRANSFORMERS_OFFLINE"] = "1"
    
    process = subprocess.Popen(
        ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd="/Users/admin22/Documents/Coding/ja-assure_rag"
    )
    
    try:
        print(f"Waiting {STARTUP_WAIT} seconds for initialization...")
        time.sleep(STARTUP_WAIT)
        
        # Test 1: Health check
        print("\n1. Testing health endpoint...")
        response = requests.get(f"{API_URL}/health", timeout=5)
        print(f"   Status: {response.status_code}")
        print(f"   Response: {response.json()}")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        print("   ✓ Health check passed")
        
        # Test 2: Query endpoint
        print("\n2. Testing query endpoint...")
        query_data = {"question": "What is the business name of MYJADEQT001?"}
        response = requests.post(f"{API_URL}/query", json=query_data, timeout=30)
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"   Question: {result['question']}")
            print(f"   Answer: {result['answer'][:100]}...")
            assert "question" in result
            assert "answer" in result
            print("   ✓ Query endpoint passed")
        else:
            print(f"   ✗ Query failed: {response.text}")
            sys.exit(1)
        
        print("\n✓ All tests passed!")
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        sys.exit(1)
        
    finally:
        print("\nStopping API server...")
        process.send_signal(signal.SIGTERM)
        process.wait(timeout=5)
        print("API server stopped.")


if __name__ == "__main__":
    test_api()
