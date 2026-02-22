# JA Assure RAG API - Usage Guide

## Overview

The JA Assure RAG system is now exposed as a REST API using FastAPI. The core architecture remains unchanged - this is a pure interface layer addition.

## Architecture

- **No business logic changes** - all core components (SmartQueryExecutor, AnalyticalEngine, QueryParser, etc.) remain identical
- **Single initialization** - embeddings and models load once at startup, not per request
- **Conversation history** - persistent query parser maintains context across API calls
- **CLI still works** - `python main.py` continues to function as before

## Installation

Ensure FastAPI and Uvicorn are installed:

```bash
pip install fastapi uvicorn
```

Or use the requirements file:

```bash
pip install -r requirements.txt
```

## Running the API

### Method 1: Using Uvicorn directly

```bash
cd /Users/admin22/Documents/Coding/ja-assure_rag
source venv/bin/activate
uvicorn api:app --reload
```

### Method 2: Running the API script

```bash
cd /Users/admin22/Documents/Coding/ja-assure_rag
source venv/bin/activate
python api.py
```

The API will start on: **http://localhost:8000**

## API Endpoints

### 1. Health Check

**GET** `/health`

Check if the API is running.

**Response:**
```json
{
  "status": "ok"
}
```

**Example (curl):**
```bash
curl http://localhost:8000/health
```

### 2. Query Endpoint

**POST** `/query`

Send a natural language question to the RAG system.

**Request Body:**
```json
{
  "question": "What is the business name of MYJADEQT001?"
}
```

**Response:**
```json
{
  "question": "What is the business name of MYJADEQT001?",
  "answer": "Business Name for MYJADEQT001: Ja Assure IN"
}
```

**Example (curl):**
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the business name of MYJADEQT001?"}'
```

## Interactive API Documentation

FastAPI provides automatic interactive documentation:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Postman Testing

### Setup

1. Open Postman
2. Create a new request
3. Set method to **POST**
4. Set URL to: `http://localhost:8000/query`
5. Go to **Body** tab
6. Select **raw**
7. Select **JSON** format
8. Enter request body

### Example Queries for Postman

**Query 1: Business Name Lookup**
```json
{
  "question": "What is the business name of MYJADEQT001?"
}
```

**Query 2: Count Query**
```json
{
  "question": "How many proposals have CCTV maintenance contracts?"
}
```

**Query 3: Entity Lookup**
```json
{
  "question": "What type of business does Suresh Kumar run?"
}
```

**Query 4: Location Query**
```json
{
  "question": "How many proposals are located in Penang?"
}
```

**Query 5: Field Lookup**
```json
{
  "question": "Does Heritage Gold and Jewels have a CCTV maintenance contract?"
}
```

**Query 6: Armed Guards**
```json
{
  "question": "Does Rapid FX Money Exchange use armed guards during transit?"
}
```

**Query 7: Background Checks**
```json
{
  "question": "What background checks does LuxGold Jewellers do?"
}
```

**Query 8: Claim History**
```json
{
  "question": "What is the claim history of Heritage Gold and Jewels?"
}
```

## Using the CLI (Unchanged)

The original CLI still works exactly as before:

```bash
cd /Users/admin22/Documents/Coding/ja-assure_rag
source venv/bin/activate
python main.py
```

Type your questions and get answers interactively.

## System Capabilities

The API supports all existing RAG system features:

1. **Predefined Q&A Fast-Path** - instant answers for common questions
2. **LLM-Assisted Query Understanding** - natural language parsing
3. **Deterministic Count Queries** - "how many" queries bypass LLM
4. **Entity Lookups** - search by business name or person name
5. **Location Queries** - filter by location
6. **Analytical Queries** - aggregations and counts
7. **Semantic RAG** - FAISS vector search with similarity threshold
8. **Hard Refusal** - refuses when data unavailable
9. **Conversation History** - maintains context across queries

## Technical Details

- **Model**: llama3.1:8b (Ollama)
- **Embeddings**: sentence-transformers/all-MiniLM-L6-v2 (384-dim)
- **Vector Store**: FAISS IndexFlatIP
- **Offline Mode**: Enabled (no internet required after initial setup)
- **Logging**: Full structured logging maintained
- **Error Handling**: Production-grade exception handling

## API Response Codes

- **200 OK** - Successful query
- **400 Bad Request** - Empty or invalid question
- **500 Internal Server Error** - Processing error
- **503 Service Unavailable** - System not initialized

## Notes

- The system initializes on first startup (may take 10-30 seconds)
- HuggingFace offline mode is forced - uses cached models only
- Query parser maintains conversation history across API calls
- All logging continues to work as before
- No changes to evaluation, formatting, or business logic
