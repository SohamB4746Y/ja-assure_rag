"""
FastAPI REST API for JA Assure RAG System

Exposes the existing RAG system via HTTP endpoints without modifying core logic.
"""
import os
import logging
from typing import Optional

# Force HuggingFace offline mode
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from main import initialize_system, handle_query
from src.query_parser import QueryParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================
# FASTAPI APP INITIALIZATION
# =============================================================

app = FastAPI(
    title="JA Assure RAG API",
    description="Production-grade insurance proposal intelligence system",
    version="1.0.0"
)

# Global state - initialized once at startup
embedder = None
llm = None
qa_store = None
analytical_engine = None
metadata = None
query_parser = None


@app.on_event("startup")
async def startup_event():
    """Initialize all system components once at startup."""
    global embedder, llm, qa_store, analytical_engine, metadata, query_parser
    
    logger.info("Starting JA Assure RAG API...")
    logger.info("Initializing system components...")
    
    # Initialize all components
    embedder, llm, qa_store, analytical_engine, metadata = initialize_system()
    
    # Create a persistent query parser for conversation history
    query_parser = QueryParser(llm)
    
    logger.info("API startup complete. Ready to handle requests.")


# =============================================================
# REQUEST/RESPONSE MODELS
# =============================================================

class QueryRequest(BaseModel):
    """Request model for query endpoint."""
    question: str = Field(
        ...,
        description="The question to ask the RAG system",
        example="What is the business name of MYJADEQT001?"
    )


class QueryResponse(BaseModel):
    """Response model for query endpoint."""
    question: str = Field(..., description="The question that was asked")
    answer: str = Field(..., description="The answer from the RAG system")


class HealthResponse(BaseModel):
    """Response model for health check endpoint."""
    status: str = Field(..., description="Health status of the API")


# =============================================================
# API ENDPOINTS
# =============================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Simple status indicator
    """
    return {"status": "ok"}


@app.post("/query", response_model=QueryResponse, tags=["Query"])
async def query_endpoint(request: QueryRequest):
    """
    Process a natural language query against the insurance proposal database.
    
    The system uses multiple retrieval strategies:
    - Predefined Q&A fast-path
    - LLM-assisted query understanding with deterministic execution
    - Analytical queries (counts, aggregations)
    - Structured field lookups
    - Semantic RAG retrieval with FAISS
    
    Args:
        request: QueryRequest containing the question
        
    Returns:
        QueryResponse with the question and answer
        
    Raises:
        HTTPException: If system components are not initialized or query fails
    """
    # Validate system is initialized
    if embedder is None or llm is None:
        logger.error("System not initialized - components are None")
        raise HTTPException(
            status_code=503,
            detail="System not initialized. Please wait for startup to complete."
        )
    
    question = request.question.strip()
    
    if not question:
        raise HTTPException(
            status_code=400,
            detail="Question cannot be empty"
        )
    
    logger.info(f"Received query: {question}")
    
    try:
        # Use the existing handle_query function with persistent query_parser
        answer = handle_query(
            query=question,
            embedder=embedder,
            llm=llm,
            qa_store=qa_store,
            analytical_engine=analytical_engine,
            query_parser=query_parser
        )
        
        logger.info(f"Generated answer (length: {len(answer)} chars)")
        
        return QueryResponse(
            question=question,
            answer=answer
        )
        
    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing query: {str(e)}"
        )


# =============================================================
# RUN INSTRUCTIONS
# =============================================================

if __name__ == "__main__":
    import uvicorn
    
    print("\n=== JA Assure RAG API ===")
    print("\nStarting FastAPI server...")
    print("API will be available at: http://localhost:8000")
    print("Interactive docs: http://localhost:8000/docs")
    print("Alternative docs: http://localhost:8000/redoc")
    print("\nPress CTRL+C to stop\n")
    
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info"
    )
