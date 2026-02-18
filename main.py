"""
JA Assure RAG System - Main Entry Point

This module implements a production-ready RAG system for insurance proposal intelligence.
It integrates all patterns from the reference architecture:
- Dual-tier retrieval with similarity thresholds
- Predefined Q&A fast-path
- Dynamic query classification
- Pandas-based analytical engine
- Structured logging
- Output sanitization
- Hard refusal when data unavailable
"""
from __future__ import annotations

import os
import json
import pickle
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import faiss
import numpy as np

from loader.excel_loader import load_excel
from loader.json_cleaner import parse_json_cell
from loader.section_extractor import extract_sections
from src.text_builder import build_section_text
from src.llm_client import LLMClient
from src.qa_store import PredefinedQAStore
from src.query_classifier import classify_query, extract_quote_id
from src.prompt_builder import build_prompt, get_refusal_message
from src.output_cleaner import clean_output
from src.analytical_engine import AnalyticalEngine
from embeddings.embedder import Embedder, cosine_similarity

# =============================================================
# CONFIGURATION CONSTANTS
# =============================================================

# File paths
EXCEL_PATH = "data/JADE-Fields DB(Integrated)_Mentor Copy.xlsx"
SHEET_NAME = "tbl_MY"
INDEX_PATH = "index/index.faiss"
METADATA_PATH = "index/metadata.pkl"
PREDEFINED_QA_PATH = "evaluation/predefined_qa.json"
LOG_DIR = "logs"
LOG_FILE = "logs/query_log.json"

# Similarity thresholds (Pattern 1: Dual-tier retrieval)
PREDEFINED_SIMILARITY_THRESHOLD = 0.85  # For predefined Q&A fast-path
CHUNK_SIMILARITY_THRESHOLD = 0.5        # For semantic chunk retrieval

# Retrieval settings
TOP_K_CHUNKS = 5

# =============================================================
# LOGGING SETUP (Pattern 7: Per-query audit logging)
# =============================================================

os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"{LOG_DIR}/system.log")
    ]
)
logger = logging.getLogger("ja_assure_rag")


def log_query(
    query: str,
    query_type: str,
    quote_id: Optional[str],
    num_chunks: int,
    top_similarity: float,
    answer: str
) -> None:
    """
    Log query details to JSON file for audit trail.
    
    Args:
        query: User's question
        query_type: Classification (predefined/analytical/structured/semantic/refused)
        quote_id: Extracted quote ID if any
        num_chunks: Number of chunks retrieved
        top_similarity: Highest similarity score
        answer: Generated answer
    """
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "query": query,
        "query_type": query_type,
        "quote_id_extracted": quote_id,
        "num_chunks_retrieved": num_chunks,
        "top_similarity_score": round(top_similarity, 4),
        "answer_length": len(answer)
    }
    
    try:
        log_path = Path(LOG_FILE)
        
        if log_path.exists():
            with open(log_path, "r") as f:
                logs = json.load(f)
        else:
            logs = []
        
        logs.append(log_entry)
        
        with open(log_path, "w") as f:
            json.dump(logs, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to write query log: {e}")


# =============================================================
# INGESTION PIPELINE
# =============================================================

def run_ingestion() -> tuple[list[dict], list[dict]]:
    """
    Run the full ingestion pipeline: load Excel -> parse JSON -> extract sections -> build text.
    
    Returns:
        Tuple of (section_chunks, text_chunks)
    """
    logger.info("Starting data ingestion...")
    
    # Phase 1: Load Excel
    df = load_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    logger.info(f"Loaded {len(df)} records from Excel")
    
    # Phase 2: Extract sections from each row
    all_sections = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        sections = extract_sections(row_dict, parse_json_cell)
        all_sections.extend(sections)
    
    logger.info(f"Extracted {len(all_sections)} section chunks")
    
    # Phase 3: Build text representations
    text_chunks = []
    for chunk in all_sections:
        text = build_section_text(chunk)
        
        if text:
            text_chunks.append({
                "quote_id": chunk["quote_id"],
                "section": chunk["section"],
                "text": text,
                "fields": chunk["data"],
                "metadata": chunk["metadata"]
            })
    
    logger.info(f"Built {len(text_chunks)} text chunks")
    
    return all_sections, text_chunks


def build_index(text_chunks: list[dict], embedder: Embedder) -> None:
    """
    Build FAISS index from text chunks with retry logic.
    
    Args:
        text_chunks: List of chunk dictionaries with 'text' key
        embedder: Embedder instance
    """
    logger.info("Building FAISS index...")
    
    texts = [chunk["text"] for chunk in text_chunks]
    
    # Build metadata list
    metadatas = [
        {
            "quote_id": chunk["quote_id"],
            "section": chunk["section"],
            "text": chunk["text"],
            "fields": chunk["fields"],
            **chunk["metadata"]
        }
        for chunk in text_chunks
    ]
    
    # Embed with retry logic (Pattern 3)
    vectors = embedder.embed_with_retry(texts)
    
    if len(vectors) == 0:
        logger.error("Failed to generate any embeddings")
        return
    
    # Build FAISS index
    dim = vectors.shape[1]
    index = faiss.IndexFlatIP(dim)  # Inner product for normalized vectors = cosine
    index.add(np.array(vectors).astype("float32"))
    
    # Save index and metadata
    os.makedirs("index", exist_ok=True)
    faiss.write_index(index, INDEX_PATH)
    
    with open(METADATA_PATH, "wb") as f:
        pickle.dump(metadatas, f)
    
    logger.info(f"Index built: {len(vectors)} vectors, {dim} dimensions")


# =============================================================
# RETRIEVAL WITH SIMILARITY THRESHOLDS (Pattern 1)
# =============================================================

def retrieve_chunks_with_threshold(
    query: str,
    embedder: Embedder,
    threshold: float = CHUNK_SIMILARITY_THRESHOLD,
    top_k: int = TOP_K_CHUNKS,
    quote_id_filter: Optional[str] = None
) -> tuple[list[dict], float]:
    """
    Retrieve chunks above similarity threshold.
    
    Args:
        query: User's question
        embedder: Embedder instance
        threshold: Minimum cosine similarity
        top_k: Maximum chunks to retrieve
        quote_id_filter: Optional quote ID to filter by
        
    Returns:
        Tuple of (filtered_chunks, top_similarity_score)
    """
    # Load index and metadata
    if not os.path.exists(INDEX_PATH) or not os.path.exists(METADATA_PATH):
        logger.warning("Index not found")
        return [], 0.0
    
    index = faiss.read_index(INDEX_PATH)
    
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    # Embed query
    query_vector = embedder.embed_single(query)
    
    # Search
    scores, indices = index.search(
        np.array([query_vector]).astype("float32"),
        top_k * 2  # Get extra candidates for filtering
    )
    
    # Filter by threshold and optionally by quote ID
    results = []
    top_similarity = 0.0
    
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
            
        if score > top_similarity:
            top_similarity = float(score)
        
        if score < threshold:
            continue
        
        chunk = metadata[idx]
        
        # Apply quote ID filter if specified
        if quote_id_filter:
            if chunk.get("quote_id") != quote_id_filter:
                continue
        
        chunk["score"] = float(score)
        results.append(chunk)
        
        if len(results) >= top_k:
            break
    
    return results, top_similarity


# =============================================================
# STRUCTURED LOOKUP (Dynamic field matching)
# =============================================================

def structured_lookup(query: str, quote_id: str) -> dict | None:
    """
    Perform deterministic lookup for a specific field of a specific record.
    
    Args:
        query: User's question
        quote_id: Quote ID to look up
        
    Returns:
        Dict with quote_id, field, and value if found, else None
    """
    if not os.path.exists(METADATA_PATH):
        return None
    
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    query_lower = query.lower()
    
    for chunk in metadata:
        if chunk.get("quote_id") != quote_id:
            continue
        
        fields = chunk.get("fields", {})
        
        if not isinstance(fields, dict):
            continue
        
        # Dynamic field matching - no hardcoded field names
        for field_name, value in fields.items():
            # Normalize field name for matching
            normalized_field = (
                field_name.lower()
                .replace("_label", "")
                .replace("_", " ")
            )
            
            # Check if field is mentioned in query
            if normalized_field in query_lower:
                return {
                    "quote_id": quote_id,
                    "field": normalized_field,
                    "value": value,
                    "section": chunk.get("section", "")
                }
    
    return None


# =============================================================
# MAIN QUERY HANDLER
# =============================================================

def handle_query(
    query: str,
    embedder: Embedder,
    llm: LLMClient,
    qa_store: PredefinedQAStore,
    analytical_engine: AnalyticalEngine
) -> str:
    """
    Main query handler implementing all patterns.
    
    Args:
        query: User's question
        embedder: Embedder instance
        llm: LLM client
        qa_store: Predefined QA store
        analytical_engine: Pandas-based analytical engine
        
    Returns:
        Answer string
    """
    query = query.strip()
    quote_id = extract_quote_id(query)
    
    # ===========================================
    # Pattern 2: Predefined Q&A Fast-Path
    # ===========================================
    query_embedding = embedder.embed_single(query)
    predefined_answer = qa_store.find_match(query_embedding, PREDEFINED_SIMILARITY_THRESHOLD)
    
    if predefined_answer:
        logger.info("Matched predefined Q&A")
        log_query(query, "predefined", quote_id, 0, PREDEFINED_SIMILARITY_THRESHOLD, predefined_answer)
        return clean_output(predefined_answer)
    
    # ===========================================
    # Pattern 8: Query Classification
    # ===========================================
    query_type = classify_query(query)
    logger.info(f"Query classified as: {query_type}")
    
    # ===========================================
    # Analytical Queries -> Pandas Engine (never LLM)
    # ===========================================
    if query_type == "analytical":
        result = analytical_engine.run(query)
        
        if result:
            logger.info("Handled by analytical engine")
            log_query(query, "analytical", quote_id, 0, 0.0, result)
            return clean_output(result)
        
        # If analytical engine couldn't handle it, fall through to semantic
        logger.info("Analytical engine returned None, falling back to semantic")
    
    # ===========================================
    # Structured Queries -> Deterministic Lookup
    # ===========================================
    if query_type == "structured" and quote_id:
        result = structured_lookup(query, quote_id)
        
        if result:
            answer = f"{result['field'].title()} for {result['quote_id']}: {result['value']}"
            logger.info("Handled by structured lookup")
            log_query(query, "structured", quote_id, 0, 1.0, answer)
            return clean_output(answer)
    
    # ===========================================
    # Semantic RAG Retrieval with Threshold
    # ===========================================
    chunks, top_similarity = retrieve_chunks_with_threshold(
        query,
        embedder,
        threshold=CHUNK_SIMILARITY_THRESHOLD,
        top_k=TOP_K_CHUNKS,
        quote_id_filter=quote_id
    )
    
    # ===========================================
    # Pattern 4: Hard Refusal When Retrieval Fails
    # ===========================================
    if not chunks:
        refusal = get_refusal_message()
        logger.info("No chunks above threshold - refusing to answer")
        log_query(query, "refused", quote_id, 0, top_similarity, refusal)
        return refusal
    
    # ===========================================
    # Pattern 5: Build Grounded Prompt
    # ===========================================
    prompt = build_prompt(
        context="\n\n".join([c["text"] for c in chunks]),
        question=query
    )
    
    # ===========================================
    # Generate Answer via LLM
    # ===========================================
    try:
        raw_answer = llm.generate(prompt)
        answer = clean_output(raw_answer)  # Pattern 6: Output Sanitization
    except Exception as e:
        logger.error(f"LLM generation failed: {e}")
        answer = get_refusal_message()
    
    log_query(query, "semantic", quote_id, len(chunks), top_similarity, answer)
    
    return answer


# =============================================================
# INITIALIZATION
# =============================================================

def initialize_system() -> tuple[Embedder, LLMClient, PredefinedQAStore, AnalyticalEngine, list[dict]]:
    """
    Initialize all system components.
    
    Returns:
        Tuple of (embedder, llm, qa_store, analytical_engine, metadata)
    """
    print("\n=== JA Assure | Production RAG System ===\n")
    
    # Initialize embedder
    embedder = Embedder()
    logger.info(f"Embedder initialized: {embedder.embedding_dim} dimensions")
    
    # Check if index exists, if not run ingestion
    if not os.path.exists(INDEX_PATH) or not os.path.exists(METADATA_PATH):
        logger.info("Index not found, running ingestion...")
        _, text_chunks = run_ingestion()
        build_index(text_chunks, embedder)
    
    # Load metadata for analytical engine
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    # Initialize predefined Q&A store (Pattern 2)
    qa_store = PredefinedQAStore()
    qa_store.load_from_file(PREDEFINED_QA_PATH)
    if qa_store.is_loaded:
        qa_store.embed_all(embedder)
        logger.info(f"Predefined Q&A loaded: {len(qa_store)} pairs")
    
    # Initialize LLM client
    llm = LLMClient()
    logger.info("LLM client initialized")
    
    # Initialize analytical engine
    # Create a minimal DataFrame for the engine (it mainly uses metadata)
    import pandas as pd
    df = pd.DataFrame()
    analytical_engine = AnalyticalEngine(df, metadata)
    logger.info(f"Analytical engine initialized: {analytical_engine.get_record_count()} records")
    
    return embedder, llm, qa_store, analytical_engine, metadata


# =============================================================
# MAIN ENTRY POINT
# =============================================================

def main():
    """Main entry point for the interactive system."""
    
    # Initialize all components
    embedder, llm, qa_store, analytical_engine, metadata = initialize_system()
    
    print("\nSystem ready. Type 'exit' to quit.")
    print("Type 'rebuild' to re-index the data.\n")
    
    while True:
        try:
            query = input("\nEnter your question: ").strip()
            
            if not query:
                continue
            
            if query.lower() == "exit":
                print("Exiting system.")
                break
            
            if query.lower() == "rebuild":
                print("Rebuilding index...")
                _, text_chunks = run_ingestion()
                build_index(text_chunks, embedder)
                
                # Reload metadata
                with open(METADATA_PATH, "rb") as f:
                    metadata = pickle.load(f)
                analytical_engine = AnalyticalEngine(None, metadata)
                
                print("Index rebuilt successfully.")
                continue
            
            # Handle the query
            answer = handle_query(query, embedder, llm, qa_store, analytical_engine)
            
            print("\n=== ANSWER ===\n")
            print(answer)
            
        except KeyboardInterrupt:
            print("\n\nInterrupted. Exiting.")
            break
        except Exception as e:
            logger.error(f"Error handling query: {e}")
            print(f"\nError: {e}")


if __name__ == "__main__":
    main()