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
from src.mappings import decode_field
from src.query_parser import QueryParser, ParsedQuery
from src.query_executor import SmartQueryExecutor, QueryResult
from src.answer_formatter import format_answer
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
# STRUCTURED LOOKUP (Purely deterministic - no embeddings)
# =============================================================

def score_field_match(field_name: str, query: str) -> int:
    """
    Score how well a field name matches a query based on word overlap.
    
    Args:
        field_name: The raw field name (e.g., "do_you_use_armoured_vehicle_label")
        query: The user's query string
        
    Returns:
        Number of significant words that overlap between field name and query
    """
    # Normalize field name: remove _label suffix, split on underscore
    normalized = field_name.replace("_label", "").replace("_", " ").lower()
    field_words = set(normalized.split())
    query_words = set(query.lower().split())
    # Remove only truly generic words that never appear in field names
    noise = {"does", "is", "the", "a", "an", "for", "of", "in",
             "what", "which", "how", "many", "have", "has", "this",
             "with", "do", "you"}
    field_words -= noise
    query_words -= noise
    # Score = number of field words found in query words
    return len(field_words & query_words)


def structured_lookup(query: str) -> Optional[str]:
    """
    Perform purely deterministic lookup for a specific field of a specific record.
    No embeddings, no similarity scores, no thresholds.
    Uses scored word matching to find the best field match.
    
    Args:
        query: User's question
        
    Returns:
        Formatted answer string if found, else None
    """
    # Step 1: Extract quote_id from query
    quote_id = extract_quote_id(query)
    if not quote_id:
        return None
    
    # Step 2: Load metadata
    if not os.path.exists(METADATA_PATH):
        return None
    
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    query_lower = query.lower()
    
    # Step 2.5: Special handling for risk_location (stored at chunk level, not in fields)
    location_keywords = ["location", "address", "where", "located", "risk location", "city", "state"]
    if any(kw in query_lower for kw in location_keywords):
        for chunk in metadata:
            if chunk.get("quote_id") != quote_id:
                continue
            risk_location = chunk.get("risk_location")
            if risk_location and isinstance(risk_location, str) and risk_location.strip():
                return f"Risk Location for {quote_id}: {risk_location}"
    
    # Track best match across all chunks
    best_match = None  # (score, field_name, value)
    
    # Step 3: Score all fields across all matching chunks
    for chunk in metadata:
        if chunk.get("quote_id") != quote_id:
            continue
        
        fields = chunk.get("fields", {})
        
        if not isinstance(fields, dict):
            continue
        
        for field_name, value in fields.items():
            score = score_field_match(field_name, query)
            
            # Track the best match (highest score)
            if score > 0:
                if best_match is None or score > best_match[0]:
                    best_match = (score, field_name, value)
    
    # Step 4: Only return if best match has score >= 2 (at least 2 words matched)
    if best_match and best_match[0] >= 2:
        field_name = best_match[1]
        value = best_match[2]
        
        decoded_value = decode_field(field_name, str(value))
        human_label = field_name.replace("_label", "").replace("_", " ").title()
        return f"{human_label} for {quote_id}: {decoded_value}"
    
    return None


# =============================================================
# FLEXIBLE CROSS-PROPOSAL SEARCH
# =============================================================

def search_proposals_by_value(query: str) -> Optional[str]:
    """
    Search across all proposals for matching values in metadata.
    Handles queries like "businesses in Penang", "proposals with CCTV", etc.
    
    Args:
        query: User's question
        
    Returns:
        Formatted answer string with matching results, or None
    """
    if not os.path.exists(METADATA_PATH):
        return None
    
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    query_lower = query.lower()
    results = []
    seen_quotes = set()
    
    # Determine search type and target field based on query keywords
    
    # Location-based search
    state_city_names = ["penang", "johor", "selangor", "kuala lumpur", "kedah", "perak", 
                        "sabah", "sarawak", "melaka", "pahang", "kelantan", "terengganu",
                        "negeri sembilan", "perlis", "putrajaya", "labuan", "johor bahru",
                        "george town", "ipoh", "kuching", "kota kinabalu", "alor setar",
                        "shah alam", "petaling jaya", "subang", "klang", "cyberjaya",
                        "muar", "batu pahat", "larkin", "senai"]
    
    # Nature of business keywords
    business_types = ["pawn", "pawn shop", "pawnshop", "pawnbroker", "money changer", 
                      "money exchange", "forex", "fx exchange", "exchange", "jeweller",
                      "goldsmith", "gold", "jewelry", "jewellery"]
    
    # Find if query mentions a location
    target_location = None
    for location in state_city_names:
        if location in query_lower:
            target_location = location
            break
    
    # Find if query mentions a business type
    target_business_type = None
    for btype in business_types:
        if btype in query_lower:
            target_business_type = btype
            break
    
    # Find what field/entity we're looking for
    looking_for_business = any(w in query_lower for w in ["business", "company", "name", "who", "which"])
    looking_for_count = any(w in query_lower for w in ["how many", "count", "number of"])
    looking_for_list = any(w in query_lower for w in ["list", "show", "all", "give"])
    
    # Search metadata
    for chunk in metadata:
        quote_id = chunk.get("quote_id")
        if not quote_id or quote_id in seen_quotes:
            continue
        
        risk_location = chunk.get("risk_location", "")
        fields = chunk.get("fields", {})
        
        # Get key field values
        business_name = None
        nature_of_business = None
        if isinstance(fields, dict):
            for fn, fv in fields.items():
                fn_lower = fn.lower()
                if "business_name" in fn_lower and not business_name:
                    business_name = fv
                elif "nature_of_business" in fn_lower and not nature_of_business:
                    nature_of_business = fv
        
        # Location-based search
        if target_location:
            if isinstance(risk_location, str) and target_location in risk_location.lower():
                seen_quotes.add(quote_id)
                
                if looking_for_business and business_name:
                    results.append(f"{business_name} ({quote_id}) - {risk_location}")
                else:
                    results.append(f"{quote_id}: {risk_location}")
                continue
        
        # Business type search
        if target_business_type:
            nature_str = str(nature_of_business).lower() if nature_of_business else ""
            name_str = str(business_name).lower() if business_name else ""
            
            if target_business_type in nature_str or target_business_type in name_str:
                seen_quotes.add(quote_id)
                
                if looking_for_business and business_name:
                    rl_short = risk_location[:50] + "..." if len(risk_location) > 50 else risk_location
                    results.append(f"{business_name} ({quote_id}) - {rl_short}")
                else:
                    results.append(f"{quote_id}: {nature_of_business or 'N/A'}")
                continue
    
    # Format response
    if results:
        if looking_for_count:
            if target_location:
                return f"Found {len(results)} proposal(s) in {target_location.title()}."
            elif target_business_type:
                return f"Found {len(results)} {target_business_type} business(es)."
            else:
                return f"Found {len(results)} proposal(s) matching your query."
        elif looking_for_business and target_location:
            if len(results) == 1:
                return f"Business operating in {target_location.title()}: {results[0]}"
            else:
                header = f"Businesses operating in {target_location.title()}:\n"
                return header + "\n".join(f"- {r}" for r in results)
        elif looking_for_business and target_business_type:
            if len(results) == 1:
                return f"{target_business_type.title()} business: {results[0]}"
            else:
                header = f"{target_business_type.title()} businesses:\n"
                return header + "\n".join(f"- {r}" for r in results)
        elif looking_for_list:
            return "\n".join(f"- {r}" for r in results)
        else:
            return "\n".join(results)
    
    return None


def analytical_query_handler(query: str) -> Optional[str]:
    """
    Handle analytical queries that aggregate data across all proposals.
    ONLY handles queries that don't mention specific locations or business types.
    
    Args:
        query: User's question
        
    Returns:
        Answer string if this is an analytical query, else None
    """
    if not os.path.exists(METADATA_PATH):
        return None
    
    query_lower = query.lower()
    
    # Skip if query mentions specific location - let search_proposals_by_value handle it
    location_names = ["penang", "johor", "selangor", "kuala lumpur", "kedah", "perak", 
                      "sabah", "sarawak", "melaka", "pahang", "kelantan", "terengganu",
                      "negeri sembilan", "perlis", "putrajaya", "labuan", "johor bahru",
                      "george town", "ipoh", "kuching", "kota kinabalu", "muar"]
    if any(loc in query_lower for loc in location_names):
        return None
    
    # Skip if query mentions business types - let search_proposals_by_value handle it
    business_types = ["pawn", "pawnshop", "money changer", "forex", "exchange", "jeweller", "goldsmith"]
    if any(bt in query_lower for bt in business_types):
        return None
    
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    query_lower = query.lower()
    
    # Count queries
    if "how many" in query_lower:
        yes_values = {"001", "yes", "true", "1"}
        counted_quotes = set()
        
        # Determine what to count based on query
        field_patterns = []
        
        if "cctv maintenance" in query_lower:
            field_patterns = ["cctv_maintenance_contract"]
        elif "alarm maintenance" in query_lower or "maintenance contract" in query_lower:
            field_patterns = ["under_maintenance_contract", "maintenance"]
        elif "armoured" in query_lower or "armored" in query_lower:
            field_patterns = ["armoured_vehicle"]
        elif "armed guards" in query_lower:
            field_patterns = ["armed_guards"]
        elif "strong room" in query_lower:
            field_patterns = ["strong_room"]
        elif "cctv" in query_lower:
            field_patterns = ["cctv", "recording"]
        elif "alarm" in query_lower:
            field_patterns = ["alarm", "do_you_have_alarm"]
        elif "safe" in query_lower:
            field_patterns = ["safe", "certified"]
        elif "proposal" in query_lower or "record" in query_lower:
            # Count total unique proposals
            for chunk in metadata:
                qid = chunk.get("quote_id")
                if qid:
                    counted_quotes.add(qid)
            return f"There are {len(counted_quotes)} proposal records in the system."
        
        if field_patterns:
            for chunk in metadata:
                quote_id = chunk.get("quote_id")
                if not quote_id or quote_id in counted_quotes:
                    continue
                
                fields = chunk.get("fields", {})
                if not isinstance(fields, dict):
                    continue
                
                for field_name, value in fields.items():
                    field_lower = field_name.lower()
                    if any(p in field_lower for p in field_patterns):
                        if str(value).lower().strip() in yes_values:
                            counted_quotes.add(quote_id)
                            break
            
            return f"{len(counted_quotes)} proposals have this feature."
    
    # List queries
    if any(w in query_lower for w in ["list all", "show all", "what are all", "give all"]):
        # List all proposals
        if "proposal" in query_lower or "quote" in query_lower:
            quote_ids = set()
            for chunk in metadata:
                qid = chunk.get("quote_id")
                if qid:
                    quote_ids.add(qid)
            return "Proposals in system:\n" + "\n".join(f"- {qid}" for qid in sorted(quote_ids))
    
    return None


# =============================================================
# MAIN QUERY HANDLER
# =============================================================

def handle_query(
    query: str,
    embedder: Embedder,
    llm: LLMClient,
    qa_store: PredefinedQAStore,
    analytical_engine: AnalyticalEngine,
    query_parser: Optional[QueryParser] = None
) -> str:
    """
    Main query handler implementing all patterns with LLM-assisted query understanding.
    
    Architecture:
    1. LLM parses query to extract intent, fields, filters
    2. Deterministic lookup retrieves exact data (no hallucination)
    3. LLM formats the answer naturally
    
    Args:
        query: User's question
        embedder: Embedder instance
        llm: LLM client
        qa_store: Predefined QA store
        analytical_engine: Pandas-based analytical engine
        query_parser: Persistent query parser with conversation history
        
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
        if query_parser:
            query_parser.add_raw_to_history(query, predefined_answer)
        return clean_output(predefined_answer)
    
    # ===========================================
    # LLM-ASSISTED QUERY UNDERSTANDING (NEW!)
    # Step 1: LLM parses the query into structured format
    # Step 2: Deterministic executor retrieves exact data
    # Step 3: Format the answer
    # ===========================================
    logger.info("Using LLM-assisted query understanding")
    
    if query_parser is None:
        query_parser = QueryParser(llm)
    query_executor = SmartQueryExecutor(METADATA_PATH)
    
    # Parse the query using LLM (with conversation history for follow-ups)
    parsed = query_parser.parse(query)
    logger.info(f"Parsed query - Intent: {parsed.intent}, Fields: {parsed.target_fields}, Filter: {parsed.filter_field}={parsed.filter_value}, Contains: {parsed.filter_contains}")
    
    # Execute the parsed query deterministically
    result = query_executor.execute(parsed)
    
    # For count/list queries, 0 is a VALID answer (don't fall back)
    # For lookup queries, we need at least 1 result
    # For entity lookups (lookup + filter_contains, no quote_id), 0 is also valid (person not found)
    should_use_result = False
    if parsed.parse_success:
        if parsed.intent in ("count", "list") and (parsed.filter_contains or parsed.filter_value):
            # For filter queries, 0 is valid (e.g., "0 shoplifting cases")
            should_use_result = True
        elif parsed.intent == "lookup" and not parsed.quote_id and parsed.filter_contains:
            # Entity lookup - always use the result (even 0 means "person not found")
            should_use_result = True
        elif result.success and result.count > 0:
            should_use_result = True
    
    if should_use_result:
        # Format the answer
        answer = format_answer(llm, parsed, result)
        logger.info(f"Smart query executor handled - {result.count} results")
        log_query(query, "smart_executor", quote_id, result.count, 1.0, answer)
        # Save to conversation history for follow-up queries
        query_parser.add_to_history(query, parsed, answer)
        return clean_output(answer)
    
    logger.info("Smart executor could not handle, trying fallback handlers")
    
    # ===========================================
    # FALLBACK: Pattern 8 - Query Classification
    # ===========================================
    query_type = classify_query(query)
    logger.info(f"Query classified as: {query_type}")
    
    # ===========================================
    # Analytical Queries -> Deterministic handlers first, then Pandas Engine
    # ===========================================
    if query_type == "analytical":
        # Try specific analytical_query_handler first (more precise)
        analytical_result = analytical_query_handler(query)
        if analytical_result:
            logger.info("Handled by analytical query handler (specific)")
            log_query(query, "analytical_specific", quote_id, 0, 1.0, analytical_result)
            if query_parser:
                query_parser.add_raw_to_history(query, analytical_result)
            return clean_output(analytical_result)
        
        # Then try general Pandas engine
        result = analytical_engine.run(query)
        
        if result:
            logger.info("Handled by analytical engine")
            log_query(query, "analytical", quote_id, 0, 0.0, result)
            if query_parser:
                query_parser.add_raw_to_history(query, result)
            return clean_output(result)
        
        # If analytical engine couldn't handle it, fall through to semantic
        logger.info("Analytical engine returned None, falling back to semantic")
    
    # ===========================================
    # Structured Lookup -> Purely Deterministic (BEFORE FAISS)
    # No embeddings, no similarity, no thresholds
    # ===========================================
    structured_result = structured_lookup(query)
    if structured_result:
        logger.info("Handled by structured lookup (deterministic)")
        log_query(query, "structured", quote_id, 0, 1.0, structured_result)
        if query_parser:
            query_parser.add_raw_to_history(query, structured_result)
        return clean_output(structured_result)
    
    # ===========================================
    # Cross-Proposal Value Search (location, business, etc.)
    # Run BEFORE analytical to catch location/business type queries
    # ===========================================
    cross_search_result = search_proposals_by_value(query)
    if cross_search_result:
        logger.info("Handled by cross-proposal value search")
        log_query(query, "cross_search", quote_id, 0, 1.0, cross_search_result)
        if query_parser:
            query_parser.add_raw_to_history(query, cross_search_result)
        return clean_output(cross_search_result)
    
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
    # Pattern 5: Build Grounded Prompt (with conversation history)
    # ===========================================
    # Build conversation context for the LLM
    history_context = ""
    if query_parser and query_parser.conversation_history:
        history_lines = ["Previous conversation:"]
        for turn in query_parser.conversation_history[-3:]:
            history_lines.append(f"User: {turn['query']}")
            history_lines.append(f"Assistant: {turn['answer_preview']}")
        history_context = "\n".join(history_lines) + "\n\n"
    
    prompt = build_prompt(
        context=history_context + "\n\n".join([c["text"] for c in chunks]),
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
    
    # Save to conversation history
    if query_parser:
        query_parser.add_raw_to_history(query, answer)
    
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
    
    # Persistent query parser with conversation history
    query_parser = QueryParser(llm)
    
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
            answer = handle_query(query, embedder, llm, qa_store, analytical_engine, query_parser)
            
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