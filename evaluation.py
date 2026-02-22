import json
import pickle
import re
import os
import numpy as np
import faiss

# Enable offline mode for HuggingFace
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

from embeddings.embedder import Embedder
from src.llm_client import LLMClient
from src.mappings import decode_field
from src.prompt_builder import build_prompt
from src.output_cleaner import clean_output

INDEX_PATH = "index/index.faiss"
METADATA_PATH = "index/metadata.pkl"
TEST_SET_PATH = "evaluation/test_set.json"

# Similarity threshold for retrieval
CHUNK_SIMILARITY_THRESHOLD = 0.5


def extract_quote_id(query: str):
    """Extract quote ID from query string."""
    match = re.search(r"MYJADEQT\d+", query, re.IGNORECASE)
    return match.group(0).upper() if match else None


def score_field_match(field_name: str, query: str) -> int:
    """Score how well a field name matches a query based on word overlap."""
    normalized = field_name.replace("_label", "").replace("_", " ").lower()
    field_words = set(normalized.split())
    query_words = set(query.lower().split())
    noise = {"does", "is", "the", "a", "an", "for", "of", "in",
             "what", "which", "how", "many", "have", "has", "this",
             "with", "do", "you"}
    field_words -= noise
    query_words -= noise
    return len(field_words & query_words)


def structured_lookup(query: str, metadata: list) -> str:
    """
    Perform deterministic lookup for a specific field of a specific record.
    Returns formatted answer string if found, else None.
    """
    quote_id = extract_quote_id(query)
    if not quote_id:
        return None
    
    query_lower = query.lower()
    
    # Special handling for risk_location (stored at chunk level)
    location_keywords = ["location", "address", "where", "located", "risk location", "city", "state"]
    if any(kw in query_lower for kw in location_keywords):
        for chunk in metadata:
            if chunk.get("quote_id") != quote_id:
                continue
            risk_location = chunk.get("risk_location")
            if risk_location and isinstance(risk_location, str) and risk_location.strip():
                return f"Risk Location for {quote_id}: {risk_location}"
    
    # Score all fields across matching chunks
    best_match = None
    
    for chunk in metadata:
        if chunk.get("quote_id") != quote_id:
            continue
        
        fields = chunk.get("fields", {})
        if not isinstance(fields, dict):
            continue
        
        for field_name, value in fields.items():
            score = score_field_match(field_name, query)
            if score > 0:
                if best_match is None or score > best_match[0]:
                    best_match = (score, field_name, value)
    
    if best_match and best_match[0] >= 2:
        field_name = best_match[1]
        value = best_match[2]
        decoded_value = decode_field(field_name, str(value))
        human_label = field_name.replace("_label", "").replace("_", " ").title()
        return f"{human_label} for {quote_id}: {decoded_value}"
    
    return None


def retrieve_chunks_filtered(query: str, embedder: Embedder, metadata: list, index, top_k: int = 5):
    """
    Retrieve chunks with quote_id filtering and similarity threshold.
    Returns (chunks, top_similarity_score).
    """
    quote_id = extract_quote_id(query)
    query_vector = embedder.embed_texts([query])[0]
    
    scores, indices = index.search(
        np.array([query_vector]).astype("float32"),
        top_k * 3  # Get extra candidates for filtering
    )
    
    results = []
    top_similarity = 0.0
    
    for score, idx in zip(scores[0], indices[0]):
        if idx == -1:
            continue
        
        if score > top_similarity:
            top_similarity = float(score)
        
        if score < CHUNK_SIMILARITY_THRESHOLD:
            continue
        
        chunk = metadata[idx]
        
        # Filter by quote_id if present in query
        if quote_id and chunk.get("quote_id") != quote_id:
            continue
        
        results.append(chunk)
        
        if len(results) >= top_k:
            break
    
    return results, top_similarity


def contains_match(predicted: str, expected: str) -> bool:
    """Returns True if expected string appears anywhere inside predicted string, case-insensitive"""
    return expected.strip().lower() in predicted.strip().lower()


def token_overlap_score(predicted: str, expected: str) -> float:
    """
    Returns ratio of expected tokens found in predicted text.
    Ignores stopwords: is, the, a, an, of, in, for, to, and, or, that, it, with
    Returns 0.0 to 1.0
    """
    STOPWORDS = {"is", "the", "a", "an", "of", "in", "for", "to", "and", "or", "that", "it", "with"}
    expected_tokens = [t for t in expected.lower().split() if t not in STOPWORDS]
    if not expected_tokens:
        return 1.0
    predicted_lower = predicted.lower()
    matched = sum(1 for t in expected_tokens if t in predicted_lower)
    return matched / len(expected_tokens)


def refused_correctly(predicted: str) -> bool:
    """Returns True if system correctly refused to answer (for DATA_NOT_AVAILABLE test cases)"""
    return "data not available" in predicted.lower()


def analytical_query(query: str, metadata: list) -> str:
    """
    Handle analytical queries that count/aggregate across all proposals.
    Returns answer string if this is an analytical query, else None.
    """
    query_lower = query.lower()
    
    # Only handle "how many" counting queries
    if "how many" not in query_lower:
        return None
    
    # Build a set of unique quote_ids that have already been counted
    counted_quotes = set()
    count = 0
    
    # Determine what field to look for based on query keywords
    field_patterns = []
    yes_values = {"001", "yes", "true", "1"}
    
    if "cctv maintenance" in query_lower or "cctv_maintenance" in query_lower:
        field_patterns = ["cctv_maintenance_contract"]
    elif "alarm" in query_lower and "maintenance" in query_lower:
        field_patterns = ["under_maintenance_contract"]
    elif "armoured" in query_lower or "armored" in query_lower:
        field_patterns = ["armoured_vehicle", "do_you_use_armoured_vehicle"]
    elif "armed guards" in query_lower:
        field_patterns = ["armed_guards", "do_you_use_armed_guards"]
    elif "strong room" in query_lower:
        field_patterns = ["strong_room", "do_you_have_a_strong_room"]
    elif "cctv" in query_lower:
        field_patterns = ["cctv", "recording"]
    elif "alarm" in query_lower:
        field_patterns = ["alarm", "do_you_have_alarm"]
    elif "safe" in query_lower:
        field_patterns = ["safe", "certified"]
    else:
        return None  # Can't determine what to count
    
    # Iterate all metadata chunks
    for chunk in metadata:
        quote_id = chunk.get("quote_id")
        if not quote_id or quote_id in counted_quotes:
            continue  # Already counted this proposal
        
        fields = chunk.get("fields", {})
        if not isinstance(fields, dict):
            continue
        
        # Check each field for the pattern
        for field_name, value in fields.items():
            field_lower = field_name.lower()
            
            # Check if any pattern matches this field name
            if any(pattern in field_lower for pattern in field_patterns):
                # Check if value indicates "Yes"
                value_str = str(value).lower().strip()
                if value_str in yes_values:
                    counted_quotes.add(quote_id)
                    count += 1
                    break  # Don't double-count this proposal
    
    if field_patterns:
        return str(count)
    
    return None


def run_evaluation():
    with open(TEST_SET_PATH, "r") as f:
        test_data = json.load(f)
    
    # Load metadata and index once
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)
    
    index = faiss.read_index(INDEX_PATH)
    embedder = Embedder()
    llm = LLMClient()

    total = len(test_data)
    contains_correct = 0
    total_token_overlap = 0.0

    print("\n=== RUNNING EVALUATION ===\n")

    for item in test_data:
        query = item["question"]
        expected = item["expected_answer"]
        
        # Step 1: Try analytical query first (for "how many" questions)
        prediction = analytical_query(query, metadata)
        
        if prediction is None:
            # Step 2: Try structured lookup (deterministic field lookup)
            prediction = structured_lookup(query, metadata)
        
        if prediction is None:
            # Step 3: Fall back to semantic RAG retrieval
            retrieved, top_sim = retrieve_chunks_filtered(query, embedder, metadata, index)
            
            if not retrieved:
                prediction = "Data not available in proposal records."
            else:
                context = "\n\n".join([chunk["text"] for chunk in retrieved])
                prompt = build_prompt(context, query)
                prediction = clean_output(llm.generate(prompt))

        # Determine correctness based on expected answer type
        if expected == "DATA_NOT_AVAILABLE":
            is_correct = refused_correctly(prediction)
            overlap = 1.0 if is_correct else 0.0
        else:
            is_correct = contains_match(prediction, expected)
            overlap = token_overlap_score(prediction, expected)

        if is_correct:
            contains_correct += 1
        total_token_overlap += overlap

        # Print per-question results
        print("Question:", query)
        print("Expected:", expected)
        print("Predicted:", prediction[:100] + "..." if len(prediction) > 100 else prediction)
        print(f"Contains Match: {is_correct} | Token Overlap: {overlap:.2f}")
        print("-" * 60)

    accuracy = contains_correct / total if total > 0 else 0
    avg_overlap = total_token_overlap / total if total > 0 else 0

    print("\n=== FINAL RESULTS ===")
    print(f"Total Questions: {total}")
    print(f"Contains Match: {contains_correct} / {total} ({accuracy * 100:.1f}%)")
    print(f"Average Token Overlap: {avg_overlap:.2f}")


if __name__ == "__main__":
    run_evaluation()