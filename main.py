from loader.excel_loader import load_excel
from loader.json_cleaner import parse_json_cell
from loader.section_extractor import extract_sections
from src.text_builder import build_section_text
from src.llm_client import LLMClient
from embeddings.embedder import Embedder
from index.faiss_index import FAISSIndex

import os
import pickle
import faiss
import numpy as np
import re


EXCEL_PATH = "data/JADE-Fields DB(Integrated)_Mentor Copy.xlsx"
SHEET_NAME = "tbl_MY"
INDEX_PATH = "index/index.faiss"
METADATA_PATH = "index/metadata.pkl"


# -------------------------------
# Phase 2.1 — Ingestion
# -------------------------------
def run_phase_2_1():
    df = load_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    all_sections = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        sections = extract_sections(row_dict, parse_json_cell)
        all_sections.extend(sections)

    print(f"[Phase 2.1] Total section chunks created: {len(all_sections)}")
    return all_sections


# -------------------------------
# Phase 2.2 — Text + Structured Storage
# -------------------------------
def run_phase_2_2(section_chunks):
    text_chunks = []

    for chunk in section_chunks:
        text = build_section_text(chunk)

        if text:
            text_chunks.append({
                "quote_id": chunk["quote_id"],
                "section": chunk["section"],
                "text": text,
                "fields": chunk["data"],       
                "metadata": chunk["metadata"]
            })

    print(f"[Phase 2.2] Text chunks created: {len(text_chunks)}")
    return text_chunks


# -------------------------------
# Phase 2.3 — Embeddings + FAISS
# -------------------------------
def run_phase_2_3(text_chunks):
    texts = [chunk["text"] for chunk in text_chunks]

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

    embedder = Embedder()
    vectors = embedder.embed_texts(texts)

    dim = vectors.shape[1]
    index = FAISSIndex(dim)
    index.add(vectors, metadatas)

    os.makedirs("index", exist_ok=True)
    index.save(INDEX_PATH, METADATA_PATH)

    print(f"[Phase 2.3] FAISS index built")
    print(f"- Vectors indexed: {len(texts)}")
    print(f"- Embedding dimension: {dim}")


# -------------------------------
# Extract Quote ID
# -------------------------------
def extract_quote_id(query: str):
    match = re.search(r"MYJADEQT\d+", query)
    return match.group(0) if match else None


# -------------------------------
# Structured Retrieval 
# -------------------------------
def structured_lookup(query: str):
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)

    query_lower = query.lower()
    quote_id = extract_quote_id(query)

    for chunk in metadata:

        # If query contains quote_id, filter strictly
        if quote_id and chunk["quote_id"] != quote_id:
            continue

        fields = chunk.get("fields", {})

        # Skip list-type sections (like claim history arrays)
        if not isinstance(fields, dict):
            continue

        for field_name, value in fields.items():
            normalized_field = (
                field_name.lower()
                .replace("_label", "")
                .replace("_", " ")
            )

            if normalized_field in query_lower:
                return {
                    "quote_id": chunk["quote_id"],
                    "field": normalized_field,
                    "value": value
                }

    return None


# -------------------------------
# Retrieval 
# -------------------------------
def retrieve_chunks(query: str, top_k=5):
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)

    quote_id_in_query = extract_quote_id(query)

    index = faiss.read_index(INDEX_PATH)
    embedder = Embedder()
    query_vector = embedder.embed_texts([query])[0]

    scores, indices = index.search(
        np.array([query_vector]).astype("float32"),
        top_k
    )

    results = []

    for idx in indices[0]:
        if idx != -1:
            chunk = metadata[idx]

            if quote_id_in_query:
                if chunk["quote_id"] == quote_id_in_query:
                    results.append(chunk)
            else:
                results.append(chunk)

    return results


# -------------------------------
# Analytical Query Logic
# -------------------------------
def analytical_query(query: str):
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)

    query_lower = query.lower()

    if "how many" in query_lower and "cctv maintenance" in query_lower:
        proposals = set()

        for chunk in metadata:
            if "cctv" in chunk["section"].lower():
                fields = chunk.get("fields", {})

                if not isinstance(fields, dict):
                    continue

                for field_name, value in fields.items():
                    if "maintenance" in field_name.lower():
                        if str(value) == "001":
                            proposals.add(chunk["quote_id"])

        return {
            "count": len(proposals),
            "quotes": sorted(list(proposals))
        }

    return None


# -------------------------------
# MAIN — Interactive System
# -------------------------------
def main():
    print("\n=== JA Assure | Structured + RAG System ===\n")

    section_chunks = run_phase_2_1()
    text_chunks = run_phase_2_2(section_chunks)
    run_phase_2_3(text_chunks)

    print("\nSystem ready. Type 'exit' to quit.\n")

    llm = LLMClient()

    while True:
        query = input("\nEnter your question: ")

        if query.lower() == "exit":
            print("Exiting system.")
            break

        # 1. Analytical Mode
        analysis = analytical_query(query)
        if analysis:
            print("\n=== ANALYTICAL RESULT ===\n")
            print(f"Count: {analysis['count']}")
            print("Proposals:", analysis["quotes"])
            continue

        # 2. Structured Deterministic Lookup
        structured = structured_lookup(query)
        if structured:
            print("\n=== ANSWER ===\n")
            print(
                f"{structured['field'].title()} "
                f"for {structured['quote_id']} is {structured['value']}"
            )
            continue

        # 3. Semantic RAG Fallback
        retrieved = retrieve_chunks(query)

        if not retrieved:
            print("No relevant proposal records found.")
            continue

        context = "\n\n".join([chunk["text"] for chunk in retrieved])

        prompt = f"""
You are an insurance intelligence assistant.
Answer ONLY using the provided proposal records.

Context:
{context}

Question:
{query}

If the answer is not present in the context, respond with:
"Data not available in proposal records."
"""

        response = llm.generate(prompt)

        print("\n=== ANSWER ===\n")
        print(response)


if __name__ == "__main__":
    main()