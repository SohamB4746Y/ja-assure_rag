from loader.excel_loader import load_excel
from loader.json_cleaner import parse_json_cell
from loader.section_extractor import extract_sections
from src.text_builder import build_section_text
from embeddings.embedder import Embedder
from index.faiss_index import FAISSIndex
import os

EXCEL_PATH = "data/JADE-Fields DB(Integrated)_Mentor Copy.xlsx"
SHEET_NAME = "tbl_MY"
INDEX_PATH = "index/index.faiss"
METADATA_PATH = "index/metadata.pkl"


def run_phase_2_1():
    """
    Phase 2.1
    ----------
    - Load Excel
    - Clean JSON fields
    - Extract section-wise chunks
    """
    df = load_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    all_sections = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()
        sections = extract_sections(row_dict, parse_json_cell)
        all_sections.extend(sections)

    print(f"[Phase 2.1] Total section chunks created: {len(all_sections)}")

    if all_sections:
        print("\n[Phase 2.1] Sample raw section chunk:")
        print(all_sections[0])

    return all_sections


def run_phase_2_2(section_chunks):
    """
    Phase 2.2
    ----------
    - Convert section JSON into deterministic natural language text
    - Prepare embedding-ready text chunks
    """
    text_chunks = []

    for chunk in section_chunks:
        text = build_section_text(chunk)
        if text:
            text_chunks.append({
                "quote_id": chunk["quote_id"],
                "section": chunk["section"],
                "text": text,
                "metadata": chunk["metadata"]
            })

    print(f"\n[Phase 2.2] Text chunks created: {len(text_chunks)}")

    if text_chunks:
        print("\n[Phase 2.2] Sample text chunk:\n")
        print(text_chunks[0]["text"])

    return text_chunks


def run_phase_2_3(text_chunks):
    """
    Phase 2.3
    ----------
    - Generate embeddings
    - Build FAISS index
    - Persist index + metadata
    """
    texts = [chunk["text"] for chunk in text_chunks]
    metadatas = [
        {
            "quote_id": chunk["quote_id"],
            "section": chunk["section"],
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

    print(f"\n[Phase 2.3] FAISS index built")
    print(f"- Vectors indexed: {len(texts)}")
    print(f"- Embedding dimension: {dim}")

    return index


def main():
    print("\n=== JA Assure | Phase 2.1, 2.2 & 2.3 Execution ===\n")

    # Phase 2.1
    section_chunks = run_phase_2_1()

    # Phase 2.2
    text_chunks = run_phase_2_2(section_chunks)

    # Phase 2.3
    run_phase_2_3(text_chunks)

    print("\n=== Phase 2.3 Completed Successfully ===\n")

    return text_chunks


if __name__ == "__main__":
    main()
