import json
import pickle
import numpy as np
import faiss

from embeddings.embedder import Embedder
from src.llm_client import LLMClient

INDEX_PATH = "index/index.faiss"
METADATA_PATH = "index/metadata.pkl"
TEST_SET_PATH = "evaluation/test_set.json"


def retrieve_chunks(query, top_k=5):
    with open(METADATA_PATH, "rb") as f:
        metadata = pickle.load(f)

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
            results.append(metadata[idx])

    return results


def exact_match(predicted, expected):
    return predicted.strip().lower() == expected.strip().lower()


def run_evaluation():
    with open(TEST_SET_PATH, "r") as f:
        test_data = json.load(f)

    llm = LLMClient()

    total = len(test_data)
    correct = 0

    print("\n=== RUNNING EVALUATION ===\n")

    for item in test_data:
        query = item["question"]
        expected = item["expected_answer"]

        retrieved = retrieve_chunks(query)
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

        prediction = llm.generate(prompt)

        is_correct = exact_match(prediction, expected)

        if is_correct:
            correct += 1

        print("Question:", query)
        print("Expected:", expected)
        print("Predicted:", prediction)
        print("Correct:", is_correct)
        print("-" * 60)

    accuracy = correct / total

    print("\n=== FINAL RESULTS ===")
    print("Total Questions:", total)
    print("Correct:", correct)
    print("Accuracy:", round(accuracy, 4))


if __name__ == "__main__":
    run_evaluation()