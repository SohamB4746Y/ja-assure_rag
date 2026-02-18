"""
Predefined Q&A Store for fast-path answers.

This module implements Pattern 2 from the Node.js reference:
- Embeds predefined questions at startup
- Matches incoming queries against predefined embeddings
- Returns trusted answers directly without LLM involvement
"""
from __future__ import annotations

import json
import numpy as np
from typing import Optional
from pathlib import Path

# Configuration constants
PREDEFINED_QA_PATH = "evaluation/predefined_qa.json"


class PredefinedQAStore:
    """
    Stores predefined question-answer pairs with their embeddings.
    Provides fast-path matching for known questions.
    """

    def __init__(self):
        self.qa_pairs: list[dict] = []
        self.question_embeddings: np.ndarray = None
        self.is_loaded: bool = False

    def load(self, qa_pairs: list[dict]) -> None:
        """
        Load predefined Q&A pairs.

        Args:
            qa_pairs: List of dicts with 'question' and 'answer' keys.
        """
        self.qa_pairs = qa_pairs
        self.is_loaded = len(qa_pairs) > 0

    def load_from_file(self, filepath: str = PREDEFINED_QA_PATH) -> None:
        """
        Load predefined Q&A pairs from a JSON file.

        Args:
            filepath: Path to the JSON file containing Q&A pairs.
        """
        path = Path(filepath)
        if not path.exists():
            self.qa_pairs = []
            self.is_loaded = False
            return

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            self.qa_pairs = data
        else:
            self.qa_pairs = data.get("qa_pairs", [])

        self.is_loaded = len(self.qa_pairs) > 0

    def embed_all(self, embedder) -> None:
        """
        Generate embeddings for all predefined questions.

        Args:
            embedder: Embedder instance with embed_texts method.
        """
        if not self.qa_pairs:
            self.question_embeddings = np.array([])
            return

        questions = [qa["question"] for qa in self.qa_pairs]
        embeddings = embedder.embed_texts(questions)
        self.question_embeddings = np.array(embeddings)

    def find_match(
        self, query_embedding: np.ndarray, threshold: float = 0.85
    ) -> Optional[str]:
        """
        Find a predefined answer matching the query above the similarity threshold.

        Args:
            query_embedding: Embedding vector of the incoming query.
            threshold: Minimum cosine similarity to consider a match.

        Returns:
            The predefined answer if a match is found, None otherwise.
        """
        if not self.is_loaded or self.question_embeddings is None:
            return None

        if len(self.question_embeddings) == 0:
            return None

        query_vec = np.array(query_embedding).flatten()

        similarities = []
        for emb in self.question_embeddings:
            sim = self._cosine_similarity(query_vec, emb)
            similarities.append(sim)

        max_idx = np.argmax(similarities)
        max_sim = similarities[max_idx]

        if max_sim >= threshold:
            return self.qa_pairs[max_idx]["answer"]

        return None

    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """
        Compute cosine similarity between two vectors.

        Args:
            vec1: First vector.
            vec2: Second vector.

        Returns:
            Cosine similarity score between -1 and 1.
        """
        vec1 = vec1.flatten()
        vec2 = vec2.flatten()

        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)

        if norm1 == 0 or norm2 == 0:
            return 0.0

        return float(dot_product / (norm1 * norm2))

    def get_all_questions(self) -> list[str]:
        """Get all predefined questions."""
        return [qa["question"] for qa in self.qa_pairs]

    def __len__(self) -> int:
        return len(self.qa_pairs)
