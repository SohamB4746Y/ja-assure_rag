"""
Embedder module with batch processing and retry logic.

This module implements Pattern 3: Batch Embedding with Retry and Exponential Backoff.
"""
from __future__ import annotations

import time
import logging
import numpy as np
from sentence_transformers import SentenceTransformer

# Configuration constants
DEFAULT_MODEL = "all-MiniLM-L6-v2"
DEFAULT_BATCH_SIZE = 16
MAX_RETRIES = 3
BASE_DELAY_SECONDS = 2

logger = logging.getLogger(__name__)


class Embedder:
    """
    Sentence Transformer wrapper with batch processing and retry logic.
    """

    def __init__(self, model_name: str = DEFAULT_MODEL):
        """
        Initialize the embedder with a sentence transformer model.

        Args:
            model_name: HuggingFace model name for embeddings.
        """
        self.model_name = model_name
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()

    def embed_texts(
        self,
        texts: list[str],
        show_progress: bool = True,
        normalize: bool = True
    ) -> np.ndarray:
        """
        Embed a list of texts using the sentence transformer.

        Args:
            texts: List of text strings to embed.
            show_progress: Whether to show a progress bar.
            normalize: Whether to normalize embeddings for cosine similarity.

        Returns:
            Numpy array of shape (n_texts, embedding_dim).
        """
        if not texts:
            return np.array([])

        return self.model.encode(
            texts,
            show_progress_bar=show_progress,
            normalize_embeddings=normalize
        )

    def embed_single(self, text: str, normalize: bool = True) -> np.ndarray:
        """
        Embed a single text string.

        Args:
            text: Text string to embed.
            normalize: Whether to normalize the embedding.

        Returns:
            Numpy array of shape (embedding_dim,).
        """
        return self.model.encode(
            [text],
            show_progress_bar=False,
            normalize_embeddings=normalize
        )[0]

    def embed_with_retry(
        self,
        texts: list[str],
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_retries: int = MAX_RETRIES
    ) -> np.ndarray:
        """
        Embed texts in batches with retry logic and exponential backoff.

        This implements Pattern 3 from the Node.js reference:
        - Process embeddings in batches
        - Up to 3 attempts per batch with delay = 2^attempt seconds
        - Log each retry attempt
        - Skip failed batches rather than crashing

        Args:
            texts: List of text strings to embed.
            batch_size: Number of texts per batch.
            max_retries: Maximum retry attempts per batch.

        Returns:
            Numpy array of all successfully embedded texts.
        """
        if not texts:
            return np.array([])

        all_embeddings = []
        total_batches = (len(texts) + batch_size - 1) // batch_size

        logger.info(f"Starting batch embedding: {len(texts)} texts in {total_batches} batches")

        for batch_idx in range(0, len(texts), batch_size):
            batch = texts[batch_idx:batch_idx + batch_size]
            batch_num = batch_idx // batch_size + 1

            batch_embeddings = self._embed_batch_with_retry(
                batch, batch_num, total_batches, max_retries
            )

            if batch_embeddings is not None:
                all_embeddings.append(batch_embeddings)

        if not all_embeddings:
            logger.error("All batches failed. No embeddings generated.")
            return np.array([])

        result = np.vstack(all_embeddings)
        logger.info(f"Batch embedding complete: {len(result)} embeddings generated")

        return result

    def _embed_batch_with_retry(
        self,
        batch: list[str],
        batch_num: int,
        total_batches: int,
        max_retries: int
    ) -> np.ndarray | None:
        """
        Embed a single batch with retry logic.

        Args:
            batch: List of texts in this batch.
            batch_num: Current batch number (for logging).
            total_batches: Total number of batches (for logging).
            max_retries: Maximum retry attempts.

        Returns:
            Numpy array of embeddings, or None if all retries failed.
        """
        for attempt in range(1, max_retries + 1):
            try:
                embeddings = self.model.encode(
                    batch,
                    show_progress_bar=False,
                    normalize_embeddings=True
                )
                logger.debug(f"Batch {batch_num}/{total_batches} succeeded on attempt {attempt}")
                return embeddings

            except Exception as e:
                delay = BASE_DELAY_SECONDS ** attempt
                logger.warning(
                    f"Batch {batch_num}/{total_batches} attempt {attempt} failed: {str(e)}. "
                    f"Retrying in {delay}s..."
                )

                if attempt < max_retries:
                    time.sleep(delay)
                else:
                    logger.error(
                        f"Batch {batch_num}/{total_batches} failed after {max_retries} attempts. "
                        f"Skipping batch."
                    )
                    return None

        return None

    def get_embedding_dimension(self) -> int:
        """Get the embedding dimension for this model."""
        return self.embedding_dim


def cosine_similarity(vec1: np.ndarray, vec2: np.ndarray) -> float:
    """
    Compute cosine similarity between two vectors.

    For normalized vectors (as produced by this embedder), this is
    equivalent to the dot product.

    Args:
        vec1: First vector.
        vec2: Second vector.

    Returns:
        Cosine similarity score between -1 and 1.
    """
    vec1 = np.asarray(vec1).flatten()
    vec2 = np.asarray(vec2).flatten()

    dot_product = np.dot(vec1, vec2)
    norm1 = np.linalg.norm(vec1)
    norm2 = np.linalg.norm(vec2)

    if norm1 == 0 or norm2 == 0:
        return 0.0

    return float(dot_product / (norm1 * norm2))


def batch_cosine_similarity(query_vec: np.ndarray, vectors: np.ndarray) -> np.ndarray:
    """
    Compute cosine similarity between a query vector and multiple vectors.

    Args:
        query_vec: Query vector of shape (dim,).
        vectors: Matrix of vectors of shape (n, dim).

    Returns:
        Array of similarity scores of shape (n,).
    """
    query_vec = np.asarray(query_vec).flatten()
    vectors = np.asarray(vectors)

    if vectors.ndim == 1:
        vectors = vectors.reshape(1, -1)

    # For normalized vectors, cosine similarity = dot product
    similarities = np.dot(vectors, query_vec)

    return similarities

