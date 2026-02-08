import faiss
import pickle
import os
import numpy as np


class FAISSIndex:
    def __init__(self, dim: int):
        self.dim = dim
        self.index = faiss.IndexFlatIP(dim)  # cosine similarity
        self.metadata = []

    def add(self, vectors, metadatas):
        self.index.add(np.array(vectors).astype("float32"))
        self.metadata.extend(metadatas)

    def search(self, query_vector, top_k=5):
        scores, indices = self.index.search(
            np.array([query_vector]).astype("float32"), top_k
        )
        results = []
        for idx, score in zip(indices[0], scores[0]):
            if idx == -1:
                continue
            results.append({
                "score": float(score),
                "metadata": self.metadata[idx]
            })
        return results

    def save(self, index_path, metadata_path):
        faiss.write_index(self.index, index_path)
        with open(metadata_path, "wb") as f:
            pickle.dump(self.metadata, f)

    def load(self, index_path, metadata_path):
        self.index = faiss.read_index(index_path)
        with open(metadata_path, "rb") as f:
            self.metadata = pickle.load(f)
