# JA Assure RAG

Pipeline for extracting section-wise chunks from the JADE Excel dataset, converting them into deterministic text, and building a FAISS index for retrieval.

## Requirements
- Python 3.9+
- Packages: `pandas`, `sentence-transformers`, `faiss-cpu`

## Setup
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt  # if available
pip install pandas sentence-transformers faiss-cpu
```

## Run
```bash
python main.py
```

## Pipeline Phases
- **Phase 2.1**: Load Excel, clean JSON fields, extract section chunks.
- **Phase 2.2**: Build deterministic text for each section.
- **Phase 2.3**: Create embeddings and persist a FAISS index.

## Output
- FAISS index: `index/index.faiss`
- Metadata: `index/metadata.pkl`

## Project Structure
```
main.py
loader/
  excel_loader.py
  json_cleaner.py
  section_extractor.py
src/
  text_builder.py
  schemas.py
  mappings.py
  validator.py
embeddings/
  embedder.py
index/
  faiss_index.py
```
