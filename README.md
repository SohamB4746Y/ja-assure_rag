# JA Assure RAG System

Production-ready RAG system for Malaysian insurance proposal intelligence. Combines deterministic analytics with semantic retrieval to answer questions about 15 insurance proposals across 3 industries.

## System Overview

**Dataset:** 15 Malaysian insurance proposals (complete submissions)
- **Jewellery & Gold:** 5 proposals, RM 26.3M insured
- **Money Services:** 6 proposals, RM 13.65M insured
- **Pawnbrokers:** 4 proposals, RM 5.63M insured
- **Total Insured Value:** RM 45.58M

**Query Capabilities:**
- Analytical aggregations (company counts, average turnaround, claim frequency)
- Field lookups (insured values, security features, business details)
- Multi-field compound queries (e.g., "values AND GPS in Perak")
- Natural language understanding via LLM
- Multi-question support with sub-question routing

**Key Metrics:**
- Average underwriting turnaround time: 17.6 days (min 15, max 22)
- Claims reported: 0 (all proposals report no claims within 3 years)
- FAISS vectors: 300 (one per section, 384 dimensions)
- Query types supported: 8+ analytical methods + semantic search

## Requirements

- Python 3.9+
- Virtual environment (venv/conda/poetry)
- 4+ GB disk space (for FAISS index)
- Cerebras API key (optional, for LLM formatting)

## Installation

```bash
# Clone repository
git clone https://github.com/SohamB4746Y/ja-assure_rag.git
cd ja-assure_rag

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set Cerebras API key (optional)
export CEREBRAS_API_KEY="your-api-key-here"
```

## Quick Start

### Query Examples

```
python main.py

# Interactive queries
> What is the average underwriting turnaround time?
Underwriting turnaround time (based on 15 proposals):
  Average: 17.6 days
  Minimum: 15 days
  Maximum: 22 days

> List companies with highest number of active policies?
Companies ranked by number of active policies (15 companies):
  1. Ja Assure IN -- 1 policy(ies)
  2. FinSecure Money Services -- 1 policy(ies)
  ... (15 companies, each with 1 policy)

> What industries have the highest total insured value?
Industry breakdown by total sum insured:
  - Jewellery & Gold: 5 proposals, total RM 26,300,000, average RM 5,260,000
  - Money Services: 6 proposals, total RM 13,650,000, average RM 2,275,000
  - Pawnbrokers: 4 proposals, total RM 5,630,000, average RM 1,407,500
```

## System Architecture

### Query Pipeline

1. **Scope Check** (~1ms) - Classify as in-scope, out-of-scope, or nonsensical
2. **Analytical Engine** - Check deterministic aggregations first (pure Python, no LLM)
3. **Compound Handler** - Process multi-field AND/OR queries
4. **Predefined QA** - Fast-path for common questions
5. **LLM Parser** - Natural language understanding via Cerebras
6. **Smart Executor** - Deterministic metadata lookups
7. **Semantic Search** - FAISS-based similarity retrieval
8. **LLM Formatter** - Format results into natural language

### Data Pipeline

1. **Excel Loading** - Read JADE-Fields DB from tbl_MY sheet (15 rows)
2. **JSON Parsing** - Robust handling of embedded JSON with smart quotes, NaN floats
3. **Section Extraction** - Split data into 20 sections per proposal (300 chunks total)
4. **Text Building** - Create readable text summary for each section
5. **Embedding** - Generate 384-dim vectors via sentence-transformers
6. **FAISS Index** - Persist index and metadata for fast retrieval

## Project Structure

```
├── main.py                          # CLI entry point and query pipeline
├── api.py                           # FastAPI REST endpoint
├── requirements.txt                 # Python dependencies
├── README.md                        # This file
│
├── data/
│   └── JADE-Fields DB(Integrated)_Mentor Copy.xlsx
│
├── index/
│   ├── index.faiss                  # FAISS vector index (300 vectors)
│   └── metadata.pkl                 # Chunk metadata + field values
│
├── logs/
│   ├── query_log.json               # Query audit trail
│   └── system.log                   # System logs
│
├── evaluation/
│   ├── predefined_qa.json           # Pre-canned Q&A pairs
│   ├── test_set.json                # Test queries
│   └── test_queries.py              # Test suite
│
├── loader/                          # Data ingestion pipeline
│   ├── excel_loader.py              # Read Excel → DataFrame
│   ├── json_cleaner.py              # Parse embedded JSON robustly
│   ├── section_extractor.py         # Split into 20 sections per proposal
│   └── __init__.py
│
├── embeddings/                      # Embedding and retrieval
│   └── embedder.py                  # sentence-transformers wrapper
│
└── src/                             # Core business logic
    ├── analytical_engine.py         # Deterministic aggregations (12 methods)
    ├── query_parser.py              # LLM-assisted query understanding
    ├── query_classifier.py          # Intent classification & scoping
    ├── query_executor.py            # Deterministic field lookups
    ├── answer_formatter.py          # LLM-based natural language formatting
    ├── compound_query_handler.py    # Multi-field query support
    ├── llm_client.py                # Cerebras API integration
    ├── qa_store.py                  # Predefined Q&A management
    ├── mappings.py                  # Code → label decodings
    ├── text_builder.py              # Section text generation
    ├── prompt_builder.py            # LLM prompt templates
    ├── output_cleaner.py            # Sanitization & formatting
    ├── schemas.py                   # Data class definitions
    ├── validator.py                 # Data validation helpers
    └── __init__.py
```

## Configuration

Edit configuration in main.py:

```python
EXCEL_PATH = "data/JADE-Fields DB(Integrated)_Mentor Copy.xlsx"
SHEET_NAME = "tbl_MY"
PREDEFINED_SIMILARITY_THRESHOLD = 0.85  # For predefined QA matching
CHUNK_SIMILARITY_THRESHOLD = 0.5        # For FAISS retrieval
TOP_K_CHUNKS = 5                        # Max chunks to retrieve
```

## Analytical Engine Methods

The AnalyticalEngine provides 12 deterministic methods:

1. **get_company_policy_counts()** - Companies ranked by policy count
2. **get_average_claim_amount()** - Avg claim amount (RM 0, no claims)
3. **get_average_underwriting_tat()** - Turnaround time (17.6 days avg)
4. **get_regions_by_claim_frequency()** - Regions ranked by claim rate
5. **get_top_insured_policies()** - Policies by insured value
6. **get_industry_totals()** - Aggregates by industry
7. **get_claim_stats_by_region()** - Claims by state
8. **get_policies_above_threshold()** - Filter by insured value
9. **get_security_features()** - CCTV, alarm, GPS, guards, etc.
10. **get_gps_stats()** - GPS tracker deployment rates
11. **get_policy_type_distribution()** - Alias for industry totals
12. **get_claim_ratio()** - Claim vs. total proposal count

## Supported Query Types

### Analytical Queries
- "What is the average underwriting turnaround time?" → 17.6 days
- "List companies with highest number of active policies" → All have 1
- "What is the average claim amount per property?" → RM 0 (no claims)
- "List regions with lowest claim frequency" → All tied at 0%

### Field Lookups
- "What is the insured value of MYJADEQT001?" → RM 8,000,000
- "Which proposals have GPS tracking?" → 15/15 have various GPS coverage

### Compound Queries
- "Values above RM 5M with GPS in Selangor" → Matching proposals
- "Jewellery & Gold businesses with strong rooms" → Filtered list

### Industry/Regional Queries
- "Top industries by total sum insured" → Full breakdown
- "Claim statistics by region" → Per-state analysis
- "Security features for all proposals" → CCTV, alarm, guards, etc.

## Development

### Rebuild FAISS Index

```bash
rm -f index/index.faiss index/metadata.pkl
python main.py --rebuild
```

This will:
1. Reload Excel data
2. Reparse all JSON sections
3. Regenerate text chunks
4. Embed to 384 dimensions
5. Rebuild FAISS index with metadata

### Run Tests

```bash
python test_queries.py      # Test analytical engine
python test_api.py          # Test REST API
python evaluation.py        # Evaluate on test set
```

### View Logs

```bash
tail -f logs/system.log     # System events
cat logs/query_log.json | python -m json.tool  # Query audit trail
```

## Code Quality

- **No external pandas usage** in core analytics (pure Python)
- **Zero LLM hallucination** on numeric values (deterministic-only)
- **Production-ready documentation** with architecture diagrams
- **Robust JSON parsing** with fallback strategies
- **Lazy-loaded singletons** for classifiers and handlers

## Data Fields Captured

**Proposal-Level:**
- quote_id, business_name, risk_location, user_name
- created_at, is_paid_on_date (for TAT calculation)
- Submission completeness score

**Financial:**
- Insured values by type (stock, forex, cash, pledged)
- Industry classification (derived from populated fields)

**Security Features:**
- CCTV (recording), Alarm, Strong room, Safe grade
- Armoured vehicles, GPS trackers (vehicles & bags)
- Armed guards (transit & premise)

**Claims & History:**
- Claim presence (no claims vs. claims within 3 years)
- Claim amount (summed from nested details)
- Shop lifting coverage

## Deployment

### REST API
```bash
uvicorn api:app --reload  # Dev
gunicorn -w 4 -k uvicorn.workers.UvicornWorker api:app  # Prod
```

API endpoint: `POST /query`
```json
{
  "query": "What is the average underwriting turnaround time?",
  "multi_question": false
}
```

### Environment Variables

```bash
CEREBRAS_API_KEY=your-key-here   # LLM formatting (optional)
LOG_LEVEL=INFO                     # Logging level
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Ensure all tests pass
4. Commit with clear messages
5. Push and create a PR

## License

Proprietary - JA Assure

## Contact

For questions or issues, please open a GitHub issue.
