# JA Assure RAG System

> **Production-grade Retrieval-Augmented Generation system for Malaysian insurance proposal intelligence.**
> Answers natural language queries over 15 insurance proposals with deterministic, hallucination-free data retrieval — the LLM is used only for language understanding and formatting, never for data extraction.

---

## Table of Contents

- [Overview](#overview)
- [Key Design Principles](#key-design-principles)
- [Architecture](#architecture)
  - [System Layers](#system-layers)
  - [Query Pipeline](#query-pipeline)
  - [Data Pipeline](#data-pipeline)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the System](#running-the-system)
  - [Interactive CLI](#interactive-cli)
  - [REST API](#rest-api)
- [Query Capabilities](#query-capabilities)
  - [Analytical Queries](#analytical-queries)
  - [Field Lookups](#field-lookups)
  - [Compound Queries](#compound-queries)
  - [Predefined Q&A](#predefined-qa)
  - [Scope Handling](#scope-handling)
- [API Reference](#api-reference)
- [Data Schema](#data-schema)
- [Analytical Engine](#analytical-engine)
- [Development Guide](#development-guide)
  - [Rebuilding the Index](#rebuilding-the-index)
  - [Running Tests](#running-tests)
  - [Logging & Monitoring](#logging--monitoring)
- [Deployment](#deployment)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

JA Assure RAG is built for **JADE Insurance Malaysia** to enable intelligent querying of insurance proposals covering three industries:

| Industry | Proposals | Total Insured Value | Avg per Policy |
|---|---|---|---|
| Jewellery & Gold | 5 | RM 26,300,000 | RM 5,260,000 |
| Money Services | 6 | RM 13,650,000 | RM 2,275,000 |
| Pawnbrokers | 4 | RM 5,630,000 | RM 1,407,500 |
| **Total** | **15** | **RM 45,580,000** | **RM 3,038,667** |

**Portfolio snapshot:**
- Claims reported: **0** (all 15 proposals report no claims within the 3-year lookback)
- Average underwriting TAT: **17.6 days** (min 15, max 22)
- Vector index: **300 chunks** at 384 dimensions (all-MiniLM-L6-v2)
- Supported query categories: analytical aggregations, field lookups, compound filters, predefined Q&A, semantic search

---

## Key Design Principles

**1. Deterministic-first architecture**
Every query passes through up to 8 deterministic handlers before any generative LLM call. Numeric values and structured fields are always looked up directly from pre-decoded metadata — never inferred or generated.

**2. Pre-decoded metadata**
All field codes (e.g., `"001"` → `"Yes"`, `"JG"` → `"Jewellery & Gold"`) are decoded at index-build time using field-name-as-routing-key. The `SmartQueryExecutor` reads human-readable values directly, eliminating code-collision risk at query time.

**3. Multi-layer scope control**
The `QueryClassifier` intercepts out-of-scope and nonsensical queries before any embedding or LLM call is made, saving latency and preventing hallucination on data that does not exist in the corpus.

**4. Strict prompt engineering**
`SYSTEM_INSTRUCTION` is hardcoded and cannot be influenced by user input. It includes explicit rules for the zero-claim dataset, the transit/guard field distinction, and all binary code mappings.

**5. Conversation context management**
`QueryParser` maintains a rolling 5-turn history with anti-context-bleed logic: entity carry-over is suppressed when the subject of the query changes between turns.

---

## Architecture

### System Layers

```
┌──────────────────────────────────────────────────────────────────┐
│  Layer 4 — Entry Points                                          │
│  main.py (interactive CLI)   api.py (FastAPI REST)               │
│  run_cerebras.sh             run_api.sh                          │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│  Layer 3 — Query Pipeline                                        │
│                                                                  │
│  QueryClassifier ──► AnalyticalEngine ──► PartialAnswerEngine    │
│       │                                                          │
│       ▼                                                          │
│  CompoundQueryHandler ──► PredefinedQAStore                      │
│       │                                                          │
│       ▼                                                          │
│  QueryParser (LLM) ──► SmartQueryExecutor ──► AnswerFormatter    │
│       │                                                          │
│       ▼                                                          │
│  structured_lookup ──► search_proposals_by_value                 │
│       │                                                          │
│       ▼                                                          │
│  FAISS Retrieval ──► FlattenedContext ──► PromptBuilder ──► LLM  │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│  Layer 2 — Ingestion Pipeline (one-time)                         │
│  ExcelLoader ──► JSONCleaner ──► SectionExtractor                │
│       │                                                          │
│       ▼                                                          │
│  TextBuilder ──► Embedder (all-MiniLM-L6-v2) ──► FAISSIndex      │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────────┐
│  Layer 1 — Data Layer                                            │
│  data/JADE-Fields DB.xlsx   index/index.faiss                    │
│  index/metadata.pkl         evaluation/predefined_qa.json        │
└──────────────────────────────────────────────────────────────────┘
```

### Query Pipeline

Each query passes through the following stages in order. The first stage that produces a result short-circuits the rest.

| Stage | Component | Description | LLM? |
|---|---|---|---|
| 1 | `QueryClassifier` | Scope/intent pre-check — pure keyword rules, < 5ms | No |
| 2 | `AnalyticalEngine` | ~15 deterministic aggregation patterns | No |
| 3 | `PartialAnswerEngine` | 30+ specialized data-driven handlers | No |
| 4 | `CompoundQueryHandler` | Multi-field AND/location filter queries | No |
| 5 | `PredefinedQAStore` | Cosine similarity match against 10 curated Q&A pairs (threshold 0.85) | No |
| 6 | `QueryParser` + `SmartQueryExecutor` | LLM parses intent → deterministic metadata lookup | Parse only |
| 7 | `structured_lookup` | Quote ID + word-overlap field matching fallback | No |
| 8 | `search_proposals_by_value` | Cross-proposal location/business-type search | No |
| 9 | FAISS + LLM | Semantic retrieval → flattened context → LLM generation | Yes |

**Out-of-scope and nonsensical queries** are rejected at Stage 1 with an explanation and a suggested alternative — no downstream processing occurs.

### Data Pipeline

Run once automatically when the FAISS index is absent:

```
Excel (tbl_MY, 15 rows)
  └─► load_excel()           — Pandas DataFrame
  └─► parse_json_cell()      — Robust JSON parsing (smart quotes, BOM, NaN floats)
  └─► extract_sections()     — 18 sections per row → 300 chunks with metadata
  └─► build_section_text()   — Human-readable text per chunk (all codes decoded)
  └─► embed_with_retry()     — all-MiniLM-L6-v2, batches of 16, exponential backoff
  └─► FAISSIndex             — IndexFlatIP (inner product ≡ cosine on normalized vectors)
  └─► metadata.pkl           — Pre-decoded field values stored alongside every chunk
```

---

## Project Structure

```
ja-assure_rag_final/
│
├── main.py                          # CLI entry point and query orchestrator
├── api.py                           # FastAPI REST server
├── evaluation.py                    # Offline evaluation harness
├── test_queries.py                  # Analytical engine tests
├── test_api.py                      # REST API tests
├── requirements.txt                 # Python dependencies (pinned)
├── run_cerebras.sh                  # CLI launcher: activates venv, loads .env
├── run_api.sh                       # API launcher: activates venv, loads .env
├── .env.example                     # Environment variable template
│
├── data/
│   └── JADE-Fields DB(Integrated)_Mentor Copy.xlsx   # Source: 15 proposals, sheet tbl_MY
│
├── index/
│   ├── index.faiss                  # FAISS vector index — 300 vectors × 384 dims (460 KB)
│   └── metadata.pkl                 # Chunk metadata + pre-decoded field values (175 KB)
│
├── logs/
│   ├── query_log.json               # JSON audit trail — one entry per query
│   └── system.log                   # Application log (INFO/WARNING/ERROR)
│
├── evaluation/
│   ├── predefined_qa.json           # 10 curated Q&A pairs for fast-path matching
│   └── test_set.json                # Evaluation question set
│
├── loader/                          # Data ingestion pipeline
│   ├── excel_loader.py              # Excel → Pandas DataFrame
│   ├── json_cleaner.py              # Defensive JSON parser for embedded cell data
│   └── section_extractor.py        # Row → per-section chunks with proposal metadata
│
├── embeddings/
│   └── embedder.py                  # sentence-transformers wrapper with retry logic
│
└── src/                             # Core business logic
    ├── query_classifier.py          # Scope gate: ANSWERABLE / PARTIALLY_ANSWERABLE / OUT_OF_SCOPE / NONSENSICAL
    ├── analytical_engine.py         # Deterministic aggregations — 11 getter methods
    ├── query_parser.py              # LLM-assisted NL → structured ParsedQuery
    ├── query_executor.py            # Deterministic metadata lookups (no LLM)
    ├── compound_query_handler.py    # Multi-field AND/location query execution
    ├── answer_formatter.py          # Routes result to LLM formatter or direct output
    ├── qa_store.py                  # Predefined Q&A embedding store
    ├── flattened_context.py         # Builds full decoded proposal text for LLM grounding
    ├── prompt_builder.py            # SYSTEM_INSTRUCTION + context assembly
    ├── llm_client.py                # Cerebras Cloud SDK wrapper
    ├── mappings.py                  # 100+ field code → label decodings across 30+ field types
    ├── output_cleaner.py            # Strip markdown, HTML, thinking tags from LLM output
    ├── text_builder.py              # Section text generation for embedding
    ├── schemas.py                   # Section schema definitions
    └── validator.py                 # sum_assured field validation
```

---

## Installation

### Prerequisites

- Python 3.9+
- A Cerebras Cloud API key — obtain from [cloud.cerebras.ai](https://cloud.cerebras.ai)
- ~2 GB disk space for Python dependencies (torch, sentence-transformers)

### Steps

```bash
# 1. Clone the repository
git clone https://github.com/SohamB4746Y/ja-assure_rag.git
cd ja-assure_rag_final

# 2. Create a virtual environment
python3 -m venv venv
source venv/bin/activate        # macOS / Linux
# venv\Scripts\activate          # Windows

# 3. Install dependencies
pip install -r requirements.txt
pip install cerebras-cloud-sdk  # LLM provider SDK

# 4. Configure environment
cp .env.example .env
# Edit .env and set CEREBRAS_API_KEY and CEREBRAS_MODEL
```

> **Note for macOS users:** Do not copy a `venv/` directory from another machine. macOS enforces binary code signatures — compiled `.so` files must be installed natively via `pip`. Always create a fresh venv and run `pip install`.

---

## Configuration

### `.env`

```bash
# Required
CEREBRAS_API_KEY=your_api_key_here
CEREBRAS_MODEL=llama-3.3-70b        # or qwen-3-235b-a22b-instruct-2507

# Optional
LOG_LEVEL=INFO                      # Python logging level (DEBUG, INFO, WARNING, ERROR)
TRANSFORMERS_OFFLINE=0              # Set to 1 to disable HuggingFace Hub network calls
HF_HUB_OFFLINE=0                    # Set to 1 to use cached model files only
```

### Runtime constants (`main.py`)

| Constant | Default | Description |
|---|---|---|
| `EXCEL_PATH` | `data/JADE-Fields DB(Integrated)_Mentor Copy.xlsx` | Source data file |
| `SHEET_NAME` | `tbl_MY` | Excel worksheet name |
| `INDEX_PATH` | `index/index.faiss` | FAISS index file |
| `METADATA_PATH` | `index/metadata.pkl` | Chunk metadata file |
| `PREDEFINED_QA_PATH` | `evaluation/predefined_qa.json` | Predefined Q&A store |
| `PREDEFINED_SIMILARITY_THRESHOLD` | `0.85` | Min cosine similarity for predefined Q&A match |
| `CHUNK_SIMILARITY_THRESHOLD` | `0.5` | Min cosine similarity for FAISS chunk retrieval |
| `TOP_K_CHUNKS` | `5` | Max chunks retrieved per FAISS query |

### LLM parameters (`src/llm_client.py`)

| Parameter | Value |
|---|---|
| `temperature` | `0.1` |
| `top_p` | `1` |
| `max_completion_tokens` | `1024` |
| `stream` | `False` |

---

## Running the System

### Interactive CLI

```bash
./run_cerebras.sh
```

The script activates the venv, loads `.env`, validates that `CEREBRAS_API_KEY` is set, and starts the interactive REPL.

**REPL commands:**

| Input | Action |
|---|---|
| Any natural language question | Run the query pipeline and print the answer |
| `exit` | Shut down gracefully |
| `rebuild` | Re-ingest the Excel file and rebuild the FAISS index |

**Multi-question input:** Separate questions with `?` or newlines — each sub-question is answered independently.

**Example session:**

```
=== JA Assure | Production RAG System ===

System ready. Type 'exit' to quit.
Type 'rebuild' to re-index the data.

Enter your question: What is the average underwriting turnaround time?
Underwriting turnaround time (based on 15 proposals):
  Average: 17.6 days
  Minimum: 15 days
  Maximum: 22 days

Enter your question: What industries have the highest total insured value?
Industry breakdown by total sum insured:
  - Jewellery & Gold: 5 proposals, total RM 26,300,000, average RM 5,260,000
  - Money Services: 6 proposals, total RM 13,650,000, average RM 2,275,000
  - Pawnbrokers: 4 proposals, total RM 5,630,000, average RM 1,407,500

Enter your question: What is the insured value of MYJADEQT001?
The insured value for MYJADEQT001 is RM 8,000,000.

Enter your question: exit
```

### REST API

```bash
./run_api.sh
```

Starts a uvicorn server at `http://0.0.0.0:8000`. Interactive API docs are available at `http://localhost:8000/docs`.

---

## Query Capabilities

### Analytical Queries

Handled by `AnalyticalEngine` with pure Python — no LLM, no FAISS retrieval.

```
What is the average underwriting turnaround time?
  → Average: 17.6 days | Minimum: 15 days | Maximum: 22 days

What industries have the highest total insured value?
  → Jewellery & Gold: RM 26.3M | Money Services: RM 13.65M | Pawnbrokers: RM 5.63M

Which proposals have insured value above RM 5 million?
  → Filtered list of proposals above the specified threshold

How many proposals have GPS tracking?
  → Count with breakdown by GPS vehicle vs. GPS bag

What are the claim statistics by region?
  → Per-state breakdown; all proposals currently report zero claims
```

### Field Lookups

Handled by `SmartQueryExecutor` after `QueryParser` extracts a structured query. Deterministic metadata lookup — no generative LLM for the data itself.

```
What is the insured value of MYJADEQT001?
What security features does MYJADEQT007 have?
Who is the contact person for MYJADEQT012?
What is the safe grade for the premises at MYJADEQT003?
What type of alarm system is installed at MYJADEQT005?
```

### Compound Queries

Handled by `CompoundQueryHandler` which builds O(1) lookup maps at initialisation.

```
List all proposals in Penang with GPS tracking
Show insured value and CCTV status for Money Services proposals
Which Jewellery & Gold businesses have a strong room?
Proposals in Selangor with alarm systems and armoured vehicles
```

### Predefined Q&A

Matched by cosine similarity (threshold 0.85) against 10 curated pairs — answered without any LLM call.

```
What types of businesses does JA Assure cover?
What security features are assessed in a proposal?
How are claims handled?
What safe grades are recognised?
What transit coverage is included?
```

### Scope Handling

The `QueryClassifier` intercepts queries before any downstream processing:

| Classification | Trigger | Response |
|---|---|---|
| `OUT_OF_SCOPE` | Premium, underwriting decisions, renewal dates, policy expiry | Explains data is not available; suggests what *is* available |
| `NONSENSICAL` | Self-contradictory queries, < 2 words | Explains the issue |
| `PARTIALLY_ANSWERABLE` | Related data exists but answer is indirect | Routed to the appropriate `PartialAnswerEngine` handler |

```
What is the premium for MYJADEQT001?
  → "Premium data is not captured in the proposal records. Available data
     includes insured values, security features, and underwriting dates."
```

---

## API Reference

### `GET /health`

Liveness check.

**Response `200 OK`:**
```json
{ "status": "ok" }
```

---

### `POST /query`

Submit a natural language question.

**Request body:**
```json
{
  "question": "What is the average underwriting turnaround time?"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `question` | string | Yes | Natural language question. Separate multiple questions with `?` or newlines for multi-question mode. |

**Response `200 OK`:**
```json
{
  "question": "What is the average underwriting turnaround time?",
  "answer": "Underwriting turnaround time (based on 15 proposals):\n  Average: 17.6 days\n  Minimum: 15 days\n  Maximum: 22 days"
}
```

**Error `500`:**
```json
{
  "detail": "Internal server error message"
}
```

---

## Data Schema

### Proposal record fields

**Identity & Dates**
| Field | Description | Example |
|---|---|---|
| `quote_id` | Unique proposal identifier | `MYJADEQT001` |
| `risk_location` | Full address of insured premises | `George Town, Penang, Malaysia` |
| `user_name` | Contact person / director name | `Ahmad bin Razali` |
| `created_at` | Proposal creation date (TAT start) | `2024-01-15` |
| `is_paid_on_date` | Payment date (TAT end) | `2024-02-01` |

**Financial**
| Field | Description |
|---|---|
| `sum_assured.stock_label` | Stock-in-trade insured value (Jewellery & Gold) |
| `sum_assured.forex_label` | Foreign exchange value (Money Services) |
| `sum_assured.cash_label` | Cash on premises |
| `sum_assured.pledged_items_label` | Pledged item value (Pawnbrokers) |

**Security Features**
| Field | Values |
|---|---|
| `cctv.recording_label` | Yes / No — CCTV recording capability |
| `alarm.alarm_type_label` | Alarm system type |
| `strong_room` | Strong room presence and specification |
| `safe.safe_grade_label` | Safe grade (TDR-15, TDR-30, etc.) |
| `transit_and_guards.armoured_vehicle_label` | Armoured vehicle in use |
| `transit_and_guards.gps_vehicle_label` | GPS on transit vehicles |
| `transit_and_guards.gps_bag_label` | GPS on transit bags |
| `transit_and_guards.armed_guard_transit_label` | Armed guard during transit |

**Claims**
| Field | Description |
|---|---|
| `claim_history` | Nested claim records (all 15 proposals: no claims) |
| `shop_lifting` | Shop lifting coverage flag |

---

## Analytical Engine

`src/analytical_engine.py` — all methods return formatted strings; zero LLM calls.

| Method | Description |
|---|---|
| `get_top_insured_policies(limit)` | Proposals ranked by insured value descending |
| `get_industry_totals()` | Count, total RM, average RM per industry |
| `get_claim_stats_by_region()` | Per-state: proposals with / without claims |
| `get_policies_above_threshold(rm)` | Filter by RM threshold; supports "above RM 5M" and "above 5 million" |
| `get_security_features()` | All 9 security flags for every proposal |
| `get_gps_stats()` | GPS vehicle and bag counts with missing-coverage lists |
| `get_company_policy_counts()` | Active policies per business name |
| `get_average_claim_amount()` | Average claim amount across proposals with claims |
| `get_average_underwriting_tat()` | Days between `created_at` and `is_paid_on_date` |
| `get_regions_by_claim_frequency(ascending)` | States ranked by claim rate |
| `is_field_available(query)` | Returns a "not in database" message for premium/expiry/renewal queries |

The `run(query)` dispatcher pattern-matches the query against ~15 trigger groups and routes to the appropriate getter. Claim-related ranking queries hit a special guard that returns the zero-claim message immediately rather than returning a misleading ranked list.

---

## Development Guide

### Rebuilding the Index

If the source data changes, rebuild the FAISS index from within the REPL:

```
Enter your question: rebuild
```

Or reset manually then restart:

```bash
rm -f index/index.faiss index/metadata.pkl
./run_cerebras.sh
```

Rebuild steps:
1. Reload Excel from `data/`
2. Re-parse all JSON sections
3. Regenerate text chunks with decoded field values
4. Re-embed to 384 dimensions (batches of 16, up to 3 retries)
5. Rebuild `IndexFlatIP` FAISS index
6. Persist `index.faiss` and `metadata.pkl`

### Running Tests

```bash
# Activate venv and load env first
source venv/bin/activate
source .env

python test_queries.py      # Analytical engine and query routing tests
python test_api.py          # REST API endpoint tests
python evaluation.py        # Evaluate system against test_set.json
```

### Logging & Monitoring

**System log** — tail live output:
```bash
tail -f logs/system.log
```

Key log events:
- `Embedder initialized: 384 dimensions` — boot complete
- `AnalyticalEngine: loaded N proposal records` — engine ready
- `Handled by analytical engine` — Stage 2 short-circuit
- `Matched predefined Q&A` — Stage 5 short-circuit
- `No chunks above threshold` — FAISS retrieval returned nothing above 0.5

**Query audit log** — inspect with pretty-print:
```bash
python -m json.tool logs/query_log.json
```

Each audit entry contains:

```json
{
  "timestamp": "2026-03-17T11:31:10.123456",
  "query": "What is the insured value of MYJADEQT001?",
  "query_type": "smart_executor",
  "quote_id_extracted": "MYJADEQT001",
  "num_chunks_retrieved": 0,
  "top_similarity_score": null,
  "answer_length": 58
}
```

**`query_type` values:**

| Value | Stage |
|---|---|
| `out_of_scope` / `refused` | Stage 1 — scope gate |
| `analytical` | Stage 2 — AnalyticalEngine |
| `partial_answer` | Stage 3 — PartialAnswerEngine |
| `compound` | Stage 4 — CompoundQueryHandler |
| `predefined` | Stage 5 — PredefinedQAStore |
| `smart_executor` | Stage 6 — QueryParser + SmartQueryExecutor |
| `structured` | Stage 7 — structured_lookup |
| `cross_search` | Stage 8 — search_proposals_by_value |
| `semantic` | Stage 9 — FAISS + LLM generation |

---

## Deployment

### Development (local)

```bash
./run_api.sh
# API available at http://localhost:8000
# Interactive docs at http://localhost:8000/docs
```

### Production

```bash
# Multi-worker uvicorn (recommended for production)
source venv/bin/activate
gunicorn -w 4 -k uvicorn.workers.UvicornWorker api:app --bind 0.0.0.0:8000
```

### Environment checklist

- [ ] `CEREBRAS_API_KEY` set in `.env` or injected via secrets manager
- [ ] `CEREBRAS_MODEL` set to the intended model name
- [ ] `index/index.faiss` and `index/metadata.pkl` present (run once without them to auto-build)
- [ ] `logs/` directory writable
- [ ] `data/` directory contains the source Excel file

---

## Contributing

1. Fork the repository and create a feature branch from `main`
2. Follow the existing layered architecture — keep data access in `loader/`, business logic in `src/`, entry points in root
3. All new analytical methods must be deterministic — no LLM calls in `AnalyticalEngine` or `SmartQueryExecutor`
4. New field code mappings belong in `src/mappings.py` under the appropriate field key
5. Ensure all tests pass before opening a pull request
6. Write clear, present-tense commit messages (`Add handler for fidelity queries`, not `Added...`)

---

## License

Proprietary — JADE Insurance Malaysia. All rights reserved.

---

## Contact

For questions or issues, open a GitHub issue on the repository.
