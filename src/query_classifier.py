"""
Dynamic Query Classifier for routing queries to appropriate handlers.

This module implements Pattern 8: Intelligent Query Type Classification.
Classification is based on linguistic patterns, not hardcoded query strings.
"""
from __future__ import annotations

import re
from typing import Literal, Optional, List

# Configuration: Signal words for each query type
# These can be modified without changing the classification logic

AGGREGATION_SIGNALS = [
    "how many",
    "count",
    "total",
    "average",
    "sum",
    "which proposals",
    "list all",
    "compare",
    "most common",
    "percentage",
    "majority",
    "all proposals",
    "number of",
    "how much",
    "across all",
    "summarize",
    "aggregate",
]

COMPARISON_SIGNALS = [
    "highest",
    "lowest",
    "maximum",
    "minimum",
    "most",
    "least",
    "top",
    "bottom",
    "best",
    "worst",
    "greater than",
    "less than",
    "more than",
    "fewer than",
]

STRUCTURED_FIELD_SIGNALS = [
    "what is the",
    "what are the",
    "does",
    "do they",
    "is there",
    "are there",
    "show me the",
    "tell me the",
    "give me the",
    "what kind of",
    "what type of",
]

# Quote ID pattern for structured queries
QUOTE_ID_PATTERN = re.compile(r"MYJADEQT\d+", re.IGNORECASE)

QueryType = Literal["predefined", "analytical", "structured", "semantic"]


def classify_query(query: str) -> QueryType:
    """
    Classify a query into one of four types based on linguistic patterns.

    Args:
        query: The user's question string.

    Returns:
        One of: "analytical", "structured", "semantic"
        Note: "predefined" is handled separately by PredefinedQAStore before this.

    Classification rules (in order of priority):
    1. analytical: Contains aggregation or comparison signals
    2. structured: Contains a quote ID AND asks about a specific field
    3. semantic: Everything else (RAG retrieval)
    """
    query_lower = query.lower().strip()

    # Check for aggregation signals (analytical queries)
    for signal in AGGREGATION_SIGNALS:
        if signal in query_lower:
            return "analytical"

    # Check for comparison signals that imply aggregation
    for signal in COMPARISON_SIGNALS:
        if signal in query_lower:
            # If comparison signal exists, it's analytical
            return "analytical"

    # Check for structured query (quote ID + field question)
    quote_id_match = QUOTE_ID_PATTERN.search(query)
    if quote_id_match:
        # Has quote ID - check if asking about a specific field
        for signal in STRUCTURED_FIELD_SIGNALS:
            if signal in query_lower:
                return "structured"
        # Has quote ID but no clear field signal - still structured
        return "structured"

    # Default to semantic retrieval
    return "semantic"


def extract_quote_id(query: str) -> Optional[str]:
    """
    Extract a quote ID from the query if present.

    Args:
        query: The user's question string.

    Returns:
        The quote ID (e.g., "MYJADEQT001") or None if not found.
    """
    match = QUOTE_ID_PATTERN.search(query)
    return match.group(0).upper() if match else None


def extract_field_keywords(query: str) -> List[str]:
    """
    Extract potential field keywords from a query.

    Args:
        query: The user's question string.

    Returns:
        List of lowercase keywords that might match field names.
    """
    # Remove common stop words and question words
    stop_words = {
        "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
        "have", "has", "had", "do", "does", "did", "will", "would", "could",
        "should", "may", "might", "must", "shall", "can", "of", "for", "to",
        "in", "on", "at", "by", "from", "with", "about", "into", "through",
        "during", "before", "after", "above", "below", "between", "under",
        "again", "further", "then", "once", "what", "which", "who", "whom",
        "this", "that", "these", "those", "am", "and", "but", "if", "or",
        "because", "as", "until", "while", "how", "many", "much", "where",
        "when", "why", "all", "each", "every", "both", "few", "more", "most",
        "other", "some", "such", "no", "not", "only", "own", "same", "so",
        "than", "too", "very", "just", "also", "now", "here", "there", "any",
        "tell", "me", "give", "show", "get", "find", "please", "thanks",
    }

    # Extract words
    words = re.findall(r"[a-zA-Z]+", query.lower())

    # Filter out stop words and short words
    keywords = [w for w in words if w not in stop_words and len(w) > 2]

    return keywords


def is_counting_query(query: str) -> bool:
    """
    Check if the query is asking for a count.

    Args:
        query: The user's question string.

    Returns:
        True if the query is asking for a count/total.
    """
    query_lower = query.lower()
    counting_signals = ["how many", "count", "total", "number of"]
    return any(signal in query_lower for signal in counting_signals)


def is_listing_query(query: str) -> bool:
    """
    Check if the query is asking for a list.

    Args:
        query: The user's question string.

    Returns:
        True if the query wants a list of items/records.
    """
    query_lower = query.lower()
    listing_signals = ["list all", "which proposals", "which records", "show all"]
    return any(signal in query_lower for signal in listing_signals)


# ======================================================================
# Scope-aware query classification
# QueryClassifier  — pure keyword engine, < 5 ms, no LLM
# PartialAnswerEngine — data-driven handlers, reads from metadata pickle
# QueryClassification — dataclass returned by QueryClassifier.classify()
# ======================================================================

import os
import pickle
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class QueryClassification:
    """Result of classifying a query for scope and answerability."""

    classification: str          # ANSWERABLE | PARTIALLY_ANSWERABLE | OUT_OF_SCOPE | NONSENSICAL
    confidence: float
    out_of_scope_reason: Optional[str] = None
    partial_handler: Optional[str] = None
    available_alternative: Optional[str] = None


class QueryClassifier:
    """
    Rule-based, keyword-driven scope classifier for insurance proposal queries.
    Runs in pure Python with no LLM calls — target < 5 ms per query.

    Classifies into:
        ANSWERABLE           — fully handled by existing pipeline
        PARTIALLY_ANSWERABLE — some data exists; include a partial answer + gap note
        OUT_OF_SCOPE         — data does not exist; explain + suggest alternative
        NONSENSICAL          — grammatically broken or self-contradictory
    """

    AVAILABLE_DATA_DOMAINS = {
        "location": [
            "risk_location", "johor", "kuala lumpur", "penang",
            "selangor", "sabah", "perak", "melaka", "malaysia",
        ],
        "security": [
            "cctv", "alarm", "safe", "strong room", "door access",
            "transit", "guards", "armoured", "gps", "display window",
        ],
        "compliance": [
            "background check", "sop", "standard operating",
            "records", "stock check", "police station",
        ],
        "coverage": [
            "sum assured", "insured value", "stock value",
            "transit value", "sum insured",
        ],
        "claims": ["claim history", "claim record", "past claims"],
        "business": [
            "nature of business", "business type", "industry",
            "business name", "company",
        ],
        "proposal": ["proposal", "quote", "record", "created"],
    }

    OUT_OF_SCOPE_DOMAINS = {
        "premium": [
            "premium", "collected premium", "total premium",
            "premium amount", "broker premium",
        ],
        "claim_amounts": [
            "claim amount", "claim value",
            "rejected claim",
            "claim cause", "peril", "non-disclosure",
        ],
        "policy_lifecycle": [
            "expiring", "expiry", "renewal",
            "approved", "modification", "underwriting",
            "turnaround time", "approval status",
        ],
        "broker": ["broker", "agent", "intermediary"],
        "temporal": [
            "this quarter", "last quarter", "this year", "last year",
            "year-over-year", "yoy", "growth", "historical",
            "increased coverage", "changed", "trend", "over time",
            "compared to last", "previous year", "prior year",
        ],
        "foreign_countries": [
            "philippines", "indonesia", "thailand",
            "singapore", "vietnam", "cambodia", "myanmar",
        ],
        # Umbrella regional terms: soft-block — lets partial handlers through
        "regional_scope": [
            "southeast asia", "asean",
        ],
        "risk_analytics": [
            "risk ranking",
            "risk score", "risk distribution", "risk map",
        ],
    }

    PARTIAL_ANSWER_PATTERNS = [
        # --- Claim / location handlers (TYPE 1, 3, 9) ---
        {
            "triggers": [
                "high-risk zone", "risk zone",
                "claim frequency by", "claim frequency in",
                "locations with claims", "fire-related claim",
                "fire-related claims", "fire claim",
                "which locations reported", "regions with lowest",
                "regions with highest", "claim history by region",
                "claim history by area", "claim history by location",
            ],
            "handler": "claims_by_location",
            "description": "Can show claim history grouped by location",
        },
        {
            "triggers": [
                "claim ratio", "claim rate", "percentage with claims",
                "how many have claims", "claims percentage",
                "claim occurrence", "claim frequency",
            ],
            "handler": "claim_rate",
            "description": "Can show claim occurrence rate across proposals",
        },
        # --- Ranking / threshold handlers (TYPE 2, 8) ---
        {
            "triggers": [
                "rank", "ranked", "top proposals",
                "highest insured", "highest value", "highest sum",
                "most insured", "largest sum", "sort by value",
                "order by sum", "by sum assured", "largest policy",
                "highest sum assured", "ranked by value",
            ],
            "handler": "rank_by_sum_assured",
            "description": "Can rank proposals by insured values",
        },
        {
            "triggers": [
                "above", "more than", "greater than", "over",
                "exceed", "higher than",
            ],
            "value_fields": ["sum_assured", "value", "amount"],
            "handler": "filter_by_threshold",
            "description": "Can filter proposals by numeric threshold",
        },
        # --- Industry / business distribution handlers (TYPE 6, 7) ---
        {
            "triggers": [
                "by industry", "by business type", "top industries",
                "industry distribution", "business distribution",
                "industries by", "sector by value",
                "industry total",
            ],
            "handler": "group_by_industry",
            "description": "Can group proposals by nature_of_business_label",
        },
        {
            "triggers": [
                "distribution of policy", "policy type",
                "types of policy", "types of policies",
                "policy breakdown", "business type distribution",
                "industry breakdown", "what types of businesses",
                "policy distribution",
            ],
            "handler": "business_type_distribution",
            "description": "Can show distribution by nature_of_business_label",
        },
        # --- Security handler ---
        {
            "triggers": [
                "anti-theft", "security features", "security devices",
                "protective measures",
            ],
            "handler": "security_feature_summary",
            "description": "Can show security features per proposal",
        },
        # --- GPS tracker handler (TYPE 5) ---
        {
            "triggers": [
                "gps tracker", "gps installed", "use gps", "have gps",
                "included gps", "gps in transit", "gps trackers",
            ],
            "handler": "gps_tracker_proposals",
            "description": "Can show which proposals have GPS trackers",
        },
        # --- Company listing handler (TYPE 4) ---
        {
            "triggers": [
                "active policies", "list all companies",
                "list companies", "all businesses",
                "how many companies", "all proposals",
                "list all businesses",
            ],
            "handler": "list_all_businesses",
            "description": "Can list all businesses with their proposals",
        },
    ]

    _OUT_OF_SCOPE_EXPLANATIONS: dict = {
        "premium": (
            "Premium and financial transaction data is not captured "
            "in proposal records."
        ),
        "claim_amounts": (
            "Claim amounts, causes, and rejection details are not stored. "
            "Only whether a claim occurred within the past 3 years is recorded."
        ),
        "policy_lifecycle": (
            "Policy lifecycle data (expiry, renewal, approval status, "
            "underwriting decisions) is not in the proposal database. "
            "You can ask 'list all businesses' to see all 15 proposals."
        ),
        "broker": (
            "Broker and intermediary data is not captured in proposals."
        ),
        "temporal": (
            "Only the current snapshot of proposal data exists. "
            "Historical comparisons and time-series analysis are not possible."
        ),
        "foreign_countries": (
            "All proposals are from Malaysia only. "
            "No data from other countries exists in this database."
        ),
        "risk_analytics": (
            "Actuarial risk scores and risk-ranking models "
            "are not available. You can ask about claim history by region "
            "or security features per proposal instead."
        ),
        "regional_scope": (
            "Only Malaysian proposals exist in this database. "
            "Multi-country Southeast Asia / ASEAN data is not available, "
            "but partial answers based on Malaysian data can be provided."
        ),
    }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def classify(self, query: str) -> QueryClassification:
        """
        Classify *query* into ANSWERABLE, PARTIALLY_ANSWERABLE,
        OUT_OF_SCOPE, or NONSENSICAL.  Pure keyword matching — no LLM.
        """
        q = query.lower()

        # 1 — detect nonsensical / self-contradictory queries
        if self._is_nonsensical(q):
            return QueryClassification(
                classification="NONSENSICAL",
                out_of_scope_reason="Query appears malformed or self-contradictory",
                available_alternative=self._suggest_alternative(q),
                confidence=0.8,
            )

        # 2 — collect triggered out-of-scope domains (one per domain)
        triggered_domains: List[str] = []
        for domain, keywords in self.OUT_OF_SCOPE_DOMAINS.items():
            for keyword in keywords:
                if keyword in q:
                    triggered_domains.append(domain)
                    break

        # 3 — check for a partial-answer handler (first match wins).
        #     Use word-boundary matching for short (<8 char, single-word) triggers
        #     to prevent false hits like "over" inside "coverage".
        partial_handler: Optional[str] = None
        for pattern in self.PARTIAL_ANSWER_PATTERNS:
            for trigger in pattern["triggers"]:
                if self._trigger_matches(trigger, q):
                    partial_handler = pattern["handler"]
                    break
            if partial_handler:
                break

        # 4 — determine final classification
        #
        # Rule A: certain OOS domains ALWAYS produce OUT_OF_SCOPE regardless
        #         of whether a partial handler also matched.
        #   • foreign_countries  — data from another country entirely
        #   • temporal           — time-based comparisons (yoy, trends, etc.)
        #                          We have no historical data for these; showing
        #                          a current ranking would be misleading.
        _always_oos = {"foreign_countries", "temporal"}
        if _always_oos & set(triggered_domains):
            return QueryClassification(
                classification="OUT_OF_SCOPE",
                out_of_scope_reason=self._explain_scope(triggered_domains, q),
                available_alternative=self._suggest_alternative(q),
                confidence=0.95,
            )

        # Rule B: other OOS domain + partial handler → PARTIALLY_ANSWERABLE
        if triggered_domains and partial_handler:
            return QueryClassification(
                classification="PARTIALLY_ANSWERABLE",
                out_of_scope_reason=(
                    f"Data not available: {', '.join(triggered_domains)}"
                ),
                partial_handler=partial_handler,
                available_alternative=self._suggest_alternative(q),
                confidence=0.85,
            )

        # Rule C: OOS domain with no handler → OUT_OF_SCOPE
        if triggered_domains:
            return QueryClassification(
                classification="OUT_OF_SCOPE",
                out_of_scope_reason=self._explain_scope(triggered_domains, q),
                available_alternative=self._suggest_alternative(q),
                confidence=0.9,
            )

        # Rule D: partial handler with NO OOS domain → PARTIALLY_ANSWERABLE
        #         (the query is answerable with special handling, e.g. ranking
        #         by sum_assured, grouping by industry)
        if partial_handler:
            return QueryClassification(
                classification="PARTIALLY_ANSWERABLE",
                partial_handler=partial_handler,
                available_alternative=self._suggest_alternative(q),
                confidence=0.85,
            )

        return QueryClassification(
            classification="ANSWERABLE",
            confidence=0.9,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _trigger_matches(trigger: str, q: str) -> bool:
        """
        Check whether *trigger* appears in lowercased query *q*.

        Multi-word / long (>=8 char) triggers use plain substring matching.
        Short single-word triggers use a word-boundary check to prevent
        false positives like "over" matching inside "coverage".
        """
        if len(trigger) >= 8 or " " in trigger:
            return trigger in q
        return bool(re.search(r"\b" + re.escape(trigger) + r"\b", q))

    def _is_nonsensical(self, q: str) -> bool:
        """
        Detect genuinely self-contradictory or unresolvable queries.

        Criteria (all other unusual queries are OUT_OF_SCOPE instead):
        1. Two DIFFERENT country names in the same query
           (e.g. "proposals from Malaysia in Philippines")
        2. Query is too short to be meaningful (fewer than 2 meaningful words)

        NOTE: year-over-year / growth / trend are NOT nonsensical.
        They are valid business questions about data that doesn't exist.
        → Those belong in OUT_OF_SCOPE_DOMAINS["temporal"].
        """
        # Rule 1 — contradictory country pair
        countries = [
            "malaysia", "philippines", "indonesia",
            "singapore", "thailand", "vietnam",
            "cambodia", "myanmar",
        ]
        found = [c for c in countries if c in q]
        if len(found) >= 2 and len(set(found)) >= 2:
            return True

        # Rule 2 — too short to interpret
        meaningful_words = [w for w in q.split() if len(w) > 2]
        if len(meaningful_words) < 2:
            return True

        return False

    def _explain_scope(self, triggered_domains: List[str], _q: str) -> str:
        domain = triggered_domains[0] if triggered_domains else "unknown"
        return self._OUT_OF_SCOPE_EXPLANATIONS.get(
            domain,
            "This type of data is not captured in the proposal database.",
        )

    def _suggest_alternative(self, q: str) -> str:  # noqa: C901
        if "claim" in q:
            return (
                "You can ask: 'What is the claim rate across all proposals?', "
                "'Show claim history by region', or 'What is the claim history "
                "of [business name]?'"
            )
        if "premium" in q:
            return (
                "You can ask about insured values: 'Which proposals have the "
                "highest sum assured?' or 'What is the stock value insured for "
                "[business name]?'"
            )
        if "country" in q or any(
            c in q for c in ["philippines", "indonesia", "singapore", "thailand"]
        ):
            return (
                "All proposals are from Malaysia. You can ask: 'Which businesses "
                "are located in [Malaysian city]?' or 'How many proposals are in "
                "Kuala Lumpur?'"
            )
        if "industry" in q or "sector" in q:
            return (
                "You can ask: 'What type of business does [name] run?' or "
                "'Which businesses are jewellers?'"
            )
        if "expir" in q or "renew" in q:
            return (
                "Expiry dates are not tracked. You can ask: 'What is the "
                "proposal creation date for [business name]?'"
            )
        if "broker" in q:
            return (
                "Broker data is not available. You can ask about business "
                "contacts: 'What is the contact number for [business name]?'"
            )
        if "risk zone" in q or "high risk" in q:
            return (
                "Risk zone analytics are not available. You can ask: 'Which "
                "proposals have had claims?' or 'Which businesses have no alarm "
                "system?'"
            )
        if "gps" in q:
            return (
                "You can ask: 'Which businesses use GPS trackers in transit?' "
                "or 'Does [business name] have GPS trackers?'"
            )
        if "southeast asia" in q or "asean" in q:
            return (
                "Only Malaysian proposals exist. You can ask about the "
                "nature_of_business distribution across all 15 proposals."
            )
        return (
            "You can ask about specific proposals, security features, "
            "locations, or business details available in the 15 proposal records."
        )


# ----------------------------------------------------------------------

class PartialAnswerEngine:
    """
    Executes data-driven partial answers for PARTIALLY_ANSWERABLE queries.
    All handlers read from the loaded metadata at runtime — nothing is
    hardcoded (no business names, quote IDs, or values).
    """

    def __init__(self, metadata_path: str = "index/metadata.pkl") -> None:
        self._path = metadata_path
        self._metadata: Optional[List[dict]] = None

    # ------------------------------------------------------------------
    # Lazy metadata access
    # ------------------------------------------------------------------

    @property
    def metadata(self) -> List[dict]:
        if self._metadata is None:
            if os.path.exists(self._path):
                with open(self._path, "rb") as f:
                    self._metadata = pickle.load(f)
            else:
                self._metadata = []
        return self._metadata

    # ------------------------------------------------------------------
    # Shared helper: quote_id → business_name map
    # ------------------------------------------------------------------

    @staticmethod
    def _build_business_name_map(metadata: list) -> dict:
        """
        Scan all business_profile section chunks and return a mapping of
        quote_id → human-readable business name.

        Must be called at the start of every partial-answer handler so that
        the correct business name is shown regardless of which section chunk
        is currently being iterated.
        """
        name_map: dict = {}
        for chunk in metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            fields = chunk.get("fields") or {}
            name = (
                df.get("business_name_label")
                or fields.get("business_name_label")
                or chunk.get("user_name")
                or qid
            )
            # Reject sentinel values
            if name and str(name).strip().lower() not in ("", "none", "nan", "unknown"):
                name_map[qid] = str(name).strip()
            else:
                name_map[qid] = qid
        return name_map

    # ------------------------------------------------------------------
    # Dispatcher
    # ------------------------------------------------------------------

    def dispatch(self, handler: str, query: str) -> str:
        """Route to the correct partial handler by name."""
        _map = {
            "rank_by_sum_assured":        lambda: self.handle_rank_by_sum_assured(),
            "filter_by_threshold":        lambda: self.handle_filter_by_threshold(query),
            "group_by_industry":          lambda: self.handle_group_by_industry(),
            "security_feature_summary":   lambda: self.handle_security_feature_summary(),
            "business_type_distribution": lambda: self.handle_business_type_distribution(),
            "claims_by_location":         lambda: self.handle_claims_by_location(),
            "claim_rate":                 lambda: self.handle_claim_rate(),
            "list_all_businesses":        lambda: self.handle_list_all_businesses(),
            "gps_tracker_proposals":      lambda: self.handle_gps_tracker_proposals(),
        }
        fn = _map.get(handler)
        return fn() if fn else "Partial data handler not available."

    # ------------------------------------------------------------------
    # Shared helper: extract primary insured value from sum_assured fields
    # ------------------------------------------------------------------

    _EMPTY_VALUES = {None, "", "None", -1, "-1", 0, "0", "nan", "N/A", "n/a"}

    @classmethod
    def _safe_float(cls, raw) -> float:
        """Convert raw field value to float. Returns 0 on failure."""
        if raw in cls._EMPTY_VALUES:
            return 0.0
        try:
            return float(str(raw).replace(",", ""))
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _get_primary_value(cls, fields: dict) -> tuple:
        """
        Extract the primary insured value from a sum_assured fields dict.

        Different business types use different value fields:
        - Jewellers (nature 1, 3): maximum_stock_in_premises_label
        - Money changers (nature 2): maximum_stock_foreign_currency_in_premise_label
        - Pawnbrokers (nature 5): value_of_pledged_stock_in_premise_label +
          value_of_cash_in_premise_label

        Returns:
            (value_float, label_string) or (0.0, None) if no data.
        """
        # 1. Jewellers: stock in premises
        v = cls._safe_float(fields.get("maximum_stock_in_premises_label"))
        if v > 0:
            return (v, "jewellery stock")

        # 2. Money changers: foreign currency stock
        v = cls._safe_float(
            fields.get("maximum_stock_foreign_currency_in_premise_label")
        )
        if v > 0:
            return (v, "foreign currency stock")

        # 3. Pawnbrokers: pledged stock + cash in premises
        pledged = cls._safe_float(
            fields.get("value_of_pledged_stock_in_premise_label")
        )
        cash = cls._safe_float(
            fields.get("value_of_cash_in_premise_label")
        )
        total = pledged + cash
        if total > 0:
            return (total, "pledged stock + cash")

        return (0.0, None)

    # ------------------------------------------------------------------
    # Shared helper: extract state from risk_location string
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_state(risk_location: str) -> str:
        """
        Extract the Malaysian state from a risk_location string.

        Format is typically: "City, State, Malaysia" or
        "Street, Area, City, State, Malaysia".
        Returns the second-to-last non-"Malaysia" comma-separated part,
        stripped and title-cased.
        """
        if not risk_location:
            return "Unknown"
        parts = [p.strip() for p in risk_location.split(",") if p.strip()]
        # Remove trailing "Malaysia" if present
        if parts and parts[-1].lower() == "malaysia":
            parts = parts[:-1]
        if not parts:
            return "Unknown"
        # The last remaining part is the state
        state = parts[-1].strip()
        # Special case: "Kuala Lumpur" is both city and state (federal territory)
        return state if state else "Unknown"

    # ------------------------------------------------------------------
    # Handler: rank proposals by total insured value (multi-field)
    # ------------------------------------------------------------------

    def handle_rank_by_sum_assured(self, top_n: int = 15) -> str:
        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)

        ranked = []
        seen: set = set()
        for chunk in metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            seen.add(qid)

            fields = chunk.get("fields") or {}
            value, label = self._get_primary_value(fields)
            if value <= 0:
                continue

            business_name = name_map.get(qid, qid)
            ranked.append((business_name, qid, value, label))

        total_proposals = len(
            {c.get("quote_id") for c in metadata if c.get("quote_id")}
        )

        if not ranked:
            return (
                "No insured value data found across proposals. "
                "You can ask about a specific business's details."
            )

        ranked.sort(key=lambda x: x[2], reverse=True)
        lines = ["Proposals ranked by total insured value (highest first):"]
        for i, (name, qid, value, label) in enumerate(ranked[:top_n], 1):
            lines.append(
                f"{i}. {name} ({qid}): RM {value:,.0f} ({label})"
            )

        missing = total_proposals - len(ranked)
        if missing > 0:
            lines.append(
                f"\nNote: {missing} proposal(s) did not submit "
                f"insured value data."
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: filter proposals by a numeric threshold (multi-field)
    # ------------------------------------------------------------------

    def handle_filter_by_threshold(self, query: str) -> str:
        # Parse threshold — handle $5M, RM 5M, 5 million, 5,000,000
        _patterns = [
            (r"\$(\d+(?:\.\d+)?)\s*[Mm]", True),
            (r"RM\s*(\d+(?:\.\d+)?)\s*[Mm]", True),
            (r"(\d+(?:\.\d+)?)\s*million", True),
            (r"(\d[\d,]+)", False),
        ]
        threshold: Optional[float] = None
        for pat, always_million in _patterns:
            m = re.search(pat, query, re.IGNORECASE)
            if m:
                val = m.group(1).replace(",", "")
                threshold = float(val)
                raw_hit = m.group(0).lower()
                if always_million or "million" in raw_hit or (
                    raw_hit[-1] == "m" and not raw_hit.endswith("rm")
                ):
                    threshold *= 1_000_000
                break

        if threshold is None:
            return (
                "Could not determine the threshold value from your query. "
                "Please specify a numeric value (e.g. 'above RM 5 million')."
            )

        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)

        results = []
        seen: set = set()
        for chunk in metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue

            fields = chunk.get("fields") or {}
            value, label = self._get_primary_value(fields)

            if value > threshold:
                business_name = name_map.get(qid, qid)
                results.append((business_name, qid, value, label))
                seen.add(qid)

        if not results:
            return (
                f"No proposals found with insured value "
                f"above RM {threshold:,.0f}."
            )

        results.sort(key=lambda x: x[2], reverse=True)
        lines = [f"Proposals with insured value above RM {threshold:,.0f}:"]
        for name, qid, val, label in results:
            lines.append(f"- {name} ({qid}): RM {val:,.0f} ({label})")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: group proposals by industry (Bug 4 fix — two-map approach)
    # ------------------------------------------------------------------

    def handle_group_by_industry(self) -> str:
        metadata = self.metadata

        # Step 1: Build industry + name maps from business_profile.
        name_map: dict = {}
        industry_map: dict = {}
        for chunk in metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            fields = chunk.get("fields") or {}

            name = (
                df.get("business_name_label")
                or fields.get("business_name_label")
                or chunk.get("user_name")
                or qid
            )
            name_map[qid] = (
                str(name).strip()
                if name and str(name).strip().lower() not in ("", "none", "unknown")
                else qid
            )

            industry = df.get("nature_of_business_label")
            if not industry or str(industry).strip().lower() in ("", "none", "unknown"):
                industry = "Other / Not Specified"
            industry_map[qid] = str(industry).strip()

        # Step 2: Build value map from sum_assured using multi-field logic.
        value_map: dict = {}
        for chunk in metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            fields = chunk.get("fields") or {}
            val, _label = self._get_primary_value(fields)
            if val > 0:
                value_map[qid] = val

        if not industry_map:
            return "Industry data not available."

        # Step 3: Group by industry using both maps.
        industry_data: dict = defaultdict(
            lambda: {"count": 0, "total_value": 0.0, "has_value_count": 0}
        )
        for qid in industry_map:
            ind = industry_map[qid]
            stock = value_map.get(qid, 0.0)
            industry_data[ind]["count"] += 1
            industry_data[ind]["total_value"] += stock
            if stock > 0:
                industry_data[ind]["has_value_count"] += 1

        # Step 4: Sort by total value descending, then count.
        sorted_industries = sorted(
            industry_data.items(),
            key=lambda x: (x[1]["total_value"], x[1]["count"]),
            reverse=True,
        )

        lines = ["Industries represented in proposal database:"]
        for industry, data in sorted_industries:
            count = data["count"]
            total = data["total_value"]
            has_val = data["has_value_count"]
            if total > 0:
                value_str = (
                    f"Total insured: RM {total:,.0f} "
                    f"({has_val}/{count} proposals have value data)"
                )
            else:
                value_str = "Insured values not submitted in proposals"
            lines.append(f"- {industry}: {count} proposal(s) \u2014 {value_str}")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: security feature summary (anti-theft handler fix)
    # ------------------------------------------------------------------

    # Maps section name → the primary Yes/No field to check in that section.
    # alarm / cctv / armoured vehicle / strong room are the four key
    # anti-theft indicators that each live in their own section chunk.
    _SECURITY_SECTION_FIELDS: dict = {
        "alarm":              "do_you_have_alarm_label",
        "cctv":               "recording_label",
        "transit_and_gaurds": "do_you_use_armoured_vehicle_label",
        "strong_room":        "do_you_have_a_strong_room_label",
    }

    # Human-readable labels for each section
    _SECURITY_SECTION_LABELS: dict = {
        "alarm":              "Alarm",
        "cctv":               "CCTV",
        "transit_and_gaurds": "Armoured Vehicle",
        "strong_room":        "Strong Room",
    }

    _YES_VALUES = {"yes", "001", "true", "1"}

    def handle_security_feature_summary(self) -> str:
        metadata = self.metadata
        # Bug 1 fix: build name map from business_profile chunks
        name_map = self._build_business_name_map(metadata)

        # Build security feature map: quote_id → {section: bool}
        security_map: dict = defaultdict(dict)
        for chunk in metadata:
            section = chunk.get("section", "")
            if section not in self._SECURITY_SECTION_FIELDS:
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            field = self._SECURITY_SECTION_FIELDS[section]
            value = df.get(field)
            if value is not None:
                security_map[qid][section] = (
                    str(value).strip().lower() in self._YES_VALUES
                )

        all_quotes = sorted(name_map.keys())
        if not all_quotes:
            return "No proposal data found."

        lines = [
            "Security features per proposal "
            "(Alarm, CCTV, Armoured Vehicle, Strong Room):"
        ]
        for qid in all_quotes:
            name = name_map.get(qid, qid)
            features = security_map.get(qid, {})
            active_parts: List[str] = []
            for sec, label in self._SECURITY_SECTION_LABELS.items():
                if features.get(sec):
                    active_parts.append(f"{label}: Yes")
            if active_parts:
                lines.append(f"- {name} ({qid}): {', '.join(active_parts)}")
            else:
                lines.append(
                    f"- {name} ({qid}): No security feature data submitted"
                )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: distribution of business types (two-map fix for correct counts)
    # ------------------------------------------------------------------

    def handle_business_type_distribution(self) -> str:
        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)
        total = len(name_map)

        # Build type → list of business names
        type_businesses: dict = defaultdict(list)
        seen: set = set()
        for chunk in metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            seen.add(qid)
            df = chunk.get("decoded_fields") or {}
            btype = df.get("nature_of_business_label")
            if not btype or str(btype).strip().lower() in ("", "none", "unknown"):
                btype = "Not Specified"
            type_businesses[str(btype).strip()].append(name_map.get(qid, qid))

        if not type_businesses:
            return "Business type data not available."

        sorted_types = sorted(
            type_businesses.items(), key=lambda x: len(x[1]), reverse=True
        )
        lines = [
            f"Policy type distribution across {total} proposals in Malaysia:"
        ]
        for btype, businesses in sorted_types:
            count = len(businesses)
            pct = round(count / total * 100) if total > 0 else 0
            biz_str = ", ".join(businesses)
            label = "Business" if count == 1 else "Businesses"
            lines.append(
                f"\n- {btype}: {count} proposal(s) ({pct}%)"
            )
            lines.append(f"  {label}: {biz_str}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: claim history grouped by location (TYPE 1 + 9)
    # ------------------------------------------------------------------

    def handle_claims_by_location(self) -> str:
        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)

        # Step 1: Build location map from any chunk (risk_location is on all)
        location_map: dict = {}  # quote_id → state
        seen_loc: set = set()
        for chunk in metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen_loc:
                continue
            seen_loc.add(qid)
            loc = chunk.get("risk_location", "")
            location_map[qid] = self._extract_state(str(loc))

        # Step 2: Build claim map from claim_history section
        claim_map: dict = {}  # quote_id → decoded claim label
        for chunk in metadata:
            if chunk.get("section") != "claim_history":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            claim_map[qid] = df.get("claim_history_label", "")

        # Step 3: Group by state
        state_data: dict = defaultdict(
            lambda: {
                "proposals": 0, "with_claims": 0,
                "no_claims": 0, "no_data": 0,
                "businesses": [],
            }
        )
        for qid in location_map:
            state = location_map[qid]
            claim_label = claim_map.get(qid, "")
            state_data[state]["proposals"] += 1
            state_data[state]["businesses"].append(name_map.get(qid, qid))
            claim_lower = claim_label.lower() if claim_label else ""
            if "no claim" in claim_lower:
                state_data[state]["no_claims"] += 1
            elif "claim" in claim_lower:
                state_data[state]["with_claims"] += 1
            else:
                state_data[state]["no_data"] += 1

        sorted_states = sorted(
            state_data.items(),
            key=lambda x: (x[1]["with_claims"], x[1]["proposals"]),
            reverse=True,
        )

        total_with_data = sum(
            d["with_claims"] + d["no_claims"] for d in state_data.values()
        )
        total_claims = sum(d["with_claims"] for d in state_data.values())
        total_no_data = sum(d["no_data"] for d in state_data.values())

        lines = ["Claim history by region across all proposals:"]
        lines.append("")
        header = f"{'State':<25} {'Proposals':>10} {'With Claims':>13} {'No Claims':>12}"
        lines.append(header)
        lines.append("-" * len(header))
        for state, data in sorted_states:
            lines.append(
                f"{state:<25} {data['proposals']:>10} "
                f"{data['with_claims']:>13} {data['no_claims']:>12}"
            )
        lines.append("")

        if total_claims == 0:
            lines.append(
                f"Note: All {total_with_data} proposals with available data "
                f"show no claims in the past 3 years."
            )
        else:
            lines.append(
                f"Summary: {total_claims} proposal(s) with claims out of "
                f"{total_with_data} with claim data."
            )
        if total_no_data > 0:
            lines.append(
                f"{total_no_data} proposal(s) have no claim data submitted."
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: claim occurrence rate / ratio (TYPE 3)
    # ------------------------------------------------------------------

    def handle_claim_rate(self) -> str:
        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)

        has_claims: list = []
        no_claims: list = []
        no_data: list = []

        for chunk in metadata:
            if chunk.get("section") != "claim_history":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            label = (df.get("claim_history_label") or "").lower()
            name = name_map.get(qid, qid)

            if "no claim" in label:
                no_claims.append(name)
            elif "claim" in label:
                has_claims.append(name)
            else:
                no_data.append(name)

        # Also find proposals with NO claim_history chunk at all
        seen_in_claims = {
            c.get("quote_id") for c in metadata
            if c.get("section") == "claim_history"
        }
        all_qids = {
            c.get("quote_id") for c in metadata if c.get("quote_id")
        }
        for qid in all_qids - seen_in_claims:
            no_data.append(name_map.get(qid, qid))

        total_with_data = len(has_claims) + len(no_claims)
        total_all = total_with_data + len(no_data)

        lines = ["Claim occurrence rate across all proposals:"]

        if total_with_data > 0:
            no_pct = round(len(no_claims) / total_with_data * 100)
            has_pct = round(len(has_claims) / total_with_data * 100)
        else:
            no_pct = has_pct = 0

        lines.append(
            f"- No claims in past 3 years: {len(no_claims)} proposals "
            f"({no_pct}% of proposals with data)"
        )
        lines.append(
            f"- Claims within past 3 years: {len(has_claims)} proposals "
            f"({has_pct}%)"
        )
        if no_data:
            no_data_str = ", ".join(no_data)
            lines.append(
                f"- No claim data submitted: {len(no_data)} proposals "
                f"({no_data_str})"
            )

        lines.append("")
        lines.append(
            "Note: Claim amounts and causes are not captured in proposal "
            "records. Only claim occurrence within 3 years is recorded."
        )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: list all businesses / proposals (TYPE 4)
    # ------------------------------------------------------------------

    def handle_list_all_businesses(self) -> str:
        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)

        # Build location map
        location_map: dict = {}
        industry_map: dict = {}
        seen: set = set()
        for chunk in metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            if chunk.get("section") == "business_profile":
                seen.add(qid)
                loc = chunk.get("risk_location", "")
                location_map[qid] = str(loc).strip() if loc else "Not specified"
                df = chunk.get("decoded_fields") or {}
                ind = df.get("nature_of_business_label")
                if ind and str(ind).strip().lower() not in ("", "none", "unknown"):
                    industry_map[qid] = str(ind).strip()
                else:
                    industry_map[qid] = "Not Specified"

        total = len(name_map)
        if total == 0:
            return "No proposal data found."

        lines = [
            f"All {total} businesses in the proposal database:"
        ]
        for i, qid in enumerate(sorted(name_map.keys()), 1):
            name = name_map[qid]
            loc = location_map.get(qid, "")
            ind = industry_map.get(qid, "")
            lines.append(
                f"{i:>2}. {name} ({qid}) — {ind}, {loc}"
            )
        lines.append("")
        lines.append(
            "Each business has exactly one proposal record in the database."
        )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: GPS tracker proposals (TYPE 5)
    # ------------------------------------------------------------------

    def handle_gps_tracker_proposals(self) -> str:
        metadata = self.metadata
        name_map = self._build_business_name_map(metadata)

        # Build location map for display
        loc_map: dict = {}
        seen_loc: set = set()
        for chunk in metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen_loc:
                continue
            seen_loc.add(qid)
            loc = chunk.get("risk_location", "")
            parts = [p.strip() for p in str(loc).split(",") if p.strip()]
            # Take the city (first meaningful part)
            loc_map[qid] = parts[0] if parts else "Unknown"

        # Scan transit_and_gaurds section for GPS fields
        gps_vehicles_yes: list = []
        gps_vehicles_no: list = []
        gps_no_data: list = []

        seen_transit = set()
        all_qids = set(name_map.keys())

        for chunk in metadata:
            if chunk.get("section") != "transit_and_gaurds":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in seen_transit:
                continue
            seen_transit.add(qid)

            df = chunk.get("decoded_fields") or {}
            gps_v = df.get(
                "installed_gps_tracker_in_transit_vehicles_label", ""
            )
            name = name_map.get(qid, qid)
            city = loc_map.get(qid, "")

            if str(gps_v).strip().lower() in ("yes", "001", "true", "1"):
                gps_vehicles_yes.append((name, qid, city))
            elif str(gps_v).strip().lower() in ("no", "002", "false", "0"):
                gps_vehicles_no.append((name, qid, city))
            else:
                gps_no_data.append((name, qid, city))

        # Proposals without transit_and_gaurds chunk at all
        for qid in all_qids - seen_transit:
            name = name_map.get(qid, qid)
            city = loc_map.get(qid, "")
            gps_no_data.append((name, qid, city))

        lines = []
        if gps_vehicles_yes:
            lines.append(
                f"Proposals with GPS trackers in transit vehicles "
                f"({len(gps_vehicles_yes)}):"
            )
            for i, (name, qid, city) in enumerate(
                sorted(gps_vehicles_yes, key=lambda x: x[1]), 1
            ):
                lines.append(f"{i:>2}. {name} ({qid}) \u2014 {city}")
        else:
            lines.append("No proposals have GPS trackers in transit vehicles.")

        if gps_vehicles_no:
            lines.append("")
            lines.append(
                f"Proposals WITHOUT GPS in vehicles ({len(gps_vehicles_no)}):"
            )
            for name, qid, city in sorted(
                gps_vehicles_no, key=lambda x: x[1]
            ):
                lines.append(f"- {name} ({qid}) \u2014 {city}")

        if gps_no_data:
            lines.append("")
            no_data_ids = ", ".join(qid for _, qid, _ in gps_no_data)
            lines.append(
                f"No transit data submitted: {no_data_ids}"
            )

        return "\n".join(lines)
