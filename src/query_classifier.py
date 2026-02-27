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
            "claim amount", "claim value", "claim ratio",
            "claim frequency", "rejected claim", "fire claim",
            "fire-related", "claim cause", "peril", "non-disclosure",
        ],
        "policy_lifecycle": [
            "expiring", "expiry", "renewal", "active policy",
            "active policies", "approved", "modification", "underwriting",
            "turnaround time", "approval status",
        ],
        "broker": ["broker", "agent", "intermediary"],
        "temporal": [
            "this quarter", "last quarter", "this year", "last year",
            "year-over-year", "yoy", "growth", "historical",
            "increased coverage", "changed",
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
            "high-risk zone", "risk zone", "risk ranking",
            "risk score", "risk distribution", "risk map",
        ],
        "regional_analytics": [
            "lowest frequency", "highest frequency",
            "regional distribution", "by region analysis",
        ],
    }

    PARTIAL_ANSWER_PATTERNS = [
        {
            "triggers": [
                "highest insured", "highest value", "highest sum",
                "most insured", "largest sum",
            ],
            "handler": "rank_by_sum_assured",
            "description": "Can rank proposals by sum_assured values",
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
        {
            "triggers": [
                "by industry", "by business type", "top industries",
                "industry distribution", "business distribution",
            ],
            "handler": "group_by_industry",
            "description": "Can group proposals by nature_of_business_label",
        },
        {
            "triggers": [
                "anti-theft", "security features", "security devices",
                "protective measures",
            ],
            "handler": "security_feature_summary",
            "description": "Can show security features per proposal",
        },
        {
            "triggers": [
                "policy type", "types of policy", "policy distribution",
            ],
            "handler": "business_type_distribution",
            "description": "Can show distribution by nature_of_business_label",
        },
    ]

    _OUT_OF_SCOPE_EXPLANATIONS: dict = {
        "premium": (
            "Premium and financial transaction data is not captured "
            "in proposal records."
        ),
        "claim_amounts": (
            "Claim amounts, ratios, and causes are not stored. "
            "Only whether a claim occurred within the past 3 years is recorded."
        ),
        "policy_lifecycle": (
            "Policy lifecycle data (expiry, renewal, approval status, "
            "underwriting decisions) is not in the proposal database."
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
            "Actuarial risk scores, risk zones, and frequency analytics "
            "are not available. Only per-proposal security features exist."
        ),
        "regional_analytics": (
            "Regional frequency distributions are not available. "
            "Location data exists per proposal but without claim frequency."
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
        # Rule A: foreign-country references ALWAYS produce OUT_OF_SCOPE —
        #         even if a partial handler matched, the user is asking about
        #         a country we have no data for.
        if "foreign_countries" in triggered_domains:
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
        """Detect self-contradictory or grammatically broken queries."""
        countries = [
            "malaysia", "philippines", "indonesia",
            "singapore", "thailand", "vietnam",
        ]
        if sum(1 for c in countries if c in q) >= 2:
            return True
        if "year-over-year" in q or "yoy" in q:
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
                "You can ask: 'Which proposals have had claims in the past "
                "3 years?' or 'What is the claim history of [business name]?'"
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
    # Dispatcher
    # ------------------------------------------------------------------

    def dispatch(self, handler: str, query: str) -> str:
        """Route to the correct partial handler by name."""
        _map = {
            "rank_by_sum_assured":       lambda: self.handle_rank_by_sum_assured(),
            "filter_by_threshold":       lambda: self.handle_filter_by_threshold(query),
            "group_by_industry":         lambda: self.handle_group_by_industry(),
            "security_feature_summary":  lambda: self.handle_security_feature_summary(),
            "business_type_distribution": lambda: self.handle_business_type_distribution(),
        }
        fn = _map.get(handler)
        return fn() if fn else "Partial data handler not available."

    # ------------------------------------------------------------------
    # Handler: rank proposals by sum assured
    # ------------------------------------------------------------------

    def handle_rank_by_sum_assured(self, top_n: int = 10) -> str:
        ranked = []
        seen: set = set()
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            raw = chunk.get("fields") or {}
            if not isinstance(raw, dict):
                continue
            stock_value = raw.get("maximum_stock_in_premises_label")
            try:
                stock_value = (
                    float(stock_value)
                    if stock_value not in (None, "", "-1")
                    else 0.0
                )
            except (TypeError, ValueError):
                stock_value = 0.0
            if stock_value <= 0:
                continue
            df = chunk.get("decoded_fields") or {}
            business_name = df.get("business_name_label") or qid
            seen.add(qid)
            ranked.append((business_name, qid, stock_value))

        if not ranked:
            return "Sum assured data not available in current records."

        ranked.sort(key=lambda x: x[2], reverse=True)
        lines = ["Proposals ranked by insured stock value (descending):"]
        for i, (name, qid, value) in enumerate(ranked[:top_n], 1):
            lines.append(f"{i}. {name} ({qid}): RM {value:,.0f}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: filter proposals by a numeric threshold (e.g. above $5M)
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

        results = []
        seen: set = set()
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            raw = chunk.get("fields") or {}
            if not isinstance(raw, dict):
                continue
            stock_val = raw.get("maximum_stock_in_premises_label")
            try:
                stock_val = (
                    float(stock_val)
                    if stock_val not in (None, "", "-1")
                    else 0.0
                )
            except (TypeError, ValueError):
                stock_val = 0.0
            if stock_val > threshold:
                df = chunk.get("decoded_fields") or {}
                business_name = df.get("business_name_label") or qid
                results.append((business_name, qid, stock_val))
                seen.add(qid)

        if not results:
            return (
                f"No proposals found with insured stock value "
                f"above RM {threshold:,.0f}."
            )

        results.sort(key=lambda x: x[2], reverse=True)
        lines = [f"Proposals with insured value above RM {threshold:,.0f}:"]
        for name, qid, val in results:
            lines.append(f"- {name} ({qid}): RM {val:,.0f}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: group proposals by industry with total sum assured
    # ------------------------------------------------------------------

    def handle_group_by_industry(self) -> str:
        industry_data: dict = defaultdict(
            lambda: {"count": 0, "total_value": 0.0}
        )
        seen: set = set()
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            seen.add(qid)
            df = chunk.get("decoded_fields") or {}
            industry = df.get("nature_of_business_label") or "Unknown"
            raw = chunk.get("fields") or {}
            try:
                stock_val = float(
                    raw.get("maximum_stock_in_premises_label") or 0
                )
            except (TypeError, ValueError):
                stock_val = 0.0
            industry_data[industry]["count"] += 1
            industry_data[industry]["total_value"] += stock_val

        if not industry_data:
            return "Industry data not available."

        sorted_industries = sorted(
            industry_data.items(),
            key=lambda x: x[1]["total_value"],
            reverse=True,
        )
        lines = ["Industries by total insured value:"]
        for industry, data in sorted_industries:
            value_str = (
                f"RM {data['total_value']:,.0f}"
                if data["total_value"] > 0
                else "Value not available"
            )
            lines.append(
                f"- {industry}: {data['count']} proposal(s), "
                f"Total: {value_str}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: security feature summary across all proposals
    # ------------------------------------------------------------------

    def handle_security_feature_summary(self) -> str:
        _SECURITY_KEYS = [
            "cctv", "alarm", "safe", "strong_room", "door_access",
            "transit", "guard", "gps", "display_window",
        ]
        rows = []
        seen: set = set()
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            seen.add(qid)
            df = chunk.get("decoded_fields") or {}
            business = df.get("business_name_label") or qid
            active: List[str] = []
            for field_name, value in df.items():
                fn_lower = field_name.lower()
                if any(sk in fn_lower for sk in _SECURITY_KEYS):
                    if str(value).strip().lower() in {"yes", "001", "true", "1"}:
                        label = (
                            field_name.replace("_label", "")
                            .replace("_", " ")
                            .title()
                        )
                        active.append(label)
            rows.append((business, qid, active))

        if not rows:
            return "Security feature data not available."

        lines = ["Security features per proposal:"]
        for business, qid, features in rows:
            feat_str = ", ".join(features) if features else "None documented"
            lines.append(f"- {business} ({qid}): {feat_str}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Handler: distribution of business types
    # ------------------------------------------------------------------

    def handle_business_type_distribution(self) -> str:
        distribution: dict = defaultdict(int)
        seen: set = set()
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            seen.add(qid)
            df = chunk.get("decoded_fields") or {}
            btype = df.get("nature_of_business_label") or "Unknown"
            distribution[btype] += 1

        if not distribution:
            return "Business type data not available."

        sorted_types = sorted(
            distribution.items(), key=lambda x: x[1], reverse=True
        )
        lines = ["Business type distribution across all proposals:"]
        for btype, count in sorted_types:
            lines.append(f"- {btype}: {count} proposal(s)")
        return "\n".join(lines)
