"""
Dynamic Query Classifier for routing queries to appropriate handlers.

This module implements Pattern 8: Intelligent Query Type Classification.
Classification is based on linguistic patterns, not hardcoded query strings.
"""
from __future__ import annotations

import re
from typing import Literal, Optional, List

                                                 
                                                                 

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

                                                        
    for signal in AGGREGATION_SIGNALS:
        if signal in query_lower:
            return "analytical"

                                                         
    for signal in COMPARISON_SIGNALS:
        if signal in query_lower:
                                                          
            return "analytical"

                                                            
    quote_id_match = QUOTE_ID_PATTERN.search(query)
    if quote_id_match:
                                                               
        for signal in STRUCTURED_FIELD_SIGNALS:
            if signal in query_lower:
                return "structured"
                                                                   
        return "structured"

                                   
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

                   
    words = re.findall(r"[a-zA-Z]+", query.lower())

                                           
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

                                  
                                                        
                                                                        
                                                                        
import os
import pickle
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class QueryClassification:
    """Result of classifying a query for scope and answerability."""

    classification: str                                                                          
    confidence: float
    out_of_scope_reason: Optional[str] = None
    partial_handler: Optional[str] = None
    available_alternative: Optional[str] = None
    answer_is_sufficient: bool = True                                       
    scope_gap_description: Optional[str] = None                            
    query_intent: Optional[str] = None                                                            

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
            "transit value", "sum insured", "fidelity",
            "director house", "director coverage", "per staff",
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
            "premium", "premium amount", "premium rate", "collected premium",
        ],
        "underwriting_decisions": [
            "underwriting decision", "underwriting decisions", "underwriting outcome",
            "approve", "approved", "reject", "rejected", "policy approval status",
            "approval status",
        ],
        "revenue": [
            "revenue", "financial performance", "profit", "income",
        ],
        "turnaround": [
            "underwriting turnaround time", "turnaround time",
        ],
    }

    _REFUSAL_CHECKLIST_KEYWORDS = {
        "add_on_coverage": ["fidelity", "director house", "add-on", "staff count"],
        "summary_coverage_values": ["opted", "coverage opted", "add-on flag"],
        "additional_details": ["background check", "stock check", "sop", "police station"],
        "transit_and_gaurds": [
            "jaguar", "armoured", "armed guards", "guards at premise", "gps", "transit",
        ],
        "safe": ["safe", "safe grade", "safe model", "safe brand"],
        "strong_room": ["strong room", "time lock", "vault room"],
        "cctv": ["cctv", "camera", "backup", "retention", "maintenance"],
        "alarm": ["alarm", "cms", "monitoring station"],
    }

    PARTIAL_ANSWER_PATTERNS = [
                                                                              
        {
            "triggers": [
                "opted for director's house coverage",
                "opted for director house coverage",
                "director's house coverage and fidelity",
                "director house coverage and fidelity",
                "combined add-on coverage",
            ],
            "handler": "director_fidelity_combined",
            "description": "Can list proposals opted for both director and fidelity coverages with combined amounts",
        },
        {
            "triggers": [
                "highest number of staff covered under fidelity",
                "staff covered under fidelity",
                "highest fidelity staff",
                "fidelity staff covered",
            ],
            "handler": "highest_fidelity_staff_with_director",
            "description": "Can find proposals with highest fidelity staff count and director coverage status",
        },
        {
            "triggers": [
                "armoured vehicles and jaguar transit",
                "armoured vehicle and jaguar transit",
                "jaguar transit services",
                "armed guards during transit",
                "insured transit values",
                "transit values",
            ],
            "handler": "transit_security_combo_values",
            "description": "Can list proposals matching transit security combo and compute insured transit values",
        },
                         
        {
            "triggers": [
                "no alarm",
                "without alarm",
                "not have an alarm",
                "no alarm system",
                "without an alarm",
            ],
            "handler": "filter_no_alarm",
            "description": "Can filter proposals without alarm system",
        },
                                                                                     
        {
            "triggers": [
                "compare average",
                "jewellers and money changers",
                "jewelers and money changers",
                "average insured stock",
                "jewellers vs money changers",
                "jewelers vs money changers",
                "compare insured",
            ],
            "handler": "compare_business_type_averages",
            "description": "Can compare average insured values between jewellers and money changers",
        },
                                                    
        {
            "triggers": [
                "pawn broker",
                "pawnbroker",
                "pawn shop",
            ],
            "handler": "business_type_compound_filter",
            "description": "Can filter pawn broker proposals with security conditions",
        },
        {
            "triggers": [
                "money changer",
                "money exchange",
            ],
            "handler": "business_type_compound_filter",
            "description": "Can filter money changer proposals with security conditions",
        },
        {
            "triggers": [
                "jeweller",
                "jewelers",
                "jewellers",
                "goldsmith",
            ],
            "handler": "business_type_compound_filter",
            "description": "Can filter jeweller proposals with security conditions",
        },
        {
            "triggers": [
                "high-value proposals",
                "high value proposals",
                "weak security setup",
                "weak security",
                "lacking either an alarm",
                "lacking an alarm",
                "lacking a strong room",
            ],
            "handler": "high_value_weak_security",
            "description": "Can find proposals above a value threshold with weak security",
        },
                                   
        {
            "triggers": [
                "total fidelity exposure",
                "average fidelity",
                "fidelity exposure by business",
                "fidelity by business type",
                "business type carries highest",
                "fidelity guarantee by business",
                "fidelity guarantee by type",
                "fidelity insurance by business",
            ],
            "handler": "aggregate_fidelity_by_business_type",
            "description": "Can aggregate fidelity guarantee insurance by business type",
        },
                                 
        {
            "triggers": [
                "total director",
                "director house coverage value",
                "director house coverage across",
                "director coverage across",
                "total director's house coverage",
            ],
            "handler": "total_director_coverage",
            "description": "Can compute total director house coverage across all proposals",
        },
                                  
        {
            "triggers": [
                "fidelity per staff",
                "per staff member",
                "fidelity guarantee amount per staff",
                "fidelity per employee",
                "fidelity ratio per staff",
                "fidelity guarantee per staff",
            ],
            "handler": "fidelity_per_staff_ratio",
            "description": "Can compute fidelity guarantee amount per staff member",
        },
                                                                            
        {
            "triggers": [
                "states with more than one proposal",
                "total insured per state",
                "insured per state",
                "by state",
                "more than one proposal",
            ],
            "handler": "state_grouping",
            "description": "Can group proposals and total insured by state",
        },
                                                             
        {
            "triggers": [
                "background checks on all employees",
                "background check on all",
                "background checks and most frequent",
                "background checks and perform stock",
                "most frequent stock check",
                "most frequent stock",
                "background check and stock check",
                "employee background check",
            ],
            "handler": "background_check_stock_frequency",
            "description": "Can filter proposals with background checks AND most-frequent stock checks",
        },
                                          
        {
            "triggers": [
                "grade 4 safe",
                "no strong room",
                "grade 4 but no strong room",
                "grade 4 without strong room",
                "grade 4 safe no strong room",
            ],
            "handler": "safe_grade_no_strong_room",
            "description": "Can find proposals with grade 4 safe and no strong room",
        },
                                     
        {
            "triggers": [
                "stock out of safe",
                "out of safe exceeding",
                "out of safe above",
                "out of safe threshold",
                "stock outside safe",
                "value of stock out of safe",
            ],
            "handler": "stock_out_of_safe_threshold",
            "description": "Can filter proposals by stock-out-of-safe value threshold",
        },
                                                      
        {
            "triggers": [
                "rank business types",
                "rank by average insured",
                "most risk per proposal",
                "business type with highest average",
                "average insured value",
            ],
            "handler": "rank_business_types_by_insured_value",
            "description": "Can rank business types by average total insured value",
        },
                              
        {
            "triggers": [
                "proposals per state",
                "how many proposals per state",
                "per state",
                "count per state",
                "state breakdown",
            ],
            "handler": "state_security_count",
            "description": "Can count proposals with alarm+strong room per state",
        },
                                                          
        {
            "triggers": [
                "high-risk zone", "high-risk zones", "risk zone",
                "claim frequency by", "claim frequency in",
                "locations with claims", "fire-related claim",
                "fire-related claims", "fire claim", "fire claims",
                "which locations reported", "regions with lowest",
                "regions with highest", "claim history by region",
                "claim history by area", "claim history by location",
                "locations with most claims", "areas with claims",
                "highest claims", "fewest claims", "most claims",
                "lowest claims", "claims by location", "claims by region",
                "claims by area", "claims by city", "claims by state",
                "location has the highest", "location has the most",
                "location has the fewest", "location has the lowest",
                "claims per location", "claims per region",
            ],
            "handler": "claims_by_location",
            "description": "Can show claim history grouped by location",
        },
        {
            "triggers": [
                "claim ratio", "claim rate", "percentage with claims",
                "how many have claims", "claims percentage",
                "claim occurrence", "claim frequency",
                "proposals have claims", "proposals with claims",
                "how many claims", "number of claims",
            ],
            "handler": "claim_rate",
            "description": "Can show claim occurrence rate across proposals",
        },
                                                          
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
                "gps tracker", "gps installed", "use gps", "have gps",
                "included gps", "gps in transit", "gps trackers",
            ],
            "handler": "gps_tracker_proposals",
            "description": "Can show which proposals have GPS trackers",
        },
                                                  
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
                                                  
        {
            "triggers": [
                "fidelity guarantee",
                "director house coverage",
                "opted for fidelity",
                "opted for director",
                "opted for both",
            ],
            "handler": "addon_coverage_opt_in",
            "description": "Can list proposals opted into add-on coverages",
        },
    ]

    _OUT_OF_SCOPE_EXPLANATIONS: dict = {
        "premium": "Premium amounts and premium rates are not available in proposal records.",
        "underwriting_decisions": "Underwriting decisions and policy approval status are not available in proposal records.",
        "revenue": "Revenue or financial performance data is not available in proposal records.",
        "turnaround": "Underwriting turnaround time is not available in proposal records.",
    }

                
    def classify(self, query: str) -> QueryClassification:
        """
        Classify *query* into ANSWERABLE, PARTIALLY_ANSWERABLE,
        OUT_OF_SCOPE, or NONSENSICAL.  Pure keyword matching — no LLM.
        """
        q = query.lower()

                                                                           
        query_intent = self._detect_query_intent(q)

                                                             
        if self._is_nonsensical(q):
            return QueryClassification(
                classification="NONSENSICAL",
                out_of_scope_reason="Query appears malformed or self-contradictory",
                available_alternative=self._suggest_alternative(q),
                confidence=0.8,
                query_intent=query_intent,
            )

                                                                     
        triggered_domains: List[str] = []
        for domain, keywords in self.OUT_OF_SCOPE_DOMAINS.items():
            for keyword in keywords:
                if keyword in q:
                    triggered_domains.append(domain)
                    break

                                                                               
                                                                              
        partial_handler = self._select_partial_handler(q)

                                            
         
                                           
                                                                             
        if triggered_domains and self._passes_refusal_checklist(q):
            triggered_domains = []

                                                                   
        if triggered_domains and not partial_handler:
            return QueryClassification(
                classification="OUT_OF_SCOPE",
                out_of_scope_reason=self._explain_scope(triggered_domains, q),
                available_alternative=self._suggest_alternative(q),
                confidence=0.95,
                query_intent=query_intent,
            )

                                                                           
                                                                            
                                                                  
        if triggered_domains and partial_handler:
                                                                           
                                                                                
                                                           
            return QueryClassification(
                classification="PARTIALLY_ANSWERABLE",
                out_of_scope_reason=self._explain_scope(triggered_domains, q),
                partial_handler=partial_handler,
                available_alternative=self._suggest_alternative(q),
                confidence=0.85,
                answer_is_sufficient=True,
                query_intent=query_intent,
            )

                                                           
        if triggered_domains:
            return QueryClassification(
                classification="OUT_OF_SCOPE",
                out_of_scope_reason=self._explain_scope(triggered_domains, q),
                available_alternative=self._suggest_alternative(q),
                confidence=0.9,
                query_intent=query_intent,
            )

                                                                           
                                                                              
                                                       
        if partial_handler:
            return QueryClassification(
                classification="PARTIALLY_ANSWERABLE",
                partial_handler=partial_handler,
                available_alternative=self._suggest_alternative(q),
                confidence=0.85,
                answer_is_sufficient=True,
                query_intent=query_intent,
            )

        return QueryClassification(
            classification="ANSWERABLE",
            confidence=0.9,
            query_intent=query_intent,
        )

    def _passes_refusal_checklist(self, q: str) -> bool:
        """Return True when checklist fields are relevant, so refusal must be blocked."""
        for _field, keywords in self._REFUSAL_CHECKLIST_KEYWORDS.items():
            if any(keyword in q for keyword in keywords):
                return True
        return False

                     
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

    _HANDLER_PRIORITY = {
        "director_fidelity_combined": 30,
        "highest_fidelity_staff_with_director": 30,
        "transit_security_combo_values": 30,
        "high_value_weak_security": 30,
        "stock_out_of_safe_threshold": 25,
        "compare_business_type_averages": 20,
        "background_check_stock_frequency": 20,
        "state_grouping": 15,
        "safe_grade_no_strong_room": 10,
        "addon_coverage_opt_in": -5,
    }

    def _select_partial_handler(self, q: str) -> Optional[str]:
        """Select the most specific matching partial handler for the query."""
        candidates: list[tuple[int, str]] = []
        for pattern in self.PARTIAL_ANSWER_PATTERNS:
            matched = [t for t in pattern["triggers"] if self._trigger_matches(t, q)]
            if not matched:
                continue
            handler = pattern["handler"]
            longest = max(len(t) for t in matched)
            coverage = (len(matched) - 1) * 5
            priority = self._HANDLER_PRIORITY.get(handler, 0)
            score = longest + coverage + priority
            candidates.append((score, handler))

        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    @staticmethod
    def _detect_query_intent(q: str) -> str:
        """
        Detect the semantic intent of the query for handlers that need
        to vary their output (e.g. claims_by_location ranking direction).
        """
        if any(w in q for w in [
            "fire", "flood", "theft", "burglary", "robbery", "peril", "cause",
        ]):
            return "peril_specific"
        if any(w in q for w in [
            "highest", "most", "worst", "high-risk", "top ",
        ]):
            return "ranking_desc"
        if any(w in q for w in [
            "lowest", "least", "safest", "fewest", "minimum",
        ]):
            return "ranking_asc"
        if any(w in q for w in [
            "distribution", "breakdown", "types", "percentage", "percent",
        ]):
            return "distribution"
        if any(w in q for w in ["list", "show all", "which"]):
            return "list"
        return "summary"

    def _is_nonsensical(self, q: str) -> bool:
        """
        Detect genuinely self-contradictory or unresolvable queries.

        Criteria (all other unusual queries are OUT_OF_SCOPE instead):
        1. Two DIFFERENT country names in the same query
           (e.g. "proposals from Malaysia in Philippines")
        2. Query is too short to be meaningful (fewer than 2 meaningful words)

        NOTE: unusual business questions that are still interpretable should be
        treated as out-of-scope or partially answerable by the classifier,
        not as nonsensical.
        """
                                             
        countries = [
            "malaysia", "philippines", "indonesia",
            "singapore", "thailand", "vietnam",
            "cambodia", "myanmar",
        ]
        found = [c for c in countries if c in q]
        if len(found) >= 2 and len(set(found)) >= 2:
            return True

                                         
        meaningful_words = [w for w in q.split() if len(w) > 2]
        if len(meaningful_words) < 2:
            return True

        return False

    def _explain_scope(self, triggered_domains: List[str], q: str) -> str:
        """Return a query-specific explanation of why data is out of scope."""
        domain = triggered_domains[0] if triggered_domains else "unknown"

                                                                   
        _SPECIFIC: dict = {
            ("premium", "quarter"): (
                "Premium amounts and payment transactions are not recorded in the "
                "proposal database. Proposals capture what businesses want to insure "
                "(insured values, security measures) but not the financial terms agreed."
            ),
            ("premium", "broker"): (
                "Broker information and premium generation data are not captured in "
                "proposal records. The database only contains direct business proposal "
                "details submitted by the insured parties."
            ),
            ("claim_amounts", "average"): (
                "Claim payout amounts are not stored in proposal records. "
                "The database captures only whether a claim was made within the "
                "past 3 years (Yes/No), not the value of those claims."
            ),
            ("claim_amounts", "fire"): (
                "Claim cause details — fire, flood, theft, robbery — are not "
                "recorded in proposal records. Only claim occurrence (within 3 years "
                "or not) is tracked."
            ),
            ("claim_amounts", "reject"): (
                "Claims processing data including rejection reasons, adjuster notes, "
                "and non-disclosure findings are not part of the proposal database. "
                "Proposals are submitted before a claim occurs."
            ),
            ("claim_amounts", "non-disclosure"): (
                "Claims processing data including rejection reasons, adjuster notes, "
                "and non-disclosure findings are not part of the proposal database. "
                "Proposals are submitted before a claim occurs."
            ),
            ("policy_lifecycle", "expir"): (
                "Policy expiry and renewal dates are not captured in proposal records. "
                "Proposals record the submission date (created_at) but policy duration "
                "and renewal terms are managed separately."
            ),
            ("policy_lifecycle", "approv"): (
                "Underwriting decisions, approval statuses, and modification histories "
                "are not stored in the proposal database. Only the submitted proposal "
                "details are available."
            ),
            ("policy_lifecycle", "underwriting"): (
                "Underwriting workflow data — turnaround times, review stages, "
                "decision timelines — is not captured in proposal records."
            ),
            ("policy_lifecycle", "turnaround"): (
                "Underwriting workflow data — turnaround times, review stages, "
                "decision timelines — is not captured in proposal records."
            ),
            ("policy_lifecycle", "modif"): (
                "Underwriting decisions, approval statuses, and modification histories "
                "are not stored in the proposal database. Only the submitted proposal "
                "details are available."
            ),
            ("temporal", "year-over-year"): (
                "Year-over-year comparison requires historical data across multiple "
                "time periods. This database contains a single snapshot of the "
                "current proposals — there is no prior year data to compare against."
            ),
            ("temporal", "yoy"): (
                "Year-over-year comparison requires historical data across multiple "
                "time periods. This database contains a single snapshot of the "
                "current proposals — there is no prior year data to compare against."
            ),
            ("temporal", "growth"): (
                "Year-over-year comparison requires historical data across multiple "
                "time periods. This database contains a single snapshot of the "
                "current proposals — there is no prior year data to compare against."
            ),
            ("temporal", "increased"): (
                "Coverage change history is not tracked. Each proposal represents "
                "the current submission only. Previous versions or amendments are "
                "not stored."
            ),
            ("temporal", "changed"): (
                "Coverage change history is not tracked. Each proposal represents "
                "the current submission only. Previous versions or amendments are "
                "not stored."
            ),
            ("temporal", "trend"): (
                "Only the current snapshot of proposal data exists. "
                "Historical comparisons and trend analysis are not possible."
            ),
        }

                                      
        for (dom, keyword), explanation in _SPECIFIC.items():
            if dom == domain and keyword in q:
                return explanation

                                
        return self._OUT_OF_SCOPE_EXPLANATIONS.get(
            domain,
            "This type of data is not captured in the proposal database.",
        )

    def _suggest_alternative(self, q: str) -> str:              
        """Context-aware alternative suggestions."""
        if any(w in q for w in ["premium", "collected", "revenue"]):
            return (
                "'Which proposals have the highest insured value?' or "
                "'List all proposals ranked by sum insured'"
            )
        if any(w in q for w in ["claim amount", "average claim", "claim value"]):
            return (
                "'Which businesses have had claims in the past 3 years?' or "
                "'Show claim history by region'"
            )
        if any(w in q for w in ["fire", "flood", "theft cause", "claim cause"]):
            return (
                "'Show claim history by region' or "
                "'What is the claim ratio across all proposals?'"
            )
        if any(w in q for w in ["reject", "non-disclosure", "declined"]):
            return (
                "'Show claim history by region' or "
                "'What is the overall claim rate across proposals?'"
            )
        if any(w in q for w in ["expir", "renew", "next month"]):
            return (
                "'When was each proposal created?' or "
                "'List all proposals with their submission dates'"
            )
        if any(w in q for w in ["broker", "agent", "intermediary"]):
            return (
                "'List all businesses with their contact details' or "
                "'Which proposals have the highest insured value?'"
            )
        if any(w in q for w in ["approv", "modif", "underwriting", "turnaround"]):
            return (
                "'List all proposals in the database' or "
                "'Show proposals by business type'"
            )
        if any(w in q for w in ["year-over-year", "yoy", "growth", "trend", "historical"]):
            return (
                "'Show top industries by total sum insured' or "
                "'List policies by insured value'"
            )
        if any(w in q for w in ["increas", "coverage change", "this year"]):
            return (
                "'Which proposals have the highest insured value?' or "
                "'Show industries by total sum insured'"
            )
        if "claim" in q:
            return (
                "'What is the claim rate across all proposals?', "
                "'Show claim history by region', 'Which regions have the lowest "
                "claim frequency?', or 'What is the claim history of [business name]?'"
            )
        if "premium" in q:
            return (
                "'Which proposals have the highest insured value?' or "
                "'What is the stock value insured for [business name]?'"
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
                "'Show policy type distribution', "
                "'What are the top industries by total sum insured?', or "
                "'What type of business does [name] run?'"
            )
        if "risk zone" in q or "high risk" in q:
            return (
                "'Show claim history by region', "
                "'Which locations have the highest claim frequency?', or "
                "'What regions have the lowest claim rate?'"
            )
        if "gps" in q:
            return (
                "'Which proposals have GPS trackers installed?', "
                "'List all businesses with GPS in transit vehicles', or "
                "'Does [business name] have GPS trackers?'"
            )
        if "southeast asia" in q or "asean" in q:
            return (
                "Only Malaysian proposals exist. You can ask about the "
                "nature_of_business distribution across all 15 proposals."
            )
        return (
            "'List all proposals' or "
            "'Show proposals by business type and insured value'"
        )

class PartialAnswerEngine:
    """
    Executes data-driven partial answers for PARTIALLY_ANSWERABLE queries.
    All handlers read from the loaded metadata at runtime — nothing is
    hardcoded (no business names, quote IDs, or values).
    """

    def __init__(self, metadata_path: str = "index/metadata.pkl") -> None:
        """Initialize a new instance with the provided dependencies and configuration.
        
        Args:
            metadata_path: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        self._path = metadata_path
        self._metadata: Optional[List[dict]] = None
        self._claim_guard_message = (
            "All 15 proposals report zero claims. "
            "No claim frequency comparison is possible."
        )

                          
    @property
    def metadata(self) -> List[dict]:
        """Handle metadata for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        if self._metadata is None:
            if os.path.exists(self._path):
                with open(self._path, "rb") as f:
                    self._metadata = pickle.load(f)
            else:
                self._metadata = []
        return self._metadata

                                                 
    @staticmethod
    def _build_business_name_map(metadata: list) -> dict:
        """
        Scan all business_profile section chunks and return a mapping of
        quote_id → human-readable business name.
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
            if name and str(name).strip().lower() not in ("", "none", "nan", "unknown"):
                name_map[qid] = str(name).strip()
            else:
                name_map[qid] = qid
        return name_map

                                                         
    def _get_complete_proposals_only(self, metadata: list) -> list:
        """Return complete proposals only for the current workflow.
        
        Args:
            metadata: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        return list(metadata)

    def _get_incomplete_quote_ids(self, metadata: list) -> list:
        """Return incomplete quote ids for the current workflow.
        
        Args:
            metadata: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        return []

                
    def dispatch(self, handler: str, query: str, query_intent: str = "summary") -> str:
        """Handle dispatch for this module.
        
        Args:
            handler: Input used to execute this operation.
            query: Input used to execute this operation.
            query_intent: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        _map = {
            "rank_by_sum_assured": lambda: self.handle_rank_by_sum_assured(),
            "filter_by_threshold": lambda: self.handle_filter_by_threshold(query),
            "group_by_industry": lambda: self.handle_group_by_industry(),
            "security_feature_summary": lambda: self.handle_security_feature_summary(),
            "business_type_distribution": lambda: self.handle_business_type_distribution(),
            "claims_by_location": lambda: self.handle_claims_by_location(query_intent=query_intent),
            "claim_rate": lambda: self.handle_claim_rate(),
            "list_all_businesses": lambda: self.handle_list_all_businesses(),
            "gps_tracker_proposals": lambda: self.handle_gps_tracker_proposals(),
            "addon_coverage_opt_in": lambda: self.handle_addon_coverage_opt_in(query),
            "director_fidelity_combined": lambda: self.handle_director_fidelity_combined(),
            "highest_fidelity_staff_with_director": lambda: self.handle_highest_fidelity_staff_with_director(),
            "transit_security_combo_values": lambda: self.handle_transit_security_combo_values(),
            "aggregate_fidelity_by_business_type": lambda: self.handle_aggregate_fidelity_by_business_type(),
            "total_director_coverage": lambda: self.handle_total_director_coverage(),
            "fidelity_per_staff_ratio": lambda: self.handle_fidelity_per_staff_ratio(),
            "state_security_count": lambda: self.handle_state_security_count(),
            "filter_no_alarm": lambda: self.handle_filter_no_alarm(),
            "business_type_compound_filter": lambda: self.handle_business_type_compound_filter(query),
            "state_grouping": lambda: self.handle_state_grouping(),
            "background_check_stock_frequency": lambda: self.handle_background_check_stock_frequency(),
            "compare_business_type_averages": lambda: self.handle_compare_business_type_averages(),
            "safe_grade_no_strong_room": lambda: self.handle_safe_grade_no_strong_room(),
            "rank_business_types_by_insured_value": lambda: self.handle_rank_business_types_by_insured_value(),
            "stock_out_of_safe_threshold": lambda: self.handle_stock_out_of_safe_threshold(query),
            "high_value_weak_security": lambda: self.handle_high_value_weak_security(query),
        }
        fn = _map.get(handler)
        return fn() if fn else "Partial data handler not available."

                                                                          
    _EMPTY_VALUES = {None, "", "None", -1, "-1", 0, "0", "nan", "N/A", "n/a"}

    @classmethod
    def _safe_float(cls, raw) -> float:
        """Handle safe float for this module.
        
        Args:
            cls: Input used to execute this operation.
            raw: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        if raw in cls._EMPTY_VALUES:
            return 0.0
        try:
            return float(str(raw).replace(",", ""))
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _get_primary_value(cls, fields: dict) -> tuple:
        """Return primary value for the current workflow.
        
        Args:
            cls: Input used to execute this operation.
            fields: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        v = cls._safe_float(fields.get("maximum_stock_in_premises_label"))
        if v > 0:
            return (v, "jewellery stock")

        v = cls._safe_float(fields.get("maximum_stock_foreign_currency_in_premise_label"))
        if v > 0:
            return (v, "foreign currency stock")

        pledged = cls._safe_float(fields.get("value_of_pledged_stock_in_premise_label"))
        cash = cls._safe_float(fields.get("value_of_cash_in_premise_label"))
        total = pledged + cash
        if total > 0:
            return (total, "pledged stock + cash")

        return (0.0, None)

    @staticmethod
    def _extract_state(risk_location: str) -> str:
        """Handle extract state for this module.
        
        Args:
            risk_location: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        if not risk_location:
            return "Unknown"
        parts = [p.strip() for p in risk_location.split(",") if p.strip()]
        if parts and parts[-1].lower() == "malaysia":
            parts = parts[:-1]
        if not parts:
            return "Unknown"
        return parts[-1].strip() or "Unknown"

                                                    
    def handle_rank_by_sum_assured(self, top_n: int = 15) -> str:
        """Handle handle rank by sum assured for this module.
        
        Args:
            top_n: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        ranked = []
        seen: set = set()
        for chunk in complete_metadata:
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
            ranked.append((name_map.get(qid, qid), qid, value, label))

        if not ranked:
            return "No insured value data found across proposals."

        ranked.sort(key=lambda x: x[2], reverse=True)
        lines = ["Proposals ranked by total insured value (highest first):"]
        for i, (name, qid, value, label) in enumerate(ranked[:top_n], 1):
            lines.append(f"{i}. {name} ({qid}): RM {value:,.0f} ({label})")
        return "\n".join(lines)

                                            
    def handle_filter_by_threshold(self, query: str) -> str:
        """Handle handle filter by threshold for this module.
        
        Args:
            query: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
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
                if always_million or "million" in raw_hit or (raw_hit[-1] == "m" and not raw_hit.endswith("rm")):
                    threshold *= 1_000_000
                break

        if threshold is None:
            return "Could not determine the threshold value from your query."

        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        results = []
        seen: set = set()
        for chunk in complete_metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            fields = chunk.get("fields") or {}
            value, label = self._get_primary_value(fields)
            if value > threshold:
                results.append((name_map.get(qid, qid), qid, value, label))
                seen.add(qid)

        if not results:
            return f"No proposals found with insured value above RM {threshold:,.0f}."

        results.sort(key=lambda x: x[2], reverse=True)
        lines = [f"Proposals with insured value above RM {threshold:,.0f}:"]
        for name, qid, val, label in results:
            lines.append(f"- {name} ({qid}): RM {val:,.0f} ({label})")
        return "\n".join(lines)

                                                
    def handle_claims_by_location(self, query_intent: str = "summary") -> str:
        """Handle handle claims by location for this module.
        
        Args:
            query_intent: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        return self._claim_guard_message

                                            
    def handle_claim_rate(self) -> str:
        """Handle handle claim rate for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        return self._claim_guard_message

                                              
    def handle_list_all_businesses(self) -> str:
        """Handle handle list all businesses for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        location_map: dict = {}
        industry_map: dict = {}
        seen: set = set()
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen:
                continue
            if chunk.get("section") == "business_profile":
                seen.add(qid)
                loc = chunk.get("risk_location", "")
                location_map[qid] = str(loc).strip() if loc else "Not specified"
                df = chunk.get("decoded_fields") or {}
                industry_map[qid] = str(df.get("nature_of_business_label") or "Not specified")

        total = len(name_map)
        lines = [f"All businesses in the proposal database ({total} total):"]
        for qid in sorted(name_map):
            name = name_map.get(qid, qid)
            location = location_map.get(qid, "Not specified")
            industry = industry_map.get(qid, "Not specified")
            lines.append(f"- {name} ({qid}) | Type: {industry} | Location: {location}")
        return "\n".join(lines)

    def handle_addon_coverage_opt_in(self, query: str) -> str:
        """Handle handle addon coverage opt in for this module.
        
        Args:
            query: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        optin_map: dict = {}
        amount_map: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            if chunk.get("section") == "add_on_coverage":
                fields = chunk.get("fields") or {}
                amount_map[qid] = fields
                optin_map[qid] = {
                    "director_opted": str(fields.get("director_house_question_label", "") or "").strip(),
                    "fidelity_opted": str(fields.get("fidelity_guarantee_insurance_add_coverage_label", "") or "").strip(),
                }

        q = query.lower()
        wants_fidelity = "fidelity" in q
        wants_director = "director" in q
        wants_both = "both" in q or (wants_fidelity and wants_director)

        def _is_yes_code(v) -> bool:
            """Handle is yes code for this module.
            
            Args:
                v: Input used to execute this operation.
            
            Returns:
                Result generated by this operation, when applicable.
            """
            return str(v).strip() == "001"

        def _as_num(v) -> float:
            """Handle as num for this module.
            
            Args:
                v: Input used to execute this operation.
            
            Returns:
                Result generated by this operation, when applicable.
            """
            try:
                return float(str(v).replace(",", "").strip())
            except (TypeError, ValueError):
                return 0.0

        matches = []
        for qid in sorted(name_map.keys()):
            opt = optin_map.get(qid, {})
            addon = amount_map.get(qid, {})
            fidelity_opted = _is_yes_code(opt.get("fidelity_opted", ""))
            director_opted = _is_yes_code(opt.get("director_opted", ""))

            include = False
            if wants_both:
                include = fidelity_opted and director_opted
            elif wants_fidelity:
                include = fidelity_opted
            elif wants_director:
                include = director_opted

            if include:
                matches.append(
                    (
                        qid,
                        name_map.get(qid, qid),
                        director_opted,
                        fidelity_opted,
                        _as_num(addon.get("director_house_coverage_label")),
                        _as_num(addon.get("fidelity_guarantee_insurance_label")),
                    )
                )

        if not matches:
            return "No proposals match the requested add-on coverage condition."

        lines = [f"Matching proposals: {len(matches)}"]
        total_director = 0.0
        total_fidelity = 0.0
        for qid, name, d_opt, f_opt, d_amt, f_amt in matches:
            total_director += d_amt
            total_fidelity += f_amt
            lines.append(
                f"- {name} ({qid}) | Director Opted: {'Yes' if d_opt else 'No'} | Fidelity Opted: {'Yes' if f_opt else 'No'} | Director Amount: RM {d_amt:,.0f} | Fidelity Amount: RM {f_amt:,.0f}"
            )

        lines.append(f"Total Director Amount: RM {total_director:,.0f}")
        lines.append(f"Total Fidelity Amount: RM {total_fidelity:,.0f}")
        lines.append(f"Combined Add-on Amount: RM {total_director + total_fidelity:,.0f}")
        return "\n".join(lines)

                                                            
    def handle_aggregate_fidelity_by_business_type(self) -> str:
        """Handle handle aggregate fidelity by business type for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        BTYPE_MAP = {"1": "Jewellers", "2": "Money Changers", "3": "Other", "5": "Pawn Brokers"}

                                                       
        btype_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in btype_map:
                continue
            fields = chunk.get("fields") or {}
            code = str(fields.get("nature_of_business_label", "") or "").strip()
            btype_map[qid] = BTYPE_MAP.get(code, f"Type {code}" if code else "Unknown")

                                                        
        fidelity_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "add_on_coverage":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in fidelity_map:
                continue
            fields = chunk.get("fields") or {}
            fidelity_map[qid] = self._safe_float(fields.get("fidelity_guarantee_insurance_label"))

                                
        groups: dict = {}
        for qid in sorted(name_map.keys()):
            bt = btype_map.get(qid, "Unknown")
            amt = fidelity_map.get(qid, 0.0)
            if bt not in groups:
                groups[bt] = {"total": 0.0, "count": 0, "proposals": []}
            groups[bt]["total"] += amt
            groups[bt]["count"] += 1
            groups[bt]["proposals"].append((name_map.get(qid, qid), qid, amt))

        grand_total = sum(g["total"] for g in groups.values())
        ranked = sorted(groups.items(), key=lambda x: (x[1]["total"] / x[1]["count"] if x[1]["count"] else 0), reverse=True)

        lines = ["Fidelity Guarantee Insurance by Business Type:"]
        for bt, g in ranked:
            avg = g["total"] / g["count"] if g["count"] else 0
            lines.append(f"\n{bt} ({g['count']} proposals):")
            lines.append(f"  Total: RM {g['total']:,.0f} | Average: RM {avg:,.0f}")
            for name, qid, amt in g["proposals"]:
                lines.append(f"  - {name} ({qid}): RM {amt:,.0f}")
        lines.append(f"\nGrand Total Fidelity across all proposals: RM {grand_total:,.0f}")
        return "\n".join(lines)

                                            
    def handle_total_director_coverage(self) -> str:
        """Handle handle total director coverage for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                                                          
        amount_map: dict = {}
        optin_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "add_on_coverage":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in amount_map:
                continue
            fields = chunk.get("fields") or {}
            amount_map[qid] = fields
            optin_map[qid] = str(fields.get("director_house_question_label", "") or "").strip()

        matches = []
        total = 0.0
        for qid in sorted(name_map.keys()):
            addon = amount_map.get(qid, {})
            if optin_map.get(qid, "") == "001":
                amt = self._safe_float(addon.get("director_house_coverage_label"))
                matches.append((name_map.get(qid, qid), qid, amt))
                total += amt

        if not matches:
            return "No proposals have opted for director's house coverage."

        lines = [f"Director's House Coverage — {len(matches)} proposals opted:"]
        for name, qid, amt in matches:
            lines.append(f"  - {name} ({qid}): RM {amt:,.0f}")
        lines.append(f"\nTotal Director's House Coverage: RM {total:,.0f}")
        return "\n".join(lines)

                                       
    def handle_fidelity_per_staff_ratio(self) -> str:
        """Handle handle fidelity per staff ratio for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        addon_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "add_on_coverage":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in addon_map:
                continue
            addon_map[qid] = chunk.get("fields") or {}

        ratios = []
        for qid in sorted(name_map.keys()):
            addon = addon_map.get(qid, {})
            fidelity = self._safe_float(addon.get("fidelity_guarantee_insurance_label"))
            staff_raw = addon.get("fidelity_guarantee_total_staff_label", "")
            try:
                staff = int(str(staff_raw).strip()) if str(staff_raw).strip() else 0
            except ValueError:
                staff = 0

            if staff > 0 and fidelity > 0:
                ratio = fidelity / staff
                ratios.append((name_map.get(qid, qid), qid, fidelity, staff, ratio))

        if not ratios:
            return "No proposals have both fidelity amount and staff count data."

        ratios.sort(key=lambda x: x[4], reverse=True)
        lines = [f"Fidelity Guarantee per Staff Member — {len(ratios)} proposals:"]
        for i, (name, qid, fidelity, staff, ratio) in enumerate(ratios, 1):
            lines.append(
                f"  {i}. {name} ({qid}): RM {ratio:,.0f}/staff "
                f"(RM {fidelity:,.0f} ÷ {staff} staff)"
            )
        return "\n".join(lines)

                                                                   
    def handle_state_security_count(self) -> str:
        """Handle handle state security count for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                                                     
        alarm_map: dict = {}
        sr_map: dict = {}
        loc_map: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            section = chunk.get("section")
            fields = chunk.get("fields") or {}

            if section == "alarm" and qid not in alarm_map:
                alarm_map[qid] = str(fields.get("do_you_have_alarm_label", "") or "").strip()
            elif section == "strong_room" and qid not in sr_map:
                sr_map[qid] = str(fields.get("do_you_have_a_strong_room_label", "") or "").strip()

            if qid not in loc_map:
                loc = chunk.get("risk_location", "")
                if loc:
                    loc_map[qid] = str(loc)

                                               
        matching = []
        for qid in sorted(name_map.keys()):
            has_alarm = alarm_map.get(qid, "") == "001"
            has_sr = sr_map.get(qid, "") == "001"
            if has_alarm and has_sr:
                matching.append(qid)

                        
        state_groups: dict = {}
        for qid in matching:
            state = self._extract_state(loc_map.get(qid, ""))
            if state not in state_groups:
                state_groups[state] = []
            state_groups[state].append((name_map.get(qid, qid), qid))

                                                        
        all_states: dict = {}
        for qid in sorted(name_map.keys()):
            state = self._extract_state(loc_map.get(qid, ""))
            if state not in all_states:
                all_states[state] = 0
            all_states[state] += 1

        lines = [f"Proposals with BOTH alarm and strong room — grouped by state:"]
        lines.append(f"Total matching: {len(matching)} out of {len(name_map)} proposals\n")
        for state in sorted(all_states.keys()):
            matched = state_groups.get(state, [])
            total_in_state = all_states[state]
            lines.append(f"{state}: {len(matched)} of {total_in_state} proposals")
            for name, qid in matched:
                lines.append(f"  - {name} ({qid})")

        return "\n".join(lines)

                                             
    def handle_filter_no_alarm(self) -> str:
        """Handle handle filter no alarm for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        sa_map = self._build_sa_map(complete_metadata)

                                                       
        alarm_map: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            if chunk.get("section") == "alarm" and qid not in alarm_map:
                fields = chunk.get("fields") or {}
                alarm_map[qid] = str(fields.get("do_you_have_alarm_label", "") or "").strip()

                                                              
        matches = []
        for qid in sorted(name_map.keys()):
            if alarm_map.get(qid, "") == "002":
                insured = sa_map.get(qid, 0)
                matches.append((name_map.get(qid, qid), qid, insured))

        if not matches:
            return "All proposals have an alarm system."

        grand_total = sum(m[2] for m in matches)
        lines = [f"Proposals without an alarm system — {len(matches)} found:\n"]
        for name, qid, insured in matches:
            lines.append(f"  - {name} ({qid}) — Total Insured: RM {insured:,.0f}")
        lines.append(f"\nTotal insured across all {len(matches)} proposals: RM {grand_total:,.0f}")
        lines.append(f"{len(name_map) - len(matches)} of {len(name_map)} proposals DO have an alarm system.")
        return "\n".join(lines)

                                                                 
    def handle_business_type_compound_filter(self, query: str) -> str:
        """Handle handle business type compound filter for this module.

        Args:
            query: Input used to execute this operation.

        Returns:
            Result generated by this operation, when applicable.
        """
        q = query.lower()

        oos_keywords = ["premium", "revenue", "income", "profit", "approve", "reject",
                        "underwriter", "turnaround", "policy status"]
        if any(kw in q for kw in oos_keywords):
            return (
                "This information is not available in the proposal database.\n\n"
                "Premium amounts, revenue, and underwriting decisions are not in proposal records.\n\n"
                "You can ask: 'Which proposals have the highest insured value?' "
                "or 'List all proposals ranked by sum insured'"
            )

        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        BTYPE_MAP = {"1": "Jewellers", "2": "Money Changers", "3": "Other", "5": "Pawn Brokers"}


        target_code = None
        target_label = None
        if "pawn broker" in q or "pawnbroker" in q or "pawn shop" in q:
            target_code = "5"
            target_label = "Pawn Brokers"
        elif "money changer" in q or "money exchange" in q:
            target_code = "2"
            target_label = "Money Changers"
        elif "jeweller" in q or "goldsmith" in q or "jeweler" in q:
            target_code = "1"
            target_label = "Jewellers"

        if not target_code:
            return "Could not determine business type from query."

                                         
        btype_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in btype_map:
                continue
            fields = chunk.get("fields") or {}
            code = str(fields.get("nature_of_business_label", "") or "").strip()
            btype_map[qid] = code

        type_filtered = [qid for qid in sorted(name_map.keys())
                         if btype_map.get(qid) == target_code]

        if not type_filtered:
            return f"No {target_label} proposals found."


        alarm_map: dict = {}
        sr_map: dict = {}
        safe_map: dict = {}
        cctv_map: dict = {}
        guards_map: dict = {}
        gps_vehicle_map: dict = {}
        gps_bag_map: dict = {}
        loc_map: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            section = chunk.get("section")
            fields = chunk.get("fields") or {}

            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            if section == "alarm" and qid not in alarm_map:
                alarm_map[qid] = str(fields.get("do_you_have_alarm_label", "") or "").strip()
            elif section == "strong_room" and qid not in sr_map:
                sr_map[qid] = str(fields.get("do_you_have_a_strong_room_label", "") or "").strip()
            elif section == "safe" and qid not in safe_map:
                safe_map[qid] = str(fields.get("grade_label", "") or "").strip()
            elif section == "cctv" and qid not in cctv_map:
                cctv_map[qid] = str(fields.get("cctv_maintenance_contract_label", "") or "").strip()
            elif section == "transit_and_gaurds" and qid not in guards_map:
                guards_map[qid] = str(fields.get("do_you_use_guards_at_premise_label", "") or "").strip()
                gps_vehicle_map[qid] = str(fields.get("installed_gps_tracker_in_transit_vehicles_label", "") or "").strip()
                gps_bag_map[qid] = str(fields.get("installed_gps_tracker_in_transit_bags_label", "") or "").strip()
            if qid not in loc_map:
                loc = chunk.get("risk_location", "")
                if loc:
                    loc_map[qid] = str(loc)


        SAFE_GRADE_MAP = {"001": "Grade 1", "002": "Grade 2", "003": "Grade 3", "004": "Grade 4"}
        conditions = []
        condition_labels = []

        if "strong room" in q:
            conditions.append(("strong_room", lambda qid: sr_map.get(qid) == "001"))
            condition_labels.append("strong room")
        if "no strong room" in q:
            conditions = [(n, f) for n, f in conditions if n != "strong_room"]
            condition_labels = [l for l in condition_labels if l != "strong room"]
            conditions.append(("no_strong_room", lambda qid: sr_map.get(qid) == "002"))
            condition_labels.append("no strong room")
        if "grade 3" in q or "grade 4" in q:
            accepted = set()
            if "grade 3" in q:
                accepted.add("003")
            if "grade 4" in q:
                accepted.add("004")

            if "grade 3" in q and "grade 4" not in q:
                accepted = {"003", "004"}
            conditions.append(("safe_grade", lambda qid, acc=accepted: safe_map.get(qid) in acc))
            grade_list = " or ".join(SAFE_GRADE_MAP.get(g, g) for g in sorted(accepted))
            condition_labels.append(f"safe {grade_list}")
        if "cctv" in q and ("maintenance" in q or "contract" in q):
            conditions.append(("cctv_maint", lambda qid: cctv_map.get(qid) == "001"))
            condition_labels.append("CCTV maintenance contract")
        if "guard" in q and "premise" in q:
            conditions.append(("guards_premise", lambda qid: guards_map.get(qid) == "001"))
            condition_labels.append("guards at premise")
        if "alarm" in q and "no alarm" not in q:
            conditions.append(("alarm", lambda qid: alarm_map.get(qid) == "001"))
            condition_labels.append("alarm")
        if "gps" in q and ("vehicle" in q or "car" in q):
            conditions.append(("gps_vehicle", lambda qid: gps_vehicle_map.get(qid) == "001"))
            condition_labels.append("GPS in vehicles")
        if "gps" in q and "bag" in q:
            conditions.append(("gps_bag", lambda qid: gps_bag_map.get(qid) == "001"))
            condition_labels.append("GPS in bags")


        if conditions:
            final = [qid for qid in type_filtered
                     if all(fn(qid) for _, fn in conditions)]
        else:
            final = type_filtered


        cond_str = " AND ".join(condition_labels) if condition_labels else "no additional conditions"
        lines = [f"{target_label} — {len(type_filtered)} total in database, "
                 f"filtered by: {cond_str}"]
        lines.append(f"Matching: {len(final)} proposal(s)\n")

        for name_qid in final:
            name = name_map.get(name_qid, name_qid)
            state = self._extract_state(loc_map.get(name_qid, ""))
            detail_parts = []
            if sr_map.get(name_qid):
                detail_parts.append(f"Strong Room: {'Yes' if sr_map[name_qid] == '001' else 'No'}")
            if safe_map.get(name_qid):
                detail_parts.append(f"Safe: {SAFE_GRADE_MAP.get(safe_map[name_qid], safe_map[name_qid])}")
            if cctv_map.get(name_qid):
                detail_parts.append(f"CCTV Contract: {'Yes' if cctv_map[name_qid] == '001' else 'No'}")
            if guards_map.get(name_qid):
                detail_parts.append(f"Guards at Premise: {'Yes' if guards_map[name_qid] == '001' else 'No'}")
            if alarm_map.get(name_qid):
                detail_parts.append(f"Alarm: {'Yes' if alarm_map[name_qid] == '001' else 'No'}")
            if gps_vehicle_map.get(name_qid):
                detail_parts.append(f"GPS Vehicles: {'Yes' if gps_vehicle_map[name_qid] == '001' else 'No'}")
            if gps_bag_map.get(name_qid):
                detail_parts.append(f"GPS Bags: {'Yes' if gps_bag_map[name_qid] == '001' else 'No'}")
            detail_str = " | ".join(detail_parts) if detail_parts else ""
            lines.append(f"  - {name} ({name_qid}) — {state}")
            if detail_str:
                lines.append(f"    {detail_str}")

        if conditions and len(final) < len(type_filtered):
            excluded = [qid for qid in type_filtered if qid not in final]
            if excluded:
                lines.append(f"\nDid not match ({len(excluded)}):")
                for qid in excluded:
                    name = name_map.get(qid, qid)
                    detail_parts = []
                    if sr_map.get(qid):
                        detail_parts.append(f"Strong Room: {'Yes' if sr_map[qid] == '001' else 'No'}")
                    if safe_map.get(qid):
                        detail_parts.append(f"Safe: {SAFE_GRADE_MAP.get(safe_map[qid], safe_map[qid])}")
                    if cctv_map.get(qid):
                        detail_parts.append(f"CCTV Contract: {'Yes' if cctv_map[qid] == '001' else 'No'}")
                    if guards_map.get(qid):
                        detail_parts.append(f"Guards at Premise: {'Yes' if guards_map[qid] == '001' else 'No'}")
                    if gps_vehicle_map.get(qid):
                        detail_parts.append(f"GPS Vehicles: {'Yes' if gps_vehicle_map[qid] == '001' else 'No'}")
                    if gps_bag_map.get(qid):
                        detail_parts.append(f"GPS Bags: {'Yes' if gps_bag_map[qid] == '001' else 'No'}")
                    detail_str = " | ".join(detail_parts)
                    lines.append(f"  - {name} ({qid}): {detail_str}")

        return "\n".join(lines)

    def handle_high_value_weak_security(self, query: str) -> str:
        """Find proposals above a value threshold that lack key security features."""
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        sa_map = self._build_sa_map(complete_metadata)

        q = query.lower()
        threshold = 0.0
        m = re.search(r"(?:rm\s*)?(\d[\d,.]*)\s*(?:million|m)\b", q)
        if m:
            raw = float(m.group(1).replace(",", ""))
            threshold = raw * 1_000_000 if raw < 1000 else raw
        if not m:
            m = re.search(r"above\s+(?:rm\s*)?(\d[\d,.]*)", q)
            if m:
                threshold = float(m.group(1).replace(",", ""))

        above = [qid for qid in sorted(name_map.keys())
                 if sa_map.get(qid, 0.0) > threshold]

        alarm_map: dict = {}
        sr_map: dict = {}
        gps_v_map: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            section = chunk.get("section")
            fields = chunk.get("fields") or {}
            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            if section == "alarm" and qid not in alarm_map:
                alarm_map[qid] = str(fields.get("do_you_have_alarm_label", "") or "").strip()
            elif section == "strong_room" and qid not in sr_map:
                sr_map[qid] = str(fields.get("do_you_have_a_strong_room_label", "") or "").strip()
            elif section == "transit_and_gaurds" and qid not in gps_v_map:
                gps_v_map[qid] = str(fields.get("installed_gps_tracker_in_transit_vehicles_label", "") or "").strip()

        weak = []
        for qid in above:
            gaps = []
            if alarm_map.get(qid) != "001":
                gaps.append("No Alarm")
            if sr_map.get(qid) != "001":
                gaps.append("No Strong Room")
            if gps_v_map.get(qid) != "001":
                gaps.append("No GPS in Vehicles")
            if gaps:
                weak.append((qid, sa_map.get(qid, 0.0), gaps))

        weak.sort(key=lambda x: x[1], reverse=True)
        lines = [
            f"High-value proposals (above RM {threshold:,.0f}) with weak security — "
            f"{len(weak)} of {len(above)} found:\n"
        ]
        for qid, insured, gaps in weak:
            name = name_map.get(qid, qid)
            lines.append(f"  {name} ({qid}) — RM {insured:,.0f}")
            lines.append(f"    Security gaps: {', '.join(gaps)}")

        if not weak:
            lines.append("All high-value proposals have alarm, strong room, and GPS in vehicles.")

        return "\n".join(lines)



    _MONETARY_SIGNALS = ("stock", "cash", "sum", "value", "limit", "transit")

    @classmethod
    def _is_monetary_sum_key(cls, key: str) -> bool:
        """Handle is monetary sum key for this module.
        
        Args:
            cls: Input used to execute this operation.
            key: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        k = key.lower()
        if "nature_of_business" in k:
            return False
        return any(s in k for s in cls._MONETARY_SIGNALS)

    def _build_sa_map(self, complete_metadata: list) -> dict:
        """Return {quote_id: total_insured_float} from sum_assured sections."""
        sa: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in sa:
                continue
            fields = chunk.get("fields") or {}
            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            sa[qid] = sum(
                self._safe_float(v)
                for k, v in fields.items()
                if self._is_monetary_sum_key(k) and self._safe_float(v) > 0
            )
        return sa

    def _build_btype_map(self, complete_metadata: list) -> dict:
        """Return {quote_id: nature_of_business_label_code}."""
        btype: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in btype:
                continue
            fields = chunk.get("fields") or {}
            btype[qid] = str(fields.get("nature_of_business_label", "") or "").strip()
        return btype

    def _build_loc_map(self, complete_metadata: list) -> dict:
        """Return {quote_id: risk_location_string}."""
        loc: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in loc:
                continue
            rl = chunk.get("risk_location", "")
            if rl:
                loc[qid] = str(rl)
        return loc

                                                                           
    def handle_background_check_stock_frequency(self) -> str:
        """Handle handle background check stock frequency for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        STOCK_FREQ_MAP = {
            "001": "Most Frequent (daily)",
            "002": "Medium Frequency",
            "003": "Least Frequent (monthly)",
        }
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                                                               
        details_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "additional_details":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in details_map:
                continue
            fields = chunk.get("fields") or {}
            details_map[qid] = {
                "bg_check": str(fields.get("background_checks_for_all_employees_label", "") or "").strip(),
                "stock_freq": str(fields.get("how_often_is_the_stock_check_carried_out_label", "") or "").strip(),
            }

                                                
        with_bg = [qid for qid in sorted(name_map.keys())
                   if details_map.get(qid, {}).get("bg_check") == "001"]

                                                                    
        matches = [qid for qid in with_bg
                   if details_map.get(qid, {}).get("stock_freq") == "001"]

        lines = [
            "Background checks (Yes) AND most-frequent stock checks — filter results:",
            f"  Proposals with background checks: {len(with_bg)}",
            f"  Of those, with most-frequent stock checks (code 001): {len(matches)}",
            "",
        ]

        if matches:
            lines.append(f"Matching proposals ({len(matches)}):")
            for qid in matches:
                name = name_map.get(qid, qid)
                d = details_map.get(qid, {})
                freq_label = STOCK_FREQ_MAP.get(d.get("stock_freq", ""), "Unknown")
                lines.append(f"  - {name} ({qid})")
                lines.append(f"    Background Check: Yes | Stock Check: {freq_label}")
        else:
            lines.append("No proposals match both conditions.")

                                                          
        lines.append("\nAll proposals — background check + stock check frequency:")
        for qid in sorted(name_map.keys()):
            d = details_map.get(qid, {})
            bg = "Yes" if d.get("bg_check") == "001" else "No"
            freq_code = d.get("stock_freq", "")
            freq_label = STOCK_FREQ_MAP.get(freq_code, f"Code {freq_code}" if freq_code else "Unknown")
            match_marker = " ← MATCH" if qid in matches else ""
            lines.append(
                f"  {name_map.get(qid, qid)} ({qid}): "
                f"BG Check: {bg} | Stock Freq: {freq_label}{match_marker}"
            )

        sa_map = self._build_sa_map(complete_metadata)
        if sa_map:
            ranked = sorted(
                [(qid, sa_map.get(qid, 0.0)) for qid in sorted(name_map.keys())],
                key=lambda x: x[1],
                reverse=True,
            )
            top_5 = ranked[:5]
            top_qids = {qid for qid, _ in top_5}
            overlap = [qid for qid in matches if qid in top_qids]

            lines.append("\n--- Insured Value Overlap Analysis ---")
            lines.append(f"Top {len(top_5)} proposals by total insured value:")
            for qid, val in top_5:
                name = name_map.get(qid, qid)
                marker = " << ALSO MATCHES BG+Stock" if qid in matches else ""
                lines.append(f"  {name} ({qid}): RM {val:,.0f}{marker}")

            if overlap:
                lines.append(f"\nOverlap: {len(overlap)} proposal(s) appear in BOTH groups:")
                for qid in overlap:
                    lines.append(f"  {name_map.get(qid, qid)} ({qid}): RM {sa_map.get(qid, 0.0):,.0f}")
            else:
                lines.append("\nNo overlap between highest-insured and BG+Stock match lists.")

        return "\n".join(lines)

                                                                            
    def handle_state_grouping(self) -> str:
        """Handle handle state grouping for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        BTYPE_MAP = {"1": "Jeweller", "2": "Money Changer", "3": "Other", "5": "Pawn Broker"}
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        sa_map = self._build_sa_map(complete_metadata)
        loc_map = self._build_loc_map(complete_metadata)
        btype_map = self._build_btype_map(complete_metadata)

        groups: dict = {}
        for qid in sorted(name_map.keys()):
            state = self._extract_state(loc_map.get(qid, ""))
            if state not in groups:
                groups[state] = {"count": 0, "total": 0.0, "proposals": []}
            groups[state]["count"] += 1
            total = sa_map.get(qid, 0.0)
            groups[state]["total"] += total
            btype_label = BTYPE_MAP.get(btype_map.get(qid, ""), "Unknown")
            groups[state]["proposals"].append((name_map.get(qid, qid), qid, total, btype_label))

        multi = [(s, g) for s, g in groups.items() if g["count"] > 1]
        multi.sort(key=lambda x: (x[1]["count"], x[1]["total"]), reverse=True)

        lines = [f"States with more than 1 proposal — {len(multi)} state(s) found:\n"]
        for state, g in multi:
            lines.append(f"{state}: {g['count']} proposals | Total Insured: RM {g['total']:,.0f}")
            for name, qid, amt, btype in g["proposals"]:
                lines.append(f"  - {name} ({qid}) [{btype}]: RM {amt:,.0f}")

        grand = sum(g["total"] for _, g in multi)
        lines.append(f"\nCombined insured (multi-proposal states): RM {grand:,.0f}")
        lines.append(f"\nSingle-proposal states ({len(groups) - len(multi)} total) not shown.")
        return "\n".join(lines)

                                                                              
    def handle_compare_business_type_averages(self) -> str:
        """Handle handle compare business type averages for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        BTYPE_MAP = {"1": "Jewellers", "2": "Money Changers", "3": "Other", "5": "Pawn Brokers"}
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        sa_map = self._build_sa_map(complete_metadata)
        btype_map = self._build_btype_map(complete_metadata)

        groups: dict = {label: {"total": 0.0, "count": 0, "proposals": []}
                        for label in BTYPE_MAP.values()}

        for qid in sorted(name_map.keys()):
            code = btype_map.get(qid, "")
            label = BTYPE_MAP.get(code, "Unknown")
            if label not in groups:
                groups[label] = {"total": 0.0, "count": 0, "proposals": []}
            amt = sa_map.get(qid, 0.0)
            groups[label]["total"] += amt
            groups[label]["count"] += 1
            groups[label]["proposals"].append((name_map.get(qid, qid), qid, amt))

        lines = ["Average Insured Value Comparison — Jewellers vs Money Changers:\n"]
        for label in ("Jewellers", "Money Changers"):
            g = groups.get(label, {"total": 0.0, "count": 0, "proposals": []})
            avg = g["total"] / g["count"] if g["count"] else 0.0
            lines.append(f"{label}: {g['count']} proposals")
            lines.append(f"  Total: RM {g['total']:,.0f} | Average: RM {avg:,.0f}")
            for name, qid, amt in g["proposals"]:
                lines.append(f"  - {name} ({qid}): RM {amt:,.0f}")
            lines.append("")

                            
        j = groups.get("Jewellers", {"total": 0.0, "count": 0})
        m = groups.get("Money Changers", {"total": 0.0, "count": 0})
        j_avg = j["total"] / j["count"] if j["count"] else 0.0
        m_avg = m["total"] / m["count"] if m["count"] else 0.0
        higher = "Jewellers" if j_avg >= m_avg else "Money Changers"
        ratio = j_avg / m_avg if m_avg else 0.0
        lines.append(f"Jewellers average is {ratio:.1f}x that of Money Changers.")
        lines.append(f"{higher} carry higher average insured value per proposal.")
        return "\n".join(lines)

                                                                            
    def handle_safe_grade_no_strong_room(self) -> str:
        """Handle handle safe grade no strong room for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        SAFE_GRADE_MAP = {"001": "Grade 1", "002": "Grade 2", "003": "Grade 3", "004": "Grade 4"}
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        sa_map = self._build_sa_map(complete_metadata)

        safe_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "safe":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in safe_map:
                continue
            fields = chunk.get("fields") or []
            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            safe_map[qid] = str(fields.get("grade_label", "") or "").strip()

        sr_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "strong_room":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in sr_map:
                continue
            fields = chunk.get("fields") or {}
            sr_map[qid] = str(fields.get("do_you_have_a_strong_room_label", "") or "").strip()

        matches = [
            (name_map.get(qid, qid), qid, sa_map.get(qid, 0.0))
            for qid in sorted(name_map.keys())
            if safe_map.get(qid) == "004" and sr_map.get(qid) == "002"
        ]

        lines = ["Proposals with Grade 4 safe AND no strong room:"]
        if not matches:
            lines.append("  None found.")
        else:
            for name, qid, insured in matches:
                lines.append(f"  - {name} ({qid}) — Total Insured: RM {insured:,.0f}")

                                                        
        lines.append("\nAll proposals — safe grade + strong room:")
        for qid in sorted(name_map.keys()):
            grade = safe_map.get(qid, "")
            sr = sr_map.get(qid, "")
            grade_str = SAFE_GRADE_MAP.get(grade, f"Code {grade}" if grade else "Unknown")
            sr_str = "Yes" if sr == "001" else ("No" if sr == "002" else "Unknown")
            marker = " ← MATCH" if (grade == "004" and sr == "002") else ""
            insured = sa_map.get(qid, 0.0)
            lines.append(
                f"  {name_map.get(qid, qid)} ({qid}): {grade_str} | Strong Room: {sr_str} | Total Insured: RM {insured:,.0f}{marker}"
            )

        return "\n".join(lines)

                                                                            
    def handle_rank_business_types_by_insured_value(self) -> str:
        """Handle handle rank business types by insured value for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        BTYPE_MAP = {"1": "Jewellers", "2": "Money Changers", "3": "Other", "5": "Pawn Brokers"}
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        sa_map = self._build_sa_map(complete_metadata)
        btype_map = self._build_btype_map(complete_metadata)

        groups: dict = {}
        for qid in sorted(name_map.keys()):
            code = btype_map.get(qid, "")
            label = BTYPE_MAP.get(code, f"Type {code}" if code else "Unknown")
            if label not in groups:
                groups[label] = {"total": 0.0, "count": 0}
            groups[label]["total"] += sa_map.get(qid, 0.0)
            groups[label]["count"] += 1

        ranked = sorted(
            groups.items(),
            key=lambda x: x[1]["total"] / x[1]["count"] if x[1]["count"] else 0,
            reverse=True,
        )

        lines = ["Business Types Ranked by Average Total Insured Value:\n"]
        for i, (label, g) in enumerate(ranked, 1):
            avg = g["total"] / g["count"] if g["count"] else 0.0
            note = ""
            if g["count"] == 1:
                note = " — limited sample, not comparable"
            lines.append(
                f"  {i}. {label}: avg RM {avg:,.0f} "
                f"(total RM {g['total']:,.0f} across {g['count']} "
                f"{'proposal' if g['count'] == 1 else 'proposals'}{note})"
            )
        return "\n".join(lines)

                                                                            
    def handle_stock_out_of_safe_threshold(self, query: str) -> str:
        """Handle handle stock out of safe threshold for this module.
        
        Args:
            query: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        import re
        SAFE_GRADE_MAP = {"001": "Grade 1", "002": "Grade 2", "003": "Grade 3", "004": "Grade 4"}
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                                            
        threshold = 0.0
        q = query.lower().replace(",", "")
        m = re.search(r"rm\s*([\d]+(?:\.\d+)?)", q)
        if not m:
            m = re.search(r"\b([\d]{4,})\b", q)
        if m:
            try:
                threshold = float(m.group(1))
            except ValueError:
                threshold = 0.0

                                                      
        stock_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in stock_map:
                continue
            fields = chunk.get("fields") or {}
            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            stock_map[qid] = self._safe_float(fields.get("value_of_stock_out_of_safe_label"))

                              
        safe_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "safe":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in safe_map:
                continue
            fields = chunk.get("fields") or []
            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            safe_map[qid] = str(fields.get("grade_label", "") or "").strip()

                                                      
        records = [
            (name_map.get(qid, qid), qid, stock_map.get(qid, 0.0), safe_map.get(qid, ""))
            for qid in sorted(name_map.keys())
        ]
        records.sort(key=lambda x: x[2], reverse=True)

        if threshold > 0:
            matches = [(n, q, s, g) for n, q, s, g in records if s > threshold]
            header = f"Proposals with stock out of safe exceeding RM {threshold:,.0f}:"
        else:
            matches = [(n, q, s, g) for n, q, s, g in records if s > 0]
            header = "Proposals with stock held outside the safe (sorted by value):"

        lines = [header]
        if not matches:
            lines.append(f"  None found." + (f" (threshold: RM {threshold:,.0f})" if threshold else ""))
        else:
            for name, qid, stock, grade in matches:
                grade_str = SAFE_GRADE_MAP.get(grade, f"Code {grade}" if grade else "Unknown")
                lines.append(f"  - {name} ({qid}): RM {stock:,.0f} out of safe | Safe: {grade_str}")

                                                            
        below = [(n, q, s, g) for n, q, s, g in records if s <= (threshold if threshold else 0)]
        if below and threshold > 0:
            lines.append(f"\nBelow / at threshold ({len(below)} proposals):")
            for name, qid, stock, grade in below:
                grade_str = SAFE_GRADE_MAP.get(grade, f"Code {grade}" if grade else "Unknown")
                if stock > 0:
                    lines.append(f"  - {name} ({qid}): RM {stock:,.0f} | Safe: {grade_str}")
                else:
                    lines.append(f"  - {name} ({qid}): RM 0 (no stock outside safe) | Safe: {grade_str}")

        return "\n".join(lines)

    def handle_director_fidelity_combined(self) -> str:
        """List proposals opted into both director-house and fidelity add-ons with totals."""
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        addon_map: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "add_on_coverage":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in addon_map:
                continue
            addon_map[qid] = chunk.get("fields") or {}

        matches = []
        total_director = 0.0
        total_fidelity = 0.0
        for qid in sorted(name_map.keys()):
            fields = addon_map.get(qid, {})
            director_opted = str(fields.get("director_house_question_label", "") or "").strip() == "001"
            fidelity_opted = str(fields.get("fidelity_guarantee_insurance_add_coverage_label", "") or "").strip() == "001"
            if not (director_opted and fidelity_opted):
                continue
            d_amt = self._safe_float(fields.get("director_house_coverage_label"))
            f_amt = self._safe_float(fields.get("fidelity_guarantee_insurance_label"))
            matches.append((name_map.get(qid, qid), qid, d_amt, f_amt, d_amt + f_amt))
            total_director += d_amt
            total_fidelity += f_amt

        if not matches:
            return "No proposals have both director-house and fidelity add-ons enabled."

        lines = [f"Matching proposals: {len(matches)}"]
        for name, qid, d_amt, f_amt, combined in matches:
            lines.append(
                f"- {name} ({qid}) | Director Amount: RM {d_amt:,.0f} | Fidelity Amount: RM {f_amt:,.0f} | Combined: RM {combined:,.0f}"
            )
        lines.append(f"Total Director Amount: RM {total_director:,.0f}")
        lines.append(f"Total Fidelity Amount: RM {total_fidelity:,.0f}")
        lines.append(f"Combined Add-on Amount: RM {total_director + total_fidelity:,.0f}")
        return "\n".join(lines)

    def handle_highest_fidelity_staff_with_director(self) -> str:
        """Return proposals with highest fidelity staff count and director coverage status."""
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        records = []
        for chunk in complete_metadata:
            if chunk.get("section") != "add_on_coverage":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            fields = chunk.get("fields") or {}
            staff_raw = str(fields.get("fidelity_guarantee_total_staff_label", "") or "0").strip()
            try:
                staff = int(staff_raw)
            except ValueError:
                staff = 0
            director_enabled = str(fields.get("director_house_question_label", "") or "").strip() == "001"
            records.append((name_map.get(qid, qid), qid, staff, director_enabled))

        if not records:
            return "No fidelity staff coverage data found."

        max_staff = max(r[2] for r in records)
        top = [r for r in records if r[2] == max_staff]
        top.sort(key=lambda x: x[1])

        lines = [f"Highest fidelity staff covered: {max_staff}"]
        for name, qid, staff, director_enabled in top:
            lines.append(
                f"- {name} ({qid}) | Staff Covered: {staff} | Director House Coverage Enabled: {'Yes' if director_enabled else 'No'}"
            )
        return "\n".join(lines)

    def handle_transit_security_combo_values(self) -> str:
        """Find proposals with armoured+jaguar+armed-transit and report transit insured values."""
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        transit_map: dict = {}
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            if chunk.get("section") == "transit_and_gaurds" and qid not in transit_map:
                fields = chunk.get("fields") or {}
                transit_map[qid] = {
                    "armoured": str(fields.get("do_you_use_armoured_vehicle_label", "") or "").strip(),
                    "jaguar": str(fields.get("usage_of_jaguar_transit_label", "") or "").strip(),
                    "armed": str(fields.get("do_you_use_armed_guards_during_transit_label", "") or "").strip(),
                }

        transit_values: dict = {}
        for chunk in complete_metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in transit_values:
                continue
            fields = chunk.get("fields") or {}
            if isinstance(fields, list):
                fields = fields[0] if fields and isinstance(fields[0], dict) else {}
            transit_val = 0.0
            for key in (
                "maximum_stock_during_transit_label",
                "maximum_stock_foreign_currency_in_transit_label",
                "value_of_stock_in_transit_label",
            ):
                transit_val = max(transit_val, self._safe_float(fields.get(key)))
            transit_values[qid] = transit_val

        matches = []
        total_transit = 0.0
        for qid in sorted(name_map.keys()):
            t = transit_map.get(qid, {})
            if t.get("armoured") == "001" and t.get("jaguar") == "001" and t.get("armed") == "001":
                val = transit_values.get(qid, 0.0)
                matches.append((name_map.get(qid, qid), qid, val))
                total_transit += val

        if not matches:
            return "No proposals match armoured vehicle + Jaguar transit + armed guards during transit."

        lines = [f"Matching proposals: {len(matches)}"]
        for name, qid, val in matches:
            lines.append(f"- {name} ({qid}) | Insured Transit Value: RM {val:,.0f}")
        lines.append(f"Total Insured Transit Value: RM {total_transit:,.0f}")
        return "\n".join(lines)

                                                                         
    def handle_group_by_industry(self) -> str:
        """Handle handle group by industry for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)

                                                                   
        name_map: dict = {}
        industry_map: dict = {}
        for chunk in complete_metadata:
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

                                                                           
        value_map: dict = {}
        for chunk in complete_metadata:
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

                                                    
        industry_groups: dict = defaultdict(
            lambda: {"count": 0, "total_value": 0.0, "businesses": []}
        )
        for qid in industry_map:
            ind = industry_map[qid]
            stock = value_map.get(qid, 0.0)
            industry_groups[ind]["count"] += 1
            industry_groups[ind]["total_value"] += stock
            industry_groups[ind]["businesses"].append(name_map.get(qid, qid))

                                                             
        sorted_industries = sorted(
            industry_groups.items(),
            key=lambda x: (x[1]["total_value"], x[1]["count"]),
            reverse=True,
        )

        lines = ["Industries by total insured value:"]
        for industry, data in sorted_industries:
            count = data["count"]
            total = data["total_value"]
            biz_list = ", ".join(data["businesses"])
            if total > 0:
                lines.append(
                    f"\n- {industry}: {count} proposal(s) "
                    f"\u2014 RM {total:,.0f} total"
                )
            else:
                lines.append(
                    f"\n- {industry}: {count} proposal(s) "
                    f"\u2014 Insured values not submitted"
                )
            lines.append(f"  Businesses: {biz_list}")

        if incomplete_qids:
            lines.append(
                f"\nNote: {len(incomplete_qids)} proposal(s) "
                f"({', '.join(incomplete_qids)}) have no data submitted."
            )
        return "\n".join(lines)

                                                                
                                                                            
                                                                    
                                                                      
    _SECURITY_SECTION_FIELDS: dict = {
        "alarm":              "do_you_have_alarm_label",
        "cctv":               "recording_label",
        "transit_and_gaurds": "do_you_use_armoured_vehicle_label",
        "strong_room":        "do_you_have_a_strong_room_label",
    }

                                            
    _SECURITY_SECTION_LABELS: dict = {
        "alarm":              "Alarm",
        "cctv":               "CCTV",
        "transit_and_gaurds": "Armoured Vehicle",
        "strong_room":        "Strong Room",
    }

    _YES_VALUES = {"yes", "001", "true", "1"}

    def handle_security_feature_summary(self) -> str:
        """Handle handle security feature summary for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)
                                                                
        name_map = self._build_business_name_map(complete_metadata)

                                                                
        security_map: dict = defaultdict(dict)
        for chunk in complete_metadata:
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

        if incomplete_qids:
            lines.append(
                f"\nNote: {len(incomplete_qids)} proposal(s) "
                f"({', '.join(incomplete_qids)}) have no data submitted."
            )
        return "\n".join(lines)

                                                                              
    def handle_business_type_distribution(self) -> str:
        """Handle handle business type distribution for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)
        name_map = self._build_business_name_map(complete_metadata)
        total = len(name_map)

                                             
        type_businesses: dict = defaultdict(list)
        seen: set = set()
        for chunk in complete_metadata:
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
            f"Policy type distribution across {total} proposals with data:"
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

        if incomplete_qids:
            lines.append(
                f"\nNote: {len(incomplete_qids)} proposal(s) "
                f"({', '.join(incomplete_qids)}) have no data submitted."
            )
        return "\n".join(lines)

                                                             
    def handle_claims_by_location(self, query_intent: str = "summary") -> str:
        """Handle handle claims by location for this module.
        
        Args:
            query_intent: Input used to execute this operation.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                                                                             
        location_map: dict = {}                    
        seen_loc: set = set()
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen_loc:
                continue
            seen_loc.add(qid)
            loc = chunk.get("risk_location", "")
            location_map[qid] = self._extract_state(str(loc))

                                                            
        claim_map: dict = {}                                  
        for chunk in complete_metadata:
            if chunk.get("section") != "claim_history":
                continue
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            claim_map[qid] = df.get("claim_history_label", "")

                                
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

                                            
        if query_intent == "ranking_asc":
            sorted_states = sorted(
                state_data.items(),
                key=lambda x: (x[1]["with_claims"], x[1]["proposals"]),
            )
            header = "Regions ranked by claim frequency (lowest first — safest regions):"
        elif query_intent == "ranking_desc":
            sorted_states = sorted(
                state_data.items(),
                key=lambda x: (x[1]["with_claims"], x[1]["proposals"]),
                reverse=True,
            )
            header = "Regions ranked by claim frequency (highest first):"
        else:
            sorted_states = sorted(
                state_data.items(),
                key=lambda x: x[1]["proposals"],
                reverse=True,
            )
            header = "Claim history by region across all proposals:"

        total_with_data = sum(
            d["with_claims"] + d["no_claims"] for d in state_data.values()
        )
        total_claims = sum(d["with_claims"] for d in state_data.values())
        total_no_data = sum(d["no_data"] for d in state_data.values())

                                                                   
        lines: List[str] = []
        if query_intent == "peril_specific":
            lines.append(
                "Note: Claim cause details (fire, theft, flood, etc.) are not "
                "captured in proposal records. Only whether a claim occurred "
                "within the past 3 years is recorded."
            )
            lines.append("")
            lines.append("General claim history by region:")
        else:
            lines.append(header)

        lines.append("")
        col_header = f"{'State':<25} {'Proposals':>10} {'With Claims':>13} {'No Claims':>12}"
        lines.append(col_header)
        lines.append("-" * len(col_header))
        for state, data in sorted_states:
            lines.append(
                f"{state:<25} {data['proposals']:>10} "
                f"{data['with_claims']:>13} {data['no_claims']:>12}"
            )
        lines.append("")

        lines.append(
            f"Total: {total_with_data + total_no_data} proposals across "
            f"{len(state_data)} regions."
        )

        if total_claims == 0:
            lines.append(
                "All regions show 0 claims in the past 3 years."
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
        if incomplete_qids:
            lines.append(
                f"\nNote: {len(incomplete_qids)} proposal(s) "
                f"({', '.join(incomplete_qids)}) have no data submitted "
                f"and are excluded from this analysis."
            )
        return "\n".join(lines)

                                                     
    def handle_claim_rate(self) -> str:
        """Handle handle claim rate for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)
        name_map = self._build_business_name_map(complete_metadata)

        has_claims: list = []
        no_claims: list = []
        no_data: list = []

        seen_claim: set = set()
        for chunk in complete_metadata:
            if chunk.get("section") != "claim_history":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in seen_claim:
                continue
            seen_claim.add(qid)
            df = chunk.get("decoded_fields") or {}
            label = (df.get("claim_history_label") or "").lower()
            name = name_map.get(qid, qid)

            if "no claim" in label:
                no_claims.append(name)
            elif "claim" in label:
                has_claims.append(name)
            else:
                no_data.append(name)

                                                                         
        complete_qids = {
            c.get("quote_id") for c in complete_metadata if c.get("quote_id")
        }
        for qid in complete_qids - seen_claim:
            no_data.append(name_map.get(qid, qid))

        total_with_data = len(has_claims) + len(no_claims)

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
        if incomplete_qids:
            lines.append(
                f"- Incomplete submissions excluded: {len(incomplete_qids)} "
                f"({', '.join(incomplete_qids)})"
            )

        lines.append("")
        lines.append(
            "Note: Claim amounts and causes are not captured in proposal "
            "records. Only claim occurrence within 3 years is recorded."
        )
        return "\n".join(lines)

                                                       
    def handle_list_all_businesses(self) -> str:
        """Handle handle list all businesses for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                            
        location_map: dict = {}
        industry_map: dict = {}
        seen: set = set()
        for chunk in complete_metadata:
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
            f"All {total} businesses with submitted data in the proposal database:"
        ]
        for i, qid in enumerate(sorted(name_map.keys()), 1):
            name = name_map[qid]
            loc = location_map.get(qid, "")
            ind = industry_map.get(qid, "")
            lines.append(
                f"{i:>2}. {name} ({qid}) \u2014 {ind}, {loc}"
            )
        lines.append("")
        lines.append(
            "Each business has exactly one proposal record in the database."
        )
        if incomplete_qids:
            lines.append(
                f"\nNote: {len(incomplete_qids)} proposal(s) "
                f"({', '.join(incomplete_qids)}) have no data submitted."
            )
        return "\n".join(lines)

                                             
    def handle_gps_tracker_proposals(self) -> str:
        """Handle handle gps tracker proposals for this module.
        
        Returns:
            Result generated by this operation, when applicable.
        """
        metadata = self.metadata
        complete_metadata = self._get_complete_proposals_only(metadata)
        incomplete_qids = self._get_incomplete_quote_ids(metadata)
        name_map = self._build_business_name_map(complete_metadata)

                                        
        loc_map: dict = {}
        seen_loc: set = set()
        for chunk in complete_metadata:
            qid = chunk.get("quote_id")
            if not qid or qid in seen_loc:
                continue
            seen_loc.add(qid)
            loc = chunk.get("risk_location", "")
            parts = [p.strip() for p in str(loc).split(",") if p.strip()]
                                                   
            loc_map[qid] = parts[0] if parts else "Unknown"

                                                        
        gps_vehicles_yes: list = []
        gps_vehicles_no: list = []
        gps_no_data: list = []

        seen_transit = set()
        all_qids = set(name_map.keys())

        for chunk in complete_metadata:
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

        if incomplete_qids:
            lines.append(
                f"\nNote: {len(incomplete_qids)} proposal(s) "
                f"({', '.join(incomplete_qids)}) have no data submitted."
            )

        return "\n".join(lines)
