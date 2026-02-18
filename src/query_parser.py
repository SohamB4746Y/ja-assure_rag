"""
LLM-Assisted Query Parser

This module uses the LLM to understand natural language queries and convert them
into structured queries that can be processed deterministically.

Architecture:
1. LLM parses the query to extract intent, fields, filters
2. Deterministic code looks up exact data (no hallucination)
3. LLM formats the final answer naturally
"""
from __future__ import annotations

import json
import re
from typing import Optional
from dataclasses import dataclass

from src.llm_client import LLMClient


# All available fields in the data (for the LLM to map to)
AVAILABLE_FIELDS = """
BUSINESS INFO:
- business_name_label: Name of the business
- nature_of_business_label: Type of business (Pawnbroker, Money Changer, etc.)
- businesstype_id_label: Business type ID code
- industry_id_label: Industry ID code
- business_registration_label: Registration number
- person_in_charge_label: Contact person name
- mobile_number_label: Mobile phone number
- office_telephone_label: Office phone
- correspondence_email_label: Email address
- mailing_address_label: Mailing address
- risk_location: Risk/proposal location (city, state, country) - this is a TOP-LEVEL field, not inside fields dict
- user_name: Person/director name associated with the proposal - this is a TOP-LEVEL field

PROPERTY & PREMISES:
- property_label: Property details
- premise_type_label: Type of premises (001=Office building, 002=Shopping centre, 003=Shop house, 004=Others)
- premise_type_others_label: Other premise type details
- floor_materials_label: Floor material (001=Concrete, 002=Tiled, 003=Metal, 004=Wood)
- wall_materials_label: Wall material (001=Concrete, 002=Tiled, 003=Metal, 004=Wood)
- roof_materials_label: Roof material (001=Concrete, 002=Tiled, 003=Metal, 004=Wood)

SECURITY - ALARMS:
- do_you_have_alarm_label: Has alarm system (001=Yes, 002=No)
- type_of_alarm_system_label: Type of alarm (001-010 codes)
- alarm_brand_name_label: Alarm brand
- alarm_model_label: Alarm model
- under_maintenance_contract_label: Alarm under maintenance (001=Yes, 002=No)
- central_monitoring_stations_label: Has central monitoring (001=Yes, 002=No)
- connection_type_label: Alarm connection type
- name_of_cms_company_label: CMS company name

SECURITY - CCTV:
- recording_label: Has CCTV recording (001=Yes, 002=No)
- cctv_maintenance_contract_label: CCTV under maintenance contract (001=Yes, 002=No)
- type_of_back_up_label: Backup type (001-006 codes)
- additional_capability_label: Additional CCTV capability
- retained_period_of_cctv_recording_label: How long CCTV is retained

SECURITY - GUARDS & TRANSIT:
- do_you_use_guards_at_premise_label: Uses guards at premises (001=Yes, 002=No)
- do_you_use_armed_guards_during_transit_label: Uses armed guards (001=Yes, 002=No)
- do_you_use_armoured_vehicle_label: Uses armoured vehicle (001=Yes, 002=No)
- installed_gps_tracker_in_transit_vehicles_label: GPS in vehicles (001=Yes, 002=No)
- installed_gps_tracker_in_transit_bags_label: GPS in bags (001=Yes, 002=No)
- usage_of_jaguar_transit_label: Uses Jaguar transit service (001=Yes, 002=No)

SECURITY - SAFE & STRONG ROOM:
- do_you_have_a_strong_room_label: Has strong room (001=Yes, 002=No)
- time_locking_label: Has time lock (001=Yes, 002=No)
- time_locking_brand_label: Time lock brand
- safe_model_label: Safe model
- safe_brand_name_label: Safe brand
- safe_weight_label: Safe weight
- grade_label: Safe grade (001-008 codes)
- certified_label: Safe certified (001=Yes, 002=No)
- key_combination_code_or_both_label: Key/Combination/Both (001-003)

SECURITY - DOORS:
- door_access_label: Door access type (001-006 codes)
- main_door_details_label: Main door material (001-004 codes)
- inner_door_details_label: Inner door material
- rear_door_label: Rear door type (001-003 codes)

SECURITY - SHOWCASES & WINDOWS:
- do_you_have_counter_showcase_label: Has counter showcase (001=Yes, 002=No)
- counter_showcase_thickness_label: Showcase glass thickness
- do_you_have_display_window_label: Has display window (001=Yes, 002=No)
- display_window_thickness_label: Display window thickness
- do_you_have_wall_showcase_label: Has wall showcase (001=Yes, 002=No)
- wall_showcase_thickness_label: Wall showcase thickness

VALUES & STOCK:
- maximum_stock_in_premises_label: Max stock value in premises
- value_of_stock_out_of_safe_label: Stock value outside safe
- maximum_stock_during_transit_label: Max stock in transit
- value_of_cash_in_premise_label: Cash in premises
- value_of_pledged_stock_in_premise_label: Pledged stock value
- value_of_non_pledged_stock_in_premise_label: Non-pledged stock value
- maximum_stock_foreign_currency_in_premise_label: Foreign currency in premises
- sum_assured_limit_label: Sum assured / coverage limit

CLAIMS & LOSSES:
- claim_history_label: Claims history status (001=No claim within 3 years, 002=Claims within past 3 years)
- description_label: Claim description
- year_of_claim_label: Year of claim
- amount_of_claim_label: Amount of claim

SHOPLIFTING:
- shop_lifting_label: Has shoplifting coverage/cases (1=Yes, 2=No)
  IMPORTANT: For "shoplifting cases" questions, use this field with filter_value="1" for Yes

EMPLOYEES:
- background_checks_for_all_employees_label: Does background checks (001-004 codes)
- fidelity_guarantee_insurance_label: Has fidelity insurance
- fidelity_guarantee_total_staff_label: Total staff covered

PROCEDURES:
- standard_operating_procedure_label: Has SOP (001=Yes, 002=No)
- do_you_keep_detailed_records_of_stock_movements_label: Keeps stock records (001=Yes, 002=No)
- how_often_is_the_stock_check_carried_out_label: Stock check frequency (001-005 codes)
- records_maintained_in_label: How records are maintained (001=Online, 002=Offline)
- the_nearest_police_station_label: Nearest police station distance (001-005 codes)

ADD-ON COVERAGE:
- director_house_coverage_label: Director house coverage details
- director_house_question_label: Director house question (001=Yes, 002=No)
- overseas_carrying_label: Overseas carrying coverage
- public_exhibitions_label: Public exhibitions coverage
"""

QUERY_PARSE_PROMPT = """You are a query parser for an insurance proposal database. Parse the user's question and extract structured information.

AVAILABLE FIELDS IN DATABASE:
{fields}

{history_section}
CURRENT USER QUESTION: {query}

Parse this question and output ONLY a JSON object with these fields:
{{
    "intent": "ONE of: count, list, lookup, compare",
    "target_fields": ["field1_label", "field2_label"],
    "filter_field": "field_name_label or null",
    "filter_value": "the coded value to filter on, or null",
    "filter_contains": "text to search for in field value or null",
    "quote_id": "MYJADEQTXXX or null",
    "output_fields": ["field1_label", "field2_label"],
    "understood_question": "brief restatement of what user is asking"
}}

PARSING RULES:
1. "intent" MUST be exactly ONE word from: count, list, lookup, compare. Never combine them.
2. For "how many" / "count" questions → intent = "count"
   EXCEPTION: "how much", "how often", "how long" for a SPECIFIC person/business → intent = "lookup" (these ask for a field VALUE, not a count)
3. For "list all", "what are", "show", "which", "give names" → intent = "list"
4. If asking "how many" AND also asking for names in the same sentence → intent = "count" (names will be added automatically)
5. For specific quote questions → intent = "lookup"
6. For "highest", "lowest" → intent = "compare"
7. Map natural language to exact field names from the list above
8. For claims/losses questions, use "claim_history_label" and filter_contains
9. For Yes/No fields coded as 001/002: filter_value should be the CODE ("001" for Yes, "002" for No)
   For shop_lifting_label coded as 1/2: filter_value="1" for Yes, filter_value="2" for No
10. output_fields = what fields to show in the answer
11. CRITICAL: If there is CONVERSATION HISTORY above, use it to resolve references like "these", "those", "them", "the above", "their names", etc. The follow-up query MUST inherit the same filter_field and filter_value from the previous query context.
12. Pay close attention to NEGATION words: "don't have", "without", "no", "not" → these flip the filter value to the opposite.

EXAMPLES:
- "How many have CCTV maintenance?" → {{"intent": "count", "target_fields": ["cctv_maintenance_contract_label"], "filter_field": "cctv_maintenance_contract_label", "filter_value": "001", "output_fields": ["business_name_label"], "understood_question": "Count proposals with CCTV maintenance (=Yes/001)"}}
- "How many businesses have shoplifting cases?" → {{"intent": "count", "target_fields": ["shop_lifting_label"], "filter_field": "shop_lifting_label", "filter_value": "1", "output_fields": ["business_name_label"], "understood_question": "Count proposals with shoplifting (shop_lifting_label=1)"}}
- "How many businesses don't have shoplifting cases?" → {{"intent": "count", "target_fields": ["shop_lifting_label"], "filter_field": "shop_lifting_label", "filter_value": "2", "output_fields": ["business_name_label"], "understood_question": "Count proposals WITHOUT shoplifting (shop_lifting_label=2)"}}
- "Which businesses have shoplifting?" → {{"intent": "list", "target_fields": ["shop_lifting_label"], "filter_field": "shop_lifting_label", "filter_value": "1", "output_fields": ["business_name_label"], "understood_question": "List proposals with shoplifting coverage"}}
- "How many have alarms?" → {{"intent": "count", "target_fields": ["do_you_have_alarm_label"], "filter_field": "do_you_have_alarm_label", "filter_value": "001", "output_fields": ["business_name_label"], "understood_question": "Count proposals with alarms (=Yes/001)"}}
- "How many don't have alarms?" → {{"intent": "count", "target_fields": ["do_you_have_alarm_label"], "filter_field": "do_you_have_alarm_label", "filter_value": "002", "output_fields": ["business_name_label"], "understood_question": "Count proposals WITHOUT alarms (=No/002)"}}
- "What is the business name of MYJADEQT001?" → {{"intent": "lookup", "quote_id": "MYJADEQT001", "output_fields": ["business_name_label"], "understood_question": "Get business name for MYJADEQT001"}}
- "How many proposals are in shopping centres?" → {{"intent": "count", "target_fields": ["premise_type_label"], "filter_field": "premise_type_label", "filter_value": "002", "output_fields": ["business_name_label"], "understood_question": "Count proposals in shopping centre premises (premise_type_label=002)"}}
- "How many proposals are located in Johor Bahru?" → {{"intent": "count", "target_fields": ["risk_location"], "filter_field": null, "filter_value": null, "filter_contains": "Johor Bahru", "output_fields": ["business_name_label"], "understood_question": "Count proposals located in Johor Bahru"}}
- "Which businesses are in Kuala Lumpur?" → {{"intent": "list", "target_fields": ["risk_location"], "filter_field": null, "filter_value": null, "filter_contains": "Kuala Lumpur", "output_fields": ["business_name_label"], "understood_question": "List proposals in Kuala Lumpur"}}
- "What is the house coverage for Suresh Kumar?" → {{"intent": "lookup", "target_fields": ["director_house_coverage_label"], "filter_field": null, "filter_value": null, "filter_contains": "Suresh Kumar", "output_fields": ["director_house_coverage_label"], "understood_question": "Get director house coverage for person named Suresh Kumar"}}
- "What type of business does City FX Exchange have?" → {{"intent": "lookup", "target_fields": ["nature_of_business_label"], "filter_field": null, "filter_value": null, "filter_contains": "City FX Exchange", "output_fields": ["nature_of_business_label"], "understood_question": "Get business type for City FX Exchange"}}
- "Does Mehta Pawn Services have a strong room?" → {{"intent": "lookup", "target_fields": ["do_you_have_a_strong_room_label"], "filter_field": null, "filter_value": null, "filter_contains": "Mehta Pawn Services", "output_fields": ["do_you_have_a_strong_room_label"], "understood_question": "Check if Mehta Pawn Services has a strong room"}}
- "What is the alarm brand for MYJADEQT003?" → {{"intent": "lookup", "quote_id": "MYJADEQT003", "output_fields": ["alarm_brand_name_label"], "understood_question": "Get alarm brand for MYJADEQT003"}}
- "How often is the stock check carried out for Suresh Kumar?" → {{"intent": "lookup", "target_fields": ["how_often_is_the_stock_check_carried_out_label"], "filter_field": null, "filter_value": null, "filter_contains": "Suresh Kumar", "output_fields": ["how_often_is_the_stock_check_carried_out_label"], "understood_question": "Get stock check frequency for Suresh Kumar"}}
- "How much cash does Heritage Gold & Jewels keep in premise?" → {{"intent": "lookup", "target_fields": ["value_of_cash_in_premise_label"], "filter_field": null, "filter_value": null, "filter_contains": "Heritage Gold & Jewels", "output_fields": ["value_of_cash_in_premise_label"], "understood_question": "Get cash in premise value for Heritage Gold & Jewels"}}

IMPORTANT REMINDERS:
- intent must be EXACTLY one of: count, list, lookup, compare. NEVER output "count|list" or any combined form.
- For shoplifting: filter_value="1" means HAS shoplifting, filter_value="2" means DOES NOT have shoplifting.
- For 001/002 coded fields: "001" = Yes, "002" = No.
- NEGATION flips the value: "don't have X" / "without X" / "no X" means filter on the NO/negative code.
- For LOCATION/ADDRESS queries ("in Johor Bahru", "located in KL"), use filter_contains with the location name. Do NOT use filter_value for locations.
- For TEXT SEARCH queries (searching by name, address, company), use filter_contains for substring matching.
- ENTITY LOOKUP: When asking "what is FIELD for PERSON/BUSINESS?", put the PERSON/BUSINESS name in filter_contains, put the FIELD in output_fields. Do NOT put the field in filter_field unless you are filtering BY that field's value.
- filter_field + filter_value are for filtering rows (e.g., alarm=001 means Yes). Do NOT use filter_field when filter_value is null.
- When the user asks about a specific PERSON or BUSINESS NAME (not a quote ID), use filter_contains with that name and intent="lookup".
- "how often", "how much", "how long" + a PERSON/BUSINESS name = intent "lookup" (NOT "count"). These ask for a specific field VALUE for a named entity.

Output ONLY the JSON, no explanation."""


@dataclass
class ParsedQuery:
    """Structured representation of a parsed query."""
    intent: str  # count, list, lookup, compare, aggregate
    target_fields: list[str]
    filter_field: Optional[str]
    filter_value: Optional[str]
    filter_contains: Optional[str]
    quote_id: Optional[str]
    output_fields: list[str]
    understood_question: str
    raw_query: str
    parse_success: bool


class QueryParser:
    """
    LLM-assisted query parser that converts natural language to structured queries.
    """
    
    def __init__(self, llm: LLMClient):
        """
        Initialize the query parser.
        
        Args:
            llm: LLM client for parsing
        """
        self.llm = llm
        self.conversation_history: list[dict] = []
    
    def add_to_history(self, query: str, parsed: ParsedQuery, answer: str) -> None:
        """
        Add a query-answer pair to conversation history.
        
        Args:
            query: The user's original question
            parsed: The parsed query result
            answer: The answer that was given
        """
        self.conversation_history.append({
            "query": query,
            "intent": parsed.intent,
            "filter_field": parsed.filter_field,
            "filter_value": parsed.filter_value,
            "filter_contains": parsed.filter_contains,
            "target_fields": parsed.target_fields,
            "output_fields": parsed.output_fields,
            "understood_question": parsed.understood_question,
            "answer_preview": answer[:200]
        })
        # Keep only last 5 turns
        if len(self.conversation_history) > 5:
            self.conversation_history = self.conversation_history[-5:]
    
    def add_raw_to_history(self, query: str, answer: str) -> None:
        """
        Add a query-answer pair to history when no ParsedQuery is available.
        Used for predefined QA, analytical, semantic RAG, and other fallback paths.
        
        Args:
            query: The user's original question
            answer: The answer that was given
        """
        self.conversation_history.append({
            "query": query,
            "intent": "unknown",
            "filter_field": None,
            "filter_value": None,
            "filter_contains": None,
            "target_fields": [],
            "output_fields": [],
            "understood_question": query,
            "answer_preview": answer[:200]
        })
        # Keep only last 5 turns
        if len(self.conversation_history) > 5:
            self.conversation_history = self.conversation_history[-5:]
    
    def _build_history_section(self) -> str:
        """
        Build a conversation history section for the prompt.
        
        Returns:
            History text or empty string
        """
        if not self.conversation_history:
            return ""
        
        lines = ["CONVERSATION HISTORY (most recent turn is the most relevant for follow-up references):"]
        for i, turn in enumerate(self.conversation_history, 1):
            lines.append(f"Turn {i}:")
            lines.append(f"  User asked: {turn['query']}")
            lines.append(f"  Understood as: {turn['understood_question']}")
            if turn.get('filter_field') or turn.get('filter_contains'):
                lines.append(f"  Intent: {turn['intent']}, Filter: {turn['filter_field']}={turn['filter_value']}, Contains: {turn['filter_contains']}")
            lines.append(f"  Answer given: {turn['answer_preview']}")
        
        # Emphasize the LAST turn for follow-up resolution
        last = self.conversation_history[-1]
        lines.append("")
        lines.append("=== MOST RECENT TURN (use this for follow-up references like 'their', 'these', 'those', 'them', 'the names') ===")
        lines.append(f"  Last question: {last['query']}")
        lines.append(f"  Last answer: {last['answer_preview']}")
        if last.get('filter_field'):
            lines.append(f"  Last filter: {last['filter_field']}={last['filter_value']}")
        if last.get('filter_contains'):
            lines.append(f"  Last contains search: {last['filter_contains']}")
        lines.append("")
        lines.append("CRITICAL RULE FOR FOLLOW-UPS: When the user says 'their names', 'give names', 'list them', 'what are they', etc.,")
        lines.append("you MUST use the EXACT SAME filter_field, filter_value, and filter_contains from the MOST RECENT turn above.")
        lines.append("Change intent to 'list' and set output_fields=['business_name_label'].")
        lines.append("")
        return "\n".join(lines)
    
    def _is_followup_reference(self, query: str) -> bool:
        """
        Detect if a query is a simple follow-up reference to previous results.
        Examples: 'give me their names', 'what are they?', 'list them', 'show the names'
        """
        if not self.conversation_history:
            return False
        
        query_lower = query.lower().strip()
        
        # Short queries with reference words are almost always follow-ups
        followup_patterns = [
            "their names", "the names", "give names", "give me names",
            "list them", "show them", "what are they", "who are they",
            "give me their", "show their", "tell me their",
            "what are those", "which are those", "name them",
            "give the names", "show the names", "list the names",
            "what about their names", "and their names",
            "names please", "names?",
        ]
        
        for pattern in followup_patterns:
            if pattern in query_lower:
                return True
        
        # Very short queries that are purely referential
        if len(query_lower.split()) <= 5 and any(w in query_lower for w in ["them", "their", "those", "these", "above", "names"]):
            return True
        
        return False
    
    def _resolve_followup(self, query: str) -> ParsedQuery:
        """
        Deterministically resolve a follow-up query using the last conversation turn.
        No LLM involved - guaranteed to use correct context.
        """
        last = self.conversation_history[-1]
        
        return ParsedQuery(
            intent="list",
            target_fields=last.get("target_fields", []),
            filter_field=last.get("filter_field"),
            filter_value=last.get("filter_value"),
            filter_contains=last.get("filter_contains"),
            quote_id=None,
            output_fields=["business_name_label"],
            understood_question=f"Follow-up: list names from previous query '{last['query']}'",
            raw_query=query,
            parse_success=True
        )
    
    def parse(self, query: str) -> ParsedQuery:
        """
        Parse a natural language query into a structured format.
        
        Args:
            query: User's natural language question
            
        Returns:
            ParsedQuery object with extracted information
        """
        # DETERMINISTIC follow-up detection (runs BEFORE LLM)
        if self._is_followup_reference(query):
            return self._resolve_followup(query)
        
        history_section = self._build_history_section()
        prompt = QUERY_PARSE_PROMPT.format(
            fields=AVAILABLE_FIELDS,
            query=query,
            history_section=history_section
        )
        
        try:
            response = self.llm.generate(prompt)
            
            # Extract JSON from response
            json_match = re.search(r'\{[\s\S]*\}', response)
            if not json_match:
                return self._fallback_parse(query)
            
            parsed = json.loads(json_match.group())
            
            # Normalize intent: extract single primary intent
            raw_intent = parsed.get("intent", "lookup").lower().strip()
            intent = self._normalize_intent(raw_intent, query)

            return ParsedQuery(
                intent=intent,
                target_fields=parsed.get("target_fields", []),
                filter_field=parsed.get("filter_field"),
                filter_value=str(parsed.get("filter_value")) if parsed.get("filter_value") is not None else None,
                filter_contains=parsed.get("filter_contains"),
                quote_id=parsed.get("quote_id"),
                output_fields=parsed.get("output_fields", []),
                understood_question=parsed.get("understood_question", query),
                raw_query=query,
                parse_success=True
            )
            
        except Exception as e:
            return self._fallback_parse(query)
    
    @staticmethod
    def _normalize_intent(raw_intent: str, query: str) -> str:
        """
        Normalize LLM intent output to a single valid intent.
        Handles cases like 'count|list', 'count/list', 'count, list', etc.
        """
        query_lower = query.lower()
        valid_intents = ["count", "list", "lookup", "compare"]

        # If already a valid single intent, return it
        if raw_intent in valid_intents:
            return raw_intent

        # Split on common delimiters
        parts = re.split(r'[|/,\s]+', raw_intent)
        found = [p.strip() for p in parts if p.strip() in valid_intents]

        if not found:
            # Infer from query keywords
            if any(w in query_lower for w in ["how many", "count", "number of", "total"]):
                return "count"
            elif any(w in query_lower for w in ["list", "show", "which", "what are", "give", "name"]):
                return "list"
            elif any(w in query_lower for w in ["highest", "lowest", "maximum", "minimum"]):
                return "compare"
            return "lookup"

        # If multiple found, prioritize based on query
        if "count" in found and any(w in query_lower for w in ["how many", "count", "number of", "total"]):
            return "count"
        if "list" in found and any(w in query_lower for w in ["list", "show", "which", "what are", "names"]):
            return "list"
        
        # Default to first found
        return found[0]

    def _fallback_parse(self, query: str) -> ParsedQuery:
        """
        Fallback parsing using simple heuristics.
        
        Args:
            query: User's question
            
        Returns:
            ParsedQuery with basic extraction
        """
        query_lower = query.lower()
        
        # Extract quote ID
        quote_match = re.search(r'MYJADEQT\d+', query, re.IGNORECASE)
        quote_id = quote_match.group().upper() if quote_match else None
        
        # Determine intent
        if any(w in query_lower for w in ["how many", "count", "number of"]):
            intent = "count"
        elif any(w in query_lower for w in ["list", "show", "what are", "which"]):
            intent = "list"
        elif any(w in query_lower for w in ["highest", "lowest", "maximum", "minimum"]):
            intent = "compare"
        else:
            intent = "lookup"
        
        return ParsedQuery(
            intent=intent,
            target_fields=[],
            filter_field=None,
            filter_value=None,
            filter_contains=None,
            quote_id=quote_id,
            output_fields=[],
            understood_question=query,
            raw_query=query,
            parse_success=False
        )
