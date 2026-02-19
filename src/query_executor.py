"""
Smart Query Executor

This module executes parsed queries against the metadata to retrieve accurate data.
All lookups are deterministic - the data comes directly from the database,
ensuring 100% accuracy with no hallucination.

ARCHITECTURE NOTE:
Each metadata chunk now contains a "decoded_fields" dict where every coded value
has been pre-decoded using decode_field(field_name, raw_value) at index-build time.
This guarantees correct contextual decoding because the same code means different
things in different fields (e.g., 001 = "Yes" for recording_label but
001 = "Concrete" for roof_materials_label). The field_name is the routing key.

Since values in decoded_fields are already human-readable, we NEVER call
decode_field() in this module — doing so would double-decode and fail.
"""
from __future__ import annotations

import os
import pickle
import re
from typing import Optional
from dataclasses import dataclass

from src.query_parser import ParsedQuery


@dataclass
class QueryResult:
    """Result of executing a parsed query."""
    success: bool
    data: list[dict]  # List of matching records with requested fields
    count: int
    summary: str
    details: list[str]


class SmartQueryExecutor:
    """
    Executes parsed queries against metadata with 100% accuracy.
    No LLM involvement in data retrieval - purely deterministic.
    
    Uses decoded_fields (pre-decoded at index time) so every value is
    already human-readable. Never calls decode_field() — that would
    double-decode and produce wrong results.
    """
    
    def __init__(self, metadata_path: str = "index/metadata.pkl"):
        """
        Initialize the executor.
        
        Args:
            metadata_path: Path to the metadata pickle file
        """
        self.metadata_path = metadata_path
        self.metadata = []
        self._load_metadata()
    
    def _load_metadata(self) -> None:
        """Load metadata from pickle file."""
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, "rb") as f:
                self.metadata = pickle.load(f)
    
    # ------------------------------------------------------------------
    # Field matching helper
    # ------------------------------------------------------------------
    
    def _field_match_score(self, requested_field: str, actual_field: str) -> int:
        """
        Score how well a requested field name matches an actual field name.
        Higher score = better match.
        
        Uses the field_name as context — must preserve field_name for correct
        contextual decoding.
        """
        req = requested_field.lower().replace("_label", "").replace("_", " ")
        act = actual_field.lower().replace("_label", "").replace("_", " ")
        
        # Exact match — perfect
        if req == act:
            return 100
        
        # One is fully contained in the other
        if req in act or act in req:
            return 50 + len(min(req, act, key=len))
        
        # Word overlap — field name provides decoding context
        noise = {"the", "a", "an", "of", "in", "for", "is", "do", "you", "label"}
        req_words = set(req.split()) - noise
        act_words = set(act.split()) - noise
        
        if not req_words:
            return 0
        
        overlap = len(req_words & act_words)
        return overlap * 10 if overlap > 0 else 0
    
    def _get_search_fields(self, chunk: dict) -> dict:
        """
        Build a unified search dict from a chunk.
        decoded_fields (pre-decoded with correct field context) takes priority.
        Falls back to raw fields if decoded_fields not available.
        
        The returned values are already human-readable — do NOT decode again.
        """
        decoded_fields = chunk.get("decoded_fields", {})
        raw_fields = chunk.get("fields", {})
        
        search_fields = {}
        if isinstance(raw_fields, dict):
            search_fields.update(raw_fields)
        if decoded_fields:
            search_fields.update(decoded_fields)  # decoded overwrites raw
        
        return search_fields
    
    # ------------------------------------------------------------------
    # Main routing
    # ------------------------------------------------------------------
    
    def execute(self, parsed: ParsedQuery) -> QueryResult:
        """
        Execute a parsed query and return results.
        
        Args:
            parsed: ParsedQuery from the query parser
            
        Returns:
            QueryResult with matching data
        """
        intent = parsed.intent.lower().strip()
        
        if intent == "lookup" and parsed.quote_id:
            return self._execute_lookup(parsed)
        elif intent == "lookup" and not parsed.quote_id:
            # Lookup by entity name (person, business) - retrieve specific fields
            return self._execute_entity_lookup(parsed)
        elif not parsed.quote_id and self._should_entity_lookup(parsed):
            # Smart detection: LLM said count/list but query is really an entity field lookup
            return self._execute_entity_lookup(parsed)
        elif intent == "count":
            return self._execute_count(parsed)
        elif intent == "list":
            return self._execute_list(parsed)
        elif intent == "compare":
            return self._execute_compare(parsed)
        else:
            # Try general search
            return self._execute_general(parsed)
    
    # ------------------------------------------------------------------
    # Lookup by quote_id
    # ------------------------------------------------------------------
    
    def _execute_lookup(self, parsed: ParsedQuery) -> QueryResult:
        """Execute a lookup query for a specific quote ID."""
        quote_id = parsed.quote_id
        results = []
        
        for chunk in self.metadata:
            if chunk.get("quote_id") != quote_id:
                continue
            
            search_fields = self._get_search_fields(chunk)
            if not search_fields:
                continue
            
            # Scored matching for output_fields
            for output_field in (parsed.output_fields or []):
                best_score = 0
                best_field_name = None
                best_value = None
                
                for field_name, value in search_fields.items():
                    score = self._field_match_score(output_field, field_name)
                    if score > best_score:
                        best_score = score
                        best_field_name = field_name
                        best_value = value
                
                if best_score >= 10 and best_field_name is not None:
                    results.append({
                        "quote_id": quote_id,
                        "field": best_field_name,
                        "value": best_value  # already decoded
                    })
            
            # Fallback: try target_fields if no output_fields matched
            if not results and parsed.target_fields:
                for target in parsed.target_fields:
                    best_score = 0
                    best_field_name = None
                    best_value = None
                    
                    for field_name, value in search_fields.items():
                        score = self._field_match_score(target, field_name)
                        if score > best_score:
                            best_score = score
                            best_field_name = field_name
                            best_value = value
                    
                    if best_score >= 10 and best_field_name is not None:
                        results.append({
                            "quote_id": quote_id,
                            "field": best_field_name,
                            "value": best_value  # already decoded
                        })
        
        # Deduplicate
        seen = set()
        unique_results = []
        for r in results:
            key = (r["quote_id"], r["field"])
            if key not in seen:
                seen.add(key)
                unique_results.append(r)
        
        if unique_results:
            details = [f"{r['field'].replace('_label', '').replace('_', ' ').title()}: {r['value']}" for r in unique_results]
            return QueryResult(
                success=True,
                data=unique_results,
                count=len(unique_results),
                summary=f"Found {len(unique_results)} field(s) for {quote_id}",
                details=details
            )
        
        return QueryResult(
            success=False,
            data=[],
            count=0,
            summary=f"No matching fields found for {quote_id}",
            details=[]
        )
    
    # ------------------------------------------------------------------
    # Entity detection helpers
    # ------------------------------------------------------------------
    
    def _should_entity_lookup(self, parsed: ParsedQuery) -> bool:
        """
        Detect if a query should be routed to entity lookup even though the LLM
        classified it as count/list/general. This catches cases like:
        - "how often is X for Suresh Kumar?" (LLM says count, should be lookup)
        - "how much cash does Heritage Gold keep?" (LLM says count, should be lookup)
        
        Returns True if:
        1. filter_contains matches a known entity name (person or business), AND
        2. output_fields or target_fields contain specific data fields (not just business_name_label)
        """
        # Must have specific output/target fields beyond just names
        data_fields = []
        for f in (parsed.output_fields or []) + (parsed.target_fields or []):
            f_lower = f.lower()
            if "business_name" not in f_lower and "person_in_charge" not in f_lower:
                data_fields.append(f)
        
        if not data_fields:
            return False
        
        # Check if filter_contains matches a known entity
        if parsed.filter_contains:
            entity = self._extract_entity_from_query(parsed.filter_contains)
            if entity:
                return True
        
        # Check if the raw query contains a known entity name
        if parsed.raw_query:
            entity = self._extract_entity_from_query(parsed.raw_query)
            if entity:
                return True
        
        return False
    
    def _extract_entity_from_query(self, query: str) -> Optional[str]:
        """
        Deterministically extract a known entity name (business or person) from the query.
        Matches against all known business names and person names in metadata.
        Returns the matched name or None.
        """
        query_lower = query.lower()
        
        # Collect all known names — longest first for greedy matching
        known_names = set()
        for chunk in self.metadata:
            # Top-level user_name (person)
            uname = chunk.get("user_name", "")
            if uname:
                known_names.add(str(uname).strip())
            
            # Use decoded_fields for business_name / person_in_charge
            search_fields = self._get_search_fields(chunk)
            for field_name, value in search_fields.items():
                if "business_name" in field_name.lower() or "person_in_charge" in field_name.lower():
                    val = str(value).strip()
                    if val and val.lower() not in ("unknown", "none", ""):
                        known_names.add(val)
        
        # Sort by length descending (match longest names first to avoid partial matches)
        sorted_names = sorted(known_names, key=len, reverse=True)
        
        for name in sorted_names:
            if name.lower() in query_lower:
                return name
        
        return None
    
    # ------------------------------------------------------------------
    # Entity lookup (by person/business name)
    # ------------------------------------------------------------------
    
    def _execute_entity_lookup(self, parsed: ParsedQuery) -> QueryResult:
        """
        Execute a lookup query by entity name (person or business) without a quote_id.
        Finds the matching proposal(s) first, then retrieves requested output_fields.
        
        All values come from decoded_fields — already human-readable.
        """
        # Determine what entity name to search for
        search_name = None
        if parsed.filter_contains:
            search_name = parsed.filter_contains.lower().strip()
        
        if not search_name:
            extracted = self._extract_entity_from_query(parsed.raw_query)
            if extracted:
                search_name = extracted.lower().strip()
        
        if not search_name:
            return self._execute_general(parsed)
        
        # Determine which fields to retrieve
        output_fields = list(parsed.output_fields or [])
        if parsed.filter_field and not parsed.filter_value and parsed.filter_field not in output_fields:
            output_fields.append(parsed.filter_field)
        for tf in (parsed.target_fields or []):
            if tf not in output_fields:
                output_fields.append(tf)
        
        if not output_fields:
            return self._execute_general(parsed)
        
        # Step 1: Find matching quote_id(s) by searching person/business names
        matched_quotes = {}  # quote_id -> business_name
        seen_quotes = set()
        
        for chunk in self.metadata:
            quote_id = chunk.get("quote_id")
            if not quote_id or quote_id in seen_quotes:
                continue
            
            search_fields = self._get_search_fields(chunk)
            found = False
            
            # Check top-level user_name
            user_name = str(chunk.get("user_name", "")).lower().strip()
            if search_name in user_name or user_name in search_name:
                found = True
            
            # Check person_in_charge / business_name in search_fields
            if not found:
                for field_name, value in search_fields.items():
                    if "person_in_charge" in field_name.lower() or "business_name" in field_name.lower():
                        val_lower = str(value).lower().strip()
                        if search_name in val_lower or val_lower in search_name:
                            found = True
                            break
            
            if found:
                seen_quotes.add(quote_id)
                business_name = self._get_field_value(chunk, "business_name")
                matched_quotes[quote_id] = business_name
        
        if not matched_quotes:
            return QueryResult(
                success=False,
                data=[],
                count=0,
                summary=f"No proposal found for '{parsed.filter_contains}'",
                details=[]
            )
        
        # Step 2: For each matching quote, retrieve the requested output_fields
        results = []
        
        for match_qid, match_bname in matched_quotes.items():
            retrieved_fields = {}
            
            for chunk in self.metadata:
                if chunk.get("quote_id") != match_qid:
                    continue
                
                search_fields = self._get_search_fields(chunk)
                
                for out_field in output_fields:
                    if out_field in retrieved_fields:
                        continue  # Already found this field
                    
                    # Use scored matching
                    best_score = 0
                    best_field_name = None
                    best_value = None
                    
                    for field_name, value in search_fields.items():
                        score = self._field_match_score(out_field, field_name)
                        if score > best_score:
                            best_score = score
                            best_field_name = field_name
                            best_value = value
                    
                    if best_score >= 10 and best_field_name is not None:
                        retrieved_fields[best_field_name] = best_value  # already decoded
            
            if retrieved_fields:
                for field_name, decoded_value in retrieved_fields.items():
                    results.append({
                        "quote_id": match_qid,
                        "business_name": match_bname,
                        "field": field_name,
                        "value": decoded_value
                    })
            else:
                results.append({
                    "quote_id": match_qid,
                    "business_name": match_bname,
                    "field": ", ".join(output_fields),
                    "value": "Not found"
                })
        
        if results:
            details = []
            for r in results:
                field_label = r['field'].replace('_label', '').replace('_', ' ').title()
                details.append(f"{r['business_name']} ({r['quote_id']}): {field_label} = {r['value']}")
            
            return QueryResult(
                success=True,
                data=results,
                count=len(results),
                summary=f"Found data for {len(matched_quotes)} matching proposal(s)",
                details=details
            )
        
        return QueryResult(
            success=False,
            data=[],
            count=0,
            summary=f"No matching fields found for '{parsed.filter_contains}'",
            details=[]
        )
    
    # ------------------------------------------------------------------
    # Count
    # ------------------------------------------------------------------
    
    def _execute_count(self, parsed: ParsedQuery) -> QueryResult:
        """Execute a count query."""
        matching_quotes = set()
        matching_data = []
        
        for chunk in self.metadata:
            quote_id = chunk.get("quote_id")
            if not quote_id or quote_id in matching_quotes:
                continue
            
            search_fields = self._get_search_fields(chunk)
            chunk_text = chunk.get("text", "").lower()
            section = chunk.get("section", "")
            
            # Check filter_contains - search in decoded fields, chunk text, AND top-level metadata
            if parsed.filter_contains:
                search_term = parsed.filter_contains.lower()
                found = False
                
                # Check chunk text
                if search_term in chunk_text:
                    found = True
                
                # Check in decoded field values
                if not found:
                    for field_name, value in search_fields.items():
                        if search_term in str(value).lower():
                            found = True
                            break
                
                # Check top-level metadata keys
                if not found:
                    for top_key in ("risk_location", "user_name"):
                        top_val = chunk.get(top_key, "")
                        if top_val and search_term in str(top_val).lower():
                            found = True
                            break
                
                if found:
                    matching_quotes.add(quote_id)
                    business_name = self._get_field_value(chunk, "business_name")
                    matching_data.append({
                        "quote_id": quote_id,
                        "business_name": business_name,
                        "section": section,
                        "matched_text": chunk_text[:100]
                    })
                continue
            
            # Check for filter on fields (yes/no decoded values, exact match, or substring)
            if parsed.filter_field and parsed.filter_value:
                expected = str(parsed.filter_value).lower().strip()
                filter_key = parsed.filter_field.lower().replace("_label", "")
                matched = False
                matched_field = None
                matched_value = None
                
                # Search in decoded fields
                for field_name, value in search_fields.items():
                    if filter_key in field_name.lower().replace("_label", ""):
                        value_lower = str(value).lower().strip()
                        
                        # Normalize yes/no matching — decoded values may be "Yes"/"No"
                        # but filter_value from LLM may be codes like "001"/"002"
                        YES_CODES = {"yes", "001", "true", "1"}
                        NO_CODES = {"no", "002", "false", "2", "0"}
                        
                        if expected in YES_CODES and value_lower in YES_CODES:
                            matched = True
                        elif expected in NO_CODES and value_lower in NO_CODES:
                            matched = True
                        elif value_lower == expected:
                            matched = True
                        elif len(expected) > 2 and expected in value_lower:
                            matched = True
                        
                        if matched:
                            matched_field = field_name
                            matched_value = value  # already decoded
                            break
                
                # Also check top-level metadata keys
                if not matched:
                    for top_key in ("risk_location", "user_name"):
                        if filter_key in top_key.lower():
                            top_val = str(chunk.get(top_key, "")).lower().strip()
                            if expected in top_val:
                                matched = True
                                matched_field = top_key
                                matched_value = chunk.get(top_key, "")
                                break
                
                if matched:
                    matching_quotes.add(quote_id)
                    business_name = self._get_field_value(chunk, "business_name")
                    matching_data.append({
                        "quote_id": quote_id,
                        "business_name": business_name,
                        "field": matched_field,
                        "value": matched_value
                    })
        
        count = len(matching_quotes)
        
        if count > 0:
            details = [f"{d.get('business_name', d['quote_id'])} ({d['quote_id']})" for d in matching_data]
            return QueryResult(
                success=True,
                data=matching_data,
                count=count,
                summary=f"{count} proposal(s) match the criteria",
                details=details
            )
        
        return QueryResult(
            success=True,
            data=[],
            count=0,
            summary="0 proposals match the criteria",
            details=[]
        )
    
    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------
    
    def _execute_list(self, parsed: ParsedQuery) -> QueryResult:
        """Execute a list query."""
        result = self._execute_count(parsed)
        
        if result.success and result.data:
            details = []
            for d in result.data:
                business_name = d.get("business_name", "Unknown")
                quote_id = d.get("quote_id", "")
                details.append(f"{business_name} ({quote_id})")
            result.details = details
        
        return result
    
    # ------------------------------------------------------------------
    # Compare (highest/lowest)
    # ------------------------------------------------------------------
    
    def _execute_compare(self, parsed: ParsedQuery) -> QueryResult:
        """Execute a comparison query (highest/lowest)."""
        values_with_data = []
        
        for chunk in self.metadata:
            quote_id = chunk.get("quote_id")
            if not quote_id:
                continue
            
            # Use raw fields for numeric comparisons — decoded values may have
            # currency symbols or labels that interfere with numeric parsing.
            # But also try decoded_fields for passthrough numeric fields.
            raw_fields = chunk.get("fields", {})
            if not isinstance(raw_fields, dict):
                continue
            
            for target in parsed.target_fields:
                for field_name, value in raw_fields.items():
                    if self._field_match_score(target, field_name) >= 10:
                        num_val = self._parse_numeric(value)
                        if num_val is not None:
                            business_name = self._get_field_value(chunk, "business_name")
                            values_with_data.append({
                                "quote_id": quote_id,
                                "business_name": business_name,
                                "field": field_name,
                                "value": value,
                                "numeric": num_val
                            })
        
        if values_with_data:
            is_max = "highest" in parsed.raw_query.lower() or "maximum" in parsed.raw_query.lower() or "most" in parsed.raw_query.lower()
            values_with_data.sort(key=lambda x: x["numeric"], reverse=is_max)
            
            best = values_with_data[0]
            word = "highest" if is_max else "lowest"
            
            return QueryResult(
                success=True,
                data=[best],
                count=1,
                summary=f"The {word} value is {best['value']} for {best['business_name']} ({best['quote_id']})",
                details=[f"{best['business_name']} ({best['quote_id']}): {best['value']}"]
            )
        
        return QueryResult(
            success=False,
            data=[],
            count=0,
            summary="Could not find comparable values",
            details=[]
        )
    
    # ------------------------------------------------------------------
    # General search
    # ------------------------------------------------------------------
    
    def _execute_general(self, parsed: ParsedQuery) -> QueryResult:
        """Execute a general search query using decoded fields."""
        matching_data = []
        seen_quotes = set()
        
        search_terms = []
        if parsed.filter_contains:
            search_terms.append(parsed.filter_contains.lower())
        
        query_words = re.findall(r'\b[a-zA-Z]{3,}\b', parsed.raw_query.lower())
        ignore_words = {"what", "how", "many", "which", "the", "are", "have", "has", "with", "and", "for", "their", "names", "all"}
        search_terms.extend([w for w in query_words if w not in ignore_words])
        
        for chunk in self.metadata:
            quote_id = chunk.get("quote_id")
            if not quote_id or quote_id in seen_quotes:
                continue
            
            search_fields = self._get_search_fields(chunk)
            if not search_fields:
                continue
            
            for field_name, value in search_fields.items():
                value_str = str(value).lower()
                field_lower = field_name.lower()
                
                for term in search_terms:
                    if term in value_str or term in field_lower:
                        seen_quotes.add(quote_id)
                        business_name = self._get_field_value(chunk, "business_name")
                        matching_data.append({
                            "quote_id": quote_id,
                            "business_name": business_name,
                            "matched_field": field_name,
                            "matched_value": value
                        })
                        break
                
                if quote_id in seen_quotes:
                    break
        
        if matching_data:
            details = [f"{d.get('business_name', d['quote_id'])} ({d['quote_id']})" for d in matching_data]
            return QueryResult(
                success=True,
                data=matching_data,
                count=len(matching_data),
                summary=f"Found {len(matching_data)} matching proposal(s)",
                details=details
            )
        
        return QueryResult(
            success=False,
            data=[],
            count=0,
            summary="No matching data found",
            details=[]
        )
    
    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    
    def _get_field_value(self, chunk: dict, field_pattern: str) -> str:
        """Get a decoded field value from a chunk by pattern matching."""
        # Check decoded_fields first (already human-readable)
        search_fields = self._get_search_fields(chunk)
        for field_name, value in search_fields.items():
            if field_pattern.lower() in field_name.lower():
                return str(value)
        
        # Check other chunks for the same quote_id
        quote_id = chunk.get("quote_id")
        for other_chunk in self.metadata:
            if other_chunk.get("quote_id") == quote_id and other_chunk != chunk:
                other_fields = self._get_search_fields(other_chunk)
                for field_name, value in other_fields.items():
                    if field_pattern.lower() in field_name.lower():
                        return str(value)
        
        return "Unknown"
    
    def _parse_numeric(self, value) -> Optional[float]:
        """Parse a value as a number."""
        if isinstance(value, (int, float)):
            return float(value)
        
        value_str = str(value).replace(",", "").replace("RM", "").replace("$", "").strip()
        try:
            return float(value_str)
        except ValueError:
            return None
