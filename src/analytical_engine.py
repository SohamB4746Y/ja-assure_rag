"""
Analytical Engine for Pandas-based aggregation queries.

This module handles all multi-record analytical queries without LLM involvement.
It operates on decoded dataframes where all codes have been converted to labels.
"""
from __future__ import annotations

import re
import pandas as pd
from typing import Optional

# Configuration: Field name patterns for fuzzy matching
CCTV_PATTERNS = ["cctv", "camera", "recording", "surveillance"]
ALARM_PATTERNS = ["alarm", "security system", "monitoring"]
GUARD_PATTERNS = ["guard", "armed", "security personnel"]
TRANSIT_PATTERNS = ["transit", "armoured", "vehicle", "transport"]
CLAIM_PATTERNS = ["claim", "loss", "incident"]
SAFE_PATTERNS = ["safe", "vault", "storage", "strong room"]
DOOR_PATTERNS = ["door", "access", "entry"]
PREMISE_PATTERNS = ["premise", "building", "location", "shop"]

# Yes/No value patterns
YES_VALUES = ["yes", "001", "true", "1"]
NO_VALUES = ["no", "002", "false", "0"]


class AnalyticalEngine:
    """
    Pandas-based engine for processing analytical queries.
    Never calls the LLM - all answers come from data operations.
    """

    def __init__(self, decoded_df: pd.DataFrame, metadata: list[dict] = None):
        """
        Initialize the analytical engine.

        Args:
            decoded_df: DataFrame with decoded (human-readable) values.
            metadata: Optional list of metadata dicts from FAISS index.
        """
        self.df = decoded_df
        self.metadata = metadata or []
        self._build_field_index()

    def _build_field_index(self) -> None:
        """Build an index of all fields across all records for fast lookup."""
        self.all_fields = set()
        self.field_to_section = {}

        for meta in self.metadata:
            if "fields" in meta and isinstance(meta["fields"], dict):
                for field in meta["fields"].keys():
                    self.all_fields.add(field.lower())
                    section = meta.get("section", "unknown")
                    self.field_to_section[field.lower()] = section

    def run(self, query: str) -> Optional[str]:
        """
        Execute an analytical query and return the result.

        Args:
            query: The user's analytical question.

        Returns:
            Formatted plain-text answer with source record IDs, or None if
            the query cannot be mapped to data columns.
        """
        query_lower = query.lower()

        # Determine the operation type
        if self._is_counting_query(query_lower):
            return self._handle_count_query(query_lower)

        if self._is_listing_query(query_lower):
            return self._handle_list_query(query_lower)

        if self._is_comparison_query(query_lower):
            return self._handle_comparison_query(query_lower)

        # Try to handle as a general analytical query
        return self._handle_general_query(query_lower)

    def _is_counting_query(self, query: str) -> bool:
        """Check if query asks for a count."""
        signals = ["how many", "count", "total", "number of"]
        return any(s in query for s in signals)

    def _is_listing_query(self, query: str) -> bool:
        """Check if query asks for a list."""
        signals = ["list all", "which proposals", "which records", "show all", "what are all"]
        return any(s in query for s in signals)

    def _is_comparison_query(self, query: str) -> bool:
        """Check if query asks for comparison/ranking."""
        signals = ["highest", "lowest", "maximum", "minimum", "most", "least", "top", "bottom"]
        return any(s in query for s in signals)

    def _handle_count_query(self, query: str) -> Optional[str]:
        """
        Handle counting queries like "How many proposals have X?"

        Args:
            query: Lowercase query string.

        Returns:
            Count result as string, or None if cannot process.
        """
        # Extract what we're counting
        field_pattern, expected_value = self._extract_condition(query)

        if not field_pattern:
            return None

        # Find matching records
        matching_ids = []
        total_checked = 0

        for meta in self.metadata:
            quote_id = meta.get("quote_id")
            fields = meta.get("fields", {})

            if not isinstance(fields, dict):
                continue

            total_checked += 1

            for field_name, value in fields.items():
                field_lower = field_name.lower()

                if self._field_matches_pattern(field_lower, field_pattern):
                    if self._value_matches(value, expected_value):
                        if quote_id and quote_id not in matching_ids:
                            matching_ids.append(quote_id)
                        break

        if matching_ids:
            count = len(matching_ids)
            return f"{count} proposal(s) match the criteria. Quote IDs: {', '.join(sorted(set(matching_ids)))}"

        return f"0 proposals match the criteria."

    def _handle_list_query(self, query: str) -> Optional[str]:
        """
        Handle listing queries like "List all proposals with X"

        Args:
            query: Lowercase query string.

        Returns:
            List of matching records, or None if cannot process.
        """
        field_pattern, expected_value = self._extract_condition(query)

        if not field_pattern:
            return None

        matching_records = []

        for meta in self.metadata:
            quote_id = meta.get("quote_id")
            fields = meta.get("fields", {})
            section = meta.get("section", "")

            if not isinstance(fields, dict):
                continue

            for field_name, value in fields.items():
                field_lower = field_name.lower()

                if self._field_matches_pattern(field_lower, field_pattern):
                    if self._value_matches(value, expected_value):
                        matching_records.append({
                            "quote_id": quote_id,
                            "section": section,
                            "field": field_name,
                            "value": value
                        })
                        break

        if matching_records:
            # Deduplicate by quote_id
            seen = set()
            unique_records = []
            for r in matching_records:
                if r["quote_id"] not in seen:
                    seen.add(r["quote_id"])
                    unique_records.append(r)

            result_lines = [f"Found {len(unique_records)} matching proposal(s):"]
            for r in unique_records[:20]:  # Limit to 20 results
                result_lines.append(f"- {r['quote_id']}: {r['field']} = {r['value']}")

            if len(unique_records) > 20:
                result_lines.append(f"... and {len(unique_records) - 20} more.")

            return "\n".join(result_lines)

        return "No proposals match the criteria."

    def _handle_comparison_query(self, query: str) -> Optional[str]:
        """
        Handle comparison queries like "highest sum assured"

        Args:
            query: Lowercase query string.

        Returns:
            Comparison result, or None if cannot process.
        """
        # Determine if looking for max or min
        is_max = any(s in query for s in ["highest", "maximum", "most", "top"])

        # Extract the numeric field being compared
        numeric_patterns = [
            ("sum assured", "sum_assured"),
            ("claim amount", "amount_of_claim"),
            ("stock", "maximum_stock"),
            ("value", "value"),
        ]

        target_pattern = None
        for keyword, pattern in numeric_patterns:
            if keyword in query:
                target_pattern = pattern
                break

        if not target_pattern:
            return None

        # Find numeric values and their quote IDs
        values_with_ids = []

        for meta in self.metadata:
            quote_id = meta.get("quote_id")
            fields = meta.get("fields", {})

            if not isinstance(fields, dict):
                continue

            for field_name, value in fields.items():
                if target_pattern in field_name.lower():
                    try:
                        # Try to parse as number
                        num_val = self._parse_numeric(value)
                        if num_val is not None:
                            values_with_ids.append((num_val, quote_id, value))
                    except (ValueError, TypeError):
                        continue

        if not values_with_ids:
            return None

        # Sort and get result
        values_with_ids.sort(key=lambda x: x[0], reverse=is_max)
        best = values_with_ids[0]

        comparison_word = "highest" if is_max else "lowest"
        return f"The {comparison_word} value is {best[2]} for proposal {best[1]}."

    def _handle_general_query(self, query: str) -> Optional[str]:
        """
        Handle general analytical queries.

        Args:
            query: Lowercase query string.

        Returns:
            Result or None.
        """
        # Try to find any matching field and aggregate
        field_pattern, _ = self._extract_condition(query)

        if not field_pattern:
            return None

        # Collect all values for this field
        value_counts = {}

        for meta in self.metadata:
            fields = meta.get("fields", {})

            if not isinstance(fields, dict):
                continue

            for field_name, value in fields.items():
                if self._field_matches_pattern(field_name.lower(), field_pattern):
                    value_str = str(value)
                    value_counts[value_str] = value_counts.get(value_str, 0) + 1

        if value_counts:
            result_lines = [f"Distribution for matching fields:"]
            for val, count in sorted(value_counts.items(), key=lambda x: -x[1]):
                result_lines.append(f"- {val}: {count} proposal(s)")
            return "\n".join(result_lines)

        return None

    def _extract_condition(self, query: str) -> tuple[str, str]:
        """
        Extract the field pattern and expected value from a query.

        Args:
            query: Lowercase query string.

        Returns:
            Tuple of (field_pattern, expected_value).
        """
        # Check for specific field patterns
        patterns_to_check = [
            (CCTV_PATTERNS, "cctv"),
            (ALARM_PATTERNS, "alarm"),
            (GUARD_PATTERNS, "guard"),
            (TRANSIT_PATTERNS, "transit"),
            (CLAIM_PATTERNS, "claim"),
            (SAFE_PATTERNS, "safe"),
            (DOOR_PATTERNS, "door"),
            (PREMISE_PATTERNS, "premise"),
        ]

        field_pattern = None
        for patterns, category in patterns_to_check:
            for p in patterns:
                if p in query:
                    field_pattern = category
                    break
            if field_pattern:
                break

        # Determine expected value
        expected_value = None
        if "no " in query or "without" in query or "don't" in query or "do not" in query:
            expected_value = "no"
        elif "have" in query or "use" in query or "with" in query:
            expected_value = "yes"
        elif "maintenance" in query:
            expected_value = "yes"

        # Special cases
        if "no claim" in query:
            expected_value = "no claim"
        if "claims within" in query or "has claim" in query:
            expected_value = "claims"

        return field_pattern, expected_value

    def _field_matches_pattern(self, field_name: str, pattern: str) -> bool:
        """Check if a field name matches a pattern category."""
        pattern_map = {
            "cctv": CCTV_PATTERNS,
            "alarm": ALARM_PATTERNS,
            "guard": GUARD_PATTERNS,
            "transit": TRANSIT_PATTERNS,
            "claim": CLAIM_PATTERNS,
            "safe": SAFE_PATTERNS,
            "door": DOOR_PATTERNS,
            "premise": PREMISE_PATTERNS,
        }

        patterns = pattern_map.get(pattern, [pattern])
        return any(p in field_name for p in patterns)

    def _value_matches(self, value: str, expected: str) -> bool:
        """Check if a value matches the expected condition."""
        if expected is None:
            return True

        value_lower = str(value).lower()
        expected_lower = expected.lower()

        if expected_lower == "yes":
            return value_lower in YES_VALUES or value_lower == "yes"
        elif expected_lower == "no":
            return value_lower in NO_VALUES or value_lower == "no"
        elif expected_lower == "no claim":
            return "no claim" in value_lower or value_lower == "001"
        elif expected_lower == "claims":
            return "claim" in value_lower and "no claim" not in value_lower

        return expected_lower in value_lower

    def _parse_numeric(self, value) -> Optional[float]:
        """Try to parse a value as a number."""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        # Try to extract number from string
        value_str = str(value).replace(",", "").replace("$", "").replace("RM", "").strip()
        match = re.search(r"[\d.]+", value_str)
        if match:
            try:
                return float(match.group())
            except ValueError:
                return None

        return None

    def get_unique_quote_ids(self) -> list[str]:
        """Get all unique quote IDs in the dataset."""
        quote_ids = set()
        for meta in self.metadata:
            qid = meta.get("quote_id")
            if qid:
                quote_ids.add(qid)
        return sorted(list(quote_ids))

    def get_record_count(self) -> int:
        """Get the total number of unique records."""
        return len(self.get_unique_quote_ids())
