"""
CompoundQueryHandler — handles queries requesting multiple fields
for a filtered set of proposals simultaneously.

Triggered when:
1. QueryParser detects multiple distinct field requests in one query
2. Query contains location/entity filter + field questions
3. Query uses "and" connecting different field types

Examples:
- "businesses in Johor with armed guards and what is their CCTV backup?"
- "proposals with strong room — what are their insured values and safe grades?"
- "which KL businesses have alarm and GPS and what is their claim history?"
"""

from __future__ import annotations

import re
from typing import List, Dict, Optional, Tuple


class CompoundQueryHandler:

    # Maps natural language phrases to (section, field_name) tuples.
    # Order matters: longer / more specific phrases should come first
    # so that "armed guards during transit" matches before "guards".
    FIELD_PHRASE_MAP = [
        # --- Security ---
        (["armed guards during transit", "armed guards", "guards during transit",
          "transit guards"],
         ("transit_and_gaurds", "do_you_use_armed_guards_during_transit_label")),
        (["armoured vehicle", "armored vehicle", "armoured car"],
         ("transit_and_gaurds", "do_you_use_armoured_vehicle_label")),
        (["gps tracker", "gps installed", "have gps", "use gps",
          "gps in transit"],
         ("transit_and_gaurds", "installed_gps_tracker_in_transit_vehicles_label")),
        (["strong room", "strongroom", "vault room"],
         ("strong_room", "do_you_have_a_strong_room_label")),
        (["alarm system", "have alarm", "alarm installed", "alarm"],
         ("alarm", "do_you_have_alarm_label")),
        (["cctv backup", "backup type", "type of backup", "cctv backup type"],
         ("cctv", "type_of_back_up_label")),
        (["cctv maintenance", "camera maintenance"],
         ("cctv", "cctv_maintenance_contract_label")),
        (["cctv retention", "how long cctv", "retention period"],
         ("cctv", "retained_period_of_cctv_recording_label")),
        (["display window", "have display window"],
         ("display_window", "do_you_have_display_window_label")),
        # --- Financial ---
        (["insured value", "sum assured", "stock value", "insured amount",
          "how much insured", "value insured"],
         ("sum_assured", "__PRIMARY_VALUE__")),  # special handler
        (["safe grade", "grade of safe"],
         ("safe", "grade_label")),
        (["safe model", "model of safe"],
         ("safe", "safe_model_label")),
        (["transit limit", "cash in transit"],
         ("transit_and_gaurds", "limit_per_transit_label")),
        # --- Compliance ---
        (["background check", "employee check", "staff screening"],
         ("additional_details", "background_checks_for_all_employees_label")),
        (["standard operating procedure", "sop"],
         ("additional_details", "standard_operating_procedure_label")),
        (["stock records", "detailed records", "keep records"],
         ("records_keeping",
          "do_you_keep_detailed_records_of_stock_movements_label")),
        (["police station", "nearest police", "distance to police"],
         ("additional_details", "the_nearest_police_station_label")),
        (["stock check", "how often stock"],
         ("additional_details",
          "how_often_is_the_stock_check_carried_out_label")),
        # --- Business ---
        (["claim history", "previous claims", "any claims"],
         ("claim_history", "claim_history_label")),
        (["door access", "access type", "entry method"],
         ("door_access", "door_access_label")),
        (["nature of business", "type of business", "business type"],
         ("business_profile", "nature_of_business_label")),
        (["contact number", "phone number"],
         ("business_profile", "contact_number_label")),
    ]

    # Condition indicators: (phrase, field_name, expected_decoded_value)
    CONDITION_INDICATORS = [
        ("have armed guards", "do_you_use_armed_guards_during_transit_label", "Yes"),
        ("use armed guards", "do_you_use_armed_guards_during_transit_label", "Yes"),
        ("with armed guards", "do_you_use_armed_guards_during_transit_label", "Yes"),
        ("have strong room", "do_you_have_a_strong_room_label", "Yes"),
        ("has strong room", "do_you_have_a_strong_room_label", "Yes"),
        ("with strong room", "do_you_have_a_strong_room_label", "Yes"),
        ("a strong room", "do_you_have_a_strong_room_label", "Yes"),
        ("have alarm", "do_you_have_alarm_label", "Yes"),
        ("with alarm", "do_you_have_alarm_label", "Yes"),
        ("have gps", "installed_gps_tracker_in_transit_vehicles_label", "Yes"),
        ("use gps", "installed_gps_tracker_in_transit_vehicles_label", "Yes"),
        ("with gps", "installed_gps_tracker_in_transit_vehicles_label", "Yes"),
        ("use armoured vehicle", "do_you_use_armoured_vehicle_label", "Yes"),
        ("with armoured vehicle", "do_you_use_armoured_vehicle_label", "Yes"),
        ("have cctv", "recording_label", "Yes"),
        ("with cctv", "recording_label", "Yes"),
        ("have display window", "do_you_have_display_window_label", "Yes"),
        ("have sop", "standard_operating_procedure_label", "Yes"),
        ("no armed guards", "do_you_use_armed_guards_during_transit_label", "No"),
        ("no strong room", "do_you_have_a_strong_room_label", "No"),
        ("no alarm", "do_you_have_alarm_label", "No"),
        ("no gps", "installed_gps_tracker_in_transit_vehicles_label", "No"),
    ]

    # Location names for extraction — longest first to avoid partial matches
    _LOCATION_NAMES = [
        "kota kinabalu", "petaling jaya", "kuala lumpur",
        "negeri sembilan", "sungai petani", "george town",
        "johor bahru",
        "johor", "penang", "selangor", "sabah", "melaka", "perak",
        "pahang", "kedah", "muar", "ipoh", "taiping",
        "seremban", "klang", "kuantan",
        "kl",
    ]

    # Sentinel values treated as empty
    _EMPTY = {"", "None", "null", "nan", "-1", "N/A", "n/a"}

    # Decoded values that mean "Yes"
    _YES_VALS = {"yes", "001", "true", "1"}

    def __init__(self, metadata: list) -> None:
        self.metadata = metadata
        # Build lookup maps at init time for O(1) access
        self._location_map = self._build_location_map()
        self._field_map = self._build_field_map()
        self._name_map = self._build_name_map()
        self._value_map = self._build_value_map()
        self._complete_qids = self._build_complete_set()

    # ------------------------------------------------------------------
    # Index-building helpers (run once at init)
    # ------------------------------------------------------------------

    def _build_location_map(self) -> Dict[str, str]:
        """quote_id -> full risk_location string."""
        loc_map: Dict[str, str] = {}
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            loc = chunk.get("risk_location", "")
            if qid and loc and qid not in loc_map:
                loc_map[qid] = str(loc)
        return loc_map

    def _build_name_map(self) -> Dict[str, str]:
        """quote_id -> business name."""
        name_map: Dict[str, str] = {}
        for chunk in self.metadata:
            if chunk.get("section") != "business_profile":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in name_map:
                continue
            df = chunk.get("decoded_fields") or {}
            fields = chunk.get("fields") or {}
            name = (
                df.get("business_name_label")
                or fields.get("business_name_label")
                or chunk.get("user_name")
                or qid
            )
            if name and str(name).strip() not in self._EMPTY:
                name_map[qid] = str(name).strip()
            else:
                name_map[qid] = qid
        return name_map

    def _build_field_map(self) -> Dict[Tuple[str, str], str]:
        """(quote_id, field_name) -> decoded value string."""
        field_map: Dict[Tuple[str, str], str] = {}
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            df = chunk.get("decoded_fields") or {}
            for field_name, value in df.items():
                if value is not None and str(value).strip() not in self._EMPTY:
                    key = (qid, field_name)
                    if key not in field_map:
                        field_map[key] = str(value).strip()
        return field_map

    def _build_value_map(self) -> Dict[str, Tuple[float, str]]:
        """quote_id -> (primary_value, value_label)."""
        val_map: Dict[str, Tuple[float, str]] = {}
        for chunk in self.metadata:
            if chunk.get("section") != "sum_assured":
                continue
            qid = chunk.get("quote_id")
            if not qid or qid in val_map:
                continue
            fields = chunk.get("fields") or {}
            val, label = self._get_primary_value(fields)
            if val > 0:
                val_map[qid] = (val, label)
        return val_map

    def _build_complete_set(self) -> set:
        """Set of quote_ids that have real submitted data."""
        complete: set = set()
        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue
            if "is_complete_submission" in chunk:
                if chunk.get("is_complete_submission"):
                    complete.add(qid)
            else:
                # Backwards-compat: check business name exists
                df = chunk.get("decoded_fields") or {}
                fields = chunk.get("fields") or {}
                name = (
                    df.get("business_name_label")
                    or fields.get("business_name_label")
                )
                if name and str(name).strip() not in self._EMPTY:
                    complete.add(qid)
        return complete

    # ------------------------------------------------------------------
    # Primary value extraction (same logic as PartialAnswerEngine)
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_float(raw) -> float:
        if raw is None or str(raw).strip() in ("", "None", "-1", "0", "nan",
                                                "N/A", "n/a"):
            return 0.0
        try:
            return float(str(raw).replace(",", ""))
        except (TypeError, ValueError):
            return 0.0

    @classmethod
    def _get_primary_value(cls, fields: dict) -> Tuple[float, str]:
        """Read correct value field based on business type."""
        # 1. Jewellers: stock in premises
        v = cls._safe_float(fields.get("maximum_stock_in_premises_label"))
        if v > 0:
            return (v, "jewellery stock")

        # 2. Money changers: foreign currency
        v = cls._safe_float(
            fields.get("maximum_stock_foreign_currency_in_premise_label")
        )
        if v > 0:
            return (v, "foreign currency")

        # 3. Pawnbrokers: pledged stock + cash
        pledged = cls._safe_float(
            fields.get("value_of_pledged_stock_in_premise_label")
        )
        cash = cls._safe_float(
            fields.get("value_of_cash_in_premise_label")
        )
        total = pledged + cash
        if total > 0:
            return (total, "pledged stock + cash")

        return (0.0, "")

    # ------------------------------------------------------------------
    # Query component extraction
    # ------------------------------------------------------------------

    def _extract_location_filter(self, q: str) -> Optional[str]:
        """Extract location name from query. Returns the matched string."""
        for loc in self._LOCATION_NAMES:
            if loc in q:
                return loc
        return None

    def _extract_condition_filters(self, q: str) -> List[Tuple[str, str]]:
        """Extract field=value conditions. Returns [(field_name, value)]."""
        conditions: List[Tuple[str, str]] = []
        seen_fields: set = set()
        for phrase, field, value in self.CONDITION_INDICATORS:
            if phrase in q and field not in seen_fields:
                conditions.append((field, value))
                seen_fields.add(field)
        return conditions

    def _extract_output_fields(self, q: str) -> List[Tuple[str, str]]:
        """Extract which fields the user wants to see.
        Returns [(section, field_name)]."""
        requested: List[Tuple[str, str]] = []
        for phrases, (section, field) in self.FIELD_PHRASE_MAP:
            for phrase in phrases:
                if phrase in q:
                    if (section, field) not in requested:
                        requested.append((section, field))
                    break
        return requested

    # ------------------------------------------------------------------
    # Proposal filtering
    # ------------------------------------------------------------------

    def _filter_by_location(self, location: str) -> List[str]:
        """Return quote_ids whose risk_location contains *location*."""
        matched: List[str] = []
        loc_lower = location.lower()
        # "kl" is shorthand for Kuala Lumpur
        if loc_lower == "kl":
            loc_lower = "kuala lumpur"
        for qid, loc in self._location_map.items():
            if loc_lower in loc.lower():
                matched.append(qid)
        return sorted(matched)

    def _filter_by_conditions(
        self,
        qids: List[str],
        conditions: List[Tuple[str, str]],
    ) -> List[str]:
        """Keep only quote_ids that match ALL field=value conditions."""
        if not conditions:
            return qids
        filtered: List[str] = []
        for qid in qids:
            ok = True
            for field, expected in conditions:
                actual = self._field_map.get((qid, field), "")
                if actual.strip().lower() != expected.strip().lower():
                    ok = False
                    break
            if ok:
                filtered.append(qid)
        return filtered

    # ------------------------------------------------------------------
    # Field value accessors
    # ------------------------------------------------------------------

    def _get_field_value(self, qid: str, section: str, field: str) -> str:
        """Get a decoded field value for a specific proposal."""
        if field == "__PRIMARY_VALUE__":
            val_data = self._value_map.get(qid)
            if val_data:
                val, label = val_data
                return "RM {:,.0f} ({})".format(val, label)
            return "Not submitted"
        value = self._field_map.get((qid, field))
        return value if value else "Not submitted"

    @staticmethod
    def _format_field_name(field: str) -> str:
        """Convert field_name_label to readable Title Case."""
        if field == "__PRIMARY_VALUE__":
            return "Insured Value"
        name = field.replace("_label", "").replace("_", " ")
        return name.title()

    def _extract_city(self, qid: str) -> str:
        """Extract city (first comma-separated part) from risk_location."""
        loc = self._location_map.get(qid, "")
        if not loc:
            return "Unknown"
        parts = [p.strip() for p in loc.split(",") if p.strip()]
        return parts[0] if parts else "Unknown"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_compound_query(self, query: str) -> bool:
        """
        Detect if *query* is a compound multi-field request.
        Returns True when the query asks for multiple different things
        about a filtered set of proposals.
        """
        q = query.lower()

        has_location = self._extract_location_filter(q) is not None
        output_fields = self._extract_output_fields(q)
        conditions = self._extract_condition_filters(q)

        # Compound if:
        # - location + >= 2 output fields
        # - location + >= 1 condition + >= 1 output field
        # - >= 1 condition + >= 2 output fields (no location needed)
        # - >= 2 conditions + >= 1 output field
        if has_location and len(output_fields) >= 2:
            return True
        if has_location and len(conditions) >= 1 and len(output_fields) >= 1:
            return True
        if len(conditions) >= 1 and len(output_fields) >= 2:
            return True
        if len(conditions) >= 2 and len(output_fields) >= 1:
            return True
        return False

    def execute(self, query: str) -> Optional[str]:
        """
        Execute a compound multi-field query.
        Returns formatted answer string, or None if not compound.
        """
        q = query.lower()

        if not self.is_compound_query(query):
            return None

        # --- Step 1: Extract all query components ---
        location_filter = self._extract_location_filter(q)
        conditions = self._extract_condition_filters(q)
        output_fields = self._extract_output_fields(q)

        # --- Step 2: Get candidate proposals (complete only) ---
        if location_filter:
            candidate_qids = self._filter_by_location(location_filter)
        else:
            candidate_qids = sorted(self._name_map.keys())

        # Keep only complete submissions
        candidate_qids = [
            qid for qid in candidate_qids
            if qid in self._complete_qids
        ]

        loc_label = location_filter.title() if location_filter else ""
        loc_str = " in {}".format(loc_label) if loc_label else ""

        if not candidate_qids:
            return "No proposals found{} with submitted data.".format(loc_str)

        # --- Step 3: Apply condition filters ---
        if conditions:
            filtered_qids = self._filter_by_conditions(
                candidate_qids, conditions
            )
        else:
            filtered_qids = candidate_qids

        # --- Step 4: Build condition fields that are used as filters ---
        condition_field_set = {f for f, _ in conditions}

        # --- Step 5: Build response ---
        if not filtered_qids:
            # None match all conditions — show what IS available
            cond_desc = " and ".join(
                "{} = {}".format(self._format_field_name(f), v)
                for f, v in conditions
            )
            lines = [
                "No proposals{} match all conditions: {}.".format(
                    loc_str, cond_desc
                ),
                "",
                "All {} proposals{}:".format(len(candidate_qids), loc_str),
            ]
            for qid in candidate_qids:
                name = self._name_map.get(qid, qid)
                city = self._extract_city(qid)
                parts = ["  - {} ({}) — {}".format(name, qid, city)]
                for field, _val in conditions:
                    actual = self._field_map.get((qid, field), "Not submitted")
                    parts.append(
                        "    {}: {}".format(
                            self._format_field_name(field), actual
                        )
                    )
                lines.extend(parts)
            return "\n".join(lines)

        # Build full multi-field answer
        if conditions:
            cond_desc = " and ".join(
                "{} = {}".format(self._format_field_name(f), v)
                for f, v in conditions
            )
            header = "Proposals{loc} where {cond} ({n} found):".format(
                loc=loc_str, cond=cond_desc, n=len(filtered_qids)
            )
        else:
            header = "Proposals{loc} ({n} found):".format(
                loc=loc_str, n=len(filtered_qids)
            )

        lines = [header, ""]

        for i, qid in enumerate(filtered_qids, 1):
            name = self._name_map.get(qid, qid)
            city = self._extract_city(qid)

            lines.append("{}. {} ({}) — {}".format(i, name, qid, city))

            # Show condition field values first
            shown_fields: set = set()
            for field, _ in conditions:
                actual = self._field_map.get((qid, field), "Not submitted")
                display = self._format_field_name(field)
                lines.append("   {:<35}: {}".format(display, actual))
                shown_fields.add(field)

            # Show requested output fields (skip duplicates)
            for section, field in output_fields:
                if field in shown_fields:
                    continue
                value = self._get_field_value(qid, section, field)
                display = self._format_field_name(field)
                lines.append("   {:<35}: {}".format(display, value))

            if i < len(filtered_qids):
                lines.append("")

        # Note about non-matching candidates (if conditions were used)
        if conditions and len(candidate_qids) > len(filtered_qids):
            non_matching = [
                qid for qid in candidate_qids if qid not in filtered_qids
            ]
            if non_matching:
                names = [self._name_map.get(q, q) for q in non_matching]
                # Build a short explanation of why they were excluded
                cond_parts: List[str] = []
                for f, v in conditions:
                    label = self._format_field_name(f)
                    opposite = "No" if v == "Yes" else "Yes"
                    cond_parts.append(
                        "do not have {}".format(
                            label.lower().replace("do you ", "")
                            .replace("have a ", "").replace("use ", "")
                        )
                    )
                reason = " or ".join(cond_parts)
                lines.append("")
                lines.append(
                    "Note: {} {} also{} but {}.".format(
                        ", ".join(names),
                        "is" if len(names) == 1 else "are",
                        loc_str, reason,
                    )
                )

        return "\n".join(lines)
