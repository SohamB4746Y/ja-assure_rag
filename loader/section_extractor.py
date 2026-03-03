import json as _json

SECTION_COLUMNS = [
    "business_profile",
    "sum_assured",
    "physical_setup",
    "cctv",
    "door_access",
    "alarm",
    "safe",
    "strong_room",
    "display_showcases",
    "display_counters",
    "counter_show_case",
    "transit_and_gaurds",
    "records_keeping",
    "additional_details",
    "add_on_coverage",
    "claim_history",
    "premise_sub_limit",
    "display_window",
    "summary_coverage_values"
]

# Simple value columns (not JSON) that need to be extracted
SIMPLE_VALUE_COLUMNS = [
    ("shop_lifting", "shop_lifting_label"),
]

# Key sections used to determine proposal completeness
_COMPLETENESS_KEY_SECTIONS = [
    "business_profile",
    "sum_assured",
    "cctv",
    "alarm",
    "transit_and_gaurds",
    "claim_history",
]

_EMPTY_SENTINELS = {None, "", "-1", -1, "None", "nan"}


def validate_proposal_completeness(row_data: dict, json_parser) -> dict:
    """
    Assess how complete a proposal submission is by checking key sections.

    Returns a dict with:
    - is_complete: bool — True if the business submitted meaningful data
    - completeness_score: float 0.0–1.0 — fraction of key sections populated
    - missing_sections: list of section names with no data
    - submission_status: "complete" | "partial" | "empty"
    """
    populated = 0
    missing = []

    for section in _COMPLETENESS_KEY_SECTIONS:
        raw = row_data.get(section)
        if raw is None:
            missing.append(section)
            continue
        parsed = json_parser(raw)
        if not parsed:
            missing.append(section)
            continue
        # Check if any value in the parsed dict is non-empty
        def _is_nonempty(v) -> bool:
            """Return True if *v* carries real data (safe for unhashable types)."""
            if isinstance(v, (list, dict)):
                return bool(v)               # non-empty container → data exists
            try:
                if v in _EMPTY_SENTINELS:
                    return False
            except TypeError:
                pass                          # unhashable edge-case → treat as data
            return str(v).strip() not in ("", "None", "-1")

        if isinstance(parsed, dict):
            has_data = any(_is_nonempty(v) for v in parsed.values())
        elif isinstance(parsed, list):
            has_data = any(
                isinstance(item, dict) and any(_is_nonempty(v) for v in item.values())
                for item in parsed
            )
        else:
            has_data = False

        if has_data:
            populated += 1
        else:
            missing.append(section)

    score = populated / len(_COMPLETENESS_KEY_SECTIONS) if _COMPLETENESS_KEY_SECTIONS else 0.0

    if score == 0.0:
        status = "empty"
    elif score < 0.4:
        status = "partial"
    else:
        status = "complete"

    return {
        "is_complete": score >= 0.4,
        "completeness_score": round(score, 3),
        "missing_sections": missing,
        "submission_status": status,
    }


def extract_sections(row: dict, json_parser):
    sections = []

    base_metadata = {
        "quote_id": row.get("quote_id"),
        "risk_location": row.get("risk_location"),
        "user_name": row.get("user_name"),
    }

    # Validate completeness for this proposal row
    completeness = validate_proposal_completeness(row, json_parser)

    # Extract JSON sections
    for section in SECTION_COLUMNS:
        parsed = json_parser(row.get(section))
        if parsed:
            sections.append({
                "quote_id": base_metadata["quote_id"],
                "section": section,
                "data": parsed,
                "metadata": {
                    **base_metadata,
                    "submission_status": completeness["submission_status"],
                    "is_complete_submission": completeness["is_complete"],
                    "completeness_score": completeness["completeness_score"],
                },
            })

    # Extract simple value columns as their own section
    for col_name, field_name in SIMPLE_VALUE_COLUMNS:
        value = row.get(col_name)
        if value is not None and str(value).strip() != "":
            sections.append({
                "quote_id": base_metadata["quote_id"],
                "section": col_name,
                "data": {field_name: str(value)},
                "metadata": {
                    **base_metadata,
                    "submission_status": completeness["submission_status"],
                    "is_complete_submission": completeness["is_complete"],
                    "completeness_score": completeness["completeness_score"],
                },
            })

    return sections
