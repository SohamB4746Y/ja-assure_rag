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

def validate_proposal_completeness(row_data: dict) -> dict:
    """
    Assess how complete a proposal submission is by checking key sections.

    Returns a dict with:
    - is_complete_submission: bool — True if the business submitted meaningful data
    - completeness_score: float 0.0–1.0 — fraction of key sections populated
    - submission_status: "complete" | "partial" | "empty"
    - missing_sections: list of section names with no data
    """
    KEY_SECTIONS = [
        "business_profile", "sum_assured", "cctv", "alarm",
        "transit_and_gaurds", "claim_history", "additional_details"
    ]
    import json
    populated = 0
    missing = []
    for section in KEY_SECTIONS:
        val = row_data.get(section)
        if val is None:
            missing.append(section)
            continue
        # Guard against pandas NaN (float NaN from empty Excel cells)
        if isinstance(val, float):
            try:
                import math
                if math.isnan(val):
                    missing.append(section)
                    continue
            except (TypeError, ValueError):
                pass
        try:
            parsed = json.loads(str(val)) if isinstance(val, str) else val
        except Exception:
            missing.append(section)
            continue
        if isinstance(parsed, dict):
            if any(v not in [None, "", -1, "-1", "null", 0] for v in parsed.values()):
                populated += 1
            else:
                missing.append(section)
        elif isinstance(parsed, list):
            if len(parsed) > 0:
                populated += 1
            else:
                missing.append(section)
        else:
            if parsed not in [None, "", -1, "-1", "null", 0]:
                populated += 1
            else:
                missing.append(section)
    score = populated / float(len(KEY_SECTIONS))
    if score == 0.0:
        status = "empty"
    elif score < 0.4:
        status = "partial"
    else:
        status = "complete"
    return {
        "is_complete_submission": score >= 0.4,
        "completeness_score": round(score, 2),
        "submission_status": status,
        "missing_sections": missing
    }


def extract_sections(row: dict, json_parser):
    sections = []

    base_metadata = {
        "quote_id": row.get("quote_id"),
        "risk_location": row.get("risk_location"),
        "user_name": row.get("user_name"),
    }

    # Validate completeness for this proposal row (called once per row)
    completeness = validate_proposal_completeness(row)

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
                    "is_complete_submission": completeness["is_complete_submission"],
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
                    "is_complete_submission": completeness["is_complete_submission"],
                    "completeness_score": completeness["completeness_score"],
                },
            })

    return sections
