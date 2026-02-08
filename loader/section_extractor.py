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

def extract_sections(row: dict, json_parser):
    sections = []

    base_metadata = {
        "quote_id": row.get("quote_id"),
        "risk_location": row.get("risk_location"),
        "user_name": row.get("user_name"),
    }

    for section in SECTION_COLUMNS:
        parsed = json_parser(row.get(section))
        if parsed:
            sections.append({
                "quote_id": base_metadata["quote_id"],
                "section": section,
                "data": parsed,
                "metadata": base_metadata
            })

    return sections
