from src.schemas import SECTION_SCHEMAS
from src.mappings import FIELD_MAPPINGS, decode_record


def build_section_text(chunk: dict) -> str:
    section = chunk["section"]
    raw_data = chunk["data"]
    quote_id = chunk["quote_id"]

    data = decode_record(raw_data, section)

    schema = SECTION_SCHEMAS.get(section)
    if not schema:
        schema = {
            "title": section.replace("_", " ").title()
        }

    mappings = FIELD_MAPPINGS.get(section, {})

    lines = []
    lines.append(f"Proposal {quote_id} – {schema['title']}:")

    def has_value(value) -> bool:
        return value not in [None, "", [], {}, -1, "-1", 0, "0"]

    def label_for(key: str) -> str:
        return mappings.get(key, key.replace("_", " ").title())

    # Special handling for claim_history section (mixed dict with nested list)
    if section == "claim_history" and isinstance(data, dict):
        claim_status = data.get("claim_history_label")
        if has_value(claim_status):
            lines.append(f"Claim Status: {claim_status}")
        
        additional_details = data.get("additional_details", [])
        if isinstance(additional_details, list):
            valid_claims = [
                item for item in additional_details
                if isinstance(item, dict) and has_value(item.get("year_of_claim_label"))
            ]
            for i, claim in enumerate(valid_claims, start=1):
                lines.append(f"Claim {i}:")
                year = claim.get("year_of_claim_label")
                amount = claim.get("amount_of_claim_label")
                desc = claim.get("description_label")
                if has_value(year):
                    lines.append(f"- Year: {year}")
                if has_value(amount):
                    lines.append(f"- Amount: {amount}")
                if has_value(desc):
                    lines.append(f"- Description: {desc}")
        
        return "\n".join(lines)

    # Array sections (e.g. claim history)
    if schema.get("array") or isinstance(data, list):
        if not data:
            lines.append("No records available.")
        else:
            for i, item in enumerate(data, start=1):
                lines.append(f"Item {i}:")
                if isinstance(item, dict):
                    for key, value in item.items():
                        label = label_for(key)
                        if label and has_value(value):
                            lines.append(f"- {label}: {value}")
                else:
                    if has_value(item):
                        lines.append(f"- Value: {item}")
        return "\n".join(lines)

    # Object sections
    if isinstance(data, dict):
        for key, value in data.items():
            label = label_for(key)
            if label and has_value(value):
                lines.append(f"{label}: {value}")
    else:
        if has_value(data):
            lines.append(f"Value: {data}")

    return "\n".join(lines)
