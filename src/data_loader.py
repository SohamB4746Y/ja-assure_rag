import pandas as pd
import json
from .mappings import YES_NO_MAP, BUSINESS_TYPE_MAP
from .validator import validate_sum_assured


def load_excel(path: str):
    return pd.read_excel(path)


def parse_json(cell):
    if pd.isna(cell):
        return {}
    if isinstance(cell, dict):
        return cell
    try:
        return json.loads(cell)
    except Exception:
        return {}


def build_document(row) -> dict:
    record_id = row["quote_id"]

    business = parse_json(row["business_profile"])
    security = parse_json(row["cctv"])
    alarm = parse_json(row["alarm"])
    sum_assured = parse_json(row["sum_assured"])
    claims = parse_json(row["claim_history"])

    if not validate_sum_assured(sum_assured):
        return None

    text = f"""
Proposal ID: {record_id}

Business Details:
- Business Name: {business.get('business_name_label', '')}
- Business Type: {BUSINESS_TYPE_MAP.get(business.get('nature_of_business_label', ''), 'Unknown')}
- Contact Email: {business.get('correspondence_email_label', '')}

Risk Location:
- Address: {row.get('risk_location', '')}

Security:
- CCTV Installed: {YES_NO_MAP.get(security.get('recording_label', ''), 'Unknown')}
- Alarm System Present: {YES_NO_MAP.get(alarm.get('do_you_have_alarm_label', ''), 'Unknown')}

Claims History:
- Claims Present: {YES_NO_MAP.get(claims.get('claim_history_label', ''), 'Unknown')}
"""

    return {
        "record_id": record_id,
        "text": text.strip()
    }


def load_documents(excel_path: str):
    df = load_excel(excel_path)
    documents = []

    for _, row in df.iterrows():
        doc = build_document(row)
        if doc:
            documents.append(doc)

    return documents
