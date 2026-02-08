import json
import re

SMART_QUOTES = {
    "“": '"',
    "”": '"',
    "‘": "'",
    "’": "'"
}

def normalize_quotes(text: str) -> str:
    for k, v in SMART_QUOTES.items():
        text = text.replace(k, v)
    return text

def parse_json_cell(cell):
    if cell is None or (isinstance(cell, float)):
        return None

    if isinstance(cell, dict) or isinstance(cell, list):
        return cell

    try:
        cleaned = normalize_quotes(str(cell).strip())
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return None
