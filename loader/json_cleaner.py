import json
import re
import math
import logging

logger = logging.getLogger("ja_assure_rag.json_cleaner")

SMART_QUOTES = {
    "\u201c": '"',
    "\u201d": '"',
    "\u2018": "'",
    "\u2019": "'",
}


def normalize_quotes(text: str) -> str:
    for k, v in SMART_QUOTES.items():
        text = text.replace(k, v)
    return text


def _sanitize_json_string(raw: str) -> str:
    """Apply all defensive normalisations to a raw JSON string."""
    s = raw.strip()
    s = normalize_quotes(s)
    s = s.lstrip("\ufeff")
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", s)
    if s.startswith("{'") or s.startswith("['"):
        s = s.replace("'", '"')
    return s


def parse_json_cell(cell):
    """
    Parse a single Excel cell that is expected to contain JSON.
    Returns the parsed Python object (dict / list) or None on failure.
    Never raises.
    """
    if cell is None:
        return None

    if isinstance(cell, float):
        if math.isnan(cell):
            return None
        return None

    if isinstance(cell, (dict, list)):
        return cell

    raw = str(cell).strip()
    if not raw:
        return None

    sanitized = _sanitize_json_string(raw)

    try:
        return json.loads(sanitized)
    except json.JSONDecodeError:
        pass

    try:
        fixed = re.sub(r",\s*([}\]])", r"\1", sanitized)
        return json.loads(fixed)
    except json.JSONDecodeError as exc:
        logger.error("JSON parse failure: %s -- first 200 chars: %s", exc, sanitized[:200])
        return None
