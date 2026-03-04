"""
JSON Cleaner: Robust Parser for Excel-Embedded JSON

This module handles the parsing of JSON data embedded in Excel cells, which often
contains formatting issues like smart quotes, leading BOM characters, trailing commas,
and NaN float values.

Parse Strategy:
    1. Defensive sanitization (smart quotes -> ASCII, strip BOM, remove control chars)
    2. Direct json.loads() attempt
    3. Fallback with trailing comma removal
    4. Never raises exceptions - always returns None on parse failures

Common Issues Handled:
    - Smart quotes (U+201C, U+201D) from Excel copy-paste
    - Byte order mark (BOM) prefix
    - Single quotes instead of double quotes
    - Trailing commas before closing braces/brackets
    - NaN float values (returns None)
    - Control characters in strings

Usage:
    >>> from loader.json_cleaner import parse_json_cell
    >>> data = parse_json_cell(excel_cell_value)
    >>> if data:  # None if parse failed
    ...     process(data)
"""

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
