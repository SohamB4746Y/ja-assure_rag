"""
Output Cleaner for LLM response sanitization.

This module implements Pattern 6: Output Sanitization.
Strips HTML, markdown, and formatting artifacts from every LLM response.
"""

import re


def clean_output(text: str) -> str:
    """
    Clean and sanitize LLM output by removing formatting artifacts.

    Removes:
    - Markdown bold (**text**)
    - Markdown italic (*text* or _text_)
    - Backticks (inline code)
    - Pound signs (headers)
    - HTML tags
    - Excess newlines (collapse to max 2)
    - Leading/trailing whitespace
    - Bullet points and numbered lists markers

    Args:
        text: Raw LLM output text.

    Returns:
        Cleaned plain text string.
    """
    if not text or not isinstance(text, str):
        return text or ""

    result = text

    # Remove HTML tags
    result = re.sub(r"<[^>]*>", "", result)

    # Remove HTML entities
    result = re.sub(r"&[a-zA-Z0-9#]+;", " ", result)

    # Remove markdown bold (**text** or __text__)
    result = re.sub(r"\*\*([^*]+)\*\*", r"\1", result)
    result = re.sub(r"__([^_]+)__", r"\1", result)

    # Remove markdown italic (*text* or _text_)
    # Be careful not to remove underscores in field names
    result = re.sub(r"(?<!\w)\*([^*]+)\*(?!\w)", r"\1", result)

    # Remove backticks (inline code)
    result = re.sub(r"`([^`]*)`", r"\1", result)

    # Remove markdown code blocks
    result = re.sub(r"```[^`]*```", "", result, flags=re.DOTALL)

    # Remove markdown headers (# ## ### etc.)
    result = re.sub(r"^#{1,6}\s+", "", result, flags=re.MULTILINE)

    # Remove bullet point markers at start of lines
    result = re.sub(r"^\s*[\-\•\*\+]\s+", "", result, flags=re.MULTILINE)

    # Remove numbered list markers at start of lines
    result = re.sub(r"^\s*\d+\.\s+", "", result, flags=re.MULTILINE)

    # Remove markdown links but keep text [text](url) -> text
    result = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", result)

    # Remove markdown images ![alt](url)
    result = re.sub(r"!\[([^\]]*)\]\([^\)]+\)", r"\1", result)

    # Collapse multiple newlines to max 2
    result = re.sub(r"\n{3,}", "\n\n", result)

    # Collapse multiple spaces to single space
    result = re.sub(r" {2,}", " ", result)

    # Remove leading/trailing whitespace
    result = result.strip()

    return result


def normalize_whitespace(text: str) -> str:
    """
    Normalize all whitespace in text to single spaces.

    Args:
        text: Input text.

    Returns:
        Text with normalized whitespace.
    """
    if not text:
        return ""

    # Replace all whitespace sequences with single space
    result = re.sub(r"\s+", " ", text)
    return result.strip()


def remove_thinking_tags(text: str) -> str:
    """
    Remove any thinking/reasoning tags that some LLMs include.

    Args:
        text: LLM output text.

    Returns:
        Text with thinking tags removed.
    """
    if not text:
        return ""

    # Remove <thinking>...</thinking> blocks
    result = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL | re.IGNORECASE)

    # Remove <reasoning>...</reasoning> blocks
    result = re.sub(r"<reasoning>.*?</reasoning>", "", result, flags=re.DOTALL | re.IGNORECASE)

    return result.strip()


def extract_answer_only(text: str) -> str:
    """
    Extract just the answer if the LLM includes extra explanation.

    Some LLMs prefix answers with "Answer:" or similar.

    Args:
        text: LLM output text.

    Returns:
        Extracted answer portion.
    """
    if not text:
        return ""

    # Check for "Answer:" prefix and extract what follows
    answer_match = re.search(r"(?:Answer|Response|Result):\s*(.+)", text, flags=re.IGNORECASE | re.DOTALL)
    if answer_match:
        return answer_match.group(1).strip()

    return text


def sanitize_for_json(text: str) -> str:
    """
    Sanitize text for safe JSON serialization.

    Args:
        text: Input text.

    Returns:
        JSON-safe text string.
    """
    if not text:
        return ""

    # Escape backslashes first
    result = text.replace("\\", "\\\\")

    # Escape double quotes
    result = result.replace('"', '\\"')

    # Escape newlines
    result = result.replace("\n", "\\n")
    result = result.replace("\r", "\\r")
    result = result.replace("\t", "\\t")

    return result


def full_clean(text: str) -> str:
    """
    Apply all cleaning operations in sequence.

    Args:
        text: Raw LLM output.

    Returns:
        Fully cleaned and sanitized text.
    """
    if not text:
        return ""

    result = remove_thinking_tags(text)
    result = clean_output(result)
    result = extract_answer_only(result)
    result = normalize_whitespace(result)

    return result
