"""
Prompt Builder for RAG-based answer generation.

This module implements Pattern 5: Strict System Prompt Engineering.
- Hardcoded system instructions that cannot be user-influenced
- Context truncation for token budget management
- Grounding constraints to prevent hallucination
"""
from __future__ import annotations

# Configuration constants
MAX_CONTEXT_TOKENS = 3000  # Approximate tokens (chars / 4)
MAX_CONTEXT_CHARS = MAX_CONTEXT_TOKENS * 4  # ~12000 characters

# System instruction - NEVER user-influenced
SYSTEM_INSTRUCTION = """You are an insurance data assistant for JA Assure. Answer ONLY from the proposal records provided below. Do not infer, assume, extrapolate, or use any knowledge outside the provided context. If the exact data needed to answer is not present, respond with exactly: Data not available in proposal records. Be concise. Output plain text only. No markdown, no bullet points, no bold, no numbered lists."""

# Refusal message for when no data is available
REFUSAL_MESSAGE = "Data not available in proposal records."


def build_prompt(context: str, question: str) -> str:
    """
    Build a complete prompt for LLM generation with strict grounding.

    Args:
        context: The decoded, retrieved proposal text chunks joined with newlines.
        question: The user's question.

    Returns:
        A formatted prompt string with system instruction, context, and question.
    """
    # Truncate context if it exceeds token budget
    truncated_context = truncate_context(context, MAX_CONTEXT_CHARS)

    prompt = f"""{SYSTEM_INSTRUCTION}

=== PROPOSAL RECORDS ===
{truncated_context}
=== END OF RECORDS ===

Question: {question}

Answer:"""

    return prompt


def build_prompt_with_chunks(chunks: list[dict], question: str) -> str:
    """
    Build a prompt from a list of retrieved chunk dictionaries.

    Args:
        chunks: List of chunk dicts with 'text' key containing the chunk text.
        question: The user's question.

    Returns:
        A formatted prompt string.
    """
    if not chunks:
        return ""

    # Join chunk texts with double newlines
    context_parts = []
    for chunk in chunks:
        if isinstance(chunk, dict) and "text" in chunk:
            context_parts.append(chunk["text"])
        elif isinstance(chunk, str):
            context_parts.append(chunk)

    context = "\n\n".join(context_parts)
    return build_prompt(context, question)


def truncate_context(context: str, max_chars: int) -> str:
    """
    Truncate context to fit within token budget, keeping complete chunks.

    Args:
        context: The full context string.
        max_chars: Maximum characters allowed.

    Returns:
        Truncated context string.
    """
    if len(context) <= max_chars:
        return context

    # Split by double newlines (chunk boundaries)
    chunks = context.split("\n\n")

    # Keep chunks until we exceed the limit
    result_chunks = []
    current_length = 0

    for chunk in chunks:
        chunk_length = len(chunk) + 2  # +2 for "\n\n"
        if current_length + chunk_length > max_chars:
            break
        result_chunks.append(chunk)
        current_length += chunk_length

    # If we couldn't fit any complete chunk, truncate the first one
    if not result_chunks and chunks:
        return chunks[0][:max_chars] + "..."

    return "\n\n".join(result_chunks)


def build_analytical_prompt(question: str, data_summary: str) -> str:
    """
    Build a prompt for analytical query summarization.

    Args:
        question: The user's analytical question.
        data_summary: Pre-computed data summary from Pandas operations.

    Returns:
        A formatted prompt for natural language formatting of analytical results.
    """
    prompt = f"""You are an insurance data assistant. Format the following analytical result as a clear, concise answer. Do not add any information not present in the data. Output plain text only.

Question: {question}

Analytical Result:
{data_summary}

Formatted Answer:"""

    return prompt


def get_refusal_message() -> str:
    """
    Get the standard refusal message for unavailable data.

    Returns:
        The refusal message string.
    """
    return REFUSAL_MESSAGE


def estimate_tokens(text: str) -> int:
    """
    Estimate the number of tokens in a text string.

    Uses the approximation of 4 characters per token.

    Args:
        text: The text to estimate tokens for.

    Returns:
        Estimated token count.
    """
    return len(text) // 4
