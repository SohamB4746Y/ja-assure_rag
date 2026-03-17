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

                      
    result = re.sub(r"<[^>]*>", "", result)

                          
    result = re.sub(r"&[a-zA-Z0-9#]+;", " ", result)

                                                 
    result = re.sub(r"\*\*([^*]+)\*\*", r"\1", result)
    result = re.sub(r"__([^_]+)__", r"\1", result)

                                               
                                                         
    result = re.sub(r"(?<!\w)\*([^*]+)\*(?!\w)", r"\1", result)

                                    
    result = re.sub(r"`([^`]*)`", r"\1", result)

                                 
    result = re.sub(r"```[^`]*```", "", result, flags=re.DOTALL)

                                             
    result = re.sub(r"^#{1,6}\s+", "", result, flags=re.MULTILINE)

                                                   
    result = re.sub(r"^\s*[\-\•\*\+]\s+", "", result, flags=re.MULTILINE)

                                                    
    result = re.sub(r"^\s*\d+\.\s+", "", result, flags=re.MULTILINE)

                                                             
    result = re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", result)

                                        
    result = re.sub(r"!\[([^\]]*)\]\([^\)]+\)", r"\1", result)

                                         
    result = re.sub(r"\n{3,}", "\n\n", result)

                                              
    result = re.sub(r" {2,}", " ", result)

                                        
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

                                            
    result = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL | re.IGNORECASE)

                                              
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

                              
    result = text.replace("\\", "\\\\")

                          
    result = result.replace('"', '\\"')

                     
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
