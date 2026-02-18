"""
Smart Answer Formatter

This module uses the LLM to format query results into natural language answers.
The data is already retrieved accurately - this just formats it nicely.
"""
from __future__ import annotations

from src.llm_client import LLMClient
from src.query_parser import ParsedQuery
from src.query_executor import QueryResult


FORMAT_PROMPT = """You are formatting a database query result into a natural language answer.

USER'S ORIGINAL QUESTION: {question}

WHAT WE UNDERSTOOD: {understood}

QUERY RESULT:
- Success: {success}
- Count: {count}
- Summary: {summary}
- Details: {details}

FORMAT RULES:
1. Use ONLY the data provided above - do not add any information
2. If count is 0 or success is False, say the data is not available
3. Be concise and direct
4. Include specific names/values from the details
5. If listing multiple items, use bullet points or numbered list
6. Do not apologize or add unnecessary filler

Write a natural, helpful response to the user's question using ONLY the data above:"""


def format_answer(
    llm: LLMClient,
    parsed: ParsedQuery,
    result: QueryResult
) -> str:
    """
    Format a query result into a natural language answer.
    
    Args:
        llm: LLM client
        parsed: The parsed query
        result: The query execution result
        
    Returns:
        Natural language answer string
    """
    # For count queries with 0 results, this is a valid answer (not a failure)
    if parsed.intent == "count" and result.count == 0:
        return "0 proposals match the criteria. No records found with the specified condition."
    
    # For list queries with 0 results - also a valid answer
    if parsed.intent == "list" and result.count == 0:
        if parsed.filter_contains:
            return f"0 proposals found with '{parsed.filter_contains}' in the records."
        return "0 proposals match the criteria."
    
    # For non-count/list queries with no results
    if not result.success or result.count == 0:
        return "Data not available in the proposal records."
    
    # For single value lookups, return directly
    if parsed.intent == "lookup" and result.count == 1:
        detail = result.details[0] if result.details else result.summary
        if parsed.quote_id:
            return f"For {parsed.quote_id}: {detail}"
        else:
            return detail
    
    # For multi-field or multi-entity lookups
    if parsed.intent == "lookup" and result.count > 1:
        if result.details:
            return "\n".join(f"- {d}" for d in result.details)
        return result.summary
    
    # For count queries - ONLY show count unless names are explicitly asked
    if parsed.intent == "count":
        query_lower = parsed.raw_query.lower()
        wants_names = any(w in query_lower for w in [
            "name", "names", "which", "list", "who", "what are"
        ])
        
        if result.count > 0 and result.details and wants_names:
            names = result.details[:20]
            if result.count <= 20:
                return f"There are {result.count} proposal(s) that match. Here are their names:\n" + "\n".join(f"- {n}" for n in names)
            else:
                return f"There are {result.count} proposal(s) that match. Here are the first 20:\n" + "\n".join(f"- {n}" for n in names) + f"\n... and {result.count - 20} more."
        else:
            # Just the count — do NOT list names
            return f"{result.count} proposal(s) match the criteria."
    
    # For list queries
    if parsed.intent == "list":
        if result.details:
            items = result.details[:15]  # Limit to 15
            header = f"Found {result.count} matching proposal(s):\n"
            listing = "\n".join(f"- {item}" for item in items)
            if result.count > 15:
                listing += f"\n... and {result.count - 15} more."
            return header + listing
        else:
            return result.summary
    
    # For compare queries
    if parsed.intent == "compare":
        return result.summary
    
    # For complex results, use LLM to format
    prompt = FORMAT_PROMPT.format(
        question=parsed.raw_query,
        understood=parsed.understood_question,
        success=result.success,
        count=result.count,
        summary=result.summary,
        details=result.details[:20] if result.details else []
    )
    
    try:
        response = llm.generate(prompt)
        return response.strip()
    except Exception:
        # Fallback to simple format
        if result.details:
            return result.summary + "\n" + "\n".join(f"- {d}" for d in result.details[:10])
        return result.summary
