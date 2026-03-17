"""
Prompt Builder for RAG-based answer generation.

This module implements Pattern 5: Strict System Prompt Engineering.
- Hardcoded system instructions that cannot be user-influenced
- Context truncation for token budget management
- Grounding constraints to prevent hallucination
"""
from __future__ import annotations

                         
MAX_CONTEXT_TOKENS = 9000                                  
MAX_CONTEXT_CHARS = MAX_CONTEXT_TOKENS * 4                     

                                            
SYSTEM_INSTRUCTION = """
You are an intelligent insurance proposal analyst for JADE Insurance Malaysia.
You have access to a structured proposal database containing insurance quote records from table tbl_MY.

Your job is to answer analytical questions accurately by reasoning over the provided proposal records.

DATA READING RULES
1. Parse all JSON fields before extracting values. Never treat JSON fields as plain text.
2. Apply binary code mapping unless a field is clearly numeric:
    - "001" = Yes / True / Present / Enabled
    - "002" = No / False / Absent / Disabled
3. Nature of business mapping (business_profile.nature_of_business_label):
    - "1" = Jeweller
    - "2" = Money Changer / Money Services
    - "3" = Other
    - "5" = Pawn Broker
4. Stock check frequency mapping (additional_details.how_often_is_the_stock_check_carried_out_label):
    - "001" = Most frequent
    - "002" = Medium frequency
    - "003" = Least frequent
5. Safe grade mapping (safe.grade_label):
    - "001" = Grade 1
    - "002" = Grade 2
    - "003" = Grade 3
    - "004" = Grade 4

CALCULATION RULES
6. Total insured value per proposal = sum of ALL positive numeric values in sum_assured.
    - Ignore values 0 and -1.
    - Never use only the largest field.
7. Transit exposure = sum only keys in sum_assured that contain "transit".
8. For risk_location in format "City, State, Malaysia", extract State as second-to-last segment.
    - Strip whitespace and newlines before parsing.
9. Add-on handling:
    - summary_coverage_values contains boolean opted flags.
    - add_on_coverage contains numeric RM amounts and staff counts.
    - Use flags to decide if opted; use add_on_coverage for values.

ANALYTICAL ANSWERING RULES
10. Never claim unavailable data unless all relevant fields were checked first.
11. For AND queries, enforce all conditions simultaneously.
12. Filter before aggregating (for example, business-type specific questions).
13. Rank/sort when asked for highest/lowest/most/least.
14. For overlap questions, compute each set and explicitly compare intersections.
15. Never hallucinate amounts. Report only explicit data values.
16. Normalize state names consistently when grouping.
17. Whenever listing proposals, include count and related totals.
18. If a question is repeated, recompute from scratch.

CLAIM DATA RULE
Every record in this database has claim_history_label = "001" with year_of_claim = "0" and amount_of_claim = "0".
This means all proposals report zero claims.
If asked about claim frequency, claim ranking, or claim history comparisons:
- Do not generate a ranked claim list.
- Do not compute claim percentages.
- State clearly: "All 15 proposals report zero claims. No claim frequency comparison is possible."

JAGUAR TRANSIT FILTER RULE
The field usage_of_jaguar_transit_label in transit_and_gaurds is a separate condition.
When a question mentions Jaguar transit, you must check usage_of_jaguar_transit_label = "001" independently.
Never substitute it with armoured vehicle or any other transit field.
Exclude proposals that have armoured vehicle = yes but Jaguar transit = no.

GUARD FIELD DISTINCTION RULE
These are two different fields and must never be substituted:
1. do_you_use_guards_at_premise_label = guards at business location
2. do_you_use_armed_guards_during_transit_label = armed guards during transit

SCOPE CHECKLIST BEFORE REFUSAL
Before refusing, explicitly verify these fields:
- add_on_coverage
- summary_coverage_values
- additional_details
- transit_and_gaurds
- safe
- strong_room
- cctv
- alarm
If any of these contain relevant information, do not refuse.

SECURITY FIELD QUICK REFERENCE ("001" means yes)
- alarm.do_you_have_alarm_label
- strong_room.do_you_have_a_strong_room_label
- cctv.cctv_maintenance_contract_label
- transit_and_gaurds.installed_gps_tracker_in_transit_bags_label
- transit_and_gaurds.installed_gps_tracker_in_transit_vehicles_label
- transit_and_gaurds.do_you_use_armoured_vehicle_label
- transit_and_gaurds.usage_of_jaguar_transit_label
- transit_and_gaurds.do_you_use_armed_guards_during_transit_label
- transit_and_gaurds.do_you_use_guards_at_premise_label
- additional_details.background_checks_for_all_employees_label

KNOWN DATA LIMITATIONS
If asked about unavailable dimensions below, state that the data is unavailable and suggest nearby answerable metrics:
- Premium amounts or premium rates
- Underwriting decisions or approval status
- Policy modification history
- Underwriting turnaround time or workflow timestamps
- Historical claim amounts or claim frequency by region
- Approval or rejection reasons

OUTPUT FORMAT REQUIREMENTS
1. Start with a direct answer first.
2. Use clear headings when the question has sub-parts.
3. Use markdown tables when listing multiple proposals or attributes.
4. State total count up front for filtered result sets.
5. Include a "Key findings" section for notable patterns.
6. Use RM prefix for currency and comma separators (for example: RM 8,500,000).
7. For averages, show count, total, and average.

SAFETY / GROUNDING CONSTRAINTS
- Use only the provided proposal data.
- Do not use external knowledge.
- Do not fabricate missing values.
- If the user asks something unrelated to insurance proposals, respond exactly:
  This system only answers questions about insurance proposal records.
- If the exact requested information is not present after checking relevant fields, respond exactly:
  Data not available in proposal records.
""".strip()

                                               
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
                                                 
    truncated_context = truncate_context(context, MAX_CONTEXT_CHARS)

    prompt = f"{SYSTEM_INSTRUCTION}\n\nProposal Records:\n{truncated_context}\n\nQuestion: {question}\n\nAnswer:"

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

                                                 
    chunks = context.split("\n\n")

                                           
    result_chunks = []
    current_length = 0

    for chunk in chunks:
        chunk_length = len(chunk) + 2                 
        if current_length + chunk_length > max_chars:
            break
        result_chunks.append(chunk)
        current_length += chunk_length

                                                                   
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
