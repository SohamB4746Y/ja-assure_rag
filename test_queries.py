"""Quick verification of the 5 bug fixes against 8 test queries."""
import sys
import os

# Suppress noisy logs and enable offline mode
os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

from main import initialize_system, handle_query
from src.query_parser import QueryParser

TEST_QUERIES = [
    {
        "q": "What type of business does Suresh Kumar run?",
        "expect_field": "nature_of_business",
        "reject_field": "business_name",
    },
    {
        "q": "Does Heritage Gold and Jewels have a CCTV maintenance contract?",
        "expect_field": "cctv_maintenance",
        "reject_field": "shop_lifting",
    },
    {
        "q": "What is the door access type used by Global Money Exchange?",
        "expect_field": "door_access",
        "reject_field": "business_name",
    },
    {
        "q": "Does Rapid FX Money Exchange use armed guards during transit?",
        "expect_field": "armed_guard",
        "reject_field": None,
    },
    {
        "q": "What background checks does LuxGold Jewellers do?",
        "expect_field": "background_check",
        "reject_field": "business_name",
    },
    {
        "q": "What is the claim history of Heritage Gold and Jewels?",
        "expect_field": "claim",
        "reject_field": None,
    },
    {
        "q": "Does Royal Gems and Jewels keep detailed records of stock movements?",
        "expect_field": "stock",
        "reject_field": None,
    },
    {
        "q": "What type of CCTV backup does Secure Pawn Brokers use?",
        "expect_field": "back_up",
        "reject_field": "director_house",
    },
]


def main():
    embedder, llm, qa_store, analytical_engine, metadata = initialize_system()
    parser = QueryParser(llm)

    passed = 0
    failed = 0

    for i, t in enumerate(TEST_QUERIES, 1):
        query = t["q"]
        print(f"\n{'='*60}")
        print(f"Q{i}: {query}")
        print(f"{'='*60}")

        # Parse to inspect what the LLM returned
        parsed = parser.parse(query)
        print(f"  [PARSE] intent={parsed.intent}  fields={parsed.target_fields}")
        print(f"          filter_field={parsed.filter_field}  filter_value={parsed.filter_value}")
        print(f"          filter_contains={parsed.filter_contains}")

        # Reset parser history between independent queries to avoid context bleed
        parser.conversation_history.clear()

        # Get actual answer
        parser2 = QueryParser(llm)
        answer = handle_query(query, embedder, llm, qa_store, analytical_engine, parser2)
        print(f"  [ANSWER] {answer[:200]}")

        # Check
        answer_lower = answer.lower()
        ok = True
        if t["expect_field"] and t["expect_field"].lower() not in " ".join(parsed.target_fields).lower() and t["expect_field"].lower() not in answer_lower:
            print(f"  [FAIL] Expected field containing '{t['expect_field']}' not found in output_fields or answer")
            ok = False
        if t["reject_field"] and t["reject_field"].lower() in " ".join(parsed.target_fields).lower():
            print(f"  [FAIL] Rejected field '{t['reject_field']}' found in output_fields")
            ok = False
        if ok:
            print(f"  [PASS]")
            passed += 1
        else:
            failed += 1

    print(f"\n{'='*60}")
    print(f"RESULTS: {passed}/{passed+failed} passed")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
