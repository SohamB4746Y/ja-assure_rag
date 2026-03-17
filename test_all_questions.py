"""Test all 24 questions against the RAG system and report responses."""
import os
import sys

os.environ.setdefault("TRANSFORMERS_VERBOSITY", "error")
os.environ["HF_HUB_OFFLINE"] = "1"
os.environ["TRANSFORMERS_OFFLINE"] = "1"

from main import initialize_system, handle_query
from src.query_parser import QueryParser

QUESTIONS = [
    "Which high-value proposals (above RM 3M) have weak security setups — specifically those lacking either an alarm system, a strong room, or GPS trackers in vehicles?",
    "Among jewellers in Malaysia, which ones have the strongest combined security profile — having alarms, strong rooms, CCTV with maintenance contracts, and GPS in both bags and vehicles?",
    "Which proposals have opted for director's house coverage AND fidelity guarantee insurance, and what is the total combined add-on coverage value for each?",
    "What is the total fidelity guarantee insurance exposure across all Malaysia proposals, and which business type carries the highest average fidelity guarantee amount?",
    "Which states in Malaysia have more than one proposal, and what is the total insured value per state?",
    "Compare the average insured stock value between jewellers and money changers in Malaysia.",
    "Which proposals use armoured vehicles AND Jaguar transit services AND armed guards during transit — and what are their total insured transit values?",
    "Which proposals conduct background checks on all employees AND perform stock checks most frequently? Do these overlap with the highest insured value proposals?",
    "Which proposals have the highest number of staff covered under fidelity guarantee insurance, and do those businesses also have director's house coverage enabled?",
    "Which proposals conduct background checks on all employees AND perform stock checks most frequently?",
    "Which proposals do NOT have an alarm system, and what are their total insured values?",
    "List all proposals where GPS trackers are installed in transit bags but NOT in transit vehicles.",
    "Which pawn brokers in Malaysia have a strong room AND a Grade 3 or higher safe?",
    "What is the total director's house coverage value across all proposals that have it enabled?",
    "Which money changers have a CCTV maintenance contract and armed guards at the premise?",
    "Which proposals have a safe of Grade 4 but no strong room — and what are their insured values?",
    "Rank all business types by their average total insured value. Which type carries the most risk per proposal?",
    "Which proposals have the highest fidelity guarantee amount per staff member covered?",
    "How many proposals per state have both an alarm and a strong room?",
    "Which proposals have stock out of safe exceeding RM 200,000, and do they have a Grade 4 safe?",
    "What is the average premium charged to jewellers in Malaysia?",
    "Which proposals were approved by the underwriter?",
    "Which region has the highest claim frequency?",
    "What is the total revenue generated from Malaysia proposals this quarter?",
]

SEPARATOR = "=" * 70


def main():
    embedder, llm, qa_store, analytical_engine, metadata = initialize_system()
    query_parser = QueryParser(llm, metadata=metadata)

    print(f"\n{SEPARATOR}")
    print(f"  RUNNING {len(QUESTIONS)} TEST QUESTIONS")
    print(f"{SEPARATOR}\n")

    for i, question in enumerate(QUESTIONS, 1):
        print(f"\n{SEPARATOR}")
        print(f"Q{i}: {question}")
        print(SEPARATOR)

        # Use a fresh parser per question to avoid cross-contamination
        fresh_parser = QueryParser(llm, metadata=metadata)
        answer = handle_query(question, embedder, llm, qa_store, analytical_engine, fresh_parser)

        print(f"A{i}: {answer}")

    print(f"\n{SEPARATOR}")
    print("  ALL QUESTIONS COMPLETED")
    print(f"{SEPARATOR}\n")


if __name__ == "__main__":
    main()
