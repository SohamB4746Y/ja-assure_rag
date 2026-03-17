from __future__ import annotations

from typing import Any


BUSINESS_TYPE_MAP = {
    "1": "Jeweller",
    "2": "Money Changer",
    "3": "Other",
    "5": "Pawn Broker",
}

STOCK_CHECK_FREQ_MAP = {
    "001": "Most Frequent",
    "002": "Medium Frequency",
    "003": "Least Frequent",
}

SAFE_GRADE_MAP = {
    "001": "Grade 1",
    "002": "Grade 2",
    "003": "Grade 3",
    "004": "Grade 4",
}


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    raw = str(value).replace(",", "").replace("RM", "").strip()
    if raw in ("", "-1"):
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _is_positive_number(value: Any) -> bool:
    return _to_float(value) > 0


def _yes_no(value: Any) -> str:
    if value is None:
        return "UNKNOWN"
    raw = str(value).strip().lower()
    if raw in ("001", "1", "yes", "true"):
        return "YES"
    if raw in ("002", "2", "no", "false"):
        return "NO"
    return str(value).strip().upper() if str(value).strip() else "UNKNOWN"


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    return raw in ("true", "1", "yes", "001")


def _extract_state(risk_location: str) -> str:
    if not risk_location:
        return "Unknown"
    cleaned = " ".join(str(risk_location).replace("\n", " ").split())
    parts = [part.strip() for part in cleaned.split(",") if part.strip()]
    if not parts:
        return "Unknown"
    if len(parts) >= 2 and parts[-1].lower() == "malaysia":
        return parts[-2]
    if len(parts) >= 2:
        return parts[-1]
    return parts[0]


def _labelize_sum_key(key: str) -> str:
    s = key.replace("_label", "").replace("_", " ").strip().title()
    s = s.replace(" Of ", " of ").replace(" In ", " in ").replace(" And ", " and ")
    return s


def _is_monetary_sum_key(key: str) -> bool:
    k = str(key).lower()
    if "nature_of_business" in k:
        return False
    monetary_signals = ("stock", "cash", "sum", "value", "limit", "transit")
    return any(signal in k for signal in monetary_signals)


def build_flattened_proposal_context(metadata: list[dict], quote_id_filter: str | None = None) -> str:
    """
    Build deterministic, pre-parsed proposal records for the LLM context.

    This removes JSON parsing burden from the model by flattening all relevant
    fields into explicit lines per proposal.
    """
    proposals: dict[str, dict[str, Any]] = {}

    for chunk in metadata:
        qid = chunk.get("quote_id")
        if not qid:
            continue
        if quote_id_filter and qid != quote_id_filter:
            continue

        if qid not in proposals:
            proposals[qid] = {
                "quote_id": qid,
                "risk_location": str(chunk.get("risk_location", "") or ""),
                "business_profile": {},
                "sum_assured": {},
                "alarm": {},
                "strong_room": {},
                "cctv": {},
                "transit_and_gaurds": {},
                "additional_details": {},
                "add_on_coverage": {},
                "summary_coverage_values": {},
                "safe": {},
            }

        rec = proposals[qid]
        if chunk.get("risk_location"):
            rec["risk_location"] = str(chunk.get("risk_location") or "")

        section = chunk.get("section")
        fields = chunk.get("fields")
        if isinstance(fields, dict) and section in rec:
            rec[section].update(fields)

    lines: list[str] = ["PROPOSAL RECORDS:"]
    for qid in sorted(proposals.keys()):
        rec = proposals[qid]
        bp = rec["business_profile"]
        sa = rec["sum_assured"]
        alarm = rec["alarm"]
        sr = rec["strong_room"]
        cctv = rec["cctv"]
        tg = rec["transit_and_gaurds"]
        ad = rec["additional_details"]
        addon = rec["add_on_coverage"]
        scv = rec["summary_coverage_values"]
        safe = rec["safe"]

        business_name = str(bp.get("business_name_label", "Unknown") or "Unknown")
        business_code = str(bp.get("nature_of_business_label", "") or "")
        business_type = BUSINESS_TYPE_MAP.get(business_code, "Unknown")
        state = _extract_state(rec["risk_location"])

        sum_components: list[tuple[str, float]] = []
        total_insured = 0.0
        transit_insured = 0.0
        for key, value in sa.items():
            if not _is_monetary_sum_key(key):
                continue
            if not _is_positive_number(value):
                continue
            amount = _to_float(value)
            total_insured += amount
            if "transit" in str(key).lower():
                transit_insured += amount
            sum_components.append((_labelize_sum_key(str(key)), amount))

        stock_check_code = str(ad.get("how_often_is_the_stock_check_carried_out_label", "") or "")
        stock_check_text = STOCK_CHECK_FREQ_MAP.get(stock_check_code, "Unknown")

        safe_grade_code = str(safe.get("grade_label", "") or "")
        safe_grade_text = SAFE_GRADE_MAP.get(safe_grade_code, f"Code {safe_grade_code}" if safe_grade_code else "Unknown")

        fidelity_amount = _to_float(addon.get("fidelity_guarantee_insurance_label"))
        director_amount = _to_float(addon.get("director_house_coverage_label"))
        staff_count_raw = addon.get("fidelity_guarantee_total_staff_label", "")
        try:
            fidelity_staff_count = int(str(staff_count_raw).strip()) if str(staff_count_raw).strip() else 0
        except ValueError:
            fidelity_staff_count = 0

        lines.append("")
        lines.append(f"[{qid}]")
        lines.append(f"Business: {business_name}")
        lines.append(f"Type: {business_type} (code {business_code or 'Unknown'})")
        lines.append(f"State: {state}")
        lines.append(f"Total Insured Value: RM {total_insured:,.0f}")
        for component_name, amount in sorted(sum_components, key=lambda x: x[0]):
            lines.append(f"  - {component_name}: RM {amount:,.0f}")
        lines.append(f"Transit Insured Value: RM {transit_insured:,.0f}")

        lines.append(f"Alarm: {_yes_no(alarm.get('do_you_have_alarm_label'))}")
        lines.append(f"Strong Room: {_yes_no(sr.get('do_you_have_a_strong_room_label'))}")
        lines.append(f"CCTV Maintenance Contract: {_yes_no(cctv.get('cctv_maintenance_contract_label'))}")
        lines.append(f"GPS Bags: {_yes_no(tg.get('installed_gps_tracker_in_transit_bags_label'))}")
        lines.append(f"GPS Vehicles: {_yes_no(tg.get('installed_gps_tracker_in_transit_vehicles_label'))}")
        lines.append(f"Armoured Vehicle: {_yes_no(tg.get('do_you_use_armoured_vehicle_label'))}")
        lines.append(f"Jaguar Transit: {_yes_no(tg.get('usage_of_jaguar_transit_label'))}")
        lines.append(f"Armed Guards Transit: {_yes_no(tg.get('do_you_use_armed_guards_during_transit_label'))}")
        lines.append(f"Guards at Premise: {_yes_no(tg.get('do_you_use_guards_at_premise_label'))}")
        lines.append(f"Background Check: {_yes_no(ad.get('background_checks_for_all_employees_label'))}")
        lines.append(f"Stock Check Frequency: {stock_check_code or 'Unknown'} ({stock_check_text})")
        lines.append(f"Fidelity Amount: RM {fidelity_amount:,.0f}")
        lines.append(f"Fidelity Staff Count: {fidelity_staff_count}")
        lines.append(f"Director House Amount: RM {director_amount:,.0f}")
        lines.append(f"Director House Opted: {'YES' if _to_bool(scv.get('director_house_coverage_label')) else 'NO'}")
        lines.append(f"Fidelity Opted: {'YES' if _to_bool(scv.get('fidelity_guarantee_insurance_label')) else 'NO'}")
        lines.append(f"Safe Grade: {safe_grade_text}")
        lines.append(f"Safe Model: {str(safe.get('safe_model_label', 'Unknown') or 'Unknown')}")
        lines.append(f"Safe Brand: {str(safe.get('safe_brand_name_label', 'Unknown') or 'Unknown')}")

    if len(lines) == 1:
        lines.append("No proposal records found.")

    return "\n".join(lines)