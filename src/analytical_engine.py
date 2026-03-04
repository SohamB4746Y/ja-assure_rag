"""
AnalyticalEngine: Deterministic, Pure-Python Analytics

This module provides the single source of truth for all numeric and aggregate
queries over the 15 insurance proposals. No LLMs are consulted for data values;
they are only used downstream for formatting output into natural language.

Architecture:
    - Ingests FAISS metadata chunks at initialization
    - Collapses chunks into one record per unique quote_id (15 records total)
    - Industry is derived from which sum_assured field is populated
    - All methods return structured data (dicts/lists) formatted by private helpers
    - The run() dispatcher routes queries to appropriate methods via pattern matching

Key Methods:
    - get_company_policy_counts(): Companies ranked by policy count
    - get_average_claim_amount(): Avg claim amount across proposals with claims
    - get_average_underwriting_tat(): Avg days from payment to creation (17.6 days)
    - get_regions_by_claim_frequency(): Regions ranked by claim rate
    - get_top_insured_policies(): Top N proposals by insured value
    - get_industry_totals(): Totals and averages by industry
    - get_claim_stats_by_region(): Claim counts by state
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional, List, Dict, Any

from src.mappings import decode_field

logger = logging.getLogger("ja_assure_rag.analytical_engine")

_YES = frozenset({"001", "1", "yes", "true"})
_NO = frozenset({"002", "2", "no", "false"})

def _yn(raw) -> str:
    """Normalise a yes/no raw value."""
    if raw is None:
        return ""
    s = str(raw).strip().lower()
    if s in _YES:
        return "Yes"
    if s in _NO:
        return "No"
    return str(raw).strip()

def _safe_float(val) -> float:
    """Best-effort conversion to float; returns 0.0 on failure."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).replace(",", "").replace("RM", "").replace("$", "").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0

def _extract_state(risk_location: str) -> str:
    if not risk_location:
        return "Unknown"
    parts = [p.strip() for p in risk_location.split(",")]
    if len(parts) >= 3:
        return parts[-2]
    if len(parts) == 2:
        return parts[-1]
    return parts[0]

def _extract_city(risk_location: str) -> str:
    if not risk_location:
        return "Unknown"
    parts = [p.strip() for p in risk_location.split(",")]
    return parts[0] if parts else "Unknown"

def _parse_date(raw) -> Optional[datetime]:
    """Best-effort parse of a date value from Excel/metadata."""
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat", ""):
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None

# Main class

class AnalyticalEngine:
    """
    Deterministic analytics engine for insurance proposal data.
    
    Processes 15 Malaysian insurance proposals from FAISS metadata. Each proposal
    is collapsed from multiple section chunks into a single record with all relevant
    fields extracted and normalized.
    
    Attributes:
        metadata (list[dict]): Raw FAISS metadata chunks (one per section)
        records (list[dict]): Processed proposal records (one per quote_id, length 15)
    
    Industry Classification:
        - Jewellery & Gold: maximum_stock_in_premises > 0
        - Money Services: maximum_stock_foreign_currency > 0
        - Pawnbrokers: value_of_pledged_stock or value_of_cash_in_premise > 0
    """

    def __init__(self, decoded_df=None, metadata: list[dict] = None):
        self.metadata = metadata or []
        self.records: List[Dict[str, Any]] = []
        self._build_records()

    # Internal: collapse per-chunk metadata into per-proposal records
    def _build_records(self) -> None:
        """Collapse metadata chunks into one record per quote_id."""
        proposals: Dict[str, Dict[str, Any]] = {}

        for chunk in self.metadata:
            qid = chunk.get("quote_id")
            if not qid:
                continue

            if qid not in proposals:
                proposals[qid] = {
                    "quote_id": qid,
                    "risk_location": str(chunk.get("risk_location", "") or ""),
                    "user_name": str(chunk.get("user_name", "") or ""),
                    "created_at": _parse_date(chunk.get("created_at")),
                    "is_paid_on_date": _parse_date(chunk.get("is_paid_on_date")),
                    "is_complete_submission": chunk.get("is_complete_submission", True),
                    "business_name": "",
                    "industry": "",
                    "industry_id": "",
                    "nature_of_business": "",
                    "insured_value": 0.0,
                    "insured_value_type": "",
                    "alarm": "",
                    "cctv": "",
                    "strong_room": "",
                    "armoured_vehicle": "",
                    "gps_vehicle": "",
                    "gps_bags": "",
                    "armed_guards_transit": "",
                    "guards_at_premise": "",
                    "jaguar_transit": "",
                    "claim_history_label": "",
                    "claim_amount": 0.0,
                    "safe_grade": "",
                    "shop_lifting": "",
                    "maximum_stock_in_premises": 0.0,
                    "maximum_stock_foreign_currency": 0.0,
                    "value_of_pledged_stock": 0.0,
                    "value_of_cash_in_premise": 0.0,
                    "maximum_stock_during_transit": 0.0,
                }

            rec = proposals[qid]
            section = chunk.get("section", "")
            raw = chunk.get("fields", {})
            if not isinstance(raw, dict):
                if isinstance(raw, list) and raw and isinstance(raw[0], dict):
                    raw = raw[0]
                else:
                    continue

            if section == "business_profile":
                rec["business_name"] = str(raw.get("business_name_label", "") or "")
                nob = raw.get("nature_of_business_label", "")
                rec["nature_of_business"] = str(nob) if nob else ""

            if section == "industry_id":
                ind_raw = raw.get("industry_id_label", "")
                rec["industry_id"] = str(ind_raw) if ind_raw else ""

            if section == "sum_assured":
                rec["maximum_stock_in_premises"] = _safe_float(
                    raw.get("maximum_stock_in_premises_label")
                )
                rec["maximum_stock_foreign_currency"] = _safe_float(
                    raw.get("maximum_stock_foreign_currency_in_premise_label")
                )
                rec["value_of_pledged_stock"] = _safe_float(
                    raw.get("value_of_pledged_stock_in_premise_label")
                )
                rec["value_of_cash_in_premise"] = _safe_float(
                    raw.get("value_of_cash_in_premise_label")
                )
                rec["maximum_stock_during_transit"] = _safe_float(
                    raw.get("maximum_stock_during_transit_label")
                )

            if section == "cctv":
                rec["cctv"] = _yn(raw.get("recording_label"))

            if section == "alarm":
                rec["alarm"] = _yn(raw.get("do_you_have_alarm_label"))

            if section == "strong_room":
                rec["strong_room"] = _yn(raw.get("do_you_have_a_strong_room_label"))

            if section == "safe":
                g = raw.get("grade_label", "")
                if g:
                    rec["safe_grade"] = decode_field("grade_label", g)

            if section == "transit_and_gaurds":
                rec["armoured_vehicle"] = _yn(
                    raw.get("do_you_use_armoured_vehicle_label")
                )
                rec["gps_vehicle"] = _yn(
                    raw.get("installed_gps_tracker_in_transit_vehicles_label")
                )
                rec["gps_bags"] = _yn(
                    raw.get("installed_gps_tracker_in_transit_bags_label")
                )
                rec["armed_guards_transit"] = _yn(
                    raw.get("do_you_use_armed_guards_during_transit_label")
                )
                rec["guards_at_premise"] = _yn(
                    raw.get("do_you_use_guards_at_premise_label")
                )
                rec["jaguar_transit"] = _yn(
                    raw.get("usage_of_jaguar_transit_label")
                )

            if section == "claim_history":
                ch = str(raw.get("claim_history_label", "")).strip()
                if ch in ("001", "1"):
                    rec["claim_history_label"] = "No claim within 3 years"
                elif ch in ("002", "2"):
                    rec["claim_history_label"] = "Claims within the past 3 years"
                else:
                    rec["claim_history_label"] = _yn(ch)
                details = raw.get("additional_details", [])
                if isinstance(details, list):
                    for item in details:
                        if isinstance(item, dict):
                            rec["claim_amount"] += _safe_float(
                                item.get("amount_of_claim_label")
                            )

            if section == "shop_lifting":
                sl = raw.get("shop_lifting_label", "")
                rec["shop_lifting"] = _yn(sl)

        # Post-process: derive industry and insured_value
        # Industry is determined by WHICH sum_assured field is populated,
        # not by nature_of_business_label (which is a sub-type).
        for rec in proposals.values():
            stock = rec["maximum_stock_in_premises"]
            forex = rec["maximum_stock_foreign_currency"]
            pledged = rec["value_of_pledged_stock"]
            cash = rec["value_of_cash_in_premise"]

            if stock > 0:
                rec["industry"] = "Jewellery & Gold"
                rec["insured_value"] = stock
                rec["insured_value_type"] = "jewellery stock"
            elif forex > 0:
                rec["industry"] = "Money Services"
                rec["insured_value"] = forex
                rec["insured_value_type"] = "foreign currency stock"
            elif pledged > 0 or cash > 0:
                rec["industry"] = "Pawnbrokers"
                rec["insured_value"] = pledged + cash
                rec["insured_value_type"] = "pledged stock + cash"
            else:
                rec["industry"] = "Unknown"
                rec["insured_value"] = 0.0
                rec["insured_value_type"] = "unknown"

        self.records = sorted(proposals.values(), key=lambda r: r["quote_id"])
        logger.info(
            "AnalyticalEngine: loaded %d proposal records", len(self.records)
        )

    # Public helpers
    def get_record_count(self) -> int:
        return len(self.records)

    def get_unique_quote_ids(self) -> List[str]:
        return [r["quote_id"] for r in self.records]

    # 1. Top insured policies
    def get_top_insured_policies(self, limit: int = 15) -> List[Dict]:
        """Return proposals sorted by insured_value descending."""
        ranked = sorted(
            self.records, key=lambda r: r["insured_value"], reverse=True
        )
        results = []
        for r in ranked[:limit]:
            results.append(
                {
                    "quote_id": r["quote_id"],
                    "business_name": r["business_name"],
                    "risk_location": r["risk_location"],
                    "insured_value": r["insured_value"],
                    "insured_value_type": r["insured_value_type"],
                    "industry": r["industry"],
                }
            )
        return results

    # 2. Industry totals
    def get_industry_totals(self) -> List[Dict]:
        """Group by industry -> count, total insured, average insured."""
        groups: Dict[str, Dict] = {}
        for r in self.records:
            ind = r["industry"]
            if ind not in groups:
                groups[ind] = {"industry": ind, "count": 0, "total": 0.0}
            groups[ind]["count"] += 1
            groups[ind]["total"] += r["insured_value"]
        for g in groups.values():
            g["average"] = (
                round(g["total"] / g["count"], 2) if g["count"] else 0.0
            )
            g["total"] = round(g["total"], 2)
        return sorted(groups.values(), key=lambda g: g["total"], reverse=True)

    # 3. Claim statistics by region
    def get_claim_stats_by_region(self) -> List[Dict]:
        """For each state: proposals with claims, proposals without."""
        regions: Dict[str, Dict] = {}
        for r in self.records:
            state = _extract_state(r["risk_location"])
            if state not in regions:
                regions[state] = {
                    "state": state,
                    "with_claims": 0,
                    "no_claims": 0,
                    "proposals": [],
                }
            ch = r["claim_history_label"].lower()
            if "claim" in ch and "no claim" not in ch:
                regions[state]["with_claims"] += 1
            else:
                regions[state]["no_claims"] += 1
            regions[state]["proposals"].append(r["quote_id"])
        return sorted(regions.values(), key=lambda g: g["state"])

    # 4. Policies above threshold
    def get_policies_above_threshold(self, threshold_rm: float) -> List[Dict]:
        """Return proposals where insured_value > threshold_rm."""
        results = []
        for r in sorted(
            self.records, key=lambda x: x["insured_value"], reverse=True
        ):
            if r["insured_value"] > threshold_rm:
                results.append(
                    {
                        "quote_id": r["quote_id"],
                        "business_name": r["business_name"],
                        "risk_location": r["risk_location"],
                        "insured_value": r["insured_value"],
                        "insured_value_type": r["insured_value_type"],
                        "industry": r["industry"],
                    }
                )
        return results

    # 5. Security features
    def get_security_features(self) -> List[Dict]:
        """For each proposal, return all security feature flags."""
        results = []
        for r in self.records:
            results.append(
                {
                    "quote_id": r["quote_id"],
                    "business_name": r["business_name"],
                    "alarm": r["alarm"],
                    "cctv": r["cctv"],
                    "strong_room": r["strong_room"],
                    "armoured_vehicle": r["armoured_vehicle"],
                    "gps_vehicle": r["gps_vehicle"],
                    "gps_bags": r["gps_bags"],
                    "armed_guards_transit": r["armed_guards_transit"],
                    "guards_at_premise": r["guards_at_premise"],
                    "safe_grade": r["safe_grade"],
                }
            )
        return results

    # 6. GPS statistics
    def get_gps_stats(self) -> Dict:
        """Count proposals with/without GPS trackers."""
        with_vehicle, without_vehicle = [], []
        with_bags, without_bags = [], []
        for r in self.records:
            if r["gps_vehicle"] == "Yes":
                with_vehicle.append(r["business_name"])
            else:
                without_vehicle.append(r["business_name"])
            if r["gps_bags"] == "Yes":
                with_bags.append(r["business_name"])
            else:
                without_bags.append(r["business_name"])
        return {
            "gps_vehicle_yes": len(with_vehicle),
            "gps_vehicle_no": len(without_vehicle),
            "gps_bags_yes": len(with_bags),
            "gps_bags_no": len(without_bags),
            "missing_vehicle_gps": without_vehicle,
            "missing_bags_gps": without_bags,
        }

    # 7. Policy type distribution
    def get_policy_type_distribution(self) -> List[Dict]:
        """Alias for industry_totals."""
        return self.get_industry_totals()

    # 8. Claim ratio
    def get_claim_ratio(self) -> Dict:
        """Claim ratio -- not computable without premium data."""
        has_claims = sum(
            1
            for r in self.records
            if "claim" in r["claim_history_label"].lower()
            and "no claim" not in r["claim_history_label"].lower()
        )
        return {
            "proposals_with_claims": has_claims,
            "proposals_total": len(self.records),
            "computable": False,
            "reason": "Premium amounts are not recorded in the proposal database.",
        }

    # 9. Company policy counts
    def get_company_policy_counts(self) -> List[Dict]:
        """Count policies per company (business_name), sorted descending."""
        counts: Dict[str, int] = {}
        for r in self.records:
            name = r["business_name"] or r["quote_id"]
            counts[name] = counts.get(name, 0) + 1
        results = [
            {"business_name": name, "count": cnt}
            for name, cnt in counts.items()
        ]
        return sorted(results, key=lambda x: x["count"], reverse=True)

    # 10. Average claim amount
    def get_average_claim_amount(self) -> Dict:
        """Average claim amount across proposals that have claims."""
        total_amount = 0.0
        with_claims = 0
        all_amounts = []
        for r in self.records:
            amt = r["claim_amount"]
            ch = r["claim_history_label"].lower()
            has_claim = "claim" in ch and "no claim" not in ch
            if has_claim and amt > 0:
                total_amount += amt
                with_claims += 1
                all_amounts.append(
                    {"business_name": r["business_name"], "quote_id": r["quote_id"], "amount": amt}
                )
        avg = round(total_amount / with_claims, 2) if with_claims else 0.0
        return {
            "total_claim_amount": round(total_amount, 2),
            "proposals_with_claims": with_claims,
            "average_claim_amount": avg,
            "total_proposals": len(self.records),
            "details": all_amounts,
        }

    # 11. Average underwriting turnaround time
    def get_average_underwriting_tat(self) -> Dict:
        """Average days between is_paid_on_date and created_at."""
        durations = []
        details = []
        for r in self.records:
            paid = r.get("is_paid_on_date")
            created = r.get("created_at")
            if paid and created:
                delta = (created - paid).days
                if delta >= 0:
                    durations.append(delta)
                    details.append({
                        "quote_id": r["quote_id"],
                        "business_name": r["business_name"],
                        "is_paid_on_date": paid.strftime("%Y-%m-%d"),
                        "created_at": created.strftime("%Y-%m-%d"),
                        "days": delta,
                    })
        avg = round(sum(durations) / len(durations), 1) if durations else 0.0
        return {
            "average_days": avg,
            "min_days": min(durations) if durations else 0,
            "max_days": max(durations) if durations else 0,
            "proposals_counted": len(durations),
            "details": details,
        }

    # 12. Regions by claim frequency
    def get_regions_by_claim_frequency(self, ascending: bool = True) -> List[Dict]:
        """Rank regions (states) by claim frequency — ascending for lowest first."""
        regions: Dict[str, Dict] = {}
        for r in self.records:
            state = _extract_state(r["risk_location"])
            if state not in regions:
                regions[state] = {"state": state, "total": 0, "with_claims": 0}
            regions[state]["total"] += 1
            ch = r["claim_history_label"].lower()
            if "claim" in ch and "no claim" not in ch:
                regions[state]["with_claims"] += 1
        for g in regions.values():
            g["claim_rate"] = (
                round(g["with_claims"] / g["total"] * 100, 1) if g["total"] else 0.0
            )
        return sorted(regions.values(), key=lambda x: x["claim_rate"], reverse=not ascending)

    # 13. Not-available detector
    _NOT_AVAILABLE_FIELDS = {
        "premium",
        "broker",
        "expiry",
        "renewal",
        "rejected",
        "non-disclosure",
        "fire cause",
        "fire-related",
        "peril cause",
        "peril type",
    }

    def is_field_available(self, query: str) -> Optional[str]:
        """If query asks for a field NOT in the database, return a message."""
        q = query.lower()
        for kw in self._NOT_AVAILABLE_FIELDS:
            if kw in q:
                return (
                    f"'{kw}' information is not stored in the proposal database. "
                    "Available data includes: insured values, security features "
                    "(CCTV, alarm, GPS, strong room, armoured vehicle, guards), "
                    "claim history (presence/absence within 3 years), business "
                    "profile, and risk location."
                )
        return None

    # 10. Master dispatcher (called from main pipeline)
    def run(self, query: str) -> Optional[str]:
        """
        Try to answer an analytical query deterministically.
        Returns a formatted string answer or None if the query is not analytical.
        """
        q = query.lower().strip()

        # Not-available check
        na = self.is_field_available(query)
        if na:
            return na

        # Top / highest insured
        if _matches(
            q,
            [
                "highest insured",
                "top insured",
                "highest value",
                "list policies",
                "list all policies",
                "policies with the highest",
                "all proposals ranked",
                "proposals by insured value",
                "rank by insured",
                "policies in malaysia with the highest insured",
            ],
        ):
            limit = _extract_limit(q) or 15
            data = self.get_top_insured_policies(limit)
            return self._fmt_top_insured(data)

        # Above threshold (e.g. "above RM 5000000")
        m = re.search(r"above\s+(?:rm\s*)?(\d[\d,.]*)", q)
        if m:
            threshold = float(m.group(1).replace(",", ""))
            data = self.get_policies_above_threshold(threshold)
            if data:
                return self._fmt_above_threshold(data, threshold)
            return f"No proposals have insured values above RM {threshold:,.0f}."

        # Above threshold with "million" (e.g. "above 5 million")
        m = re.search(r"(?:rm\s*)?(\d[\d,.]*)\s*(?:million|m)\b", q)
        if m and any(
            kw in q
            for kw in ["above", "over", "exceed", "more than", "high-value"]
        ):
            raw_num = m.group(1).replace(",", "")
            multiplier = 1_000_000 if float(raw_num) < 1000 else 1
            threshold = float(raw_num) * multiplier
            data = self.get_policies_above_threshold(threshold)
            if data:
                return self._fmt_above_threshold(data, threshold)
            return f"No proposals have insured values above RM {threshold:,.0f}."

        # Industry totals
        if _matches(
            q,
            [
                "industry total",
                "top industries",
                "industry insured",
                "industries insured",
                "by total sum",
                "industry breakdown",
                "total sum insured by industry",
            ],
        ):
            data = self.get_industry_totals()
            return self._fmt_industry_totals(data)

        # Security features
        if _matches(
            q,
            [
                "anti-theft",
                "anti theft",
                "security features",
                "security devices",
                "include anti-theft",
                "security measures",
            ],
        ):
            data = self.get_security_features()
            return self._fmt_security_features(data)

        # GPS
        if _matches(
            q,
            [
                "gps tracker",
                "gps status",
                "gps in vehicle",
                "gps in bag",
                "gps stat",
            ],
        ):
            data = self.get_gps_stats()
            return self._fmt_gps_stats(data)

        # Claim stats (general, non-ranked)
        if _matches(
            q,
            [
                "claim stat",
                "claims by region",
                "high-risk zone",
                "high risk zone",
                "risk zone",
                "claim history across",
                "claims across",
            ],
        ):
            data = self.get_claim_stats_by_region()
            return self._fmt_claim_stats(data)

        # Claim ratio
        if _matches(q, ["claim ratio", "loss ratio"]):
            data = self.get_claim_ratio()
            return self._fmt_claim_ratio(data)

        # Company policy counts
        if _matches(
            q,
            [
                "company policy count",
                "companies with highest number",
                "companies with most",
                "policies per company",
                "active policies",
                "number of active policies",
                "highest number of active",
                "most policies",
                "policies by company",
                "company with the most",
                "businesses with the most",
                "list companies",
            ],
        ):
            data = self.get_company_policy_counts()
            return self._fmt_company_policy_counts(data)

        # Average claim amount
        if _matches(
            q,
            [
                "average claim amount",
                "mean claim amount",
                "avg claim amount",
                "average claim per",
                "claim amount per property",
                "average amount of claim",
                "typical claim amount",
                "claim amount average",
            ],
        ):
            data = self.get_average_claim_amount()
            return self._fmt_average_claim_amount(data)

        # Underwriting turnaround time
        if _matches(
            q,
            [
                "underwriting turnaround",
                "underwriting tat",
                "turnaround time",
                "processing time",
                "average underwriting",
                "underwriting time",
                "time to process",
                "how long does underwriting",
                "days to underwrite",
            ],
        ):
            data = self.get_average_underwriting_tat()
            return self._fmt_underwriting_tat(data)

        # Regions by claim frequency (lowest / highest)
        if _matches(
            q,
            [
                "lowest claim frequency",
                "highest claim frequency",
                "regions by claim",
                "region claim frequency",
                "safest region",
                "riskiest region",
                "states by claim",
                "claim frequency by region",
                "claim frequency by state",
            ],
        ):
            ascending = any(kw in q for kw in ["lowest", "safest", "least", "fewest"])
            data = self.get_regions_by_claim_frequency(ascending=ascending)
            return self._fmt_regions_by_claim_frequency(data, ascending)

        # How many proposals
        if "how many" in q and any(
            kw in q for kw in ["proposal", "polic", "record"]
        ):
            return (
                f"There are {self.get_record_count()} proposal records "
                "in the system."
            )

        return None

    # Formatting helpers
    def _fmt_top_insured(self, data: List[Dict]) -> str:
        lines = [
            f"Policies ranked by insured value (highest first) "
            f"-- {len(data)} proposal(s):"
        ]
        for i, d in enumerate(data, 1):
            bname = d["business_name"]
            qid = d["quote_id"]
            val = d["insured_value"]
            vtype = d["insured_value_type"]
            lines.append(f"  {i}. {bname} ({qid}) -- RM {val:,.0f} ({vtype})")
        return "\n".join(lines)

    def _fmt_above_threshold(self, data: List[Dict], threshold: float) -> str:
        lines = [f"Proposals with insured value above RM {threshold:,.0f}:"]
        for d in data:
            bname = d["business_name"]
            qid = d["quote_id"]
            val = d["insured_value"]
            vtype = d["insured_value_type"]
            lines.append(f"  - {bname} ({qid}) -- RM {val:,.0f} ({vtype})")
        return "\n".join(lines)

    def _fmt_industry_totals(self, data: List[Dict]) -> str:
        lines = ["Industry breakdown by total sum insured:"]
        for d in data:
            ind = d["industry"]
            cnt = d["count"]
            tot = d["total"]
            avg = d["average"]
            lines.append(
                f"  - {ind}: {cnt} proposals, "
                f"total RM {tot:,.0f}, average RM {avg:,.0f}"
            )
        return "\n".join(lines)

    def _fmt_security_features(self, data: List[Dict]) -> str:
        lines = [
            f"Security / anti-theft features for all {len(data)} proposals:"
        ]
        feature_keys = [
            ("alarm", "Alarm"),
            ("cctv", "CCTV"),
            ("strong_room", "Strong Room"),
            ("armoured_vehicle", "Armoured Vehicle"),
            ("gps_vehicle", "GPS Vehicle"),
            ("gps_bags", "GPS Bags"),
            ("armed_guards_transit", "Armed Guards (Transit)"),
            ("guards_at_premise", "Guards at Premise"),
        ]
        for d in data:
            parts = []
            for key, label in feature_keys:
                val = d.get(key, "")
                parts.append(f"{label}: {val or 'N/A'}")
            bname = d["business_name"]
            qid = d["quote_id"]
            joined = " | ".join(parts)
            lines.append(f"  {bname} ({qid}): {joined}")
        return "\n".join(lines)

    def _fmt_gps_stats(self, data: Dict) -> str:
        lines = [
            "GPS Tracker Statistics:",
            f"  GPS in transit vehicles: "
            f"{data['gps_vehicle_yes']} Yes, {data['gps_vehicle_no']} No",
            f"  GPS in transit bags: "
            f"{data['gps_bags_yes']} Yes, {data['gps_bags_no']} No",
        ]
        if data["missing_vehicle_gps"]:
            names = ", ".join(data["missing_vehicle_gps"])
            lines.append(f"  Missing vehicle GPS: {names}")
        if data["missing_bags_gps"]:
            names = ", ".join(data["missing_bags_gps"])
            lines.append(f"  Missing bags GPS: {names}")
        return "\n".join(lines)

    def _fmt_claim_stats(self, data: List[Dict]) -> str:
        any_claims = any(d["with_claims"] > 0 for d in data)
        lines = ["Claim statistics by region (state):"]
        for d in data:
            st = d["state"]
            total = d["with_claims"] + d["no_claims"]
            wc = d["with_claims"]
            nc = d["no_claims"]
            lines.append(
                f"  {st}: {total} proposals -- "
                f"{wc} with claims, {nc} no claims"
            )
        if not any_claims:
            lines.append(
                "\n  Note: All proposals report no claims within "
                "the past 3 years."
            )
        return "\n".join(lines)

    def _fmt_claim_ratio(self, data: Dict) -> str:
        wc = data["proposals_with_claims"]
        total = data["proposals_total"]
        lines = [f"Proposals with claims: {wc} out of {total}."]
        if not data["computable"]:
            lines.append(data["reason"])
        return "\n".join(lines)

    def _fmt_company_policy_counts(self, data: List[Dict]) -> str:
        lines = [
            f"Companies ranked by number of active policies "
            f"({len(data)} companies):"
        ]
        for i, d in enumerate(data, 1):
            lines.append(f"  {i}. {d['business_name']} -- {d['count']} policy(ies)")
        return "\n".join(lines)

    def _fmt_average_claim_amount(self, data: Dict) -> str:
        wc = data["proposals_with_claims"]
        total = data["total_proposals"]
        avg = data["average_claim_amount"]
        total_amt = data["total_claim_amount"]
        lines = [
            f"Claim amount statistics across {total} proposals:",
            f"  Proposals with claims: {wc}",
            f"  Total claim amount: RM {total_amt:,.2f}",
            f"  Average claim amount (among those with claims): RM {avg:,.2f}",
        ]
        if wc == 0:
            lines.append(
                "\n  Note: No proposals report claim amounts greater than zero. "
                "All 15 proposals report no claims within the past 3 years."
            )
        if data["details"]:
            lines.append("\n  Breakdown:")
            for item in data["details"]:
                lines.append(
                    f"    - {item['business_name']} ({item['quote_id']}): "
                    f"RM {item['amount']:,.2f}"
                )
        return "\n".join(lines)

    def _fmt_underwriting_tat(self, data: Dict) -> str:
        avg = data["average_days"]
        mn = data["min_days"]
        mx = data["max_days"]
        count = data["proposals_counted"]
        lines = [
            f"Underwriting turnaround time (based on {count} proposals):",
            f"  Average: {avg} days",
            f"  Minimum: {mn} days",
            f"  Maximum: {mx} days",
        ]
        if data["details"]:
            lines.append("\n  Per-proposal breakdown:")
            for d in data["details"]:
                lines.append(
                    f"    - {d['business_name']} ({d['quote_id']}): "
                    f"{d['days']} days "
                    f"(paid {d['is_paid_on_date']}, created {d['created_at']})"
                )
        return "\n".join(lines)

    def _fmt_regions_by_claim_frequency(self, data: List[Dict], ascending: bool) -> str:
        direction = "lowest" if ascending else "highest"
        lines = [f"Regions ranked by claim frequency ({direction} first):"]
        for d in data:
            lines.append(
                f"  - {d['state']}: {d['with_claims']} claims out of "
                f"{d['total']} proposals ({d['claim_rate']}% claim rate)"
            )
        if all(d["with_claims"] == 0 for d in data):
            lines.append(
                "\n  Note: All proposals report no claims within the past 3 years."
            )
        return "\n".join(lines)

# Module-level helpers

def _matches(query: str, patterns: List[str]) -> bool:
    """Return True if query contains any of the trigger patterns."""
    return any(p in query for p in patterns)

def _extract_limit(query: str) -> Optional[int]:
    """Extract 'top N' or 'first N' from query."""
    m = re.search(r"(?:top|first|best|bottom|worst)\s+(\d+)", query)
    if m:
        return int(m.group(1))
    return None
