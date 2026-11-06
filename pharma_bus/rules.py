"""Cross-field rules JSON Schema cannot express (date ordering, signatures ...).

Same style as CDMOAgentContracts ``contracts/rules.py``: each rule yields
(path, message) pairs and runs only after the payload passed schema
validation, so fields can be assumed well-formed.

For upstream manufacturing contracts the upstream rules are used when the
``contracts`` package is installed; otherwise only the CapacityRequest window
rule (needed by the candidate -> CMC flow) is applied locally.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable, Iterator

Rule = Callable[[dict], Iterator[tuple[str, str]]]


def _dt(v: str) -> datetime:
    return datetime.fromisoformat(v.replace("Z", "+00:00"))


def _d(v: str) -> date:
    return date.fromisoformat(v)


def _before(data: dict, a: str, b: str, parse: Callable[[str], Any] = _d) -> Iterator[tuple[str, str]]:
    """``b`` must not be earlier than ``a`` when both are present."""
    if a in data and b in data and parse(data[b]) < parse(data[a]):
        yield f"$.{b}", f"{b} ({data[b]}) is before {a} ({data[a]})"


# ------------------------------------------------------------------ pharma
def _candidate_nomination(d: dict) -> Iterator[tuple[str, str]]:
    nominated = _dt(d["nominated_at"]).date()
    if _d(d["target_ind_date"]) < nominated:
        yield "$.target_ind_date", f"target_ind_date ({d['target_ind_date']}) is before nominated_at ({d['nominated_at']})"
    pb = d["planned_batches"]
    if pb.get("tox", 0) + pb.get("gmp_clinical", 0) < 1:
        yield "$.planned_batches", "plan at least one tox or gmp_clinical batch"


def _study_report(d: dict) -> Iterator[tuple[str, str]]:
    yield from _before(d, "start_date", "end_date")
    if d.get("status") in ("final", "amended") and "end_date" not in d:
        yield "$.end_date", "a final or amended report needs end_date"


def _stage_gate(d: dict) -> Iterator[tuple[str, str]]:
    if d["decision"] in ("go", "conditional_go") and not any(s["meaning"] == "approved" for s in d["signatures"]):
        yield "$.signatures", f"decision '{d['decision']}' needs at least one signature with meaning 'approved'"
    if d["decision"] in ("go", "conditional_go") and d["from_stage"] == d["to_stage"]:
        yield "$.to_stage", "a go decision must move the asset to a different stage"


_SUBMITTED = ("submitted", "accepted", "granted", "approved", "rejected")


def _submission(d: dict) -> Iterator[tuple[str, str]]:
    yield from _before(d, "submitted_date", "decision_date")
    if d["status"] in _SUBMITTED and "submitted_date" not in d:
        yield "$.submitted_date", f"status '{d['status']}' needs submitted_date"
    if d["kind"] == "designation" and "designation" not in d:
        yield "$.designation", "a designation request must name the designation"


def _deliverable(d: dict) -> Iterator[tuple[str, str]]:
    if d["status"] != "expected":
        if "received_at" not in d:
            yield "$.received_at", f"status '{d['status']}' needs received_at"
        if not d.get("documents"):
            yield "$.documents", f"status '{d['status']}' needs at least one document"


def _deal_terms(d: dict) -> Iterator[tuple[str, str]]:
    yield from _before(d, "effective_date", "expiry_date")
    yield from _before(d, "signed_date", "expiry_date")
    if d["status"] == "signed" and "signed_date" not in d:
        yield "$.signed_date", "a signed deal needs signed_date"


def _valuation(d: dict) -> Iterator[tuple[str, str]]:
    if "patent_expiry_year" in d and d["patent_expiry_year"] < _d(d["as_of"]).year - 30:
        yield "$.patent_expiry_year", "patent_expiry_year is implausibly far before as_of"


# ------------------------------------------------- upstream (local fallback)
def _capacity_request(d: dict) -> Iterator[tuple[str, str]]:
    yield from _before(d, "earliest_start", "latest_end")


RULES: dict[str, Rule] = {
    "CandidateNomination": _candidate_nomination,
    "StudyReport": _study_report,
    "StageGateDecision": _stage_gate,
    "SubmissionPackage": _submission,
    "PartnerDeliverable": _deliverable,
    "DealTerms": _deal_terms,
    "ValuationSnapshot": _valuation,
}
LOCAL_UPSTREAM_RULES: dict[str, Rule] = {"CapacityRequest": _capacity_request}


def _upstream_semantic(name: str, data: dict) -> list[tuple[str, str]] | None:
    try:
        from contracts.rules import RULES as UP  # type: ignore[import-not-found]
    except Exception:  # noqa: BLE001
        return None
    rule = UP.get(name)
    return list(rule(data)) if rule else []


def semantic_errors(name: str, data: Any) -> list[tuple[str, str]]:
    if not isinstance(data, dict):
        return []
    if name in RULES:
        return list(RULES[name](data))
    up = _upstream_semantic(name, data)
    if up is not None:
        return up
    rule = LOCAL_UPSTREAM_RULES.get(name)
    return list(rule(data)) if rule else []
