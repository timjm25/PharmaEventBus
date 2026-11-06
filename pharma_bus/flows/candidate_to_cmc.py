"""End-to-end flow: CandidateNomination -> CMC plan -> CapacityRequest.

When a candidate is nominated, the CMC planner works backwards from the
target IND date to the manufacturing window the programme needs, and asks
the CDMO side for suite capacity with an upstream CDMOAgentContracts
``CapacityRequest`` 1.0.0. The request carries ``correlation_id`` = asset_id
and ``causation_id`` = the nomination's message_id, so the asset's thread can
be followed across both contract sets. Every published request is logged to
the audit callback with action ``AUTO_LOG``.

Backwards plan (calendar days, defaults in :data:`PLANNING_DAYS`)::

    target IND date
      - ind_authoring (60)        CMC module 3 written from released GMP data
      = GMP release needed by
      - release_testing (30)      QC release of the last GMP batch
      = latest_end                last GMP batch off the line
    target IND date
      - ind_authoring (60) - tox_study (180)   GLP tox in-life + report
      = tox material needed by
      - release_testing (30)
      = tox batches complete by
      - (engineering + tox batches) x days per batch
      - tech_transfer (90, only for a defined process)
      = earliest_start
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from ..audit import AuditCallback, AuditClient
from ..bus import Bus, Subscription
from ..validate import make_message

#: Asset stage -> CDMOAgentContracts CapacityRequest.phase (CONVENTIONS.md mapping table).
STAGE_TO_CAPACITY_PHASE: dict[str, str] = {
    "discovery": "preclinical", "lead_optimization": "preclinical", "candidate": "preclinical",
    "ind_enabling": "preclinical", "ind_filed": "preclinical", "phase1": "phase_1", "phase2": "phase_2",
    "phase3": "phase_3", "filed": "phase_3", "approved": "commercial",
}
#: Sponsor-side modalities that CDMOAgentContracts Common 1.0.0 does not list.
SPONSOR_EXTRAS = frozenset({"small_molecule", "oligonucleotide", "peptide"})
#: Typical manufacturing days per batch (suite occupancy), by sponsor modality.
BATCH_DAYS: dict[str, int] = {
    "autologous_cell_therapy": 21, "allogeneic_cell_therapy": 35, "gene_therapy_aav": 30,
    "gene_therapy_lvv": 30, "viral_vector": 30, "plasmid_dna": 21, "mrna": 14, "monoclonal_antibody": 45,
    "adc": 60, "recombinant_protein": 45, "small_molecule": 30, "oligonucleotide": 30, "peptide": 30, "other": 30,
}
PLANNING_DAYS: dict[str, int] = {"ind_authoring": 60, "release_testing": 30, "tox_study": 180, "tech_transfer": 90}


class FlowError(ValueError):
    """The nomination cannot be turned into a capacity request (e.g. terminated asset)."""


def map_modality(modality: str) -> str:
    """Sponsor modality -> CDMOAgentContracts Common modality (extras -> 'other')."""
    return "other" if modality in SPONSOR_EXTRAS else modality


def map_phase(stage: str) -> str:
    try:
        return STAGE_TO_CAPACITY_PHASE[stage]
    except KeyError:
        raise FlowError(f"asset stage {stage!r} has no CapacityRequest phase (terminated assets need no capacity)") from None


@dataclass
class CmcPlan:
    """The planner's view of what manufacturing an IND-enabling programme needs."""

    asset_id: str
    nomination_id: str
    modality: str
    cdmo_modality: str
    phase: str
    process_type: str
    engineering_batches: int
    tox_batches: int
    gmp_batches: int
    duration_days_per_batch: float
    target_ind_date: date
    tox_material_by: date
    gmp_release_by: date
    earliest_start: date
    latest_end: date
    warnings: list[str] = field(default_factory=list)

    @property
    def batches(self) -> int:
        """Batches requested from the CDMO: planned tox + GMP clinical."""
        return self.tox_batches + self.gmp_batches

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for k, v in d.items():
            if isinstance(v, date):
                d[k] = v.isoformat()
        d["batches"] = self.batches
        return d


def build_cmc_plan(nomination: dict, today: date | None = None) -> CmcPlan:
    """Plan the manufacturing window backwards from ``target_ind_date``.

    If the backwards-planned start is already in the past the window starts
    today and, where needed, ends later than the IND plan allows; both cases
    add a warning that goes into the request notes."""
    today = today or datetime.now(timezone.utc).date()
    pb = nomination["planned_batches"]
    eng, tox, gmp = int(pb.get("engineering", 0)), int(pb["tox"]), int(pb["gmp_clinical"])
    if tox + gmp < 1:
        raise FlowError("nomination plans no tox or GMP batches; nothing to request")
    cmc = nomination.get("cmc_requirements", {})
    modality = nomination["modality"]
    days = float(cmc.get("duration_days_per_batch") or BATCH_DAYS.get(modality, BATCH_DAYS["other"]))
    tech_transfer = cmc.get("process_readiness") == "defined_process"
    P = PLANNING_DAYS

    ind = date.fromisoformat(nomination["target_ind_date"])
    gmp_release_by = ind - timedelta(days=P["ind_authoring"])
    latest_end = gmp_release_by - timedelta(days=P["release_testing"])
    tox_material_by = ind - timedelta(days=P["ind_authoring"] + P["tox_study"])
    tox_done_by = tox_material_by - timedelta(days=P["release_testing"])
    lead_in = (eng + tox) * days + (P["tech_transfer"] if tech_transfer else 0)
    earliest_start = tox_done_by - timedelta(days=round(lead_in))

    warnings: list[str] = []
    gmp_room = (latest_end - max(tox_done_by, earliest_start)).days
    if gmp * days > gmp_room:
        warnings.append(f"{gmp} GMP batch(es) need {gmp * days:.0f} days but only {gmp_room} fit between tox "
                        "completion and latest_end; parallel suites or a later IND date are needed")
    if earliest_start < today:
        warnings.append(f"backwards-planned start {earliest_start.isoformat()} is already past; window opens today")
        earliest_start = today
        needed = earliest_start + timedelta(days=round((eng + tox + gmp) * days))
        if latest_end < needed:
            warnings.append(f"target IND date {ind.isoformat()} is at risk: manufacturing cannot finish before "
                            f"{needed.isoformat()}")
            latest_end = needed

    return CmcPlan(
        asset_id=nomination["asset_id"], nomination_id=nomination["nomination_id"], modality=modality,
        cdmo_modality=map_modality(modality), phase=map_phase(nomination["stage"]),
        process_type="tech_transfer" if tech_transfer else "full_process", engineering_batches=eng,
        tox_batches=tox, gmp_batches=gmp, duration_days_per_batch=days, target_ind_date=ind,
        tox_material_by=tox_material_by, gmp_release_by=gmp_release_by, earliest_start=earliest_start,
        latest_end=latest_end, warnings=warnings,
    )


def capacity_request_from_plan(plan: CmcPlan, nomination: dict, requested_by: str) -> dict:
    """Build an upstream CDMOAgentContracts CapacityRequest 1.0.0 payload."""
    cmc = nomination.get("cmc_requirements", {})
    payload: dict[str, Any] = {
        "request_id": f"{plan.asset_id}-CMC-{plan.nomination_id}"[:128],
        "requested_by": requested_by,
        "program_id": plan.asset_id,
        "modality": plan.cdmo_modality,
        "process_type": plan.process_type,
        "phase": plan.phase,
        "batches": plan.batches,
        "duration_days_per_batch": plan.duration_days_per_batch,
        "earliest_start": plan.earliest_start.isoformat(),
        "latest_end": plan.latest_end.isoformat(),
    }
    suite = {k: cmc[k] for k in ("cleanroom_grade", "biosafety_level") if k in cmc}
    if suite:
        payload["suite_requirements"] = suite
    if "scale" in cmc:
        payload["scale"] = cmc["scale"]
    if cmc.get("preferred_partner_ids"):
        payload["preferred_site_ids"] = list(cmc["preferred_partner_ids"])
    if "priority" in nomination:
        payload["priority"] = nomination["priority"]
    name = nomination.get("code_name") or nomination["asset_name"]
    notes = [
        f"CMC plan for {plan.asset_id} ({name}) from candidate nomination {plan.nomination_id}.",
        f"Target IND {plan.target_ind_date.isoformat()}; tox material needed by {plan.tox_material_by.isoformat()}; "
        f"GMP release needed by {plan.gmp_release_by.isoformat()}.",
        f"Batches: {plan.tox_batches} tox + {plan.gmp_batches} GMP clinical"
        + (f" (plus {plan.engineering_batches} engineering run(s) before tox)" if plan.engineering_batches else "")
        + f", about {plan.duration_days_per_batch:g} days each; {plan.process_type.replace('_', ' ')}.",
    ]
    if plan.modality != plan.cdmo_modality:
        notes.append(f"Sponsor modality {plan.modality} sent as 'other'.")
    notes.extend(f"Warning: {w}" for w in plan.warnings)
    payload["notes"] = " ".join(notes)[:5000]
    payload["x-asset-id"] = plan.asset_id
    payload["x-cmc-plan"] = {k: v for k, v in plan.as_dict().items() if k not in ("asset_id", "warnings")}
    return payload


class CandidateToCmcFlow:
    """Subscribes to CandidateNomination and publishes a CapacityRequest per nomination.

        flow = CandidateToCmcFlow(bus, audit=my_audit).attach()
        bus.publish(nomination_message)   # InMemoryBus: request published immediately
        bus.drain()                       # SQLiteBus / Redis: deliver now
    """

    message_type = "CandidateNomination"

    def __init__(self, bus: Bus, audit: AuditCallback | None = None, source_agent: str = "cmc-planner",
                 group: str = "cmc-planner", target_agent: str | None = "capacity-scheduler",
                 today: date | None = None):
        self.bus = bus
        self.audit: AuditCallback = audit if audit is not None else AuditClient.from_env()
        self.source_agent = source_agent
        self.group = group
        self.target_agent = target_agent
        self.today = today
        self.published: list[str] = []

    def attach(self) -> "CandidateToCmcFlow":
        self.subscription: Subscription = self.bus.subscribe(self.message_type, self.handle, group=self.group)
        return self

    def build_request(self, envelope: dict) -> dict:
        """The enveloped CapacityRequest for a CandidateNomination message (validated, not published)."""
        nomination = envelope["payload"]
        plan = build_cmc_plan(nomination, today=self.today)
        payload = capacity_request_from_plan(plan, nomination, self.source_agent)
        extra: dict[str, Any] = {"correlation_id": plan.asset_id, "causation_id": envelope["message_id"],
                                 "gxp_relevant": bool(envelope.get("gxp_relevant", False))}
        if self.target_agent:
            extra["target_agent"] = self.target_agent
        return make_message("CapacityRequest", payload, source_agent=self.source_agent, version="1.0.0", **extra)

    def handle(self, envelope: dict) -> str:
        """Bus handler: build, publish and audit the CapacityRequest. Returns its message_id."""
        msg = self.build_request(envelope)
        mid = self.bus.publish(msg, publisher=self.source_agent)
        self.published.append(mid)
        p = msg["payload"]
        self._audit(resource=f"asset:{p['program_id']}", detail=(
            f"CapacityRequest {mid} ({p['request_id']}) published from CandidateNomination "
            f"{envelope['message_id']}: {p['batches']} batch(es), {p['process_type']}, {p['phase']}, "
            f"window {p['earliest_start']} to {p['latest_end']}"))
        return mid

    def _audit(self, resource: str, detail: str) -> None:
        try:
            self.audit(source_system="PharmaEventBus", actor=self.source_agent, actor_type="automated_process",
                       action="AUTO_LOG", resource=resource, detail=detail)
        except Exception:  # noqa: BLE001 - audit must never break the flow; AuditClient already falls back
            pass
