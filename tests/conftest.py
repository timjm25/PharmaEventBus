"""Shared fixtures: examples, enveloped messages and fresh buses."""
from __future__ import annotations

import copy
import json
from datetime import date
from pathlib import Path

import pytest

from pharma_bus import InMemoryBus, SQLiteBus

ROOT = Path(__file__).resolve().parent.parent
EX = ROOT / "examples"
TODAY = date(2026, 11, 6)
SNAKE = {
    "CandidateNomination": "candidate_nomination", "AssayResult": "assay_result", "StudyReport": "study_report",
    "StageGateDecision": "stage_gate_decision", "SubmissionPackage": "submission_package",
    "ValuationSnapshot": "valuation_snapshot", "PartnerDeliverable": "partner_deliverable", "DealTerms": "deal_terms",
}


def ex(name: str) -> dict:
    return json.loads((EX / f"{name}.json").read_text())


def msg(name: str) -> dict:
    return json.loads((EX / "messages" / f"{SNAKE[name]}.json").read_text())


def new_id(envelope: dict, n: int) -> dict:
    """Copy of an envelope with a distinct (valid) message_id."""
    e = copy.deepcopy(envelope)
    e["message_id"] = f"{n:08d}-aaaa-4000-8000-{n:012d}"
    return e


class AuditSpy:
    """Collects audit calls (the injectable audit callback)."""

    def __init__(self) -> None:
        self.events: list[dict] = []

    def __call__(self, **fields) -> dict:
        self.events.append(fields)
        return {"mode": "test"}


@pytest.fixture
def memory_bus() -> InMemoryBus:
    return InMemoryBus()


@pytest.fixture
def sqlite_path(tmp_path) -> Path:
    return tmp_path / "bus.db"


@pytest.fixture
def sqlite_bus(sqlite_path):
    bus = SQLiteBus(sqlite_path)
    yield bus
    bus.close()


@pytest.fixture
def audit() -> AuditSpy:
    return AuditSpy()
