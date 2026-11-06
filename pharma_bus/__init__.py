"""PharmaEventBus: sponsor-side (virtual pharma) contracts on CDMOAgentContracts, plus a durable event bus.

Contracts: CandidateNomination, AssayResult, StudyReport, StageGateDecision,
SubmissionPackage, ValuationSnapshot, PartnerDeliverable, DealTerms (every
payload carries the Asset ID). Backends: InMemoryBus, SQLiteBus,
RedisStreamsBus. See README.md.
"""
from .bus import Bus, InMemoryBus, RedisStreamsBus, SQLiteBus, Subscription, open_bus
from .registry import PHARMA_CONTRACTS, UnknownContract, list_contracts, load_schema, resolve_version
from .validate import (ContractValidationError, Issue, contract_errors, is_valid, make_message, message_errors,
                       validate, validate_message)

__version__ = "0.1.0"
__all__ = [
    "PHARMA_CONTRACTS", "Bus", "ContractValidationError", "InMemoryBus", "Issue", "RedisStreamsBus", "SQLiteBus",
    "Subscription", "UnknownContract", "contract_errors", "is_valid", "list_contracts", "load_schema",
    "make_message", "message_errors", "open_bus", "resolve_version", "validate", "validate_message",
]
