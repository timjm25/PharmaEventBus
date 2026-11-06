"""Event flows that link tools through the bus."""
from .candidate_to_cmc import CandidateToCmcFlow, CmcPlan, FlowError, build_cmc_plan, capacity_request_from_plan

__all__ = ["CandidateToCmcFlow", "CmcPlan", "FlowError", "build_cmc_plan", "capacity_request_from_plan",
           "attach_default_flows"]


def attach_default_flows(bus, audit=None) -> list:
    """Attach every built-in flow to ``bus``; returns the flow objects."""
    return [CandidateToCmcFlow(bus, audit=audit).attach()]
