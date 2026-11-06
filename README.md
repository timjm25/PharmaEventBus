# PharmaEventBus

PharmaEventBus extends the CDMOAgentContracts message contracts from manufacturing
into the sponsor side of drug development, and gives every tool one place to publish
and subscribe instead of being wired to each other by hand. It adds eight contracts
(CandidateNomination, AssayResult, StudyReport, StageGateDecision, SubmissionPackage,
ValuationSnapshot, PartnerDeliverable, DealTerms), each carrying the permanent Asset ID
(`AST-000001`), in the same schema layout and URN namespace as CDMOAgentContracts so the
two sets can later merge into one registry. A validating bus (in-memory, durable SQLite
or Redis Streams) stores every message, delivers it once to each subscribing tool,
retries failures and parks what cannot be processed in a dead-letter store. The first
end-to-end flow turns a candidate nomination into a backwards-planned CMC window and
publishes an upstream `CapacityRequest` to the CDMO side.

All example data is fictional and marked EXAMPLE.

## What it links

| Existing repo / tool | Contract(s) | Direction | How |
|---|---|---|---|
| CDMOAgentContracts | `Common`, `MessageEnvelope`, `CapacityRequest` 1.0.0 | used | Vendored byte-for-byte from `schemas/` at commit `8c7bf2c` (see `vendor/UPSTREAM.md`); `contracts/validate.py`, `registry.py`, `rules.py` style mirrored in `pharma_bus/` |
| CDMOAgentContracts (runtime) | `BatchRecord`, `DeviationReport`, `ReleaseRequest`, `ReleaseDecision`, `CapacityResponse` ... | validated on the same bus | If the `contracts` package is installed (or `CDMO_CONTRACTS_SCHEMA_DIR` is set) its whole schema folder is merged into the registry |
| DrugDiscoveryAgent | CandidateNomination, AssayResult | publishes | Contract (envelope over HTTP `POST /publish` or library `bus.publish`) |
| Laboratory-LIMS, AssayVariabilityPredictor | AssayResult | publish / subscribe | Contract |
| INDBLADrafter | SubmissionPackage (publishes); StudyReport, AssayResult (subscribes) | both | Contract |
| DealStructureSimulator | DealTerms | publishes | Contract; `DealTerms.milestones[]` carry `amount`, `timing_years`, `probability` like the milestone components in `dealsim/model.py`, and `dealsim_name` names the deal there |
| thesis-generator | ValuationSnapshot | publishes | Contract; `rnpv`, `cumulative_pos` match `thesisgen/valuation.py` outputs |
| ProgramDelayPredictor, PTSPredictor (`ptspredict/model.py`) | StageGateDecision | subscribe | Contract; stage mapping per CONVENTIONS.md |
| ProgramDirectorAgent | StageGateDecision | publishes | Contract; `run_id` is its run |
| batch-release-agent, QInvestigationAgent | BatchRecord, ReleaseRequest, ReleaseDecision, DeviationReport | publish / subscribe | Upstream CDMOAgentContracts contracts on the same bus |
| DocumentIngestion | PartnerDeliverable | publishes | Contract; `ingestion.source_system` / `document_id` point back to the ingested file |
| CDMO capacity / scheduling agents | CapacityRequest | subscribe | Published by the candidate -> CMC flow (`pharma_bus/flows/candidate_to_cmc.py`) |
| AuditTrailService (P56) | audit events | writes | HTTP `POST /log` form fields, `AUTO_LOG`, resource `asset:AST-...`; local JSONL fallback with the same hash chain as `audittrail/model.py` |
| PharmaPlatformShell | all | gateway | Forwards `POST /publish` with `X-Platform-User`, recorded as the publisher |
| portfolio-agent | descriptor | discovered by | `agent.yaml` in the `scaffold/agent.yaml` format |

## Quick start

```bash
pip install -e ".[dev]"            # add ,redis for the Redis Streams backend
pytest -q

pharma-bus list                    # contracts and versions
pharma-bus validate-message examples/messages/*.json examples/messages/*.jsonl
pharma-bus publish examples/messages/candidate_nomination.json --db data/bus.db --publisher tim
pharma-bus worker --db data/bus.db --once     # runs the candidate -> CMC flow
pharma-bus tail --db data/bus.db              # nomination, then the CapacityRequest it caused
pharma-bus serve --db data/bus.db             # HTTP API on port 5102
```

From Python:

```python
from pharma_bus import SQLiteBus, make_message
from pharma_bus.flows import CandidateToCmcFlow

bus = SQLiteBus("data/bus.db")
CandidateToCmcFlow(bus).attach()                        # subscriber on CandidateNomination
bus.subscribe("CapacityRequest", print, group="capacity-scheduler")

msg = make_message("CandidateNomination", payload, source_agent="drug-discovery-agent")
bus.publish(msg)            # validates first; invalid -> ContractValidationError(.issues)
bus.drain()                 # deliver: nomination -> CMC plan -> CapacityRequest -> scheduler
```

## Contract catalogue

| Contract | Latest | What it carries |
|---|---|---|
| `CandidateNomination` | 1.0.0 | A development candidate nominated for an asset: modality, stage, rationale, target product profile, target IND date, planned engineering / tox / GMP batches, CMC requirements |
| `AssayResult` | 1.0.0 | One assay readout (binding, potency, cytotoxicity, PK, release tests ...) with value, unit, acceptance, LIMS source and status |
| `StudyReport` | 1.0.0 | Outcome of an in vitro, in vivo, tox, CMC or clinical study: dates, GLP flag, findings, linked assay results, documents |
| `StageGateDecision` | 1.0.0 | Go / no_go / hold / conditional_go between stages, score 0-100, criteria with evidence, signatures (field list fixed by CONVENTIONS.md) |
| `SubmissionPackage` | 1.0.0 | Pre-IND, IND, CTA, BLA, NDA, MAA or designation package: agency, status, dates, CTD sections and their state |
| `ValuationSnapshot` | 1.0.0 | rNPV snapshot in USD millions: rNPV, unrisked NPV, cumulative PoS, peak sales, discount rate, years to launch, sources (field list fixed by CONVENTIONS.md) |
| `PartnerDeliverable` | 1.0.0 | What a CRO / CDMO / licensee owes or sent: type, status, due and received dates, documents, ingestion reference |
| `DealTerms` | 1.0.0 | Deal economics: type, status, upfront, milestones (with timing and probability), royalty and tiers, territory, dates, DealStructureSimulator name |
| `PharmaCommon` | 1.0.0 | Shared `$defs`: `assetId`, canonical ids (TGT, AST, IND, STY, BCH, SUB, DEL, PTN, IPR), `assetStage`, `sponsorModality`, `money` ... (not a message) |
| `MessageEnvelope`, `Common`, `CapacityRequest` | 1.0.0 | Vendored unchanged from CDMOAgentContracts |

Schemas live in `schemas/<Contract>/<MAJOR.MINOR.PATCH>.json` (JSON Schema draft
2020-12, `$id` = `urn:cdmo-agent-contracts:<Contract>:<version>`). Payload objects are
closed (`additionalProperties: false`) except for `x-...` extension fields, so typos
are caught. Each contract has a valid example in `examples/` and an enveloped sample in
`examples/messages/`; `candidate_to_cmc.jsonl` is a nomination and the CapacityRequest
the flow produced from it.

Cross-field rules (`pharma_bus/rules.py`) on top of the schemas:

- For the 8 pharma contracts the envelope `correlation_id` must equal `payload.asset_id`.
- StageGateDecision `go` / `conditional_go` needs at least one signature with meaning `approved`, and must change stage.
- End dates after start dates (study, deal, submission decision, CapacityRequest window); target IND date after nomination.
- Status-dependent fields: a received deliverable has `received_at` and a document; a signed deal has `signed_date`; a submitted package has `submitted_date`; a final study report has `end_date`.

## The bus

| | InMemoryBus | SQLiteBus | RedisStreamsBus |
|---|---|---|---|
| Use | tests, single process | one host, durable | several hosts |
| Delivery | synchronous inside `publish` | `poll()` / `run_once()` / `drain()` / `run_forever()` | same, via `XREADGROUP` |
| Storage | process memory | append-only `messages` table (UPDATE / DELETE blocked by triggers), `consumer_offsets`, `delivery_attempts`, `dead_letters` | stream `<prefix>:messages`, consumer groups, `<prefix>:dead-letters` |
| Retries | immediate, up to `max_attempts` | next poll, in order, up to `max_attempts` | pending entries re-read, up to `max_attempts` |

Semantics shared by all three: `publish` validates the envelope and payload and raises
`ContractValidationError` before anything is stored. A *group* is one consuming tool:
each message is delivered once per group, and every group gets its own copy. Within a
group an exact-type handler wins over a `"*"` handler. After `max_attempts` failures the
message, the error and the attempt count go to the dead-letter store and the group moves
on. Re-publishing the same `message_id` is ignored (idempotent) on the SQLite and
in-memory backends. `open_bus(url)` accepts `memory://`, `sqlite:///path.db` and
`redis://host:6379/0` (env `PHARMA_BUS_URL`).

## Candidate nomination -> CMC plan flow

`pharma_bus/flows/candidate_to_cmc.py` subscribes to CandidateNomination (group
`cmc-planner`) and publishes a CDMOAgentContracts `CapacityRequest` 1.0.0, validated
against the vendored schema:

| CapacityRequest field | From |
|---|---|
| `correlation_id` / `causation_id` | asset_id / the nomination's `message_id` |
| `modality` | nomination modality; sponsor extras (small_molecule, oligonucleotide, peptide) become `other` |
| `phase` | asset stage via the CONVENTIONS.md table (candidate -> `preclinical`, phase1 -> `phase_1`, filed -> `phase_3`, approved -> `commercial`) |
| `process_type` | `tech_transfer` when `cmc_requirements.process_readiness` is `defined_process`, else `full_process` |
| `batches` | planned tox + GMP clinical batches (engineering runs are noted and planned before tox) |
| `earliest_start`, `latest_end` | planned backwards from `target_ind_date` (below) |
| `scale`, `suite_requirements`, `preferred_site_ids`, `priority` | nomination `cmc_requirements` and `priority` |
| `notes`, `x-asset-id`, `x-cmc-plan` | plain-English summary and the full plan |

Backwards plan: GMP release is needed 60 days before the IND (module 3 authoring), so the
last GMP batch must finish 30 days before that (release testing): that is `latest_end`.
Tox material is needed 240 days before the IND (60 authoring + 180 GLP tox in-life and
report); tox batches finish 30 days earlier; engineering and tox batches (and 90 days of
tech transfer when relevant) run before that: that is `earliest_start`. Days per batch
come from the nomination or a modality default (for example 35 for allogeneic cell
therapy, 21 for autologous). If the start is already past, the window opens today and the
notes warn that the IND date is at risk. For the example asset (IND 2028-03-31, 2
engineering + 1 tox + 2 GMP batches) the window is 2027-03-22 to 2028-01-01.

Each published request is logged to an injectable audit callback with action `AUTO_LOG`
and resource `asset:AST-000001`. The default `AuditClient` posts to AuditTrailService
(`AUDIT_TRAIL_URL`) and falls back to a hash-chained local JSONL
(`PHARMA_BUS_AUDIT_FALLBACK`, default `data/audit_fallback.jsonl`) so the flow never fails
because the audit service is down.

## HTTP API (port 5102)

| Method and path | Result |
|---|---|
| `POST /publish` | Envelope JSON. `202 {"message_id": ...}`, or `422 {"errors": [{"path", "message"}]}`. Optional `X-Platform-User` header is recorded as the publisher. In-process flows run right after the publish |
| `GET /messages?correlation_id=AST-000001&type=StageGateDecision&limit=100` | `{"messages": [{seq, message_id, message_type, correlation_id, causation_id, source_agent, publisher, published_at, envelope}], "count"}` |
| `GET /contracts` | `{"contracts": [{name, versions, latest, origin, pharma}]}` |
| `GET /contracts/<name>?version=1.0.0` | The schema, or 404 |
| `GET /dead-letters?group=...` | Messages a group gave up on, with error and attempts |
| `GET /healthz` | Backend, contract count, whether upstream schemas are loaded |
| `GET /openapi.json` | Minimal OpenAPI description |

## CLI

```
pharma-bus list | show <Contract> [--version] | check
pharma-bus validate <Contract> <files...>          # .json, .jsonl or -
pharma-bus validate-message <files...>
pharma-bus publish <files...> [--db path | --url URL] [--publisher who]
pharma-bus tail [--db path] [--group g] [--type T] [--correlation-id AST-...] [-f] [--json]
pharma-bus worker [--db path] [--once]             # run the built-in flows
pharma-bus serve [--db path] [--host] [--port 5102]
```

`tail` without `--group` reads the log; with `--group` it consumes as that group and
advances its offset.

## Data model

- **Envelope** (CDMOAgentContracts `MessageEnvelope` 1.0.0): `message_id` (uuid4),
  `message_type`, `schema_version`, `source_agent`, optional `target_agent`,
  `correlation_id` (= asset_id for pharma contracts), optional `causation_id` (the
  message that caused this one), `sent_at` (RFC 3339, Z), `gxp_relevant`, `payload`.
- **Identifiers**: canonical ids `^(TGT|AST|IND|STY|BCH|SUB|DEL|PTN|IPR)-\d{6}$`;
  record ids (nomination, result, report ...) use Common `identifier`.
- **Money**: USD millions unless a `money` object says otherwise.
- **SQLite tables**: `messages` (append-only log), `consumer_offsets` (group -> last
  seq), `delivery_attempts`, `dead_letters` (append-only).

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `PHARMA_BUS_URL` | `sqlite:///data/pharma_bus.db` | Bus for `open_bus()` and the HTTP app |
| `PHARMA_BUS_DB` | `data/pharma_bus.db` | SQLite file for CLI commands without `--db` |
| `CDMO_CONTRACTS_SCHEMA_DIR` | unset | Upstream checkout to merge (and to compare vendored files against in tests) |
| `PHARMA_BUS_DISABLE_UPSTREAM` | unset | `1` to ignore upstream schemas |
| `AUDIT_TRAIL_URL` | unset | AuditTrailService base URL, e.g. `http://localhost:5056` |
| `PHARMA_BUS_AUDIT_FALLBACK` | `data/audit_fallback.jsonl` | Local audit chain when the service is down |
| `REDIS_URL` | `redis://localhost:6379/15` | Only for the Redis tests |

## What's next

- **Azure Service Bus backend** (production target): one topic per message type (or one
  topic with a `message_type` filter), one subscription per group, dead-lettering and
  max delivery count native to the service, sessions keyed on `correlation_id` for
  per-asset ordering, managed identity for auth. `open_bus("azure-servicebus://...")`
  raises `NotImplementedError` in v1.
- More flows: StageGateDecision -> ProgramDelayPredictor / PTSPredictor refresh;
  StudyReport and AssayResult -> INDBLADrafter section status; DealTerms ->
  ValuationSnapshot recompute; PartnerDeliverable from DocumentIngestion -> StudyReport.
- Dead-letter replay (`pharma-bus replay <id>`) and retry back-off; competing consumers
  in one group on SQLite (claim rows) instead of one process per group.
- Lookup of asset context from AssetProgramRegistry (`GET /assets/<id>/lineage`) so the
  CMC plan can use real batches, partners and indication data.
- Merge the 8 contracts and `PharmaCommon` into CDMOAgentContracts once stable, and add
  the compatibility checker (`contracts/compat.py`) to this repo's `check`.
- Message signing and authentication on `POST /publish` (today the gateway is trusted).
