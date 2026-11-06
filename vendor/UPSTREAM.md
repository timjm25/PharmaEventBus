# Vendored upstream files

| | |
|---|---|
| Source repo | `timjm25/CDMOAgentContracts` |
| Commit | `8c7bf2c` (merge of PR #1, library 1.1.0) |
| Source folder | `schemas/` |
| Copied to | `schemas/` in this repo (same layout, same URN `$id`s) |
| Manifest | `vendor/upstream.json` (SHA-256 of each file) |

Files, copied byte-for-byte:

| File | Why it is here |
|---|---|
| `schemas/Common/1.0.0.json` | Shared `$defs` (identifier, agentId, timestamp, date, person, signature, attachment, quantity, modality) that `PharmaCommon` and every pharma contract `$ref` |
| `schemas/MessageEnvelope/1.0.0.json` | The envelope every bus message travels in |
| `schemas/CapacityRequest/1.0.0.json` | What the candidate -> CMC flow publishes to the CDMO side |

Rules:

- Never edit these files here. Change them upstream, then run
  `python scripts/sync_upstream.py /path/to/CDMOAgentContracts --commit <sha>`
  and update this page.
- `tests/test_upstream_sync.py` always checks the files against the manifest,
  and, when `CDMO_CONTRACTS_SCHEMA_DIR` points at an upstream checkout (root or
  `schemas/`), checks they are identical to upstream. It skips that second
  check otherwise, so CI needs no access to the private repo.
- At runtime, if the upstream `contracts` package (`cdmo-agent-contracts`) is
  installed, or `CDMO_CONTRACTS_SCHEMA_DIR` is set, its whole schema folder is
  merged into the registry, so BatchRecord, DeviationReport, ReleaseRequest
  and the other manufacturing contracts validate on the same bus.
