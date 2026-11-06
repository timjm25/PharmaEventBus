"""Flask API: publish 202/422, queries, contracts, flows over HTTP."""
from __future__ import annotations

import pytest

from conftest import msg
from pharma_bus import InMemoryBus, SQLiteBus
from pharma_bus.http import create_app


@pytest.fixture
def client(audit):
    app = create_app(InMemoryBus(), audit=audit)
    app.config["TESTING"] = True
    return app.test_client()


def test_publish_202_and_query_by_correlation_and_type(client):
    r = client.post("/publish", json=msg("StageGateDecision"), headers={"X-Platform-User": "tim@example.invalid"})
    assert r.status_code == 202 and r.get_json()["message_id"] == msg("StageGateDecision")["message_id"]
    client.post("/publish", json=msg("DealTerms"))
    r = client.get("/messages?correlation_id=AST-000001&type=StageGateDecision")
    body = r.get_json()
    assert body["count"] == 1
    assert body["messages"][0]["publisher"] == "tim@example.invalid"
    assert body["messages"][0]["envelope"]["payload"]["decision"] == "go"
    assert client.get("/messages?correlation_id=AST-000001").get_json()["count"] == 2
    assert client.get("/messages?correlation_id=AST-000404").get_json()["count"] == 0


def test_publish_422_with_error_list(client):
    bad = msg("StageGateDecision")
    bad["correlation_id"] = "AST-000002"
    bad["payload"]["score"] = 150
    r = client.post("/publish", json=bad)
    assert r.status_code == 422
    errors = r.get_json()["errors"]
    assert all(set(e) == {"path", "message"} for e in errors)
    assert "$.payload.score" in {e["path"] for e in errors}
    r = client.post("/publish", data="not json", content_type="application/json")
    assert r.status_code == 422 and r.get_json()["errors"][0]["path"] == "$"
    assert client.get("/messages").get_json()["count"] == 0


def test_contracts_endpoints_and_health(client):
    body = client.get("/contracts").get_json()
    names = {c["name"]: c for c in body["contracts"]}
    assert names["StageGateDecision"]["pharma"] and not names["CapacityRequest"]["pharma"]
    assert "Common" not in names and "PharmaCommon" not in names
    s = client.get("/contracts/ValuationSnapshot").get_json()
    assert s["$id"] == "urn:cdmo-agent-contracts:ValuationSnapshot:1.0.0"
    assert client.get("/contracts/ValuationSnapshot?version=1").status_code == 200
    assert client.get("/contracts/Nope").status_code == 404
    h = client.get("/healthz").get_json()
    assert h["status"] == "ok" and h["backend"] == "memory" and h["contracts"] >= 10
    assert "/publish" in client.get("/openapi.json").get_json()["paths"]
    assert client.get("/dead-letters").get_json() == {"dead_letters": [], "count": 0}


def test_nomination_over_http_triggers_capacity_request(tmp_path, audit):
    bus = SQLiteBus(tmp_path / "http.db")
    app = create_app(bus, audit=audit)
    c = app.test_client()
    assert c.post("/publish", json=msg("CandidateNomination"), headers={"X-Platform-User": "cso"}).status_code == 202
    rows = c.get("/messages?correlation_id=AST-000001&type=CapacityRequest").get_json()["messages"]
    assert len(rows) == 1
    assert rows[0]["causation_id"] == msg("CandidateNomination")["message_id"]
    assert len(audit.events) == 1
    bus.close()
