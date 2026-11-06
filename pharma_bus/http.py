"""HTTP API for the bus (Flask, default port 5102).

    POST /publish                 envelope JSON -> 202 {"message_id"} | 422 {"errors": [{"path","message"}]}
    GET  /contracts               contract catalogue
    GET  /contracts/<name>        schema (?version=1.0.0)
    GET  /messages                ?correlation_id=AST-000001&type=StageGateDecision&limit=100
    GET  /dead-letters            ?group=cmc-planner
    GET  /healthz
    GET  /openapi.json

``X-Platform-User`` (set by the PharmaPlatformShell gateway) is recorded as the
publisher of a message. With ``DRAIN_ON_PUBLISH`` (default on) the server
delivers to its in-process subscribers (the built-in flows) right after each
publish; separate workers can consume the same SQLite file with
``pharma-bus worker``.
"""
from __future__ import annotations

import os
from typing import Any

from flask import Flask, jsonify, request

from .bus import Bus, open_bus
from .registry import PHARMA_CONTRACTS, UnknownContract, catalog, list_contracts, load_schema, origin, resolve_version, upstream_schema_dir
from .validate import ContractValidationError, Issue

DEFAULT_PORT = 5102


def _contract_entry(name: str, versions: list[str]) -> dict[str, Any]:
    return {"name": name, "versions": versions, "latest": versions[-1], "origin": origin(name),
            "pharma": name in PHARMA_CONTRACTS}


def _openapi() -> dict:
    def op(summary: str, responses: dict[str, str]) -> dict:
        return {"summary": summary, "responses": {k: {"description": v} for k, v in responses.items()}}
    return {
        "openapi": "3.0.3",
        "info": {"title": "PharmaEventBus", "version": "0.1.0"},
        "paths": {
            "/publish": {"post": op("Publish an enveloped contract message", {"202": "accepted", "422": "invalid"})},
            "/contracts": {"get": op("List contracts and versions", {"200": "catalogue"})},
            "/contracts/{name}": {"get": op("Get a contract schema", {"200": "schema", "404": "unknown"})},
            "/messages": {"get": op("Query stored messages by correlation_id / type", {"200": "messages"})},
            "/dead-letters": {"get": op("Messages a consumer group gave up on", {"200": "dead letters"})},
            "/healthz": {"get": op("Liveness", {"200": "ok"})},
        },
    }


def create_app(bus: Bus | None = None, attach_flows: bool = True, audit: Any = None,
               drain_on_publish: bool = True) -> Flask:
    """Build the Flask app. ``bus`` defaults to ``open_bus()`` (env ``PHARMA_BUS_URL``)."""
    app = Flask(__name__)
    app.config["BUS"] = bus = bus if bus is not None else open_bus()
    app.config["DRAIN_ON_PUBLISH"] = drain_on_publish
    if attach_flows:
        from .flows import attach_default_flows
        app.config["FLOWS"] = attach_default_flows(bus, audit=audit)

    def _errors(issues: list[Issue], status: int = 422):
        return jsonify({"errors": [i.as_dict() for i in issues]}), status

    @app.post("/publish")
    def publish():
        envelope = request.get_json(silent=True)
        if not isinstance(envelope, dict):
            return _errors([Issue("$", "body must be a JSON object (a message envelope)")])
        publisher = (request.headers.get("X-Platform-User") or "").strip() or None
        try:
            mid = bus.publish(envelope, publisher=publisher)
        except ContractValidationError as exc:
            return _errors(exc.issues)
        if app.config["DRAIN_ON_PUBLISH"]:
            bus.drain()
        return jsonify({"message_id": mid}), 202

    @app.get("/contracts")
    def contracts():
        return jsonify({"contracts": [_contract_entry(n, v) for n, v in list_contracts().items()]})

    @app.get("/contracts/<name>")
    def contract(name: str):
        try:
            v = resolve_version(name, request.args.get("version"))
            return jsonify(load_schema(name, v))
        except UnknownContract as exc:
            return jsonify({"error": str(exc)}), 404

    @app.get("/messages")
    def messages():
        try:
            limit = max(1, min(int(request.args.get("limit", 100)), 1000))
        except ValueError:
            return jsonify({"error": "limit must be an integer"}), 400
        rows = bus.messages(correlation_id=request.args.get("correlation_id") or None,
                            message_type=request.args.get("type") or None, limit=limit)
        return jsonify({"messages": rows, "count": len(rows)})

    @app.get("/dead-letters")
    def dead_letters():
        rows = bus.dead_letters(group=request.args.get("group") or None)
        return jsonify({"dead_letters": rows, "count": len(rows)})

    @app.get("/healthz")
    def healthz():
        return jsonify({"status": "ok", "backend": bus.backend, "contracts": len(list_contracts()),
                        "upstream_schemas": upstream_schema_dir() is not None,
                        "manufacturing_contracts": sorted(set(catalog()) - set(PHARMA_CONTRACTS)
                                                          - {"Common", "PharmaCommon", "MessageEnvelope"})})

    @app.get("/openapi.json")
    def openapi():
        return jsonify(_openapi())

    return app


def serve(host: str = "127.0.0.1", port: int | None = None, bus: Bus | None = None) -> None:
    """Run the development server (port from ``PORT`` or 5102)."""
    port = port or int(os.environ.get("PORT", DEFAULT_PORT))
    create_app(bus).run(host=host, port=port)
