"""``pharma-bus`` / ``python -m pharma_bus`` command line."""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
from pathlib import Path

from .bus import Bus, SQLiteBus, open_bus
from .registry import UnknownContract, list_contracts, load_schema, origin
from .validate import ContractValidationError, contract_errors, message_errors

DEFAULT_DB = "data/pharma_bus.db"


def read_records(path: str) -> list[tuple[str, object]]:
    """Read a .json file (one object or a list) or .jsonl (one per line).
    '-' reads stdin. Returns (label, record) pairs; bad JSON comes back as the exception."""
    text = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8-sig")
    if path.endswith((".jsonl", ".ndjson")):
        out: list[tuple[str, object]] = []
        for i, line in enumerate(text.splitlines(), 1):
            if line.strip():
                try:
                    out.append((f"{path}:{i}", json.loads(line)))
                except json.JSONDecodeError as exc:
                    out.append((f"{path}:{i}", exc))
        return out
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return [(path, exc)]
    if isinstance(data, list):
        return [(f"{path}[{i}]", d) for i, d in enumerate(data)]
    return [(path, data)]


def _report(label: str, issues) -> bool:
    if not issues:
        print(f"OK    {label}")
        return True
    print(f"FAIL  {label}")
    for i in issues:
        print(f"        {i}")
    return False


def _bus(args) -> Bus:
    if getattr(args, "url", None):
        return open_bus(args.url)
    return SQLiteBus(args.db or os.environ.get("PHARMA_BUS_DB", DEFAULT_DB))


def _line(rec: dict) -> str:
    who = f" by {rec['publisher']}" if rec.get("publisher") else ""
    return (f"#{rec['seq']:<5} {rec['published_at']}  {rec['message_type']:<20} {rec.get('correlation_id') or '-':<12} "
            f"{rec['message_id']}  from {rec['source_agent']}{who}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="pharma-bus", description="Pharma contracts + event bus.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="list contracts and versions")
    p = sub.add_parser("show", help="print a schema")
    p.add_argument("name")
    p.add_argument("--version")
    p = sub.add_parser("validate", help="validate payload files (.json / .jsonl / -) against a contract")
    p.add_argument("name")
    p.add_argument("files", nargs="+")
    p.add_argument("--version", help="1, 1.0 or 1.0.0 (default: latest)")
    p = sub.add_parser("validate-message", help="validate enveloped messages (.json / .jsonl / -)")
    p.add_argument("files", nargs="+")
    sub.add_parser("check", help="lint every schema and example")
    for name, hlp in (("publish", "publish enveloped messages to a durable bus"),
                      ("tail", "print messages on the bus (or consume them as a group)"),
                      ("worker", "run the built-in flows against a durable bus"),
                      ("serve", "run the HTTP API")):
        p = sub.add_parser(name, help=hlp)
        p.add_argument("--db", help=f"SQLite file (default: $PHARMA_BUS_DB or {DEFAULT_DB})")
        p.add_argument("--url", help="bus URL instead of --db (sqlite:///..., redis://..., memory://)")
        if name == "publish":
            p.add_argument("files", nargs="+")
            p.add_argument("--publisher", help="who is publishing (recorded with the message)")
        if name == "tail":
            p.add_argument("--group", help="consume as this group (advances its offset) instead of reading the log")
            p.add_argument("--type", dest="message_type", help="only this message type")
            p.add_argument("--correlation-id")
            p.add_argument("-n", "--limit", type=int, default=20)
            p.add_argument("-f", "--follow", action="store_true", help="keep polling for new messages")
            p.add_argument("--json", action="store_true", help="print full envelopes as JSON lines")
        if name == "worker":
            p.add_argument("--once", action="store_true", help="drain once and exit")
            p.add_argument("--interval", type=float, default=1.0)
        if name == "serve":
            p.add_argument("--host", default="127.0.0.1")
            p.add_argument("--port", type=int, default=int(os.environ.get("PORT", 5102)))
    args = ap.parse_args(argv)

    try:
        if args.cmd == "list":
            for name, versions in list_contracts().items():
                print(f"{name:<20} {', '.join(versions):<12} {origin(name)}")
            return 0
        if args.cmd == "show":
            print(json.dumps(load_schema(args.name, args.version), indent=2))
            return 0
        if args.cmd in ("validate", "validate-message"):
            ok = True
            for f in args.files:
                if f != "-" and not Path(f).exists():
                    print(f"FAIL  {f}\n        file not found")
                    ok = False
                    continue
                for label, rec in read_records(f):
                    if isinstance(rec, Exception):
                        ok = _report(label, [f"not valid JSON: {rec}"]) and ok
                    elif args.cmd == "validate":
                        ok = _report(label, contract_errors(args.name, rec, args.version)) and ok
                    else:
                        ok = _report(label, message_errors(rec)) and ok
            return 0 if ok else 1
        if args.cmd == "check":
            from .lint import run
            problems = run()
            for pr in problems:
                print(f"  - {pr}")
            print(f"schemas and examples checked: {'OK' if not problems else f'{len(problems)} problem(s)'}")
            return 1 if problems else 0
        if args.cmd == "publish":
            ok = True
            with _bus(args) as bus:
                for f in args.files:
                    if not Path(f).exists():
                        print(f"FAIL  {f}\n        file not found")
                        ok = False
                        continue
                    for label, rec in read_records(f):
                        if isinstance(rec, Exception):
                            ok = _report(label, [f"not valid JSON: {rec}"]) and ok
                            continue
                        try:
                            mid = bus.publish(rec, publisher=args.publisher)  # type: ignore[arg-type]
                            print(f"OK    {label}  -> {mid}")
                        except ContractValidationError as exc:
                            ok = _report(label, exc.issues) and ok
            return 0 if ok else 1
        if args.cmd == "tail":
            return _tail(args)
        if args.cmd == "worker":
            from .flows import attach_default_flows
            with _bus(args) as bus:
                attach_default_flows(bus)
                if args.once:
                    print(f"processed {bus.drain()} message(s)")
                    return 0
                print(f"worker running on {bus.backend} bus; groups: {', '.join(bus.groups())} (Ctrl-C to stop)")
                try:
                    bus.run_forever(interval=args.interval)
                except KeyboardInterrupt:
                    pass
            return 0
        if args.cmd == "serve":
            from .http import create_app
            create_app(_bus(args)).run(host=args.host, port=args.port)
            return 0
    except UnknownContract as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    return 2


def _tail(args) -> int:
    def show(rec_or_env: dict) -> None:
        if args.json:
            print(json.dumps(rec_or_env.get("envelope", rec_or_env)), flush=True)
        elif "seq" in rec_or_env:
            print(_line(rec_or_env), flush=True)
        else:  # an envelope delivered to a group
            e = rec_or_env
            print(f"{e['sent_at']}  {e['message_type']:<20} {e.get('correlation_id') or '-':<12} {e['message_id']}"
                  f"  from {e['source_agent']}", flush=True)

    with _bus(args) as bus:
        if args.group:
            def handler(env: dict) -> None:
                if args.correlation_id and env.get("correlation_id") != args.correlation_id:
                    return
                show(env)
            bus.subscribe(args.message_type or "*", handler, group=args.group)
            if not args.follow:
                bus.drain()
                return 0
            try:
                bus.run_forever(interval=1.0)
            except KeyboardInterrupt:
                pass
            return 0
        rows = bus.messages(correlation_id=args.correlation_id, message_type=args.message_type, limit=args.limit)
        for r in rows:
            show(r)
        if not args.follow:
            return 0
        last = rows[-1]["seq"] if rows else 0
        stop = threading.Event()
        try:
            while not stop.is_set():
                new = [r for r in bus.messages(correlation_id=args.correlation_id, message_type=args.message_type,
                                               limit=1000) if r["seq"] > last]
                for r in new:
                    show(r)
                    last = r["seq"]
                stop.wait(1.0)
        except KeyboardInterrupt:
            pass
    return 0
