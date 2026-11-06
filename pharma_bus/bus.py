"""The event bus: tools publish contract messages and subscribe to them.

Every backend shares one interface (:class:`Bus`):

* ``publish(envelope, publisher=None) -> message_id`` validates the envelope
  and payload first; an invalid message raises ``ContractValidationError``
  and is never stored or delivered.
* ``subscribe(message_type | "*", handler, group)`` registers a handler.
  A *group* is one logical consumer (one tool). Each message is delivered once
  per group; different groups each get their own copy (fan-out). Within a
  group, at most one handler is called per message: the exact-type handler if
  there is one, otherwise the group's ``"*"`` handler.
* A handler that raises is retried, up to ``max_attempts`` in total, then the
  message goes to the dead-letter store with the error, and the group moves on.

Backends:

* :class:`InMemoryBus` dispatches synchronously inside ``publish``. For tests
  and single-process use; nothing survives the process.
* :class:`SQLiteBus` is durable: an append-only ``messages`` log plus
  per-group consumer offsets. Delivery happens in ``poll()`` / ``run_once()`` /
  ``drain()`` (or ``run_forever()`` in a worker). Offsets survive restarts.
* :class:`RedisStreamsBus` uses Redis Streams consumer groups
  (XADD / XGROUP CREATE / XREADGROUP / XACK). Needs the ``redis`` extra.

Azure Service Bus is the intended production target (topics + subscriptions
map one-to-one onto message types + groups); see README "What's next".
"""
from __future__ import annotations

import json
import os
import socket
import sqlite3
import threading
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .registry import resolve_version
from .validate import ContractValidationError, message_errors, utc_now

Handler = Callable[[dict], Any]
WILDCARD = "*"


@dataclass(frozen=True)
class Subscription:
    """A registered handler: which messages, which consumer group."""

    message_type: str
    group: str
    handler: Handler


def _default_group(handler: Handler) -> str:
    mod = getattr(handler, "__module__", None) or "handler"
    name = getattr(handler, "__qualname__", None) or type(handler).__name__
    return f"{mod}.{name}"


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:4000]


def _record(seq: int, envelope: dict, publisher: str | None, published_at: str) -> dict:
    return {
        "seq": seq,
        "message_id": envelope["message_id"],
        "message_type": envelope["message_type"],
        "schema_version": envelope["schema_version"],
        "correlation_id": envelope.get("correlation_id"),
        "causation_id": envelope.get("causation_id"),
        "source_agent": envelope["source_agent"],
        "publisher": publisher,
        "published_at": published_at,
        "envelope": envelope,
    }


def _matches(rec: dict, correlation_id: str | None, message_type: str | None) -> bool:
    return (correlation_id is None or rec["correlation_id"] == correlation_id) and (
        message_type is None or rec["message_type"] == message_type)


class Bus(ABC):
    """Common publish / subscribe behaviour; backends store and deliver."""

    backend = "abstract"

    def __init__(self, max_attempts: int = 3):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.max_attempts = max_attempts
        self._subs: dict[str, dict[str, Handler]] = {}
        self._lock = threading.RLock()

    # ---------------------------------------------------------------- publish
    def publish(self, envelope: dict, publisher: str | None = None) -> str:
        """Validate and append a message; return its message_id.

        Raises ``ContractValidationError`` (with ``.issues``) if the envelope or
        payload breaks its contract. ``publisher`` records who published it
        (e.g. the X-Platform-User the gateway passed), separate from the
        envelope's ``source_agent``."""
        issues = message_errors(envelope)
        if issues:
            name = envelope.get("message_type", "?") if isinstance(envelope, dict) else "?"
            raise ContractValidationError(f"MessageEnvelope<{name}>", resolve_version("MessageEnvelope"), issues)
        return self._append(envelope, publisher)

    # -------------------------------------------------------------- subscribe
    def subscribe(self, message_type: str, handler: Handler, group: str | None = None) -> Subscription:
        """Register ``handler`` for ``message_type`` (or ``"*"``) in ``group``.

        ``group`` defaults to the handler's qualified name. A group may have
        one handler per message type; registering a second raises ValueError."""
        if message_type != WILDCARD:
            resolve_version(message_type)  # UnknownContract for typos
        group = group or _default_group(handler)
        with self._lock:
            handlers = self._subs.setdefault(group, {})
            if message_type in handlers:
                raise ValueError(f"group {group!r} already has a handler for {message_type!r}")
            handlers[message_type] = handler
            self._on_subscribe(group)
        return Subscription(message_type, group, handler)

    def unsubscribe(self, group: str, message_type: str | None = None) -> None:
        """Remove one handler (or every handler) of a group. Offsets are kept."""
        with self._lock:
            if message_type is None:
                self._subs.pop(group, None)
            else:
                self._subs.get(group, {}).pop(message_type, None)

    def groups(self) -> list[str]:
        return sorted(self._subs)

    def _handler_for(self, group: str, message_type: str) -> Handler | None:
        handlers = self._subs.get(group, {})
        return handlers.get(message_type) or handlers.get(WILDCARD)

    def _on_subscribe(self, group: str) -> None:
        """Hook for backends that keep per-group state."""

    # ---------------------------------------------------------------- deliver
    def poll(self, group: str | None = None, max_messages: int = 100) -> int:
        """Deliver pending messages to one group (or all). Returns how many
        log entries were processed (delivered, attempted or skipped)."""
        with self._lock:
            groups = [group] if group else list(self._subs)
        return sum(self._poll_group(g, max_messages) for g in groups)

    def run_once(self) -> int:
        """One delivery pass over every subscribed group."""
        return self.poll()

    def drain(self, max_rounds: int = 1000) -> int:
        """Poll until nothing is left (handlers may publish follow-on messages)."""
        total = 0
        for _ in range(max_rounds):
            n = self.poll()
            if n == 0:
                break
            total += n
        return total

    def run_forever(self, interval: float = 1.0, stop: threading.Event | None = None) -> None:
        """Worker loop: poll, sleep ``interval`` seconds when idle, until ``stop`` is set."""
        stop = stop or threading.Event()
        while not stop.is_set():
            if self.poll() == 0:
                stop.wait(interval)

    def _poll_group(self, group: str, max_messages: int) -> int:
        return 0

    # ------------------------------------------------------------------ query
    @abstractmethod
    def _append(self, envelope: dict, publisher: str | None) -> str: ...

    @abstractmethod
    def messages(self, correlation_id: str | None = None, message_type: str | None = None,
                 limit: int = 100) -> list[dict]:
        """Stored messages, oldest first, optionally filtered; at most ``limit`` (newest kept)."""

    @abstractmethod
    def dead_letters(self, group: str | None = None, limit: int = 100) -> list[dict]:
        """Messages a group gave up on, with the error and attempt count."""

    def close(self) -> None:
        """Release resources."""

    def __enter__(self) -> "Bus":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ============================================================== in-memory
class InMemoryBus(Bus):
    """Synchronous, single-process bus. ``publish`` delivers before it returns.

    Messages a handler publishes are queued and delivered after the current
    one finishes (breadth-first), so chains do not recurse."""

    backend = "memory"

    def __init__(self, max_attempts: int = 3):
        super().__init__(max_attempts)
        self._log: list[dict] = []
        self._dead: list[dict] = []
        self._ids: set[str] = set()
        self._queue: deque[dict] = deque()
        self._dispatching = False

    def _append(self, envelope: dict, publisher: str | None) -> str:
        with self._lock:
            mid = envelope["message_id"]
            if mid in self._ids:  # idempotent re-publish
                return mid
            rec = _record(len(self._log) + 1, json.loads(json.dumps(envelope)), publisher, utc_now())
            self._log.append(rec)
            self._ids.add(mid)
            self._queue.append(rec)
            if self._dispatching:
                return mid
            self._dispatching = True
            try:
                while self._queue:
                    self._dispatch(self._queue.popleft())
            finally:
                self._dispatching = False
            return mid

    def _dispatch(self, rec: dict) -> None:
        for group in list(self._subs):
            handler = self._handler_for(group, rec["message_type"])
            if handler is None:
                continue
            last: BaseException | None = None
            for _attempt in range(self.max_attempts):
                try:
                    handler(rec["envelope"])
                    last = None
                    break
                except Exception as exc:  # noqa: BLE001 - any handler failure is retried
                    last = exc
            if last is not None:
                self._dead.append({
                    "id": len(self._dead) + 1, "group": group, "seq": rec["seq"],
                    "message_id": rec["message_id"], "message_type": rec["message_type"],
                    "correlation_id": rec["correlation_id"], "attempts": self.max_attempts,
                    "error": _error_text(last), "dead_at": utc_now(), "envelope": rec["envelope"],
                })

    def messages(self, correlation_id: str | None = None, message_type: str | None = None,
                 limit: int = 100) -> list[dict]:
        out = [r for r in self._log if _matches(r, correlation_id, message_type)]
        return out[-limit:] if limit else out

    def dead_letters(self, group: str | None = None, limit: int = 100) -> list[dict]:
        out = [d for d in self._dead if group is None or d["group"] == group]
        return out[-limit:] if limit else out


# ================================================================= SQLite
_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id     TEXT NOT NULL UNIQUE,
    message_type   TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    correlation_id TEXT,
    causation_id   TEXT,
    source_agent   TEXT NOT NULL,
    publisher      TEXT,
    published_at   TEXT NOT NULL,
    envelope       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_messages_corr ON messages(correlation_id);
CREATE INDEX IF NOT EXISTS ix_messages_type ON messages(message_type);
CREATE TRIGGER IF NOT EXISTS messages_no_update BEFORE UPDATE ON messages
BEGIN SELECT RAISE(ABORT, 'messages is append-only'); END;
CREATE TRIGGER IF NOT EXISTS messages_no_delete BEFORE DELETE ON messages
BEGIN SELECT RAISE(ABORT, 'messages is append-only'); END;

CREATE TABLE IF NOT EXISTS consumer_offsets (
    group_name TEXT PRIMARY KEY,
    last_seq   INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_attempts (
    group_name TEXT NOT NULL,
    seq        INTEGER NOT NULL,
    attempts   INTEGER NOT NULL,
    last_error TEXT,
    PRIMARY KEY (group_name, seq)
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name     TEXT NOT NULL,
    seq            INTEGER NOT NULL,
    message_id     TEXT NOT NULL,
    message_type   TEXT NOT NULL,
    correlation_id TEXT,
    attempts       INTEGER NOT NULL,
    error          TEXT NOT NULL,
    dead_at        TEXT NOT NULL,
    envelope       TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS dead_letters_no_update BEFORE UPDATE ON dead_letters
BEGIN SELECT RAISE(ABORT, 'dead_letters is append-only'); END;
"""


class SQLiteBus(Bus):
    """Durable bus on one SQLite file.

    ``messages`` is append-only (UPDATE / DELETE are blocked by triggers).
    Each group has an offset (last delivered ``seq``); a failing message stays
    at the head of its group, is retried on the next poll, and after
    ``max_attempts`` goes to ``dead_letters`` so the group can move on.

    ``start_from`` controls where a *new* group begins: ``"earliest"`` (replay
    the whole log, default) or ``"latest"`` (only messages published later).
    One process should consume a given group at a time."""

    backend = "sqlite"

    def __init__(self, path: str | os.PathLike = "data/pharma_bus.db", max_attempts: int = 3,
                 start_from: str = "earliest"):
        super().__init__(max_attempts)
        if start_from not in ("earliest", "latest"):
            raise ValueError("start_from must be 'earliest' or 'latest'")
        self.path = str(path)
        self.start_from = start_from
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None, timeout=30)
        self._conn.row_factory = sqlite3.Row
        if self.path != ":memory:":
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(_SQLITE_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ----------------------------------------------------------------- write
    def _append(self, envelope: dict, publisher: str | None) -> str:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO messages (message_id, message_type, schema_version, correlation_id, "
                "causation_id, source_agent, publisher, published_at, envelope) VALUES (?,?,?,?,?,?,?,?,?)",
                (envelope["message_id"], envelope["message_type"], envelope["schema_version"],
                 envelope.get("correlation_id"), envelope.get("causation_id"), envelope["source_agent"],
                 publisher, utc_now(), json.dumps(envelope, sort_keys=True)),
            )
        return envelope["message_id"]

    def _on_subscribe(self, group: str) -> None:
        start = 0
        if self.start_from == "latest":
            start = self._conn.execute("SELECT COALESCE(MAX(seq), 0) FROM messages").fetchone()[0]
        self._conn.execute("INSERT OR IGNORE INTO consumer_offsets (group_name, last_seq, updated_at) VALUES (?,?,?)",
                           (group, start, utc_now()))

    def offset(self, group: str) -> int | None:
        row = self._conn.execute("SELECT last_seq FROM consumer_offsets WHERE group_name = ?", (group,)).fetchone()
        return None if row is None else int(row[0])

    def _advance(self, group: str, seq: int) -> None:
        self._conn.execute("UPDATE consumer_offsets SET last_seq = ?, updated_at = ? WHERE group_name = ? AND last_seq < ?",
                           (seq, utc_now(), group, seq))
        self._conn.execute("DELETE FROM delivery_attempts WHERE group_name = ? AND seq = ?", (group, seq))

    # --------------------------------------------------------------- deliver
    def _poll_group(self, group: str, max_messages: int) -> int:
        with self._lock:
            if group not in self._subs:
                return 0
            self._on_subscribe(group)
            last = self.offset(group) or 0
            rows = self._conn.execute("SELECT * FROM messages WHERE seq > ? ORDER BY seq LIMIT ?",
                                      (last, max_messages)).fetchall()
        processed = 0
        for row in rows:
            processed += 1
            handler = self._handler_for(group, row["message_type"])
            if handler is None:
                with self._lock:
                    self._advance(group, row["seq"])
                continue
            envelope = json.loads(row["envelope"])
            try:
                handler(envelope)
            except Exception as exc:  # noqa: BLE001 - any handler failure is retried
                if self._record_failure(group, row, envelope, exc):
                    continue  # dead-lettered; group moves on
                break  # keep order: retry this message on the next poll
            with self._lock:
                self._advance(group, row["seq"])
        return processed

    def _record_failure(self, group: str, row: sqlite3.Row, envelope: dict, exc: BaseException) -> bool:
        """Count a failed attempt; dead-letter after max_attempts. True if dead-lettered."""
        err = _error_text(exc)
        with self._lock:
            self._conn.execute(
                "INSERT INTO delivery_attempts (group_name, seq, attempts, last_error) VALUES (?,?,1,?) "
                "ON CONFLICT(group_name, seq) DO UPDATE SET attempts = attempts + 1, last_error = excluded.last_error",
                (group, row["seq"], err))
            attempts = self._conn.execute("SELECT attempts FROM delivery_attempts WHERE group_name = ? AND seq = ?",
                                          (group, row["seq"])).fetchone()[0]
            if attempts < self.max_attempts:
                return False
            self._conn.execute("BEGIN")
            try:
                self._conn.execute(
                    "INSERT INTO dead_letters (group_name, seq, message_id, message_type, correlation_id, attempts, "
                    "error, dead_at, envelope) VALUES (?,?,?,?,?,?,?,?,?)",
                    (group, row["seq"], row["message_id"], row["message_type"], row["correlation_id"], attempts,
                     err, utc_now(), json.dumps(envelope, sort_keys=True)))
                self._advance(group, row["seq"])
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            return True

    # ----------------------------------------------------------------- query
    def messages(self, correlation_id: str | None = None, message_type: str | None = None,
                 limit: int = 100, after_seq: int = 0) -> list[dict]:
        sql, args = "SELECT * FROM messages WHERE seq > ?", [after_seq]
        if correlation_id is not None:
            sql += " AND correlation_id = ?"
            args.append(correlation_id)
        if message_type is not None:
            sql += " AND message_type = ?"
            args.append(message_type)
        sql += " ORDER BY seq DESC LIMIT ?"
        args.append(limit if limit else -1)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [_record(r["seq"], json.loads(r["envelope"]), r["publisher"], r["published_at"]) for r in reversed(rows)]

    def dead_letters(self, group: str | None = None, limit: int = 100) -> list[dict]:
        sql, args = "SELECT * FROM dead_letters", []
        if group is not None:
            sql += " WHERE group_name = ?"
            args.append(group)
        sql += " ORDER BY id DESC LIMIT ?"
        args.append(limit if limit else -1)
        with self._lock:
            rows = self._conn.execute(sql, args).fetchall()
        return [{"id": r["id"], "group": r["group_name"], "seq": r["seq"], "message_id": r["message_id"],
                 "message_type": r["message_type"], "correlation_id": r["correlation_id"],
                 "attempts": r["attempts"], "error": r["error"], "dead_at": r["dead_at"],
                 "envelope": json.loads(r["envelope"])} for r in reversed(rows)]


# ========================================================== Redis Streams
class RedisStreamsBus(Bus):
    """Redis Streams backend (``pip install "pharma-event-bus[redis]"``).

    One stream holds every message (``<prefix>:messages``); each group is a
    Redis consumer group on it. ``poll`` first re-reads this consumer's pending
    (un-acked) entries, which is how failed messages are retried, then reads
    new ones with ``XREADGROUP ... >``. Success is ``XACK``ed; after
    ``max_attempts`` failures the entry is copied to ``<prefix>:dead-letters``
    and acked."""

    backend = "redis"

    def __init__(self, url: str = "redis://localhost:6379/0", prefix: str = "pharma-bus", max_attempts: int = 3,
                 consumer: str | None = None, start_from: str = "earliest", client: Any = None):
        super().__init__(max_attempts)
        if client is None:
            try:
                import redis  # type: ignore[import-not-found]
            except ImportError as exc:  # pragma: no cover - depends on environment
                raise ImportError('RedisStreamsBus needs redis-py: pip install "pharma-event-bus[redis]"') from exc
            client = redis.Redis.from_url(url, decode_responses=True)
        if start_from not in ("earliest", "latest"):
            raise ValueError("start_from must be 'earliest' or 'latest'")
        self.r = client
        self.prefix = prefix
        self.stream = f"{prefix}:messages"
        self.dead_stream = f"{prefix}:dead-letters"
        self.consumer = consumer or f"{socket.gethostname()}-{os.getpid()}"
        self.start_from = start_from

    def close(self) -> None:
        try:
            self.r.close()
        except Exception:  # noqa: BLE001
            pass

    def _append(self, envelope: dict, publisher: str | None) -> str:
        fields = {
            "message_id": envelope["message_id"],
            "message_type": envelope["message_type"],
            "correlation_id": envelope.get("correlation_id") or "",
            "publisher": publisher or "",
            "published_at": utc_now(),
            "envelope": json.dumps(envelope, sort_keys=True),
        }
        self.r.xadd(self.stream, fields)
        return envelope["message_id"]

    def _on_subscribe(self, group: str) -> None:
        try:
            self.r.xgroup_create(self.stream, group, id="0" if self.start_from == "earliest" else "$", mkstream=True)
        except Exception as exc:  # noqa: BLE001 - BUSYGROUP means it already exists
            if "BUSYGROUP" not in str(exc):
                raise

    def _read(self, group: str, start: str, count: int) -> list[tuple[str, dict]]:
        resp = self.r.xreadgroup(group, self.consumer, {self.stream: start}, count=count)
        if not resp:
            return []
        _stream, entries = resp[0]
        return list(entries)

    def _poll_group(self, group: str, max_messages: int) -> int:
        if group not in self._subs:
            return 0
        entries = self._read(group, "0", max_messages) or self._read(group, ">", max_messages)
        attempts_key = f"{self.prefix}:attempts:{group}"
        processed = 0
        for entry_id, fields in entries:
            processed += 1
            if not fields:  # entry trimmed from the stream while pending
                self.r.xack(self.stream, group, entry_id)
                continue
            handler = self._handler_for(group, fields["message_type"])
            if handler is None:
                self.r.xack(self.stream, group, entry_id)
                continue
            envelope = json.loads(fields["envelope"])
            try:
                handler(envelope)
            except Exception as exc:  # noqa: BLE001
                n = int(self.r.hincrby(attempts_key, entry_id, 1))
                if n < self.max_attempts:
                    break
                self.r.xadd(self.dead_stream, {
                    "group": group, "entry_id": entry_id, "message_id": fields["message_id"],
                    "message_type": fields["message_type"], "correlation_id": fields.get("correlation_id", ""),
                    "attempts": str(n), "error": _error_text(exc), "dead_at": utc_now(),
                    "envelope": fields["envelope"]})
                self.r.xack(self.stream, group, entry_id)
                self.r.hdel(attempts_key, entry_id)
                continue
            self.r.xack(self.stream, group, entry_id)
            self.r.hdel(attempts_key, entry_id)
        return processed

    def messages(self, correlation_id: str | None = None, message_type: str | None = None,
                 limit: int = 100) -> list[dict]:
        out = []
        for i, (_eid, f) in enumerate(self.r.xrange(self.stream, "-", "+"), 1):
            rec = _record(i, json.loads(f["envelope"]), f.get("publisher") or None, f["published_at"])
            if _matches(rec, correlation_id, message_type):
                out.append(rec)
        return out[-limit:] if limit else out

    def dead_letters(self, group: str | None = None, limit: int = 100) -> list[dict]:
        out = []
        for i, (eid, f) in enumerate(self.r.xrange(self.dead_stream, "-", "+"), 1):
            if group is None or f["group"] == group:
                out.append({"id": eid, "group": f["group"], "seq": i, "message_id": f["message_id"],
                            "message_type": f["message_type"], "correlation_id": f.get("correlation_id") or None,
                            "attempts": int(f["attempts"]), "error": f["error"], "dead_at": f["dead_at"],
                            "envelope": json.loads(f["envelope"])})
        return out[-limit:] if limit else out


# ================================================================ factory
def open_bus(url: str | None = None, **kwargs: Any) -> Bus:
    """Open a bus from a URL (default: env ``PHARMA_BUS_URL`` or ``sqlite:///data/pharma_bus.db``).

    * ``memory://``
    * ``sqlite:///relative/path.db`` or ``sqlite:////absolute/path.db`` (or a bare ``*.db`` path)
    * ``redis://host:6379/0`` / ``rediss://...``
    * ``azure-servicebus://...`` is the production target and not implemented in v1.
    """
    url = url or os.environ.get("PHARMA_BUS_URL") or "sqlite:///data/pharma_bus.db"
    if url.startswith("memory://"):
        return InMemoryBus(**kwargs)
    if url.startswith("sqlite://"):
        return SQLiteBus(url[len("sqlite:///"):] if url.startswith("sqlite:///") else url[len("sqlite://"):], **kwargs)
    if url.startswith(("redis://", "rediss://", "unix://")):
        return RedisStreamsBus(url, **kwargs)
    if url.startswith(("azure-servicebus://", "sb://")):
        raise NotImplementedError("Azure Service Bus backend is planned (see README 'What's next'); "
                                  "use sqlite:// or redis:// in v1")
    if url.endswith(".db") or url == ":memory:":
        return SQLiteBus(url, **kwargs)
    raise ValueError(f"unrecognised bus URL {url!r}")

