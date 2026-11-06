#!/usr/bin/env python3
"""Copy (or check) the vendored CDMOAgentContracts schemas.

    python scripts/sync_upstream.py /path/to/CDMOAgentContracts            # copy files, update manifest
    python scripts/sync_upstream.py /path/to/CDMOAgentContracts --check    # exit 1 if they differ
    python scripts/sync_upstream.py --verify                               # files still match vendor/upstream.json

The upstream path may be the checkout root or its schemas/ folder. Defaults to
$CDMO_CONTRACTS_SCHEMA_DIR. After a copy, record the new upstream commit with
--commit <sha> (written to vendor/upstream.json; update vendor/UPSTREAM.md too).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = ROOT / "schemas"
MANIFEST = ROOT / "vendor" / "upstream.json"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def upstream_dir(arg: str | None) -> Path:
    raw = arg or os.environ.get("CDMO_CONTRACTS_SCHEMA_DIR")
    if not raw:
        sys.exit("give the upstream checkout path or set CDMO_CONTRACTS_SCHEMA_DIR")
    p = Path(raw)
    if (p / "schemas").is_dir():
        p = p / "schemas"
    if not (p / "Common").is_dir():
        sys.exit(f"{raw} does not look like a CDMOAgentContracts checkout (no schemas/Common)")
    return p


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("upstream", nargs="?")
    ap.add_argument("--check", action="store_true", help="compare only; exit 1 on any difference")
    ap.add_argument("--verify", action="store_true", help="check local files against vendor/upstream.json")
    ap.add_argument("--commit", help="upstream commit sha to record after a copy")
    args = ap.parse_args(argv)
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))

    if args.verify:
        bad = [f for f, h in manifest["files"].items() if sha256(SCHEMAS / f) != h]
        for f in bad:
            print(f"MODIFIED  schemas/{f} (vendored files must stay byte-identical to upstream)")
        print("vendored schemas match manifest" if not bad else f"{len(bad)} vendored file(s) changed")
        return 1 if bad else 0

    src = upstream_dir(args.upstream)
    differ = [f for f in manifest["files"] if not (src / f).exists() or (src / f).read_bytes() != (SCHEMAS / f).read_bytes()]
    if args.check:
        for f in differ:
            print(f"DIFFERS   {f}")
        print("vendored schemas identical to upstream" if not differ else f"{len(differ)} file(s) differ")
        return 1 if differ else 0
    for f in manifest["files"]:
        (SCHEMAS / f).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src / f, SCHEMAS / f)
        manifest["files"][f] = sha256(SCHEMAS / f)
        print(f"copied    {f}")
    if args.commit:
        manifest["commit"] = args.commit
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
