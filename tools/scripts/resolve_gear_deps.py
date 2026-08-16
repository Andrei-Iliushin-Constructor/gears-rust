#!/usr/bin/env python3
"""
resolve_gear_deps.py — Resolve all workspace crates belonging to a gear and
their in-repo reverse dependencies.

Given a gear name (e.g. ``file-parser``), this script:

1. Discovers every Cargo crate under ``gears/<gear>/`` (main, SDK, plugins,
   libs — anything with a ``Cargo.toml``).
2. Builds a reverse-dependency graph from ``cargo metadata --no-deps`` over all
   workspace members.
3. Walks the graph transitively to find every workspace crate that depends on
   any of the gear's own crates.
4. Prints the combined set as ``-p <pkg1> -p <pkg2> …`` (suitable for passing
   to ``cargo nextest run``, ``cargo clippy``, etc.).

Usage:
  python resolve_gear_deps.py <gear>           # e.g. file-parser
  python resolve_gear_deps.py <gear> --names   # print package names only (one per line)

The script exits 0 on success and 1 if the gear directory does not exist or
contains no crates.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Set

ROOT_DIR = Path(__file__).resolve().parent.parent.parent


def _find_gear_dir(gear: str) -> Path:
    """Locate the gear directory by searching standard layout roots.

    *gear* may be a bare name (``file-parser``) or a relative path
    (``libs/toolkit-node-info``, ``gears/system/cluster``).  Trailing
    slashes are stripped.

    Search order for bare names (first match wins):
      gears/<gear>/          — top-level gear  (e.g. file-parser)
      gears/*/<gear>/        — grouped gear    (e.g. bss/ledger, system/cluster)
      libs/<gear>/           — library crate   (e.g. toolkit-db)
    """
    # If gear looks like a relative path, resolve it directly
    gear = gear.rstrip("/")
    if "/" in gear:
        candidate = ROOT_DIR / gear
        if candidate.is_dir():
            return candidate
        print(f"ERROR: gear path not found: {candidate}", file=sys.stderr)
        sys.exit(1)

    # Direct child of gears/
    candidate = ROOT_DIR / "gears" / gear
    if candidate.is_dir():
        return candidate

    # Grouped under a sub-directory of gears/ (system/, bss/, chat-engine/, …)
    for sub in sorted((ROOT_DIR / "gears").iterdir()):
        if sub.is_dir():
            candidate = sub / gear
            if candidate.is_dir():
                return candidate

    # Under libs/
    candidate = ROOT_DIR / "libs" / gear
    if candidate.is_dir():
        return candidate

    # Nothing found — list searched locations for a helpful error
    searched = [
        f"  gears/{gear}/",
        f"  gears/*/{gear}/",
        f"  libs/{gear}/",
    ]
    print(
        f"ERROR: gear directory not found for '{gear}'.\n"
        f"Searched:\n" + "\n".join(searched),
        file=sys.stderr,
    )
    sys.exit(1)


def discover_gear_packages(gear: str) -> List[str]:
    """Return package names for every Cargo.toml under the gear directory."""
    gear_dir = _find_gear_dir(gear)

    manifests = sorted(gear_dir.rglob("Cargo.toml"))
    # Filter out anything inside a target/ directory
    manifests = [m for m in manifests if "target" not in m.parts]

    if not manifests:
        print(f"ERROR: no Cargo.toml found under {gear_dir}", file=sys.stderr)
        sys.exit(1)

    # Read package names via cargo metadata for accuracy
    meta = _load_cargo_metadata()
    pkg_by_manifest: Dict[str, str] = {}
    for pkg in meta["packages"]:
        mpath = Path(pkg["manifest_path"])
        pkg_by_manifest[str(mpath)] = pkg["name"]

    gear_pkgs: List[str] = []
    for m in manifests:
        name = pkg_by_manifest.get(str(m))
        if name:
            gear_pkgs.append(name)

    if not gear_pkgs:
        print(f"ERROR: no workspace packages found under {gear_dir}", file=sys.stderr)
        sys.exit(1)

    return gear_pkgs


_METADATA_CACHE: dict | None = None


def _load_cargo_metadata() -> dict:
    global _METADATA_CACHE
    if _METADATA_CACHE is not None:
        return _METADATA_CACHE

    proc = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        capture_output=True, text=True, cwd=str(ROOT_DIR),
    )
    if proc.returncode != 0:
        print(f"ERROR: cargo metadata failed:\n{proc.stderr}", file=sys.stderr)
        sys.exit(1)

    _METADATA_CACHE = json.loads(proc.stdout)
    return _METADATA_CACHE


def build_reverse_dep_graph(meta: dict) -> Dict[str, Set[str]]:
    """Build a mapping: package_name -> set of workspace packages that depend on it."""
    workspace_names: Set[str] = {pkg["name"] for pkg in meta["packages"]}

    rev: Dict[str, Set[str]] = {name: set() for name in workspace_names}
    for pkg in meta["packages"]:
        for dep in pkg.get("dependencies", []):
            dep_name = dep["name"]
            if dep_name in workspace_names:
                rev[dep_name].add(pkg["name"])

    return rev


def transitive_rdeps(seeds: List[str], rev_graph: Dict[str, Set[str]]) -> Set[str]:
    """Walk the reverse-dependency graph from *seeds* and return all reachable packages."""
    visited: Set[str] = set()
    stack = list(seeds)
    while stack:
        pkg = stack.pop()
        if pkg in visited:
            continue
        visited.add(pkg)
        for rdep in rev_graph.get(pkg, set()):
            if rdep not in visited:
                stack.append(rdep)
    return visited


def resolve(gear: str) -> List[str]:
    """Return the sorted list of all packages to test for *gear*."""
    gear_pkgs = discover_gear_packages(gear)
    meta = _load_cargo_metadata()
    rev_graph = build_reverse_dep_graph(meta)
    all_pkgs = transitive_rdeps(gear_pkgs, rev_graph)
    return sorted(all_pkgs)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve gear crates and their reverse dependencies.",
    )
    parser.add_argument("gear", help="Gear name (e.g. file-parser).")
    parser.add_argument(
        "--names", action="store_true",
        help="Print package names one per line instead of -p flags.",
    )
    args = parser.parse_args()

    pkgs = resolve(args.gear)

    if args.names:
        for p in pkgs:
            print(p)
    else:
        print(" ".join(f"-p {p}" for p in pkgs))

    return 0


if __name__ == "__main__":
    sys.exit(main())
