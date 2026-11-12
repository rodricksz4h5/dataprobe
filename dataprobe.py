"""dataprobe — exact, local structure and quality checks over large data files.

Three read-only tools for CSV/TSV and JSONL (NDJSON) files that are too big to
read into a model's context:

  * data_profile  — per-column / per-field type, null rate, distinct count,
                    numeric statistics and top values; for JSONL, the inferred
                    union schema with per-field presence.
  * data_validate — only the actionable problems: mixed-type columns (with the
                    offending line numbers), ragged rows, duplicate rows,
                    primary-key candidates that are not unique, constant / all-
                    empty columns, and malformed JSONL lines.
  * data_head     — a clean, *parsed* preview of the first N records (handles
                    quoted CSV and JSONL correctly, unlike a raw byte `head`).

Everything is streamed with bounded memory and bounded CPU, standard library
only (csv, json, statistics). No network, no credentials, no third-party deps.
Reads only the path you pass, and honours the same read denylist Hermes' core
read_file enforces, so credential stores and .env files are refused.
"""
from __future__ import annotations

import csv
import json
import math
import os
import stat
import statistics
from collections import Counter, OrderedDict
from typing import Any, Dict, List, Optional, Tuple

# --- Bounds (all surfaced in the result so a truncated run is never silent) ---
SCAN_ROW_CAP = 2_000_000       # stop after this many data rows / records
MAX_CELL_CHARS = 50_000        # truncate a single cell/value longer than this
MAX_DISTINCT = 10_000          # stop growing a column's distinct-value set here
MAX_NUMERIC_STORE = 500_000    # keep up to this many numbers per column for exact quantiles
MAX_COLUMNS = 4_096            # refuse pathological width
TOP_N_DEFAULT = 10
MAX_TYPE_EXAMPLES = 5          # example line numbers kept per minority-type flag
MAX_DUP_EXAMPLES = 5

# Field size for the csv module must allow our cell cap.
csv.field_size_limit(MAX_CELL_CHARS * 4)

_TRUE = {"true", "false"}  # case-insensitive bool tokens (strict)


# --- File safety / IO ---------------------------------------------------------
def _read_block_error(abs_path: str) -> Any:
    """Honour Hermes' read denylist when running inside the agent.

    Mirrors Core read_file: credential stores, internal caches and .env files
    are refused. Outside a Hermes runtime (standalone tests) the module is
    absent and the check no-ops, matching Core's best-effort posture.
    """
    try:
        from agent.file_safety import get_read_block_error
    except Exception:
        return None
    try:
        return get_read_block_error(abs_path)
    except Exception:
        return None


# Like Hermes' read_file, never read device, fd or process files: they can block
# forever (/dev/zero, a FIFO) or leak process state (/proc/<pid>/environ).
_REFUSED_ROOTS = ("/proc", "/dev")


def _under_refused_root(path: str) -> bool:
    """True if the path, or the file a symlink resolves to, is under /proc or /dev."""
    candidates = [os.path.abspath(path)]
    try:
        candidates.append(os.path.realpath(path))
    except (OSError, ValueError):
        pass
    return any(
        candidate == root or candidate.startswith(root + "/")
        for candidate in candidates
        for root in _REFUSED_ROOTS
    )


def _resolve(path: Any) -> str:
    if not isinstance(path, str) or not path.strip():
        raise ValueError("No file path provided")
    path = os.path.expanduser(path)
    blocked = _read_block_error(os.path.abspath(path))
    if blocked:
        raise ValueError(blocked)
    # Both checks run before anything is opened; stat() does not open the file.
    if _under_refused_root(path):
        raise ValueError(f"Refusing to read {path}: device and /proc paths are not data files")
    try:
        mode = os.stat(path).st_mode
    except FileNotFoundError:
        raise ValueError(f"File not found: {path}") from None
    except OSError as exc:
        raise ValueError(f"Cannot access {path}: {exc.strerror or exc}") from None
    if stat.S_ISDIR(mode):
        raise ValueError(f"Path is a directory, not a file: {path}")
    if not stat.S_ISREG(mode):
        raise ValueError(f"Refusing to read {path}: not a regular file")
    return path


def _err(message: str, **extra: Any) -> str:
    return json.dumps({"ok": False, "error": message, **extra})


def _truncate(value: str) -> str:
    return value if len(value) <= MAX_CELL_CHARS else value[:MAX_CELL_CHARS]


# --- Format detection ---------------------------------------------------------
def _sample_bytes(path: str, n: int = 65_536) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(n)


def _has_bom(raw: bytes) -> bool:
    return raw.startswith(b"\xef\xbb\xbf")


def _first_nonblank_line(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    for line in text.splitlines():
        if line.strip():
            return line
    return ""


def _detect_format(path: str, override: Optional[str]) -> Tuple[str, str]:
    """Return (format, delimiter). format in {csv, jsonl}. delimiter for csv."""
    ext = os.path.splitext(path)[1].lower()
    if override:
        fmt = override.lower()
        if fmt in ("ndjson", "jsonl"):
            return "jsonl", ""
        if fmt == "tsv":
            return "csv", "\t"
        if fmt == "csv":
            return "csv", ","
        raise ValueError(f"Unknown format override: {override!r} (use csv, tsv or jsonl)")

    raw = _sample_bytes(path)
    first = _first_nonblank_line(raw)
    # JSONL: extension, or the first non-blank line is a JSON object/array.
    if ext in (".jsonl", ".ndjson"):
        return "jsonl", ""
    if first[:1] in ("{", "["):
        try:
            json.loads(first)
            return "jsonl", ""
        except Exception:
            pass
    if ext == ".tsv":
        return "csv", "\t"
    # Sniff the delimiter from a text sample.
    try:
        sample = raw.decode("utf-8", errors="replace")
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|")
        return "csv", dialect.delimiter
    except Exception:
        return "csv", ","


# --- Value typing -------------------------------------------------------------
def _family(t: str) -> str:
    """Group int and float into one 'number' family; others stand alone.

    A column mixing ints and floats is numeric, not 'mixed type'.
    """
    return "number" if t in ("int", "float") else t


def _classify(value: str) -> str:
    """Classify a raw string cell into a coarse type token."""
    v = value.strip()
    if v == "":
        return "empty"
    low = v.lower()
    if low in _TRUE:
        return "bool"
    # int (no leading-zero restriction; allow sign)
    if _is_int(v):
        return "int"
    if _is_float(v):
        return "float"
    if _is_date(v):
        return "date"
    return "string"


def _is_int(v: str) -> bool:
    if v and v[0] in "+-":
        v = v[1:]
    return v.isdigit() and v != ""


def _is_float(v: str) -> bool:
    # Only finite numbers count as numeric; literal nan/inf fall through to
    # string so they never poison min/max/mean aggregates.
    try:
        f = float(v)
    except (ValueError, OverflowError):
        return False
    return math.isfinite(f)


def _is_date(v: str) -> bool:
    # Cheap, allocation-free shape checks for the common ISO forms only.
    if len(v) < 8:
        return False
    if len(v) >= 10 and v[4] == "-" and v[7] == "-" \
            and v[:4].isdigit() and v[5:7].isdigit() and v[8:10].isdigit():
        return True
    return False


# --- Per-column accumulator ---------------------------------------------------
class _Column:
    __slots__ = ("name", "type_counts", "type_lines", "null", "total", "distinct",
                 "distinct_capped", "top", "nums", "nums_capped", "num_min", "num_max",
                 "num_sum", "num_sumsq", "num_count")

    def __init__(self, name: str):
        self.name = name
        self.type_counts: Counter = Counter()
        self.type_lines: Dict[str, List[int]] = {}   # per raw type -> example line numbers
        self.null = 0
        self.total = 0
        self.distinct: set = set()
        self.distinct_capped = False
        self.top: Counter = Counter()
        self.nums: List[float] = []
        self.nums_capped = False
        self.num_min: Optional[float] = None
        self.num_max: Optional[float] = None
        self.num_sum = 0.0
        self.num_sumsq = 0.0
        self.num_count = 0

    def add(self, value: str, lineno: int) -> None:
        self.total += 1
        t = _classify(value)
        self.type_counts[t] += 1
        if t == "empty":
            self.null += 1
            return
        bucket = self.type_lines.get(t)
        if bucket is None:
            self.type_lines[t] = [lineno]
        elif len(bucket) < MAX_TYPE_EXAMPLES:
            bucket.append(lineno)
        v = value.strip()
        if not self.distinct_capped:
            self.distinct.add(v)
            if len(self.distinct) >= MAX_DISTINCT:
                self.distinct_capped = True
        if len(self.top) < MAX_DISTINCT or v in self.top:
            self.top[v] += 1
        if t in ("int", "float"):
            try:
                f = float(v)
            except (ValueError, OverflowError):
                return
            self.num_count += 1
            self.num_sum += f
            self.num_sumsq += f * f
            self.num_min = f if self.num_min is None else min(self.num_min, f)
            self.num_max = f if self.num_max is None else max(self.num_max, f)
            if not self.nums_capped:
                self.nums.append(f)
                if len(self.nums) >= MAX_NUMERIC_STORE:
                    self.nums_capped = True

    def _family_counts(self) -> Counter:
        fam: Counter = Counter()
        for t, c in self.type_counts.items():
            if t == "empty":
                continue
            fam[_family(t)] += c
        return fam

    def _dominant_family(self) -> str:
        fam = self._family_counts()
        if not fam:
            return "empty"
        # argmax by count, deterministic tie-break by family name
        return sorted(fam.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]

    def summary(self, top_n: int) -> Dict[str, Any]:
        dom_family = self._dominant_family()
        # minority = non-empty values whose family differs from the dominant family
        minority: Dict[str, List[int]] = {}
        for t, lines in self.type_lines.items():
            if _family(t) != dom_family:
                minority[t] = lines
        out: Dict[str, Any] = {
            "name": self.name,
            "inferred_type": dom_family,
            "count": self.total,
            "null_count": self.null,
            "null_pct": round(100.0 * self.null / self.total, 2) if self.total else 0.0,
            "distinct": len(self.distinct),
            "distinct_capped": self.distinct_capped,
            "type_breakdown": dict(self.type_counts),
            "top_values": [{"value": v, "count": c} for v, c in self.top.most_common(top_n)],
        }
        if minority:
            out["minority_type_lines"] = minority
        if self.num_count:
            mean = self.num_sum / self.num_count
            stats: Dict[str, Any] = {
                "min": self.num_min,
                "max": self.num_max,
                "mean": round(mean, 6),
            }
            if self.num_count > 1:
                var = max(0.0, (self.num_sumsq - self.num_count * mean * mean) / (self.num_count - 1))
                stats["stdev"] = round(math.sqrt(var), 6)
            if self.nums and not self.nums_capped:
                srt = sorted(self.nums)
                stats["median"] = statistics.median(srt)
                if len(srt) >= 4:
                    stats["p25"] = srt[len(srt) // 4]
                    stats["p75"] = srt[(len(srt) * 3) // 4]
            elif self.nums_capped:
                stats["quantiles"] = "omitted (numeric sample capped)"
            out["numeric"] = stats
        return out


# --- CSV path -----------------------------------------------------------------
def _looks_like_header(row: List[str]) -> bool:
    if not row:
        return False
    for cell in row:
        c = cell.strip()
        if c == "" or _is_int(c) or _is_float(c):
            return False
    return True


def _profile_csv(path: str, delimiter: str, top_n: int, scan_cap: int) -> Dict[str, Any]:
    notes: List[str] = []
    raw = _sample_bytes(path, 4096)
    if _has_bom(raw):
        notes.append("UTF-8 BOM present at start of file")

    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        try:
            header = next(reader)
        except StopIteration:
            # Same shape as a non-empty result so callers (data_validate) never KeyError.
            return {
                "ok": True,
                "format": "csv",
                "delimiter": "\\t" if delimiter == "\t" else delimiter,
                "has_header": False,
                "rows": 0,
                "column_count": 0,
                "duplicate_rows": 0,
                "duplicate_row_examples": [],
                "ragged_rows": [],
                "columns": [],
                "notes": [*notes, "empty file"],
            }
        has_header = _looks_like_header(header)
        if has_header:
            names = [h.strip() or f"col{i}" for i, h in enumerate(header)]
            first_data_row = None
            first_lineno = 2
        else:
            names = [f"col{i}" for i in range(len(header))]
            first_data_row = header
            first_lineno = 1

        if len(names) > MAX_COLUMNS:
            raise ValueError(f"Too many columns: {len(names)} (cap {MAX_COLUMNS})")

        cols = [_Column(n) for n in names]
        n_cols = len(names)
        rows = 0
        ragged = []           # (lineno, actual_cols)
        row_hashes: set = set()
        dup_rows = 0
        dup_examples: List[int] = []
        hash_capped = False

        def handle_row(values: List[str], lineno: int) -> None:
            nonlocal rows, dup_rows, hash_capped
            rows += 1
            if len(values) != n_cols:
                if len(ragged) < 50:
                    ragged.append({"line": lineno, "fields": len(values)})
            # duplicate detection via a bounded hash set
            if not hash_capped:
                key = hash(tuple(values))
                if key in row_hashes:
                    dup_rows += 1
                    if len(dup_examples) < MAX_DUP_EXAMPLES:
                        dup_examples.append(lineno)
                else:
                    row_hashes.add(key)
                    if len(row_hashes) >= SCAN_ROW_CAP:
                        hash_capped = True
            for i in range(n_cols):
                val = values[i] if i < len(values) else ""
                val = _truncate(val)
                cols[i].add(val, lineno)

        if first_data_row is not None:
            handle_row(first_data_row, first_lineno)

        truncated = False
        for lineno, values in enumerate(reader, start=first_lineno + (1 if first_data_row is not None else 0)):
            if rows >= scan_cap:
                truncated = True
                break
            handle_row(values, lineno)

    if truncated:
        notes.append(f"scan stopped at row cap {scan_cap}")
    if hash_capped:
        notes.append("duplicate-row detection stopped (row cap reached)")

    return {
        "ok": True,
        "format": "csv",
        "delimiter": "\\t" if delimiter == "\t" else delimiter,
        "has_header": has_header,
        "rows": rows,
        "column_count": n_cols,
        "duplicate_rows": dup_rows,
        "duplicate_row_examples": dup_examples,
        "ragged_rows": ragged,
        "columns": [c.summary(top_n) for c in cols],
        "notes": notes,
    }


# --- JSONL path ---------------------------------------------------------------
def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "unknown"


def _profile_jsonl(path: str, top_n: int, scan_cap: int) -> Dict[str, Any]:
    notes: List[str] = []
    fields: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
    records = 0
    parse_errors: List[Dict[str, Any]] = []
    non_object = 0
    non_object_examples: List[int] = []
    truncated = False

    with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
        for lineno, line in enumerate(fh, start=1):
            if not line.strip():
                continue
            if records >= scan_cap:
                truncated = True
                break
            try:
                obj = json.loads(line)
            except Exception as exc:
                if len(parse_errors) < 50:
                    parse_errors.append({"line": lineno, "error": str(exc)[:200]})
                continue
            records += 1
            if not isinstance(obj, dict):
                non_object += 1
                if len(non_object_examples) < MAX_TYPE_EXAMPLES:
                    non_object_examples.append(lineno)
                continue
            for key, val in obj.items():
                f = fields.get(key)
                if f is None:
                    f = {"present": 0, "types": Counter(), "examples": []}
                    fields[key] = f
                f["present"] += 1
                f["types"][_json_type(val)] += 1
                if len(f["examples"]) < 3 and not isinstance(val, (dict, list)):
                    f["examples"].append(val)

    if truncated:
        notes.append(f"scan stopped at record cap {scan_cap}")

    field_summary = []
    for name, f in fields.items():
        present = f["present"]
        field_summary.append({
            "field": name,
            "present": present,
            "presence_pct": round(100.0 * present / records, 2) if records else 0.0,
            "types": dict(f["types"]),
            "optional": present < records,
            "examples": f["examples"],
        })
    # Stable, useful ordering: most-present first.
    field_summary.sort(key=lambda x: (-x["present"], x["field"]))

    return {
        "ok": True,
        "format": "jsonl",
        "records": records,
        "parse_errors": parse_errors,
        "parse_error_count": len(parse_errors),
        "non_object_records": non_object,
        "non_object_examples": non_object_examples,
        "distinct_fields": len(fields),
        "fields": field_summary,
        "notes": notes,
    }


# --- Schemas ------------------------------------------------------------------
_PATH = {"type": "string", "description": "Path to a local CSV/TSV or JSONL file."}
_FORMAT = {"type": "string", "description": "Override format detection: csv, tsv or jsonl."}

DATA_PROFILE = {
    "name": "data_profile",
    "description": (
        "Profile a local CSV/TSV or JSONL data file that is too big to read. For "
        "CSV/TSV: per-column inferred type, null rate, distinct count, numeric "
        "statistics (min/max/mean/stdev/quartiles) and top values, plus row count, "
        "duplicate rows and ragged rows. For JSONL: the inferred union schema with "
        "per-field presence %, type distribution and example values. Exact, computed "
        "from the whole file, standard library only."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "path": _PATH,
            "format": _FORMAT,
            "top_values": {"type": "integer", "description": f"Top-N values per column (default {TOP_N_DEFAULT}, cap 100)."},
            "max_rows": {"type": "integer", "description": f"Scan at most this many rows (default and cap {SCAN_ROW_CAP})."},
        },
        "required": ["path"],
    },
}

DATA_VALIDATE = {
    "name": "data_validate",
    "description": (
        "Report only the actionable data-quality problems in a local CSV/TSV or "
        "JSONL file: columns whose values are mostly one type with a few of another "
        "(with the offending line numbers), rows with the wrong number of fields, "
        "duplicate rows, candidate key columns that are not unique, constant or all-"
        "empty columns, and malformed JSONL lines. Use this to decide whether a data "
        "file is clean before the agent relies on it."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "path": _PATH,
            "format": _FORMAT,
            "key": {"type": "string", "description": "Column/field name to check for uniqueness (primary-key candidate)."},
            "max_rows": {"type": "integer", "description": f"Scan at most this many rows (default and cap {SCAN_ROW_CAP})."},
        },
        "required": ["path"],
    },
}

DATA_HEAD = {
    "name": "data_head",
    "description": (
        "Return the first N *parsed* records of a local CSV/TSV or JSONL file as "
        "clean structured rows — correctly handling quoted CSV fields and JSON "
        "objects, which a raw byte preview mangles. Optionally restrict to named "
        "columns/fields."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "path": _PATH,
            "format": _FORMAT,
            "n": {"type": "integer", "description": "Number of records to return (default 10, cap 1000)."},
            "columns": {"type": "array", "items": {"type": "string"}, "description": "Only include these columns/fields."},
        },
        "required": ["path"],
    },
}


# --- Handlers -----------------------------------------------------------------
def _clamp(value: Any, default: int, lo: int, hi: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return max(lo, min(hi, n))


def data_profile(args: Dict[str, Any], **_: Any) -> str:
    try:
        path = _resolve(args.get("path"))
    except ValueError as exc:
        return _err(str(exc))
    top_n = _clamp(args.get("top_values"), TOP_N_DEFAULT, 1, 100)
    scan_cap = _clamp(args.get("max_rows"), SCAN_ROW_CAP, 1, SCAN_ROW_CAP)
    try:
        fmt, delim = _detect_format(path, args.get("format"))
    except ValueError as exc:
        return _err(str(exc))
    try:
        if fmt == "jsonl":
            result = _profile_jsonl(path, top_n, scan_cap)
        else:
            result = _profile_csv(path, delim, top_n, scan_cap)
    except ValueError as exc:
        return _err(str(exc))
    except OSError as exc:
        return _err(f"Could not read file: {exc}")
    except csv.Error as exc:
        return _err(f"Could not parse CSV: {exc}")
    result["file"] = path
    return json.dumps(result)


def data_validate(args: Dict[str, Any], **_: Any) -> str:
    try:
        path = _resolve(args.get("path"))
    except ValueError as exc:
        return _err(str(exc))
    scan_cap = _clamp(args.get("max_rows"), SCAN_ROW_CAP, 1, SCAN_ROW_CAP)
    key = args.get("key")
    try:
        fmt, delim = _detect_format(path, args.get("format"))
    except ValueError as exc:
        return _err(str(exc))

    issues: List[Dict[str, Any]] = []
    try:
        if fmt == "jsonl":
            prof = _profile_jsonl(path, TOP_N_DEFAULT, scan_cap)
            records = prof["records"]
            if prof["parse_error_count"]:
                issues.append({"kind": "malformed_json_lines", "count": prof["parse_error_count"],
                               "examples": prof["parse_errors"][:MAX_TYPE_EXAMPLES]})
            if prof["non_object_records"]:
                issues.append({"kind": "non_object_records", "count": prof["non_object_records"],
                               "example_lines": prof["non_object_examples"]})
            for f in prof["fields"]:
                # Families among non-null JSON types; null is absence, not a type clash.
                fams = {_family(t) for t in f["types"] if t != "null"}
                if len(fams) > 1:
                    issues.append({"kind": "mixed_field_type", "field": f["field"], "types": f["types"]})
                if f["optional"] and f["presence_pct"] < 100.0:
                    issues.append({"kind": "optional_field", "field": f["field"],
                                   "presence_pct": f["presence_pct"]})
            summary = {"format": "jsonl", "records": records, "fields": prof["distinct_fields"]}
        else:
            prof = _profile_csv(path, delim, TOP_N_DEFAULT, scan_cap)
            rows = prof["rows"]
            if prof["ragged_rows"]:
                issues.append({"kind": "ragged_rows", "count": len(prof["ragged_rows"]),
                               "expected_fields": prof["column_count"],
                               "examples": prof["ragged_rows"][:MAX_TYPE_EXAMPLES]})
            if prof["duplicate_rows"]:
                issues.append({"kind": "duplicate_rows", "count": prof["duplicate_rows"],
                               "example_lines": prof["duplicate_row_examples"]})
            for col in prof["columns"]:
                if col.get("minority_type_lines"):
                    issues.append({"kind": "mixed_column_type", "column": col["name"],
                                   "type_breakdown": col["type_breakdown"],
                                   "minority_type_lines": col["minority_type_lines"]})
                if col["null_count"] == col["count"] and col["count"] > 0:
                    issues.append({"kind": "all_empty_column", "column": col["name"]})
                elif col["distinct"] == 1 and not col["distinct_capped"]:
                    issues.append({"kind": "constant_column", "column": col["name"],
                                   "value": col["top_values"][0]["value"] if col["top_values"] else None})
            summary = {"format": "csv", "rows": rows, "columns": prof["column_count"]}
    except ValueError as exc:
        return _err(str(exc))
    except OSError as exc:
        return _err(f"Could not read file: {exc}")
    except csv.Error as exc:
        return _err(f"Could not parse CSV: {exc}")

    # Optional key-uniqueness check (second pass, bounded).
    if key:
        ok, detail = _check_key_unique(path, fmt, delim, key, scan_cap)
        if ok is False:
            issues.append(detail)

    return json.dumps({"ok": True, "file": path, "summary": summary,
                       "issue_count": len(issues), "issues": issues})


def _check_key_unique(path: str, fmt: str, delim: str, key: str, scan_cap: int) -> Tuple[Optional[bool], Dict[str, Any]]:
    seen: set = set()
    dups: List[Any] = []
    missing = 0
    rows = 0
    try:
        if fmt == "jsonl":
            with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
                for line in fh:
                    if not line.strip() or rows >= scan_cap:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    rows += 1
                    if not isinstance(obj, dict) or key not in obj:
                        missing += 1
                        continue
                    v = obj[key]
                    if not isinstance(v, (str, int, float, bool)):
                        v = json.dumps(v)
                    if v in seen:
                        if len(dups) < MAX_DUP_EXAMPLES:
                            dups.append(v)
                    else:
                        seen.add(v)
        else:
            with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
                reader = csv.reader(fh, delimiter=delim)
                try:
                    header = next(reader)
                except StopIteration:
                    return None, {}
                header = [h.strip() for h in header]
                if key not in header:
                    return False, {"kind": "key_not_found", "key": key, "available": header}
                idx = header.index(key)
                for values in reader:
                    if rows >= scan_cap:
                        break
                    rows += 1
                    v = values[idx] if idx < len(values) else ""
                    if v == "":
                        missing += 1
                        continue
                    if v in seen:
                        if len(dups) < MAX_DUP_EXAMPLES:
                            dups.append(v)
                    else:
                        seen.add(v)
    except (OSError, csv.Error):
        return None, {}
    if dups or missing:
        return False, {"kind": "key_not_unique", "key": key,
                       "duplicate_values": dups, "missing_or_empty": missing}
    return True, {}


def data_head(args: Dict[str, Any], **_: Any) -> str:
    try:
        path = _resolve(args.get("path"))
    except ValueError as exc:
        return _err(str(exc))
    n = _clamp(args.get("n"), 10, 1, 1000)
    cols_filter = args.get("columns")
    if cols_filter is not None and not isinstance(cols_filter, list):
        return _err("`columns` must be a list of names")
    try:
        fmt, delim = _detect_format(path, args.get("format"))
    except ValueError as exc:
        return _err(str(exc))

    out: List[Any] = []
    try:
        if fmt == "jsonl":
            with open(path, "r", encoding="utf-8-sig", errors="replace") as fh:
                for line in fh:
                    if len(out) >= n:
                        break
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    if isinstance(obj, dict) and cols_filter:
                        obj = {k: obj.get(k) for k in cols_filter}
                    out.append(obj)
            return json.dumps({"ok": True, "file": path, "format": "jsonl", "records": out})
        else:
            with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
                reader = csv.reader(fh, delimiter=delim)
                try:
                    header = next(reader)
                except StopIteration:
                    return json.dumps({"ok": True, "file": path, "format": "csv", "rows": []})
                has_header = _looks_like_header(header)
                names = [h.strip() or f"col{i}" for i, h in enumerate(header)] if has_header \
                    else [f"col{i}" for i in range(len(header))]
                rows: List[Dict[str, Any]] = []
                if not has_header:
                    rows.append(_row_to_dict(names, header, cols_filter))
                for values in reader:
                    if len(rows) >= n:
                        break
                    rows.append(_row_to_dict(names, values, cols_filter))
            return json.dumps({"ok": True, "file": path, "format": "csv",
                               "has_header": has_header, "columns": names, "rows": rows})
    except OSError as exc:
        return _err(f"Could not read file: {exc}")
    except csv.Error as exc:
        return _err(f"Could not parse CSV: {exc}")


def _row_to_dict(names: List[str], values: List[str], cols_filter: Optional[List[str]]) -> Dict[str, Any]:
    d = OrderedDict()
    for i, name in enumerate(names):
        d[name] = values[i] if i < len(values) else None
    if cols_filter:
        d = OrderedDict((k, d.get(k)) for k in cols_filter)
    return d
