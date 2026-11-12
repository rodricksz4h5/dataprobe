# dataprobe

Exact, local structure and quality checks over large data files, as three Hermes
tools. A language model can't read a 2 GB CSV or a million-line JSONL dataset, and
asked "is this data clean?" it will guess from the handful of rows it can see.
dataprobe streams the file on disk and returns **ground truth**: the real schema,
per-column statistics, and the specific rows that are wrong.

## Why a tool and not a prompt or skill

The answers are *computed over the whole file* — the column that is 99% integer
with 143 stray strings on lines you can name, the duplicate primary keys, the
JSONL field that is present in only 82% of records. None of that is visible in a
sampled preview, and a skill that just instructs the model cannot produce it
reliably. It is also dependency-free: unlike pandas-based tooling it pulls in
nothing and downloads nothing.

## Tools

### `data_profile`
Full picture of a CSV/TSV or JSONL file.

| arg | type | required | default | meaning |
|---|---|---|---|---|
| `path` | string | yes | — | local data file |
| `format` | string | no | auto | force `csv` \| `tsv` \| `jsonl` |
| `top_values` | integer | no | `10` | top-N values per column (cap 100) |
| `max_rows` | integer | no | `2000000` | scan at most this many rows |

**CSV/TSV** returns row count, column count, duplicate rows, ragged rows, and for
each column: inferred type, null count/percent, distinct count, a type breakdown,
top values, numeric `min/max/mean/stdev/median/p25/p75`, and
`minority_type_lines` — the line numbers of values that don't match the column's
dominant type.

**JSONL** returns the record count, parse errors with line numbers, and the
inferred **union schema**: every field with its presence percentage, type
distribution, whether it is optional, and example values.

### `data_validate`
Only the actionable problems — use it to gate whether the agent should trust a file.

| arg | type | required | default | meaning |
|---|---|---|---|---|
| `path` | string | yes | — | local data file |
| `format` | string | no | auto | force `csv` \| `tsv` \| `jsonl` |
| `key` | string | no | — | column/field to check for uniqueness |
| `max_rows` | integer | no | `2000000` | scan at most this many rows |

Flags: `mixed_column_type` / `mixed_field_type` (with offending line numbers),
`ragged_rows`, `duplicate_rows`, `constant_column`, `all_empty_column`,
`optional_field`, `malformed_json_lines`, `non_object_records`, and — when `key`
is given — `key_not_unique` / `key_not_found`.

### `data_head`
First N **parsed** records as clean structured rows, correctly handling quoted
CSV fields and JSON objects (a raw byte `head` mangles both).

| arg | type | required | default | meaning |
|---|---|---|---|---|
| `path` | string | yes | — | local data file |
| `format` | string | no | auto | force `csv` \| `tsv` \| `jsonl` |
| `n` | integer | no | `10` | records to return (cap 1000) |
| `columns` | array | no | — | only include these columns/fields |

All three always return a JSON string; a bad path, blocked path or malformed
input returns `{ "ok": false, "error": "..." }` rather than raising.

## Format detection

`.jsonl` / `.ndjson`, or a first non-blank line that parses as a JSON object, is
treated as JSONL. `.tsv` is tab-delimited. Otherwise the CSV delimiter is sniffed
from a sample (`, \t ; |`). A UTF-8 BOM is handled and noted. Pass `format` to
override.

## Safety and disclosure

- **Reads only the file path you pass**, read-only. Never writes, deletes, or
  touches any other path.
- **Honours Hermes' read denylist.** Inside the agent, every path is passed
  through the same check Core's `read_file` uses, so credential stores, internal
  caches and `.env` files are refused — dataprobe can't be used to read what the
  host protects.
- **No network, no credentials, no telemetry, no background processes, no
  self-update. Standard library only** (`csv`, `json`, `statistics`).
- A single call stops after 2,000,000 rows, truncates cells over 50,000 chars,
  and caps per-column distinct tracking at 10,000 — all flagged in the result —
  so runtime and memory stay bounded.

## Install

```bash
hermes plugins install rodricksz4h5/dataprobe
```

## License

MIT — see [LICENSE](./LICENSE).
