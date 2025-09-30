#!/usr/bin/env python3

"""
csv_to_sqlite.py

Safely import a CSV file into an SQLite database.

Usage:
  python csv_to_sqlite.py OUTPUT_DB.sqlite INPUT.csv

Behavior:
  - Sanitizes column names to safe identifiers (no spaces or quotes).
  - Infers SQLite column types (INTEGER, REAL, TEXT) from the data.
  - Creates a table named after the CSV filename (sanitized).
  - Inserts rows using parameterized queries to prevent SQL injection.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import sqlite3
import sys
from typing import Iterable, List, Optional, Sequence, Tuple


SQLITE_RESERVED_KEYWORDS = {
    # Common SQL/SQLite keywords
    "abort", "action", "add", "after", "all", "alter", "analyze", "and", "as", "asc",
    "attach", "autoincrement", "before", "begin", "between", "by", "cascade", "case",
    "cast", "check", "collate", "column", "commit", "conflict", "constraint", "create",
    "cross", "current_date", "current_time", "current_timestamp", "database", "default",
    "deferrable", "deferred", "delete", "desc", "detach", "distinct", "drop", "each",
    "else", "end", "escape", "except", "exclusive", "exists", "explain", "fail", "for",
    "foreign", "from", "full", "globa", "group", "having", "if", "ignore", "immediate",
    "in", "index", "indexed", "initially", "inner", "insert", "instead", "intersect",
    "into", "is", "isnull", "join", "key", "left", "like", "limit", "match", "natural",
    "no", "not", "notnull", "null", "of", "offset", "on", "or", "order", "outer",
    "plan", "pragma", "primary", "query", "raise", "recursive", "references", "regexp",
    "reindex", "release", "rename", "replace", "restrict", "right", "rollback", "row",
    "savepoint", "select", "set", "table", "temporary", "then", "to", "transaction",
    "trigger", "union", "unique", "update", "using", "vacuum", "values", "view", "virtual",
    "when", "where", "without",
    # Common type names to avoid conflicts as identifiers
    "integer", "real", "text", "blob"
}


IDENTIFIER_CLEAN_RE = re.compile(r"[^A-Za-z0-9_]+")


def sanitize_identifier(raw: Optional[str], fallback_prefix: str, used: Optional[set] = None) -> str:
    """Return a safe SQL identifier: lowercase, alnum+underscore, starts with letter/underscore.

    Ensures no spaces or quotes are needed and avoids reserved keywords. Guarantees uniqueness
    if a "used" set is provided.
    """
    text = (raw or "").replace("\ufeff", "").strip()
    lowered = text.lower()
    cleaned = IDENTIFIER_CLEAN_RE.sub("_", lowered).strip("_")
    cleaned = re.sub(r"_+", "_", cleaned)

    if not cleaned:
        cleaned = fallback_prefix

    if cleaned[0].isdigit():
        cleaned = f"{fallback_prefix}_{cleaned}"

    if cleaned in SQLITE_RESERVED_KEYWORDS:
        cleaned = f"{cleaned}_"

    if used is not None:
        base = cleaned
        suffix = 1
        while cleaned in used:
            cleaned = f"{base}_{suffix}"
            suffix += 1
        used.add(cleaned)

    return cleaned


def sanitize_headers(headers: Sequence[str]) -> List[str]:
    """Sanitize a list of header names into valid, unique SQL identifiers."""
    used: set = set()
    sanitized: List[str] = []
    for index, raw in enumerate(headers):
        name = sanitize_identifier(raw, fallback_prefix=f"col_{index+1}", used=used)
        sanitized.append(name)
    return sanitized


NULL_TOKENS = {"", "na", "n/a", "null", "none"}


def parse_null(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    v = value.strip()
    return None if v.lower() in NULL_TOKENS else v


INT_RE = re.compile(r"^[+-]?\d+$")


def is_int(value: str) -> bool:
    return bool(INT_RE.match(value))


def is_float(value: str) -> bool:
    try:
        number = float(value)
    except ValueError:
        return False
    return math.isfinite(number) and not is_int(value)


def observed_cell_type(value: Optional[str]) -> Optional[str]:
    """Return the SQLite type implied by a single cell's value: INTEGER, REAL, TEXT, or None for null."""
    v = parse_null(value)
    if v is None:
        return None
    if is_int(v):
        return "INTEGER"
    if is_float(v):
        return "REAL"
    return "TEXT"


def upgrade_type(current: str, observed: Optional[str]) -> str:
    """Upgrade the current type based on an observed cell type, preserving the least-permissive type.

    Precedence (least to most permissive): INTEGER < REAL < TEXT
    """
    if observed is None:
        return current
    if current == observed:
        return current
    order = {"INTEGER": 0, "REAL": 1, "TEXT": 2}
    return current if order[current] >= order[observed] else observed


def infer_column_types(csv_path: str, num_columns: int) -> Tuple[List[str], int]:
    """Scan the CSV to infer per-column SQLite types. Returns (types, row_count)."""
    types: List[str] = ["INTEGER"] * num_columns
    rows_count = 0
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # skip header row; already read separately
        except StopIteration:
            return types, 0

        for row in reader:
            rows_count += 1
            # Normalize row length to header length
            if len(row) < num_columns:
                row = row + [""] * (num_columns - len(row))
            elif len(row) > num_columns:
                row = row[:num_columns]

            for i, cell in enumerate(row):
                obs = observed_cell_type(cell)
                types[i] = upgrade_type(types[i], obs)

    return types, rows_count


def convert_value(value: Optional[str], target_type: str):
    """Convert a CSV string value into a Python value compatible with the target SQLite type."""
    v = parse_null(value)
    if v is None:
        return None
    if target_type == "INTEGER":
        try:
            return int(v)
        except ValueError:
            return None
    if target_type == "REAL":
        try:
            number = float(v)
            return number if math.isfinite(number) else None
        except ValueError:
            return None
    return v


def build_create_table_sql(table: str, columns: Sequence[Tuple[str, str]]) -> str:
    column_defs = ", ".join(f"{name} {stype}" for name, stype in columns)
    return f"CREATE TABLE {table} ({column_defs})"


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Import a CSV into an SQLite database safely.")
    parser.add_argument("output_db", help="Path to output SQLite database file")
    parser.add_argument("input_csv", help="Path to input CSV file")
    args = parser.parse_args(argv)

    output_db = args.output_db
    input_csv = args.input_csv

    if not os.path.isfile(input_csv):
        print(f"Error: CSV file not found: {input_csv}", file=sys.stderr)
        return 1

    # Read header row and sanitize column names
    try:
        with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                raw_headers = next(reader)
            except StopIteration:
                print("Error: CSV appears to be empty (no header row).", file=sys.stderr)
                return 1

    except OSError as e:
        print(f"Error: Failed to open CSV file: {e}", file=sys.stderr)
        return 1
    except csv.Error as e:
        print(f"Error: Failed to parse CSV header: {e}", file=sys.stderr)
        return 1

    if not raw_headers or all(h is None or str(h).strip() == "" for h in raw_headers):
        print("Error: CSV header row is missing or contains only empty column names.", file=sys.stderr)
        return 1

    sanitized_headers = sanitize_headers([str(h) for h in raw_headers])
    num_columns = len(sanitized_headers)

    # Infer types by scanning the CSV (excluding header)
    try:
        inferred_types, row_count = infer_column_types(input_csv, num_columns)
    except csv.Error as e:
        print(f"Error: CSV parsing error during type inference: {e}", file=sys.stderr)
        return 1

    # Determine table name from CSV file name
    csv_basename = os.path.splitext(os.path.basename(input_csv))[0]
    table_name = sanitize_identifier(csv_basename, fallback_prefix="data")

    # Create database, drop existing table of the same name, create new table
    try:
        conn = sqlite3.connect(output_db)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")

        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_sql = build_create_table_sql(table_name, list(zip(sanitized_headers, inferred_types)))
        conn.execute(create_sql)

        # Prepare parametrized insert statement to prevent SQL injection
        placeholders = ", ".join(["?"] * num_columns)
        columns_sql = ", ".join(sanitized_headers)
        insert_sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES ({placeholders})"

        # Insert rows in batches
        batch: List[Tuple] = []
        batch_size = 1000
        inserted_rows = 0

        conn.execute("BEGIN")
        with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header
            except StopIteration:
                pass

            for row in reader:
                if len(row) < num_columns:
                    row = row + [""] * (num_columns - len(row))
                elif len(row) > num_columns:
                    row = row[:num_columns]

                converted = tuple(
                    convert_value(row[i], inferred_types[i]) for i in range(num_columns)
                )
                batch.append(converted)

                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    inserted_rows += len(batch)
                    batch.clear()

            if batch:
                conn.executemany(insert_sql, batch)
                inserted_rows += len(batch)
                batch.clear()

        conn.commit()

    except sqlite3.Error as e:
        print(f"Error: SQLite operation failed: {e}", file=sys.stderr)
        try:
            conn.rollback()
        except Exception:
            pass
        return 1
    except OSError as e:
        print(f"Error: File I/O failure during insertion: {e}", file=sys.stderr)
        try:
            conn.rollback()
        except Exception:
            pass
        return 1
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(
        f"Imported {inserted_rows} row(s) into '{output_db}' table '{table_name}' with {num_columns} column(s)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


