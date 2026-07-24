#!/usr/bin/env python3
"""
Generate a PostgreSQL schema (DDL) that mirrors the MongoDB `mailbox` database
as Auger exposes it. It introspects the LIVE inferred schema through Superset's
SQL Lab API — nothing is guessed here, the column set and types come straight
from Auger (which already speaks the PostgreSQL wire protocol).

    docker exec \
        -e SUPERSET_URL=http://localhost:8088 \
        -e SUPERSET_USER=admin \
        -e SUPERSET_PASS='your-admin-password' \
        -e DB_NAME='Auger-Mailbox' \
        -i superset_app python - < generate_pg_schema.py > schema.sql

Types are mapped Arrow/Mongo -> Postgres. `_id` and `*Id`/`*By` columns are
resolved to uuid or char(24) (ObjectId) by inspecting a real sample value, and
known references are annotated with `-- FK ->` comments (not enforced
constraints: a Mongo dump does not guarantee referential integrity).
"""

import os
import re
import sys
import requests

URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.environ.get("SUPERSET_USER")
PASS = os.environ.get("SUPERSET_PASS")
DB_NAME = os.environ.get("DB_NAME", "Auger-Mailbox")
SCHEMA = os.environ.get("SCHEMA", "mailbox")

if not USER or not PASS:
    sys.exit("set SUPERSET_USER and SUPERSET_PASS in the environment")

# Fallback list (from Compass) if information_schema is not served by Auger.
FALLBACK_TABLES = [
    "contact_book", "contact_book_members", "email_message",
    "email_message_credential", "email_message_recipient",
    "email_message_recipient_read", "email_message_sender",
    "email_notification", "profile", "recently_recipient", "tags", "user",
]

# Columns whose target is known, annotated as comments.
FK = {
    "emailMessageId": "email_message(_id)",
    "emailMessageRecipientId": "email_message_recipient(_id)",
    "senderId": "profile(_id)",
    "recipientId": "profile(_id)",
    "userId": "user(_id)",
    "createdBy": "user(_id)",
    "emailProfileId": "profile(_id)",
}

UUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
                     r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
OID_RE = re.compile(r"^[0-9a-fA-F]{24}$")

s = requests.Session()


def login():
    tok = s.post(f"{URL}/api/v1/security/login",
                 json={"username": USER, "password": PASS,
                       "provider": "db", "refresh": True})
    if tok.status_code != 200:
        sys.exit(f"login failed: {tok.status_code} {tok.text[:200]}")
    s.headers["Authorization"] = f"Bearer {tok.json()['access_token']}"
    s.headers["X-CSRFToken"] = s.get(f"{URL}/api/v1/security/csrf_token/").json()["result"]
    s.headers["Referer"] = URL


def db_id():
    for r in s.get(f"{URL}/api/v1/database/", params={"q": "(page_size:200)"}).json()["result"]:
        if r["database_name"] == DB_NAME:
            return r["id"]
    sys.exit(f"database '{DB_NAME}' not found")


def run_sql(dbid, sql):
    r = s.post(f"{URL}/api/v1/sqllab/execute/",
               json={"database_id": dbid, "schema": SCHEMA, "sql": sql,
                     "runAsync": False, "queryLimit": 50})
    if r.status_code != 200:
        return None, f"{r.status_code}: {r.text[:200]}"
    return r.json(), None


def list_tables(dbid):
    j, err = run_sql(
        dbid,
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{SCHEMA}' ORDER BY table_name")
    if j and j.get("data"):
        names = [row.get("table_name") for row in j["data"] if row.get("table_name")]
        if names:
            return names
    sys.stderr.write("  (information_schema unavailable; using fallback table list)\n")
    return FALLBACK_TABLES


def quote(ident):
    return '"' + ident.replace('"', '""') + '"'


def pg_type(name, type_str, sample):
    t = (type_str or "").upper()
    is_id = name == "_id" or name.endswith("Id") or name.endswith("By")

    if isinstance(sample, list):
        return "jsonb"
    if isinstance(sample, dict):
        return "jsonb"
    if isinstance(sample, bool):
        return "boolean"

    if is_id and isinstance(sample, str):
        if UUID_RE.match(sample):
            return "uuid"
        if OID_RE.match(sample):
            return "char(24)"

    if "BOOL" in t:
        return "boolean"
    if "INT" in t:
        return "bigint"
    if any(k in t for k in ("DOUBLE", "FLOAT", "REAL", "NUMERIC", "DECIMAL")):
        return "double precision"
    if any(k in t for k in ("TIMESTAMP", "DATETIME", "DATE", "TIME")):
        return "timestamptz"

    # A JSON object/array serialised as a string (e.g. firstName, primaryName).
    if isinstance(sample, str) and sample[:1] in ("{", "["):
        return "jsonb"

    return "text"


def first_sample(rows, col):
    for row in rows:
        v = row.get(col)
        if v is not None:
            return v
    return None


def emit_table(dbid, table):
    j, err = run_sql(dbid, f'SELECT * FROM {SCHEMA}.{quote(table)} LIMIT 5')
    if err or not j:
        print(f"-- !! could not introspect {SCHEMA}.{table}: {err}")
        print(f"-- CREATE TABLE {SCHEMA}.{quote(table)} ( ... );  -- inspect manually\n")
        return
    cols = j.get("columns") or []
    rows = j.get("data") or []
    if not cols:
        print(f"-- {SCHEMA}.{table}: no columns returned (empty collection?)\n")
        return

    # _id first, then original order.
    names = [c.get("name") for c in cols]
    names = (["_id"] if "_id" in names else []) + [n for n in names if n != "_id"]
    typemap = {c.get("name"): c.get("type") for c in cols}

    width = max(len(quote(n)) for n in names)
    lines = []
    for n in names:
        sample = first_sample(rows, n)
        pgt = pg_type(n, typemap.get(n), sample)
        col_def = f"    {quote(n).ljust(width)}  {pgt}"
        if n == "_id":
            col_def += " NOT NULL"
        if n in FK:
            col_def += f"  -- FK -> {SCHEMA}.{FK[n]}"
        lines.append(col_def)

    print(f"CREATE TABLE {SCHEMA}.{quote(table)} (")
    body = ",\n".join(lines)
    if "_id" in names:
        body += ",\n    PRIMARY KEY (_id)"
    print(body)
    print(");\n")


def main():
    login()
    dbid = db_id()
    print("-- PostgreSQL schema generated from Auger's live inferred MongoDB schema.")
    print(f"-- source: MongoDB database '{SCHEMA}' via Auger ({DB_NAME})")
    print("-- Types/columns come from Auger; refine map/array columns (jsonb) as needed.\n")
    print(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};\n")
    for t in list_tables(dbid):
        sys.stderr.write(f"  introspecting {t}...\n")
        emit_table(dbid, t)


if __name__ == "__main__":
    main()
