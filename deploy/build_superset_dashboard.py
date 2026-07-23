#!/usr/bin/env python3
"""
Build a "Mailbox Overview" dashboard in Superset over the Auger (MongoDB) database,
entirely through the REST API: virtual datasets -> charts -> dashboard.

Run it from inside the superset_app container so it can reach the API on localhost.
Credentials come from the environment; nothing is hard-coded.

    docker exec \
        -e SUPERSET_URL=http://localhost:8088 \
        -e SUPERSET_USER=admin \
        -e SUPERSET_PASS='your-admin-password' \
        -e DB_NAME='Auger-Mailbox' \
        -i superset_app python - < build_dashboard.py

Re-running is safe: datasets/charts/dashboard are looked up by name and reused.
Chart params and viz types vary between Superset versions; if one chart is
rejected the script keeps going and prints which failed, so it can be tuned.
"""

import json
import os
import sys
import requests

URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
USER = os.environ.get("SUPERSET_USER")
PASS = os.environ.get("SUPERSET_PASS")
DB_NAME = os.environ.get("DB_NAME", "Auger-Mailbox")
SCHEMA = os.environ.get("SCHEMA", "mailbox")
DASHBOARD_TITLE = os.environ.get("DASHBOARD_TITLE", "Mailbox Overview")

if not USER or not PASS:
    sys.exit("set SUPERSET_USER and SUPERSET_PASS in the environment")

s = requests.Session()


def die(msg, resp=None):
    if resp is not None:
        msg += f"\n  HTTP {resp.status_code}: {resp.text[:500]}"
    sys.exit("error: " + msg)


def login():
    r = s.post(
        f"{URL}/api/v1/security/login",
        json={"username": USER, "password": PASS, "provider": "db", "refresh": True},
    )
    if r.status_code != 200:
        die("login failed - check SUPERSET_USER/SUPERSET_PASS", r)
    s.headers["Authorization"] = f"Bearer {r.json()['access_token']}"
    # CSRF token is tied to the session cookie the same session now holds.
    r = s.get(f"{URL}/api/v1/security/csrf_token/")
    if r.status_code != 200:
        die("could not fetch CSRF token", r)
    s.headers["X-CSRFToken"] = r.json()["result"]
    s.headers["Referer"] = URL
    print(f"logged in to {URL} as {USER}")


def api_version():
    try:
        r = s.get(f"{URL}/api/v1/menu/")  # cheap authenticated call
        print(f"Superset reachable (menu HTTP {r.status_code})")
    except Exception as e:  # noqa
        print(f"note: version probe failed: {e}")


def find_database():
    r = s.get(f"{URL}/api/v1/database/", params={"q": "(page_size:200)"})
    if r.status_code != 200:
        die("listing databases failed", r)
    for row in r.json()["result"]:
        if row["database_name"] == DB_NAME:
            print(f"database '{DB_NAME}' -> id {row['id']}")
            return row["id"]
    die(f"database named '{DB_NAME}' not found in Superset")


def get_or_create_dataset(db_id, table_name, sql):
    # Look up an existing virtual dataset by name first.
    q = (
        "(filters:!((col:table_name,opr:eq,value:'%s')),page_size:100)" % table_name
    )
    r = s.get(f"{URL}/api/v1/dataset/", params={"q": q})
    if r.status_code == 200:
        for row in r.json().get("result", []):
            if row["table_name"] == table_name:
                print(f"  dataset '{table_name}' exists -> id {row['id']}")
                return row["id"]
    body = {
        "database": db_id,
        "schema": SCHEMA,
        "table_name": table_name,
        "sql": sql,
    }
    r = s.post(f"{URL}/api/v1/dataset/", json=body)
    if r.status_code not in (200, 201):
        die(f"creating dataset '{table_name}' failed", r)
    ds_id = r.json()["id"]
    # Column metadata is what charts reference; make sure it is populated.
    cols = s.get(f"{URL}/api/v1/dataset/{ds_id}").json()["result"].get("columns", [])
    print(f"  dataset '{table_name}' created -> id {ds_id} ({len(cols)} columns)")
    return ds_id


def metric(col, agg, label=None):
    """An adhoc SIMPLE metric, e.g. SUM(messages)."""
    return {
        "expressionType": "SIMPLE",
        "column": {"column_name": col},
        "aggregate": agg,
        "label": label or f"{agg}({col})",
        "optionName": f"metric_{agg}_{col}",
    }


def get_or_create_chart(name, viz_type, ds_id, params):
    q = "(filters:!((col:slice_name,opr:eq,value:'%s')),page_size:100)" % name
    r = s.get(f"{URL}/api/v1/chart/", params={"q": q})
    if r.status_code == 200:
        for row in r.json().get("result", []):
            if row["slice_name"] == name:
                print(f"  chart '{name}' exists -> id {row['id']}")
                return row["id"]
    params = dict(params)
    params.setdefault("datasource", f"{ds_id}__table")
    params.setdefault("viz_type", viz_type)
    params.setdefault("time_range", "No filter")
    params.setdefault("row_limit", 1000)
    params.setdefault("adhoc_filters", [])
    body = {
        "slice_name": name,
        "viz_type": viz_type,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
    }
    r = s.post(f"{URL}/api/v1/chart/", json=body)
    if r.status_code not in (200, 201):
        print(f"  !! chart '{name}' FAILED: HTTP {r.status_code}: {r.text[:300]}")
        return None
    cid = r.json()["id"]
    print(f"  chart '{name}' created -> id {cid}")
    return cid


def build_position(title, rows):
    """rows: list of list of (chart_id, name, width). height fixed per row kind."""
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {"text": title},
        },
    }
    for ri, row in enumerate(rows):
        row_id = f"ROW-{ri}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
        }
        for (cid, name, width, height) in row:
            if cid is None:
                continue
            comp = f"CHART-{cid}"
            pos[row_id]["children"].append(comp)
            pos[comp] = {
                "type": "CHART",
                "id": comp,
                "children": [],
                "meta": {
                    "chartId": cid,
                    "width": width,
                    "height": height,
                    "sliceName": name,
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
            }
    return pos


def get_or_create_dashboard(title, position):
    q = "(filters:!((col:dashboard_title,opr:eq,value:'%s')),page_size:100)" % title
    r = s.get(f"{URL}/api/v1/dashboard/", params={"q": q})
    existing = None
    if r.status_code == 200:
        for row in r.json().get("result", []):
            if row["dashboard_title"] == title:
                existing = row["id"]
                break
    body = {
        "dashboard_title": title,
        "published": True,
        "position_json": json.dumps(position),
    }
    if existing:
        r = s.put(f"{URL}/api/v1/dashboard/{existing}", json=body)
        if r.status_code not in (200, 201):
            die("updating dashboard failed", r)
        print(f"dashboard '{title}' updated -> id {existing}")
        return existing
    r = s.post(f"{URL}/api/v1/dashboard/", json=body)
    if r.status_code not in (200, 201):
        die("creating dashboard failed", r)
    did = r.json()["id"]
    print(f"dashboard '{title}' created -> id {did}")
    return did


def main():
    login()
    api_version()
    db_id = find_database()

    print("datasets:")
    ds = {}
    ds["kpi_messages"] = get_or_create_dataset(
        db_id, "auger_kpi_messages",
        "SELECT COUNT(*) AS total_messages FROM mailbox.email_message")
    ds["kpi_users"] = get_or_create_dataset(
        db_id, "auger_kpi_users",
        "SELECT COUNT(*) AS total_users FROM mailbox.user WHERE deleted = FALSE")
    ds["by_month"] = get_or_create_dataset(
        db_id, "auger_msg_by_month",
        "SELECT date_trunc('month', createdAt) AS month, COUNT(*) AS messages "
        "FROM mailbox.email_message GROUP BY 1 ORDER BY 1")
    ds["by_hour"] = get_or_create_dataset(
        db_id, "auger_msg_by_hour",
        "SELECT CAST(date_part('hour', createdAt) AS INT) AS hour, "
        "COUNT(*) AS messages FROM mailbox.email_message GROUP BY 1 ORDER BY 1")
    ds["by_type"] = get_or_create_dataset(
        db_id, "auger_msg_by_type",
        "SELECT COALESCE(emailMessageType, 'unknown') AS type, "
        "COUNT(*) AS messages FROM mailbox.email_message GROUP BY 1 ORDER BY 2 DESC")
    ds["top_senders"] = get_or_create_dataset(
        db_id, "auger_top_senders",
        "SELECT p.primaryName['TH'] AS sender_name, COUNT(*) AS msg_count "
        "FROM mailbox.email_message_sender s "
        "JOIN mailbox.profile p ON s.senderId = p._id "
        "WHERE p.\"type\" = 'group' AND p.primaryName['TH'] IS NOT NULL "
        "GROUP BY 1 ORDER BY 2 DESC LIMIT 10")

    print("charts:")
    c = {}
    c["kpi_messages"] = get_or_create_chart(
        "Total Messages", "big_number_total", ds["kpi_messages"],
        {"metric": metric("total_messages", "SUM", "Total Messages")})
    c["kpi_users"] = get_or_create_chart(
        "Active Users", "big_number_total", ds["kpi_users"],
        {"metric": metric("total_users", "SUM", "Active Users")})
    c["by_month"] = get_or_create_chart(
        "Messages per Month", "echarts_timeseries_bar", ds["by_month"],
        {"x_axis": "month", "metrics": [metric("messages", "SUM", "messages")],
         "groupby": []})
    c["by_hour"] = get_or_create_chart(
        "Messages by Hour", "echarts_timeseries_bar", ds["by_hour"],
        {"x_axis": "hour", "metrics": [metric("messages", "SUM", "messages")],
         "groupby": []})
    c["by_type"] = get_or_create_chart(
        "Messages by Type", "pie", ds["by_type"],
        {"groupby": ["type"], "metric": metric("messages", "SUM", "messages")})
    c["top_senders"] = get_or_create_chart(
        "Top Senders (group)", "table", ds["top_senders"],
        {"query_mode": "raw", "all_columns": ["sender_name", "msg_count"],
         "order_by_cols": ['["msg_count", false]']})

    print("dashboard:")
    layout = build_position(DASHBOARD_TITLE, [
        [(c["kpi_messages"], "Total Messages", 6, 20),
         (c["kpi_users"], "Active Users", 6, 20)],
        [(c["by_month"], "Messages per Month", 12, 50)],
        [(c["by_hour"], "Messages by Hour", 6, 50),
         (c["by_type"], "Messages by Type", 6, 50)],
        [(c["top_senders"], "Top Senders (group)", 12, 50)],
    ])
    did = get_or_create_dashboard(DASHBOARD_TITLE, layout)

    ok = [k for k, v in c.items() if v]
    bad = [k for k, v in c.items() if not v]
    print("\ndone.")
    print(f"  charts created: {len(ok)}  ({', '.join(ok)})")
    if bad:
        print(f"  charts FAILED : {len(bad)}  ({', '.join(bad)}) - tell me the errors above")
    print(f"  open: {URL}/superset/dashboard/{did}/")


if __name__ == "__main__":
    main()
