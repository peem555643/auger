#!/usr/bin/env python3
"""
Build the "Mailbox Overview" dashboard in Superset over the Auger (MongoDB)
database, entirely through the REST API: virtual datasets -> charts -> dashboard.

Run it from inside the superset_app container so it can reach the API on
localhost. Credentials come from the environment; nothing is hard-coded.

    docker exec \
        -e SUPERSET_URL=http://localhost:8088 \
        -e SUPERSET_USER=admin \
        -e SUPERSET_PASS='your-admin-password' \
        -e DB_NAME='Auger-Mailbox' \
        -i superset_app python - < build_superset_dashboard.py

Re-running is safe and CONVERGENT: datasets are reused by name; charts and the
dashboard are UPSERTED, so editing a chart's viz or params here and re-running
updates the live chart rather than leaving the old definition in place.

NOTE: get_or_create_dataset reuses an existing dataset by name WITHOUT updating
its SQL. If you change a dataset's SQL here, either rename it or PUT the new SQL
onto the live dataset once; a plain re-run will keep the old SQL.
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
    r = s.get(f"{URL}/api/v1/security/csrf_token/")
    if r.status_code != 200:
        die("could not fetch CSRF token", r)
    s.headers["X-CSRFToken"] = r.json()["result"]
    s.headers["Referer"] = URL
    print(f"logged in to {URL} as {USER}")


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
    q = "(filters:!((col:table_name,opr:eq,value:'%s')),page_size:100)" % table_name
    r = s.get(f"{URL}/api/v1/dataset/", params={"q": q})
    if r.status_code == 200:
        for row in r.json().get("result", []):
            if row["table_name"] == table_name:
                print(f"  dataset '{table_name}' exists -> id {row['id']}")
                return row["id"]
    body = {"database": db_id, "schema": SCHEMA, "table_name": table_name, "sql": sql}
    r = s.post(f"{URL}/api/v1/dataset/", json=body)
    if r.status_code not in (200, 201):
        die(f"creating dataset '{table_name}' failed", r)
    ds_id = r.json()["id"]
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


def bar_horizontal(cat_col, value_col, value_label="Messages", limit=10):
    """Horizontal ECharts bar: one bar per category, biggest on top, value on bar."""
    return {
        "viz_type": "echarts_timeseries_bar",
        "x_axis": cat_col,
        "metrics": [metric(value_col, "SUM", value_label)],
        "groupby": [],
        "orientation": "horizontal",
        "x_axis_sort": value_label,
        "x_axis_sort_asc": True,
        "row_limit": limit,
        "show_value": True,
        "show_legend": False,
        "y_axis_format": "SMART_NUMBER",
        "color_scheme": "supersetColors",
        "x_axis_title": value_label,
    }


def find_chart(name):
    q = "(filters:!((col:slice_name,opr:eq,value:'%s')),page_size:100)" % name
    r = s.get(f"{URL}/api/v1/chart/", params={"q": q})
    if r.status_code == 200:
        for row in r.json().get("result", []):
            if row["slice_name"] == name:
                return row["id"]
    return None


def upsert_chart(name, viz_type, ds_id, params, aliases=None):
    """Create the chart, or UPDATE it in place if it (or one of its former
    `aliases`) already exists."""
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
    cid = find_chart(name)
    if cid is None and aliases:
        for alias in aliases:
            cid = find_chart(alias)
            if cid:
                break
    if cid:
        r = s.put(f"{URL}/api/v1/chart/{cid}", json=body)
        if r.status_code not in (200, 201):
            print(f"  !! chart '{name}' UPDATE FAILED {r.status_code}: {r.text[:300]}")
            return None
        print(f"  chart '{name}' updated -> id {cid}")
        return cid
    r = s.post(f"{URL}/api/v1/chart/", json=body)
    if r.status_code not in (200, 201):
        print(f"  !! chart '{name}' CREATE FAILED {r.status_code}: {r.text[:300]}")
        return None
    cid = r.json()["id"]
    print(f"  chart '{name}' created -> id {cid}")
    return cid


def build_position(title, rows):
    """rows: list of list of (chart_id, name, width, height)."""
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
    }
    for ri, row in enumerate(rows):
        row_id = f"ROW-{ri}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {
            "type": "ROW", "id": row_id, "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
        }
        for (cid, name, width, height) in row:
            if cid is None:
                continue
            comp = f"CHART-{cid}"
            pos[row_id]["children"].append(comp)
            pos[comp] = {
                "type": "CHART", "id": comp, "children": [],
                "meta": {"chartId": cid, "width": width, "height": height, "sliceName": name},
                "parents": ["ROOT_ID", "GRID_ID", row_id],
            }
    return pos


def link_charts(did, chart_ids):
    """position_json referencing a chartId is not enough on its own: without the
    chart<->dashboard relationship the tile renders 'no chart definition'."""
    print("linking charts to dashboard:")
    for cid in chart_ids:
        r = s.put(f"{URL}/api/v1/chart/{cid}", json={"dashboards": [did]})
        ok = r.status_code in (200, 201)
        print(f"  chart {cid} -> dashboard {did}: "
              + ("ok" if ok else f"FAILED {r.status_code}: {r.text[:200]}"))


def upsert_dashboard(title, position):
    q = "(filters:!((col:dashboard_title,opr:eq,value:'%s')),page_size:100)" % title
    r = s.get(f"{URL}/api/v1/dashboard/", params={"q": q})
    existing = None
    if r.status_code == 200:
        for row in r.json().get("result", []):
            if row["dashboard_title"] == title:
                existing = row["id"]
                break
    body = {"dashboard_title": title, "published": True, "position_json": json.dumps(position)}
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


# Top senders/orgs over profiles. group = organisation; the "Top Senders" chart
# takes every sender (parity with the original Drill query — individuals never
# appear as senders, so filtering type='person' returns nothing).
TOP_BY_TYPE = (
    "SELECT p.primaryName['TH'] AS sender_name, COUNT(*) AS msg_count "
    "FROM mailbox.email_message_sender s "
    "JOIN mailbox.profile p ON s.senderId = p._id "
    "WHERE p.\"type\" = '%s' AND p.primaryName['TH'] IS NOT NULL "
    "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
)
TOP_SENDERS_ALL = (
    "SELECT p.primaryName['TH'] AS sender_name, COUNT(*) AS msg_count "
    "FROM mailbox.email_message_sender s "
    "JOIN mailbox.profile p ON s.senderId = p._id "
    "WHERE p.primaryName['TH'] IS NOT NULL "
    "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
)
# recipientId joins profile._id, same id space as senderId.
TOP_RECIPIENTS = (
    "SELECT p.primaryName['TH'] AS recipient_name, COUNT(*) AS msg_count "
    "FROM mailbox.email_message_recipient r "
    "JOIN mailbox.profile p ON r.recipientId = p._id "
    "WHERE p.primaryName['TH'] IS NOT NULL "
    "GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
)


def main():
    login()
    db_id = find_database()

    print("datasets:")
    ds = {}
    ds["kpi_messages"] = get_or_create_dataset(db_id, "auger_kpi_messages",
        "SELECT COUNT(*) AS total_messages FROM mailbox.email_message")
    ds["kpi_users_active"] = get_or_create_dataset(db_id, "auger_kpi_users",
        "SELECT COUNT(*) AS total_users FROM mailbox.user WHERE deleted = FALSE")
    ds["kpi_users_total"] = get_or_create_dataset(db_id, "auger_kpi_total_users",
        "SELECT COUNT(*) AS total_users FROM mailbox.user")
    ds["kpi_storage"] = get_or_create_dataset(db_id, "auger_kpi_storage",
        "SELECT SUM(size) / 1073741824.0 AS total_gb FROM mailbox.email_message_sender")
    ds["by_month"] = get_or_create_dataset(db_id, "auger_msg_by_month",
        "SELECT date_trunc('month', createdAt) AS month, COUNT(*) AS messages "
        "FROM mailbox.email_message GROUP BY 1 ORDER BY 1")
    ds["by_hour"] = get_or_create_dataset(db_id, "auger_msg_by_hour",
        "SELECT CAST(date_part('hour', createdAt) AS INT) AS hour, COUNT(*) AS messages "
        "FROM mailbox.email_message GROUP BY 1 ORDER BY 1")
    ds["by_type"] = get_or_create_dataset(db_id, "auger_msg_by_type",
        "SELECT COALESCE(emailMessageType, 'unknown') AS type, COUNT(*) AS messages "
        "FROM mailbox.email_message GROUP BY 1 ORDER BY 2 DESC")
    ds["new_users_month"] = get_or_create_dataset(db_id, "auger_new_users_by_month",
        "SELECT date_trunc('month', createdAt) AS month, COUNT(*) AS users "
        "FROM mailbox.user GROUP BY 1 ORDER BY 1")
    ds["read_status"] = get_or_create_dataset(db_id, "auger_recipient_read_status",
        "SELECT COALESCE(status, 'unknown') AS status, COUNT(*) AS n "
        "FROM mailbox.email_message_recipient GROUP BY 1 ORDER BY 2 DESC")
    ds["avg_size_month"] = get_or_create_dataset(db_id, "auger_avg_size_by_month",
        "SELECT date_trunc('month', createdAt) AS month, AVG(size) / 1024.0 AS avg_kb "
        "FROM mailbox.email_message_sender GROUP BY 1 ORDER BY 1")
    ds["top_orgs"] = get_or_create_dataset(db_id, "auger_top_senders", TOP_BY_TYPE % "group")
    ds["top_persons"] = get_or_create_dataset(db_id, "auger_top_persons", TOP_SENDERS_ALL)
    ds["top_recipients"] = get_or_create_dataset(db_id, "auger_top_recipients", TOP_RECIPIENTS)

    print("charts:")
    c = {}
    c["kpi_messages"] = upsert_chart("Total Messages", "big_number_total", ds["kpi_messages"],
        {"metric": metric("total_messages", "SUM", "Total Messages")})
    c["kpi_users_total"] = upsert_chart("Total Users", "big_number_total", ds["kpi_users_total"],
        {"metric": metric("total_users", "SUM", "Total Users")})
    c["kpi_users_active"] = upsert_chart("Active Users", "big_number_total", ds["kpi_users_active"],
        {"metric": metric("total_users", "SUM", "Active Users")})
    c["kpi_storage"] = upsert_chart("Total Storage (GB)", "big_number_total", ds["kpi_storage"],
        {"metric": metric("total_gb", "SUM", "Total Storage (GB)"),
         "y_axis_format": ".2f", "number_format": ".2f"})
    c["by_month"] = upsert_chart("Messages per Month", "echarts_timeseries_bar", ds["by_month"],
        {"x_axis": "month", "metrics": [metric("messages", "SUM", "messages")], "groupby": []})
    c["by_hour"] = upsert_chart("Messages by Hour", "echarts_timeseries_bar", ds["by_hour"],
        {"x_axis": "hour", "metrics": [metric("messages", "SUM", "messages")], "groupby": []})
    c["by_type"] = upsert_chart("Messages by Type", "pie", ds["by_type"],
        {"groupby": ["type"], "metric": metric("messages", "SUM", "messages")})
    c["new_users_month"] = upsert_chart("New Users per Month", "echarts_timeseries_bar", ds["new_users_month"],
        {"x_axis": "month", "metrics": [metric("users", "SUM", "users")], "groupby": []})
    c["read_status"] = upsert_chart("Read vs Unread", "pie", ds["read_status"],
        {"groupby": ["status"], "metric": metric("n", "SUM", "recipients")})
    c["avg_size_month"] = upsert_chart("Avg Message Size (KB)", "echarts_timeseries_line", ds["avg_size_month"],
        {"x_axis": "month", "metrics": [metric("avg_kb", "AVG", "avg KB")], "groupby": []})
    c["top_persons"] = upsert_chart("Top Senders", "echarts_timeseries_bar", ds["top_persons"],
        bar_horizontal("sender_name", "msg_count"))
    c["top_orgs"] = upsert_chart("Top Orgs", "echarts_timeseries_bar", ds["top_orgs"],
        bar_horizontal("sender_name", "msg_count"), aliases=["Top Senders (group)"])
    c["top_recipients"] = upsert_chart("Top Recipients", "echarts_timeseries_bar", ds["top_recipients"],
        bar_horizontal("recipient_name", "msg_count"))

    print("dashboard:")
    layout = build_position(DASHBOARD_TITLE, [
        [(c["kpi_messages"], "Total Messages", 3, 20),
         (c["kpi_users_total"], "Total Users", 3, 20),
         (c["kpi_users_active"], "Active Users", 3, 20),
         (c["kpi_storage"], "Total Storage (GB)", 3, 20)],
        [(c["by_month"], "Messages per Month", 12, 50)],
        [(c["new_users_month"], "New Users per Month", 6, 50),
         (c["read_status"], "Read vs Unread", 6, 50)],
        [(c["by_hour"], "Messages by Hour", 6, 50),
         (c["by_type"], "Messages by Type", 6, 50)],
        [(c["top_persons"], "Top Senders", 6, 50),
         (c["top_orgs"], "Top Orgs", 6, 50)],
        [(c["top_recipients"], "Top Recipients", 6, 50),
         (c["avg_size_month"], "Avg Message Size (KB)", 6, 50)],
    ])
    did = upsert_dashboard(DASHBOARD_TITLE, layout)
    link_charts(did, [v for v in c.values() if v])

    ok = [k for k, v in c.items() if v]
    bad = [k for k, v in c.items() if not v]
    print("\ndone.")
    print(f"  charts ok: {len(ok)}  ({', '.join(ok)})")
    if bad:
        print(f"  charts FAILED: {len(bad)}  ({', '.join(bad)}) - see errors above")
    print(f"  open: {URL}/superset/dashboard/{did}/")


if __name__ == "__main__":
    main()
