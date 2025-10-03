#!/usr/bin/env python3
"""
Fetch Meta insights → build .hyper -> publish to Tableau Cloud.
"""

import os, sys, json, requests
from datetime import date, timedelta, datetime, timezone
from dotenv import load_dotenv
from tableauhyperapi import (
    HyperProcess, Connection, Telemetry,
    TableDefinition, SqlType, TableName, Inserter, CreateMode, HyperException
)
import tableauserverclient as TSC

# ---------------- ENV + CONFIG ----------------

API_VER = "v19.0"
BASE = f"https://graph.facebook.com/{API_VER}"

FIELDS = ["ad_id","ad_name","adset_id","adset_name","campaign_id","campaign_name",
          "spend","impressions","clicks","date_start","date_stop"]

def normalise_account_id(a: str) -> str:
    a = a.strip()
    return a if a.startswith("act_") else f"act_{a}"

def get_env():
    load_dotenv()
    token = os.getenv("FB_ACCESS_TOKEN")
    acct_ids = os.getenv("FB_AD_ACCOUNT_ID", "")
    accounts = [normalise_account_id(a) for a in acct_ids.split(",") if a.strip()]
    if not token or not accounts:
        print("[ERROR] Missing FB_ACCESS_TOKEN or FB_AD_ACCOUNT_ID", file=sys.stderr); sys.exit(1)

    tb_server = os.getenv("TB_SERVER")
    tb_site   = os.getenv("TB_SITE_NAME")
    tb_pat    = os.getenv("TB_PAT_NAME")
    tb_secret = os.getenv("TB_PAT_SECRET")
    tb_proj   = os.getenv("TB_PROJECT_NAME")

    return token, accounts, tb_server, tb_site, tb_pat, tb_secret, tb_proj

# ---------------- FETCH META ----------------

def build_params(since: str, until: str):
    return {
        "level": "ad",
        "time_increment": 1,
        "fields": ",".join(FIELDS),
        "time_range": json.dumps({"since": since, "until": until})
    }

def get_with_paging(url, params, headers):
    session = requests.Session()
    while url:
        resp = session.get(url, params=params, headers=headers, timeout=60)
        if not resp.ok:
            raise RuntimeError(f"API error {resp.status_code}: {resp.text[:500]}")
        payload = resp.json()
        for r in payload.get("data", []):
            yield r
        url = payload.get("paging", {}).get("next")
        params = None

def fetch_rows(token, accounts, since, until):
    headers = {"Authorization": f"Bearer {token}"}
    for acct in accounts:
        url = f"{BASE}/{acct}/insights"
        params = build_params(since, until)
        for row in get_with_paging(url, params, headers):
            yield {
                "date_start": row.get("date_start"),
                "date_stop": row.get("date_stop"),
                "account_id": acct,
                "ad_id": row.get("ad_id"),
                "ad_name": row.get("ad_name"),
                "adset_id": row.get("adset_id"),
                "adset_name": row.get("adset_name"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "spend_aud": float(row.get("spend", 0) or 0),
                "impressions": int(row.get("impressions", 0) or 0),
                "clicks": int(row.get("clicks", 0) or 0),
            }

# ---------------- BUILD HYPER ----------------

def to_date_or_none(iso_str):
    return date.fromisoformat(iso_str) if iso_str else None

def ensure_extract_schema(conn: Connection):
    # Create "Extract" schema, ignore if it already exists
    try:
        conn.catalog.create_schema("Extract")
    except HyperException as e:
        msg = str(e).lower()
        if "already exists" in msg or "schema exists" in msg:
            pass
        else:
            raise

def build_hyper(rows_iterable, outfile="meta_insights.hyper"):
    table = TableDefinition(
        table_name=TableName("Extract", "meta_spend_ad_day"),
        columns=[
            TableDefinition.Column("date_start",    SqlType.date()),
            TableDefinition.Column("date_stop",     SqlType.date()),
            TableDefinition.Column("account_id",    SqlType.text()),
            TableDefinition.Column("ad_id",         SqlType.text()),
            TableDefinition.Column("ad_name",       SqlType.text()),
            TableDefinition.Column("adset_id",      SqlType.text()),
            TableDefinition.Column("adset_name",    SqlType.text()),
            TableDefinition.Column("campaign_id",   SqlType.text()),
            TableDefinition.Column("campaign_name", SqlType.text()),
            TableDefinition.Column("spend_aud",     SqlType.double()),
            TableDefinition.Column("impressions",   SqlType.big_int()),
            TableDefinition.Column("clicks",        SqlType.big_int()),
            TableDefinition.Column("load_ts_utc",   SqlType.timestamp()),
        ]
    )

    load_ts = datetime.now(timezone.utc).replace(microsecond=0)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=outfile, create_mode=CreateMode.CREATE_AND_REPLACE) as conn:
            ensure_extract_schema(conn)          # ← fixed here
            conn.catalog.create_table(table)
            with Inserter(conn, table) as inserter:
                rows = 0
                for r in rows_iterable:
                    inserter.add_row([
                        to_date_or_none(r["date_start"]),
                        to_date_or_none(r["date_stop"]),
                        r["account_id"],
                        r["ad_id"], r.get("ad_name"),
                        r["adset_id"], r.get("adset_name"),
                        r["campaign_id"], r.get("campaign_name"),
                        r["spend_aud"], r["impressions"], r["clicks"],
                        load_ts
                    ])
                    rows += 1
                inserter.execute()
        print(f"[INFO] Hyper file written: {outfile}")
    return outfile

# ---------------- PUBLISH TO TABLEAU CLOUD ----------------

def publish_to_tableau(hyper_file, tb_server, tb_site, tb_pat, tb_secret, tb_proj):
    server = TSC.Server(tb_server, use_server_version=True)
    auth = TSC.PersonalAccessTokenAuth(tb_pat, tb_secret, tb_site)

    with server.auth.sign_in(auth):
        # Find project
        all_projects, _ = server.projects.get()
        project = next((p for p in all_projects if p.name == tb_proj), None)
        if not project:
            raise RuntimeError(f"Project '{tb_proj}' not found")

        datasource_item = TSC.DatasourceItem(project_id=project.id, name="Meta Insights Extract")
        print("[INFO] Publishing to Tableau Cloud...")
        new_ds = server.datasources.publish(datasource_item, hyper_file, mode=TSC.Server.PublishMode.Overwrite)
        print(f"[INFO] Published datasource: {new_ds.name} (ID: {new_ds.id})")

# ---------------- MAIN ----------------

if __name__ == "__main__":
    token, accounts, tb_server, tb_site, tb_pat, tb_secret, tb_proj = get_env()

    # Default = yesterday
    y = date.today() - timedelta(days=1)
    since = until = y.isoformat()

    rows = fetch_rows(token, accounts, since, until)
    hyper_file = build_hyper(rows)
    publish_to_tableau(hyper_file, tb_server, tb_site, tb_pat, tb_secret, tb_proj)
