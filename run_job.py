#!/usr/bin/env python3
"""
SOCTRA – Hourly Data Puller & Valuation Engine
================================================

What it does:
1. Reads the list of entities (YouTube channels) from Supabase.
2. Calls the YouTube Data API to fetch:
   - total view count
   - total subscriber count
   (for an MVP we treat these two numbers as the "total engagement").
3. Calculates the price using the deterministic formula you supplied.
4. Stores raw numbers and the calculated price back into Supabase.

All secrets (YouTube API key, Supabase URL & service‑role key) are read
from environment variables – they are injected via GitHub Actions secrets,
so they never appear in the public repo.
"""

import os
import math
import datetime
from googleapiclient.discovery import build
from supabase import create_client, Client

# ----------- CONFIGURATION (read from env vars) -----------------
YT_API_KEY = os.getenv('YT_API_KEY')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
# -----------------------------------------------------------------

if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
    raise RuntimeError(
        "One or more required env vars are missing. "
        "Check GH Actions secrets: YT_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY."
    )

# Initialise clients
yt = build('youtube', 'v3', developerKey=YT_API_KEY)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ---------- Helper Functions -----------------------------------
def fetch_channel_stats(channel_id: str):
    """
    Returns (total_views, total_subscribers) for a given YouTube channel.
    """
    resp = yt.channels().list(
        part='statistics',
        id=channel_id
    ).execute()
    if not resp['items']:
        raise ValueError(f"Channel ID {channel_id} not found.")
    stats = resp['items'][0]['statistics']
    total_views = int(stats.get('viewCount', 0))
    total_subs = int(stats.get('subscriberCount', 0))  # note: this may be hidden for some channels;
                                                       # we treat hidden as 0 for simplicity.
    return total_views, total_subs

def get_last_raw_metric(entity_id: int):
    """
    Pull the most recent raw metric row for an entity.
    Returns a dict with keys e_total, e_delta, ts or None.
    """
    rows = (supabase
            .table('raw_metrics')
            .select('e_total, e_delta, ts')
            .eq('entity_id', entity_id)
            .order('ts', desc=True)
            .limit(1)
            .execute())
    return rows.data[0] if rows.data else None

def insert_raw_metric(entity_id: int, e_total: int, e_delta: int):
    supabase.table('raw_metrics').insert({
        'entity_id': entity_id,
        'e_total': e_total,
        'e_delta': e_delta,
        'ts': datetime.datetime.utcnow().isoformat()
    }).execute()

def upsert_valuation(entity_id: int, result: dict, ts_iso: str):
    """
    Stores the calculated valuation pieces in the `valuation` table.
    """
    supabase.table('valuation').upsert({
        'entity_id': entity_id,
        'ts': ts_iso,
        's_score': result['S'],
        'v_pre': result['Vpre'],
        'phi': result['Phi'],
        'psi': result['Psi'],
        'v_final': result['Vfinal']
    }, on_conflict=['entity_id', 'ts']).execute()

# ---------- THE FORMULA (exactly as you gave) ------------------

def compute_valuation(e_total: int, e_delta: int,
                     volume_surge: float = 1.0,
                     anomaly_flag: int = 0):
    """
    Returns a dict with the intermediate and final values.
    For the MVP we set:
    - volume_surge = 1.0 (no transaction volume yet)
    - anomaly_flag = 0 (no anomaly yet)
    """

    # === Tuneable parameters (you can change later) ===
    ω1 = 0.2          # weight of log term
    ω2 = 0.8          # weight of short‑term momentum
    α  = 1.0          # scaling for S(t) → Vpre
    Vbase = 1.0       # base price to avoid zero
    β  = 0.5          # surge adjustment scale
    κ  = 2.0          # surge steepness
    δ  = 0.2          # anomaly penalty factor
    # ===================================================

    # 1️⃣ S(t) – combined long‑term + short‑term score
    S = ω1 * math.log(1 + e_total) + ω2 * (e_delta / max(1, e_total))

    # 2️⃣ Vpre(t)
    Vpre = Vbase + α * S

    # 3️⃣ Φ(t) – surge factor (uses volume_surge = TV/MAV)
    # For now volume_surge is passed in; later you could compute it from the trades table.
    Phi = 1 + β * math.tanh(κ * (volume_surge - 1))

    # 4️⃣ Ψ(t) – anomaly factor
    Psi = 1 - δ * anomaly_flag   # anomaly_flag = 0 or 1

    # 5️⃣ Final price
    Vfinal = Vpre * Phi * Psi

    return {
        'S': S,
        'Vpre': Vpre,
        'Phi': Phi,
        'Psi': Psi,
        'Vfinal': Vfinal
    }

# ---------- MAIN LOOP -----------------------------------------
def main():
    now_iso = datetime.datetime.utcnow().isoformat()

    # 1️⃣ Pull all entities that have platform = youtube
    entity_rows = (supabase
                   .table('entities')
                   .select('id, external_id')
                   .eq('platform', 'youtube')
                   .execute())
    entities = entity_rows.data

    for ent in entities:
        entity_id = ent['id']
        channel_id = ent['external_id']

        # a) Get the current totals from YouTube
        total_views, total_subs = fetch_channel_stats(channel_id)
        e_total = total_views + total_subs   # your definition of "total engagement"

        # b) Compute the delta since the previous capture
        last_metric = get_last_raw_metric(entity_id)
        if last_metric:
            e_delta = max(0, e_total - last_metric['e_total'])
        else:
            e_delta = 0   # first run – no delta yet

        # c) Store raw numbers (so you can see historical raw data)
        insert_raw_metric(entity_id, e_total, e_delta)

        # d) Compute the valuation (using the formula)
        #    We set volume_surge=1 (no trades yet) and anomaly_flag=0 for now.
        valuation = compute_valuation(e_total, e_delta,
                                     volume_surge=1.0,
                                     anomaly_flag=0)

        # e) Write the valuation into the DB
        upsert_valuation(entity_id, valuation, now_iso)

        # Print a tiny line to the GitHub Action log (optional)
        print(f"✅ {ent['external_id']} – e_total={e_total:,} Δ={e_delta:,} → price={valuation['Vfinal']:.2f}")

if __name__ == '__main__':
    main()
