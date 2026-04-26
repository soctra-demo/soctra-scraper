#!/usr/bin/env python3
from datetime import timezone

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
import hashlib
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
        'ts': datetime.datetime.now(timezone.utc).isoformat()
    }).execute()

def insert_valuation(entity_id: int, result: dict, ts_iso: str):
    """
    INSERT a new valuation row (no upsert needed - each hour creates new history)
    """
    supabase.table('valuation').insert({
        'entity_id': entity_id,
        'ts': ts_iso,
        's_score': result['S'],
        'v_pre': result['Vpre'],
        'phi': result['Phi'],
        'psi': result['Psi'],
        'v_final': result['Vfinal']
    }).execute()

# ---------- THE FORMULA (exactly as you gave) ------------------

def compute_valuation(e_total: int, e_delta: int,
                     volume_surge: float = 1.0,
                     anomaly_flag: int = 0,
                     entity_category: str = 'general'):
    """
    SOCTRA Valuation Formula (Patent-Compliant Version)
    Matches Provisional Application Filed by Jeffery Savio Titus
    """
    # ========== PATENT PARAMETERS (Tunable for Volatility) ==========
    # To increase price movement, increase ω2 (omega2)
    ω1 = 0.3              # Long-term popularity weight (Patent Formula)
    ω2 = 2.5              # Short-term momentum weight (INCREASED for volatility)
    α  = 2.0              # Sensitivity scaling (Patent Formula)
    Vbase = 10.0          # Baseline constant (Patent Formula)
    β  = 0.8              # Surge adjustment scale (Patent Formula)
    κ  = 3.0              # Surge steepness (Patent Formula)
    δ  = 0.3              # Anomaly penalty (Patent Formula)
    # ================================================================
    
    # 1️⃣ S(t) – Patented Formula Structure
    # Note: Using max(1, e_total) exactly as per Patent Page 9, Item (d)
    momentum = e_delta / max(1, e_total)
    S = ω1 * math.log(1 + e_total) + ω2 * momentum
    
    # 2️⃣ Vpre(t) – Patented Formula
    Vpre = Vbase + α * S
    
    # 3️⃣ Φ(t) – Patented Surge Factor (using tanh)
    Phi = 1 + β * math.tanh(κ * (volume_surge - 1))
    
    # 4️⃣ Ψ(t) – Patented Anomaly Penalty
    Psi = 1 - δ * anomaly_flag
    
    # 5️⃣ Deterministic Volatility (Hash-Based, NOT Random)
    # This ensures prices move daily but remain reproducible (Patent Page 3, Object c)
    current_date = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d')
    seed_string = f"{current_date}:{e_total}:{e_delta}"
    seed_hash = int(hashlib.md5(seed_string.encode()).hexdigest(), 16)
    # Creates a deterministic factor between 0.97 and 1.03 (±3%)
    sentiment_factor = 1 + ((seed_hash % 1000) / 1000 - 0.5) * 2 * 0.03
    
    # 6️⃣ V(t) – Final Patented Formula
    Vfinal_raw = Vpre * Phi * Psi * sentiment_factor
    
    # 7️⃣ Fractionalization (Patent Claim 7: 0.25 increments)
    Vfinal = round(Vfinal_raw * 4) / 4
    
    # 8️⃣ Minimum Price Floor
    Vfinal = max(0.25, Vfinal)
    
    return {
        'S': round(S, 4),
        'Vpre': round(Vpre, 4),
        'Phi': round(Phi, 4),
        'Psi': round(Psi, 4),
        'Vfinal': round(Vfinal, 2)
    }
# ---------- MAIN LOOP -----------------------------------------
def main():
    now_iso = datetime.datetime.now(timezone.utc).isoformat()

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
        insert_valuation(entity_id, valuation, now_iso)

        # Print a tiny line to the GitHub Action log (optional)
        print(f"✅ {ent['external_id']} – e_total={e_total:,} Δ={e_delta:,} → price={valuation['Vfinal']:.2f}")

if __name__ == '__main__':
    main()
