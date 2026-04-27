"""
╔══════════════════════════════════════════════════════════════════════╗
║  SENTINEL  —  Enterprise Fraud Detection Dashboard                   ║
║  Stack: Streamlit · aiokafka · asyncpg · Plotly · Altair             ║
║  Author: Senior Full-Stack Data Engineer (AI-generated scaffold)     ║
╚══════════════════════════════════════════════════════════════════════╝

Run:
    pip install streamlit aiokafka asyncpg plotly altair pandas numpy psycopg2-binary
    streamlit run dashboard.py
"""
# ─── Standard Library ────────────────────────────────────────────────────────
import asyncio
import json
import time
import threading
import random
from collections import deque
#from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta
from typing import Optional

# ─── Third-Party ─────────────────────────────────────────────────────────────
import numpy as np
import pandas as pd
from streamlit.runtime.scriptrunner import add_script_run_ctx
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

try:
    import altair as alt
    ALTAIR_AVAILABLE = True
except ImportError:
    ALTAIR_AVAILABLE = False

try:
    from aiokafka import AIOKafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    st.warning("aiokafka no instalado. Usando datos simulados.")

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIG & CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
KAFKA_BOOTSTRAP    = "localhost:19092"
KAFKA_TOPIC        = "fraud-results"
KAFKA_GROUP        = "sentinel-dashboard"
PG_DSN             = "postgresql://sentinel_admin:super_secret_password@localhost:5432/sentinel_db"
PG_TABLE           = "fraud_logs"

MAX_LIVE_ROWS      = 2_000   # rolling window in session state
REFRESH_INTERVAL   = 1.5     # seconds between UI refreshes
MAX_FEED_ROWS      = 50      # event feed visible rows
FRAUD_PROB_ALERT   = 0.95    # SOC critical threshold

TRANSACTION_TYPES  = ["CASH_OUT", "TRANSFER", "PAYMENT", "CASH_IN", "DEBIT"]

# ══════════════════════════════════════════════════════════════════════════════
#  STREAMLIT PAGE CONFIG  (must be first Streamlit call)
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="SENTINEL · Fraud Detection",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL CSS  — Dark SOC / Glassmorphism / Neon-Red Matrix aesthetic
# ══════════════════════════════════════════════════════════════════════════════
SENTINEL_CSS = """
<style>
/* ── Google Fonts ─────────────────────────────────────────────────── */
@import url('https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@300;400;500;600;700&family=Orbitron:wght@400;700;900&display=swap');

/* ── Root Variables ───────────────────────────────────────────────── */
:root {
    --bg-base:        #020408;
    --bg-surface:     #070d14;
    --bg-card:        rgba(8, 20, 36, 0.72);
    --bg-card-hover:  rgba(12, 28, 50, 0.85);
    --border-dim:     rgba(0, 200, 255, 0.08);
    --border-glow:    rgba(255, 20, 60, 0.45);
    --border-safe:    rgba(0, 255, 100, 0.35);
    --accent-red:     #ff1e3c;
    --accent-red-dim: #8b0020;
    --accent-green:   #00ff65;
    --accent-cyan:    #00c8ff;
    --accent-yellow:  #ffcc00;
    --text-primary:   #e8f4ff;
    --text-secondary: #7a9bb5;
    --text-mono:      'Share Tech Mono', monospace;
    --text-display:   'Orbitron', sans-serif;
    --text-body:      'Rajdhani', sans-serif;
    --shadow-red:     0 0 24px rgba(255, 30, 60, 0.3);
    --shadow-cyan:    0 0 18px rgba(0, 200, 255, 0.2);
}

/* ── Base & Background ────────────────────────────────────────────── */
html, body, [data-testid="stAppViewContainer"],
[data-testid="stMain"], .main {
    background: var(--bg-base) !important;
    color: var(--text-primary) !important;
    font-family: var(--text-body) !important;
}

/* Animated scanline overlay */
[data-testid="stAppViewContainer"]::before {
    content: '';
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(0,0,0,0.03) 2px,
        rgba(0,0,0,0.03) 4px
    );
    pointer-events: none;
    z-index: 9999;
}

/* ── Sidebar ──────────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: var(--bg-surface) !important;
    border-right: 1px solid var(--border-dim) !important;
}

/* ── Header / Title area ──────────────────────────────────────────── */
.sentinel-header {
    display: flex;
    align-items: center;
    gap: 1rem;
    padding: 1.2rem 1.6rem;
    background: linear-gradient(135deg,
        rgba(255,30,60,0.08) 0%,
        rgba(0,200,255,0.04) 50%,
        rgba(0,0,0,0) 100%);
    border-bottom: 1px solid var(--border-glow);
    margin-bottom: 1.2rem;
    position: relative;
    overflow: hidden;
}
.sentinel-header::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg,
        transparent, var(--accent-red), var(--accent-cyan), transparent);
    animation: scanH 3s linear infinite;
}
@keyframes scanH { from{transform:translateX(-100%)} to{transform:translateX(100%)} }

.sentinel-logo {
    font-family: var(--text-display);
    font-size: 2rem;
    font-weight: 900;
    color: var(--accent-red);
    text-shadow: var(--shadow-red), 0 0 60px rgba(255,30,60,0.4);
    letter-spacing: 0.12em;
}
.sentinel-subtitle {
    font-family: var(--text-mono);
    font-size: 0.72rem;
    color: var(--text-secondary);
    letter-spacing: 0.18em;
    text-transform: uppercase;
}
.sentinel-status {
    margin-left: auto;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-family: var(--text-mono);
    font-size: 0.75rem;
    color: var(--accent-green);
}
.status-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    background: var(--accent-green);
    box-shadow: 0 0 8px var(--accent-green);
    animation: pulse 1.8s ease-in-out infinite;
}
@keyframes pulse {
    0%,100%{opacity:1;transform:scale(1)}
    50%{opacity:0.5;transform:scale(0.8)}
}

/* ── Tabs ─────────────────────────────────────────────────────────── */
[data-testid="stTabs"] button {
    font-family: var(--text-display) !important;
    font-size: 0.72rem !important;
    letter-spacing: 0.14em !important;
    color: var(--text-secondary) !important;
    background: transparent !important;
    border: none !important;
    border-bottom: 2px solid transparent !important;
    text-transform: uppercase !important;
    padding: 0.6rem 1.2rem !important;
    transition: all 0.25s ease !important;
}
[data-testid="stTabs"] button:hover {
    color: var(--accent-cyan) !important;
}
[data-testid="stTabs"] button[aria-selected="true"] {
    color: var(--accent-red) !important;
    border-bottom: 2px solid var(--accent-red) !important;
    text-shadow: var(--shadow-red) !important;
}
[data-testid="stTabsContent"] {
    background: transparent !important;
    border: none !important;
}

/* ── KPI Cards (glassmorphism) ────────────────────────────────────── */
.kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 1rem;
    margin-bottom: 1.4rem;
}
.kpi-card {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 12px;
    padding: 1.1rem 1.4rem;
    backdrop-filter: blur(16px);
    -webkit-backdrop-filter: blur(16px);
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}
.kpi-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg,
        transparent, var(--accent-red), transparent);
    opacity: 0.6;
}
.kpi-card:hover {
    background: var(--bg-card-hover);
    border-color: rgba(255,30,60,0.25);
    box-shadow: var(--shadow-red);
    transform: translateY(-2px);
}
.kpi-card.green::before { background: linear-gradient(90deg,transparent,var(--accent-green),transparent); }
.kpi-card.cyan::before  { background: linear-gradient(90deg,transparent,var(--accent-cyan),transparent); }
.kpi-card.yellow::before{ background: linear-gradient(90deg,transparent,var(--accent-yellow),transparent); }

.kpi-label {
    font-family: var(--text-mono);
    font-size: 0.65rem;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--text-secondary);
    margin-bottom: 0.35rem;
}
.kpi-value {
    font-family: var(--text-display);
    font-size: 1.8rem;
    font-weight: 700;
    color: var(--text-primary);
    line-height: 1;
}
.kpi-value.red    { color: var(--accent-red);    text-shadow: var(--shadow-red); }
.kpi-value.green  { color: var(--accent-green);  text-shadow: 0 0 16px rgba(0,255,100,0.4); }
.kpi-value.cyan   { color: var(--accent-cyan);   text-shadow: var(--shadow-cyan); }
.kpi-value.yellow { color: var(--accent-yellow); text-shadow: 0 0 16px rgba(255,204,0,0.4); }
.kpi-delta {
    font-family: var(--text-mono);
    font-size: 0.65rem;
    color: var(--text-secondary);
    margin-top: 0.3rem;
}

/* ── Section Headers ──────────────────────────────────────────────── */
.section-title {
    font-family: var(--text-display);
    font-size: 0.78rem;
    font-weight: 700;
    letter-spacing: 0.22em;
    text-transform: uppercase;
    color: var(--accent-cyan);
    margin: 1.2rem 0 0.7rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.section-title::after {
    content: '';
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, var(--accent-cyan), transparent);
    opacity: 0.3;
    margin-left: 0.5rem;
}

/* ── Event Feed ───────────────────────────────────────────────────── */
.event-feed {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 10px;
    padding: 1rem;
    backdrop-filter: blur(12px);
    max-height: 360px;
    overflow-y: auto;
    font-family: var(--text-mono);
    font-size: 0.72rem;
}
.event-feed::-webkit-scrollbar { width: 4px; }
.event-feed::-webkit-scrollbar-track { background: transparent; }
.event-feed::-webkit-scrollbar-thumb { background: var(--accent-red-dim); border-radius: 2px; }

.feed-row {
    display: flex;
    gap: 1rem;
    padding: 0.3rem 0;
    border-bottom: 1px solid rgba(255,255,255,0.03);
    align-items: center;
}
.feed-row:last-child { border-bottom: none; }
.feed-ts   { color: var(--text-secondary); min-width: 80px; }
.feed-amt  { color: var(--accent-cyan); min-width: 90px; text-align: right; }
.feed-type { color: var(--text-secondary); min-width: 80px; }
.feed-badge-fraud   { color: var(--bg-base); background: var(--accent-red);    padding: 1px 6px; border-radius: 4px; font-size: 0.62rem; }
.feed-badge-suspect { color: var(--bg-base); background: var(--accent-yellow); padding: 1px 6px; border-radius: 4px; font-size: 0.62rem; }
.feed-badge-ok      { color: var(--bg-base); background: var(--accent-green);  padding: 1px 6px; border-radius: 4px; font-size: 0.62rem; }

/* ── Alert Table ──────────────────────────────────────────────────── */
.alert-critical {
    background: rgba(255,30,60,0.06);
    border: 1px solid var(--border-glow);
    border-radius: 10px;
    padding: 1rem;
}

/* ── Dataframe overrides ──────────────────────────────────────────── */
[data-testid="stDataFrame"] {
    background: var(--bg-card) !important;
    border: 1px solid var(--border-dim) !important;
    border-radius: 8px !important;
}
[data-testid="stDataFrame"] th {
    font-family: var(--text-mono) !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.12em !important;
    text-transform: uppercase !important;
    color: var(--accent-cyan) !important;
    background: rgba(0,200,255,0.05) !important;
}
[data-testid="stDataFrame"] td {
    font-family: var(--text-mono) !important;
    font-size: 0.75rem !important;
    color: var(--text-primary) !important;
}

/* ── Metric widget overrides ──────────────────────────────────────── */
[data-testid="stMetric"] {
    background: var(--bg-card);
    border: 1px solid var(--border-dim);
    border-radius: 10px;
    padding: 0.8rem 1rem;
    backdrop-filter: blur(12px);
}
[data-testid="stMetricLabel"] {
    font-family: var(--text-mono) !important;
    font-size: 0.65rem !important;
    letter-spacing: 0.15em !important;
    text-transform: uppercase !important;
    color: var(--text-secondary) !important;
}
[data-testid="stMetricValue"] {
    font-family: var(--text-display) !important;
    font-size: 1.5rem !important;
    color: var(--text-primary) !important;
}

/* ── Progress / misc ──────────────────────────────────────────────── */
.stProgress > div > div > div {
    background: linear-gradient(90deg, var(--accent-red), var(--accent-cyan)) !important;
}

/* ── Plotly charts background transparency ────────────────────────── */
.js-plotly-plot .plotly, .js-plotly-plot .plotly .svg-container {
    background: transparent !important;
}

/* ── Hide default streamlit elements ─────────────────────────────── */
#MainMenu, footer, header { visibility: hidden; }
[data-testid="stToolbar"] { display: none; }
</style>
"""

st.markdown(SENTINEL_CSS, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
#  PLOTLY DARK TEMPLATE
# ══════════════════════════════════════════════════════════════════════════════
PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(7,13,20,0.6)",
    font=dict(family="Share Tech Mono, monospace", color="#e8f4ff", size=11),
    xaxis=dict(
        gridcolor="rgba(0,200,255,0.06)",
        zerolinecolor="rgba(0,200,255,0.12)",
        tickfont=dict(family="Share Tech Mono", size=10),
    ),
    yaxis=dict(
        gridcolor="rgba(0,200,255,0.06)",
        zerolinecolor="rgba(0,200,255,0.12)",
        tickfont=dict(family="Share Tech Mono", size=10),
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor="rgba(255,255,255,0.08)",
        borderwidth=1,
        font=dict(family="Share Tech Mono", size=10),
    ),
    margin=dict(l=40, r=20, t=40, b=40),
)

# ══════════════════════════════════════════════════════════════════════════════
#  SESSION STATE BOOTSTRAP
# ══════════════════════════════════════════════════════════════════════════════
def _init_state():
    defaults = {
        "tx_buffer":       deque(maxlen=MAX_LIVE_ROWS),
        "kafka_running":   False,
        "kafka_thread":    None,
        "last_refresh":    0.0,
        "infra_cpu":       deque(maxlen=120),
        "infra_ram":       deque(maxlen=120),
        "infra_latency":   deque(maxlen=120),
        "infra_engines":   3,
        "infra_ts":        deque(maxlen=120),
        "total_processed": 0,
        "total_volume":    0.0,
        "total_capital_saved": 0.0, 
        "total_risk_amount":   0.0,  
        "total_tp": 0,  
        "total_tn": 0,  
        "total_fp": 0,  
        "total_fn": 0,  
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init_state()

# ══════════════════════════════════════════════════════════════════════════════
#  DATA LAYER — Kafka consumer thread (async inside thread)
# ══════════════════════════════════════════════════════════════════════════════
def _generate_mock_transaction() -> dict:
    """Simulated payload when Kafka is unavailable or before connection."""
    tx_type   = random.choice(TRANSACTION_TYPES)
    amount    = round(random.lognormvariate(7, 2), 2)
    old_bal   = round(random.uniform(0, 200_000), 2)
    new_bal   = max(0.0, round(old_bal - amount + random.uniform(-500, 500), 2))
    fp        = random.betavariate(0.3, 3)                # skewed toward 0
    if random.random() < 0.04:                            # ~4% fraud
        fp = random.betavariate(3, 0.4)                   # skewed toward 1
    is_fraud  = fp > 0.5
    # Introduce label noise for realistic confusion matrix
    real_label = is_fraud if random.random() > 0.08 else (not is_fraud)
    return {
        "amount":           amount,
        "is_fraud":         is_fraud,
        "real_label_Fraud": real_label,
        "fraud_probability": round(fp, 4),
        "type":             tx_type,
        "hourOfDay":        random.randint(0, 23),
        "oldbalanceOrg":    old_bal,
        "newbalanceOrig":   new_bal,
        "processed_at":     datetime.utcnow().isoformat(),
        "_id":              f"TX-{random.randint(100000,999999)}",
    }


async def _kafka_consumer_loop():
    """
    Async Kafka consumer.  Appends parsed messages to session state buffer.
    Falls back to mock data on connection failure.
    """
    if not KAFKA_AVAILABLE:
        await _mock_data_loop()
        return

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=f"sentinel-dash-{random.randint(1, 10000)}",
        #group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    try:
        await consumer.start()
        async for msg in consumer:
            tx = msg.value
            if "_id" not in tx:
                tx["_id"] = f"TX-{msg.offset}"
            
            amt = float(tx.get("amount", 0.0))
            is_fraud = bool(tx.get("is_fraud", False))
            is_real  = bool(tx.get("real_label_Fraud", False))

            st.session_state.tx_buffer.appendleft(tx)
            st.session_state.total_processed += 1
            st.session_state.total_volume += amt
            if is_fraud and is_real:
                st.session_state.total_tp += 1
                st.session_state.total_capital_saved += amt
            elif not is_fraud and not is_real:
                st.session_state.total_tn += 1
            elif is_fraud and not is_real:
                st.session_state.total_fp += 1
            elif not is_fraud and is_real:
                st.session_state.total_fn += 1
                st.session_state.total_risk_amount += amt

            if not st.session_state.kafka_running:
                break
    except Exception:
        print(f"CRITICAL ERROR IN THE KAFKA THREAD: {Exception}")
        # Kafka unreachable — fall back to mock
        await consumer.stop() if consumer else None
        await _mock_data_loop()
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass


async def _mock_data_loop():
    """Generates synthetic transactions when Kafka is unavailable."""
    while st.session_state.kafka_running:
        # Burst: 1-4 transactions per tick
        for _ in range(random.randint(1, 4)):
            tx = _generate_mock_transaction()
            st.session_state.tx_buffer.appendleft(tx)
            st.session_state.total_processed += 1
        # Simulate infra metrics
        now = datetime.utcnow().isoformat()
        st.session_state.infra_cpu.append(
            min(98, max(5, st.session_state.infra_cpu[-1] + random.gauss(0, 4)))
            if st.session_state.infra_cpu else 35.0
        )
        st.session_state.infra_ram.append(
            min(95, max(20, st.session_state.infra_ram[-1] + random.gauss(0, 2)))
            if st.session_state.infra_ram else 52.0
        )
        st.session_state.infra_latency.append(
            max(0.8, abs(random.gauss(12, 5)))
        )
        st.session_state.infra_ts.append(now)
        await asyncio.sleep(REFRESH_INTERVAL)


def _run_async_loop_in_thread():
    import sys
    import asyncio
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(_kafka_consumer_loop())
    loop.close()


def start_kafka_consumer():
    if not st.session_state.kafka_running:
        st.session_state.kafka_running = True
        t = threading.Thread(target=_run_async_loop_in_thread, daemon=True)
        
        add_script_run_ctx(t)

        t.start()
        st.session_state.kafka_thread = t


# ══════════════════════════════════════════════════════════════════════════════
#  POSTGRES LOADER  (historical bootstrap)
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=30, show_spinner=False)
def load_postgres_history(limit: int = 5_000) -> pd.DataFrame:
    """Load historical records from PostgreSQL and unpack JSON payload."""
    if not ASYNCPG_AVAILABLE:
        return pd.DataFrame()
    async def _fetch():
        try:
            conn = await asyncpg.connect(PG_DSN)
            rows = await conn.fetch(
                f"""
                SELECT id, transaction_data, is_fraud, fraud_probability, timestamp
                FROM {PG_TABLE}
                ORDER BY timestamp DESC
                LIMIT $1
                """,
                limit
            )
            await conn.close()
            return [dict(r) for r in rows]
        except Exception as e:
            print(f"\nPOSTGRES ALERT: {e}\n")  
            return []
    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(_fetch())
    loop.close()
    if not result:
        return pd.DataFrame()
    df_raw = pd.DataFrame(result)
    
    try:
        def parse_payload(val):
            if isinstance(val, str):
                return json.loads(val)
            return val
        json_list = df_raw['transaction_data'].apply(parse_payload).tolist()
        df_json = pd.DataFrame(json_list)
        df_raw.rename(columns={'timestamp': 'processed_at'}, inplace=True)
        df_final = pd.concat([df_raw[['id', 'is_fraud', 'fraud_probability', 'processed_at']], df_json], axis=1)
        # Normalize column names to match live schema
        renames = {
            "real_label_fraud": "real_label_Fraud",
            "hour_of_day":      "hourOfDay",
            "old_balance_orig": "oldbalanceOrg",
            "new_balance_orig": "newbalanceOrig",
        }
        renames_seguros = {k: v for k, v in renames.items() if k in df_final.columns}
        df_final.rename(columns=renames_seguros, inplace=True)
        return df_final

    except Exception as e:
            print(f"\nERROR UNPACKING JSON: {e}\n")
            return pd.DataFrame()


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def buffer_to_df() -> pd.DataFrame:
    rows = list(st.session_state.tx_buffer)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["processed_at"] = pd.to_datetime(df["processed_at"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df["fraud_probability"] = pd.to_numeric(df["fraud_probability"], errors="coerce")
    return df


def fmt_currency(v: float) -> str:
    if v >= 1e9:  return f"${v/1e9:.2f}B"
    if v >= 1e6:  return f"${v/1e6:.2f}M"
    if v >= 1e3:  return f"${v/1e3:.1f}K"
    return f"${v:,.2f}"


def kpi_card(label: str, value: str, accent: str = "", delta: str = "") -> str:
    return f"""
    <div class="kpi-card {accent}">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value {accent}">{value}</div>
      {'<div class="kpi-delta">' + delta + '</div>' if delta else ''}
    </div>
    """


def section_title(title: str, icon: str = "◈") -> None:
    st.markdown(
        f'<div class="section-title"><span>{icon}</span> {title}</div>',
        unsafe_allow_html=True
    )


# ══════════════════════════════════════════════════════════════════════════════
#  CHART BUILDERS
# ══════════════════════════════════════════════════════════════════════════════
def chart_volume_timeseries(df: pd.DataFrame) -> go.Figure:
    if df.empty or "processed_at" not in df.columns:
        return go.Figure().update_layout(**PLOTLY_LAYOUT, title="Awaiting data…")

    ts = (df.set_index("processed_at")["amount"]
           .resample("10s").sum()
           .reset_index())
    ts.columns = ["time", "volume"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ts["time"], y=ts["volume"],
        mode="lines",
        name="Volume",
        line=dict(color="#00c8ff", width=1.5),
        fill="tozeroy",
        fillcolor="rgba(0,200,255,0.06)",
    ))
    # Overlay fraud spikes
    fraud_df = df[df["is_fraud"] == True].copy()
    if not fraud_df.empty:
        fraud_ts = (fraud_df.set_index("processed_at")["amount"]
                    .resample("10s").sum()
                    .reset_index())
        fraud_ts.columns = ["time", "volume"]
        fig.add_trace(go.Scatter(
            x=fraud_ts["time"], y=fraud_ts["volume"],
            mode="lines",
            name="Fraud",
            line=dict(color="#ff1e3c", width=2),
            fill="tozeroy",
            fillcolor="rgba(255,30,60,0.08)",
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Transactional Volume — Live", font=dict(size=12, family="Orbitron")),
        hovermode="x unified",
    )
    return fig


def chart_scatter_live(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure().update_layout(**PLOTLY_LAYOUT, title="Awaiting data…")

    def classify(row):
        if row["is_fraud"]:
            return "FRAUD", "#ff1e3c", 10
        if row["fraud_probability"] > 0.4:
            return "SUSPECT", "#ffcc00", 7
        return "NORMAL", "#00ff65", 5

    df = df.copy().dropna(subset=["processed_at", "amount"]).tail(500)
    classes = df.apply(classify, axis=1, result_type="expand")
    df["_class"]  = classes[0]
    df["_color"]  = classes[1]
    df["_size"]   = classes[2]

    fig = go.Figure()
    for cls, color in [("NORMAL","#00ff65"), ("SUSPECT","#ffcc00"), ("FRAUD","#ff1e3c")]:
        sub = df[df["_class"] == cls]
        if sub.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sub["processed_at"],
            y=sub["amount"],
            mode="markers",
            name=cls,
            marker=dict(
                color=color,
                size=sub["_size"],
                opacity=0.75,
                line=dict(width=0.5, color="rgba(255,255,255,0.2)"),
            ),
            text=sub.get("_id", sub.index),
            hovertemplate="<b>%{text}</b><br>Amount: $%{y:,.2f}<br>Time: %{x}<extra></extra>",
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Transaction Stream — Scatter", font=dict(size=12, family="Orbitron")),
        yaxis_title="Amount (USD)",
        showlegend=True,
    )
    return fig


def chart_confusion_matrix(df: pd.DataFrame) -> go.Figure:
    if df.empty:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)

    tp = len(df[(df["is_fraud"]==True)  & (df["real_label_Fraud"]==True)])
    fp = len(df[(df["is_fraud"]==True)  & (df["real_label_Fraud"]==False)])
    fn = len(df[(df["is_fraud"]==False) & (df["real_label_Fraud"]==True)])
    tn = len(df[(df["is_fraud"]==False) & (df["real_label_Fraud"]==False)])

    z    = [[tp, fp], [fn, tn]]
    text = [[f"HIT\n{tp:,}", f"FRICTION\n{fp:,}"],
            [f"LOSS\n{fn:,}", f"OK\n{tn:,}"]]
    xlbl = ["Predicted FRAUD", "Predicted NORMAL"]
    ylbl = ["Actual FRAUD", "Actual NORMAL"]

    fig = go.Figure(go.Heatmap(
        z=z, x=xlbl, y=ylbl,
        text=text, texttemplate="%{text}",
        colorscale=[
            [0.0, "rgba(0,255,100,0.15)"],
            [0.5, "rgba(0,200,255,0.35)"],
            [1.0, "rgba(255,30,60,0.6)"],
        ],
        showscale=False,
        textfont=dict(family="Orbitron", size=14, color="#e8f4ff"),
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Confusion Matrix — Real-Time", font=dict(size=12, family="Orbitron")),
        height=320,
    )
    return fig


def chart_fraud_prob_histogram(df: pd.DataFrame) -> go.Figure:
    if df.empty or "fraud_probability" not in df.columns:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=df["fraud_probability"],
        nbinsx=40,
        # marker=dict(
        #     color=df["fraud_probability"].apply(
        #         lambda p: f"rgba({int(255*p)},{int(255*(1-p))},60,0.7)"
        #     ).tolist()[0] if len(df) else "#00c8ff",
        #     colorscale=[
        #         [0.0, "rgba(0,255,100,0.7)"],
        #         [0.5, "rgba(255,204,0,0.7)"],
        #         [1.0, "rgba(255,30,60,0.85)"],
        #     ],
        #     color=df["fraud_probability"],
        #     coloraxis="coloraxis",
        # ),
        marker=dict(
            color=df["fraud_probability"],
            coloraxis="coloraxis",
        ),
        name="P(fraud)",
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Fraud Probability Distribution", font=dict(size=12, family="Orbitron")),
        coloraxis=dict(colorscale=[
            [0,"#00ff65"],[0.5,"#ffcc00"],[1,"#ff1e3c"]
        ], showscale=False),
        xaxis_title="P(fraud)",
        yaxis_title="Count",
        bargap=0.05,
    )
    return fig


def chart_fraud_by_type(df: pd.DataFrame) -> go.Figure:
    if df.empty or "type" not in df.columns:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)
    fraud_df = df[df["is_fraud"] == True]
    if fraud_df.empty:
        return go.Figure().update_layout(**PLOTLY_LAYOUT, title="No fraud events yet")
    counts = fraud_df["type"].value_counts().reset_index()
    counts.columns = ["type", "count"]
    fig = go.Figure(go.Pie(
        labels=counts["type"],
        values=counts["count"],
        hole=0.55,
        marker=dict(
            colors=["#ff1e3c","#ff6b35","#ffcc00","#00c8ff","#00ff65"],
            line=dict(color="#020408", width=2),
        ),
        textfont=dict(family="Share Tech Mono", size=10),
    ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Fraud by Transaction Type", font=dict(size=12, family="Orbitron")),
        height=320,
    )
    return fig


def chart_hourly_heatmap(df: pd.DataFrame) -> go.Figure:
    if df.empty or "hourOfDay" not in df.columns:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)
    fraud_df = df[df["is_fraud"] == True]
    if fraud_df.empty:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)

    # Group by hour and type
    pivot = fraud_df.groupby(["hourOfDay", "type"]).size().reset_index(name="count")
    matrix = pivot.pivot(index="type", columns="hourOfDay", values="count").fillna(0)

    fig = go.Figure(go.Heatmap(
        z=matrix.values,
        x=[f"{h:02d}h" for h in matrix.columns],
        y=matrix.index.tolist(),
        colorscale=[
            [0.0, "rgba(0,200,255,0.05)"],
            [0.4, "rgba(255,204,0,0.4)"],
            [1.0, "rgba(255,30,60,0.9)"],
        ],
        showscale=True,
        colorbar=dict(
            tickfont=dict(family="Share Tech Mono", size=9, color="#7a9bb5"),
            thickness=10, len=0.8,
        ),
    ))
    fig.update_layout(**PLOTLY_LAYOUT)
    fig.update_layout(
        title=dict(text="Attack Heatmap — Hour × Type", font=dict(size=12, family="Orbitron")),
        height=280,
        xaxis=dict(tickfont=dict(size=9)),
        yaxis=dict(tickfont=dict(size=9)),
    )
    return fig


def chart_fraud_trend(df: pd.DataFrame) -> go.Figure:
    if df.empty or "processed_at" not in df.columns:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)
    fraud_df = df[df["is_fraud"] == True].copy()
    all_ts = df.set_index("processed_at")["amount"].resample("15s").count().reset_index()
    all_ts.columns = ["time", "all"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=all_ts["time"], y=all_ts["all"],
        name="All Transactions",
        line=dict(color="rgba(0,200,255,0.35)", width=1),
        fill="tozeroy",
        fillcolor="rgba(0,200,255,0.04)",
    ))
    if not fraud_df.empty:
        fraud_ts = fraud_df.set_index("processed_at")["amount"].resample("15s").count().reset_index()
        fraud_ts.columns = ["time", "fraud"]
        fig.add_trace(go.Scatter(
            x=fraud_ts["time"], y=fraud_ts["fraud"],
            name="Fraud Attempts",
            line=dict(color="#ff1e3c", width=2),
            fill="tozeroy",
            fillcolor="rgba(255,30,60,0.12)",
        ))
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Attack Trend — 15s Bins", font=dict(size=12, family="Orbitron")),
    )
    return fig


def chart_gauge(value: float, title: str, max_val: float = 100,
                color_thresholds=None) -> go.Figure:
    if color_thresholds is None:
        color_thresholds = [
            [0,   "rgba(0,255,100,0.8)"],
            [0.6, "rgba(255,204,0,0.8)"],
            [0.85,"rgba(255,30,60,0.9)"],
        ]
    steps = []
    for i, (pct, col) in enumerate(color_thresholds):
        end_pct = color_thresholds[i+1][0] if i+1 < len(color_thresholds) else 1.0
        steps.append(dict(range=[pct*max_val, end_pct*max_val], color=col))

    pct = value / max_val
    bar_color = "#00ff65" if pct < 0.6 else "#ffcc00" if pct < 0.85 else "#ff1e3c"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title=dict(text=title, font=dict(family="Orbitron", size=11, color="#7a9bb5")),
        gauge=dict(
            axis=dict(range=[0, max_val],
                      tickfont=dict(family="Share Tech Mono", size=9, color="#7a9bb5"),
                      tickcolor="#7a9bb5"),
            bar=dict(color=bar_color, thickness=0.22),
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,200,255,0.1)",
            borderwidth=1,
            steps=steps,
            threshold=dict(
                line=dict(color="#ff1e3c", width=2),
                thickness=0.8,
                value=max_val * 0.9,
            ),
        ),
        number=dict(
            font=dict(family="Orbitron", size=22, color=bar_color),
            suffix="%" if max_val == 100 else "ms",
        ),
    ))
    # fig.update_layout(
    #     **PLOTLY_LAYOUT,
    #     height=230,
    #     margin=dict(l=20, r=20, t=40, b=10),
    # )
    fig.update_layout(**PLOTLY_LAYOUT)
    fig.update_layout(
        height=230,
        margin=dict(l=20, r=20, t=40, b=10)
    )
    return fig


def chart_infra_timeseries(ts_list, cpu_list, ram_list, lat_list) -> go.Figure:
    if not ts_list:
        return go.Figure().update_layout(**PLOTLY_LAYOUT)
    ts  = list(ts_list)
    cpu = list(cpu_list)
    ram = list(ram_list)
    lat = list(lat_list)

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=("CPU & RAM (%)", "Latency (ms)"),
                        vertical_spacing=0.14)
    fig.add_trace(go.Scatter(x=ts, y=cpu, name="CPU",
                             line=dict(color="#ff1e3c", width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=ts, y=ram, name="RAM",
                             line=dict(color="#00c8ff", width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=ts, y=lat, name="Latency",
                             line=dict(color="#ffcc00", width=1.5),
                             fill="tozeroy",
                             fillcolor="rgba(255,204,0,0.06)"), row=2, col=1)
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(text="Infrastructure Metrics — Live", font=dict(size=12, family="Orbitron")),
        height=350,
        showlegend=True,
    )
    for ann in fig.layout.annotations:
        ann.font.family = "Orbitron"
        ann.font.size   = 10
        ann.font.color  = "#7a9bb5"
    return fig


# ══════════════════════════════════════════════════════════════════════════════
#  EVENT FEED HTML
# ══════════════════════════════════════════════════════════════════════════════
def render_event_feed(df: pd.DataFrame) -> str:
    if df.empty:
        return '<div class="event-feed"><span style="color:#7a9bb5;font-family:\'Share Tech Mono\'">Waiting for stream…</span></div>'

    rows = df.head(MAX_FEED_ROWS)
    html = '<div class="event-feed">'
    for _, row in rows.iterrows():
        ts_str = row["processed_at"].strftime("%H:%M:%S") if pd.notna(row.get("processed_at")) else "—"
        tx_id  = row.get("_id", "—")
        amt    = fmt_currency(row.get("amount", 0))
        tx_type= row.get("type", "—")
        fp     = float(row.get("fraud_probability", 0))
        is_fr  = bool(row.get("is_fraud", False))

        if is_fr:
            badge = '<span class="feed-badge-fraud">FRAUD</span>'
        elif fp > 0.4:
            badge = '<span class="feed-badge-suspect">SUSPECT</span>'
        else:
            badge = '<span class="feed-badge-ok">OK</span>'

        html += f"""
        <div class="feed-row">
          <span class="feed-ts">{ts_str}</span>
          <span class="feed-type" style="color:#7a9bb5;font-size:0.65rem">{tx_id}</span>
          <span class="feed-amt">{amt}</span>
          <span class="feed-type">{tx_type}</span>
          {badge}
          <span style="color:#7a9bb5;font-size:0.62rem;margin-left:auto">P={fp:.2%}</span>
        </div>"""
    html += "</div>"
    return html


# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD HEADER
# ══════════════════════════════════════════════════════════════════════════════
def render_header(df: pd.DataFrame):
    total  = st.session_state.get("total_processed", 0)
    window_size = len(df)
    fraud_rate = (df["is_fraud"].sum() / window_size * 100) if window_size > 0 else 0
    status_color = "#ff1e3c" if fraud_rate > 10 else "#00ff65"
    status_text  = "ALERT — HIGH FRAUD" if fraud_rate > 10 else "OPERATIONAL"
    now_str = datetime.utcnow().strftime("%Y-%m-%d  %H:%M:%S UTC")

    st.markdown(f"""
    <div class="sentinel-header">
      <div>
        <div class="sentinel-logo">🛡 SENTINEL</div>
        <div class="sentinel-subtitle">Fraud Detection System · Enterprise · v2.0</div>
      </div>
      <div style="margin-left:3rem;font-family:'Share Tech Mono';font-size:0.72rem;color:#7a9bb5">
        <div>Stream: <span style="color:#00c8ff">Redpanda @ localhost:19092</span></div>
        <div>DB: <span style="color:#00c8ff">PostgreSQL @ localhost:5432</span></div>
        <div>Topic: <span style="color:#00c8ff">fraud-results</span></div>
      </div>
      <div class="sentinel-status" style="margin-left:auto;flex-direction:column;align-items:flex-end;gap:0.3rem">
        <div style="display:flex;align-items:center;gap:0.5rem">
          <div class="status-dot" style="background:{status_color};box-shadow:0 0 8px {status_color}"></div>
          <span style="color:{status_color};font-size:0.72rem">{status_text}</span>
        </div>
        <span style="color:#7a9bb5;font-size:0.65rem">{now_str}</span>
        <span style="color:#7a9bb5;font-size:0.65rem">
          <b style="color:#e8f4ff">{total:,}</b> events · Fraud rate (2k): <span style="color:{status_color}">{fraud_rate:.1f}%</span>
        </span>
      </div>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 1 — BUSINESS
# ══════════════════════════════════════════════════════════════════════════════
def tab_business(df: pd.DataFrame):
    # ── KPIs ────────────────────────────────────────────────────────────
    total_vol = st.session_state.get("total_volume", 0.0)
    capital_saved   = st.session_state.get("total_capital_saved", 0.0)
    risk_amount     = st.session_state.get("total_risk_amount", 0.0)
    total_tx        = st.session_state.get("total_processed", 0)
    avg_ticket      = df["amount"].mean() if not df.empty else 0
    avg_fraud_ticket= df[df["is_fraud"]==True]["amount"].mean() if (not df.empty and df["is_fraud"].any()) else 0

    kpis_html = f"""
    <div class="kpi-grid">
      {kpi_card("Total Volume Processed",  fmt_currency(total_vol),    "cyan",   f"{total_tx:,} transactions")}
      {kpi_card("Capital Protected",        fmt_currency(capital_saved),"green",  "True Positives blocked")}
      {kpi_card("Amount at Risk (FN)",      fmt_currency(risk_amount),  "red",    "False Negatives — missed fraud")}
      {kpi_card("Avg Ticket — General",     fmt_currency(avg_ticket),   "",       "")}
      {kpi_card("Avg Ticket — Fraud",       fmt_currency(avg_fraud_ticket), "yellow", "vs normal")}
    </div>"""
    st.markdown(kpis_html, unsafe_allow_html=True)

    # ── Volume chart + Scatter ───────────────────────────────────────────
    col1, col2 = st.columns([1, 1])
    with col1:
        section_title("Volume Timeline", "📈")
        st.plotly_chart(chart_volume_timeseries(df), use_container_width=True, config={"displayModeBar": False})
    with col2:
        section_title("Live Transaction Scatter", "⬤")
        #st.plotly_chart(chart_scatter_live(df), use_container_width=True, config={"displayModeBar": False})
        st.plotly_chart(chart_scatter_live(df), use_container_width=True, config={"displayModeBar": False}, key="scatter_live_business_tab")

    # ── Event Feed ───────────────────────────────────────────────────────
    section_title("Event Feed", "⚡")
    st.markdown(render_event_feed(df), unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 2 — INFRA / OPERATION
# ══════════════════════════════════════════════════════════════════════════════
def tab_infra():
    cpu_list = list(st.session_state.infra_cpu)
    ram_list = list(st.session_state.infra_ram)
    lat_list = list(st.session_state.infra_latency)
    ts_list  = list(st.session_state.infra_ts)

    cpu_val  = cpu_list[-1] if cpu_list else 35.0
    ram_val  = ram_list[-1] if ram_list else 52.0
    lat_val  = lat_list[-1] if lat_list else 12.0
    engines  = st.session_state.infra_engines

    # Auto-scale simulation
    if cpu_val > 80 and engines < 8:
        st.session_state.infra_engines += 1
    elif cpu_val < 40 and engines > 2:
        st.session_state.infra_engines -= 1

    # ── KPIs ────────────────────────────────────────────────────────────
    kpis_html = f"""
    <div class="kpi-grid">
      {kpi_card("Inference Engines (HPA)", str(engines), "cyan",   "Pods active")}
      {kpi_card("CPU Cluster",   f"{cpu_val:.1f}%", "red" if cpu_val>80 else "green", "All pods avg")}
      {kpi_card("RAM Cluster",   f"{ram_val:.1f}%", "yellow" if ram_val>70 else "",   "Heap allocated")}
      {kpi_card("P99 Latency",   f"{lat_val:.1f}ms","yellow" if lat_val>20 else "green", "Per transaction")}
      {kpi_card("Throughput",    f"{random.randint(800,1400)} tx/s", "cyan", "Current RPS")}
    </div>"""
    st.markdown(kpis_html, unsafe_allow_html=True)

    # ── Gauges ──────────────────────────────────────────────────────────
    section_title("Resource Saturation", "⚙")
    g1, g2, g3 = st.columns(3)
    with g1:
        st.plotly_chart(chart_gauge(cpu_val, "CPU Utilization"), use_container_width=True, config={"displayModeBar": False})
    with g2:
        st.plotly_chart(chart_gauge(ram_val, "RAM Utilization"), use_container_width=True, config={"displayModeBar": False})
    with g3:
        st.plotly_chart(chart_gauge(min(lat_val, 100), "P99 Latency", max_val=100,
                                    color_thresholds=[[0,"rgba(0,255,100,0.8)"],[0.3,"rgba(255,204,0,0.8)"],[0.6,"rgba(255,30,60,0.9)"]]),
                        use_container_width=True, config={"displayModeBar": False})

    # ── Time-series ──────────────────────────────────────────────────────
    section_title("Infrastructure Metrics Timeline", "📡")
    st.plotly_chart(chart_infra_timeseries(ts_list, cpu_list, ram_list, lat_list),
                    use_container_width=True, config={"displayModeBar": False})

    # ── HPA info ────────────────────────────────────────────────────────
    section_title("HPA Auto-Scale Logic", "🔄")
    st.markdown(f"""
    <div style="background:rgba(0,200,255,0.04);border:1px solid rgba(0,200,255,0.1);
                border-radius:8px;padding:1rem;font-family:'Share Tech Mono';font-size:0.72rem;
                color:#7a9bb5;line-height:1.9">
      <span style="color:#00c8ff">► RULE:</span> ScaleOut when CPU &gt; 80% (2min window) — ScaleIn when CPU &lt; 40%<br>
      <span style="color:#00c8ff">► MIN_REPLICAS:</span> <span style="color:#e8f4ff">2</span>  &nbsp;
      <span style="color:#00c8ff">MAX_REPLICAS:</span> <span style="color:#e8f4ff">8</span><br>
      <span style="color:#00c8ff">► CURRENT:</span> <span style="color:#{'ff1e3c' if cpu_val>80 else '00ff65'}">
        {engines} pods — {'SCALING OUT ↑' if cpu_val>80 else 'STABLE' if cpu_val>40 else 'SCALING IN ↓'}
      </span><br>
      <span style="color:#00c8ff">► LAST ACTION:</span> <span style="color:#e8f4ff">{datetime.utcnow().strftime('%H:%M:%S UTC')}</span>
    </div>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 3 — ML METRICS
# ══════════════════════════════════════════════════════════════════════════════
def tab_ml(df: pd.DataFrame):

    tp = st.session_state.get("total_tp", 0)
    tn = st.session_state.get("total_tn", 0)
    fp = st.session_state.get("total_fp", 0)
    fn = st.session_state.get("total_fn", 0)

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall    = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1        = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    fpr       = fp / (fp + tn) if (fp + tn) > 0 else 0
    total_hist = tp + tn + fp + fn
    accuracy   = (tp + tn) / total_hist if total_hist > 0 else 0.0
    

    kpis_html = f"""
    <div class="kpi-grid">
      {kpi_card("Recall (Sensitivity)",  f"{recall:.1%}",    "green",  "TP / (TP+FN)")}
      {kpi_card("Precision",             f"{precision:.1%}", "cyan",   "TP / (TP+FP)")}
      {kpi_card("F1-Score",              f"{f1:.1%}",        "yellow", "Harmonic mean")}
      {kpi_card("False Positive Rate",   f"{fpr:.1%}",       "red",    "FP / (FP+TN)")}
      {kpi_card("Accuracy",              f"{accuracy:.1%}",  "",       "Overall")}
    </div>"""
    st.markdown(kpis_html, unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        section_title("Confusion Matrix - Last 2K", "🔲")
        st.plotly_chart(chart_confusion_matrix(df), use_container_width=True, config={"displayModeBar": False}, key="cm_ml_tab")
    with col2:
        section_title("Fraud Probability Distribution", "📊")
        st.plotly_chart(chart_fraud_prob_histogram(df), use_container_width=True, config={"displayModeBar": False}, key="hist_ml_tab")

    section_title("Confusion Matrix Detail", "📋")
    detail_html = f"""
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-top:0.5rem">
      <div style="background:rgba(0,255,100,0.06);border:1px solid rgba(0,255,100,0.2);
                  border-radius:10px;padding:1rem;font-family:'Share Tech Mono';font-size:0.8rem">
        <div style="color:#00ff65;font-size:0.65rem;letter-spacing:0.2em;margin-bottom:0.5rem">TRUE POSITIVE — HIT</div>
        <div style="font-size:1.6rem;font-family:'Orbitron';color:#00ff65">{tp:,}</div>
        <div style="color:#7a9bb5;font-size:0.65rem;margin-top:0.3rem">Fraud correctly detected</div>
      </div>
      <div style="background:rgba(255,30,60,0.06);border:1px solid rgba(255,30,60,0.2);
                  border-radius:10px;padding:1rem;font-family:'Share Tech Mono';font-size:0.8rem">
        <div style="color:#ff1e3c;font-size:0.65rem;letter-spacing:0.2em;margin-bottom:0.5rem">FALSE NEGATIVE — LOSS</div>
        <div style="font-size:1.6rem;font-family:'Orbitron';color:#ff1e3c">{fn:,}</div>
        <div style="color:#7a9bb5;font-size:0.65rem;margin-top:0.3rem">Fraud missed — financial risk</div>
      </div>
      <div style="background:rgba(255,204,0,0.06);border:1px solid rgba(255,204,0,0.2);
                  border-radius:10px;padding:1rem;font-family:'Share Tech Mono';font-size:0.8rem">
        <div style="color:#ffcc00;font-size:0.65rem;letter-spacing:0.2em;margin-bottom:0.5rem">FALSE POSITIVE — FRICTION</div>
        <div style="font-size:1.6rem;font-family:'Orbitron';color:#ffcc00">{fp:,}</div>
        <div style="color:#7a9bb5;font-size:0.65rem;margin-top:0.3rem">Legit tx blocked — UX cost</div>
      </div>
      <div style="background:rgba(0,200,255,0.06);border:1px solid rgba(0,200,255,0.15);
                  border-radius:10px;padding:1rem;font-family:'Share Tech Mono';font-size:0.8rem">
        <div style="color:#00c8ff;font-size:0.65rem;letter-spacing:0.2em;margin-bottom:0.5rem">TRUE NEGATIVE — OK</div>
        <div style="font-size:1.6rem;font-family:'Orbitron';color:#00c8ff">{tn:,}</div>
        <div style="color:#7a9bb5;font-size:0.65rem;margin-top:0.3rem">Legit tx correctly passed</div>
      </div>
    </div>"""
    st.markdown(detail_html, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 4 — EDA
# ══════════════════════════════════════════════════════════════════════════════
def tab_eda(df: pd.DataFrame):
    df_hist = load_postgres_history(limit=50000)
    df = df_hist if not df_hist.empty else df

    if df.empty:
        st.info("Awaiting data from stream or database…")
        return
    origen = "PostgreSQL (Historical)" if not df_hist.empty else "RAM (Live Buffer)"
    st.markdown(f"""
        <div style='color:#00c8ff; font-family:"Share Tech Mono"; font-size:0.75rem; 
                    margin-bottom:1rem; border-left: 2px solid #00c8ff; padding-left: 8px;'>
            ► EDA DATA SOURCE: <b style='color:#e8f4ff'>{origen}</b> — Analyzing {len(df):,} records
        </div>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        section_title("Fraud by Transaction Type", "")
        st.plotly_chart(chart_fraud_by_type(df), use_container_width=True, config={"displayModeBar": False})
    with col2:
        section_title("Attack Heatmap — Hour × Type", "🌡")
        st.plotly_chart(chart_hourly_heatmap(df), use_container_width=True, config={"displayModeBar": False})

    section_title("Fraud Attempt Trend", "📉")
    st.plotly_chart(chart_fraud_trend(df), use_container_width=True, config={"displayModeBar": False})

    section_title("Balance Anomaly Distribution", "💰")
    # Compute balance delta
    if "oldbalanceOrg" in df.columns and "newbalanceOrig" in df.columns:
        df2 = df.copy()
        df2["balance_delta"] = df2["oldbalanceOrg"] - df2["newbalanceOrig"]
        df2["delta_diff"]    = df2["balance_delta"] - df2["amount"]
        fig = go.Figure()
        fig.add_trace(go.Histogram(
            x=df2[df2["is_fraud"]==False]["delta_diff"].clip(-5000, 5000),
            name="Normal", nbinsx=60,
            marker_color="rgba(0,255,100,0.5)",
        ))
        fig.add_trace(go.Histogram(
            x=df2[df2["is_fraud"]==True]["delta_diff"].clip(-5000, 5000),
            name="Fraud", nbinsx=60,
            marker_color="rgba(255,30,60,0.6)",
        ))
        fig.update_layout(
            **PLOTLY_LAYOUT,
            barmode="overlay",
            title=dict(text="Balance Delta Anomaly (oldBalance − newBalance − amount)", font=dict(size=11, family="Orbitron")),
            xaxis_title="Delta ($)",
        )
        st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


# ══════════════════════════════════════════════════════════════════════════════
#  TAB 5 — SOC
# ══════════════════════════════════════════════════════════════════════════════
def tab_soc(df: pd.DataFrame):
    if df.empty:
        st.info("Awaiting events…")
        return

    # ── Critical Alerts (P > 95%) ────────────────────────────────────────
    section_title("Critical Alerts  [P(fraud) > 95%]", "🚨")
    critical = df[df["fraud_probability"] >= FRAUD_PROB_ALERT].copy()

    alert_count = len(critical)
    alert_color = "#ff1e3c" if alert_count > 0 else "#00ff65"
    st.markdown(f"""
    <div style="font-family:'Share Tech Mono';font-size:0.75rem;margin-bottom:0.7rem">
      <span style="color:{alert_color}">{alert_count:,} CRITICAL ALERT{'S' if alert_count!=1 else ''}</span>
      <span style="color:#7a9bb5"> — transactions requiring immediate review</span>
    </div>""", unsafe_allow_html=True)

    if not critical.empty:
        display_cols = [c for c in ["_id","processed_at","amount","type","fraud_probability",
                                    "is_fraud","real_label_Fraud","oldbalanceOrg","newbalanceOrig"]
                        if c in critical.columns]
        st.dataframe(
            critical[display_cols].sort_values("fraud_probability", ascending=False)
                                  .reset_index(drop=True),
            use_container_width=True,
            height=280,
        )
    else:
        st.markdown('<div style="color:#00ff65;font-family:\'Share Tech Mono\';font-size:0.72rem;padding:0.5rem 0">✓ No critical alerts at this moment</div>', unsafe_allow_html=True)

    # ── Divergence Table ─────────────────────────────────────────────────
    section_title("Forensic Divergence Table  [is_fraud ≠ real_label]", "🔍")
    divergent = df[df["is_fraud"] != df["real_label_Fraud"]].copy()
    div_count = len(divergent)
    div_color = "#ffcc00" if div_count > 0 else "#00ff65"
    st.markdown(f"""
    <div style="font-family:'Share Tech Mono';font-size:0.75rem;margin-bottom:0.7rem">
      <span style="color:{div_color}">{div_count:,} DIVERGENCE{'S' if div_count!=1 else ''}</span>
      <span style="color:#7a9bb5"> — label mismatches flagged for audit</span>
    </div>""", unsafe_allow_html=True)

    if not divergent.empty:
        divergent["divergence_type"] = divergent.apply(
            lambda r: "FALSE POSITIVE" if (r["is_fraud"] and not r["real_label_Fraud"])
                      else "FALSE NEGATIVE", axis=1
        )
        display_cols = [c for c in ["_id","processed_at","amount","type",
                                    "fraud_probability","is_fraud","real_label_Fraud","divergence_type"]
                        if c in divergent.columns]
        st.dataframe(
            divergent[display_cols].reset_index(drop=True),
            use_container_width=True,
            height=280,
        )

    # ── Balance Inspector ────────────────────────────────────────────────
    section_title("Balance Anomaly Inspector", "⚖")
    if all(c in df.columns for c in ["oldbalanceOrg", "newbalanceOrig", "amount"]):
        insp = df[df["is_fraud"] == True].copy().head(20)
        if not insp.empty:
            insp["expected_new_bal"] = insp["oldbalanceOrg"] - insp["amount"]
            insp["actual_new_bal"]   = insp["newbalanceOrig"]
            insp["anomaly_gap"]      = (insp["actual_new_bal"] - insp["expected_new_bal"]).abs()
            insp["anomaly_flag"]     = insp["anomaly_gap"].apply(
                lambda x: "⚠ HIGH" if x > 1000 else ("◦ LOW" if x > 100 else "✓ NORMAL")
            )
            display_cols = [c for c in ["_id","amount","oldbalanceOrg","expected_new_bal",
                                        "actual_new_bal","anomaly_gap","anomaly_flag"]
                            if c in insp.columns]
            st.dataframe(insp[display_cols].reset_index(drop=True),
                         use_container_width=True, height=320)

            fig = go.Figure()
            idx = list(range(len(insp)))
            fig.add_trace(go.Bar(x=idx, y=insp["oldbalanceOrg"],
                                 name="Old Balance", marker_color="rgba(0,200,255,0.6)"))
            fig.add_trace(go.Bar(x=idx, y=insp["amount"],
                                 name="Amount", marker_color="rgba(255,30,60,0.7)"))
            fig.add_trace(go.Bar(x=idx, y=insp["actual_new_bal"],
                                 name="New Balance", marker_color="rgba(0,255,100,0.5)"))
            fig.update_layout(
                **PLOTLY_LAYOUT,
                barmode="group",
                title=dict(text="Balance Jump Inspection — Fraud Transactions", font=dict(size=11, family="Orbitron")),
                height=300,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
        else:
            st.markdown('<div style="color:#7a9bb5;font-family:\'Share Tech Mono\';font-size:0.72rem">No fraud transactions to inspect yet</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  SIDEBAR  — Controls
# ══════════════════════════════════════════════════════════════════════════════
def render_sidebar():
    with st.sidebar:
        st.markdown("""
        <div style="font-family:'Orbitron';font-size:0.9rem;color:#ff1e3c;
                    letter-spacing:0.15em;margin-bottom:1rem">
          SENTINEL CONTROLS
        </div>""", unsafe_allow_html=True)

        if not st.session_state.kafka_running:
            if st.button("▶  Start Stream", type="primary", use_container_width=True):
                start_kafka_consumer()
                st.rerun()
        else:
            if st.button("⏹  Stop Stream", use_container_width=True):
                st.session_state.kafka_running = False
                st.rerun()

        st.markdown("---")
        st.markdown(f"""
        <div style="font-family:'Share Tech Mono';font-size:0.65rem;color:#7a9bb5;line-height:2">
          <b style="color:#00c8ff">EVENTS</b>: {st.session_state.total_processed:,}<br>
          <b style="color:#00c8ff">BUFFER</b>: {len(st.session_state.tx_buffer):,} / {MAX_LIVE_ROWS}<br>
          <b style="color:#00c8ff">ENGINES</b>: {st.session_state.infra_engines}<br>
          <b style="color:#00c8ff">STATUS</b>:
          <span style="color:{'#00ff65' if st.session_state.kafka_running else '#ff1e3c'}">
            {'● LIVE' if st.session_state.kafka_running else '○ STOPPED'}
          </span>
        </div>""", unsafe_allow_html=True)

        st.markdown("---")
        refresh_rate = st.slider("Refresh interval (s)", 1, 10, 3, key="refresh_slider")
        if st.button("🗑 Clear Buffer", use_container_width=True):
            st.session_state.tx_buffer.clear()
            st.session_state.infra_cpu.clear()
            st.session_state.infra_ram.clear()
            st.session_state.infra_latency.clear()
            st.session_state.infra_ts.clear()
            st.session_state.total_processed = 0
            st.rerun()
        return refresh_rate


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN RENDER LOOP
# ══════════════════════════════════════════════════════════════════════════════
@st.fragment(run_every="0.5s")
def live_dashboard_ui():

    # Obtain current dataframe snapshot
    df = buffer_to_df()

    # ── Header ──────────────────────────────────────────────────────────
    render_header(df)
    
    # ── Tabs ────────────────────────────────────────────────────────────
    tab_labels = [
        "📊  Business",
        "⚙  Operation",
        "🤖  ML Metrics",
        "🔬  EDA",
        "🛡  SOC",
    ]
    tabs = st.tabs(tab_labels)

    with tabs[0]: tab_business(df)
    with tabs[1]: tab_infra()
    with tabs[2]: tab_ml(df)
    with tabs[3]: tab_eda(df)
    with tabs[4]: tab_soc(df)

def main(): 
    render_sidebar()
   
    # Auto-start on first load
    if not st.session_state.kafka_running:
        start_kafka_consumer()

    # Optionally merge PostgreSQL historical data (first load)
    # Uncomment below to enable historical bootstrapping:
    # hist_df = load_postgres_history(2000)
    # if not hist_df.empty and not df.empty:
    #     df = pd.concat([hist_df, df], ignore_index=True).drop_duplicates()
    # elif not hist_df.empty:
    #     df = hist_df
    live_dashboard_ui()


if __name__ == "__main__":
    main()