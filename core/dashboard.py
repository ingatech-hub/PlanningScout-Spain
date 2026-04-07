import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import re

# ════════════════════════════════════════════════════════════
# PAGE CONFIG — must be first Streamlit call
# ════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PlanningScout — Madrid Intelligence",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ════════════════════════════════════════════════════════════
# SHEET ID — edit this to match your Google Sheet
# ════════════════════════════════════════════════════════════
SHEET_ID = st.secrets.get("SHEET_ID", "")

# ════════════════════════════════════════════════════════════
# STYLING
# ════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* Hide Streamlit branding */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Main background */
.main { background-color: #f8f9fc; }

/* Header banner */
.ps-header {
    background: linear-gradient(135deg, #1565c0, #0d47a1);
    color: white;
    padding: 20px 28px;
    border-radius: 10px;
    margin-bottom: 20px;
}
.ps-header h1 { margin: 0; font-size: 26px; font-weight: 700; }
.ps-header p  { margin: 6px 0 0; opacity: .85; font-size: 14px; }

/* Metric cards */
.metric-card {
    background: white;
    border-radius: 10px;
    padding: 18px 22px;
    box-shadow: 0 2px 8px rgba(0,0,0,.07);
    text-align: center;
    border-left: 4px solid #1565c0;
}
.metric-card .value { font-size: 32px; font-weight: 700; color: #1565c0; }
.metric-card .label { font-size: 13px; color: #666; margin-top: 4px; }

/* Score badge */
.badge-gold   { background: #e8f5e9; color: #1b5e20; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 700; }
.badge-orange { background: #fff3e0; color: #e65100; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 700; }
.badge-yellow { background: #fffde7; color: #f57f17; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 700; }
.badge-grey   { background: #f5f5f5; color: #757575; padding: 3px 10px; border-radius: 12px; font-size: 12px; font-weight: 700; }

/* Permit type chips */
.chip {
    display: inline-block;
    background: #e3f2fd;
    color: #0d47a1;
    padding: 2px 10px;
    border-radius: 12px;
    font-size: 11px;
    font-weight: 600;
}

/* Profile selector */
.profile-btn {
    width: 100%;
    background: white;
    border: 2px solid #e0e0e0;
    border-radius: 8px;
    padding: 12px;
    cursor: pointer;
    text-align: left;
    margin-bottom: 8px;
}
.profile-btn:hover { border-color: #1565c0; }
.profile-btn.active { border-color: #1565c0; background: #e3f2fd; }

/* Table row hover */
.datarow:hover { background: #f0f7ff !important; }
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS CONNECTION
# ════════════════════════════════════════════════════════════
@st.cache_data(ttl=300)   # refresh every 5 minutes
def load_data():
    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        gc = gspread.authorize(creds)
        sheet_id = st.secrets.get("SHEET_ID", SHEET_ID)
        ws = gc.open_by_key(sheet_id).worksheet("Permits")
        data = ws.get_all_records()
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        st.error(f"Error connecting to Google Sheets: {e}")
        return pd.DataFrame()

def parse_value(v):
    """Parse a value column that may have commas, dots, or be empty."""
    if not v or str(v).strip() in ("", "—", "N/A"):
        return 0.0
    s = str(v).replace("€","").replace(" ","").replace(".","").replace(",",".")
    s = re.sub(r'[^\d.]', '', s)
    try:
        return float(s)
    except:
        return 0.0

def parse_score(v):
    try:
        return int(str(v).strip()) if str(v).strip() else 0
    except:
        return 0

def score_badge(sc):
    if sc >= 65:
        return f'<span class="badge-gold">🟢 {sc} pts</span>'
    elif sc >= 40:
        return f'<span class="badge-orange">🟠 {sc} pts</span>'
    elif sc >= 20:
        return f'<span class="badge-yellow">🟡 {sc} pts</span>'
    else:
        return f'<span class="badge-grey">⚪ {sc} pts</span>'

# ════════════════════════════════════════════════════════════
# CLIENT PROFILES — filter presets per buyer type
# ════════════════════════════════════════════════════════════
PROFILES = {
    "🏙️ Vista General": {
        "description": "Todos los proyectos detectados esta semana",
        "min_score": 0,
        "min_value": 0,
        # Empty list = no type filter applied = show everything
        "permit_types": [],
        "color": "#1565c0",
    },
    "🔧 Instaladores": {
        "description": "Obra mayor, cambios de uso y rehabilitaciones. Ideal para instaladores de ascensores, HVAC y protección contra incendios.",
        "min_score": 0,
        "min_value": 50_000,
        # Broad — installers care about any significant building work
        "permit_types": [
            "obra mayor",           # matches: obra mayor nueva construcción, obra mayor rehabilitación, obra mayor industrial
            "cambio de uso",
            "declaración responsable",
            "licencia primera ocupación",
            "rehabilitación",
            "reforma",
            "urbanización",         # urbanizaciones need all trades
        ],
        "color": "#00838f",
    },
    "📐 Promotores Medianos": {
        "description": "Nuevas construcciones, planes especiales y urbanizaciones. Para gestores de proyecto y promotores inmobiliarios.",
        "min_score": 20,
        "min_value": 200_000,
        "permit_types": [
            "obra mayor",           # any obra mayor qualifies
            "urbanización",
            "plan especial",
            "plan parcial",
            "declaración responsable",
            "cambio de uso",
        ],
        "color": "#6a1b9a",
    },
    "🏢 Gran Constructora / Fondo": {
        "description": "Grandes urbanizaciones, infraestructuras y proyectos >€5M. Para FCC, fondos y grandes promotoras.",
        "min_score": 40,
        "min_value": 1_000_000,
        "permit_types": [
            "urbanización",
            "plan especial",
            "plan parcial",
            "obra mayor industrial",
            "obra mayor nueva construcción",
        ],
        "color": "#b71c1c",
    },
}

# ════════════════════════════════════════════════════════════
# SIDEBAR — profile selector + filters
# ════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🏗️ PlanningScout")
    st.markdown("---")

    st.markdown("**Perfil de cliente**")
    selected_profile = st.radio(
        label="Perfil",
        options=list(PROFILES.keys()),
        label_visibility="collapsed",
    )
    prof = PROFILES[selected_profile]
    st.caption(prof["description"])
    st.markdown("---")

    st.markdown("**Filtros adicionales**")
    min_score_override = st.slider(
        "Puntuación mínima", 0, 100,
        value=prof["min_score"], step=5,
    )
    min_value_override = st.number_input(
        "PEM mínimo (€)", value=prof["min_value"],
        min_value=0, step=100_000, format="%d",
    )

    municipios_filter = st.multiselect(
        "Municipio", options=[], placeholder="Todos"
    )
    days_back = st.selectbox(
        "Período", [7, 14, 30, 60, 90],
        index=1, format_func=lambda x: f"Últimos {x} días"
    )

    st.markdown("---")
    if st.button("🔄 Actualizar datos"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Datos: BOCM (registros públicos oficiales)")

# ════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════
st.markdown(f"""
<div class="ps-header">
    <h1>🏗️ PlanningScout — Radar de Proyectos Madrid</h1>
    <p>Detección automática de licencias de obras, urbanizaciones y planes especiales · BOCM · {datetime.now().strftime('%d %B %Y')}</p>
</div>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# LOAD + FILTER DATA
# ════════════════════════════════════════════════════════════
with st.spinner("Cargando proyectos del BOCM…"):
    df_raw = load_data()

if df_raw.empty:
    st.warning("No hay datos en la hoja de cálculo todavía. Ejecuta el scraper primero.")
    st.stop()

# Normalise column names (Sheet headers may vary slightly)
col_map = {
    "Date Granted":             "fecha",
    "Municipality":             "municipio",
    "Full Address":             "direccion",
    "Applicant":                "promotor",
    "Permit Type":              "tipo",
    "Declared Value PEM (€)":  "pem_raw",
    "Est. Build Value (€)":    "est_raw",
    "Maps Link":                "maps",
    "Description":              "descripcion",
    "Source URL":               "bocm_url",
    "PDF URL":                  "pdf_url",
    "Mode":                     "modo",
    "Confidence":               "confianza",
    "Date Found":               "fecha_encontrado",
    "Lead Score":               "score_raw",
    "Expediente":               "expediente",
}
df = df_raw.rename(columns={k: v for k, v in col_map.items() if k in df_raw.columns})

# Parse numerics
df["pem"]   = df["pem_raw"].apply(parse_value)   if "pem_raw" in df.columns else 0.0
df["est"]   = df["est_raw"].apply(parse_value)   if "est_raw" in df.columns else 0.0
df["score"] = df["score_raw"].apply(parse_score) if "score_raw" in df.columns else 0

# Parse date_found
if "fecha_encontrado" in df.columns:
    df["fecha_dt"] = pd.to_datetime(df["fecha_encontrado"].str[:10], errors="coerce")
else:
    df["fecha_dt"] = pd.NaT

# Date filter
cutoff = datetime.now() - timedelta(days=days_back)
df_f = df[df["fecha_dt"] >= cutoff] if "fecha_dt" in df.columns else df.copy()

# Score filter
# Score filter — leads with score=0 may just be unscored, so treat 0 as neutral
df_f = df_f[(df_f["score"] >= min_score_override) | (df_f["score"] == 0)]

# Value filter
df_f = df_f[df_f["pem"] >= min_value_override]

# Permit type filter (profile)
if prof["permit_types"] and "tipo" in df_f.columns:
    pattern = "|".join(re.escape(t) for t in prof["permit_types"])
    df_f = df_f[df_f["tipo"].str.contains(pattern, case=False, na=False)]

# Municipio filter
if municipios_filter and "municipio" in df_f.columns:
    df_f = df_f[df_f["municipio"].isin(municipios_filter)]

# Update municipio options with full dataset
if "municipio" in df.columns:
    all_munis = sorted(df["municipio"].dropna().unique().tolist())
    # Re-render the sidebar multiselect with real options
    # (Streamlit limitation: we set options after load)

# Sort by score descending
df_f = df_f.sort_values("score", ascending=False)

# ════════════════════════════════════════════════════════════
# METRICS ROW
# ════════════════════════════════════════════════════════════
total_pem    = df_f["pem"].sum()
total_est    = df_f["est"].sum()
count        = len(df_f)
high_leads   = len(df_f[df_f["score"] >= 65])

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="metric-card">
        <div class="value">{count}</div>
        <div class="label">Proyectos detectados</div>
    </div>""", unsafe_allow_html=True)
with c2:
    pem_str = f"€{int(total_pem/1_000_000):.0f}M" if total_pem >= 1_000_000 else f"€{int(total_pem/1000):.0f}K"
    st.markdown(f"""<div class="metric-card">
        <div class="value">{pem_str}</div>
        <div class="label">PEM total</div>
    </div>""", unsafe_allow_html=True)
with c3:
    est_str = f"€{int(total_est/1_000_000):.0f}M" if total_est >= 1_000_000 else f"€{int(total_est/1000):.0f}K"
    st.markdown(f"""<div class="metric-card">
        <div class="value">{est_str}</div>
        <div class="label">Valor obra estimado</div>
    </div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="metric-card">
        <div class="value" style="color:#1b5e20">{high_leads}</div>
        <div class="label">🟢 Leads prioritarios (≥65 pts)</div>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# LEADS TABLE
# ════════════════════════════════════════════════════════════
if df_f.empty:
    st.info("No hay proyectos que cumplan los filtros actuales. Prueba a ampliar el período o reducir el PEM mínimo.")
    st.stop()

st.markdown(f"### Proyectos detectados ({count})")
st.caption(f"Período: últimos {days_back} días · Perfil: {selected_profile} · Score ≥ {min_score_override} · PEM ≥ €{min_value_override:,}")

# Build display table row by row
for _, row in df_f.iterrows():
    sc    = int(row.get("score", 0))
    pem   = row.get("pem", 0)
    muni  = str(row.get("municipio", "Madrid"))
    addr  = str(row.get("direccion", ""))
    prom  = str(row.get("promotor", ""))
    tipo  = str(row.get("tipo", ""))
    desc  = str(row.get("descripcion", ""))[:200]
    fecha = str(row.get("fecha", ""))
    maps  = str(row.get("maps", ""))
    bocm  = str(row.get("bocm_url", ""))
    pdf   = str(row.get("pdf_url", ""))
    expd  = str(row.get("expediente", ""))
    conf  = str(row.get("confianza", ""))

    pem_str = f"€{int(pem/1_000_000):.1f}M" if pem >= 1_000_000 else (f"€{int(pem/1000):.0f}K" if pem >= 1000 else ("—" if pem == 0 else f"€{int(pem):,}"))
    badge   = score_badge(sc)

    # Card layout
    with st.container():
        col_score, col_main, col_links = st.columns([1, 6, 1])

        with col_score:
            st.markdown(badge, unsafe_allow_html=True)
            st.caption(pem_str)

        with col_main:
            header = f"**{muni}** · {addr}" if addr else f"**{muni}**"
            st.markdown(header)
            st.markdown(f'<span class="chip">{tipo}</span>', unsafe_allow_html=True)
            if prom:
                st.caption(f"👤 {prom}")
            if desc:
                st.caption(desc)
            if fecha:
                st.caption(f"📅 Concedida: {fecha}" + (f" · Exp: {expd}" if expd else ""))

        with col_links:
            if maps and maps not in ("", "nan"):
                st.markdown(f"[📍 Mapa]({maps})")
            if bocm and bocm not in ("", "nan"):
                st.markdown(f"[📄 BOCM]({bocm})")
            if pdf and pdf not in ("", "nan"):
                st.markdown(f"[📑 PDF]({pdf})")

        st.divider()

# ════════════════════════════════════════════════════════════
# FOOTER
# ════════════════════════════════════════════════════════════
st.markdown("""
<div style="text-align:center;color:#aaa;font-size:12px;margin-top:40px;padding:20px;border-top:1px solid #e0e0e0">
    <strong>PlanningScout</strong> — Inteligencia de proyectos de construcción para Madrid<br>
    Datos extraídos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales<br>
    PEM = Presupuesto de Ejecución Material · Est. Obra = PEM / 0.03
</div>
""", unsafe_allow_html=True)
