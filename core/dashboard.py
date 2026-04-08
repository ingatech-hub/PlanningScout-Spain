import subprocess, sys, base64, os, re, json
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "streamlit", "gspread", "google-auth", "pandas", "-q"])

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# ════════════════════════════════════════════════════════════
# PAGE CONFIG
# ════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PlanningScout Madrid",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ════════════════════════════════════════════════════════════
# GLOBAL CSS
# ════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* Base */
[data-testid="stAppViewContainer"] { background: #f4f6f9; }
[data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid #e8eaed; }
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] { padding-top: 0; }
section.main { padding-top: 0.5rem; }

/* Hide Streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

/* Radio buttons as pill group */
div[role="radiogroup"] { gap: 4px; }
div[role="radiogroup"] label {
    background: #f0f2f5; border: 1.5px solid transparent;
    border-radius: 20px; padding: 6px 14px; cursor: pointer;
    font-size: 13px; font-weight: 500; color: #444;
    transition: all 0.15s ease; margin-bottom: 2px;
    display: block; width: 100%;
}
div[role="radiogroup"] label:hover { background: #e8edf5; border-color: #c0d0e8; }
div[role="radiogroup"] label[data-checked="true"],
div[role="radiogroup"] label:has(input:checked) {
    background: #1e3a5f !important; color: white !important;
    border-color: #1e3a5f !important;
}

/* Cards */
.lead-card {
    background: #ffffff; border-radius: 14px; padding: 20px 22px;
    margin-bottom: 14px; border: 1px solid #e8eaed;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    transition: box-shadow 0.2s ease;
}
.lead-card:hover { box-shadow: 0 4px 16px rgba(0,0,0,0.10); }

/* Score badges */
.score-badge {
    display: inline-flex; align-items: center; justify-content: center;
    width: 52px; height: 52px; border-radius: 50%;
    font-size: 15px; font-weight: 700; color: white;
    flex-shrink: 0;
}
.score-gold    { background: #16a34a; }
.score-good    { background: #c8860a; }
.score-ok      { background: #1e3a5f; }
.score-low     { background: #94a3b8; }

/* Tags */
.tag {
    display: inline-block; border-radius: 20px;
    padding: 3px 12px; font-size: 12px; font-weight: 600;
    margin-right: 6px; margin-top: 4px; white-space: nowrap;
}
.tag-type    { background: #f0f2f5; color: #374151; }
.tag-phase-d { background: #dcfce7; color: #166534; }  /* definitivo = green */
.tag-phase-i { background: #fef9c3; color: #854d0e; }  /* inicial = yellow */
.tag-phase-l { background: #dbeafe; color: #1e40af; }  /* licitacion = blue */
.tag-phase-p { background: #f3f4f6; color: #4b5563; }  /* primera_ocupacion = gray */
.tag-phase-e { background: #f3f4f6; color: #6b7280; }  /* en_tramite = gray */
.tag-pem     { background: #fef3c7; color: #92400e; font-size: 13px; font-weight: 700; }
.tag-muni    { background: #ede9fe; color: #5b21b6; }

/* Card text */
.card-meta   { font-size: 12px; color: #9ca3af; margin-bottom: 6px; }
.card-title  { font-size: 17px; font-weight: 700; color: #111827; margin: 6px 0 10px 0; line-height: 1.35; }
.card-desc   { font-size: 13px; color: #4b5563; margin: 10px 0 8px 0; line-height: 1.5; }
.card-eval   {
    background: #f0f7ff; border-left: 3px solid #3b82f6;
    border-radius: 0 8px 8px 0; padding: 10px 14px;
    font-size: 12.5px; color: #374151; margin: 10px 0 8px 0; line-height: 1.5;
}
.card-supplies {
    background: #f0fdf4; border-left: 3px solid #22c55e;
    border-radius: 0 8px 8px 0; padding: 10px 14px;
    font-size: 12px; color: #374151; margin: 8px 0; line-height: 1.6;
}
.card-detail { font-size: 12px; color: #6b7280; margin: 4px 0; }
.card-detail strong { color: #374151; }

/* Action buttons */
.card-actions { display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; }
.btn-action {
    display: inline-block; padding: 7px 14px; border-radius: 8px;
    font-size: 12px; font-weight: 600; text-decoration: none;
    border: 1.5px solid; cursor: pointer; white-space: nowrap;
}
.btn-bocm   { background: #1e3a5f; color: white; border-color: #1e3a5f; }
.btn-map    { background: white; color: #1e3a5f; border-color: #c0d0e8; }
.btn-pdf    { background: white; color: #dc2626; border-color: #fca5a5; }

/* Stats cards */
.stat-box {
    background: white; border-radius: 12px; padding: 16px 20px;
    text-align: center; border: 1px solid #e8eaed;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
.stat-num  { font-size: 32px; font-weight: 800; color: #1e3a5f; line-height: 1; }
.stat-lbl  { font-size: 12px; color: #6b7280; margin-top: 4px; }

/* Top bar */
.top-bar {
    display: flex; align-items: center; justify-content: space-between;
    padding: 14px 0 18px 0; margin-bottom: 8px;
    border-bottom: 1px solid #e8eaed;
}
.top-bar-left  { font-size: 13px; color: #6b7280; }
.live-dot      { display: inline-block; width: 8px; height: 8px; background: #22c55e;
                 border-radius: 50%; margin-right: 6px; }
.section-title { font-size: 22px; font-weight: 800; color: #111827; margin: 16px 0 4px 0; }
.section-sub   { font-size: 13px; color: #6b7280; margin-bottom: 20px; }

/* Sidebar logo */
.logo-area { text-align: center; padding: 18px 10px 14px 10px; border-bottom: 1px solid #f0f2f5; margin-bottom: 16px; }
.logo-area img { height: 36px; object-fit: contain; }
.logo-area .app-name { font-size: 13px; font-weight: 700; color: #1e3a5f; letter-spacing: 0.05em; margin-top: 6px; }

/* Empty state */
.empty-state {
    text-align: center; padding: 48px 20px;
    background: white; border-radius: 14px; border: 1px dashed #d1d5db;
}
.empty-icon { font-size: 36px; margin-bottom: 12px; }
.empty-title { font-size: 17px; font-weight: 700; color: #374151; }
.empty-sub   { font-size: 13px; color: #9ca3af; margin-top: 6px; }

/* Sidebar section headers */
.sidebar-section {
    font-size: 10px; font-weight: 700; color: #9ca3af;
    letter-spacing: 0.12em; text-transform: uppercase;
    margin: 18px 0 8px 0;
}

/* Profile tip */
.profile-tip {
    background: #fffbeb; border: 1px solid #fde68a;
    border-radius: 8px; padding: 10px 12px;
    font-size: 12px; color: #78350f; margin-top: 8px; line-height: 1.5;
}

/* Count badge */
.count-badge {
    display: inline-block; background: #1e3a5f; color: white;
    border-radius: 20px; padding: 3px 12px; font-size: 13px;
    font-weight: 700; float: right; margin-top: 2px;
}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# LOGO LOADER
# ════════════════════════════════════════════════════════════
@st.cache_data
def load_logo():
    for path in ["core/navbar.png", "navbar.png", "assets/navbar.png"]:
        if os.path.exists(path):
            with open(path, "rb") as f:
                return base64.b64encode(f.read()).decode()
    return None

LOGO_B64 = load_logo()

def logo_img(height=32):
    if LOGO_B64:
        return f'<img src="data:image/png;base64,{LOGO_B64}" style="height:{height}px;object-fit:contain;">'
    return '<span style="font-size:18px;font-weight:800;color:#1e3a5f;">🏗️ PLANNING SCOUT</span>'

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ════════════════════════════════════════════════════════════
SHEET_ID = st.secrets.get("SHEET_ID", "")
HDRS = [
    "Date Granted","Municipality","Full Address","Applicant",
    "Permit Type","Declared Value PEM (€)","Est. Build Value (€)",
    "Maps Link","Description","Source URL","PDF URL",
    "Mode","Confidence","Date Found","Lead Score","Expediente","Phase",
    "AI Evaluation","Supplies Needed",
]

@st.cache_data(ttl=300)
def load_data():
    try:
        sa_json = st.secrets.get("GCP_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "")
        if not sa_json:
            return pd.DataFrame()
        info  = json.loads(sa_json)
        creds = Credentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet("Permits")
        rows = ws.get_all_values()
        if len(rows) < 2:
            return pd.DataFrame()
        header = rows[0]
        data   = rows[1:]
        # Pad rows to at least len(HDRS)
        padded = [r + [""] * max(0, len(HDRS) - len(r)) for r in data]
        df = pd.DataFrame(padded, columns=HDRS[:len(padded[0])] if padded else HDRS)
        return df
    except Exception as e:
        st.error(f"No se pudo conectar con la hoja: {e}")
        return pd.DataFrame()

# ════════════════════════════════════════════════════════════
# PROFILE DEFINITIONS
# ════════════════════════════════════════════════════════════
PROFILES = {
    "instaladores": {
        "label": "🔧 Instaladores MEP",
        "short": "MEP",
        "desc": "Ascensores · HVAC · Climatización · PCI",
        "types": ["obra mayor nueva construcción","obra mayor rehabilitación",
                  "declaración responsable obra mayor","licencia primera ocupación",
                  "urbanización","demolición y nueva planta"],
        "min_pem": 80_000,
        "days": 30,
        "tip": "💡 Un edificio plurifamiliar de 40 viv. = 4 ascensores + HVAC + PCI. Contacta al promotor ANTES de que el contratista cierre contratos.",
    },
    "expansion": {
        "label": "🏪 Expansión Retail",
        "short": "Retail",
        "desc": "Nuevas aperturas · Ubicaciones · Cambios de uso",
        "types": ["urbanización","plan especial","plan especial / parcial",
                  "cambio de uso","licencia de actividad"],
        "min_pem": 0,
        "days": 60,
        "tip": "💡 Urbanización AD-10 Paracuellos = 2.500 viviendas nuevas en 3 años. ¿Ya buscas local?",
    },
    "promotores": {
        "label": "📐 Promotores / RE",
        "short": "Promotores",
        "desc": "Reparcelaciones · Planes parciales · Suelo",
        "types": ["urbanización","plan especial / parcial","plan especial","cambio de uso"],
        "min_pem": 300_000,
        "days": 60,
        "tip": "💡 Reparcelación aprobada hoy = suelo urbanizable. Contacta a la Junta de Compensación antes de que salga al mercado.",
    },
    "constructora": {
        "label": "🏢 Gran Constructora",
        "short": "Constructora",
        "desc": "Licitaciones · Urbanismo · Infraestructuras",
        "types": ["urbanización","licitación de obras","plan especial / parcial",
                  "plan especial","obra mayor nueva construcción","obra mayor industrial"],
        "min_pem": 2_000_000,
        "days": 90,
        "tip": "💡 Las Tablas Oeste €106M PEM → licitación prevista 2026. Prepara equipos técnicos ya.",
    },
    "industrial": {
        "label": "🏭 Industrial / Log.",
        "short": "Industrial",
        "desc": "Naves · Polígonos · Centros de distribución",
        "types": ["obra mayor industrial","urbanización","licitación de obras",
                  "licencia de actividad"],
        "min_pem": 200_000,
        "days": 60,
        "tip": "💡 Nave Alcobendas 8.500m² → demolición + obra nueva en 6 meses. Sé el primero en llamar al promotor.",
    },
    "compras": {
        "label": "🛒 Compras / Materiales",
        "short": "Materiales",
        "desc": "Acero · Hormigón · Fachadas · Instalaciones",
        "types": None,   # all types
        "min_pem": 150_000,
        "days": 30,
        "tip": "💡 Todos los proyectos grandes son tu oportunidad. Preséntate antes de que el constructor adjudique suministros.",
    },
    "general": {
        "label": "🏙️ Vista General",
        "short": "General",
        "desc": "Todos los proyectos sin filtrar",
        "types": None,
        "min_pem": 0,
        "days": 14,
        "tip": "💡 Vista completa de todo lo publicado en el BOCM esta semana.",
    },
}

PHASE_TAG = {
    "definitivo":        ("Aprobación definitiva",  "tag-phase-d"),
    "inicial":           ("Aprobación inicial",     "tag-phase-i"),
    "licitacion":        ("Licitación activa",       "tag-phase-l"),
    "primera_ocupacion": ("1ª Ocupación",            "tag-phase-p"),
    "en_tramite":        ("En trámite",              "tag-phase-e"),
}

TYPE_DISPLAY = {
    "urbanización":                    "Urbanización",
    "plan especial / parcial":         "Plan Parcial / Especial",
    "plan especial":                   "Plan Especial",
    "obra mayor nueva construcción":   "Obra nueva residencial",
    "obra mayor industrial":           "Industrial",
    "obra mayor rehabilitación":       "Rehabilitación",
    "cambio de uso":                   "Cambio de uso",
    "declaración responsable obra mayor": "Decl. Responsable",
    "licencia primera ocupación":      "1ª Ocupación",
    "licencia de actividad":           "Lic. Actividad",
    "licitación de obras":             "Licitación obras",
    "demolición y nueva planta":       "Demo + Nueva planta",
    "obra mayor":                      "Obra mayor",
}

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def parse_pem(val):
    if not val or str(val).strip() in ("","0"): return None
    s = str(val).strip().replace("€","").replace(" ","")
    try:
        if "," in s and "." in s: s = s.replace(".","").replace(",",".")
        elif "," in s: s = s.replace(",",".")
        else: s = s.replace(".","")
        v = float(s)
        return v if 0 < v < 3_000_000_000 else None
    except: return None

def fmt_pem(val):
    v = parse_pem(val)
    if v is None: return None
    if v >= 1_000_000: return f"€{v/1_000_000:.1f}M PEM"
    if v >= 1_000:    return f"€{int(v/1_000)}K PEM"
    return f"€{int(v)} PEM"

def parse_date(s):
    if not s: return None
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]:
        try: return datetime.strptime(str(s)[:10], fmt)
        except: pass
    return None

def score_class(sc):
    try: sc = int(sc)
    except: return "score-low"
    if sc >= 65: return "score-gold"
    if sc >= 40: return "score-good"
    if sc >= 20: return "score-ok"
    return "score-low"

def card_title(row):
    """Generate a punchy title from description, permit type, and address."""
    desc = (str(row.get("Description","")) or "").strip()
    pt   = (str(row.get("Permit Type","")) or "").strip()
    addr = (str(row.get("Full Address","")) or "").strip()
    muni = (str(row.get("Municipality","")) or "").strip()

    # Try to pull key info from description
    if desc and len(desc) > 10:
        # Clean up "Aprobación definitiva: " prefix
        d = re.sub(r'^(?:aprobación definitiva|se concede|se otorga)[:\s]+', '', desc, flags=re.I)
        d = d[:80].strip()
        if len(d) > 8:
            # Capitalise first letter
            d = d[0].upper() + d[1:]
            # Add address suffix if short
            if addr and len(d) < 55:
                d = f"{d} · {addr[:35]}"
            return d[:90]

    # Fallback: permit type + municipality
    t = TYPE_DISPLAY.get(pt.lower(), pt.title())
    return f"{t}{' · ' + addr[:40] if addr else ''}{' · ' + muni if muni and muni not in (addr or '') else ''}"

def filter_df(df, profile_key, period_days, min_pem, min_score):
    prof   = PROFILES[profile_key]
    cutoff = datetime.now() - timedelta(days=period_days)

    mask = pd.Series([True] * len(df))

    # Date filter (use Date Found as the index — when we found it)
    if "Date Found" in df.columns:
        df["_dt"] = df["Date Found"].apply(lambda x: parse_date(str(x)[:10]))
        mask &= df["_dt"].apply(lambda d: d is not None and d >= cutoff)

    # Permit type filter
    if prof["types"]:
        types_lower = [t.lower() for t in prof["types"]]
        mask &= df["Permit Type"].apply(lambda x: str(x).lower().strip() in types_lower)

    # PEM filter
    if min_pem > 0:
        df["_pem"] = df["Declared Value PEM (€)"].apply(parse_pem)
        mask &= df["_pem"].apply(lambda v: v is not None and v >= min_pem)
    else:
        df["_pem"] = df["Declared Value PEM (€)"].apply(parse_pem)

    # Min score filter
    if min_score > 0:
        df["_sc"] = pd.to_numeric(df["Lead Score"], errors="coerce").fillna(0)
        mask &= df["_sc"] >= min_score

    filtered = df[mask].copy()
    # Sort by score desc
    if "Lead Score" in filtered.columns:
        filtered["_sc_sort"] = pd.to_numeric(filtered["Lead Score"], errors="coerce").fillna(0)
        filtered = filtered.sort_values("_sc_sort", ascending=False)
    return filtered

def render_card(row, idx):
    """Render one lead card as HTML."""
    sc    = str(row.get("Lead Score","0") or "0").strip()
    try: sc_int = int(float(sc))
    except: sc_int = 0

    muni  = str(row.get("Municipality","")).strip() or "Madrid"
    addr  = str(row.get("Full Address","")).strip()
    appl  = str(row.get("Applicant","")).strip()
    pt    = str(row.get("Permit Type","")).strip()
    phase = str(row.get("Phase","")).strip().lower()
    date_g= str(row.get("Date Granted","")).strip()
    exp   = str(row.get("Expediente","")).strip()
    conf  = str(row.get("Confidence","")).strip()
    bocm  = str(row.get("Source URL","")).strip()
    pdf   = str(row.get("PDF URL","")).strip()
    maps  = str(row.get("Maps Link","")).strip()
    desc  = str(row.get("Description","")).strip()
    ai_ev = str(row.get("AI Evaluation","") or "").strip()
    sup   = str(row.get("Supplies Needed","") or "").strip()
    pem_v = row.get("_pem") or parse_pem(row.get("Declared Value PEM (€)",""))

    # Format date
    dt_obj = parse_date(date_g)
    date_str = dt_obj.strftime("%-d %b %Y") if dt_obj else date_g[:10]

    # Score badge
    sc_cls   = score_class(sc_int)
    score_html = f'<div class="score-badge {sc_cls}">{sc_int}<br><span style="font-size:9px;font-weight:500">pts</span></div>'

    # Phase tag
    phase_label, phase_cls = PHASE_TAG.get(phase, ("","tag-phase-e"))
    phase_html = f'<span class="tag {phase_cls}">{phase_label}</span>' if phase_label else ""

    # PEM tag
    pem_str  = fmt_pem(pem_v) if pem_v else ""
    pem_html = f'<span class="tag tag-pem">{pem_str}</span>' if pem_str else ""

    # Type tag
    type_display = TYPE_DISPLAY.get(pt.lower(), pt.title() if pt else "")
    type_html    = f'<span class="tag tag-type">{type_display}</span>' if type_display else ""

    # Title
    title = card_title(row)

    # BOCM ID
    bocm_id = ""
    m = re.search(r'BOCM-(\d{8}-\d+)', bocm, re.I)
    if m: bocm_id = m.group(0)

    # Meta line
    bocm_id_str = f"BOCM {bocm_id} · " if bocm_id else ""
    meta = f'{bocm_id_str}<strong>{muni}</strong>'
    if date_str and date_str != "nan":
        meta += f' · {date_str}'

    # Description (cleaned)
    desc_html = ""
    if desc and desc != "nan" and len(desc) > 15:
        d_clean = re.sub(r'^(?:aprobación definitiva[:\s]+|se concede[:\s]+|se otorga[:\s]+)', '', desc, flags=re.I)
        desc_html = f'<div class="card-desc">📋 {d_clean[:260]}</div>'

    # AI Evaluation
    eval_html = ""
    if ai_ev and ai_ev != "nan" and len(ai_ev) > 20:
        eval_html = f'<div class="card-eval">🤖 <strong>Análisis IA:</strong> {ai_ev[:400]}</div>'

    # Supplies needed
    sup_html = ""
    if sup and sup != "nan" and len(sup) > 10:
        sup_html = f'<div class="card-supplies">⚒️ <strong>Materiales estimados:</strong><br>{sup}</div>'

    # Details row
    details = []
    if appl and appl != "nan": details.append(f'<strong>Promotor:</strong> {appl[:50]}')
    if exp  and exp  != "nan": details.append(f'<strong>Exp.:</strong> {exp}')
    if conf and conf != "nan": details.append(f'<strong>Fiabilidad:</strong> {conf.capitalize()}')
    details_html = " &nbsp;·&nbsp; ".join(details)
    details_row  = f'<div class="card-detail">{details_html}</div>' if details_html else ""

    # Action buttons
    btns = []
    if bocm: btns.append(f'<a class="btn-action btn-bocm" href="{bocm}" target="_blank">↗ Ver en BOCM</a>')
    if maps: btns.append(f'<a class="btn-action btn-map" href="{maps}" target="_blank">📍 Mapa</a>')
    if pdf and pdf != bocm: btns.append(f'<a class="btn-action btn-pdf" href="{pdf}" target="_blank">📄 PDF</a>')

    btns_html = f'<div class="card-actions">{"".join(btns)}</div>' if btns else ""

    # Assemble card
    return f"""
<div class="lead-card">
  <div style="display:flex;gap:16px;align-items:flex-start;">
    {score_html}
    <div style="flex:1;min-width:0;">
      <div class="card-meta">{meta}</div>
      <div class="card-title">{title}</div>
      <div style="margin-top:6px;">{type_html}{phase_html}{pem_html}</div>
      {desc_html}
      {eval_html}
      {sup_html}
      {details_row}
      {btns_html}
    </div>
  </div>
</div>
"""

# ════════════════════════════════════════════════════════════
# TOKEN / ACCESS CONTROL (optional — set REQUIRE_TOKEN=true)
# ════════════════════════════════════════════════════════════
def check_access():
    require = str(st.secrets.get("REQUIRE_TOKEN","false")).lower() == "true"
    if not require: return True

    params = st.query_params
    token  = params.get("token","")

    # Check token in secrets [client_tokens] section
    try:
        tokens = dict(st.secrets.get("client_tokens", {}))
        if token in tokens.values() or token in tokens:
            return True
    except: pass

    st.error("🔒 Acceso restringido. Solicita tu enlace de acceso a PlanningScout.")
    st.stop()
    return False

# ════════════════════════════════════════════════════════════
# MAIN APP
# ════════════════════════════════════════════════════════════
check_access()

# ── SIDEBAR ──────────────────────────────────────────────────
with st.sidebar:
    # Logo
    st.markdown(f"""
    <div class="logo-area">
      {logo_img(height=34)}
      <div class="app-name">PLANNING SCOUT</div>
    </div>
    """, unsafe_allow_html=True)

    # Profile selector
    st.markdown('<div class="sidebar-section">PERFIL</div>', unsafe_allow_html=True)
    profile_key = st.radio(
        "perfil",
        options=list(PROFILES.keys()),
        format_func=lambda k: PROFILES[k]["label"],
        label_visibility="collapsed",
        key="profile_radio",
    )

    prof = PROFILES[profile_key]

    # Profile tip
    st.markdown(f'<div class="profile-tip">{prof["tip"]}</div>', unsafe_allow_html=True)

    # Filters
    st.markdown('<div class="sidebar-section">FILTROS</div>', unsafe_allow_html=True)

    # Period
    period_options = {
        "Últimos 7 días":  7,
        "Últimos 14 días": 14,
        "Últimos 30 días": 30,
        "Últimos 60 días": 60,
        "Últimos 90 días": 90,
    }
    default_period = min(prof["days"], 90)
    period_idx = list(period_options.values()).index(
        min(period_options.values(), key=lambda x: abs(x - default_period)))
    period_label = st.selectbox("Período", list(period_options.keys()),
                                index=period_idx, key="period_sel")
    period_days = period_options[period_label]

    # PEM minimum
    pem_default = prof["min_pem"]
    st.markdown("**PEM mínimo (€)**")
    col_minus, col_val, col_plus = st.columns([1,2,1])
    with col_val:
        min_pem = st.number_input("pem", value=pem_default, step=50000,
                                   label_visibility="collapsed", key="min_pem_inp",
                                   min_value=0)
    with col_minus:
        if st.button("−", key="pem_minus", use_container_width=True):
            st.session_state["min_pem_inp"] = max(0, min_pem - 50000)
            st.rerun()
    with col_plus:
        if st.button("+", key="pem_plus", use_container_width=True):
            st.session_state["min_pem_inp"] = min_pem + 50000
            st.rerun()

    # Min score
    min_score = st.slider("Puntuación mínima", 0, 100, 0, 5, key="score_slider")

    # Refresh
    st.markdown('<div class="sidebar-section">DATOS</div>', unsafe_allow_html=True)
    if st.button("🔄 Actualizar datos", use_container_width=True, key="refresh_btn"):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        '<div style="font-size:11px;color:#d1d5db;margin-top:8px;text-align:center;">'
        'Datos del BOCM · Registros públicos<br>Actualizado diariamente</div>',
        unsafe_allow_html=True)

# ── MAIN PANEL ──────────────────────────────────────────────
# Load and filter data
with st.spinner("Cargando datos…"):
    df_all = load_data()

# URL param: ?perfil=expansion (for direct profile links)
url_perfil = st.query_params.get("perfil","")
if url_perfil and url_perfil in PROFILES and url_perfil != profile_key:
    st.session_state["profile_radio"] = url_perfil
    st.rerun()

if df_all.empty:
    st.markdown(f"""
    <div class="empty-state">
      <div class="empty-icon">📡</div>
      <div class="empty-title">Sin datos disponibles</div>
      <div class="empty-sub">Conectando con la base de datos... o añade GCP_SERVICE_ACCOUNT_JSON a los secrets.</div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

df_filtered = filter_df(df_all.copy(), profile_key, period_days, min_pem, min_score)

# ── TOP BAR ──────────────────────────────────────────────────
total_pem = df_filtered["_pem"].sum() if "_pem" in df_filtered else 0
priority   = (df_filtered["Lead Score"].apply(
    lambda x: int(float(str(x).replace(",",".") or 0) if x else 0) >= 65
).sum()) if "Lead Score" in df_filtered else 0

pem_display = (f"€{total_pem/1_000_000:.1f}M" if total_pem >= 1_000_000
               else f"€{int(total_pem):,}" if total_pem > 0 else "N/D")

st.markdown(f"""
<div class="top-bar">
  <div class="top-bar-left">
    <span class="live-dot"></span>
    <strong>EN DIRECTO</strong> &nbsp;·&nbsp; 179+ municipios de Madrid &nbsp;·&nbsp; Leads diarios
  </div>
  <div>
    {logo_img(height=28)}
  </div>
</div>
""", unsafe_allow_html=True)

# ── STAT CARDS ──────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"""<div class="stat-box">
      <div class="stat-num">{len(df_filtered)}</div>
      <div class="stat-lbl">Proyectos detectados</div>
    </div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="stat-box">
      <div class="stat-num" style="color:#c8860a;">{pem_display}</div>
      <div class="stat-lbl">PEM total</div>
    </div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="stat-box">
      <div class="stat-num" style="color:#16a34a;">{priority}</div>
      <div class="stat-lbl">🟢 Prioritarios ≥65 pts</div>
    </div>""", unsafe_allow_html=True)
with c4:
    top_sc = 0
    if "Lead Score" in df_filtered.columns and len(df_filtered):
        top_sc = int(pd.to_numeric(df_filtered["Lead Score"],errors="coerce").max() or 0)
    st.markdown(f"""<div class="stat-box">
      <div class="stat-num">{top_sc} pts</div>
      <div class="stat-lbl">Score más alto</div>
    </div>""", unsafe_allow_html=True)

# ── SECTION HEADER ──────────────────────────────────────────
munis = df_filtered["Municipality"].dropna().unique().tolist()[:4]
munis_str = " · ".join(munis) + ("…" if len(df_filtered["Municipality"].unique()) > 4 else "")

st.markdown(f"""
<div style="margin-top:24px;display:flex;align-items:baseline;justify-content:space-between;">
  <div>
    <div class="section-title">Tus leads esta semana — {len(df_filtered)} proyectos</div>
    <div class="section-sub" style="color:#9ca3af;">{munis_str}</div>
  </div>
  <div>
    <a href="?perfil={profile_key}" style="font-size:12px;color:#1e3a5f;text-decoration:none;">
      🔗 Comparte esta vista
    </a>
  </div>
</div>
""", unsafe_allow_html=True)

# ── EXPORT ──────────────────────────────────────────────────
if len(df_filtered) > 0:
    csv = df_filtered.drop(columns=[c for c in df_filtered.columns if c.startswith("_")],
                           errors="ignore").to_csv(index=False).encode("utf-8")
    st.download_button(
        f"⬇️ Exportar {len(df_filtered)} leads CSV",
        csv,
        f"planningscout_{profile_key}_{datetime.now().strftime('%Y%m%d')}.csv",
        "text/csv",
        key="csv_dl",
    )

# ── LEAD CARDS ──────────────────────────────────────────────
if df_filtered.empty:
    st.markdown(f"""
    <div class="empty-state" style="margin-top:20px;">
      <div class="empty-icon">🔍</div>
      <div class="empty-title">Sin resultados para este perfil</div>
      <div class="empty-sub">
        Prueba a ampliar el período, reducir el PEM mínimo, o cambiar de perfil.<br>
        El BOCM publica nuevas licencias cada día hábil.
      </div>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown(f'<span class="count-badge">{len(df_filtered)} resultados</span>',
                unsafe_allow_html=True)
    for i, (_, row) in enumerate(df_filtered.iterrows()):
        st.markdown(render_card(row, i), unsafe_allow_html=True)

# ── FOOTER ──────────────────────────────────────────────────
st.markdown("""
<div style="margin-top:40px;padding:20px 0;border-top:1px solid #e8eaed;
            text-align:center;font-size:12px;color:#9ca3af;">
  <strong style="color:#1e3a5f;">PlanningScout</strong> &nbsp;·&nbsp;
  Datos del BOCM (Boletín Oficial de la Comunidad de Madrid) &nbsp;·&nbsp;
  Registros públicos oficiales &nbsp;·&nbsp;
  Actualización diaria
</div>
""", unsafe_allow_html=True)
