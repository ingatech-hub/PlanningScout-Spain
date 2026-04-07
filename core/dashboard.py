import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
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
# CLIENT TOKEN AUTH
# Each client gets a URL like:
#   planningscout.streamlit.app?token=instaladores-2024
# You set CLIENT_TOKENS in Streamlit secrets.
# If no token is required (demo mode), set REQUIRE_TOKEN=false.
#
# How to add a new client:
# 1. In Streamlit secrets, add to [client_tokens]:
#    "carlos_vimad" = "expansion-retail"
#    (token → profile key)
# 2. Share with client: planningscout.streamlit.app?token=carlos_vimad
# ════════════════════════════════════════════════════════════

# Read URL query params
qp = st.query_params
url_token   = qp.get("token", "")
url_profile = qp.get("perfil", "")   # direct profile override (for demos)

# Load client token → profile mapping from secrets (optional)
client_tokens = {}
try:
    ct = st.secrets.get("client_tokens", {})
    client_tokens = dict(ct) if ct else {}
except Exception:
    pass

require_token = str(st.secrets.get("REQUIRE_TOKEN", "false")).lower() == "true"

# Resolve which profile is forced by token
forced_profile_key = None
if url_token and url_token in client_tokens:
    forced_profile_key = client_tokens[url_token]
elif url_profile:
    forced_profile_key = url_profile.lower().replace(" ", "_")

# If token required but not supplied/valid → show login wall
if require_token and not forced_profile_key:
    st.markdown("## 🔒 Acceso restringido")
    st.info("Por favor, accede a través del enlace personalizado que te hemos enviado.")
    st.stop()

# ════════════════════════════════════════════════════════════
# SHEET CONFIG
# ════════════════════════════════════════════════════════════
SHEET_ID = st.secrets.get("SHEET_ID", "")

# ════════════════════════════════════════════════════════════
# STYLING
# ════════════════════════════════════════════════════════════
st.markdown("""
<style>
#MainMenu {visibility:hidden;} footer {visibility:hidden;} header {visibility:hidden;}
.main {background:#f8f9fc;}
.ps-header {
    background:linear-gradient(135deg,#1565c0,#0d47a1);
    color:white; padding:20px 28px; border-radius:10px; margin-bottom:20px;
}
.ps-header h1 {margin:0;font-size:24px;font-weight:700;}
.ps-header p  {margin:6px 0 0;opacity:.85;font-size:13px;}
.mcard {
    background:white; border-radius:10px; padding:16px 18px;
    box-shadow:0 2px 8px rgba(0,0,0,.07); text-align:center;
    border-left:4px solid #1565c0; margin-bottom:8px;
}
.mcard .val {font-size:28px;font-weight:700;color:#1565c0;}
.mcard .lbl {font-size:12px;color:#666;margin-top:3px;}
.bg {background:#e8f5e9;color:#1b5e20;padding:3px 9px;border-radius:12px;font-size:11px;font-weight:700;}
.bo {background:#fff3e0;color:#e65100;padding:3px 9px;border-radius:12px;font-size:11px;font-weight:700;}
.by {background:#fffde7;color:#f57f17;padding:3px 9px;border-radius:12px;font-size:11px;font-weight:700;}
.bb {background:#f5f5f5;color:#757575;padding:3px 9px;border-radius:12px;font-size:11px;font-weight:700;}
.chip {display:inline-block;background:#e3f2fd;color:#0d47a1;padding:2px 9px;border-radius:12px;font-size:11px;font-weight:600;}
.tip-box {background:#e8f5e9;border-left:4px solid #43a047;padding:12px 16px;border-radius:6px;font-size:13px;margin-bottom:16px;}
</style>
""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# 7 CLIENT PROFILES — one per buyer segment
# Built from analysis of 80 LinkedIn connections
# ════════════════════════════════════════════════════════════
PROFILES = {

    # ── 1. Instaladores MEP ─────────────────────────────────
    # Elevadores (Orona, ELEVALIA), HVAC (Groupe Atlantic, Geoclima),
    # Protección contra incendios (PCI), Climatización
    # They need: any significant building work → they quote MEP systems
    "🔧 Instaladores MEP": {
        "key":         "instaladores",
        "description": "Ascensores, HVAC, climatización, PCI. Cualquier obra mayor que necesite instalaciones.",
        "tip":         "💡 Contacta al promotor 6-12 meses antes de la obra para entrar en las especificaciones técnicas.",
        "min_score":   0,
        "min_value":   80_000,
        "days_default": 30,
        "permit_types": [
            "obra mayor",         # matches all: nueva construcción, rehabilitación, industrial
            "cambio de uso",
            "declaración responsable",
            "licencia primera ocupación",
            "urbanización",
        ],
        "color":       "#00838f",
        "demo_lead":   "Edificio plurifamiliar de 40 viviendas → 4 ascensores + HVAC + PCI = €800K en instalaciones",
    },

    # ── 2. Expansión Retail / Restauración ──────────────────
    # Carlos (VIMAD), Auto1, Miss Sushi, INMOVERSE, Iner Madrid
    # They need: new neighborhoods = new locations to open franchises/offices
    "🏪 Expansión Retail": {
        "key":         "expansion",
        "description": "Directores de expansión de retail, restauración y servicios. Nuevas zonas comerciales y desarrollos residenciales.",
        "tip":         "💡 Las urbanizaciones aprobadas = nuevos barrios en 2-3 años. Negocia el local ahora antes de que suba el precio.",
        "min_score":   0,
        "min_value":   0,
        "days_default": 60,
        "permit_types": [
            "urbanización",
            "plan especial",
            "plan parcial",
            "cambio de uso",
            "licencia de actividad",
            "obra mayor nueva construcción",
        ],
        "color":       "#e65100",
        "demo_lead":   "Urbanización AD-10 Paracuellos (€74M) → 2.500 nuevas viviendas = 10.000 residentes = tu próxima ubicación",
    },

    # ── 3. Promotores / Real Estate Investment ───────────────
    # Kategora (Pedro), Peterland, SUMA Inmobiliaria, Onhaus
    # They need: land approved for development, rezoning opportunities
    "📐 Promotores / Real Estate": {
        "key":         "promotores",
        "description": "Gestores de proyecto e inversores inmobiliarios. Urbanizaciones, reparcelaciones y nuevas construcciones.",
        "tip":         "💡 Una reparcelación aprobada = suelo urbanizable. Contacta a la Junta de Compensación antes de que salga al mercado.",
        "min_score":   20,
        "min_value":   300_000,
        "days_default": 60,
        "permit_types": [
            "urbanización",
            "plan parcial",
            "plan especial",
            "obra mayor nueva construcción",
            "cambio de uso",
            "declaración responsable obra mayor",
        ],
        "color":       "#6a1b9a",
        "demo_lead":   "Plan Especial López de Hoyos 220 → edificio residencial + comercial en zona prime Madrid",
    },

    # ── 4. Gran Constructora / Infraestructuras ──────────────
    # FCC (Fernando), ACR, Eigo, CALTER, Artelia
    # They need: large projects > €2M to bid as main contractor
    "🏢 Gran Constructora": {
        "key":         "constructora",
        "description": "Grandes constructoras e infraestructuras. Proyectos > €2M listos para licitar.",
        "tip":         "💡 La aprobación definitiva de un plan = licitación en 12-18 meses. Empieza a preparar el dossier técnico ya.",
        "min_score":   35,
        "min_value":   2_000_000,
        "days_default": 90,
        "permit_types": [
            "urbanización",
            "plan especial",
            "plan parcial",
            "obra mayor industrial",
            "obra mayor nueva construcción",
        ],
        "color":       "#b71c1c",
        "demo_lead":   "Proyecto Urbanización Las Tablas Oeste (€106M PEM) → licitación en 2026",
    },

    # ── 5. Industrial / Logística ────────────────────────────
    # Norton edificios industriales, RAFE Demoliciones, Grupo Avintia
    # They need: industrial parks, warehouses, demolition+rebuild
    "🏭 Industrial / Logística": {
        "key":         "industrial",
        "description": "Construcción industrial, logística y demolición. Naves, almacenes y parques empresariales.",
        "tip":         "💡 Una licencia de nave industrial = obra en 3-6 meses. Contacta al promotor para la demolición previa o la ejecución.",
        "min_score":   0,
        "min_value":   200_000,
        "days_default": 60,
        "permit_types": [
            "obra mayor industrial",
            "urbanización",
            "obra mayor nueva construcción",
            "cambio de uso",
        ],
        "color":       "#37474f",
        "demo_lead":   "Nave industrial 8.500m² polígono Alcobendas (€3.2M PEM) → demolición previa + obra nueva",
    },

    # ── 6. Compras / Materiales de Construcción ─────────────
    # Jefes de compras (ITEVELESA, Avintia), METALUSA, proveedores
    # They need: ALL big projects = they supply materials to the winner
    "🛒 Compras / Materiales": {
        "key":         "compras",
        "description": "Proveedores y jefes de compras. Todos los proyectos grandes = oportunidad de suministro.",
        "tip":         "💡 Con el nombre del promotor y el expediente, puedes presentar tus materiales antes de que la constructora gane la obra.",
        "min_score":   0,
        "min_value":   150_000,
        "days_default": 30,
        "permit_types": [],   # ALL types — maximum coverage
        "color":       "#4527a0",
        "demo_lead":   "18 proyectos esta semana = €5.6B en materiales potenciales — acero, cemento, fachada, instalaciones",
    },

    # ── 7. Vista General (admin / demos) ────────────────────
    "🏙️ Vista General": {
        "key":         "general",
        "description": "Todos los proyectos detectados. Sin filtros de tipo.",
        "tip":         "Selecciona un perfil específico para ver solo los leads relevantes para tu negocio.",
        "min_score":   0,
        "min_value":   0,
        "days_default": 14,
        "permit_types": [],
        "color":       "#1565c0",
        "demo_lead":   "",
    },
}

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def parse_value(v):
    if not v or str(v).strip() in ("", "—", "N/A"):
        return 0.0
    s = re.sub(r'[^\d,.]', '', str(v))
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    else:
        s = s.replace(".", "") if s.count(".") > 1 else s
    try:
        return float(s)
    except Exception:
        return 0.0

def parse_score(v):
    try:
        return int(float(str(v).strip())) if str(v).strip() else 0
    except Exception:
        return 0

def fmt_eur(v):
    if v == 0:
        return "—"
    if v >= 1_000_000:
        return f"€{v/1_000_000:.1f}M"
    if v >= 1_000:
        return f"€{int(v/1000)}K"
    return f"€{int(v):,}"

def badge(sc):
    if sc >= 65:
        return f'<span class="bg">🟢 {sc} pts</span>'
    elif sc >= 40:
        return f'<span class="bo">🟠 {sc} pts</span>'
    elif sc >= 20:
        return f'<span class="by">🟡 {sc} pts</span>'
    else:
        return f'<span class="bb">⚪ {sc} pts</span>'

def contact_links(promotor, muni, expediente):
    """Generate action links to find the promotor's contact."""
    links = []
    if promotor and promotor.strip() and promotor.strip() not in ("—", "nan"):
        q = promotor.strip().replace(" ", "+")
        links.append(f"[🔍 Buscar en LinkedIn](https://www.linkedin.com/search/results/all/?keywords={q})")
        links.append(f"[🌐 Google]( https://www.google.com/search?q={q}+contacto+construccion)")
    if muni and muni not in ("—", "nan", "Madrid"):
        ayto_q = f"ayuntamiento+{muni.replace(' ','+').lower()}+urbanismo+licencias+contacto"
        links.append(f"[🏛️ Ayuntamiento {muni}](https://www.google.com/search?q={ayto_q})")
    return "  ·  ".join(links) if links else ""

# ════════════════════════════════════════════════════════════
# LOAD DATA — FIRST, before sidebar (fixes municipio dropdown)
# ════════════════════════════════════════════════════════════
@st.cache_data(ttl=300)
def load_data():
    try:
        sa_info = dict(st.secrets["gcp_service_account"])
        creds = Credentials.from_service_account_info(
            sa_info,
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"],
        )
        gc = gspread.authorize(creds)
        sid = st.secrets.get("SHEET_ID", SHEET_ID)
        ws  = gc.open_by_key(sid).worksheet("Permits")
        data = ws.get_all_records()
        return pd.DataFrame(data) if data else pd.DataFrame()
    except Exception as e:
        st.error(f"Error conectando a Google Sheets: {e}")
        return pd.DataFrame()

COL_MAP = {
    "Date Granted":            "fecha",
    "Municipality":            "municipio",
    "Full Address":            "direccion",
    "Applicant":               "promotor",
    "Permit Type":             "tipo",
    "Declared Value PEM (€)": "pem_raw",
    "Est. Build Value (€)":   "est_raw",
    "Maps Link":               "maps",
    "Description":             "descripcion",
    "Source URL":              "bocm_url",
    "PDF URL":                 "pdf_url",
    "Mode":                    "modo",
    "Confidence":              "confianza",
    "Date Found":              "fecha_encontrado",
    "Lead Score":              "score_raw",
    "Expediente":              "expediente",
}

with st.spinner("Cargando proyectos del BOCM…"):
    df_raw = load_data()

if df_raw.empty:
    st.warning("No hay datos todavía. Ejecuta el scraper primero (`--weeks 8` para backfill).")
    st.stop()

df = df_raw.rename(columns={k: v for k, v in COL_MAP.items() if k in df_raw.columns})
df["pem"]      = df["pem_raw"].apply(parse_value)   if "pem_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["est"]      = df["est_raw"].apply(parse_value)   if "est_raw"          in df.columns else pd.Series(0.0, index=df.index)
df["score"]    = df["score_raw"].apply(parse_score) if "score_raw"        in df.columns else pd.Series(0,   index=df.index)
df["fecha_dt"] = pd.to_datetime(df["fecha_encontrado"].str[:10], errors="coerce") if "fecha_encontrado" in df.columns else pd.NaT

# Build list of unique municipios with counts for the dropdown
all_munis = []
if "municipio" in df.columns:
    mc = df["municipio"].dropna().value_counts()
    all_munis = [f"{m} ({c})" for m, c in mc.items() if str(m).strip()]
    muni_lookup = {f"{m} ({c})": m for m, c in mc.items()}
else:
    muni_lookup = {}

# ════════════════════════════════════════════════════════════
# SIDEBAR — built after data load so municipios are populated
# ════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🏗️ PlanningScout")
    st.markdown(f"*Actualizado: {datetime.now().strftime('%d %b %Y %H:%M')}*")
    st.markdown("---")

    # Profile selector — locked if client has a token
    st.markdown("**Perfil de cliente**")
    profile_names = list(PROFILES.keys())

    if forced_profile_key:
        # Find profile by key
        matched = next(
            (n for n, p in PROFILES.items() if p["key"] == forced_profile_key),
            profile_names[0]
        )
        selected_profile = matched
        st.success(f"Vista personalizada: **{selected_profile}**")
    else:
        selected_profile = st.radio(
            "Perfil", profile_names, label_visibility="collapsed"
        )

    prof = PROFILES[selected_profile]
    st.caption(prof["description"])
    st.markdown("---")

    st.markdown("**Período de búsqueda**")
    days_back = st.selectbox(
        "Período",
        [7, 14, 30, 60, 90],
        index=[7,14,30,60,90].index(prof["days_default"]) if prof["days_default"] in [7,14,30,60,90] else 1,
        format_func=lambda x: f"Últimos {x} días",
        label_visibility="collapsed",
    )
    st.caption("Para backfill de 8 semanas: ejecuta el scraper con `--weeks 8` en GitHub Actions.")

    st.markdown("**Filtros adicionales**")
    min_pem_input = st.number_input(
        "PEM mínimo (€)", value=prof["min_value"],
        min_value=0, step=50_000, format="%d",
    )
    min_score_input = st.slider("Puntuación mínima", 0, 100, value=prof["min_score"], step=5)

    # Municipio — now has real options
    muni_sel_display = st.multiselect(
        "Municipio (filtrar)", options=all_munis, placeholder="Todos los municipios",
    )
    muni_sel = [muni_lookup[m] for m in muni_sel_display if m in muni_lookup]

    st.markdown("---")
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("🔄 Actualizar"):
            st.cache_data.clear()
            st.rerun()
    with col_b:
        st.caption("")  # spacer

    # Show client-specific URL tip (only in admin mode)
    if not forced_profile_key:
        with st.expander("🔗 Compartir con cliente"):
            prof_key = prof["key"]
            base = "https://planningscout.streamlit.app"
            st.code(f"{base}?perfil={prof_key}", language=None)
            st.caption("El cliente ve solo su perfil y no puede cambiar el filtro.")

    st.caption("Datos: BOCM — registros públicos oficiales CM Madrid")

# ════════════════════════════════════════════════════════════
# HEADER
# ════════════════════════════════════════════════════════════
profile_color = prof["color"]
st.markdown(f"""
<div class="ps-header" style="background:linear-gradient(135deg,{profile_color},{profile_color}dd);">
    <h1>🏗️ PlanningScout — {selected_profile}</h1>
    <p>Detección automática de proyectos de construcción · BOCM · {datetime.now().strftime('%d %B %Y')} · Últimos {days_back} días</p>
</div>
""", unsafe_allow_html=True)

# Show profile tip
if prof.get("tip"):
    st.markdown(f'<div class="tip-box">{prof["tip"]}</div>', unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# FILTER DATA
# ════════════════════════════════════════════════════════════
cutoff = datetime.now() - timedelta(days=days_back)
df_f   = df[df["fecha_dt"] >= cutoff].copy() if "fecha_dt" in df.columns else df.copy()

# Score filter — score=0 means unscored (not bad), let through at score 0 threshold
if min_score_input > 0:
    df_f = df_f[(df_f["score"] >= min_score_input) | (df_f["score"] == 0)]

# PEM filter
df_f = df_f[df_f["pem"] >= min_pem_input]

# Permit type filter
if prof["permit_types"] and "tipo" in df_f.columns:
    pattern = "|".join(re.escape(t) for t in prof["permit_types"])
    df_f = df_f[df_f["tipo"].str.contains(pattern, case=False, na=False)]

# Municipio filter
if muni_sel and "municipio" in df_f.columns:
    df_f = df_f[df_f["municipio"].isin(muni_sel)]

# Sort: score desc, then PEM desc
df_f = df_f.sort_values(["score", "pem"], ascending=[False, False]).reset_index(drop=True)

# ════════════════════════════════════════════════════════════
# METRICS
# ════════════════════════════════════════════════════════════
total_pem  = df_f["pem"].sum()
total_est  = df_f["est"].sum()
count      = len(df_f)
high_leads = len(df_f[df_f["score"] >= 65])

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f'<div class="mcard"><div class="val">{count}</div><div class="lbl">Proyectos detectados</div></div>', unsafe_allow_html=True)
with c2:
    st.markdown(f'<div class="mcard"><div class="val">{fmt_eur(total_pem)}</div><div class="lbl">PEM total (coste real obra)</div></div>', unsafe_allow_html=True)
with c3:
    est_val = fmt_eur(total_est) if total_est > 0 else (fmt_eur(total_pem / 0.03) if total_pem > 0 else "—")
    st.markdown(f'<div class="mcard"><div class="val">{est_val}</div><div class="lbl">Valor proyecto estimado</div></div>', unsafe_allow_html=True)
with c4:
    st.markdown(f'<div class="mcard"><div class="val" style="color:#1b5e20">{high_leads}</div><div class="lbl">🟢 Leads prioritarios ≥65 pts</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# EXPORT BUTTON
# ════════════════════════════════════════════════════════════
if not df_f.empty:
    export_cols = ["fecha", "municipio", "direccion", "promotor", "tipo", "pem_raw", "est_raw", "descripcion", "expediente", "bocm_url"]
    export_cols = [c for c in export_cols if c in df_f.columns]
    csv = df_f[export_cols].to_csv(index=False).encode("utf-8")
    st.download_button(
        label=f"⬇️ Exportar {count} leads a CSV",
        data=csv,
        file_name=f"planningscout_{prof['key']}_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        help="Descarga todos los leads filtrados para trabajar con ellos en Excel o CRM.",
    )

# ════════════════════════════════════════════════════════════
# LEAD CARDS
# ════════════════════════════════════════════════════════════
if df_f.empty:
    st.info(f"No hay proyectos que cumplan los filtros actuales para el perfil **{selected_profile}**.")
    st.markdown(f"**Sugerencias:**")
    st.markdown(f"- Amplía el período de búsqueda (actualmente: {days_back} días)")
    st.markdown(f"- Reduce el PEM mínimo (actualmente: {fmt_eur(min_pem_input)})")
    st.markdown(f"- Ejecuta el scraper con `--weeks 8` en GitHub Actions para hacer un backfill de 8 semanas")
    if prof.get("demo_lead"):
        st.markdown(f"**Ejemplo de lead que verás cuando haya datos:**")
        st.success(prof["demo_lead"])
    st.stop()

st.markdown(f"### Proyectos detectados ({count})")
st.caption(f"Período: últimos {days_back} días · Perfil: {selected_profile} · Score ≥ {min_score_input} · PEM ≥ {fmt_eur(min_pem_input)}")

for i, row in df_f.iterrows():
    sc    = int(row.get("score", 0))
    pem   = row.get("pem", 0)
    muni  = str(row.get("municipio", "")) or "Madrid"
    addr  = str(row.get("direccion", "")) or ""
    prom  = str(row.get("promotor", "")) or ""
    tipo  = str(row.get("tipo", "")) or ""
    desc  = str(row.get("descripcion", "")) or ""
    fecha = str(row.get("fecha", "")) or ""
    maps  = str(row.get("maps", "")) or ""
    bocm  = str(row.get("bocm_url", "")) or ""
    pdf   = str(row.get("pdf_url", "")) or ""
    expd  = str(row.get("expediente", "")) or ""
    conf  = str(row.get("confianza", "")) or ""

    # Clean nans
    for var in [addr, prom, tipo, desc, fecha, maps, bocm, pdf, expd, conf]:
        if var == "nan":
            var = ""

    pem_str = fmt_eur(pem)

    with st.container():
        col_score, col_main, col_links = st.columns([1, 6, 1.2])

        with col_score:
            st.markdown(badge(sc), unsafe_allow_html=True)
            st.markdown(f"**{pem_str}**")
            if conf and conf not in ("", "nan"):
                conf_icon = "🟢" if conf == "high" else "🟡" if conf == "medium" else "🔴"
                st.caption(f"{conf_icon} {conf}")

        with col_main:
            title = f"**{muni}**"
            if addr and addr not in ("nan", ""):
                title += f" · {addr[:80]}"
            st.markdown(title)
            if tipo and tipo not in ("nan", ""):
                st.markdown(f'<span class="chip">{tipo}</span>', unsafe_allow_html=True)
            if prom and prom not in ("nan", ""):
                st.caption(f"👤 {prom}")
            if desc and desc not in ("nan", ""):
                st.caption(desc[:220])
            detail_parts = []
            if fecha and fecha not in ("nan",""):
                detail_parts.append(f"📅 {fecha}")
            if expd and expd not in ("nan",""):
                detail_parts.append(f"Exp: {expd}")
            if detail_parts:
                st.caption("  ·  ".join(detail_parts))

            # Contact action links
            c_links = contact_links(prom, muni, expd)
            if c_links:
                st.markdown(c_links)

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
st.markdown(f"""
<div style="text-align:center;color:#aaa;font-size:12px;margin-top:32px;padding:20px;border-top:1px solid #e0e0e0">
    <strong>PlanningScout</strong> — Inteligencia de proyectos de construcción para Madrid<br>
    Datos extraídos del BOCM (Boletín Oficial de la Comunidad de Madrid) · Registros públicos oficiales<br>
    PEM = Presupuesto de Ejecución Material · Est. Proyecto = PEM / 0.03 · {count} proyectos en esta vista
</div>
""", unsafe_allow_html=True)
