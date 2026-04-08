import subprocess, sys, base64, os, re, json, urllib.parse
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "streamlit", "gspread", "google-auth", "pandas", "requests", "-q"])

import streamlit as st
import pandas as pd
import gspread
import requests as http_requests
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta

# ════════════════════════════════════════════════════════════
# PAGE CONFIG — must be first Streamlit call
# ════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="PlanningScout Madrid",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ════════════════════════════════════════════════════════════
# CSS — complete visual rewrite (v2)
# The previous version used classes that Streamlit's markdown
# parser sometimes strips. This version uses 100% inline styles
# inside Python f-strings to guarantee rendering.
# ════════════════════════════════════════════════════════════
st.markdown("""<style>
/* ── Layout ───────────────────────────────────────────── */
[data-testid="stAppViewContainer"] { background:#f0f2f6 }
[data-testid="stSidebar"] {
    background:#ffffff; border-right:1px solid #e2e8f0;
    min-width:260px !important; max-width:280px !important;
}
section.main > div { padding-top:1rem }
#MainMenu, footer, header, .stDeployButton { visibility:hidden; display:none }

/* ── Sidebar radio pills ───────────────────────────────── */
div[role="radiogroup"] > label {
    border-radius:8px; padding:8px 14px; font-size:13px;
    font-weight:500; color:#374151; background:#f8fafc;
    border:1.5px solid #e2e8f0; margin-bottom:4px;
    display:block; cursor:pointer; transition:all .15s;
}
div[role="radiogroup"] > label:hover { background:#eff6ff; border-color:#bfdbfe }
div[role="radiogroup"] > label:has(input:checked) {
    background:#1e3a5f !important; color:white !important;
    border-color:#1e3a5f !important; font-weight:600 !important;
}

/* ── Stat pills ────────────────────────────────────────── */
.stat-pill {
    background:white; border-radius:12px; padding:14px 18px;
    border:1px solid #e2e8f0; box-shadow:0 1px 3px rgba(0,0,0,.06);
}
/* ── Card base ─────────────────────────────────────────── */
.ps-card {
    background:white; border-radius:16px; margin-bottom:16px;
    border:1px solid #e2e8f0; box-shadow:0 2px 8px rgba(0,0,0,.06);
    overflow:hidden; transition:box-shadow .2s;
}
.ps-card:hover { box-shadow:0 6px 20px rgba(0,0,0,.12) }

/* ── Buttons ───────────────────────────────────────────── */
.stButton>button, .stDownloadButton>button {
    border-radius:8px; font-weight:600; font-size:13px;
    padding:6px 16px;
}
</style>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# LOGO
# ════════════════════════════════════════════════════════════
@st.cache_data
def load_logo_b64():
    for p in ["core/navbar.png","navbar.png","assets/navbar.png"]:
        if os.path.exists(p):
            return base64.b64encode(open(p,"rb").read()).decode()
    return None

LOGO = load_logo_b64()
def logo_html(h=30):
    if LOGO:
        return f'<img src="data:image/png;base64,{LOGO}" style="height:{h}px;object-fit:contain;vertical-align:middle">'
    return '<span style="font-size:16px;font-weight:800;color:#1e3a5f">🏗️ PLANNING SCOUT</span>'

# ════════════════════════════════════════════════════════════
# DATA — 19 columns (v9 engine output)
# ════════════════════════════════════════════════════════════
COLS = [
    "Date Granted","Municipality","Full Address","Applicant",
    "Permit Type","Declared Value PEM (€)","Est. Build Value (€)",
    "Maps Link","Description","Source URL","PDF URL",
    "Mode","Confidence","Date Found","Lead Score","Expediente","Phase",
    "AI Evaluation","Supplies Needed",
]

@st.cache_data(ttl=300, show_spinner=False)
def load_sheet():
    try:
        sid    = st.secrets.get("SHEET_ID","")
        sa_raw = st.secrets.get("GCP_SERVICE_ACCOUNT_JSON","") or os.environ.get("GCP_SERVICE_ACCOUNT_JSON","")
        if not sid or not sa_raw: return pd.DataFrame(columns=COLS)
        creds = Credentials.from_service_account_info(
            json.loads(sa_raw),
            scopes=["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"])
        ws   = gspread.authorize(creds).open_by_key(sid).worksheet("Permits")
        rows = ws.get_all_values()
        if len(rows) < 2: return pd.DataFrame(columns=COLS)
        data = [r + [""]*(max(0, len(COLS)-len(r))) for r in rows[1:]]
        return pd.DataFrame(data, columns=COLS[:len(data[0])] if data else COLS)
    except Exception as e:
        st.error(f"Error al cargar datos: {e}"); return pd.DataFrame(columns=COLS)

# ════════════════════════════════════════════════════════════
# PROFILES
# ════════════════════════════════════════════════════════════
PROFILES = {
    "instaladores": {
        "icon":"🔧","label":"Instaladores MEP",
        "desc":"Ascensores · HVAC · Climatización · PCI",
        "types":["obra mayor nueva construcción","obra mayor rehabilitación",
                 "declaración responsable obra mayor","licencia primera ocupación",
                 "urbanización","demolición y nueva planta"],
        "min_pem":80_000,"days":30,
        "tip":"Un edificio plurifamiliar de 40 viviendas = 4 ascensores + HVAC completo + PCI. Contacta al promotor ANTES de que el constructor principal cierre contratos.",
        "color":"#0369a1",
    },
    "expansion": {
        "icon":"🏪","label":"Expansión Retail",
        "desc":"Nuevas aperturas · Ubicaciones · Cambios de uso",
        "types":["urbanización","plan especial","plan especial / parcial",
                 "cambio de uso","licencia de actividad"],
        "min_pem":0,"days":60,
        "tip":"Urbanización AD-10 Paracuellos = 2.500 viviendas → 10.000 residentes en 3 años. Encuentra tu próxima apertura antes de que el suelo suba de precio.",
        "color":"#7c3aed",
    },
    "promotores": {
        "icon":"📐","label":"Promotores / RE",
        "desc":"Reparcelaciones · Planes parciales · Convenios",
        "types":["urbanización","plan especial / parcial","plan especial",
                 "cambio de uso"],
        "min_pem":300_000,"days":60,
        "tip":"Reparcelación aprobada = suelo urbanizable disponible. Contacta a la Junta de Compensación antes de que la operación salga al mercado.",
        "color":"#0f766e",
    },
    "constructora": {
        "icon":"🏢","label":"Gran Constructora",
        "desc":"Licitaciones · Urbanismo · Infraestructuras",
        "types":["urbanización","licitación de obras","plan especial / parcial",
                 "plan especial","obra mayor nueva construcción","obra mayor industrial"],
        "min_pem":2_000_000,"days":90,
        "tip":"Aprobación definitiva de un plan = licitación en 12-18 meses. Prepara equipos técnicos, alianzas y ofertas antes que cualquier competidor.",
        "color":"#1e3a5f",
    },
    "industrial": {
        "icon":"🏭","label":"Industrial / Log.",
        "desc":"Naves · Polígonos · Centros de distribución",
        "types":["obra mayor industrial","urbanización","licitación de obras",
                 "licencia de actividad"],
        "min_pem":200_000,"days":60,
        "tip":"Nave en polígono de Valdemoro = oportunidad logística. Detecta proyectos en el momento de concesión de licencia para ser el primero en llamar al promotor.",
        "color":"#b45309",
    },
    "compras": {
        "icon":"🛒","label":"Compras / Materiales",
        "desc":"Acero · Hormigón · Fachadas · Instalaciones",
        "types":None,
        "min_pem":150_000,"days":30,
        "tip":"Todos los proyectos grandes son tu oportunidad. Preséntate antes de que el constructor adjudique suministros a tu competencia.",
        "color":"#be185d",
    },
    "general": {
        "icon":"🏙️","label":"Vista General",
        "desc":"Todos los proyectos sin filtrar",
        "types":None,
        "min_pem":0,"days":14,
        "tip":"Vista completa de todo lo publicado en el BOCM esta semana para la Comunidad de Madrid.",
        "color":"#374151",
    },
}

# ════════════════════════════════════════════════════════════
# HELPERS
# ════════════════════════════════════════════════════════════
def pem_float(v):
    if v is None or str(v).strip() in ("","0","nan"): return None
    s = str(v).strip().replace("€","").replace(" ","")
    try:
        if "," in s and "." in s: s=s.replace(".","").replace(",",".")
        elif "," in s: s=s.replace(",",".")
        else: s=s.replace(".","")
        f = float(s)
        return f if 0 < f < 3_000_000_000 else None
    except: return None

def pem_str(v):
    f = pem_float(v)
    if f is None: return None
    if f >= 1_000_000: return f"€{f/1_000_000:.1f}M"
    if f >= 1_000:    return f"€{f/1_000:.0f}K"
    return f"€{int(f)}"

def parse_dt(s):
    if not s: return None
    for fmt in ["%Y-%m-%d","%Y-%m-%d %H:%M","%d/%m/%Y","%Y/%m/%d"]:
        try: return datetime.strptime(str(s)[:16].strip(), fmt)
        except: pass
    return None

def score_color(sc):
    if sc >= 65: return "#16a34a"   # green
    if sc >= 40: return "#c8860a"   # amber
    if sc >= 20: return "#1e3a5f"   # navy
    return "#94a3b8"                # gray

PHASE_MAP = {
    "definitivo":        ("Aprobación definitiva", "#dcfce7","#166534"),
    "inicial":           ("Aprobación inicial",    "#fef9c3","#854d0e"),
    "licitacion":        ("Licitación activa",     "#dbeafe","#1e40af"),
    "primera_ocupacion": ("1ª Ocupación",          "#f3f4f6","#4b5563"),
    "en_tramite":        ("En trámite",            "#f3f4f6","#6b7280"),
}
TYPE_MAP = {
    "urbanización":                    "Urbanización",
    "plan especial / parcial":         "Plan Parcial",
    "plan especial":                   "Plan Especial",
    "obra mayor nueva construcción":   "Obra nueva",
    "obra mayor industrial":           "Industrial",
    "obra mayor rehabilitación":       "Rehabilitación",
    "cambio de uso":                   "Cambio de uso",
    "declaración responsable obra mayor":"Decl. Responsable",
    "licencia primera ocupación":      "1ª Ocupación",
    "licencia de actividad":           "Lic. Actividad",
    "licitación de obras":             "Licitación obras",
    "demolición y nueva planta":       "Demo + Nueva planta",
    "obra mayor":                      "Obra mayor",
}

def is_nuevo(date_found_str):
    """True if found in last 48 hours."""
    dt = parse_dt(date_found_str)
    if not dt: return False
    return (datetime.now() - dt).total_seconds() < 48*3600

def promotor_url(applicant):
    """Build LinkedIn people search URL for the applicant."""
    if not applicant or len(applicant) < 3: return None
    q = urllib.parse.quote(applicant)
    return f"https://www.linkedin.com/search/results/people/?keywords={q}"

def einforma_url(applicant):
    """Build Einforma company search URL."""
    if not applicant or len(applicant) < 3: return None
    q = urllib.parse.quote(applicant)
    return f"https://www.einforma.com/busqueda/{q}"

# ════════════════════════════════════════════════════════════
# CRM WEBHOOK SENDER
# Configure ZAPIER_WEBHOOK_URL in Streamlit secrets.
# When client clicks "Send to CRM", the lead JSON is POSTed there.
# Zapier/Make can route it to HubSpot, Salesforce, Pipedrive, etc.
# ════════════════════════════════════════════════════════════
def send_to_crm(lead_dict):
    webhook = st.secrets.get("ZAPIER_WEBHOOK_URL","")
    if not webhook:
        return False, "No ZAPIER_WEBHOOK_URL configured in secrets"
    try:
        r = http_requests.post(webhook, json=lead_dict, timeout=10)
        return r.status_code < 300, f"Status {r.status_code}"
    except Exception as e:
        return False, str(e)

# ════════════════════════════════════════════════════════════
# FILTER
# ════════════════════════════════════════════════════════════
def filter_data(df, prof_key, period_days, min_pem_val, min_score):
    if df.empty: return df
    prof   = PROFILES[prof_key]
    cutoff = datetime.now() - timedelta(days=period_days)

    # Parse dates
    df["_found_dt"] = df["Date Found"].apply(parse_dt)
    df["_pem_f"]    = df["Declared Value PEM (€)"].apply(pem_float)
    df["_score_i"]  = pd.to_numeric(df["Lead Score"], errors="coerce").fillna(0).astype(int)

    mask = pd.Series([True]*len(df), index=df.index)

    # Date filter
    mask &= df["_found_dt"].apply(lambda d: d is not None and d >= cutoff)

    # Type filter
    if prof["types"]:
        tl = [t.lower() for t in prof["types"]]
        mask &= df["Permit Type"].apply(lambda x: str(x).lower().strip() in tl)

    # PEM filter
    if min_pem_val > 0:
        mask &= df["_pem_f"].apply(lambda v: v is not None and v >= min_pem_val)

    # Score filter
    if min_score > 0:
        mask &= df["_score_i"] >= min_score

    out = df[mask].copy()
    out = out.sort_values("_score_i", ascending=False)
    return out

# ════════════════════════════════════════════════════════════
# CARD RENDERER — 100% inline styles, guaranteed to render
# ════════════════════════════════════════════════════════════
def render_card(row, idx, profile_key):
    # ── Raw values ──────────────────────────────────────────
    score    = int(row.get("_score_i", 0) or 0)
    muni     = (str(row.get("Municipality","")) or "Madrid").strip()
    addr     = (str(row.get("Full Address","")) or "").strip()
    appl     = (str(row.get("Applicant","")) or "").strip()
    pt       = (str(row.get("Permit Type","")) or "").strip()
    phase    = (str(row.get("Phase","")) or "").strip().lower()
    date_g   = (str(row.get("Date Granted","")) or "").strip()
    date_f   = (str(row.get("Date Found","")) or "").strip()
    exp      = (str(row.get("Expediente","")) or "").strip()
    conf     = (str(row.get("Confidence","")) or "").strip()
    bocm_url = (str(row.get("Source URL","")) or "").strip()
    pdf_url  = (str(row.get("PDF URL","")) or "").strip()
    maps_url = (str(row.get("Maps Link","")) or "").strip()
    desc     = (str(row.get("Description","")) or "").strip()
    ai_eval  = (str(row.get("AI Evaluation","")) or "").strip()
    supplies = (str(row.get("Supplies Needed","")) or "").strip()
    pem_raw  = row.get("_pem_f")

    # Sanitise "nan" strings
    for var_name in ["appl","addr","exp","conf","desc","ai_eval","supplies"]:
        val = locals()[var_name]
        if val.lower() == "nan": locals()[var_name]

    # Python 3 — reassign properly
    if appl.lower() == "nan": appl = ""
    if addr.lower() == "nan": addr = ""
    if exp.lower()  == "nan": exp  = ""
    if conf.lower() == "nan": conf = ""
    if desc.lower() == "nan": desc = ""
    if ai_eval.lower() == "nan": ai_eval = ""
    if supplies.lower() == "nan": supplies = ""

    # ── Derived values ───────────────────────────────────────
    sc_color  = score_color(score)
    nuevo     = is_nuevo(date_f)
    pem_label = pem_str(pem_raw) if pem_raw else None

    # Date formatting
    dg_dt = parse_dt(date_g)
    dg_str = dg_dt.strftime("%-d %b %Y") if dg_dt else (date_g[:10] if date_g else "")

    # Date Found for display
    df_dt = parse_dt(date_f)
    df_str = ""
    if df_dt:
        delta = datetime.now() - df_dt
        if delta.days == 0: df_str = "Hoy"
        elif delta.days == 1: df_str = "Ayer"
        else: df_str = f"Hace {delta.days}d"

    # BOCM document ID
    bocm_id = ""
    m = re.search(r"BOCM-\d{8}-\d+", bocm_url, re.I)
    if m: bocm_id = m.group(0)

    # Phase badge
    ph_label, ph_bg, ph_fg = PHASE_MAP.get(phase, ("","#f3f4f6","#6b7280"))

    # Type badge
    type_label = TYPE_MAP.get(pt.lower(), pt.title() if pt else "")

    # Title: first 90 chars of description, cleaned
    if desc and len(desc) > 12:
        title = re.sub(r"^(?:aprobación definitiva[:\s]+|se concede[:\s]+|se otorga[:\s]+|se aprueba[:\s]+)",
                       "", desc, flags=re.I)
        title = title[0].upper() + title[1:] if title else desc
        title = title[:95] + ("…" if len(title) > 95 else "")
    else:
        parts = [TYPE_MAP.get(pt.lower(), pt.title() if pt else "")]
        if addr: parts.append(addr[:45])
        elif muni != "Madrid": parts.append(muni)
        title = " · ".join(p for p in parts if p)

    # ── HTML ASSEMBLY (all inline styles — no external CSS classes) ─────────

    # Score circle
    score_html = (
        f'<div style="min-width:54px;width:54px;height:54px;border-radius:50%;'
        f'background:{sc_color};display:flex;flex-direction:column;align-items:center;'
        f'justify-content:center;flex-shrink:0;color:white;font-weight:700;">'
        f'<span style="font-size:15px;line-height:1">{score}</span>'
        f'<span style="font-size:9px;font-weight:400;opacity:.85">pts</span>'
        f'</div>'
    )

    # ⚡ Nuevo badge
    nuevo_html = ""
    if nuevo:
        nuevo_html = (
            '<span style="background:#fef08a;color:#713f12;border-radius:20px;'
            'padding:2px 10px;font-size:11px;font-weight:700;margin-left:8px;'
            'vertical-align:middle;">⚡ NUEVO</span>'
        )

    # Meta line (BOCM ID · Municipality · Date)
    meta_parts = []
    if bocm_id:  meta_parts.append(f'<span style="color:#94a3b8">{bocm_id}</span>')
    meta_parts.append(f'<strong style="color:#374151">{muni}</strong>')
    if dg_str:   meta_parts.append(f'<span style="color:#94a3b8">{dg_str}</span>')
    if df_str:   meta_parts.append(f'<span style="color:#94a3b8;font-style:italic">Detectado: {df_str}</span>')
    meta_html = ' <span style="color:#e2e8f0">·</span> '.join(meta_parts)

    # Tags row (type, phase, PEM)
    tags_html = ""
    if type_label:
        tags_html += (f'<span style="background:#f1f5f9;color:#334155;border-radius:20px;'
                      f'padding:3px 12px;font-size:12px;font-weight:600;margin-right:6px;">'
                      f'{type_label}</span>')
    if ph_label:
        tags_html += (f'<span style="background:{ph_bg};color:{ph_fg};border-radius:20px;'
                      f'padding:3px 12px;font-size:12px;font-weight:600;margin-right:6px;">'
                      f'{ph_label}</span>')
    if pem_label:
        tags_html += (f'<span style="background:#fef3c7;color:#92400e;border-radius:20px;'
                      f'padding:3px 12px;font-size:13px;font-weight:700;margin-right:6px;">'
                      f'{pem_label} PEM</span>')

    # Description block — always show if present
    desc_html = ""
    if desc and len(desc) > 12:
        desc_clean = re.sub(r"^(?:aprobación definitiva[:\s]+|se concede[:\s]+|se otorga[:\s]+)",
                            "", desc, flags=re.I)
        desc_clean = desc_clean[:280] + ("…" if len(desc_clean) > 280 else "")
        desc_html = (
            f'<div style="margin:12px 0 8px 0;font-size:13px;color:#4b5563;line-height:1.55;">'
            f'📋 {desc_clean}'
            f'</div>'
        )

    # AI Evaluation block (blue left border)
    ai_html = ""
    if ai_eval and len(ai_eval) > 20:
        ai_html = (
            f'<div style="margin:10px 0;background:#eff6ff;border-left:3px solid #3b82f6;'
            f'border-radius:0 8px 8px 0;padding:11px 14px;">'
            f'<div style="font-size:11px;font-weight:700;color:#1d4ed8;margin-bottom:4px;'
            f'letter-spacing:.04em;">🤖 ANÁLISIS IA</div>'
            f'<div style="font-size:12.5px;color:#374151;line-height:1.55;">{ai_eval[:450]}</div>'
            f'</div>'
        )
    else:
        # Show placeholder so users know the field exists (will populate after engine rerun)
        ai_html = (
            f'<div style="margin:10px 0;background:#f8fafc;border-left:3px solid #cbd5e1;'
            f'border-radius:0 8px 8px 0;padding:9px 14px;">'
            f'<div style="font-size:11px;color:#94a3b8;">🤖 Análisis IA — disponible tras próxima actualización del motor</div>'
            f'</div>'
        )

    # Supplies block (green left border)
    sup_html = ""
    if supplies and len(supplies) > 10:
        sup_html = (
            f'<div style="margin:8px 0;background:#f0fdf4;border-left:3px solid #22c55e;'
            f'border-radius:0 8px 8px 0;padding:11px 14px;">'
            f'<div style="font-size:11px;font-weight:700;color:#15803d;margin-bottom:4px;'
            f'letter-spacing:.04em;">⚒️ ESTIMACIÓN DE MATERIALES Y SERVICIOS</div>'
            f'<div style="font-size:12px;color:#374151;line-height:1.65;">{supplies[:280]}</div>'
            f'</div>'
        )
    else:
        sup_html = (
            f'<div style="margin:8px 0;background:#f8fafc;border-left:3px solid #cbd5e1;'
            f'border-radius:0 8px 8px 0;padding:9px 14px;">'
            f'<div style="font-size:11px;color:#94a3b8;">⚒️ Estimación de materiales — disponible tras próxima actualización</div>'
            f'</div>'
        )

    # Applicant + Expediente detail row
    det_parts = []
    if appl: det_parts.append(f'<strong>Promotor:</strong> {appl[:60]}')
    if exp:  det_parts.append(f'<strong>Expediente:</strong> {exp}')
    if conf: det_parts.append(f'<strong>Fiabilidad:</strong> {conf.capitalize()}')
    if addr: det_parts.append(f'<strong>Dirección:</strong> {addr[:50]}')
    det_html = ""
    if det_parts:
        det_html = (
            f'<div style="font-size:12px;color:#6b7280;margin:10px 0 4px 0;line-height:1.7;">'
            + " &nbsp;·&nbsp; ".join(det_parts)
            + "</div>"
        )

    # Action buttons row — BOCM, Mapa, PDF, Promotor (LinkedIn), Einforma
    btn_style = ("display:inline-block;padding:7px 14px;border-radius:8px;"
                 "font-size:12px;font-weight:600;text-decoration:none;"
                 "margin-right:6px;margin-top:6px;cursor:pointer;")
    btns = []
    if bocm_url:
        btns.append(f'<a href="{bocm_url}" target="_blank" style="{btn_style}'
                    f'background:#1e3a5f;color:white;border:1.5px solid #1e3a5f;">↗ Ver BOCM</a>')
    if maps_url:
        btns.append(f'<a href="{maps_url}" target="_blank" style="{btn_style}'
                    f'background:white;color:#1e3a5f;border:1.5px solid #bfdbfe;">📍 Mapa</a>')
    if pdf_url and pdf_url != bocm_url:
        btns.append(f'<a href="{pdf_url}" target="_blank" style="{btn_style}'
                    f'background:white;color:#dc2626;border:1.5px solid #fca5a5;">📄 PDF</a>')
    if appl:
        li_url = promotor_url(appl)
        ei_url = einforma_url(appl)
        if li_url:
            btns.append(f'<a href="{li_url}" target="_blank" style="{btn_style}'
                        f'background:white;color:#0a66c2;border:1.5px solid #bfdbfe;">🔍 LinkedIn</a>')
        if ei_url:
            btns.append(f'<a href="{ei_url}" target="_blank" style="{btn_style}'
                        f'background:white;color:#374151;border:1.5px solid #e2e8f0;">📊 Einforma</a>')

    btns_html = f'<div style="margin-top:12px;">{"".join(btns)}</div>' if btns else ""

    # ── Assemble full card ──────────────────────────────────
    return f"""
<div style="background:white;border-radius:16px;margin-bottom:16px;
            border:1px solid #e2e8f0;box-shadow:0 2px 8px rgba(0,0,0,.06);overflow:hidden;">
  <div style="padding:18px 22px;">
    <div style="display:flex;gap:16px;align-items:flex-start;">
      {score_html}
      <div style="flex:1;min-width:0;">

        <div style="font-size:12px;margin-bottom:5px;">{meta_html}{nuevo_html}</div>
        <div style="font-size:17px;font-weight:700;color:#111827;line-height:1.35;margin:4px 0 10px 0;">
          {title}
        </div>
        <div style="margin-bottom:4px;">{tags_html}</div>

        {desc_html}
        {ai_html}
        {sup_html}
        {det_html}
        {btns_html}

      </div>
    </div>
  </div>
</div>
"""

# ════════════════════════════════════════════════════════════
# ACCESS CONTROL
# ════════════════════════════════════════════════════════════
def check_access():
    if str(st.secrets.get("REQUIRE_TOKEN","false")).lower() != "true":
        return True
    token = st.query_params.get("token","")
    try:
        tokens = dict(st.secrets.get("client_tokens",{}))
        if token in tokens or token in tokens.values():
            return True
    except: pass
    st.error("🔒 Acceso restringido. Solicita tu enlace a PlanningScout.")
    st.stop()

check_access()

# ════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown(
        f'<div style="text-align:center;padding:16px 8px 14px;'
        f'border-bottom:1px solid #f1f5f9;margin-bottom:12px;">'
        f'{logo_html(32)}'
        f'<div style="font-size:12px;font-weight:800;color:#1e3a5f;'
        f'letter-spacing:.08em;margin-top:6px;">PLANNING SCOUT</div>'
        f'</div>',
        unsafe_allow_html=True)

    st.markdown('<p style="font-size:10px;font-weight:700;color:#94a3b8;'
                'letter-spacing:.12em;margin:14px 0 6px 0;">PERFIL</p>',
                unsafe_allow_html=True)

    profile_key = st.radio(
        "perfil_radio",
        list(PROFILES.keys()),
        format_func=lambda k: f"{PROFILES[k]['icon']} {PROFILES[k]['label']}",
        label_visibility="collapsed",
        key="profile_sel",
    )

    prof = PROFILES[profile_key]
    st.markdown(
        f'<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:8px;'
        f'padding:10px 12px;font-size:12px;color:#78350f;margin:8px 0 16px 0;line-height:1.5;">'
        f'💡 {prof["tip"]}</div>',
        unsafe_allow_html=True)

    st.markdown('<p style="font-size:10px;font-weight:700;color:#94a3b8;'
                'letter-spacing:.12em;margin:6px 0 6px 0;">FILTROS</p>',
                unsafe_allow_html=True)

    period_opts = {"7 días":7,"14 días":14,"30 días":30,"60 días":60,"90 días":90}
    default_idx = min(range(len(period_opts)),
                      key=lambda i: abs(list(period_opts.values())[i]-prof["days"]))
    period_label = st.selectbox("Período", list(period_opts.keys()),
                                index=default_idx, key="period_box")
    period_days  = period_opts[period_label]

    min_pem_val = st.number_input("PEM mínimo (€)", value=prof["min_pem"],
                                  step=50_000, min_value=0, key="pem_num")
    min_score   = st.slider("Puntuación mínima", 0, 100, 0, 5, key="score_sl")

    st.markdown('<p style="font-size:10px;font-weight:700;color:#94a3b8;'
                'letter-spacing:.12em;margin:14px 0 6px 0;">CRM</p>',
                unsafe_allow_html=True)

    # CRM webhook URL (shown only if configured)
    crm_url = st.secrets.get("ZAPIER_WEBHOOK_URL","")
    if crm_url:
        st.success("✅ CRM conectado (Zapier/Make)")
    else:
        st.info("Añade ZAPIER_WEBHOOK_URL en secrets para activar integración CRM")

    st.markdown('<p style="font-size:10px;font-weight:700;color:#94a3b8;'
                'letter-spacing:.12em;margin:14px 0 6px 0;">DATOS</p>',
                unsafe_allow_html=True)
    if st.button("🔄 Actualizar", use_container_width=True, key="refresh"):
        st.cache_data.clear(); st.rerun()

    st.markdown(
        '<div style="font-size:10px;color:#cbd5e1;margin-top:10px;text-align:center;">'
        'Datos: BOCM oficial · 179 municipios CM<br>Actualización diaria</div>',
        unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# MAIN — load and filter
# ════════════════════════════════════════════════════════════

# Honour ?perfil= URL param (deep links from website)
url_perfil = st.query_params.get("perfil","")
if url_perfil and url_perfil in PROFILES:
    st.session_state["profile_sel"] = url_perfil

with st.spinner("Cargando datos del BOCM…"):
    df_raw = load_sheet()

if df_raw.empty:
    st.markdown(
        '<div style="background:white;border-radius:16px;padding:48px;'
        'text-align:center;border:1px dashed #d1d5db;margin-top:20px;">'
        '<div style="font-size:36px;margin-bottom:12px;">📡</div>'
        '<div style="font-size:18px;font-weight:700;color:#374151;">Sin datos disponibles</div>'
        '<div style="font-size:13px;color:#9ca3af;margin-top:8px;">'
        'Configura GCP_SERVICE_ACCOUNT_JSON y SHEET_ID en secrets.</div>'
        '</div>', unsafe_allow_html=True)
    st.stop()

df = filter_data(df_raw.copy(), profile_key, period_days, min_pem_val, min_score)

# ── TOP BAR ──────────────────────────────────────────────────
total_pem = df["_pem_f"].sum()
priority  = int((df["_score_i"] >= 65).sum())
new_48h   = int(df["Date Found"].apply(is_nuevo).sum())

pem_display = (f"€{total_pem/1_000_000:.1f}M" if total_pem >= 1_000_000
               else f"€{int(total_pem):,}" if total_pem > 0 else "N/D")

top_score = int(df["_score_i"].max()) if len(df) else 0

st.markdown(
    f'<div style="display:flex;align-items:center;justify-content:space-between;'
    f'padding-bottom:16px;border-bottom:1px solid #e2e8f0;margin-bottom:20px;">'
    f'<div style="display:flex;align-items:center;gap:10px;">'
    f'<span style="display:inline-block;width:8px;height:8px;background:#22c55e;'
    f'border-radius:50%;"></span>'
    f'<span style="font-size:12px;color:#6b7280;">EN DIRECTO · 179 municipios CM Madrid · '
    f'Leads diarios</span>'
    f'</div>'
    f'<div>{logo_html(28)}</div>'
    f'</div>',
    unsafe_allow_html=True)

# ── STATS ────────────────────────────────────────────────────
c1, c2, c3, c4, c5 = st.columns(5)
for col, num, label, color in [
    (c1, len(df),       "Proyectos",          "#1e3a5f"),
    (c2, pem_display,   "PEM total",           "#c8860a"),
    (c3, priority,      "🟢 Prioritarios",     "#16a34a"),
    (c4, f"{top_score} pts", "Score más alto", "#374151"),
    (c5, f"⚡ {new_48h}", "Últimas 48h",      "#d97706"),
]:
    col.markdown(
        f'<div style="background:white;border-radius:12px;padding:14px 16px;'
        f'border:1px solid #e2e8f0;box-shadow:0 1px 3px rgba(0,0,0,.05);">'
        f'<div style="font-size:26px;font-weight:800;color:{color};">{num}</div>'
        f'<div style="font-size:12px;color:#6b7280;margin-top:2px;">{label}</div>'
        f'</div>',
        unsafe_allow_html=True)

# ── SECTION HEADER ──────────────────────────────────────────
st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

munis_uniq = [m for m in df["Municipality"].dropna().unique().tolist()[:5] if m]
muni_preview = " · ".join(munis_uniq) + ("…" if len(df["Municipality"].unique()) > 5 else "")

header_col, export_col = st.columns([3,1])
with header_col:
    st.markdown(
        f'<div style="font-size:22px;font-weight:800;color:#111827;margin-bottom:2px;">'
        f'Tus leads — {len(df)} proyectos'
        f'</div>'
        f'<div style="font-size:13px;color:#9ca3af;margin-bottom:16px;">'
        f'{muni_preview}'
        f'</div>',
        unsafe_allow_html=True)

with export_col:
    if len(df):
        csv = df.drop(columns=[c for c in df.columns if c.startswith("_")],
                      errors="ignore").to_csv(index=False).encode("utf-8")
        st.download_button(
            f"⬇️ Exportar CSV",
            csv,
            f"planningscout_{profile_key}_{datetime.now().strftime('%Y%m%d')}.csv",
            "text/csv", use_container_width=True, key="csv_dl")

# ── LEAD CARDS ──────────────────────────────────────────────
if df.empty:
    st.markdown(
        '<div style="background:white;border-radius:16px;padding:48px;'
        'text-align:center;border:1px dashed #d1d5db;">'
        '<div style="font-size:32px;margin-bottom:12px;">🔍</div>'
        '<div style="font-size:17px;font-weight:700;color:#374151;">Sin resultados</div>'
        '<div style="font-size:13px;color:#9ca3af;margin-top:8px;">'
        'Amplía el período o reduce el PEM mínimo.</div>'
        '</div>', unsafe_allow_html=True)
else:
    for i, (_, row) in enumerate(df.iterrows()):
        card_html = render_card(row, i, profile_key)
        st.markdown(card_html, unsafe_allow_html=True)

        # ── CRM Send Button (renders BELOW each card via Streamlit) ────────
        # We can't put Streamlit buttons inside st.markdown HTML,
        # so we render a native st.button right after each card.
        crm_webhook = st.secrets.get("ZAPIER_WEBHOOK_URL","")
        if crm_webhook:
            lead_id = str(row.get("Source URL",""))[-20:].replace("/","_")
            btn_key = f"crm_{i}_{lead_id}"
            if st.button("📤 Enviar a CRM", key=btn_key, type="secondary"):
                lead_data = {
                    "municipality":   str(row.get("Municipality","")),
                    "permit_type":    str(row.get("Permit Type","")),
                    "applicant":      str(row.get("Applicant","")),
                    "pem_eur":        float(row.get("_pem_f") or 0),
                    "description":    str(row.get("Description",""))[:300],
                    "ai_evaluation":  str(row.get("AI Evaluation",""))[:400],
                    "supplies":       str(row.get("Supplies Needed",""))[:200],
                    "source_url":     str(row.get("Source URL","")),
                    "lead_score":     int(row.get("_score_i",0)),
                    "phase":          str(row.get("Phase","")),
                    "date_granted":   str(row.get("Date Granted","")),
                    "profile":        profile_key,
                    "sent_at":        datetime.now().isoformat(),
                }
                ok, msg = send_to_crm(lead_data)
                if ok:
                    st.success("✅ Lead enviado al CRM")
                else:
                    st.error(f"❌ Error enviando: {msg}")

# ── FOOTER ──────────────────────────────────────────────────
st.markdown(
    '<div style="margin-top:40px;padding:20px 0;border-top:1px solid #e2e8f0;'
    'text-align:center;font-size:12px;color:#94a3b8;">'
    '<strong style="color:#1e3a5f;">PlanningScout</strong> &nbsp;·&nbsp; '
    'Boletín Oficial de la Comunidad de Madrid (BOCM) &nbsp;·&nbsp; '
    'Datos públicos oficiales &nbsp;·&nbsp; Actualización diaria &nbsp;·&nbsp; '
    '179 municipios de la Comunidad de Madrid'
    '</div>',
    unsafe_allow_html=True)
