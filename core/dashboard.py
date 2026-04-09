import subprocess, sys, base64, os, re, json, urllib.parse, html as html_module
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "streamlit", "gspread", "google-auth", "pandas", "requests", "-q"])

import streamlit as st
import pandas as pd
import gspread
import requests as http_requests
from google.oauth2.service_account import Credentials
from datetime import datetime

# PAGE CONFIG
st.set_page_config(
    page_title="PlanningScout Madrid",
    page_icon="🏗️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- PROFESSIONAL UI/UX STYLING ---
st.markdown("""
<style>
    /* Clean background */
    [data-testid="stAppViewContainer"] { background-color: #F8FAFC; }
    
    /* Sidebar styling: Smaller text, no more dark-blue clash */
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 1px solid #E2E8F0;
    }
    [data-testid="stSidebar"] .stRadio label {
        font-size: 14px !important;
        color: #475569 !important;
    }
    
    /* Modern Card Look */
    .project-card {
        background: white;
        padding: 24px;
        border-radius: 12px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        margin-bottom: 20px;
    }
    
    .card-header {
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        border-bottom: 1px solid #F1F5F9;
        padding-bottom: 12px;
        margin-bottom: 16px;
    }

    .municipality-tag {
        font-size: 12px;
        font-weight: 700;
        color: #64748B;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    .score-badge {
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 14px;
    }

    .data-row {
        display: flex;
        margin-bottom: 8px;
        font-size: 14px;
    }
    
    .data-label { color: #64748B; width: 120px; font-weight: 500; }
    .data-value { color: #1E293B; font-weight: 600; }

    /* Expandable Details Styling */
    .details-box {
        background: #F8FAFC;
        border-radius: 8px;
        padding: 15px;
        font-size: 14px;
        color: #334155;
        line-height: 1.6;
        margin-top: 10px;
        border-left: 4px solid #CBD5E1;
    }

    /* Remove Streamlit branding */
    #MainMenu, footer, header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# --- CONNECTION LOGIC ---
@st.cache_resource(ttl=600)
def get_gspread_client():
    try:
        # Fixed logic to match your specific Secrets format
        if "GCP_SERVICE_ACCOUNT_JSON" in st.secrets:
            info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT_JSON"])
        else:
            return None
        
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_info(info, scopes=scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"Connection Error: {e}")
        return None

def load_data():
    client = get_gspread_client()
    if not client: return pd.DataFrame()
    try:
        sheet = client.open_by_key(st.secrets["SHEET_ID"]).worksheet("Leads")
        df = pd.DataFrame(sheet.get_all_records())
        # Formatting
        df["_score_i"] = pd.to_numeric(df.get("AI Score", 0), errors="coerce").fillna(0).astype(int)
        df["_pem_f"] = pd.to_numeric(df.get("PEM (€)", 0), errors="coerce").fillna(0.0)
        return df
    except:
        return pd.DataFrame()

# --- WEBHOOK ---
def send_to_crm(data):
    url = st.secrets.get("MAKE_WEBHOOK_URL", "")
    if not url: return False
    try:
        r = http_requests.post(url, json=data, timeout=10)
        return r.status_code == 200
    except: return False

# --- SIDEBAR ---
st.sidebar.title("🏗️ PlanningScout")
df = load_data()

if df.empty:
    st.error("Error: Could not connect to Google Sheets. Check your Secrets.")
    st.stop()

profiles = ["Todas las Licencias"] + sorted(list(df["Target Profile"].unique()))
selected_profile = st.sidebar.radio("Sectores", profiles)

# Filtering
filtered_df = df if selected_profile == "Todas las Licencias" else df[df["Target Profile"] == selected_profile]
filtered_df = filtered_df.sort_values("_score_i", ascending=False)

# --- MAIN UI ---
st.title(f"{selected_profile}")
st.write(f"Encontradas {len(filtered_df)} licencias hoy.")

for idx, row in filtered_df.iterrows():
    score = int(row["_score_i"])
    # Color logic
    color = "#22C55E" if score >= 80 else "#F59E0B" if score >= 50 else "#EF4444"
    bg_color = "#DCFCE7" if score >= 80 else "#FEF3C7" if score >= 50 else "#FEE2E2"
    
    pem = f"{row['_pem_f']:,.0f} €".replace(",",".")
    
    # HTML Card
    card_content = f"""
    <div class="project-card">
        <div class="card-header">
            <div>
                <div class="municipality-tag">{row.get('Municipality', 'MADRID')} • {row.get('Date Granted', 'Reciente')}</div>
                <div style="font-size: 18px; font-weight: 800; color: #0F172A; margin-top: 4px;">{row.get('Permit Type', 'Licencia de Obra')}</div>
            </div>
            <div class="score-badge" style="background-color: {bg_color}; color: {color};">
                {score}% Match
            </div>
        </div>
        
        <div class="data-row">
            <div class="data-label">PROMOTOR</div>
            <div class="data-value">{row.get('Applicant', 'No disponible')}</div>
        </div>
        <div class="data-row">
            <div class="data-label">PRESUPUESTO</div>
            <div class="data-value">{pem}</div>
        </div>
        
        <div style="background: #F0FDF4; padding: 12px; border-radius: 8px; margin: 15px 0; border: 1px solid #BBF7D0;">
            <div style="font-weight: 700; color: #166534; font-size: 13px; margin-bottom: 4px;">💡 EVALUACIÓN AI</div>
            <div style="color: #15803D; font-size: 14px;">{row.get('AI Evaluation', 'Analizando...')}</div>
        </div>
    </div>
    """
    st.markdown(card_content, unsafe_allow_html=True)
    
    # Description Dropdown using Streamlit's native expander (cleaner behavior)
    with st.expander("📄 Ver descripción técnica completa"):
        st.write(row.get("Description", "Sin descripción disponible."))
        st.markdown(f"[Ver fuente original ↗]({row.get('Source URL', '#')})")

    # Action Button
    if st.button(f"Enviar a CRM (Lead #{idx})", key=f"crm_{idx}"):
        payload = row.to_dict()
        if send_to_crm(payload):
            st.success("¡Enviado!")
        else:
            st.error("Error al enviar.")
    
    st.markdown("<br>", unsafe_allow_html=True)
