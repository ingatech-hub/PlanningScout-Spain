import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread",
    "google-auth", "python-dateutil", "openai", "-q"])

import requests, re, io, time, json, os, smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dateutil import parser as dateparser
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials as SACredentials
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ════════════════════════════════════════════════════════════
# CONFIG — edit these or load from a JSON client file
# ════════════════════════════════════════════════════════════
import argparse
parser = argparse.ArgumentParser(description="ConstructorScout Engine")
parser.add_argument("--client", required=True, help="Path to client JSON config")
parser.add_argument("--weeks",  type=int, default=1, help="Weeks to look back")
parser.add_argument("--digest", action="store_true", help="Send weekly digest email and exit")
args = parser.parse_args()

with open(args.client, "r", encoding="utf-8") as f:
    CFG = json.load(f)

SHEET_ID         = CFG["sheet_id"]
CLIENT_EMAIL_VAR = CFG["email_to_secret_name"]
MIN_VALUE_EUR    = CFG.get("min_declared_value_eur", 0)
WEEKS_BACK       = args.weeks

# OpenAI is optional — if key is not set, falls back to keyword extraction
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "")
USE_AI           = bool(OPENAI_API_KEY)

# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

log(f"Mode: {'AI extraction (GPT-4o-mini)' if USE_AI else 'Keyword extraction (no API key)'}")

# ════════════════════════════════════════════════════════════
# BOCM SCRAPER
# The BOCM (Boletín Oficial de la Comunidad de Madrid) publishes
# daily at bocm.es. We search for building permit keywords and
# collect announcement URLs, then fetch each one.
# ════════════════════════════════════════════════════════════
BOCM_SEARCH_URL = "https://www.bocm.es/buscador"
BOCM_BASE       = "https://www.bocm.es"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Keywords that indicate a building permit grant
PERMIT_KEYWORDS = [
    "licencia de obras mayor",
    "licencia urbanística",
    "licencia de obras",
    "licencia de actividad",
    "declaración responsable de obras",
    "autorización de obras",
    "licencia de primera ocupación",
    "concesión de licencia",
    "resolución favorable",
]

# Keywords that indicate a DENIAL — we exclude these (wrong product)
DENY_KEYWORDS = [
    "denegación", "denegada", "deniega", "inadmisión",
    "desestimación", "archivo", "caducidad",
]

def safe_get(url, timeout=20, retries=2):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout, verify=False)
            if r.status_code == 200:
                return r
            log(f"  HTTP {r.status_code} for {url[:60]}")
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
            else:
                log(f"  GET failed: {e}")
    return None

def search_bocm(keyword, date_from, date_to):
    """
    Search the BOCM for a keyword within a date range.
    Returns a list of announcement URLs.
    """
    urls = []
    page = 1

    while True:
        params = {
            "texto": keyword,
            "fecha_desde": date_from.strftime("%d/%m/%Y"),
            "fecha_hasta": date_to.strftime("%d/%m/%Y"),
            "pagina": page,
        }
        full_url = f"{BOCM_SEARCH_URL}?{urlencode(params)}"
        log(f"  Searching: '{keyword}' page {page}")

        r = safe_get(full_url)
        if not r:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # Find result links — BOCM result items typically have class "resultado" or similar
        result_links = []

        # Try multiple selectors as BOCM may update their HTML
        for selector in [
            "a[href*='/boletin/']",
            "a[href*='/anuncio/']",
            ".resultado a",
            ".resultado-busqueda a",
            "article a",
            ".item-busqueda a",
        ]:
            found = soup.select(selector)
            if found:
                result_links = found
                break

        # Fallback: find any link containing typical BOCM URL patterns
        if not result_links:
            result_links = soup.find_all("a", href=re.compile(r"/(boletin|anuncio|bocm)/", re.I))

        if not result_links:
            log(f"  No results on page {page} — stopping")
            break

        new_urls = []
        for a in result_links:
            href = a.get("href", "")
            if not href:
                continue
            full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
            if full not in urls:
                new_urls.append(full)
                urls.append(full)

        log(f"  Found {len(new_urls)} new links on page {page}")

        # Check if there is a next page
        next_page = soup.find("a", string=re.compile(r"siguiente|next|>", re.I))
        if not next_page or not new_urls:
            break

        page += 1
        time.sleep(1)

    return urls

def fetch_announcement_text(url):
    """
    Fetch the text content of a BOCM announcement page.
    Also tries to fetch and extract any linked PDF.
    Returns the full text string.
    """
    r = safe_get(url)
    if not r:
        return ""

    soup = BeautifulSoup(r.text, "html.parser")

    # Get the main body text
    text_parts = []

    for selector in [".contenido-boletin", ".anuncio", "article", "main", ".content", "#content"]:
        el = soup.select_one(selector)
        if el:
            text_parts.append(el.get_text(separator=" ", strip=True))
            break

    if not text_parts:
        text_parts.append(soup.get_text(separator=" ", strip=True)[:5000])

    # Try to also get the PDF if there is one linked
    pdf_link = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower() or "descargar-pdf" in href.lower() or "pdf" in href.lower():
            pdf_link = urljoin(BOCM_BASE, href) if href.startswith("/") else href
            break

    if pdf_link:
        pdf_text = extract_pdf_text(pdf_link)
        if pdf_text:
            text_parts.append(pdf_text)

    return " ".join(text_parts)

def extract_pdf_text(url):
    """Download and extract text from a PDF."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=40, verify=False)
        if r.status_code != 200 or len(r.content) < 500:
            return ""
        if r.content[:4] != b"%PDF":
            return ""
        text = ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for page in pdf.pages[:8]:  # max 8 pages
                t = page.extract_text()
                if t:
                    text += t + " "
        return text[:8000]
    except Exception as e:
        log(f"  PDF error: {e}")
        return ""

# ════════════════════════════════════════════════════════════
# EXTRACTION — two modes
# Mode 1: Keyword-based (no API key needed)
# Mode 2: GPT-4o-mini (better accuracy, ~€0.001 per permit)
# ════════════════════════════════════════════════════════════

def is_permit_grant(text):
    """
    Returns True if the text describes a GRANTED permit (not a denial).
    This is the first filter applied before any extraction.
    """
    text_lower = text.lower()

    # Must contain at least one permit keyword
    has_permit = any(kw in text_lower for kw in PERMIT_KEYWORDS)
    if not has_permit:
        return False

    # Must NOT be a denial
    has_denial = any(kw in text_lower for kw in DENY_KEYWORDS)
    if has_denial:
        return False

    # Must contain grant language
    grant_phrases = [
        "se concede", "se otorga", "se autoriza", "se resuelve favorablemente",
        "licencia concedida", "concesión de licencia", "se aprueba",
        "otorgamiento de licencia", "favorable", "conceder",
    ]
    has_grant = any(ph in text_lower for ph in grant_phrases)

    return has_grant

def extract_keyword_mode(text, url, pub_date):
    """
    Extract permit data using regex patterns — no AI needed.
    Less accurate than AI but works immediately with zero cost.
    """
    result = {
        "address": None,
        "applicant": None,
        "permit_type": "otro",
        "declared_value_eur": None,
        "date_granted": pub_date,
        "description": None,
        "confidence": "medium",
        "source_url": url,
        "extraction_mode": "keyword",
    }

    text_clean = re.sub(r'\s+', ' ', text)

    # ── Address extraction ───────────────────────────────────────────────────
    # Spanish address patterns: "calle X, número Y", "Av. X nº Y", "C/ X, Y"
    address_patterns = [
        r'(?:calle|c/|c\.)\s+([A-ZÁÉÍÓÚÑ][^,\n]{3,50}),?\s+n[úu]?m?\.?\s*(\d+[a-z]?)',
        r'(?:avenida|av\.?|avda\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{3,50}),?\s+n[úu]?m?\.?\s*(\d+)',
        r'(?:paseo|pso\.?|po\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{3,50}),?\s+n[úu]?m?\.?\s*(\d+)',
        r'(?:plaza|pl\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{3,50}),?\s+n[úu]?m?\.?\s*(\d+)',
        r'(?:camino|cm\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{3,50}),?\s+n[úu]?m?\.?\s*(\d+)',
        # Direct "C/ Nombre 23" pattern
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40}),?\s+(\d+[a-z]?)',
    ]
    for pattern in address_patterns:
        m = re.search(pattern, text_clean, re.IGNORECASE)
        if m:
            result["address"] = f"{m.group(0).strip()}"
            break

    # ── Applicant extraction ─────────────────────────────────────────────────
    # Look for "solicitante:", "a instancia de", "presentado por", "el/la interesado/a"
    applicant_patterns = [
        r'(?:solicitante|interesado/a|promovido por|a instancia de|presentado por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n]{3,60})',
        r'(?:don|doña|d\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3})',
        r'(?:empresa|entidad|sociedad|s\.a\.|s\.l\.|s\.l\.u\.)\s+([A-ZÁÉÍÓÚÑ][^,\.\n]{3,60})',
        r'([A-ZÁÉÍÓÚÑ][^,\.\n]{5,40},?\s+S\.?[AL]\.?U?\.?)',
    ]
    for pattern in applicant_patterns:
        m = re.search(pattern, text_clean, re.IGNORECASE)
        if m:
            applicant = m.group(1).strip().rstrip(".,")
            if len(applicant) > 3:
                result["applicant"] = applicant
                break

    # ── Permit type classification ────────────────────────────────────────────
    text_lower = text_clean.lower()
    if any(p in text_lower for p in ["nueva construcción", "nueva planta", "edificio de nueva"]):
        result["permit_type"] = "obra mayor nueva construcción"
    elif any(p in text_lower for p in ["rehabilitación", "reforma", "renovación", "ampliación"]):
        result["permit_type"] = "obra mayor rehabilitación"
    elif any(p in text_lower for p in ["obra menor", "instalación menor"]):
        result["permit_type"] = "obra menor"
    elif any(p in text_lower for p in ["actividad", "local comercial", "establecimiento", "bar", "restaurante", "oficina"]):
        result["permit_type"] = "licencia de actividad comercial"

    # ── Declared value extraction ─────────────────────────────────────────────
    # Look for ICIO (Impuesto sobre Construcciones) base or PEM (Presupuesto de Ejecución Material)
    value_patterns = [
        # "presupuesto de ejecución material: 450.000 euros"
        r'(?:presupuesto de ejecución material|p\.e\.m\.|pem)[:\s]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        # "base imponible del ICIO: 320.000,00 €"
        r'(?:base imponible|base del icio|icio)[:\s]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        # "valorado en 500.000 euros"
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        # Generic "XXXXX euros" near obra context
        r'([0-9]{1,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)\s*(?:euros?|€)',
    ]
    for pattern in value_patterns:
        m = re.search(pattern, text_clean, re.IGNORECASE)
        if m:
            val_str = m.group(1).replace(".", "").replace(",", ".")
            try:
                val = float(val_str)
                if val > 1000:  # Filter out tiny numbers
                    result["declared_value_eur"] = val
                    break
            except ValueError:
                pass

    # ── Description — first 300 chars of clean text ──────────────────────────
    # Find the most relevant sentence containing the works description
    desc_match = re.search(
        r'(?:obras? de|construcción de|rehabilitación de|instalación de|reforma de)\s+[^.]{10,200}',
        text_clean, re.IGNORECASE
    )
    if desc_match:
        result["description"] = desc_match.group(0).strip()[:250]
    else:
        result["description"] = text_clean[:200].strip()

    return result


def extract_ai_mode(text, url, pub_date):
    """
    Extract permit data using GPT-4o-mini.
    More accurate than keyword mode. Cost: ~€0.001 per permit.
    Only called when OPENAI_API_KEY is set.
    """
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        prompt = f"""Eres un extractor de datos de licencias de obras del Boletín Oficial de la Comunidad de Madrid (BOCM).

Dado el siguiente texto de un anuncio oficial, extrae la siguiente información si está presente:

1. Dirección completa (calle, número, municipio)
2. Nombre del solicitante (persona o empresa que solicita la licencia)
3. Tipo de licencia (EXACTAMENTE uno de: "obra mayor nueva construcción", "obra mayor rehabilitación", "obra menor", "licencia de actividad comercial", "otro")
4. Valor declarado de construcción en euros (busca base imponible del ICIO, presupuesto de ejecución material o PEM)
5. Fecha de concesión de la licencia
6. Descripción breve de las obras

Responde ÚNICAMENTE en JSON válido con estas claves exactas:
{{
  "address": "",
  "applicant": "",
  "permit_type": "",
  "declared_value_eur": null,
  "date_granted": "",
  "description": "",
  "confidence": "high/medium/low"
}}

Si un campo no está presente en el texto, usa null. NO inventes datos.

Texto a analizar:
{text[:3000]}"""

        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=500,
        )

        raw = response.choices[0].message.content.strip()
        # Strip markdown code fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw)
        raw = re.sub(r'\s*```$', '', raw)

        data = json.loads(raw)
        data["source_url"] = url
        data["extraction_mode"] = "ai"
        if not data.get("date_granted"):
            data["date_granted"] = pub_date

        # Normalise declared_value_eur to float if it came back as string
        if isinstance(data.get("declared_value_eur"), str):
            try:
                data["declared_value_eur"] = float(
                    data["declared_value_eur"].replace(".", "").replace(",", ".")
                )
            except ValueError:
                data["declared_value_eur"] = None

        return data

    except Exception as e:
        log(f"  AI extraction failed: {e} — falling back to keyword mode")
        return extract_keyword_mode(text, url, pub_date)


def extract_permit(text, url, pub_date):
    """Main extraction dispatcher."""
    if USE_AI:
        return extract_ai_mode(text, url, pub_date)
    else:
        return extract_keyword_mode(text, url, pub_date)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS
# ════════════════════════════════════════════════════════════
SHEET_HEADERS = [
    "Date Published", "Municipality", "Full Address", "Applicant Name",
    "Permit Type", "Declared Value (€)", "Est. Construction Value (€)",
    "Google Maps Link", "Description", "Source URL",
    "Extraction Mode", "Confidence", "Date Found", "Client Notes",
]

_ws = None
_existing_urls = set()

def get_sheet():
    global _ws
    if _ws:
        return _ws
    sa_json = os.environ.get("GCP_SERVICE_ACCOUNT_JSON", "").strip()
    if not sa_json:
        log("❌ GCP_SERVICE_ACCOUNT_JSON not set — cannot connect to Sheets")
        return None
    try:
        info  = json.loads(sa_json)
        creds = SACredentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(SHEET_ID).worksheet("Permits")
        existing = ws.row_values(1)
        if existing != SHEET_HEADERS:
            ws.update(values=[SHEET_HEADERS], range_name="A1")
            log("✅ Sheet headers written")
        else:
            log("✅ Google Sheet connected")
        _ws = ws
        return _ws
    except Exception as e:
        log(f"❌ Sheet connection failed: {e}")
        return None

def load_existing_urls():
    global _existing_urls
    ws = get_sheet()
    if not ws:
        return
    try:
        urls = ws.col_values(10)  # Column J = Source URL
        _existing_urls = set(urls[1:])
        log(f"✅ Loaded {len(_existing_urls)} existing URLs (dedup cache)")
    except Exception as e:
        log(f"⚠️  Could not load existing URLs: {e}")

def write_permit(permit):
    ws = get_sheet()
    if not ws:
        return False

    url = permit.get("source_url", "")
    if url in _existing_urls:
        log(f"  ⏭️  Duplicate: {url[:60]}")
        return False

    # Calculate estimated construction value
    # ICIO base is typically 2–4% of actual construction cost
    est_value = None
    if permit.get("declared_value_eur"):
        mid_rate  = 0.03  # 3% midpoint
        est_value = round(permit["declared_value_eur"] / mid_rate)

    # Google Maps link from address
    maps_link = ""
    if permit.get("address"):
        encoded = permit["address"].replace(" ", "+")
        maps_link = f"https://www.google.com/maps/search/{encoded}+Madrid"

    row = [
        permit.get("date_granted", ""),
        "Madrid",
        permit.get("address") or "",
        permit.get("applicant") or "",
        permit.get("permit_type") or "",
        permit.get("declared_value_eur") or "",
        est_value or "",
        maps_link,
        (permit.get("description") or "")[:300],
        url,
        permit.get("extraction_mode", "keyword"),
        permit.get("confidence", ""),
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        "",  # Client Notes — empty, filled manually
    ]

    try:
        ws.append_row(row)
        _existing_urls.add(url)

        # Colour row by confidence
        try:
            all_rows = ws.get_all_values()
            row_num  = len(all_rows)
            conf     = permit.get("confidence", "")
            if conf == "high":
                r, g, b = 0.85, 0.93, 0.85   # green
            elif conf == "medium":
                r, g, b = 0.99, 0.96, 0.80   # yellow
            else:
                r, g, b = 0.96, 0.90, 0.90   # light red

            ws.spreadsheet.batch_update({"requests": [{"repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": row_num-1, "endRowIndex": row_num},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": r, "green": g, "blue": b}}},
                "fields": "userEnteredFormat.backgroundColor",
            }}]})
        except Exception:
            pass

        log(f"  💾 SAVED: {permit.get('address','?')} | {permit.get('permit_type','?')} | €{permit.get('declared_value_eur','?')}")
        return True
    except Exception as e:
        log(f"  ❌ Sheet write failed: {e}")
        return False

# ════════════════════════════════════════════════════════════
# WEEKLY EMAIL DIGEST
# ════════════════════════════════════════════════════════════
def send_weekly_digest():
    """
    Read the last 7 days of permits from the Sheet and send a
    formatted HTML email to the client.
    """
    ws = get_sheet()
    if not ws:
        log("❌ Cannot send digest — no Sheet connection")
        return

    try:
        all_rows = ws.get_all_values()
        if len(all_rows) < 2:
            log("⚠️  No data in sheet — skipping digest")
            return

        headers = all_rows[0]
        cutoff  = datetime.now() - timedelta(days=7)
        recent  = []

        for row in all_rows[1:]:
            if len(row) < 13:
                continue
            date_found_str = row[12]  # Column M = Date Found
            try:
                date_found = datetime.strptime(date_found_str[:10], "%Y-%m-%d")
                if date_found >= cutoff:
                    recent.append(row)
            except Exception:
                continue

        log(f"📧 Sending digest with {len(recent)} permits from last 7 days")

        # Build HTML email
        permit_rows_html = ""
        for row in sorted(recent, key=lambda r: r[5] if r[5] else "0", reverse=True):
            address   = row[2] or "—"
            applicant = row[3] or "—"
            ptype     = row[4] or "—"
            value     = f"€{int(float(row[5])):,}" if row[5] else "—"
            est       = f"€{int(float(row[6])):,}" if row[6] else "—"
            maps      = row[7]
            desc      = row[8][:120] if row[8] else ""
            url       = row[9]
            date_pub  = row[0]

            permit_rows_html += f"""
            <tr style="border-bottom:1px solid #eee;">
              <td style="padding:12px 8px;font-weight:600;color:#1a1a1a;">{address}</td>
              <td style="padding:12px 8px;color:#444;">{applicant}</td>
              <td style="padding:12px 8px;">
                <span style="background:#e8f5e9;color:#2e7d32;padding:3px 8px;border-radius:4px;font-size:12px;">{ptype}</span>
              </td>
              <td style="padding:12px 8px;font-weight:600;color:#1565c0;">{value}</td>
              <td style="padding:12px 8px;color:#666;">{est}</td>
              <td style="padding:12px 8px;font-size:13px;color:#555;">{desc}</td>
              <td style="padding:12px 8px;">
                {"<a href='" + maps + "' style='color:#1565c0;text-decoration:none;'>📍 Map</a>" if maps else ""}
                {"&nbsp;<a href='" + url + "' style='color:#888;font-size:12px;'>BOCM</a>" if url else ""}
              </td>
            </tr>"""

        total_value = 0
        for row in recent:
            if row[5]:
                try:
                    total_value += float(row[5])
                except Exception:
                    pass

        html = f"""
        <html><body style="font-family:Arial,sans-serif;max-width:1000px;margin:0 auto;color:#1a1a1a;">
          <div style="background:#1565c0;color:white;padding:24px;border-radius:8px 8px 0 0;">
            <h1 style="margin:0;font-size:22px;">🏗️ ConstructorScout — Madrid Building Permits</h1>
            <p style="margin:8px 0 0;opacity:0.85;">Week of {(datetime.now() - timedelta(days=7)).strftime("%d %b")} — {datetime.now().strftime("%d %b %Y")}</p>
          </div>

          <div style="background:#e3f2fd;padding:16px 24px;display:flex;gap:32px;">
            <div><strong style="font-size:28px;color:#1565c0;">{len(recent)}</strong><br><span style="color:#555;font-size:13px;">New permits this week</span></div>
            <div><strong style="font-size:28px;color:#1565c0;">€{int(total_value):,}</strong><br><span style="color:#555;font-size:13px;">Total declared value</span></div>
          </div>

          <table style="width:100%;border-collapse:collapse;margin-top:16px;">
            <thead>
              <tr style="background:#f5f5f5;text-align:left;">
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Address</th>
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Applicant</th>
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Type</th>
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Declared €</th>
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Est. Build €</th>
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Description</th>
                <th style="padding:10px 8px;font-size:12px;color:#666;text-transform:uppercase;">Links</th>
              </tr>
            </thead>
            <tbody>
              {permit_rows_html if permit_rows_html else '<tr><td colspan="7" style="padding:24px;text-align:center;color:#888;">No permits found this week</td></tr>'}
            </tbody>
          </table>

          <div style="margin-top:24px;padding:16px;background:#f9f9f9;border-radius:4px;font-size:13px;color:#666;">
            <strong>ConstructorScout</strong> — Building permit intelligence for Madrid.
            Each permit represents a project starting soon. Contact the site before your competitors do.
            <br><br>
            Data sourced from BOCM (Boletín Oficial de la Comunidad de Madrid) — official public records.
          </div>
        </body></html>"""

        gmail_from     = os.environ.get("GMAIL_FROM", "")
        gmail_password = os.environ.get("GMAIL_APP_PASSWORD", "")
        gmail_to       = os.environ.get(CLIENT_EMAIL_VAR, "")

        if not all([gmail_from, gmail_password, gmail_to]):
            log("⚠️  Email credentials not set — printing digest to console instead")
            log(f"Would send {len(recent)} permits to {CLIENT_EMAIL_VAR}")
            return

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"ConstructorScout Madrid — {len(recent)} New Permits This Week (w/e {datetime.now().strftime('%d %b %Y')})"
        msg["From"]    = gmail_from
        msg["To"]      = gmail_to
        msg.attach(MIMEText(html, "html"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_from, gmail_password)
            server.sendmail(gmail_from, gmail_to.split(","), msg.as_string())

        log(f"✅ Digest sent to {gmail_to} ({len(recent)} permits)")

    except Exception as e:
        log(f"❌ Digest failed: {e}")


# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
def run():
    # ── Email-only mode ──────────────────────────────────────
    if args.digest:
        log("📧 Digest-only mode")
        get_sheet()
        send_weekly_digest()
        return

    today     = datetime.now()
    date_to   = today
    date_from = today - timedelta(weeks=WEEKS_BACK)

    log("=" * 60)
    log(f"🏗️  ConstructorScout — Madrid Building Permit Engine")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')}")
    log(f"🤖  Extraction mode: {'AI (GPT-4o-mini)' if USE_AI else 'Keyword (no API key)'}")
    log("=" * 60)

    # ── Connect to Sheet ─────────────────────────────────────
    get_sheet()
    load_existing_urls()

    # ── Collect announcement URLs ────────────────────────────
    all_urls = set()
    search_terms = [
        "licencia de obras mayor",
        "licencia urbanística concedida",
        "licencia de actividad",
        "obras de nueva construcción",
        "declaración responsable de obras",
    ]

    for keyword in search_terms:
        log(f"\n🔎 Keyword: '{keyword}'")
        urls = search_bocm(keyword, date_from, date_to)
        for u in urls:
            all_urls.add(u)
        log(f"  → {len(urls)} URLs found")
        time.sleep(2)

    # Remove already-processed URLs
    new_urls = [u for u in all_urls if u not in _existing_urls]
    log(f"\n📋 {len(all_urls)} total URLs found, {len(new_urls)} are new")

    # ── Process each announcement ────────────────────────────
    saved   = 0
    skipped = 0

    for idx, url in enumerate(new_urls):
        log(f"\n[{idx+1}/{len(new_urls)}] {url[:80]}")

        text = fetch_announcement_text(url)
        if not text or len(text) < 100:
            log("  ⚠️  No text extracted — skip")
            skipped += 1
            continue

        # First filter: is this actually a grant?
        if not is_permit_grant(text):
            log("  ⏭️  Not a permit grant (denial or irrelevant) — skip")
            skipped += 1
            continue

        # Extract structured data
        pub_date = today.strftime("%Y-%m-%d")
        permit   = extract_permit(text, url, pub_date)

        # Value filter
        val = permit.get("declared_value_eur")
        if val and MIN_VALUE_EUR and val < MIN_VALUE_EUR:
            log(f"  ⏭️  Value €{val:,.0f} below minimum €{MIN_VALUE_EUR:,.0f} — skip")
            skipped += 1
            continue

        # Write to sheet
        if write_permit(permit):
            saved += 1
        else:
            skipped += 1

        time.sleep(1.5)

    log(f"\n{'='*60}")
    log(f"✅ Done. {saved} permits saved, {skipped} skipped.")
    log(f"{'='*60}")

    # ── Send digest on Mondays ───────────────────────────────
    if datetime.now().weekday() == 0:  # Monday = 0
        log("\n📧 It's Monday — sending weekly digest")
        send_weekly_digest()
    else:
        log(f"(Not Monday — digest will send next Monday)")


# ── Colab auth ───────────────────────────────────────────────
if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth
        auth.authenticate_user()
        log("✅ Colab auth done")
    except Exception:
        pass

run()
