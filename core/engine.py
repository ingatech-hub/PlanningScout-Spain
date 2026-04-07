import subprocess, sys
subprocess.check_call([sys.executable, "-m", "pip", "install",
    "requests", "beautifulsoup4", "pdfplumber", "gspread",
    "google-auth", "python-dateutil", "openai", "-q"])

import requests, re, io, time, json, os, smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import urljoin, quote
from bs4 import BeautifulSoup
import pdfplumber
import gspread
from google.oauth2.service_account import Credentials as SACredentials
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ════════════════════════════════════════════════════════════
# ARGS
# ════════════════════════════════════════════════════════════
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("--client",  required=True)
parser.add_argument("--weeks",   type=int, default=2)
parser.add_argument("--digest",  action="store_true")
parser.add_argument("--resume",  action="store_true")
args = parser.parse_args()

with open(args.client, "r", encoding="utf-8") as f:
    CFG = json.load(f)

SHEET_ID         = CFG["sheet_id"]
CLIENT_EMAIL_VAR = CFG["email_to_secret_name"]
MIN_VALUE_EUR    = CFG.get("min_declared_value_eur", 0)
WEEKS_BACK       = args.weeks
OPENAI_API_KEY   = os.environ.get("OPENAI_API_KEY", "").strip()
USE_AI           = bool(OPENAI_API_KEY)
QUEUE_FILE       = "/tmp/bocm_queue.json"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

# ════════════════════════════════════════════════════════════
# HTTP SESSION
# ════════════════════════════════════════════════════════════
BOCM_BASE = "https://www.bocm.es"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}
_session = None
_consecutive_bad = 0
MAX_BAD = 5

def make_session():
    s = requests.Session()
    s.headers.update(HEADERS)
    for name in ["cookies-agreed","cookie-agreed","has_js","bocm_cookies","cookie_accepted"]:
        s.cookies.set(name, "1", domain="www.bocm.es")
    return s

def get_session():
    global _session
    if _session is None: _session = make_session()
    return _session

def rotate_session():
    global _session, _consecutive_bad
    log("  🔄 Rotating session…")
    _session = make_session(); _consecutive_bad = 0; time.sleep(15)

def safe_get(url, timeout=30, retries=3, backoff_base=10):
    global _consecutive_bad
    for attempt in range(retries):
        try:
            r = get_session().get(url, timeout=timeout, verify=False, allow_redirects=True)
            if r.status_code == 200:
                _consecutive_bad = 0; return r
            if r.status_code in (502, 503, 429):
                _consecutive_bad += 1
                wait = backoff_base * (3 ** attempt)
                log(f"  ⚠️  HTTP {r.status_code} — waiting {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
                if _consecutive_bad >= MAX_BAD: rotate_session()
                continue
            log(f"  HTTP {r.status_code}: {url[:80]}")
            return r
        except requests.exceptions.Timeout:
            wait = backoff_base * (2 ** attempt)
            log(f"  ⏱️ Timeout — waiting {wait}s"); time.sleep(wait)
        except Exception as e:
            log(f"  ❌ {type(e).__name__}: {e}")
            if attempt < retries - 1: time.sleep(backoff_base)
    return None

# ════════════════════════════════════════════════════════════
# BOCM SEARCH — URL BUILDING
#
# Confirmed from user's cURL (Step 1) + pagination (Step 4):
#   Date format: DD-MM-YYYY (dashes, NOT slashes)
#   Section: 8387 = III. Administración Local Ayuntamientos
#   Pagination: path-based (not query params)
# ════════════════════════════════════════════════════════════
SECTION_LOCAL = "8387"
BOCM_RSS      = "https://www.bocm.es/boletines.rss"

def build_search_url(keyword, date_from, date_to):
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")
    params = (
        f"search_api_views_fulltext_1={quote(keyword)}"
        f"&field_bulletin_field_date%5Bdate%5D={df}"
        f"&field_bulletin_field_date_1%5Bdate%5D={dt}"
        f"&field_orden_seccion={SECTION_LOCAL}"
        f"&field_orden_apartado_1=All"
        f"&field_orden_tipo_disposicin_1=All"
        f"&field_orden_organo_y_organismo_1_1=All"
        f"&field_orden_organo_y_organismo_1=All"
        f"&field_orden_organo_y_organismo_2=All"
        f"&field_orden_apartado_adm_local_3=All"
        f"&field_orden_organo_y_organismo_3=All"
        f"&field_orden_apartado_y_organo_4=All"
        f"&field_orden_organo_5=All"
    )
    return f"{BOCM_BASE}/advanced-search?{params}"

def build_page_url(keyword, date_from, date_to, page):
    """Pagination URL — exact path format confirmed from Step 4."""
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")
    kw = quote(keyword)
    return (
        f"{BOCM_BASE}/advanced-search/p"
        f"/field_bulletin_field_date/date__{df}"
        f"/field_bulletin_field_date_1/date__{dt}"
        f"/field_orden_organo_y_organismo_1_1/All"
        f"/field_orden_organo_y_organismo_1/All"
        f"/field_orden_organo_y_organismo_2/All"
        f"/field_orden_organo_y_organismo_3/All"
        f"/field_orden_apartado_y_organo_4/All"
        f"/busqueda/{kw}"
        f"/seccion/{SECTION_LOCAL}"
        f"/apartado/All/disposicion/All/administracion_local/All/organo_5/All"
        f"/search_api_aggregation_2/{kw}"
        f"/page/{page}"
    )

# ════════════════════════════════════════════════════════════
# SEARCH KEYWORDS — evidence-based, comprehensive
#
# LEGAL FRAMEWORK (Ley 9/2001 del Suelo CM, modified by Ley 1/2020):
#
# ❶ LICENCIA DE OBRA MAYOR (Art. 152):
#    Required for new construction, demolition+rebuild, major reform, change of use.
#    Grant signal: "se concede", "se otorga", "expedición de licencia"
#
# ❷ DECLARACIÓN RESPONSABLE URBANÍSTICA (Art. 155, post-Ley 1/2020):
#    Since 2020, replaces licencias for many obra mayor categories.
#    Signal: "declaración responsable de obra mayor", "toma conocimiento"
#    SAME commercial value as a licencia de obra mayor.
#
# ❸ PROYECTO DE URBANIZACIÓN (Reglamento Gestión Urbanística):
#    Infrastructure for entire new neighborhoods — streets, sewers, power, water.
#    Approved by Junta de Gobierno after "aprobación definitiva".
#    Promotor: usually a "Junta de Compensación".
#    Value: typically €10M–€200M. HIGHEST PRIORITY LEAD.
#
# ❹ PLAN ESPECIAL / PLAN PARCIAL (Ley 9/2001, Title II):
#    Definitive approval unlocks construction of entire new sectors.
#    "aprobación definitiva" = construction can legally proceed.
#    "aprobación inicial" = first step, public comment follows (track but lower score).
#
# ❺ LICENCIA DE PRIMERA OCUPACIÓN:
#    Building completed, certified for occupation.
#    Opportunity for finishing trades and MEP suppliers.
# ════════════════════════════════════════════════════════════
SEARCH_KEYWORDS = [
    # Tier A: Explicit licencias de obra mayor (most precise)
    "se concede licencia de obra mayor",
    "se otorga licencia de obra mayor",
    "licencia de obras mayor concedida",
    "concesión licencia obra mayor",
    "se concede licencia urbanística",
    "se otorga licencia urbanística",
    "licencia de obras mayor",
    "licencia de edificación concedida",
    "resolución favorable licencia obras",

    # Tier B: Declaración responsable (post-2020 replacement for licencias)
    "declaración responsable de obra mayor",
    "declaración responsable urbanística de obra mayor",
    "se toma conocimiento de la declaración responsable",

    # Tier C: Urbanismo — HUGE leads (entire neighborhoods)
    "proyecto de urbanización",           # catches "aprobar definitivamente el proyecto de urbanización"
    "junta de compensación",              # always a major development project
    "aprobar definitivamente",            # final approval — any construction doc
    "aprobación definitiva del plan",     # plan especial/parcial final approval
    "plan parcial de reforma interior",   # PERI = major urban transformation
    "plan especial de cambio de uso",     # change of use plan
    "plan especial de reforma interior",  # urban reform plan
    "plan especial para",                 # catches many types of plan especial

    # Tier D: Other valuable types
    "licencia de primera ocupación",      # building done
    "se concede licencia de actividad para nave",        # warehouse/industrial
    "se concede licencia de actividad para almacén",     # warehouse
    "se concede licencia de actividad para centro",      # commercial centers
]

def is_bad_url(url):
    if not url or "bocm.es" not in url: return True
    low = url.lower()
    bad_exts  = (".xml",".css",".js",".png",".jpg",".gif",".ico",".woff",".svg",".zip",".epub")
    bad_paths = ("/advanced-search","/login","/user","/admin","/sites/","/modules/","#","javascript:")
    return any(low.endswith(x) for x in bad_exts) or any(x in low for x in bad_paths)

def url_date_ok(url, date_from):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m:
        try:
            url_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            return url_date >= date_from - timedelta(days=1)
        except ValueError: pass
    return True

def extract_result_links(soup):
    links = []
    for sel in ["a[href*='/boletin/']","a[href*='/anuncio/']","a[href*='/bocm-']",
                ".view-content .views-row a",".view-content a","article h3 a",
                "article h2 a",".field--name-title a","h3.field-content a"]:
        found = soup.select(sel)
        if found:
            for a in found:
                href = a.get("href","")
                if href:
                    full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                    links.append(full)
            if links: break
    if not links:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
            if "bocm.es" in full and any(s in full for s in ["/boletin/","/anuncio/","/bocm-"]):
                links.append(full)
    return links

def search_keyword(keyword, date_from, date_to):
    log(f"  🔎 '{keyword}'")
    seen = set(); urls = []; page = 0; max_pages = 15

    while page < max_pages:
        url = build_search_url(keyword, date_from, date_to) if page == 0 \
              else build_page_url(keyword, date_from, date_to, page)

        r = safe_get(url, timeout=25, backoff_base=8)
        if not r or r.status_code != 200:
            log(f"    No response on page {page} — stopping"); break

        soup  = BeautifulSoup(r.text, "html.parser")
        links = extract_result_links(soup)
        new   = 0
        for link in links:
            if is_bad_url(link): continue
            if not url_date_ok(link, date_from): continue
            if link not in seen:
                seen.add(link); urls.append(link); new += 1

        log(f"    Page {page}: {new} new links (total {len(urls)})")
        if new == 0: break

        has_next = bool(
            soup.select_one("li.pager-next a") or
            soup.select_one(".pager__item--next a") or
            soup.find("a", string=re.compile(r"Siguiente|siguiente|Next|»", re.I))
        )
        if not has_next: break
        page += 1; time.sleep(2)

    return urls

# ════════════════════════════════════════════════════════════
# RSS FEED (supplemental)
# ════════════════════════════════════════════════════════════
def get_rss_pdf_links(date_from, date_to):
    log("📡 Fetching RSS feed…")
    pdf_urls = []
    r = safe_get(BOCM_RSS, timeout=20)
    if not r: log("  ⚠️  RSS unavailable"); return pdf_urls
    try:
        import xml.etree.ElementTree as ET
        root  = ET.fromstring(r.content)
        items = root.findall(".//item") or root.findall(".//entry")
        for item in items:
            pub = ""
            for tag in ["pubDate","published","updated","date"]:
                el = item.find(tag)
                if el is not None and el.text: pub = el.text; break
            pub_date = None
            for fmt in ["%a, %d %b %Y %H:%M:%S %z","%a, %d %b %Y %H:%M:%S +0000","%Y-%m-%dT%H:%M:%S%z"]:
                try: pub_date = datetime.strptime(pub[:30], fmt).replace(tzinfo=None); break
                except ValueError: pass
            if not pub_date:
                try:
                    from dateutil import parser as dp
                    pub_date = dp.parse(pub).replace(tzinfo=None)
                except: pass
            if pub_date and (pub_date < date_from or pub_date > date_to): continue
            link_el = item.find("link")
            link    = link_el.text if link_el is not None else ""
            if not link: continue
            br = safe_get(link, timeout=20)
            if not br: continue
            bsoup = BeautifulSoup(br.text, "html.parser")
            for a in bsoup.find_all("a", href=True):
                href = a["href"]
                if ".PDF" in href.upper() or ".pdf" in href:
                    full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                    if "bocm.es" in full and full not in pdf_urls:
                        pdf_urls.append(full)
            time.sleep(1)
    except Exception as e:
        log(f"  ⚠️  RSS error: {e}")
    log(f"  📡 RSS: {len(pdf_urls)} links")
    return pdf_urls

# ════════════════════════════════════════════════════════════
# FETCH — JSON-LD first, PDF as fallback
#
# KEY DISCOVERY: BOCM embeds full structured data in every HTML page
# as a <script type="application/ld+json"> block.
# The "text" field contains the full clean document text.
# This is far better than PDF scraping — clean text, exact date, no OCR.
#
# URL patterns:
#   HTML page: https://www.bocm.es/bocm-20260325-46
#   PDF:       https://www.bocm.es/boletin/CM_Orden_BOCM/2026/03/25/BOCM-20260325-46.PDF
# ════════════════════════════════════════════════════════════
def extract_date_from_url(url):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return ""

def extract_jsonld(soup):
    """Extract JSON-LD structured data from BOCM HTML page."""
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string)
            if isinstance(data, list):
                data = data[0]
            if data.get("text"):
                text = data["text"]
                date = (data.get("datePublished","") or "").replace("/","-")
                name = data.get("name","")
                # Extract PDF URL from encoding list
                pdf_url = None
                for enc in data.get("encoding", []):
                    cu = enc.get("contentUrl","")
                    if cu.upper().endswith(".PDF"):
                        pdf_url = cu
                        break
                return text, date[:10], name, pdf_url
        except Exception:
            continue
    return None, None, None, None

def extract_pdf_text(url):
    try:
        r = get_session().get(url, timeout=45, verify=False, allow_redirects=True,
                              headers={**HEADERS, "Accept":"application/pdf,*/*"})
        if r.status_code != 200 or len(r.content) < 400: return ""
        if r.content[:4] != b"%PDF": return ""
        txt = ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for pg in pdf.pages[:15]:
                t = pg.extract_text()
                if t: txt += t + "\n"
        return txt[:15000]
    except Exception as e:
        log(f"    PDF error: {e}"); return ""

def fetch_announcement(url):
    """
    Fetch a BOCM entry. Returns (text, pdf_url, pub_date, doc_title).

    Strategy:
    1. If HTML page (bocm.es/bocm-YYYYMMDD-NN) → extract JSON-LD (fastest, cleanest)
    2. If direct PDF → extract with pdfplumber
    3. Fallback: parse HTML body + find PDF link
    """
    url_low = url.lower()

    # ── Direct PDF ─────────────────────────────────────────────────────────
    if url_low.endswith(".pdf"):
        text     = extract_pdf_text(url)
        pub_date = extract_date_from_url(url)
        return text, url, pub_date, ""

    # ── HTML page ──────────────────────────────────────────────────────────
    r = safe_get(url, timeout=25)
    if not r or r.status_code != 200:
        return "", None, "", ""

    soup = BeautifulSoup(r.text, "html.parser")

    # Try JSON-LD first (BOCM structured data — always present on entry pages)
    jtext, jdate, jname, jpdf = extract_jsonld(soup)
    if jtext and len(jtext.strip()) > 100:
        # Clean the text
        text = re.sub(r'\s+', ' ', jtext).strip()
        pub_date = jdate or extract_date_from_url(url)
        return text, jpdf or url, pub_date, jname or ""

    # ── HTML body fallback ──────────────────────────────────────────────────
    parts = []
    for sel in [".field--name-body",".field-name-body",".contenido-boletin",
                ".anuncio-texto",".anuncio","article .content","article","main","#content"]:
        el = soup.select_one(sel)
        if el:
            parts.append(el.get_text(separator=" ", strip=True))
            break
    if not parts:
        for tag in soup.find_all(["nav","header","footer","aside","script","style"]):
            tag.decompose()
        parts.append(soup.get_text(separator=" ", strip=True)[:10000])

    pub_date = extract_date_from_url(url)
    if not pub_date:
        m = re.search(r'\b(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})\b', " ".join(parts))
        if m: pub_date = m.group(0)

    pdf_url = None
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if ".pdf" in h.lower() or ".PDF" in h:
            pdf_url = urljoin(BOCM_BASE, h) if h.startswith("/") else h; break

    if pdf_url:
        ptext = extract_pdf_text(pdf_url)
        if ptext: parts.append(ptext)

    return re.sub(r'\s+', ' ', " ".join(parts)).strip(), pdf_url, pub_date, ""

# ════════════════════════════════════════════════════════════
# CLASSIFICATION  —  5-stage filter
#
# UNDERSTANDING THE BOCM DOCUMENT TYPES:
#
# ─ SOLICITUD (application, NOT a grant):
#   "se ha SOLICITADO licencia" + "a fin de que quienes se consideren afectados"
#   → Public notice period. Nothing approved yet. → REJECT
#
# ─ CONCESIÓN (grant):
#   "se CONCEDE licencia de obra mayor" → LEAD ✓
#
# ─ DECLARACIÓN RESPONSABLE (post-Ley 1/2020):
#   Since Oct 2020, many obra mayor acts use DR instead of licencia.
#   "declara bajo su responsabilidad... obra mayor"
#   "se toma conocimiento de declaración responsable"
#   → Same as a licencia grant. → LEAD ✓
#
# ─ PROYECTO DE URBANIZACIÓN (definitively approved):
#   "aprobar definitivamente el proyecto de urbanización... con un presupuesto de X euros"
#   Promotor = Junta de Compensación. Infrastructure for entire neighborhoods.
#   → HIGHEST VALUE LEAD ✓
#
# ─ PLAN ESPECIAL / PARCIAL (definitively approved):
#   "aprobación definitiva del plan especial/parcial"
#   → Future construction guaranteed. → HIGH VALUE LEAD ✓
#
# ─ "DEJAR SIN EFECTO" (correction of previous agreement):
#   Appears in urbanismo docs as "Quinto.—Dejar sin efecto el Acuerdo de [date]"
#   This means the CURRENT document CORRECTS a previous one. The construction IS approved.
#   → NOT A DENIAL. This is normal Spanish administrative practice. → KEEP ✓
#
# ─ "APROBACIÓN INICIAL" vs "APROBACIÓN DEFINITIVA":
#   Inicial = first step, public comment follows → track at lower tier
#   Definitiva = FINAL APPROVAL → construction can proceed → gold tier
# ════════════════════════════════════════════════════════════

# Stage 1: Hard administrative noise — impossible to be construction permits
HARD_REJECT = [
    # Financial admin
    "subvención", "subvenciones para", "convocatoria de subvención",
    "bases reguladoras para la concesión de ayudas",
    "ayuda económica", "ayudas económicas para",
    "aportación dineraria", "aportaciones dinerarias",
    "modificación presupuestaria",
    "suplemento de crédito", "presupuesto municipal",
    "modificación del presupuesto",
    "modificación del plan estratégico de subvenciones",
    # HR / staffing
    "nombramiento funcionari", "cese ", "personal laboral",
    "plantilla de personal", "oferta de empleo público",
    # Tax / fiscal (standalone — not a building permit)
    "ordenanza fiscal reguladora",
    "impuesto sobre actividades económicas",
    "inicio del período voluntario de pago de",
    "matrícula del impuesto",
    "precios públicos para festejos",
    # Events / sports / culture
    "festejos taurinos", "certamen de teatro", "certamen de",
    "convocatoria de premios", "actividades deportivas",
    "acción social en el ámbito del deporte",
    "clubes y asociaciones deportivas federadas",
    "actividades educativas", "proyectos educativos",
    # Governance
    "juez de paz", "comisión informativa permanente",
    "composición del pleno", "composición de las comisiones",
    "encomienda de gestión",
    "modificación de la composición",
    # Transport (not construction)
    "eurotaxi", "autotaxi", "vehículos autotaxi",
    # Pure planning norms (no specific project)
    "normas subsidiarias de urbanismo",          # policy document, not a permit
    "criterio interpretativo vinculante",         # planning interpretation, not a permit
    "regulación del deber de conservación",       # maintenance obligation, not a permit
    # Procurement (not a permit)
    "licitación", "pliego de cláusulas administrativas",
    # Corrección de errores (typo fix in old document)
    "corrección de errores del bocm",
    "corrección de hipervínculo",
    # Approval of plans to allow subventions (administrative)
    "aprobación definitiva del plan estratégico de subvenciones",
    "aprobación inicial del expediente de modificación del anexo",
]

# Stage 2: Application phase — permit SOLICITED but NOT granted
# IMPORTANT: These are the "solicitud" documents with 20-day public comment period.
# BUT: "en período de información pública" also appears in urbanismo APPROVALS
# (they mention the past public comment period that was completed).
# We must only reject if the document IS the public notice, not just mentions it.
APPLICATION_SIGNALS = [
    "se ha solicitado licencia",
    "ha solicitado licencia",
    "se solicita licencia de",
    "lo que se hace público en cumplimiento de lo preceptuado",  # exact boilerplate
    "a fin de que quienes se consideren afectados de algún modo",  # exact boilerplate
    "quienes se consideren afectados puedan formular",
    "formular por escrito las observaciones pertinentes",
    "durante el plazo de veinte días",
    "durante el plazo de treinta días",
    "presentarán en el registro general del ayuntamiento",
]

# Stage 3: Denial
DENIAL_SIGNALS = [
    "denegación de licencia", "se deniega la licencia",
    "inadmisión", "desestimación de la solicitud",
    "se desestima", "resolución denegatoria",
    "no se concede", "caducidad de la licencia",
    "archivo del expediente",
]
# NOTE: "dejar sin efecto" is NOT a denial — it's a correction of a previous agreement.
# The Las Tablas Oeste document explicitly says "Quinto.—Dejar sin efecto el Acuerdo
# de 29 de enero de 2026..." meaning the CURRENT document supersedes and is the valid one.

# Stage 4: Grant signals — must be present
GRANT_SIGNALS = [
    # Licencias
    "se concede", "se otorga", "se autoriza",
    "concesión de licencia", "licencia concedida",
    "se resuelve favorablemente", "otorgamiento de licencia",
    "se acuerda conceder", "se acuerda otorgar",
    "resolución estimatoria", "expedición de licencia",
    # Urbanismo approvals (Ley 9/2001)
    "aprobar definitivamente",           # "aprobar definitivamente el proyecto de urbanización..."
    "aprobación definitiva",             # "aprobación definitiva del plan especial..."
    "aprobación inicial",                # lower tier but still a lead
    "aprobación provisional",            # intermediate step, worth tracking
    "se aprueba definitivamente",        # variation
    # Declaración responsable (Ley 1/2020) — valid as of Oct 2020
    "declaración responsable de obra mayor",
    "declaración responsable urbanística",
    "toma de conocimiento de la declaración responsable",
    # Specific project phrases that imply approval
    "con un presupuesto",               # appears in urbanización approvals
    "promovido por la junta de compensación",  # always an approved development
]

# Stage 5: Must have construction-specific content
CONSTRUCTION_SIGNALS = [
    # Obra mayor
    "obra mayor", "obras mayores", "licencia de obras",
    "licencia urbanística", "licencia de edificación",
    "declaración responsable",
    # New construction
    "nueva construcción", "nueva planta", "obra nueva", "edificio de nueva",
    "viviendas de nueva", "edificio plurifamiliar", "complejo residencial",
    # Urbanismo (infrastructure for new neighborhoods)
    "proyecto de urbanización", "obras de urbanización",
    "unidad de ejecución", "área de planeamiento específico",
    "junta de compensación",
    # Reform / rehab
    "rehabilitación integral", "rehabilitación de edificio",
    "reforma integral", "reforma estructural",
    "demolición y construcción", "demolición y nueva planta",
    "ampliación de edificio",
    # Industrial / logistics
    "nave industrial", "naves industriales", "almacén industrial",
    "centro logístico", "plataforma logística", "parque empresarial",
    "instalación industrial",
    # Commercial / other
    "hotel", "bloque de viviendas", "complejo residencial",
    "cambio de uso", "primera ocupación",
    "plan especial", "plan parcial",
    "proyecto urbanístico",
    # Budget indicators
    "presupuesto de ejecución material", "p.e.m", "base imponible del icio",
    "base imponible icio",
]

# Small activity licences — NOT interesting for construction suppliers
SMALL_ACTIVITY = [
    "peluquería", "barbería", "salón de belleza", "estética",
    "pastelería", "panadería", "carnicería", "pescadería",
    "frutería", "estanco", "locutorio", "quiosco",
    "taller mecánico", "academia de idiomas", "academia de danza",
    "centro de yoga", "pilates", "clínica dental", "consulta médica",
    "farmacia", "bar ", "cafetería", "restaurante",
    "heladería", "pizzería", "kebab",
    "lavandería", "tintorería", "zapatería", "cerrajería",
    "papelería", "floristería", "gestoría",
]

def classify_permit(text):
    """
    5-stage classification.
    Returns (is_lead: bool, reason: str, tier: int 1-5)
    Tier 1 = gold (urbanización/plan definitivo), Tier 5 = marginal.
    """
    t = text.lower()

    # ── Stage 1: Hard admin noise ─────────────────────────────────────────────
    for kw in HARD_REJECT:
        if kw in t:
            return False, f"Admin noise: '{kw}'", 0

    # ── Stage 2: Application phase (solicitud, NOT a grant) ───────────────────
    # Detect the specific boilerplate of "solicitud" notices.
    # Must match 2+ signals to avoid false positives on urbanismo approval docs
    # that mention past public comment periods.
    app_count = sum(1 for kw in APPLICATION_SIGNALS if kw in t)
    if app_count >= 2:
        return False, f"Application phase (solicitud not grant): {app_count} signals", 0

    # ── Stage 3: Denial ───────────────────────────────────────────────────────
    for kw in DENIAL_SIGNALS:
        if kw in t:
            return False, f"Denial: '{kw}'", 0

    # ── Stage 4: Grant + construction check ───────────────────────────────────
    has_grant        = any(p in t for p in GRANT_SIGNALS)
    has_construction = any(p in t for p in CONSTRUCTION_SIGNALS)

    if not has_grant:
        return False, "No grant language found", 0
    if not has_construction:
        return False, "Grant language but no construction content (likely subvention)", 0

    # ── Stage 5: Small activity filter (only if no major construction) ────────
    has_major = any(p in t for p in ["obra mayor","nueva construcción","nueva planta",
                                      "nave industrial","proyecto de urbanización",
                                      "rehabilitación integral","plan especial","plan parcial",
                                      "bloque de viviendas","junta de compensación"])
    if not has_major:
        for kw in SMALL_ACTIVITY:
            if kw in t:
                return False, f"Small activity: '{kw}'", 0

    # ── Tier assignment ───────────────────────────────────────────────────────
    # Tier 1: Urbanismo gold — entire neighborhoods, years of supply opportunity
    if any(p in t for p in ["proyecto de urbanización","junta de compensación",
                             "plan parcial","aprobación definitiva del plan"]):
        if any(p in t for p in ["aprobar definitivamente","aprobación definitiva","presupuesto"]):
            return True, "Tier-1: Urbanismo definitivo (neighborhood-scale)", 1

    # Tier 2: Plan especial definitivo — specific major transformation
    if any(p in t for p in ["plan especial","reforma interior","área de planeamiento"]):
        if any(p in t for p in ["definitiv","presupuesto","pem"]):
            return True, "Tier-2: Plan especial / PERI definitivo", 2

    # Tier 3: Obra mayor nueva construcción / industrial (ground-up)
    if any(p in t for p in ["nueva construcción","nueva planta","nave industrial",
                             "bloque de viviendas","demolición y construcción",
                             "rehabilitación integral"]):
        return True, "Tier-3: Obra mayor nueva construcción / industrial", 3

    # Tier 4: Obra mayor rehabilitación / cambio de uso
    if any(p in t for p in ["obra mayor","reforma integral","cambio de uso",
                             "ampliación de edificio","declaración responsable"]):
        return True, "Tier-4: Obra mayor rehabilitación / cambio de uso", 4

    # Tier 5: Primera ocupación / activity licence for large commercial
    return True, "Tier-5: Licencia primera ocupación / actividad grande", 5


# ════════════════════════════════════════════════════════════
# LEAD SCORING  (0–100)
# ════════════════════════════════════════════════════════════
def score_lead(p):
    score = 0
    t = ((p.get("description","") or "") + " " + (p.get("permit_type","") or "")).lower()

    # Project type
    if any(k in t for k in ["proyecto de urbanización","junta de compensación",
                              "plan parcial","plan especial reforma interior"]):
        score += 40
    elif any(k in t for k in ["nave industrial","centro logístico","parque empresarial"]):
        score += 35
    elif any(k in t for k in ["nueva construcción","nueva planta","bloque de viviendas",
                               "rehabilitación integral"]):
        score += 28
    elif any(k in t for k in ["plan especial","reforma integral","cambio de uso",
                               "obra mayor"]):
        score += 18
    elif "primera ocupación" in t:
        score += 10

    # Budget / value
    val = p.get("declared_value_eur")
    if val and isinstance(val, (int, float)):
        if val >= 10_000_000: score += 35
        elif val >= 2_000_000: score += 28
        elif val >= 500_000:  score += 20
        elif val >= 100_000:  score += 12
        elif val >= 50_000:   score += 6

    # Data completeness
    if p.get("address"):       score += 8
    if p.get("applicant"):     score += 8
    if p.get("expediente"):    score += 2
    if p.get("municipality") not in (None, "", "Madrid"):
        score += 2  # specific town = actionable

    return min(score, 100)


# ════════════════════════════════════════════════════════════
# DATA EXTRACTION
# ════════════════════════════════════════════════════════════
MONTHS_ES = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
             "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}

def parse_spanish_date(s):
    if not s: return ""
    if re.match(r"\d{4}-\d{2}-\d{2}$", s): return s
    m = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', s, re.I)
    if m:
        mo = MONTHS_ES.get(m.group(2).lower())
        if mo:
            try: return datetime(int(m.group(3)), mo, int(m.group(1))).strftime("%Y-%m-%d")
            except: pass
    m = re.search(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', s)
    if m:
        try: return datetime(int(m.group(3)),int(m.group(2)),int(m.group(1))).strftime("%Y-%m-%d")
        except: pass
    return s[:10] if len(s) >= 10 else s

def extract_municipality(text):
    """Extract Ayuntamiento name. BOCM PDFs always have 'AYUNTAMIENTO DE [NAME]' in header."""
    patterns = [
        r'AYUNTAMIENTO\s+DE\s+([A-ZÁÉÍÓÚÑ][A-ZÁÉÍÓÚÑ\s\-]+?)(?:\n|\s{2,}|LICENCIAS|OTROS|CONTRATACIÓN|URBANISMO)',
        r'ayuntamiento de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:\.|,|\n)',
        r'(?:en|En)\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?),\s+a\s+\d{1,2}\s+de\s+\w+\s+de\s+\d{4}',
        r'Distrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s\-]+?)(?:,|\.|$)',
    ]
    noise = {"null","madrid","comunidad","boletín","oficial","administración","spain","españa"}
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            name = m.group(1).strip().rstrip(".,; ").strip()
            if name.lower() not in noise and 3 < len(name) < 60:
                return name.title()
    return "Madrid"

def extract_expediente(text):
    """Extract expediente number: 'Expediente: 511/2024/30810'"""
    m = re.search(r'[Ee]xpediente[:\s]+(\d{2,6}/\d{4}/\d{3,8})', text)
    if m: return m.group(1)
    m = re.search(r'[Ee]xp\.\s*n[úu]?m\.?\s*([\d\-/]+)', text)
    if m: return m.group(1)
    return ""

def extract_pem_value(text):
    """
    Extract PEM (Presupuesto de Ejecución Material) — the TRUE construction cost.
    For multi-stage projects (urbanización), sums all stage PEMs.

    Precedence:
    1. Explicit "PEM" label with value (most precise)
    2. "presupuesto de ejecución material" 
    3. "base imponible ICIO" (tax base = PEM)
    4. Table with ETAPA rows (sum all Etapa PEM values)
    5. "valorado en X euros"
    6. Generic large amount with "presupuesto"
    """
    c = text

    # Priority 1: Named PEM in table (urbanización multi-etapa)
    # Pattern: "ETAPA 1 25.036.881,15 € ..." or "PEM\nETAPA 1\n25.036.881,15 €"
    etapa_pems = re.findall(
        r'[Ee][Tt][Aa][Pp][Aa]\s*\d+[^\n]*?(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
        c)
    if etapa_pems:
        total = 0
        for vs in etapa_pems:
            v = _parse_euro(vs)
            if v and v >= 10000: total += v
        if total > 0: return round(total, 2)

    # Priority 2: Explicit PEM/ICIO patterns
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)\s*[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'(?:base imponible(?:\s+del\s+ICIO)?|cuota\s+ICIO)\s*[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # Priority 3: "presupuesto, X% IVA incluido, de Y euros" (urbanización docs)
    m = re.search(r'presupuesto,\s*\d+\s*%\s*IVA\s+incluido,\s*de\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*euros', c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    # Priority 4: Generic presupuesto amount
    for pat in [
        r'(?:presupuesto|importe)\s*[:\-]\s*([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{2})?)\s*(?:euros?|€)?',
        r'([0-9]{1,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

    return None

def _parse_euro(s):
    """Parse European number format: '25.036.881,15' or '25,036,881.15' → float"""
    s = s.strip()
    if not s: return None
    if "," in s and "." in s:
        s = s.replace(".","").replace(",",".")
    elif "," in s:
        s = s.replace(",",".")
    else:
        s = s.replace(".","")
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None

def keyword_extract(text, url, pub_date):
    res = {
        "address":            None,
        "applicant":          None,
        "municipality":       extract_municipality(text),
        "permit_type":        "obra mayor",
        "declared_value_eur": extract_pem_value(text),
        "date_granted":       parse_spanish_date(pub_date) or extract_date_from_url(url),
        "description":        None,
        "confidence":         "medium",
        "source_url":         url,
        "extraction_mode":    "keyword",
        "lead_score":         0,
        "expediente":         extract_expediente(text),
    }
    c = re.sub(r'\s+', ' ', text)

    # ── Address ────────────────────────────────────────────────────────────────
    for pat in [
        r'(?:calle|c/)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+[a-zA-Z]?)',
        r'(?:avenida|av\.?|avda\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:paseo|po\.?|pso\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:plaza|pl\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:camino|glorieta|ronda|travesía|urbanización)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40})[,\s]+n[úu]?[mº°]?\.?\s*(\d+)',
        # For urbanismo: "Área de Planeamiento Específico 08.21 "Las Tablas Oeste", Distrito de Fuencarral"
        r'Área de\s+[Pp]laneamiento\s+[A-Za-záéíóúñ\s]+[\"\']([^\"\']{3,80})[\"\']',
        r'[Uu]nidad de [Ee]jecución\s+(?:n[úu]?[mº°]\.?\s*)?(\w+)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            res["address"] = m.group(0).strip().rstrip(".,;"); break

    # If no street address, use district/area reference for urbanismo
    if not res["address"]:
        for pat in [
            r'[Dd]istrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\-\s]+?)(?:,|\.|$)',
            r'parcela\s+(?:situada\s+en\s+)?([A-Za-záéíóúñ\s,º]+\d+)',
        ]:
            m = re.search(pat, c, re.I)
            if m:
                res["address"] = m.group(0).strip().rstrip(".,;"); break

    # ── Applicant / Promotor ─────────────────────────────────────────────────
    # For urbanismo: "promovido por la Junta de Compensación [NAME]"
    # For licencias: "a instancia de [NAME]" or "don/doña [NAME]" or "[COMPANY SA/SL]"
    for pat in [
        r'(?:promovido por|promotora?|a cargo de)\s+(?:la\s+)?([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{5,80})',
        r'(?:a instancia de|solicitante|interesado[/a]*|presentado por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:[Jj]unta de [Cc]ompensación\s+[\"\']?)([A-ZÁÉÍÓÚÑ][^\"\']{3,60}[\"\']?)',
        r'(?:don|doña|d\.|dña\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})',
        r'([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s&,\-]{3,50}(?:\bS\.?[AL]\.?U?\.?\b|\bSLU\b|\bS\.?L\.?\b|\bS\.?A\.?\b))',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            a = m.group(1).strip().rstrip(".,;\"'")
            if 3 < len(a) < 90:
                # Check if it includes "Junta de Compensación" in the match
                if "junta de compensación" in pat.lower():
                    a = f"Junta de Compensación {a}"
                res["applicant"] = a; break

    # ── Permit type ───────────────────────────────────────────────────────────
    t = c.lower()
    if any(p in t for p in ["proyecto de urbanización","obras de urbanización","junta de compensación"]):
        res["permit_type"] = "urbanización"
    elif any(p in t for p in ["plan parcial","plan especial de reforma interior","peri"]):
        res["permit_type"] = "plan especial / parcial"
    elif any(p in t for p in ["plan especial de cambio de uso","cambio de uso de local a vivienda",
                               "cambio de uso de locales a vivienda"]):
        res["permit_type"] = "cambio de uso"
    elif any(p in t for p in ["plan especial para","plan especial de"]):
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["nave industrial","almacén industrial","plataforma logística",
                               "centro logístico","naves industriales","parque empresarial"]):
        res["permit_type"] = "obra mayor industrial"
    elif any(p in t for p in ["nueva construcción","nueva planta","obra nueva","edificio de nueva",
                               "viviendas de nueva","edificio plurifamiliar"]):
        res["permit_type"] = "obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación integral","restauración de edificio","reconstrucción",
                               "reforma integral","reforma estructural"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["reforma","ampliación","cambio de uso","modificación de edificio"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif "primera ocupación" in t:
        res["permit_type"] = "licencia primera ocupación"
    elif any(p in t for p in ["declaración responsable"]):
        res["permit_type"] = "declaración responsable obra mayor"
    elif any(p in t for p in ["actividad","local comercial","establecimiento"]):
        res["permit_type"] = "licencia de actividad"

    # ── Description — commercial and action-oriented ──────────────────────────
    desc = None
    # For urbanismo: extract the specific project name + budget
    m = re.search(r'(?:aprobar definitivamente|aprobación definitiva)\s+(?:el|del)\s+([^\.]{20,300})', c, re.I)
    if m: desc = "Aprobación definitiva: " + m.group(1).strip()[:250]

    if not desc:
        m = re.search(r'licencia(?:\s+de\s+obra\s+mayor)?\s+para\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()

    if not desc:
        m = re.search(
            r'(?:obras? de|construcción de|rehabilitación de|reforma de|instalación de|ampliación de|urbanización de)\s+[^\.]{15,250}',
            c, re.I)
        if m: desc = m.group(0).strip()

    if not desc:
        for gp in ["se concede","se otorga","se acuerda conceder","se aprueba definitivamente"]:
            idx = t.find(gp)
            if idx >= 0:
                desc = c[idx:idx+300].strip(); break

    res["description"] = (desc or c[:250]).strip()[:350]
    res["lead_score"]  = score_lead(res)
    return res


def ai_extract(text, url, pub_date):
    if not USE_AI:
        return keyword_extract(text, url, pub_date)

    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)

        sys_prompt = """You are an elite construction intelligence analyst for Spain.
You read official Madrid regional bulletin (BOCM) documents to extract actionable leads for construction supply companies.

CRITICAL RULES:
1. Return ONLY valid JSON — no markdown, no explanations.
2. If this document is NOT about a specific construction project (e.g. it's a subvention, HR appointment, tax ordinance), return: {"permit_type":"none","confidence":"low"} — nothing else.
3. Fields must use EXACTLY these key names: applicant, address, municipality, permit_type, description, declared_value_eur, date_granted, confidence, lead_score, expediente.
4. "permit_type": choose from:
   "urbanización" | "plan especial" | "plan especial / parcial" | "obra mayor nueva construcción" |
   "obra mayor industrial" | "obra mayor rehabilitación" | "cambio de uso" |
   "declaración responsable obra mayor" | "licencia primera ocupación" | "licencia de actividad" | "none"
5. "declared_value_eur": Extract ONLY the PEM (Presupuesto de Ejecución Material).
   For multi-stage projects, SUM all stage PEMs (Etapa 1 + Etapa 2...) — NOT the IVA-included total.
   For "proyecto de urbanización" the PEM is in the table rows, NOT the "con presupuesto X IVA incluido" figure.
   Return a NUMBER (float). null if not found.
6. "applicant": The PROMOTOR — who commissioned the project. For urbanización = "Junta de Compensación [NAME]".
   For licencias = the person/company who applied. NEVER leave blank — use "Ayuntamiento" if council-driven.
7. "address": Full street address. For urbanismo = district/area name (e.g. "Área 08.21 Las Tablas Oeste, Fuencarral-El Pardo").
8. "municipality": The specific town/city within CM (e.g. "Getafe", "Tres Cantos", "Madrid"). NOT "Comunidad de Madrid".
9. "description": ONE commercial sentence describing WHAT will be built and WHY it matters commercially.
   Examples: "Urbanización de 74ha en Fuencarral-El Pardo con €74M PEM — inicio obras previsto 24-36 meses"
             "Nave industrial de 8.500m² para uso logístico en polígono de Alcobendas"
             "Nueva planta residencial de 32 viviendas VPO con garaje subterráneo"
10. "lead_score": Integer 0-100. High score = large budget + new construction + industrial/urbanización.
    Low score = small activity licence, no budget info, uncertain status.
11. "expediente": The expediente number if present (e.g. "511/2024/30810"). null if not found.
12. "confidence": "high" (all key fields found, grant confirmed), "medium" (some missing), "low".

IMPORTANT NOTES ON BOCM DOCUMENT TYPES:
- "se ha SOLICITADO" + "plazo de veinte días" = APPLICATION phase, NOT a grant → return permit_type:"none"
- "aprobar DEFINITIVAMENTE el proyecto de urbanización" = FINAL APPROVAL → urbanización
- "aprobar INICIALMENTE el plan especial" = first step → plan especial (still a lead, lower confidence)
- "Quinto.—Dejar sin efecto el Acuerdo de [date]" = CORRECTION of previous error. The CURRENT document IS valid. Do NOT reject.
- "declaración responsable de obra mayor" = valid as of Ley 1/2020, same as licencia de obra mayor"""

        user_prompt = f"URL: {url}\n\nTexto BOCM:\n{text[:5500]}"

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":sys_prompt},
                      {"role":"user","content":user_prompt}],
            temperature=0, max_tokens=700,
            response_format={"type":"json_object"})

        d = json.loads(resp.choices[0].message.content.strip())

        # Reject if AI says not a permit
        if str(d.get("permit_type","")).lower() in ("none","null","","otro","n/a"):
            log("    AI: not a construction permit → skip")
            return None

        d["source_url"]      = url
        d["extraction_mode"] = "ai"
        dg = d.get("date_granted") or pub_date
        d["date_granted"] = parse_spanish_date(str(dg)) if dg else extract_date_from_url(url)

        val = d.get("declared_value_eur")
        if isinstance(val, str):
            try:
                v = val.replace(".","").replace(",",".").replace("€","").strip()
                d["declared_value_eur"] = float(re.sub(r'[^\d.]','',v)) if v else None
            except: d["declared_value_eur"] = None

        if not d.get("lead_score"):
            d["lead_score"] = score_lead(d)
        if not d.get("municipality"):
            d["municipality"] = extract_municipality(text)
        if not d.get("expediente"):
            d["expediente"] = extract_expediente(text)
        return d

    except Exception as e:
        log(f"    AI error ({e}) → keyword fallback")
        return keyword_extract(text, url, pub_date)

def extract(text, url, pub_date):
    return ai_extract(text, url, pub_date) if USE_AI else keyword_extract(text, url, pub_date)

# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS  —  16 columns
# ════════════════════════════════════════════════════════════
HDRS = [
    "Date Granted","Municipality","Full Address","Applicant",
    "Permit Type","Declared Value PEM (€)","Est. Build Value (€)",
    "Maps Link","Description","Source URL","PDF URL",
    "Mode","Confidence","Date Found","Lead Score","Expediente",
]
_ws = None; _seen_urls = set()

def get_sheet():
    global _ws
    if _ws: return _ws
    sa = os.environ.get("GCP_SERVICE_ACCOUNT_JSON","").strip()
    if not sa: log("❌ GCP_SERVICE_ACCOUNT_JSON not set"); return None
    try:
        info  = json.loads(sa)
        creds = SACredentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"])
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        try:    ws = sh.worksheet("Permits")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet("Permits", 1000, 20)
        if ws.row_values(1) != HDRS:
            ws.update(values=[HDRS], range_name="A1"); log("✅ Headers written")
        else:
            log("✅ Sheet connected")
        _ws = ws; return _ws
    except Exception as e:
        log(f"❌ Sheet: {e}"); return None

def load_seen():
    global _seen_urls
    ws = get_sheet()
    if not ws: return
    try:
        _seen_urls = set(u.strip() for u in ws.col_values(10)[1:] if u.strip())
        log(f"✅ {len(_seen_urls)} existing URLs loaded")
    except Exception as e:
        log(f"⚠️  load_seen: {e}")

def write_permit(p, pdf_url=""):
    ws  = get_sheet()
    url = p.get("source_url","")
    if url in _seen_urls:
        log(f"  ⏭️  Dup: {url[-60:]}"); return False

    dec  = p.get("declared_value_eur")
    # Est. build value: PEM / 0.03 approximates total project cost
    # (PEM is ~3% of what supply companies can quote against)
    est  = round(dec / 0.03) if dec and isinstance(dec,(int,float)) and dec > 0 else ""
    addr = p.get("address") or ""
    muni = p.get("municipality") or "Madrid"
    maps = ""
    if addr:
        maps = ("https://www.google.com/maps/search/"
                + (addr + " " + muni + " España").replace(" ","+").replace(",",""))

    row = [
        p.get("date_granted",""),
        muni, addr,
        p.get("applicant") or "",
        p.get("permit_type") or "obra mayor",
        dec or "", est, maps,
        (p.get("description") or "")[:350],
        url, pdf_url or "",
        p.get("extraction_mode","keyword"),
        p.get("confidence",""),
        datetime.now().strftime("%Y-%m-%d %H:%M"),
        p.get("lead_score", 0),
        p.get("expediente",""),
    ]
    try:
        if ws:
            ws.append_row(row, value_input_option="USER_ENTERED")
            _seen_urls.add(url)
            # Row colour by lead score
            try:
                rn = len(ws.get_all_values())
                sc = p.get("lead_score", 0)
                if sc >= 65:   rb,gb,bb = 0.80, 0.93, 0.80   # green  — gold lead
                elif sc >= 40: rb,gb,bb = 1.00, 0.96, 0.76   # amber  — good lead
                elif sc >= 20: rb,gb,bb = 1.00, 1.00, 0.85   # yellow — marginal
                else:          rb,gb,bb = 0.98, 0.93, 0.93   # pink   — weak
                ws.spreadsheet.batch_update({"requests":[{"repeatCell":{
                    "range":{"sheetId":ws.id,"startRowIndex":rn-1,"endRowIndex":rn},
                    "cell":{"userEnteredFormat":{"backgroundColor":{"red":rb,"green":gb,"blue":bb}}},
                    "fields":"userEnteredFormat.backgroundColor"}}]})
            except: pass
        _dec_str = f"€{dec:,.0f}" if dec else "N/A"
        log(f"  💾 [{p.get('lead_score',0):02d}pts] {muni} | {addr[:40]} | {p.get('permit_type','?')[:20]} | {_dec_str}")
        return True
    except Exception as e:
        log(f"  ❌ Write: {e}"); return False

# ════════════════════════════════════════════════════════════
# EMAIL DIGEST
# ════════════════════════════════════════════════════════════
def send_digest():
    ws = get_sheet()
    if not ws: log("❌ No sheet"); return
    try:
        rows   = ws.get_all_values()
        if len(rows) < 2: log("⚠️  Sheet empty"); return
        cutoff = datetime.now() - timedelta(days=7)
        recent = []
        for row in rows[1:]:
            if len(row) < 14: continue
            try:
                if datetime.strptime(row[13][:10],"%Y-%m-%d") >= cutoff:
                    recent.append(row)
            except: pass

        def get_val(r):
            try:
                s = str(r[5]).replace(".","").replace(",",".")
                return float(re.sub(r'[^\d.]','',s)) if s else 0.0
            except: return 0.0

        def get_score(r):
            try: return int(r[14]) if len(r) > 14 and r[14] else 0
            except: return 0

        recent.sort(key=get_score, reverse=True)
        total = sum(get_val(r) for r in recent)
        log(f"📧 Digest: {len(recent)} permits, €{int(total):,} total PEM")

        rhtml = ""
        for r in recent:
            raw_v = str(r[5]).strip() if len(r) > 5 and r[5] else ""
            if raw_v:
                _cv = re.sub(r'[^\d.]', '', raw_v.replace('.', '').replace(',', '.'))
                dec = f"€{int(float(_cv)):,}" if _cv else "—"
            else:
                dec = "—"
            raw_e = str(r[6]).strip() if len(r) > 6 and r[6] else ""
            if raw_e:
                _ce = re.sub(r'[^\d.]', '', raw_e.replace('.', '').replace(',', '.'))
                est = f"€{int(float(_ce)):,}" if _ce else "—"
            else:
                est = "—"
            sc    = get_score(r)
            sc_c  = "#1b5e20" if sc >= 65 else "#e65100" if sc >= 40 else "#b71c1c"
            sc_bg = "#e8f5e9" if sc >= 65 else "#fff3e0" if sc >= 40 else "#fce4ec"
            expd  = r[15] if len(r) > 15 and r[15] else ""
            rhtml += f"""<tr style="border-bottom:1px solid #eee">
              <td style="padding:10px 7px;font-weight:600;font-size:13px">{r[1] or "—"}</td>
              <td style="padding:10px 7px;font-size:12px;color:#333">{r[2] or "—"}</td>
              <td style="padding:10px 7px;font-size:12px;color:#444">{r[3] or "—"}</td>
              <td style="padding:10px 7px"><span style="background:#e3f2fd;color:#0d47a1;padding:3px 7px;border-radius:10px;font-size:11px;white-space:nowrap">{r[4] or "—"}</span></td>
              <td style="padding:10px 7px;font-weight:700;color:#1565c0;font-size:14px">{dec}</td>
              <td style="padding:10px 7px;font-size:12px;color:#555">{(r[8] or "")[:140]}</td>
              <td style="padding:10px 7px;text-align:center"><span style="background:{sc_bg};color:{sc_c};padding:3px 8px;border-radius:10px;font-size:12px;font-weight:700">{sc}</span></td>
              <td style="padding:10px 7px;white-space:nowrap;font-size:11px;color:#888">{expd}</td>
              <td style="padding:10px 7px;white-space:nowrap">{"<a href='"+r[7]+"' style='color:#1565c0'>📍</a>&nbsp;" if r[7] else ""}{"<a href='"+r[9]+"' style='color:#999;font-size:11px'>BOCM</a>" if r[9] else ""}</td>
            </tr>"""

        ws_d = (datetime.now()-timedelta(days=7)).strftime("%d %b")
        we_d = datetime.now().strftime("%d %b %Y")
        html = f"""<html><body style="font-family:Arial,sans-serif;max-width:1200px;margin:20px auto;color:#1a1a1a">
<div style="background:linear-gradient(135deg,#1565c0,#0d47a1);color:white;padding:24px 28px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">🏗️ ConstructorScout — Oportunidades Madrid</h1>
  <p style="margin:8px 0 0;opacity:.85;font-size:14px">Semana {ws_d} – {we_d} · Ordenado por puntuación de oportunidad</p>
</div>
<div style="display:flex;background:#e3f2fd;border-bottom:2px solid #bbdefb">
  <div style="flex:1;padding:16px 24px;border-right:1px solid #bbdefb">
    <div style="font-size:34px;font-weight:700;color:#1565c0">{len(recent)}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Proyectos detectados</div>
  </div>
  <div style="flex:1;padding:16px 24px;border-right:1px solid #bbdefb">
    <div style="font-size:34px;font-weight:700;color:#1565c0">€{int(total):,}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">PEM total</div>
  </div>
  <div style="flex:1;padding:16px 24px">
    <div style="font-size:34px;font-weight:700;color:#1565c0">€{int(total/0.03):,}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Valor obra estimado</div>
  </div>
</div>
<div style="padding:12px 28px;background:#fffde7;border-left:4px solid #f9a825">
  <p style="margin:0;font-size:13px;color:#555">
  <strong>Verde ≥65pts</strong> (urbanización/plan definitivo · escala barrio) &nbsp;|&nbsp;
  <strong>Naranja ≥40pts</strong> (obra mayor grande) &nbsp;|&nbsp;
  <strong>Amarillo ≥20pts</strong> (obra mayor estándar).
  Contacta al promotor ANTES que tu competencia.
  </p>
</div>
<div style="overflow-x:auto;padding:0 28px 24px">
<table style="width:100%;border-collapse:collapse;min-width:900px">
  <thead><tr style="background:#f5f5f5;text-align:left">
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Municipio</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Dirección/Área</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Promotor</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Tipo</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">PEM</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Descripción</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Score</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Expediente</th>
    <th style="padding:8px 7px;font-size:11px;color:#777;border-bottom:2px solid #e0e0e0">Links</th>
  </tr></thead>
  <tbody>{rhtml or '<tr><td colspan="9" style="padding:24px;text-align:center;color:#aaa">Sin proyectos esta semana</td></tr>'}</tbody>
</table></div>
<div style="padding:14px 28px;background:#f9f9f9;font-size:12px;color:#888;border-top:1px solid #e8e8e8">
  <strong>ConstructorScout</strong> — Datos extraídos del BOCM (registros públicos oficiales CM Madrid).<br>
  PEM = Presupuesto de Ejecución Material (coste real obra sin IVA ni gastos generales).
  Est. Obra = PEM / 0.03 (proxy del valor total del proyecto).
</div></body></html>"""

        gf = os.environ.get("GMAIL_FROM","")
        gp = os.environ.get("GMAIL_APP_PASSWORD","")
        gt = os.environ.get(CLIENT_EMAIL_VAR,"")
        if not all([gf,gp,gt]): log("⚠️  Email vars missing"); return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🏗️ ConstructorScout Madrid — {len(recent)} proyectos | €{int(total):,} PEM | {ws_d}–{we_d}"
        msg["From"] = gf; msg["To"] = gt
        msg.attach(MIMEText(html,"html","utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com",465) as s:
            s.login(gf,gp)
            s.sendmail(gf,[t.strip() for t in gt.split(",")],msg.as_string())
        log(f"✅ Digest sent to {gt}")
    except Exception as e:
        log(f"❌ Digest error: {e}"); import traceback; traceback.print_exc()

# ════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════
def run():
    if args.digest:
        log("📧 Digest-only mode"); get_sheet(); send_digest(); return

    today     = datetime.now()
    date_to   = today
    date_from = today - timedelta(weeks=WEEKS_BACK)

    log("="*68)
    log(f"🏗️  ConstructorScout Madrid  —  Engine v4")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} ({WEEKS_BACK}w)")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword extraction'}")
    log(f"💰  {'Min €' + f'{MIN_VALUE_EUR:,.0f}' if MIN_VALUE_EUR else 'No value filter'}")
    log("="*68)

    get_sheet(); load_seen()

    # ── COLLECTION ─────────────────────────────────────────────────────────────
    if args.resume and os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE) as f:
            all_urls = json.load(f)
        log(f"▶️  Resuming: {len(all_urls)} URLs from saved queue")
    else:
        all_urls = []; seen_set = set()

        for kw in SEARCH_KEYWORDS:
            urls = search_keyword(kw, date_from, date_to)
            added = 0
            for u in urls:
                if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                    seen_set.add(u); all_urls.append(u); added += 1
            log(f"  +{added} new | '{kw}' | total {len(all_urls)}")
            time.sleep(3)

        rss = get_rss_pdf_links(date_from, date_to)
        rss_added = 0
        for u in rss:
            if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                seen_set.add(u); all_urls.append(u); rss_added += 1
        log(f"  RSS: +{rss_added} | total {len(all_urls)}")

        all_urls = [u for u in all_urls if u not in _seen_urls]
        log(f"\n📋 {len(all_urls)} new URLs to process")

        with open(QUEUE_FILE,"w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved — use --resume to restart from this point if interrupted")

    if not all_urls:
        log("ℹ️  Nothing new.")
        if today.weekday() == 0: send_digest()
        return

    # ── PROCESSING ─────────────────────────────────────────────────────────────
    saved = skipped = errors = 0

    for idx, url in enumerate(all_urls):
        log(f"\n[{idx+1}/{len(all_urls)}] {url}")
        try:
            text, pdf_url, pub_date, doc_title = fetch_announcement(url)

            if not text or len(text.strip()) < 80:
                log("  ⚠️  Too little text — skip"); skipped += 1; continue

            is_lead, reason, tier = classify_permit(text)
            if not is_lead:
                log(f"  ⏭️  {reason}"); skipped += 1; continue

            log(f"  ✅ Tier-{tier} lead — extracting… {doc_title[:60]}")
            p = extract(text, url, pub_date)

            if p is None:
                log("  ⏭️  AI rejected as non-permit"); skipped += 1; continue

            log(f"  [{p.get('lead_score',0):02d}pts] "
                f"muni='{p.get('municipality','?')}' "
                f"type='{p.get('permit_type','?')[:25]}' "
                f"val=€{p.get('declared_value_eur','?')} "
                f"prom='{(p.get('applicant') or '')[:30]}'")

            dec = p.get("declared_value_eur")
            if MIN_VALUE_EUR and dec and isinstance(dec,(int,float)) and dec < MIN_VALUE_EUR:
                log(f"  ⏭️  €{dec:,.0f} below min €{MIN_VALUE_EUR:,.0f}"); skipped += 1; continue

            if write_permit(p, pdf_url or ""): saved += 1
            else: skipped += 1

        except Exception as e:
            log(f"  ❌ {e}"); import traceback; traceback.print_exc(); errors += 1

        time.sleep(2)

    log(f"\n{'='*68}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors")
    log("="*68)

    if os.path.exists(QUEUE_FILE):
        os.remove(QUEUE_FILE)

    if today.weekday() == 0:
        log("\n📧 Monday → digest"); send_digest()

if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
    except: pass

run()
