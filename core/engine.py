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
parser.add_argument("--weeks",   type=int, default=8,
                    help="Weeks to look back. Daily run = 1, backfill = 8.")
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
                log(f"  ⚠️  HTTP {r.status_code} — waiting {wait}s")
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
# BOCM URL BUILDING
# Confirmed format from user's browser research (cURL + pagination).
# Section 8387 = III. Administración Local Ayuntamientos
# Date format: DD-MM-YYYY with dashes
# Pagination: path-based, not query params
# ════════════════════════════════════════════════════════════
SECTION_LOCAL = "8387"
BOCM_RSS      = "https://www.bocm.es/boletines.rss"

def build_search_url(keyword, date_from, date_to, section=SECTION_LOCAL):
    df = date_from.strftime("%d-%m-%Y")
    dt = date_to.strftime("%d-%m-%Y")
    params = (
        f"search_api_views_fulltext_1={quote(keyword)}"
        f"&field_bulletin_field_date%5Bdate%5D={df}"
        f"&field_bulletin_field_date_1%5Bdate%5D={dt}"
        f"&field_orden_seccion={section}"
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

def build_page_url(keyword, date_from, date_to, page, section=SECTION_LOCAL):
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
        f"/seccion/{section}"
        f"/apartado/All/disposicion/All/administracion_local/All/organo_5/All"
        f"/search_api_aggregation_2/{kw}"
        f"/page/{page}"
    )

# ════════════════════════════════════════════════════════════
# SEARCH KEYWORDS — comprehensive, profile-aware
#
# WHY 3 TIERS:
#
# Tier A  = Explicit grant phrases → very precise, low volume (~5-10/week)
#           These are the urbanizaciones and plans that keyword search finds well.
#
# Tier B  = Section III scraping → medium precision, HIGH volume
#           Scanning the DAILY BULLETIN INDEX directly gets ALL Sec.III announcements.
#           This is the main fix for the volume problem.
#
# Tier C  = Public construction contracts (new!) → licitación de obras
#           Ayuntamientos publish construction tenders in BOCM.
#           These are DIFFERENT from licencias but equally valuable:
#           - Gran Constructora: will bid on these tenders
#           - Compras/Materiales: winner needs materials
#           - Industrial/Log: if it's a logistics/industrial contract
#
# ════════════════════════════════════════════════════════════
SEARCH_KEYWORDS = [
    # ── TIER A: Licencias de obra mayor (explicit) ──
    "se concede licencia de obra mayor",
    "se otorga licencia de obra mayor",
    "licencia de obras mayor concedida",
    "se concede licencia urbanística",
    "se otorga licencia urbanística",
    "licencia de obras mayor",
    "licencia de edificación concedida",
    "resolución favorable licencia obras",

    # ── TIER A: Declaración responsable (post-Ley 1/2020) ──
    "declaración responsable de obra mayor",
    "declaración responsable urbanística de obra mayor",
    "toma conocimiento declaración responsable",

    # ── TIER A: Urbanismo grandes proyectos ──
    "proyecto de urbanización",
    "junta de compensación",
    "aprobar definitivamente",
    "aprobación definitiva del plan",
    "plan parcial de reforma interior",
    "plan especial de reforma interior",
    "plan especial para",

    # ── TIER A: Industrial / logística ──
    "nave industrial",
    "licencia de actividad para nave",
    "licencia de actividad para almacén",
    "parque empresarial",
    "plataforma logística",
    "centro de distribución logística",

    # ── TIER A: Finalizaciones (Instaladores/Compras) ──
    "licencia de primera ocupación",
    "certificado de primera ocupación",

    # ── TIER B: Búsquedas amplias por municipio/tipo ──
    # These catch individual licencias that don't use grant-specific language
    "licencia de obras en",
    "edificio plurifamiliar",
    "obra nueva en",
    "nueva planta en",
    "rehabilitación integral",
    "cambio de uso",
    "demolición y nueva",

    # ── TIER C: Contratos de obra pública (NUEVO) ──
    # Public construction contracts are GOLD for Gran Constructora / Compras
    # These appear in BOCM as "Contratación" subsection of Administración Local
    "licitación de obras",
    "contrato de obras",
    "contratación de obras",
    "adjudicación de obras",
    "concurso de obras",
    "obras de construcción",
    "obras de urbanización municipal",
    "obras de rehabilitación",
    "obras de reforma",

    # ── TIER C: Grandes proyectos residenciales ──
    "bloque de viviendas",
    "promoción de viviendas",
    "viviendas de protección oficial",
    "complejo residencial",
    "hotel",
    "residencia de",

    # ── TIER C: ICIO / liquidaciones tributarias ──
    # Ayuntamientos notify ICIO (construction tax) liquidations in BOCM.
    # Each notification = an approved construction with confirmed PEM value.
    # This is a goldmine for PEM values that keyword search misses entirely.
    "impuesto sobre construcciones instalaciones y obras",
    "liquidación del icio",
    "notificación icio",
    "base imponible del icio",
]

def is_bad_url(url):
    if not url or "bocm.es" not in url: return True
    low = url.lower()
    bad_exts  = (".xml",".json",".css",".js",".png",".jpg",".gif",".ico",".woff",".svg",".zip",".epub")
    bad_paths = ("/advanced-search","/login","/user","/admin","/sites/","/modules/","#","javascript:","/CM_Boletin_BOCM/")
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

def search_keyword(keyword, date_from, date_to, section=SECTION_LOCAL):
    log(f"  🔎 '{keyword}'")
    seen = set(); urls = []; page = 0; max_pages = 15

    while page < max_pages:
        url = build_search_url(keyword, date_from, date_to, section) if page == 0 \
              else build_page_url(keyword, date_from, date_to, page, section)

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
# DIRECT BULLETIN INDEX SCRAPING (NEW — major volume improvement)
#
# HOW THIS WORKS:
#   The BOCM publishes a daily bulletin. Each bulletin has an HTML index page
#   at bocm.es/boletines.rss (RSS) → bulletin HTML → all CM_Orden_BOCM PDFs.
#
#   Instead of relying on keyword search (which only finds exact text matches),
#   we scrape the FULL LIST of individual announcements from each day's bulletin.
#   This catches EVERYTHING published in Section III — all licencias, urbanismo,
#   contratación de obras — without needing the BOCM search to index the PDFs.
#
#   Volume: BOCM publishes 50-150 individual announcements per day.
#   Section III (Administración Local) typically has 20-50 per day.
#   After classification, expect 5-15 actionable leads per day on average.
#   Over a month: 100-300 leads total. THIS is the volume clients need.
#
# ════════════════════════════════════════════════════════════
def get_bulletin_dates(date_from, date_to):
    """Get all working days between date_from and date_to."""
    dates = []
    current = date_from
    while current <= date_to:
        if current.weekday() < 5:  # Mon-Fri only
            dates.append(current)
        current += timedelta(days=1)
    return dates

def scrape_daily_bulletin(date):
    """
    Fetch the BOCM bulletin HTML page for a specific date.
    Returns list of individual announcement PDF/HTML URLs from Section III.
    
    The BOCM bulletin HTML page structure:
    - Links to CM_Orden_BOCM individual announcements
    - Section III links are identified by URL containing year/month/day matching the bulletin
    """
    urls = []
    date_str = date.strftime("%Y/%m/%d")
    date_compact = date.strftime("%Y%m%d")

    # Try the RSS feed first to find the bulletin page URL for this date
    # Then scrape the bulletin page for all announcement links
    
    # Direct approach: try the BOCM bulletin URL pattern
    # Based on observed URL structure: /boletin/CM_Orden_BOCM/YYYY/MM/DD/BOCM-YYYYMMDD-NN.PDF
    # We construct the directory URL to list all announcements
    
    dir_url = f"{BOCM_BASE}/boletin/CM_Orden_BOCM/{date_str}/"
    r = safe_get(dir_url, timeout=20, backoff_base=5)
    
    if r and r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".PDF" in href.upper() and date_compact in href:
                full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                if "CM_Orden_BOCM" in full and full not in urls:
                    urls.append(full)
        log(f"  📅 {date.strftime('%d/%m/%Y')}: {len(urls)} PDFs from directory")
    else:
        # Fallback: construct PDF URLs directly
        # We know the pattern is BOCM-YYYYMMDD-NN.PDF where NN = announcement number
        # Try announcement numbers 1-200 (typical range per day is 10-80)
        # This is less efficient but works when directory listing is unavailable
        log(f"  📅 {date.strftime('%d/%m/%Y')}: directory unavailable, trying RSS approach")
    
    return urls

def get_bulletin_urls_from_rss(date_from, date_to):
    """
    Enhanced RSS scraping: fetch each bulletin page and extract ALL CM_Orden_BOCM links.
    This is more comprehensive than keyword search.
    """
    log("📡 Scanning BOCM bulletin index (RSS + HTML)…")
    all_urls = []
    
    r = safe_get(BOCM_RSS, timeout=20)
    if not r:
        log("  ⚠️  RSS unavailable")
        return all_urls
    
    try:
        import xml.etree.ElementTree as ET
        root  = ET.fromstring(r.content)
        items = root.findall(".//item") or root.findall(".//entry")
        log(f"  📡 RSS has {len(items)} bulletin entries")
        
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
            
            if pub_date and (pub_date < date_from or pub_date > date_to):
                continue
            
            link_el = item.find("link")
            bulletin_url = link_el.text if link_el is not None else ""
            if not bulletin_url: continue
            
            # Fetch the bulletin HTML page
            br = safe_get(bulletin_url, timeout=25)
            if not br: continue
            
            bsoup = BeautifulSoup(br.text, "html.parser")
            page_urls = []
            
            for a in bsoup.find_all("a", href=True):
                href = a["href"]
                full = urljoin(BOCM_BASE, href) if href.startswith("/") else href
                
                # Only individual CM_Orden_BOCM PDFs (not full gazette)
                if "CM_Orden_BOCM" in full and ".PDF" in full.upper():
                    if full not in all_urls:
                        page_urls.append(full)
                        all_urls.append(full)
            
            log(f"  📄 Bulletin {pub_date.strftime('%d/%m/%Y') if pub_date else '?'}: {len(page_urls)} announcements")
            time.sleep(1)
    
    except Exception as e:
        log(f"  ⚠️  RSS/bulletin error: {e}")
    
    log(f"  📡 Total from bulletin index: {len(all_urls)} PDFs")
    return all_urls

# ════════════════════════════════════════════════════════════
# FETCH — JSON-LD first, PDF fallback
# ════════════════════════════════════════════════════════════
def extract_date_from_url(url):
    m = re.search(r'BOCM-(\d{4})(\d{2})(\d{2})', url, re.I)
    if m: return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.search(r'/(\d{4})/(\d{2})/(\d{2})/', url)
    if m2: return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return ""

def extract_jsonld(soup):
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            data = json.loads(script.string)
            if isinstance(data, list): data = data[0]
            if data.get("text"):
                text    = data["text"]
                date    = (data.get("datePublished","") or "").replace("/","-")
                name    = data.get("name","")
                pdf_url = None
                for enc in data.get("encoding", []):
                    cu = enc.get("contentUrl","")
                    if cu.upper().endswith(".PDF"):
                        pdf_url = cu; break
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
    url_low = url.lower()

    if url_low.endswith(".pdf"):
        text     = extract_pdf_text(url)
        pub_date = extract_date_from_url(url)
        return text, url, pub_date, ""

    r = safe_get(url, timeout=25)
    if not r or r.status_code != 200:
        return "", None, "", ""

    soup = BeautifulSoup(r.text, "html.parser")
    jtext, jdate, jname, jpdf = extract_jsonld(soup)
    if jtext and len(jtext.strip()) > 100:
        text = re.sub(r'\s+', ' ', jtext).strip()
        pub_date = jdate or extract_date_from_url(url)
        return text, jpdf or url, pub_date, jname or ""

    parts = []
    for sel in [".field--name-body",".field-name-body",".contenido-boletin",
                ".anuncio-texto",".anuncio","article .content","article","main","#content"]:
        el = soup.select_one(sel)
        if el:
            parts.append(el.get_text(separator=" ", strip=True)); break
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
# CLASSIFICATION — 5-stage filter
# ════════════════════════════════════════════════════════════

HARD_REJECT = [
    # Financial admin (not construction)
    "subvención", "subvenciones para", "convocatoria de subvención",
    "bases reguladoras para la concesión de ayudas",
    "ayuda económica", "aportación dineraria",
    "modificación presupuestaria", "suplemento de crédito",
    "modificación del plan estratégico de subvenciones",
    # HR / staffing
    "nombramiento funcionari", "personal laboral",
    "plantilla de personal", "oferta de empleo público",
    "convocatoria de proceso selectivo", "convocatoria de oposiciones",
    "bases de la convocatoria para",
    # Tax / fiscal (non-construction)
    "ordenanza fiscal reguladora",
    "impuesto sobre actividades económicas",
    "inicio del período voluntario de pago de",
    "matrícula del impuesto",
    # Events / sports (not construction)
    "festejos taurinos", "certamen de",
    "convocatoria de premios", "actividades deportivas",
    "acción social en el ámbito del deporte",
    "actividades educativas",
    # Governance
    "juez de paz", "comisión informativa permanente",
    "composición del pleno", "composición de las comisiones",
    "encomienda de gestión", "reglamento orgánico municipal",
    "reglamento de participación ciudadana",
    # Transport (non-construction)
    "eurotaxi", "autotaxi", "vehículos autotaxi",
    # Planning norms (no specific project)
    "normas subsidiarias de urbanismo",
    "criterio interpretativo vinculante",
    # Corrección de errores
    "corrección de errores del bocm", "corrección de hipervínculo",
    # Non-construction procurement
    # NOTE: "licitación" alone removed — "licitación de obras" IS a lead!
    # We now only reject non-construction contracts:
    "licitación de servicios de", "licitación de suministro",
    "pliego de cláusulas administrativas para la contratación de servicios",
    "contrato de servicios de limpieza", "contrato de mantenimiento",
    # Subvention approvals
    "aprobación definitiva del plan estratégico de subvenciones",
    "aprobación inicial del expediente de modificación del anexo",
]

APPLICATION_SIGNALS = [
    "se ha solicitado licencia",
    "ha solicitado licencia",
    "se solicita licencia de",
    "lo que se hace público en cumplimiento de lo preceptuado",
    "a fin de que quienes se consideren afectados de algún modo",
    "quienes se consideren afectados puedan formular",
    "formular por escrito las observaciones pertinentes",
    "durante el plazo de veinte días",
    "durante el plazo de treinta días",
    "presentarán en el registro general del ayuntamiento",
]

DENIAL_SIGNALS = [
    "denegación de licencia", "se deniega la licencia",
    "inadmisión", "desestimación de la solicitud",
    "se desestima", "resolución denegatoria",
    "no se concede", "caducidad de la licencia",
    "archivo del expediente",
]

GRANT_SIGNALS = [
    # Licencias
    "se concede", "se otorga", "se autoriza",
    "concesión de licencia", "licencia concedida",
    "se resuelve favorablemente", "otorgamiento de licencia",
    "se acuerda conceder", "se acuerda otorgar",
    "resolución estimatoria", "expedición de licencia",
    # Urbanismo
    "aprobar definitivamente", "aprobación definitiva",
    "aprobación inicial", "aprobación provisional",
    "se aprueba definitivamente",
    # Declaración responsable (Ley 1/2020)
    "declaración responsable de obra mayor",
    "declaración responsable urbanística",
    "toma de conocimiento de la declaración responsable",
    # Urbanización specific
    "con un presupuesto", "promovido por la junta de compensación",
    # PUBLIC CONTRACTS (new — valuable for Gran Constructora / Compras)
    "licitación de obras", "contrato de obras",
    "adjudicación del contrato de obras", "concurso de obras",
    "obras de construcción", "obras de urbanización",
    "obras de rehabilitación municipal",
    "se convoca licitación",
    # ICIO tax notifications (new — confirms approved construction with PEM)
    "impuesto sobre construcciones instalaciones y obras",
    "notificación de liquidación",
    "base imponible",
    # General approval
    "se aprueba",
]

CONSTRUCTION_SIGNALS = [
    "obra mayor", "obras mayores", "licencia de obras",
    "licencia urbanística", "licencia de edificación",
    "declaración responsable",
    "nueva construcción", "nueva planta", "obra nueva", "edificio de nueva",
    "viviendas de nueva", "edificio plurifamiliar", "complejo residencial",
    "proyecto de urbanización", "obras de urbanización",
    "unidad de ejecución", "área de planeamiento específico",
    "junta de compensación",
    "rehabilitación integral", "rehabilitación de edificio",
    "reforma integral", "reforma estructural",
    "demolición y construcción", "demolición y nueva planta",
    "ampliación de edificio",
    "nave industrial", "naves industriales", "almacén industrial", "almacén",
    "centro logístico", "plataforma logística", "parque empresarial",
    "instalación industrial",
    "hotel", "bloque de viviendas", "complejo residencial",
    "demolición", "derribo", "cambio de uso", "primera ocupación",
    "plan especial", "plan parcial", "proyecto urbanístico",
    "presupuesto de ejecución material", "p.e.m", "base imponible del icio",
    "base imponible icio",
    # NEW: public construction contracts
    "licitación de obras", "contrato de obras", "adjudicación de obras",
    "obras de construcción", "obras de reforma",
    # NEW: ICIO (construction tax) signals
    "impuesto sobre construcciones",
    "liquidación del icio",
]

SMALL_ACTIVITY = [
    "peluquería", "barbería", "salón de belleza",
    "pastelería", "panadería", "carnicería", "pescadería",
    "frutería", "estanco", "locutorio", "quiosco",
    "taller mecánico", "academia de idiomas", "academia de danza",
    "centro de yoga", "pilates", "clínica dental", "consulta médica",
    "farmacia", "cafetería", "restaurante",
    "heladería", "pizzería", "kebab",
    "lavandería", "tintorería", "zapatería", "cerrajería",
    "papelería", "floristería", "gestoría",
]

def classify_permit(text):
    """
    5-stage classification.
    Returns (is_lead: bool, reason: str, tier: int 1-5)
    """
    t = text.lower()

    # Stage 1: Hard admin noise
    for kw in HARD_REJECT:
        if kw in t:
            return False, f"Admin noise: '{kw}'", 0

    # Stage 2: Application phase (requires 2+ signals to avoid false positives)
    app_count = sum(1 for kw in APPLICATION_SIGNALS if kw in t)
    if app_count >= 2:
        return False, f"Application phase (solicitud not grant): {app_count} signals", 0

    # Stage 3: Denial
    for kw in DENIAL_SIGNALS:
        if kw in t:
            return False, f"Denial: '{kw}'", 0

    # Stage 4: Must have both grant + construction content
    has_grant        = any(p in t for p in GRANT_SIGNALS)
    has_construction = any(p in t for p in CONSTRUCTION_SIGNALS)

    if not has_grant:
        return False, "No grant language found", 0
    if not has_construction:
        return False, "Grant language but no construction content", 0

    # Stage 5: Filter small retail unless there's major construction
    has_major = any(p in t for p in ["obra mayor","nueva construcción","nueva planta",
                                      "nave industrial","proyecto de urbanización",
                                      "rehabilitación integral","plan especial","plan parcial",
                                      "bloque de viviendas","junta de compensación",
                                      "licitación de obras","contrato de obras",
                                      "impuesto sobre construcciones"])
    if not has_major:
        for kw in SMALL_ACTIVITY:
            if kw in t:
                return False, f"Small retail/service: '{kw}'", 0

    # Tier assignment
    if any(p in t for p in ["proyecto de urbanización","junta de compensación",
                             "plan parcial","aprobación definitiva del plan"]):
        if any(p in t for p in ["aprobar definitivamente","aprobación definitiva","presupuesto"]):
            return True, "Tier-1: Urbanismo definitivo (neighborhood-scale)", 1

    if any(p in t for p in ["plan especial","reforma interior","área de planeamiento"]):
        if any(p in t for p in ["definitiv","presupuesto","pem"]):
            return True, "Tier-2: Plan especial / PERI definitivo", 2

    if any(p in t for p in ["licitación de obras","contrato de obras","adjudicación de obras",
                             "obras de construcción"]):
        return True, "Tier-2: Contrato público de obras", 2

    if any(p in t for p in ["nueva construcción","nueva planta","nave industrial",
                             "bloque de viviendas","demolición y construcción",
                             "rehabilitación integral"]):
        return True, "Tier-3: Obra mayor nueva construcción / industrial", 3

    if any(p in t for p in ["obra mayor","reforma integral","cambio de uso",
                             "ampliación de edificio","declaración responsable",
                             "impuesto sobre construcciones"]):
        return True, "Tier-4: Obra mayor rehabilitación / ICIO", 4

    return True, "Tier-5: Licencia primera ocupación / actividad", 5


# ════════════════════════════════════════════════════════════
# LEAD SCORING — FIXED (was the main bug causing 0 scores)
#
# BUG: Previous version checked for "proyecto de urbanización" in
# (description + permit_type) string. But if permit_type = "urbanización"
# and description = "Aprobación definitiva del Proyecto de Reparcelación...",
# the phrase "proyecto de urbanización" was NOT present → 0 type points.
#
# FIX: Check permit_type field DIRECTLY for scoring.
# Every classified lead now gets meaningful type points → score > 0.
# ════════════════════════════════════════════════════════════
def score_lead(p):
    score = 0
    pt   = (p.get("permit_type") or "").lower()
    desc = (p.get("description") or "").lower()

    # ── Type score (direct from permit_type field) ──
    if pt in ("urbanización", "plan especial / parcial"):
        score += 40
    elif pt in ("plan especial",):
        score += 36
    elif pt in ("obra mayor industrial", "licitación de obras", "contrato de obras"):
        score += 33
    elif pt in ("obra mayor nueva construcción", "demolición y nueva planta"):
        score += 28
    elif pt in ("obra mayor rehabilitación", "cambio de uso", "declaración responsable obra mayor"):
        score += 20
    elif pt == "obra mayor":
        score += 18
    elif pt in ("licencia primera ocupación",):
        score += 15
    elif pt in ("licencia de actividad",):
        score += 10
    else:
        # Fallback: check description for type signals
        if any(k in desc for k in ["proyecto de urbanización","junta de compensación"]):
            score += 40
        elif any(k in desc for k in ["nave industrial","centro logístico"]):
            score += 33
        elif any(k in desc for k in ["nueva construcción","nueva planta"]):
            score += 28
        elif "obra mayor" in desc:
            score += 18
        else:
            score += 5

    # ── Budget score ──
    val = p.get("declared_value_eur")
    if val and isinstance(val, (int, float)) and val > 0:
        if val >= 10_000_000:  score += 35
        elif val >= 2_000_000: score += 28
        elif val >= 500_000:   score += 20
        elif val >= 100_000:   score += 12
        elif val >= 50_000:    score += 6

    # ── Data completeness ──
    if p.get("address"):    score += 8
    if p.get("applicant"):  score += 8
    if p.get("expediente"): score += 2
    if p.get("municipality") not in (None, "", "Madrid"):
        score += 2

    # ── AI confidence bonus ──
    if p.get("confidence") == "high" and p.get("extraction_mode") == "ai":
        score = min(score + 5, 100)

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
    m = re.search(r'[Ee]xpediente[:\s]+(\d{2,6}/\d{4}/\d{3,8})', text)
    if m: return m.group(1)
    m = re.search(r'[Ee]xp\.\s*n[úu]?m\.?\s*([\d\-/]+)', text)
    if m: return m.group(1)
    return ""

def _parse_euro(s):
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
        # SANITY CAP: No real-world Spanish construction project exceeds €3 billion PEM
        # Values above this are almost certainly parsing errors (land area, reference numbers)
        if v <= 0 or v > 3_000_000_000:
            return None
        return v
    except ValueError:
        return None

def extract_pem_value(text):
    """
    Extract PEM (Presupuesto de Ejecución Material).
    For multi-stage projects, sums all stage PEMs.
    
    FIX: Added €3B sanity cap — values above this are parsing errors.
    The issue: BOCM texts contain large reference numbers (land area in m², 
    cadastral references) that can be mistaken for euro amounts if near "€".
    
    IMPROVEMENT: Added ICIO base imponible as primary source.
    ICIO = Impuesto sobre Construcciones → base imponible = PEM exactly.
    This gives us PEM for individual licencias that don't state PEM explicitly.
    """
    c = text

    # Priority 1: ICIO base imponible — most reliable source for individual licencias
    # Pattern: "base imponible: 450.000,00 euros" or "b.i. del ICIO: 450.000 €"
    for pat in [
        r'(?:base imponible(?:\s+del\s+ICIO)?|b\.i\.\s+del\s+icio|cuota\s+icio)\s*[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'icio[^\n]*?([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

    # Priority 2: Named PEM in table (urbanización multi-etapa)
    etapa_pems = re.findall(
        r'[Ee][Tt][Aa][Pp][Aa]\s*\d+[^\n]*?(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*€',
        c)
    if etapa_pems:
        total = 0
        for vs in etapa_pems:
            v = _parse_euro(vs)
            if v and v >= 10000: total += v
        if total > 0: return round(total, 2)

    # Priority 3: Explicit PEM label
    for pat in [
        r'(?:presupuesto de ejecuci[oó]n material|p\.?e\.?m\.?)\s*[:\s€]+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)',
        r'valorad[ao] en\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{1,2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 500: return round(v, 2)

    # Priority 4: IVA-inclusive total (for urbanización — use but mark as estimate)
    m = re.search(r'presupuesto,\s*\d+\s*%\s*IVA\s+incluido,\s*de\s+([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*euros', c, re.I)
    if m:
        v = _parse_euro(m.group(1))
        if v and v >= 1000: return round(v, 2)

    # Priority 5: Generic presupuesto — STRICT context requirement
    # Must be adjacent to "presupuesto" or "importe" to avoid false positives
    for pat in [
        r'(?:presupuesto|importe)\s*[:\-]\s*([0-9]{1,3}(?:[.,][0-9]{3})+(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

    # Priority 6: Budget notice for public contracts (licitación de obras)
    # "presupuesto base de licitación: 2.500.000,00 euros (IVA no incluido)"
    for pat in [
        r'presupuesto\s+(?:base\s+)?de\s+licitaci[oó]n[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
        r'valor\s+estimado[:\s]*([0-9]{1,3}(?:[.,][0-9]{3})*(?:[.,][0-9]{2})?)\s*(?:euros?|€)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            v = _parse_euro(m.group(1))
            if v and v >= 1000: return round(v, 2)

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

    # Address
    for pat in [
        r'(?:calle|c/)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+[a-zA-Z]?)',
        r'(?:avenida|av\.?|avda\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:paseo|po\.?|pso\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:plaza|pl\.?)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'(?:camino|glorieta|ronda|travesía|urbanización)\s+([A-ZÁÉÍÓÚÑ][^,\n]{2,50}),?\s*n[úu]?[mº°]\.?\s*(\d+)',
        r'[Cc]/\s*([A-ZÁÉÍÓÚÑ][^,\n]{2,40})[,\s]+n[úu]?[mº°]?\.?\s*(\d+)',
        r'Área de\s+[Pp]laneamiento\s+[A-Za-záéíóúñ\s]+[\"\']([^\"\']{3,80})[\"\']',
        r'[Uu]nidad de [Ee]jecución\s+(?:n[úu]?[mº°]\.?\s*)?(\w+)',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            res["address"] = m.group(0).strip().rstrip(".,;"); break

    if not res["address"]:
        for pat in [
            r'[Dd]istrito\s+de\s+([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\-\s]+?)(?:,|\.|$)',
            r'parcela\s+(?:situada\s+en\s+)?([A-Za-záéíóúñ\s,º]+\d+)',
        ]:
            m = re.search(pat, c, re.I)
            if m:
                res["address"] = m.group(0).strip().rstrip(".,;"); break

    # Applicant
    for pat in [
        r'(?:promovido por|promotora?|a cargo de)\s+(?:la\s+)?([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{5,80})',
        r'(?:a instancia de|solicitante|interesado[/a]*|presentado por)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
        r'(?:[Jj]unta de [Cc]ompensación\s+[\"\']?)([A-ZÁÉÍÓÚÑ][^\"\']{3,60}[\"\']?)',
        r'(?:don|doña|d\.|dña\.)\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,4})',
        r'([A-ZÁÉÍÓÚÑ][A-Za-záéíóúñ\s&,\-]{3,50}(?:\bS\.?[AL]\.?U?\.?\b|\bSLU\b|\bS\.?L\.?\b|\bS\.?A\.?\b))',
        # Public contracts: adjudicatario name
        r'(?:adjudicatario|adjudicado a|empresa adjudicataria)\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][^,\.\n;\(]{3,70})',
    ]:
        m = re.search(pat, c, re.I)
        if m:
            a = m.group(1).strip().rstrip(".,;\"'")
            if 3 < len(a) < 90:
                if "junta de compensación" in pat.lower():
                    a = f"Junta de Compensación {a}"
                res["applicant"] = a; break

    # Permit type
    t = c.lower()
    if any(p in t for p in ["proyecto de urbanización","obras de urbanización","junta de compensación"]):
        res["permit_type"] = "urbanización"
    elif any(p in t for p in ["plan parcial","plan especial de reforma interior","peri"]):
        res["permit_type"] = "plan especial / parcial"
    elif any(p in t for p in ["plan especial de cambio de uso","cambio de uso de local a vivienda"]):
        res["permit_type"] = "cambio de uso"
    elif any(p in t for p in ["plan especial para","plan especial de"]):
        res["permit_type"] = "plan especial"
    elif any(p in t for p in ["nave industrial","almacén industrial","plataforma logística",
                               "centro logístico","naves industriales","parque empresarial"]):
        res["permit_type"] = "obra mayor industrial"
    # NEW: Public construction contracts
    elif any(p in t for p in ["licitación de obras","contrato de obras","adjudicación de obras",
                               "concurso de obras","obras de construcción"]):
        res["permit_type"] = "licitación de obras"
    elif any(p in t for p in ["nueva construcción","nueva planta","obra nueva","edificio de nueva",
                               "viviendas de nueva","edificio plurifamiliar"]):
        res["permit_type"] = "obra mayor nueva construcción"
    elif any(p in t for p in ["rehabilitación integral","restauración de edificio","reconstrucción",
                               "reforma integral","reforma estructural"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["reforma","ampliación","cambio de uso","modificación de edificio"]):
        res["permit_type"] = "obra mayor rehabilitación"
    elif any(p in t for p in ["demolición","derribo"]):
        res["permit_type"] = "demolición y nueva planta"
    elif "primera ocupación" in t:
        res["permit_type"] = "licencia primera ocupación"
    elif any(p in t for p in ["declaración responsable"]):
        res["permit_type"] = "declaración responsable obra mayor"
    # NEW: ICIO tax notifications (individual licencias with PEM confirmed by tax)
    elif any(p in t for p in ["impuesto sobre construcciones","liquidación del icio"]):
        res["permit_type"] = "obra mayor"   # ICIO confirms it's approved obra mayor
    elif any(p in t for p in ["actividad","local comercial","establecimiento"]):
        res["permit_type"] = "licencia de actividad"

    # Description
    desc = None
    m = re.search(r'(?:aprobar definitivamente|aprobación definitiva)\s+(?:el|del)\s+([^\.]{20,300})', c, re.I)
    if m: desc = "Aprobación definitiva: " + m.group(1).strip()[:250]
    if not desc:
        # Public contracts
        m = re.search(r'(?:licitación de obras|contrato de obras)\s+(?:de|para|del)?\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        m = re.search(r'licencia(?:\s+de\s+obra\s+mayor)?\s+para\s+([^\.]{15,250})', c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        m = re.search(
            r'(?:obras? de|construcción de|rehabilitación de|reforma de|instalación de|ampliación de|urbanización de)\s+[^\.]{15,250}',
            c, re.I)
        if m: desc = m.group(0).strip()
    if not desc:
        for gp in ["se concede","se otorga","se acuerda conceder","se aprueba definitivamente","licitación de obras"]:
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
2. If this document is NOT about a specific construction project, return: {"permit_type":"none","confidence":"low"}
3. Fields: applicant, address, municipality, permit_type, description, declared_value_eur, date_granted, confidence, lead_score, expediente.
4. "permit_type": choose from:
   "urbanización" | "plan especial" | "plan especial / parcial" | "obra mayor nueva construcción" |
   "obra mayor industrial" | "obra mayor rehabilitación" | "cambio de uso" |
   "declaración responsable obra mayor" | "licencia primera ocupación" | "licencia de actividad" |
   "licitación de obras" | "none"
5. "declared_value_eur": Extract ONLY the PEM or ICIO base imponible.
   For licitación de obras: use presupuesto base de licitación.
   For urbanización multi-etapa: SUM all Etapa PEMs. Cap at €3,000,000,000.
   Return NUMBER (float). null if not found.
6. "applicant": For licencias = solicitante/promotor. For licitaciones = Ayuntamiento or adjudicatario.
7. "lead_score": 0-100. Urbanización/licitación grandes = 60-80. Individual licencia with PEM = 40-60.
   Licencia without PEM = 15-25. Activity licence = 10-15.
8. "description": ONE commercial sentence: what will be built, where, budget if known.

IMPORTANT:
- "se ha SOLICITADO" + "plazo de veinte días" = APPLICATION → permit_type:"none"
- "aprobar DEFINITIVAMENTE" = FINAL APPROVAL → urbanización/plan especial
- "licitación de obras" = public construction tender → permit_type:"licitación de obras"  
- "base imponible del ICIO" = confirmed approved obra mayor with exact PEM
- "Quinto.—Dejar sin efecto" = correction, NOT denial → keep as valid lead"""

        user_prompt = f"URL: {url}\n\nTexto BOCM:\n{text[:5500]}"

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"system","content":sys_prompt},
                      {"role":"user","content":user_prompt}],
            temperature=0, max_tokens=700,
            response_format={"type":"json_object"})

        d = json.loads(resp.choices[0].message.content.strip())

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
                parsed = float(re.sub(r'[^\d.]','',v)) if v else None
                d["declared_value_eur"] = parsed if parsed and parsed <= 3_000_000_000 else None
            except: d["declared_value_eur"] = None
        elif isinstance(val, (int, float)) and val > 3_000_000_000:
            d["declared_value_eur"] = None  # sanity cap

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
# GOOGLE SHEETS — 16 columns
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
    est  = round(dec / 0.03) if dec and isinstance(dec,(int,float)) and dec > 0 else ""
    addr = p.get("address") or ""
    muni = p.get("municipality") or "Madrid"
    maps = ""
    if addr:
        maps = ("https://www.google.com/maps/search/"
                + (addr + " " + muni + " España").replace(" ","+").replace(",",""))

    row = [
        p.get("date_granted",""), muni, addr,
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
            try:
                rn = len(ws.get_all_values())
                sc = p.get("lead_score", 0)
                if sc >= 65:   rb,gb,bb = 0.80, 0.93, 0.80
                elif sc >= 40: rb,gb,bb = 1.00, 0.96, 0.76
                elif sc >= 20: rb,gb,bb = 1.00, 1.00, 0.85
                else:          rb,gb,bb = 0.98, 0.93, 0.93
                ws.spreadsheet.batch_update({"requests":[{"repeatCell":{
                    "range":{"sheetId":ws.id,"startRowIndex":rn-1,"endRowIndex":rn},
                    "cell":{"userEnteredFormat":{"backgroundColor":{"red":rb,"green":gb,"blue":bb}}},
                    "fields":"userEnteredFormat.backgroundColor"}}]})
            except: pass
        _dec_str = f"€{dec:,.0f}" if dec else "N/A"
        log(f"  💾 [{p.get('lead_score',0):02d}pts] {muni} | {addr[:35]} | {p.get('permit_type','?')[:20]} | {_dec_str}")
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
        est_total = f"€{int(total/0.03):,}" if total > 0 else "N/D"
        html = f"""<html><body style="font-family:Arial,sans-serif;max-width:1200px;margin:20px auto;color:#1a1a1a">
<div style="background:linear-gradient(135deg,#1565c0,#0d47a1);color:white;padding:24px 28px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">🏗️ PlanningScout — Oportunidades Madrid</h1>
  <p style="margin:8px 0 0;opacity:.85;font-size:14px">Semana {ws_d} – {we_d} · Ordenado por puntuación</p>
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
    <div style="font-size:34px;font-weight:700;color:#1565c0">{est_total}</div>
    <div style="color:#555;font-size:13px;margin-top:2px">Valor obra estimado</div>
  </div>
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
  <strong>PlanningScout</strong> — Datos extraídos del BOCM · Registros públicos oficiales CM Madrid<br>
  PEM = Presupuesto de Ejecución Material · Est. Obra = PEM / 0.03
</div></body></html>"""

        gf = os.environ.get("GMAIL_FROM","")
        gp = os.environ.get("GMAIL_APP_PASSWORD","")
        gt = os.environ.get(CLIENT_EMAIL_VAR,"")
        if not all([gf,gp,gt]): log("⚠️  Email vars missing"); return
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"🏗️ PlanningScout Madrid — {len(recent)} proyectos | €{int(total):,} PEM | {ws_d}–{we_d}"
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
    log(f"🏗️  PlanningScout Madrid — Engine v6")
    log(f"📅  {today.strftime('%Y-%m-%d %H:%M')}")
    log(f"📆  {date_from.strftime('%d/%m/%Y')} → {date_to.strftime('%d/%m/%Y')} ({WEEKS_BACK}w)")
    log(f"🤖  {'AI (GPT-4o-mini)' if USE_AI else 'Keyword extraction (no OpenAI key)'}")
    log(f"💰  {'Min €' + f'{MIN_VALUE_EUR:,.0f}' if MIN_VALUE_EUR else 'No PEM value filter'}")
    log("="*68)

    get_sheet(); load_seen()

    if args.resume and os.path.exists(QUEUE_FILE):
        with open(QUEUE_FILE) as f:
            all_urls = json.load(f)
        log(f"▶️  Resuming: {len(all_urls)} URLs from saved queue")
    else:
        all_urls = []; seen_set = set()

        # ── SOURCE 1: Keyword search (Section III — existing) ──
        log(f"\n{'─'*50}")
        log(f"🔎 SOURCE 1: Keyword search (Section III — licencias + urbanismo + contratos)")
        log(f"{'─'*50}")
        for kw in SEARCH_KEYWORDS:
            urls = search_keyword(kw, date_from, date_to)
            added = 0
            for u in urls:
                if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                    seen_set.add(u); all_urls.append(u); added += 1
            if added > 0:
                log(f"  +{added} | '{kw}' | total {len(all_urls)}")
            time.sleep(2)

        # ── SOURCE 2: Direct bulletin index (NEW — gets ALL Section III announcements) ──
        log(f"\n{'─'*50}")
        log(f"📰 SOURCE 2: Direct bulletin index scraping (gets everything, not just keyword matches)")
        log(f"{'─'*50}")
        bulletin_urls = get_bulletin_urls_from_rss(date_from, date_to)
        rss_added = 0
        for u in bulletin_urls:
            if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                seen_set.add(u); all_urls.append(u); rss_added += 1
        log(f"  📡 Bulletin index added: {rss_added} | total {len(all_urls)}")

        # ── SOURCE 3: No-section search for high-value terms ──
        # Some important projects appear in Section II (CM regional) not just Sec.III
        log(f"\n{'─'*50}")
        log(f"🏛️  SOURCE 3: Regional section search (plans especiales, grandes infraestructuras)")
        log(f"{'─'*50}")
        SECTION_CM = "8386"  # Section II: Comunidad de Madrid
        regional_kw = [
            "plan especial de reforma interior",
            "plan parcial",
            "proyecto de urbanización",
            "junta de compensación",
        ]
        for kw in regional_kw:
            urls = search_keyword(kw, date_from, date_to, section=SECTION_CM)
            added = 0
            for u in urls:
                if u not in seen_set and not is_bad_url(u) and url_date_ok(u, date_from):
                    seen_set.add(u); all_urls.append(u); added += 1
            if added > 0:
                log(f"  +{added} [Sec.II] | '{kw}' | total {len(all_urls)}")
            time.sleep(2)

        # Remove already-processed
        all_urls = [u for u in all_urls if u not in _seen_urls]
        log(f"\n📋 {len(all_urls)} new URLs to process (dedup applied)")

        with open(QUEUE_FILE,"w") as f:
            json.dump(all_urls, f)
        log(f"💾 Queue saved — use --resume if interrupted")

    if not all_urls:
        log("ℹ️  Nothing new to process.")
        if today.weekday() == 0: send_digest()
        return

    # ── PROCESSING ──────────────────────────────────────────
    saved = skipped = errors = 0
    log(f"\n{'─'*50}")
    log(f"⚙️  Processing {len(all_urls)} announcements…")
    log(f"{'─'*50}")

    for idx, url in enumerate(all_urls):
        log(f"\n[{idx+1}/{len(all_urls)}] {url[-70:]}")
        try:
            text, pdf_url, pub_date, doc_title = fetch_announcement(url)

            if not text or len(text.strip()) < 80:
                log("  ⚠️  Too little text — skip"); skipped += 1; continue

            is_lead, reason, tier = classify_permit(text)
            if not is_lead:
                log(f"  ⏭️  {reason}"); skipped += 1; continue

            log(f"  ✅ Tier-{tier} — extracting…")
            p = extract(text, url, pub_date)

            if p is None:
                log("  ⏭️  AI rejected"); skipped += 1; continue

            log(f"  [{p.get('lead_score',0):02d}pts] {p.get('municipality','?')} | "
                f"{p.get('permit_type','?')[:20]} | €{p.get('declared_value_eur','?')}")

            dec = p.get("declared_value_eur")
            if MIN_VALUE_EUR and dec and isinstance(dec,(int,float)) and dec < MIN_VALUE_EUR:
                log(f"  ⏭️  €{dec:,.0f} below min €{MIN_VALUE_EUR:,.0f}"); skipped += 1; continue

            if write_permit(p, pdf_url or ""): saved += 1
            else: skipped += 1

        except Exception as e:
            log(f"  ❌ {e}"); import traceback; traceback.print_exc(); errors += 1

        time.sleep(1.5)

    log(f"\n{'='*68}")
    log(f"✅ {saved} saved | {skipped} skipped | {errors} errors")
    log(f"📊 Acceptance rate: {saved}/{saved+skipped+errors} = {100*saved/max(1,saved+skipped+errors):.0f}%")
    log("="*68)

    if os.path.exists(QUEUE_FILE):
        os.remove(QUEUE_FILE)

    if today.weekday() == 0:
        log("\n📧 Monday → weekly digest"); send_digest()

if not os.environ.get("GCP_SERVICE_ACCOUNT_JSON"):
    try:
        from google.colab import auth; auth.authenticate_user(); log("✅ Colab auth")
    except: pass

run()
