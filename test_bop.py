import os
import requests
import pdfplumber
import io
import json
import urllib3

# Suppress annoying security warnings since government sites often have bad SSL certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 1. Load the Spanish Config
try:
    with open("bop_barcelona.json", "r", encoding="utf-8") as f:
        config = json.load(f)
    triggers = config["pdf_triggers"]
except FileNotFoundError:
    print("❌ Error: bop_barcelona.json file not found! Make sure it is in the same folder.")
    triggers = []

# 2. Get the URL from GitHub Actions (or fallback to your test link)
pdf_url = os.getenv("TARGET_URL", "https://bop.diba.cat/anuncio/descargar-pdf/3912642").strip()

def test_spanish_pdf(url):
    print(f"📥 Downloading PDF from BOP: {url}")
    
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        r = requests.get(url, headers=headers, verify=False, timeout=15)
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return

    if r.status_code != 200:
        print(f"❌ Failed to download. HTTP {r.status_code}")
        return

    print("🔎 Scanning document...")
    text = ""
    try:
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted.lower() + " "
    except Exception as e:
        print(f"❌ PDF Parsing Error: {e}")
        return

    print(f"✅ Extracted {len(text):,} characters from the legal notice.\n")
    
    # Scan for Triggers
    found_triggers = []
    for t in triggers:
        if t in text:
            found_triggers.append(t)
            
    # Output Results
    print("========================================")
    print("📊 SPANISH URBANISMO RESULTS")
    print("========================================")
    if found_triggers:
        print("🏆 QUALIFIED LEAD! Found the following legal triggers:")
        for ft in found_triggers:
            print(f"   🚨 {ft.upper()}")
            
        first_trigger = found_triggers[0]
        idx = text.find(first_trigger)
        snippet = text[max(0, idx-100):min(len(text), idx+150)]
        print(f"\n📄 Context Snippet:\n...{snippet.strip()}...\n")
    else:
        print("⏭️  No legal triggers found. This is just a standard notice.")
    print("========================================")

if __name__ == "__main__":
    if not pdf_url or pdf_url == "":
        print("⚠️  No URL provided! Please pass a PDF URL.")
    else:
        test_spanish_pdf(pdf_url)
