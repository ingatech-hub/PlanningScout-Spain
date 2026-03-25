import requests
import pdfplumber
import io
import json

# 1. Load the Spanish Config
with open("bop_barcelona.json", "r", encoding="utf-8") as f:
    config = json.load(f)

triggers = config["pdf_triggers"]

# 2. Paste a live PDF link from the BOP Barcelona here
pdf_url = "PASTE_BOP_PDF_LINK_HERE" 

def test_spanish_pdf(url):
    print(f"📥 Downloading PDF from BOP: {url}")
    
    # Download the PDF
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, verify=False, timeout=15)
    
    if r.status_code != 200:
        print(f"❌ Failed to download. HTTP {r.status_code}")
        return

    # Extract Text
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
            
        # Optional: Print a small snippet around the first trigger to see the context
        first_trigger = found_triggers[0]
        idx = text.find(first_trigger)
        snippet = text[max(0, idx-100):min(len(text), idx+150)]
        print(f"\n📄 Context Snippet:\n...{snippet.strip()}...\n")
    else:
        print("⏭️  No legal triggers found. This is just a standard notice.")
    print("========================================")

if __name__ == "__main__":
    if pdf_url == "https://bop.diba.cat/anuncio/descargar-pdf/3912642":
        print("⚠️  Please paste a real PDF URL into the code first!")
    else:
        test_spanish_pdf(pdf_url)
