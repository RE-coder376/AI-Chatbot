"""
Rebuilds the store DB with one clean chunk per product.
Parses all existing chunks, deduplicates by (name, price), and re-ingests.
"""
import chromadb, re, json
from pathlib import Path
from langchain_core.documents import Document

DB_PATH = "databases/store"
COLLECTION = "langchain"

# ── Step 1: Load all chunks ───────────────────────────────────────────────────
client = chromadb.PersistentClient(path=DB_PATH)
col = client.get_collection(COLLECTION)
all_data = col.get(limit=500, include=["documents", "metadatas"])
print(f"Total chunks in DB: {len(all_data['documents'])}")

# ── Step 2: Parse products ────────────────────────────────────────────────────
# Regex: $PRICE <name> <specs> — stops at next $ or end
PROD_RE = re.compile(
    r'\$(\d[\d,]*\.?\d*)\s+'         # price
    r'([A-Z][A-Za-z0-9\s\(\)\-\.]+?' # product name (capitalized)
    r'(?:,'                           # specs start after comma
    r'[^\$]{10,300}?))'              # specs body
    r'(?=\s*\$|\s*\Z)',              # stops at next price or end
    re.S
)

NOISE_RE = re.compile(
    r'Test Sites Home Computers (?:Laptops |Tablets |Phones |Touch )?(?:Phones|Tablets|Laptops)?'
    r'|Web Scraper.*?No software to download.*?\.\.'
    r'|CLOUD SCRAPER.*?CONTACT US.*?info@\S+'
    r'|HDD:\s*[\d\s]+'
    r'|\b\d+\s+reviews?\b'
    r'|Select color.*?(?=\n|\Z)'
    r'|PRODUCTS Web Scraper.*',
    re.S | re.I
)

def clean_chunk(text):
    return NOISE_RE.sub(' ', text).strip()

products = {}  # key = f"{price}|{name_normalized}" for dedup

def parse_products_from_text(text, source=""):
    """Extract products from a chunk of text."""
    cleaned = clean_chunk(text)
    found = []
    for m in PROD_RE.finditer(cleaned):
        price_str = m.group(1).replace(',', '')
        try:
            price_num = float(price_str)
        except:
            continue
        raw_spec = m.group(2).strip()
        # Remove duplicate name (product pages repeat name twice)
        # e.g. "Dell Inspiron 15 Dell Inspiron 15, 15.6"..." → keep from comma
        comma_idx = raw_spec.find(',')
        if comma_idx > 0:
            name_part = raw_spec[:comma_idx].strip()
            # If name repeats (e.g. "Foo Bar Foo Bar, specs"), deduplicate
            half = len(name_part) // 2
            if name_part[:half].strip() == name_part[half:].strip():
                name_part = name_part[:half].strip()
            specs_part = raw_spec[comma_idx+1:].strip()
        else:
            name_part = raw_spec
            specs_part = ""

        # Strip "Brand..." breadcrumb prefix (listing page artifact: "Dell Inspiron... Dell Inspiron 15")
        name_clean = re.sub(r'^[^\s]+(?:\s+[^\s]+){0,3}\.{2,}\s+', '', name_part.strip())
        # Also strip trailing " reviews" noise
        name_clean = re.sub(r'\s+reviews?\s*$', '', name_clean, flags=re.I).strip()
        name_norm = re.sub(r'\s+', ' ', name_clean.lower().strip())
        key = f"{price_str}|{name_norm[:60]}"
        if key not in products:
            products[key] = {
                "price_num": price_num,
                "price": f"${price_str}",
                "name": name_clean if name_clean else name_part.strip(),
                "specs": specs_part.strip(),
                "source": source,
            }
            found.append(key)
    return found

# ── Detect if DB is already in structured format ─────────────────────────────
_first_doc = all_data["documents"][0] if all_data["documents"] else ""
_structured = _first_doc.startswith("Product:")

if _structured:
    # Parse structured chunks: "Product: ...\nPrice: $...\n..."
    STRUCT_RE = re.compile(
        r'Product:\s*(.+?)\s*\n'
        r'(?:Category:[^\n]*\n)?'
        r'Price:\s*\$?([\d,]+\.?\d*)\s*\n'
        r'((?:.|\n)*)',
        re.I
    )
    for doc, meta in zip(all_data["documents"], all_data["metadatas"]):
        src = meta.get("source", "") if meta else ""
        m = STRUCT_RE.match(doc)
        if not m:
            continue
        raw_name = m.group(1).strip()
        price_str = m.group(2).replace(',', '')
        rest = m.group(3)
        try:
            price_num = float(price_str)
        except:
            continue
        # Extract specs from "Full specs:" line
        fs_match = re.search(r'Full specs:\s*(.+?)(?:\n|$)', rest, re.I)
        full_spec = fs_match.group(1).strip() if fs_match else ""
        # Extract specs portion (after the product name in full_spec)
        comma_idx = full_spec.find(',')
        specs_part = full_spec[comma_idx+1:].strip() if comma_idx > 0 else ""
        # Normalize name: strip "Brand..." prefix
        name_clean = re.sub(r'^[^\s]+(?:\s+[^\s]+){0,3}\.{2,}\s+', '', raw_name)
        name_clean = re.sub(r'\s+reviews?\s*$', '', name_clean, flags=re.I).strip()
        name_norm = re.sub(r'\s+', ' ', name_clean.lower().strip())
        key = f"{price_str}|{name_norm[:60]}"
        if key not in products:
            products[key] = {
                "price_num": price_num,
                "price": f"${price_str}",
                "name": name_clean if name_clean else raw_name,
                "specs": specs_part,
                "source": src,
            }
else:
    for doc, meta in zip(all_data["documents"], all_data["metadatas"]):
        src = meta.get("source", "") if meta else ""
        parse_products_from_text(doc, src)

print(f"Parsed {len(products)} unique products")

# ── Step 3: Build clean documents ────────────────────────────────────────────
def make_chunk(p):
    """Format a product as a clean, structured text chunk."""
    name = p["name"]
    price = p["price"]
    specs = p["specs"]

    # Extract known spec fields for explicit labeling
    gpu   = ""
    ram   = ""
    stor  = []
    cpu   = ""
    os_   = ""
    disp  = ""

    for part in [s.strip() for s in specs.split(",")]:
        pl = part.lower()
        if re.search(r'geforce|nvidia|radeon|amd|gtx|rtx|mx\d|intel hd|intel uhd|940mx', pl):
            gpu = part
        elif re.search(r'\d+gb(?:\s+ram)?$|\bddr\b', pl):
            ram = part
        elif re.search(r'\d+gb\s+ssd|\d+tb\s+ssd|\d+gb\s+hdd|\d+tb\s+hdd|\d+tb|\d+gb\s+emmc', pl):
            stor.append(part)
        elif re.search(r'core i\d|celeron|pentium|ryzen|athlon', pl):
            cpu = part
        elif re.search(r'windows|linux|dos|macos|android|chrome', pl, re.I):
            os_ = part
        elif re.search(r'"\s*(?:hd|fhd|uhd|ips|touch)', pl) or re.search(r'\d+\.?\d*"', pl):
            disp = part

    # Tag dedicated-GPU laptops as "gaming laptop" for retrieval
    _dedicated_gpu = bool(re.search(r'geforce|gtx|rtx|radeon\s+r[579]|radeon\s+rx', gpu.lower()) if gpu else False)
    _has_ssd = any(re.search(r'ssd', s.lower()) for s in stor)

    lines = [f"Product: {name}",
             f"Price: {price}"]
    if cpu:   lines.append(f"Processor: {cpu}")
    if ram:   lines.append(f"RAM: {ram}")
    if stor:  lines.append(f"Storage: {', '.join(stor)}")
    if gpu:   lines.append(f"GPU: {gpu}")
    if disp:  lines.append(f"Display: {disp}")
    if os_:   lines.append(f"OS: {os_}")
    lines.append(f"Full specs: {name}, {specs}" if specs else f"Full specs: {name}")

    # ── Attribute normalization tags (same as _smart_chunk_page in app.py) ──────
    _tags = []
    if _dedicated_gpu:
        _tags.append("gaming laptop dedicated GPU")
    if re.search(r'\btouch\b', disp, re.I) or re.search(r'\btouch\b', name, re.I):
        _tags.append("touchscreen display")
    if re.search(r'2\s*in\s*1|360|yoga|spin\b', name, re.I):
        _tags.append("convertible 2-in-1 laptop")
    if _has_ssd:
        _tags.append("fast SSD storage")
    if re.search(r'windows', os_, re.I):
        _tags.append("Windows laptop")
    if re.search(r'android', os_, re.I):
        _tags.append("Android device")
    if _tags:
        lines.append("Features: " + ", ".join(_tags))

    return "\n".join(lines)

docs = []
for key, p in sorted(products.items(), key=lambda x: x[1]["price_num"]):
    chunk_text = make_chunk(p)
    docs.append(Document(
        page_content=chunk_text,
        metadata={"source": p["source"], "price": p["price_num"], "name": p["name"]}
    ))

print(f"Built {len(docs)} structured product documents")
print("\nSample chunk:")
print(docs[10].page_content if len(docs) > 10 else docs[0].page_content)
print("\n--- Verifying Dell 7567 ---")
for d in docs:
    if "7567" in d.page_content:
        print(d.page_content)
        print()

# ── Step 4: Clear old collection and re-ingest ────────────────────────────────
print("\nClearing old collection...")
client.delete_collection(COLLECTION)
col = client.create_collection(COLLECTION)
print("Collection cleared and recreated.")

# Use FastEmbed (same model as chatbot)
from fastembed import TextEmbedding
model = TextEmbedding("BAAI/bge-small-en-v1.5")

texts = [d.page_content for d in docs]
metas = [d.metadata for d in docs]
ids   = [f"prod_{i}" for i in range(len(docs))]

print(f"Embedding {len(texts)} products...")
embeddings = list(model.embed(texts))
print("Embedding done. Ingesting...")

BATCH = 50
for i in range(0, len(texts), BATCH):
    col.add(
        ids=ids[i:i+BATCH],
        documents=texts[i:i+BATCH],
        embeddings=[e.tolist() for e in embeddings[i:i+BATCH]],
        metadatas=metas[i:i+BATCH],
    )
    print(f"  Ingested {min(i+BATCH, len(texts))}/{len(texts)}")

print(f"\nDone. Total chunks in DB: {col.count()}")
print("One product per chunk — structured and clean.")
