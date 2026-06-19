"""Offline logic test for the multi-product fan-out (compare/order/basket).
Builds Row objects from the live ground truth the Codex report exported, then runs
the new engine path on the EXACT hard-question strings. No DB / network needed —
validates the algorithm (name extraction, per-product resolution, computation),
including the cases that previously failed (superset titles, sibling rows,
glued-digit titles, near-duplicate D&G vial)."""
import sys, os, re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.catalog_query import Row, _parse_multi, _answer_multi

def R(title, price, cur="Rs."):
    tl = title.lower()
    return Row(title, price, "available", f"https://x/{re.sub('[^a-z0-9]+','-',tl)}", tl, cur, "")

# --- catalog rows (titles+prices from the report's Expected ground truth) ---
rows = [R(t, p) for t, p in [
    # babyfy
    ("Wall Mounted Ping Pong Practice Table Tennis", 2050.0),
    ("Electric Gear Building Blocks (83 Pcs).", 2490.0),
    ("Cute Safety Helmet For Baby Head Protection", 1500.0),
    ("Cartoon Dinosaur Baseball Set", 3130.0),
    ("Magical Painting Set - With Brush", 4750.0),
    ("Pop N Play", 1200.0),  # SUPERSET case: query adds '- Quick Push Pop Game'
    ("Butterfly Flower Wings Light & Music For Girls | Rechargeable", 5450.0),
    ("Instant Camera Printer For Kids - With 2 Extra Reels", 7750.0),
    ("DIY Color Changing Magic Food Set", 5860.0),
    ("Mini Cartoon Table Tennis", 2200.0),
    ("Islamic Learning Tablet", 2300.0),
    ("Unicorn Mini Bank", 3350.0),
    ("Toddler Dentist Play Set", 4850.0),
    ("Dino Transport Car Adventure Set", 3950.0),
    ("Musical Jump Outdoor", 6250.0),
    ("Magnetic Transform Engineering Car Assembled Toys - 35 PCS", 5750.0),
    ("Diecast Model Porsche Palamela", 5700.0),
    ("Magical Princess Play Tent", 5460.0),
    ("Diecast Model Honda Civic Type R 1/32", 3400.0),
    # decoys that share words (sibling-rejection check)
    ("Magical Unicorn Painting Set", 999.0),
    ("Mini Table Tennis Bat", 350.0),
    # perfumeshop (incl glued-digit + D&G vial near-duplicate)
    ("Deep Red By Hugo Boss For Women Eau De Parfum", 11990.0),
    ("Liam Blue Shine For Men By Lattafa", 5430.0),
    ("Guess 1981 Indigo For Men7590", 7299.0),
    ("Azzaro Shine For Unisex By Azzaro", 7890.0),
    ("Light Blue Eau Intense Women By Dolce & Gabbana", 18790.0),
    ("Light Blue Eau Intense Women By D&G Vial", 350.0),
    ("Pure Havane By Mugler For Men", 38800.0),
    ("Baiser Vole By Cartier For Women Eau De Parfum", 29500.0),
    ("Still For Women By Jennifer Lopez EDP", 9749.0),
    ("Viva La Juicy Rose For Women By Juicy Couture", 14990.0),
    ("Windsor By English Laundry For Men Eau De Parfum", 3500.0),
    ("Blue Lady By Rasasi For Women Eau De Parfum", 3250.0),
    # stationery
    ("Exotic Desert - Eraser Box", 390.0),
    ("SUPER HEROES - MONEY BOX", 245.0),
    ("Super Heroes - Eraser Box", 390.0),  # decoy that hijacked old answer
    ("Marble Lotus - Deal of 6", 999.0),
    ("Marble Lotus Claw Clip - Set of 2", 199.0),
    ("Marble Lotus - Claws", 245.0),  # decoy
]]

# (question, op, expected facts to assert)
TESTS = [
    ('Between "Wall Mounted Ping Pong Practice Table Tennis" and "Electric Gear Building Blocks (83 Pcs).", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "Wall Mounted Ping Pong", "diff": 440.0}),
    ('Between "Cute Safety Helmet For Baby Head Protection" and "Cartoon Dinosaur Baseball Set", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "Cute Safety Helmet", "diff": 1630.0}),
    ('Order these from most to least expensive and include each price: "Magical Painting Set - With Brush", "Pop N Play - Quick Push Pop Game", "Butterfly Flower Wings Light & Music For Girls | Rechargeable".',
     "order", {"order": ["Butterfly", "Magical Painting", "Pop N Play"]}),
    ('Order these from most to least expensive and include each price: "Instant Camera Printer For Kids - With 2 Extra Reels", "DIY Color Changing Magic Food Set", "Mini Cartoon Table Tennis".',
     "order", {"order": ["Instant Camera", "DIY Color", "Mini Cartoon"]}),
    ('What is the total cost of buying one "Islamic Learning Tablet" and one "Unicorn Mini Bank"? Show both prices and the total.',
     "basket", {"total": 5650.0}),
    ('What is the total cost of buying one "Toddler Dentist Play Set" and one "Dino Transport Car Adventure Set"? Show both prices and the total.',
     "basket", {"total": 8800.0}),
    ('What is the total cost of buying one "Musical Jump Outdoor" and one "Magnetic Transform Engineering Car Assembled Toys - 35 PCS"? Show both prices and the total.',
     "basket", {"total": 12000.0}),
    ('Order these from most to least expensive and include each price: "Diecast Model Porsche Palamela", "Magical Princess Play Tent", "Diecast Model Honda Civic Type R 1/32".',
     "order", {"order": ["Porsche Palamela", "Princess Play Tent", "Honda Civic"]}),
    ('Between "Deep Red By Hugo Boss For Women Eau De Parfum" and "Liam Blue Shine For Men By Lattafa", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "Liam Blue Shine", "diff": 6560.0}),
    ('Between "Guess 1981 Indigo For Men7590" and "Azzaro Shine For Unisex By Azzaro", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "Guess 1981 Indigo", "diff": 591.0}),
    ('Between "Light Blue Eau Intense Women By Dolce & Gabbana" and "Pure Havane By Mugler For Men", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "Light Blue Eau Intense Women By Dolce", "diff": 20010.0}),
    ('Order these from most to least expensive and include each price: "Baiser Vole By Cartier For Women Eau De Parfum", "Still For Women By Jennifer Lopez EDP", "Viva La Juicy Rose For Women By Juicy Couture".',
     "order", {"order": ["Baiser Vole", "Viva La Juicy", "Still For Women"]}),
    ('What is the total cost of buying one "Windsor By English Laundry For Men Eau De Parfum" and one "Blue Lady By Rasasi For Women Eau De Parfum"? Show both prices and the total.',
     "basket", {"total": 6750.0}),
    ('Between "Exotic Desert - Eraser Box" and "SUPER HEROES - MONEY BOX", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "SUPER HEROES - MONEY BOX", "diff": 145.0}),
    ('Between "Marble Lotus - Deal of 6" and "Marble Lotus Claw Clip - Set of 2", which is cheaper and by exactly how much?',
     "compare", {"cheaper": "Marble Lotus Claw Clip", "diff": 800.0}),
]

def money(s):  # pull all Rs.N numbers
    return [float(x.replace(",", "")) for x in re.findall(r"Rs\.?([\d,]+(?:\.\d+)?)", s)]

passed = 0
for q, exp_op, exp in TESTS:
    mp = _parse_multi(q)
    if not mp:
        print(f"FAIL [no-parse] {q[:60]}"); continue
    if mp["op"] != exp_op:
        print(f"FAIL [op {mp['op']}!={exp_op}] {q[:60]}"); continue
    res = _answer_multi(mp, rows)
    if res is None:
        print(f"FAIL [unresolved] {q[:70]}"); continue
    txt, _ = res
    ok = True
    if "diff" in exp:
        nums = money(txt)
        if exp["diff"] not in nums: ok = False
        if exp["cheaper"].lower() not in txt.lower() or "cheaper by" not in txt.lower(): ok = False
        # cheaper name must appear on the "cheaper by" sentence
        last = txt.lower().rsplit("\n", 1)[-1]
        if exp["cheaper"].lower() not in last: ok = False
    if "total" in exp:
        nums = money(txt)
        if not nums or abs(nums[-1] - exp["total"]) > 0.01: ok = False
    if "order" in exp:
        idxs = [txt.lower().find(name.lower()) for name in exp["order"]]
        if any(i < 0 for i in idxs) or idxs != sorted(idxs): ok = False
    print(("PASS " if ok else "FAIL ") + f"[{exp_op}] {q[:64]}")
    if not ok:
        print("   ---\n   " + txt.replace("\n", "\n   "))
    passed += ok

print(f"\n{passed}/{len(TESTS)} hard questions PASS")
