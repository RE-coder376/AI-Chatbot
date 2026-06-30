"""Fresh5 50Q generator — genuinely fresh customer wording (no Fresh1-4 overlap),
~50% Roman-Urdu. count_split prompts deliberately use vocab OUTSIDE the engine's
known lexicon (khatam/ghayab/mojood/done/up-for-grabs/back-ordered) to test SEMANTIC
generalization, not phrase-matching. GT is refreshed live by run_diecast_100q."""
import json

items = []


def add(q, cat, rule, lang, history=None, llm=False, coll=None, forbid=None):
    it = {"id": len(items) + 1, "turn": 2 if history else 1, "history": history or [],
          "question": q, "lang": lang, "category": cat, "expect": [],
          "forbid": forbid or [], "gt_rule": rule}
    if coll:
        it["gt_collection"] = coll
    if llm:
        it["llm_judge"] = True
    items.append(it)


def H(u, a):
    return [{"role": "user", "content": u}, {"role": "assistant", "content": a}]


# ---- count_split (10): novel vocab OUTSIDE the added lexicon on purpose ----
add("Action Figures ka stock kaise divided hai - ready aur khatam", "count_split", {"type": "count_split", "collection": "Action Figures"}, "ur", coll="Action Figures")
add("RC Cars me kitne haazir kitne ghayab", "count_split", {"type": "count_split", "collection": "RC Cars"}, "ur", coll="RC Cars")
add("Bikes: how many up for grabs vs how many done", "count_split", {"type": "count_split", "collection": "Bikes"}, "en", coll="Bikes")
add("SUVs stock me kitne mojood kitne nahi", "count_split", {"type": "count_split", "collection": "SUVs"}, "ur", coll="SUVs")
add("Rolls Royce inventory - sellable headcount and dead headcount", "count_split", {"type": "count_split", "collection": "Rolls Royce"}, "en", coll="Rolls Royce")
add("Formula One kitne mil sakte kitne nahi", "count_split", {"type": "count_split", "collection": "Formula One (F1)"}, "ur", coll="Formula One (F1)")
add("Diecast Decor ka ready-to-ship vs back-ordered count", "count_split", {"type": "count_split", "collection": "Diecast Decor"}, "en", coll="Diecast Decor")
add("Racing Track Set stock ka breakup do", "count_split", {"type": "count_split", "collection": "Racing Track Set"}, "ur", coll="Racing Track Set")
add("RC Drift Cars: in-hand vs out-the-door numbers", "count_split", {"type": "count_split", "collection": "RC Drift Cars"}, "en", coll="RC Drift Cars")
add("RC Construction kitne available kitne finish ho gaye", "count_split", {"type": "count_split", "collection": "RC Construction"}, "ur", coll="RC Construction")

# ---- price (5) ----
add("Iron Man Action Figure ( 12 inches ) ka rate kya hai", "price", {"type": "price", "products": ["Iron Man Action Figure ( 12 inches )"]}, "ur")
add("how much do you want for the BMW M8 1:24", "price", {"type": "price", "products": ["BMW M8 1:24"]}, "en")
add("Suzuki GS125 1:12 Scale price btao", "price", {"type": "price", "products": ["Suzuki GS125 1:12 Scale"]}, "ur")
add("what's the damage on Lamborghini Aventador SVJ63 1:18", "price", {"type": "price", "products": ["Lamborghini Aventador SVJ63 1:18"]}, "en")
add("Mercedes Benz G550 4X4 1 : 24 Scale kitne ka", "price", {"type": "price", "products": ["Mercedes Benz G550 4X4 1 : 24 Scale"]}, "ur")

# ---- stock (5) ----
add("BMW S1000RR Model haath me hai abhi", "stock", {"type": "stock", "products": ["BMW S1000RR Model"]}, "ur")
add("can I grab the Lamborghini URUS 1/24 right now", "stock", {"type": "stock", "products": ["Lamborghini URUS 1/24"]}, "en")
add("Kawasaki Ninja H2R 1/12 Model milega", "stock", {"type": "stock", "products": ["Kawasaki Ninja H2R 1/12 Model"]}, "ur")
add("is the Aston Martin F1 Model 1/18 on the shelf", "stock", {"type": "stock", "products": ["Aston Martin F1 Model 1/18"]}, "en")
add("Toyota Land Cruiser 300 1/24 stock me aya kya", "stock", {"type": "stock", "products": ["Toyota Land Cruiser 300 1/24"]}, "ur")

# ---- minmax incl 'including unavailable' (4) ----
add("SUVs me sabse mehnga konsa chahe khatam ho chuka ho", "minmax", {"type": "minmax", "collection": "SUVs", "mode": "max", "availability": None}, "ur", coll="SUVs")
add("priciest Formula One piece counting the sold ones too", "minmax", {"type": "minmax", "collection": "Formula One (F1)", "mode": "max", "availability": None}, "en", coll="Formula One (F1)")
add("Bikes ka sabse sasta jo abhi khareed sakun", "minmax", {"type": "minmax", "collection": "Bikes", "mode": "min", "availability": True}, "ur", coll="Bikes")
add("cheapest Rolls Royce ignoring whether it's sold out", "minmax", {"type": "minmax", "collection": "Rolls Royce", "mode": "min", "availability": None}, "en", coll="Rolls Royce")

# ---- list sold-out / available (4) ----
add("Bikes jo bik chuke sirf wahi gin do", "list", {"type": "list", "collection": "Bikes", "availability": False, "limit": 4}, "ur", coll="Bikes")
add("show me only the RC Cars I can actually buy", "list", {"type": "list", "collection": "RC Cars", "availability": True, "limit": 4}, "en", coll="RC Cars")
add("Rolls Royce me konse out of stock pade hain", "list", {"type": "list", "collection": "Rolls Royce", "availability": False, "limit": 3}, "ur", coll="Rolls Royce")
add("which SUVs are sitting in stock", "list", {"type": "list", "collection": "SUVs", "availability": True, "limit": 4}, "en", coll="SUVs")

# ---- compare (4) ----
add("BMW M8 1:24 aur BMW 760li 1:24 me konsa sasta", "compare", {"type": "compare", "products": ["BMW M8 1:24", "BMW 760li 1:24"], "mode": "cheaper"}, "ur")
add("between Lexus LX570 1/24 and Land Cruiser 80 Off Road 1/24 which costs more", "compare", {"type": "compare", "products": ["Lexus LX570 1/24", "Land Cruiser 80 Off Road 1/24"], "mode": "expensive"}, "en")
add("Harley Sportster S Diecast Model ya Kawasaki Ninja H2R 1/12 Model - konsa stock me", "compare", {"type": "compare", "products": ["Harley Sportster S Diecast Model", "Kawasaki Ninja H2R 1/12 Model"], "mode": "stock"}, "ur")
add("Flash Action Figure ( 12 inches ) vs Venom Action Figure ( 12 inches ) which is cheaper", "compare", {"type": "compare", "products": ["Flash Action Figure ( 12 inches )", "Venom Action Figure ( 12 inches )"], "mode": "cheaper"}, "en")

# ---- basket (2) ----
add("agar BMW M8 1:24 aur Iron Man Action Figure ( 12 inches ) dono lun to total", "basket", {"type": "basket", "products": ["BMW M8 1:24", "Iron Man Action Figure ( 12 inches )"]}, "ur")
add("combined bill for Suzuki GS125 1:12 Scale plus Kawasaki Ninja H2R 1/12 Model", "basket", {"type": "basket", "products": ["Suzuki GS125 1:12 Scale", "Kawasaki Ninja H2R 1/12 Model"]}, "en")

# ---- ordering (2) ----
add("Rolls Royce Phantom 1/24, Rolls Royce Spectre 1/24, Rolls Royce SUV Cullinan 1:20 Big Scale ko saste se mehnge tak lagao", "ordering", {"type": "ordering", "products": ["Rolls Royce Phantom 1/24", "Rolls Royce Spectre 1/24", "Rolls Royce SUV Cullinan 1:20 Big Scale"]}, "ur")
add("rank Iron Man Action Figure ( 12 inches ), Flash Action Figure ( 12 inches ), Dragon Ball Z Son Goku Action Figure by price", "ordering", {"type": "ordering", "products": ["Iron Man Action Figure ( 12 inches )", "Flash Action Figure ( 12 inches )", "Dragon Ball Z Son Goku Action Figure"]}, "en")

# ---- budget (2) ----
add("koi SUV jo 4500 se kam ho aur available ho", "budget", {"type": "budget", "collection": "SUVs", "max": 4500, "availability": True, "limit": 3}, "ur", coll="SUVs")
add("action figure under 4000 that I can buy", "budget", {"type": "budget", "collection": "Action Figures", "max": 4000, "availability": True, "limit": 3}, "en", coll="Action Figures")

# ---- coref follow-ups (12): fresh referential wording ----
add("in dono me jo sasta hai wo stock me hai", "followup", {"type": "compare", "products": ["BMW M8 1:24", "BMW 760li 1:24"], "mode": "cheaper"}, "ur",
    history=H("BMW M8 1:24 vs BMW 760li 1:24 price", "BMW M8 1:24 is Rs.4,950 and BMW 760li 1:24 is Rs.4,950."))
add("us teen me sabse mehnga konsa", "followup", {"type": "compare", "products": ["Rolls Royce Phantom 1/24", "Rolls Royce Spectre 1/24", "Rolls Royce SUV Cullinan 1:20 Big Scale"], "mode": "expensive"}, "ur",
    history=H("Rolls Phantom Spectre Cullinan dikhao", "Rolls Royce Phantom 1/24 Rs.4,330, Rolls Royce Spectre 1/24 Rs.4,420, Rolls Royce SUV Cullinan 1:20 Big Scale Rs.4,780."))
add("jo available wala tha uska price", "followup", {"type": "price", "products": ["Lamborghini URUS 1/24"]}, "ur",
    history=H("BMW XM vs Lamborghini URUS 1/24 stock", "BMW XM SUV 1:24 is out of stock. Lamborghini URUS 1/24 is available at Rs.4,250."))
add("repeat just the sold-out tally", "followup", {"type": "count_split", "collection": "RC Cars"}, "en",
    history=H("RC Cars in/out tally", "RC Cars has 19 total, 9 available, 10 out of stock."), coll="RC Cars")
add("un me se sirf jo abhi khareed sakta hun", "followup", {"type": "compare", "products": ["Harley Sportster S Diecast Model", "Kawasaki Ninja H2R 1/12 Model"], "mode": "stock"}, "ur",
    history=H("Harley Sportster S Diecast Model vs Kawasaki Ninja H2R 1/12 Model", "Harley Sportster S Diecast Model is out of stock; Kawasaki Ninja H2R 1/12 Model is available at Rs.3,650."))
add("wahi do dobara total karke batao", "followup", {"type": "basket", "products": ["Suzuki GS125 1:12 Scale", "Kawasaki Ninja H2R 1/12 Model"]}, "ur",
    history=H("Suzuki GS125 1:12 Scale and Kawasaki Ninja H2R 1/12 Model price", "Suzuki GS125 1:12 Scale is Rs.3,250 and Kawasaki Ninja H2R 1/12 Model is Rs.3,650."))
add("now just give the live headcount", "followup", {"type": "count_split", "collection": "SUVs"}, "en",
    history=H("SUVs availability split", "SUVs has 7 total, 5 available, 2 out of stock."), coll="SUVs")
add("sort those same three cheapest first", "followup", {"type": "ordering", "products": ["Iron Man Action Figure ( 12 inches )", "Flash Action Figure ( 12 inches )", "Dragon Ball Z Son Goku Action Figure"]}, "en",
    history=H("Iron Man Flash Goku figures prices", "Iron Man Action Figure ( 12 inches ) Rs.3,650, Flash Action Figure ( 12 inches ) Rs.3,950, Dragon Ball Z Son Goku Action Figure Rs.4,500."))
add("us category ka gone wala count", "followup", {"type": "count_split", "collection": "Bikes"}, "ur",
    history=H("Bikes available vs sold out", "Bikes has 10 total, 4 available, 6 out of stock."), coll="Bikes")
add("which of the two is pricier", "followup", {"type": "compare", "products": ["Lexus LX570 1/24", "Land Cruiser 80 Off Road 1/24"], "mode": "expensive"}, "en",
    history=H("Lexus LX570 1/24 aur Land Cruiser 80 Off Road 1/24", "Lexus LX570 1/24 is Rs.4,470 and Land Cruiser 80 Off Road 1/24 is Rs.4,990."))
add("uska rate kya tha", "followup", {"type": "price", "products": ["Mercedes AMG ONE 1:24"]}, "ur",
    history=H("Mercedes AMG ONE 1:24 available?", "Mercedes AMG ONE 1:24 is available."))
add("first one - is it buyable", "followup", {"type": "stock", "products": ["BMW M4 IM Diecast Model 1/24"]}, "en",
    history=H("BMW M4 IM Diecast Model 1/24 and BMW M3 E30 1/24", "BMW M4 IM Diecast Model 1/24 is Rs.5,950 and BMW M3 E30 1/24 is Rs.5,150."))

# ---- advisory + policy (4, llm-judged) ----
add("bachay ke liye koi acha gift car suggest karo 5000 ke andar", "advisory", {"type": "budget", "collection": "Diecast Models", "max": 5000, "availability": True, "limit": 3}, "ur", llm=True, coll="Diecast Models")
add("what would you recommend as a gift under 4000", "advisory", {"type": "budget", "collection": "Diecast Models", "max": 4000, "availability": True, "limit": 3}, "en", llm=True, coll="Diecast Models")
add("delivery kitne din me hoti hai", "policy", {"type": "none"}, "ur", llm=True)
add("what's your return policy", "policy", {"type": "none"}, "en", llm=True)

json.dump(items, open(r"C:\Users\User\Documents\Projects\AI_Chatbot\DIECAST_FRESH5_50Q_20260630.json", "w", encoding="utf-8"), ensure_ascii=False, indent=1)
print("wrote", len(items), "items | count_split=", sum(1 for i in items if i["category"] == "count_split"),
      "followup=", sum(1 for i in items if i["category"] == "followup"),
      "ur=", sum(1 for i in items if i["lang"] == "ur"))
