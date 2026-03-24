
import json
from pathlib import Path

# Mock config
cfg = {
    "bot_name": "MALI",
    "business_name": "My Anime List",
    "topics": "anime, manga, characters",
    "business_description": "A comprehensive database of anime and manga",
    "secondary_prompt": "You are a hardcore anime fan and expert. Use anime slang like 'Sugoi', 'Kawaii', or 'Nani' occasionally. Always try to relate the user's query to a popular anime if it's even slightly relevant. Keep the tone energetic and otaku-friendly."
}

def get_system_prompt(cfg, context, doc_count: int = 0, is_urdu: bool = False):
    bot_name = cfg.get("bot_name", "AI Assistant")
    biz_name = cfg.get("business_name", "the company")
    topics = cfg.get("topics", "general information")
    biz_desc = cfg.get("business_description", "")
    contact_email = cfg.get("contact_email", "")
    secondary_prompt = cfg.get("secondary_prompt", "")

    context_empty  = not context or context.strip() == ""
    context_sparse = not context_empty and doc_count <= 2

    if context_empty:
        kb_section = "(No relevant documents found for this query)"
        grounding_rule = "Use TIER 3 only."
    else:
        kb_section = context
        grounding_rule = "Apply framework."

    sec_section = ""
    if secondary_prompt.strip():
        sec_section = f"\n\nDOMAIN-SPECIFIC MANDATES (Expert Runbook for {biz_name}):\n{secondary_prompt.strip()}\n"

    return f"""You are {bot_name}, the specialized assistant for {biz_name}.

BUSINESS CONTEXT (always available):
- Business: {biz_name}
- What they offer: {biz_desc if biz_desc else topics}
- Topics covered: {topics}
{sec_section}
KNOWLEDGE BASE:
{kb_section}
"""

prompt = get_system_prompt(cfg, "This is some mock context.", 5)
print("PROMPT GENERATED SUCCESSFULLY:")
print(prompt)
