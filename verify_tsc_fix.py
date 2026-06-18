"""Verify the tsc retrieval fixes against the REAL Modal client DB (pulled to
modal_verify/databases/tsc_pk). Drives retrieve_context with fast=True (no LLM)
and asserts the named products / true global max enter the context.
"""
import asyncio, os, re
os.environ.setdefault("RERANK_CROSS_ENCODER", "0")
from langchain_chroma import Chroma
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings
from services.retrieval import retrieve_context, _split_comparison_entities

DBP = "modal_verify/databases/tsc_pk"
db = Chroma(persist_directory=DBP,
            embedding_function=FastEmbedEmbeddings(model_name="BAAI/bge-small-en-v1.5"))

Q = [
    "Compare Winfun MY BUSY HOUSE, Mont Marte Watercolor Pencils Signature, and Keep Smiling Oil Paints Tubes from most to least expensive",
    "How much more expensive is Winfun MY BUSY HOUSE than Keep Smiling Oil Paints Tubes?",
    "What is the most expensive product in the catalog, and what is the next-most-expensive item shown?",
]
CHECK = {
    0: ["winfun my busy", "mont marte watercolor", "keep smiling oil"],
    1: ["winfun my busy", "keep smiling oil"],
    2: ["981,000", "ultra xtr"],
}


async def main():
    print("chunks:", db._collection.count())
    print("split Q1:", _split_comparison_entities(Q[0]))
    print("split Q2:", _split_comparison_entities(Q[1]))
    print("split Q3:", _split_comparison_entities(Q[2]))
    allok = True
    for i, q in enumerate(Q):
        ctx, n, src = await retrieve_context(q, db, fast=True)
        cl = (ctx or "").lower()
        print(f"\n=== Q{i+1} (docs={n}): {q[:70]}")
        for kw in CHECK[i]:
            ok = kw in cl
            allok = allok and ok
            print(f"    [{'PASS' if ok else 'FAIL'}] context contains {kw!r}")
        head = [l.strip() for l in (ctx or "").split("\n") if re.search(r'rs\.?\s*[\d,]|price', l, re.I)][:4]
        for l in head:
            print("      |", l[:95])
    print("\n==>", "ALL PASS" if allok else "SOME FAIL")


asyncio.run(main())
