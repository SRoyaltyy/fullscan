"""Add a spaCy + K-Means analysis layer to the static scanner-backtest page.

For every intake article, spaCy dependency parsing extracts:
  - named entities grouped by type (ORG / PERSON / GPE / ...)
  - Subject-Verb-Object triples (who did what to whom)
  - subject/object noun phrase lists (the displaCy-style term division)
  - the article's theme cluster from a K-Means model fit on TF-IDF vectors
    (fixed random_state -> fully deterministic and reproducible)

Outputs dashboard/scanner-backtest/spacy.jsonl (per article) and
spacy-clusters.json (cluster id -> label terms -> member count).

Run:
    python scripts/export_spacy_analysis.py                 # full intake
    python scripts/export_spacy_analysis.py --limit 200     # smoke test
"""
from __future__ import annotations

import argparse
import json
import os
import re
import time
from collections import Counter
from pathlib import Path

import numpy as np
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer

ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "dashboard" / "scanner-backtest"
SPACY_OUT = OUT_DIR / "spacy.jsonl"
CLUSTERS_OUT = OUT_DIR / "spacy-clusters.json"

N_CLUSTERS = 30
SEED = 42
MAX_TOKENS = 6000  # spaCy parse cap per article (speed); theme vector uses full text

_NLP = None


def _init_worker():
    """Load the spaCy model once per worker; the cluster model is read-only."""
    global _NLP
    import spacy
    _NLP = spacy.load("en_core_web_sm")


def _load_cluster_model(articles: list[dict]):
    """Fit TF-IDF + K-Means on all intake texts. Deterministic: fixed seed."""
    texts = [str(a.get("text") or "") for a in articles]
    vectorizer = TfidfVectorizer(max_features=20000, stop_words="english", min_df=3)
    matrix = vectorizer.fit_transform(texts)
    kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=SEED, n_init=10)
    labels = kmeans.fit_predict(matrix)
    terms = np.array(vectorizer.get_feature_names_out())
    clusters = {}
    for k in range(N_CLUSTERS):
        centroid = kmeans.cluster_centers_[k]
        top = centroid.argsort()[::-1][:8]
        clusters[str(k)] = {
            "label": ", ".join(terms[i] for i in top),
            "count": int((labels == k).sum()),
        }
    return vectorizer, kmeans, clusters, labels


SUBJ_DEPS = {"nsubj", "nsubjpass", "agent"}
OBJ_DEPS = {"dobj", "obj", "pobj", "iobj"}


def _phrase(token, max_words: int = 6) -> str:
    words = [w.text for w in token.subtree if not w.is_punct][:max_words]
    return re.sub(r"\s+", " ", " ".join(words)).strip()


def analyze_article(item) -> dict:
    article, cluster = item
    text = str(article.get("text") or "")
    head = text[: MAX_TOKENS * 6]  # rough char cap for the parse
    doc = _NLP(head)

    entities: dict[str, list[str]] = {}
    SKIP_ENT = {"CARDINAL", "ORDINAL", "QUANTITY", "PERCENT", "MONEY", "DATE", "TIME"}
    for ent in doc.ents:
        if ent.label_ in SKIP_ENT:
            continue  # numbers and dates are not relation terms
        clean = re.sub(r"\s+", " ", ent.text).strip(" :;,.").strip()
        words = clean.split()
        if not clean or len(words) > 5:
            continue  # navigation/menu spam tends to be long runs
        ascii_alpha = sum(1 for ch in clean if ch.isascii() and ch.isalpha())
        alpha = sum(1 for ch in clean if ch.isalpha())
        if alpha and ascii_alpha / alpha < 0.7:
            continue  # drop "Dansk Deutsch Eλληνικά"-style menu junk
        entities.setdefault(ent.label_, [])
        if len(entities[ent.label_]) < 10 and clean not in entities[ent.label_]:
            entities[ent.label_].append(clean)

    svos, subjects, objects = [], [], []
    for token in doc:
        if token.pos_ != "VERB":
            continue
        subj = next((c for c in token.children if c.dep_ in SUBJ_DEPS), None)
        obj = next((c for c in token.children if c.dep_ in OBJ_DEPS), None)
        if subj is not None:
            s = _phrase(subj)
            if s and s.lower() not in {x.lower() for x in subjects} and len(subjects) < 10:
                subjects.append(s)
        if obj is not None:
            o = _phrase(obj)
            if o and o.lower() not in {x.lower() for x in objects} and len(objects) < 10:
                objects.append(o)
        if subj is not None and obj is not None and len(svos) < 8:
            svos.append([_phrase(subj), token.lemma_, _phrase(obj)])

    return {
        "id": article.get("id"),
        "entities": entities,
        "subjects": subjects,
        "objects": objects,
        "svos": svos,
        "cluster": cluster,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--news", type=Path, default=ROOT / "data" / "news_intake" / "2026-10-17" / "documents.json")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 2) - 2))
    args = parser.parse_args(argv)

    import sys
    sys.path.insert(0, str(ROOT))
    from src.link_correlation import load_articles

    started = time.time()
    articles = load_articles(args.news, 0)
    if args.limit:
        articles = articles[: args.limit]
    print(f"intake: {len(articles)} articles")

    print("fitting TF-IDF + K-Means (deterministic, seed=42)…")
    _, _, clusters, labels = _load_cluster_model(articles)
    CLUSTERS_OUT.write_text(json.dumps({"seed": SEED, "k": N_CLUSTERS, "clusters": clusters}, indent=1), encoding="utf-8")
    print(f"clusters: {N_CLUSTERS}  ({time.time() - started:.0f}s)")

    import concurrent.futures as futures
    out_path = SPACY_OUT if not args.limit else SPACY_OUT.with_suffix(f".smoke{args.limit}.jsonl")
    done = 0
    with out_path.open("w", encoding="utf-8") as out, \
            futures.ProcessPoolExecutor(max_workers=args.workers, initializer=_init_worker) as pool:
        indexed = list(zip(articles, (int(x) for x in labels)))
        for result in pool.map(analyze_article, indexed, chunksize=16):
            out.write(json.dumps(result, ensure_ascii=False) + "\n")
            done += 1
            if done % 500 == 0:
                out.flush()
                rate = done / (time.time() - started)
                print(f"analyzed {done}/{len(articles)}  {rate:.1f}/s  eta {(len(articles)-done)/max(rate,0.01)/60:.1f} min", flush=True)

    print(f"wrote {out_path.name}: {done} articles  ({time.time() - started:.0f}s total)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
