#!/usr/bin/env python3
"""
Compute country-specific digital skill and knowledge keyness using
the log-likelihood ratio (G^2) test after Dunning (1993).

Definitions:
- digital skills: ESCO skill concepts in the S5 branch ("working with computers")
- digital knowledge: ESCO knowledge concepts in the ICT branch (code 06*)

Counts are based on ESCO concept mentions in country-level *_esco.jsonl files.
For each country and each concept, compare its relative frequency in that
country against the pooled frequency in all other analysed countries.
"""

from __future__ import annotations

import json
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ESCO_DB = ROOT.parent / "comprehensive_esco.db"
ESCO_RESULTS_DIR = ROOT / "esco_skills"
OUT_PATH = ROOT / "digital_keyness.json"

COUNTRY_LABELS = {
    "in": "India",
    "jp": "Japan",
    "kr": "South Korea",
    "malaysia": "Malaysia",
    "mx": "Mexico",
    "ph": "Philippines",
    "pl": "Poland",
    "sg": "Singapore",
    "th": "Thailand",
    "tw": "Taiwan",
    "vn": "Vietnam",
}

MIN_COUNTRY_COUNT = 3
MIN_GLOBAL_COUNT = 10
TOP_N = 15


def normalize_label(s: str) -> str:
    return " ".join(str(s).strip().lower().replace("\u00a0", " ").split())


def load_esco():
    conn = sqlite3.connect(ESCO_DB)
    cur = conn.cursor()
    cur.execute("SELECT code, uri, title, skill_type, level FROM esco_concepts")
    rows = cur.fetchall()
    conn.close()

    by_uri = {}
    skill_s5_uris = set()
    knowledge_06_uris = set()

    for code, uri, title, skill_type, level in rows:
        meta = {
            "code": code,
            "uri": uri,
            "title": title,
            "skill_type": skill_type,
            "level": level,
        }
        by_uri[uri] = meta

        if skill_type == "skills" and code.startswith("S5"):
            skill_s5_uris.add(uri)
        if skill_type == "knowledge" and code.startswith("06"):
            knowledge_06_uris.add(uri)

    return by_uri, skill_s5_uris, knowledge_06_uris


def g2(a: int, b: int, c: int, d: int) -> float:
    total = a + b + c + d
    if total == 0:
        return 0.0

    row1 = a + b
    row2 = c + d
    col1 = a + c
    col2 = b + d

    expected = [
        row1 * col1 / total,
        row1 * col2 / total,
        row2 * col1 / total,
        row2 * col2 / total,
    ]
    observed = [a, b, c, d]

    score = 0.0
    for obs, exp in zip(observed, expected):
        if obs > 0 and exp > 0:
            score += obs * math.log(obs / exp)
    return 2.0 * score


def rank_country(country_counts, totals_by_country, meta_by_uri):
    total_all = sum(totals_by_country.values())
    global_counts = Counter()
    for counts in country_counts.values():
        global_counts.update(counts)

    result = {}
    for country, counts in country_counts.items():
        country_total = totals_by_country[country]
        other_total = total_all - country_total
        rows = []
        for uri, a in counts.items():
            total_term = global_counts[uri]
            if a < MIN_COUNTRY_COUNT or total_term < MIN_GLOBAL_COUNT:
                continue

            b = country_total - a
            c = total_term - a
            d = other_total - c
            if d < 0:
                continue

            country_rate = a / country_total if country_total else 0.0
            other_rate = c / other_total if other_total else 0.0
            if country_rate <= other_rate:
                continue

            rows.append({
                "uri": uri,
                "code": meta_by_uri[uri]["code"],
                "label": meta_by_uri[uri]["title"],
                "country_count": a,
                "other_count": c,
                "country_rate": country_rate,
                "other_rate": other_rate,
                "lift": (country_rate / other_rate) if other_rate else None,
                "g2": g2(a, b, c, d),
            })

        rows.sort(key=lambda row: (row["g2"], row["country_count"]), reverse=True)
        result[country] = rows[:TOP_N]
    return result


def collect_counts(skill_uris, knowledge_uris):
    skill_counts = defaultdict(Counter)
    knowledge_counts = defaultdict(Counter)
    skill_totals = Counter()
    knowledge_totals = Counter()
    offers = Counter()

    for path in sorted(ESCO_RESULTS_DIR.glob("jobads_*_esco.jsonl")):
        key = path.stem.removeprefix("jobads_").removesuffix("_esco")
        if key not in COUNTRY_LABELS:
            continue
        country = COUNTRY_LABELS[key]

        with open(path) as fh:
            for line in fh:
                offers[country] += 1
                row = json.loads(line)
                for item in row.get("skills", []):
                    uri = item.get("uri")
                    if not uri:
                        continue
                    if uri in skill_uris:
                        skill_counts[country][uri] += 1
                        skill_totals[country] += 1
                    if uri in knowledge_uris:
                        knowledge_counts[country][uri] += 1
                        knowledge_totals[country] += 1

    return skill_counts, knowledge_counts, skill_totals, knowledge_totals, offers


def main():
    by_uri, skill_s5_uris, knowledge_06_uris = load_esco()

    skill_counts, knowledge_counts, skill_totals, knowledge_totals, offers = collect_counts(
        skill_s5_uris,
        knowledge_06_uris,
    )

    payload = {
        "definition": {
            "digital_skills": "ESCO S5 skill branch (working with computers)",
            "digital_knowledge": "ESCO knowledge branch 06* (information and communication technologies)",
            "digital_skill_uri_count": len(skill_s5_uris),
            "digital_knowledge_uri_count": len(knowledge_06_uris),
            "min_country_count": MIN_COUNTRY_COUNT,
            "min_global_count": MIN_GLOBAL_COUNT,
        },
        "country_order": [country for country in COUNTRY_LABELS.values() if country in offers],
        "countries": {
            country: {
                "offers": offers[country],
                "digital_skill_mentions": skill_totals[country],
                "digital_knowledge_mentions": knowledge_totals[country],
            }
            for country in offers
        },
        "skills": rank_country(skill_counts, skill_totals, by_uri),
        "knowledge": rank_country(knowledge_counts, knowledge_totals, by_uri),
    }

    OUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"Saved {OUT_PATH}")


if __name__ == "__main__":
    main()
