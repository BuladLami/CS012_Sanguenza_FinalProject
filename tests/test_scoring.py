"""Checks that the formula library reproduces the thesis baseline score S'.

Run:  python -m unittest discover -s tests
"""
import csv
import unittest
from pathlib import Path

from common import scoring as sc

DATA = Path(__file__).resolve().parent.parent / "data" / "talomo_site_features.csv"


def load():
    with open(DATA, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in ("building_density", "road_distance", "facility_distance", "shoreline_distance",
                  "baseline_score"):
            r[k] = float(r[k])
        for h in sc.HAZARD_FEATURES:
            r[h] = int(r[h])
        r["road_accessibility"] = sc.f2_road_accessibility(r["road_distance"])
    return rows


def centralised(rows):
    b = sc.local_bounds(rows, sc.ALL_FEATURES)
    raw = {}
    for r in rows:
        norm = {f: sc.f1_minmax(r[f], *b[f]) for f in sc.ALL_FEATURES}
        raw[r["site_id"]] = sc.f3_composite(norm)
    retained = [raw[r["site_id"]] for r in rows if r["shoreline_distance"] >= sc.SHORELINE_BUFFER_M]
    lo, hi = min(retained), max(retained)
    return {k: sc.f5_rescale(v, lo, hi) for k, v in raw.items()}


class ScoringTest(unittest.TestCase):
    def test_reproduces_baseline(self):
        rows = load()
        self.assertEqual(len(rows), 1502)
        s = centralised(rows)
        worst = max(abs(s[r["site_id"]] - r["baseline_score"]) for r in rows)
        self.assertLess(worst, 1e-9)

    def test_sharded_bounds_equal_global(self):
        rows = load()
        parts = [sc.local_bounds(rows[i::3], sc.ALL_FEATURES) for i in range(3)]
        self.assertEqual(sc.merge_bounds(parts), sc.local_bounds(rows, sc.ALL_FEATURES))

    def test_class_cut_matches_rank_definition(self):
        scores = [0.1, 0.2, 0.2, 0.3, 0.5]
        cut = sc.f15_class_cut(sorted(scores))
        for s in scores:
            p = sum(1 for o in scores if o < s) / len(scores)
            self.assertEqual(p >= 0.5, s > cut)

    def test_gates(self):
        self.assertEqual(sc.f22_gates(10, {h: 0 for h in sc.HAZARD_FEATURES}), (True, False, False))
        hz = {h: 0 for h in sc.HAZARD_FEATURES}
        hz["flood_susceptibility"] = 3
        self.assertEqual(sc.f22_gates(500, hz), (False, True, False))


if __name__ == "__main__":
    unittest.main()
