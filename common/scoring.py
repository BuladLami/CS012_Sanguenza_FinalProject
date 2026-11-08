"""POLARIS formula library (pure Python, no NumPy needed on the workers).

Formula IDs match the thesis formula register (POLARIS_model.ipynb, Cell 3):
  F1  Min-Max normalisation           x' = (x - min) / (max - min)
  F2  Road accessibility              RA = 1 / (1 + d)
  F3  Hazard-integrated composite     S  = 0.40 BD' + 0.25 RA' + 0.15 FD'
                                           - 0.08 FL' - 0.07 LS' - 0.05 SS'
  F5  Score rescaling                 S' = clip((S - S_min) / (S_max - S_min), 0, 1)
                                      bounds taken from sites OUTSIDE the shoreline setback
  F14 Suitability classes             tau1 = 0.25, tau2 = 0.40, tau3 = 0.60
  F15 Intra-class percentile          P = (# same-class sites strictly lower) / n_class
                                      P >= 0.50 -> upper recommendation level
  F22 Hard eligibility gates          ineligible if d_coast < 50 m OR any hazard class == 3

The min/max in F1 and F5 and the percentile in F15 are GLOBAL statistics over all
1,502 sites, but the sites live on three shards. The distributed job therefore runs
as map -> reduce -> broadcast rounds; the helpers below are the map and reduce steps.
"""
from __future__ import annotations

import math
from typing import Dict, Iterable, List, Optional, Tuple

SERVICE_WEIGHTS = {"building_density": 0.40, "road_accessibility": 0.25, "facility_distance": 0.15}
HAZARD_WEIGHTS = {"flood_susceptibility": 0.08, "landslide_susceptibility": 0.07,
                  "storm_surge_susceptibility": 0.05}
SERVICE_FEATURES = list(SERVICE_WEIGHTS)
HAZARD_FEATURES = list(HAZARD_WEIGHTS)
ALL_FEATURES = SERVICE_FEATURES + HAZARD_FEATURES

TAU1, TAU2, TAU3 = 0.25, 0.40, 0.60
SHORELINE_BUFFER_M = 50.0
CLASSES = ["Not Suitable", "Low Suitability", "Moderately Suitable", "Highly Suitable"]
CLASS_LEVEL_RANGE = {"Not Suitable": (0, 1), "Low Suitability": (1, 2),
                     "Moderately Suitable": (2, 3), "Highly Suitable": (3, 4)}
LEVEL_LABELS = {0: "Not Recommended", 1: "Conditionally Suitable",
                2: "Suitable with Reservations", 3: "Recommended", 4: "Highly Recommended"}

Bounds = Dict[str, Tuple[float, float]]


def f2_road_accessibility(d_m: float) -> float:
    return 1.0 / (1.0 + float(d_m))


def f1_minmax(x: float, lo: float, hi: float) -> float:
    return 0.0 if hi - lo == 0 else (x - lo) / (hi - lo)


def local_bounds(rows: Iterable[Dict[str, float]], keys: List[str]) -> Bounds:
    out: Bounds = {}
    for r in rows:
        for k in keys:
            v = float(r[k])
            lo, hi = out.get(k, (math.inf, -math.inf))
            out[k] = (min(lo, v), max(hi, v))
    return out


def merge_bounds(parts: Iterable[Bounds]) -> Bounds:
    """REDUCE step: combine per-shard (min, max) into global (min, max)."""
    out: Bounds = {}
    for b in parts:
        for k, (lo, hi) in b.items():
            glo, ghi = out.get(k, (math.inf, -math.inf))
            out[k] = (min(glo, lo), max(ghi, hi))
    return out


def f3_composite(norm: Dict[str, float]) -> float:
    s = sum(w * norm[f] for f, w in SERVICE_WEIGHTS.items())
    return s - sum(w * norm[f] for f, w in HAZARD_WEIGHTS.items())


def f5_rescale(raw: float, lo: float, hi: float) -> float:
    if hi - lo == 0:
        return 0.0
    return min(1.0, max(0.0, (raw - lo) / (hi - lo)))


def f14_classify(score: float) -> str:
    if score < TAU1:
        return CLASSES[0]
    if score < TAU2:
        return CLASSES[1]
    if score < TAU3:
        return CLASSES[2]
    return CLASSES[3]


def f15_class_cut(sorted_scores: List[float]) -> Optional[float]:
    """Global cut for one class: a site is in the upper level iff score > cut.

    P >= 0.5  <=>  #strictly-lower >= ceil(n/2) = k  <=>  score > a[k-1]
    where a is the class's scores sorted ascending.
    """
    n = len(sorted_scores)
    if n == 0:
        return None
    k = math.ceil(n / 2)
    return sorted_scores[k - 1]


def f22_gates(shoreline_m: float, hazards: Dict[str, int]) -> Tuple[bool, bool, bool]:
    shore = float(shoreline_m) < SHORELINE_BUFFER_M
    high = any(int(hazards[h]) == 3 for h in HAZARD_FEATURES)
    return shore, high, not (shore or high)
