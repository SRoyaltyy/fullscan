"""Dense per-letter descriptors read off Simple View--Calculation.xlsx.

The live file is Sheet1 A1:JO364 — 275 letters, 35,769 formulas, CF fills.
Screenshot row-1 headers (noodle strip the owner scrolls):

  A Date  B Close  C Open  D High  E Low  F Volume
  G Vol ratio  H Intraday  I Daily  J Range/open-to-open
  K Body  L Candl  M Boomer  N Yesterday  O Volume sls
  P-Y tally / flag / #VALUE! mix
  DF / DG / DH 1-bar / 2-bar / 3-bar candle TEXT
  AH FR FQ ER EP EN JB JC ... open-44 prior-print tallies

Clock (excel_clock_gate): lag 0 only for fill_mine_open / value_mine_open.
H and I are labels on row T and features on rows above T. Same-row
DF/DG/DH/BB/BQ/BU is a leak — lag 1+ only.

This factory is the high-definition paint: every letter at every legal
lag emits presence, fill family, shade (pale / mid / deep), sign, frozen
quantiles, sheet-native thresholds, exact streak length, window extrema,
ratios, candle tokens, and horizontal past-X-columns counts.
FDR + holdout still sit on top.

Shade (classify_fill lightness, only when a hex fill is present):
  green pale score==1.0, mid==1.5, deep>=2.0
  red   pink score==-1.0, midr==-1.5, deepr<=-2.0
Unknown intensity (family without hex) fires _g / _r only — never
pretends to be deep. _g remains the mixed bucket.

Research only. flatten_robust untouched.
"""
from __future__ import annotations

from excel_clock_gate import FILL_OPEN, VALUE_OPEN_44, SKIP_VALUE_OPS
from signals import classify_fill

WINDOWS = (3, 5, 10, 20, 40)
STREAK_GATES = (2, 3, 4, 5, 8, 12)
SHADE_STREAK_GATES = (2, 3, 5)
COUNT_GATES = (2, 3, 4, 6, 8, 12, 16)
SHADE_COUNT_GATES = (2, 3, 6, 8, 12)
LAGS = (0, 1, 2, 3, 5)
# classify_fill scores: green 1.0 / 1.5 / 2.0, red -1.0 / -1.5 / -2.0
SHADE_GREEN = ("pale", "mid", "deepg")
SHADE_RED = ("pink", "midr", "deepr")
RATIO_ANCHORS = ("C", "J", "H", "G", "I")
COL_WINDOWS = (3, 5, 8, 15)
