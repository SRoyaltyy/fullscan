# Pixel descriptors — read off Simple View--Calculation.xlsx

The live file is Sheet1 **A1:JO364**: 275 letters, 35,769 formulas, CF fills.
`NOTES.md` + `CLOCK_MAP.md` + `excel_open_features.py` are the read of that file.

## Column families (from the replica)

| letters | in the sheet | mined as |
|---|---|---|
| A | date serial + regime fill | fill lag 0; date number skipped |
| B | close + open fill | fill lag 0; number lag 1+ |
| C | open | fill + number lag 0 |
| D E F | high / low / volume | lag 1+ |
| G K L M O | camera fills (timing-open) | fill lag 0; numbers lag 1+ |
| H | (high-low)/open | **label on T**; feature on T-1, T-2 |
| I | close/prev-1 | **label on T**; feature on T-1, T-2 |
| J | open-to-open | number + fill lag 0 |
| DF DG DH | candle text | exact token lag 1+ |
| BB BQ BU | doji / money-flow / typical-sign | lag 1+ |
| AH FR FQ ER EP EN JB JC | prior-print tallies (open-44) | number lag 0 |
| every other A–JO | formula + CF | lag 1+ fill, number, text, streak, window |

## Pixels per letter

Lag L in {0,1,2} when the clock allows:

- present / blank
- fill green / red / deep-green / deep-red / none
- number pos / neg / zero + vs p10 p25 p50 p75 p90
- exact text token (DF/DG/DH and anything stored as text)
- streak of + / − / green / red ending at that lag (2,3,5,8)
- windows 5/10/20: green count, red count, max, min, last-is-extreme
- ratio vs C, J, H[t-1] at >1, >1.5, <0.5, <0

Horizontal bands (“in the past X columns” on one row):
A–O, P–Z, AA–AZ, BA–BZ, CA–CZ, DA–DH, EA–EZ, FA–FZ, GA–GZ, HA–HZ, IA–JO
on lag-1 and lag-2: green/red counts and majority paint.

FDR + holdout + Apriori stay on top.
