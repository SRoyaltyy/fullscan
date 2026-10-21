# updown_v5_preopen (research only, add-only)

Starts from v4's base (v3 variant B features, the same walk-forward windows, purge and HistGB+Platt) and adds five free-source blocks, one at a time.
Each block is tested with and without it. Brier is scored against the coin flip (0.25) and the base rate at 1d/5d/21d/63d, with Holm per family. Leak test: `test_leak.py`.
How each block's data is lagged is set out in `sources.py`. Insider filings count only if their FILING_DATE is on or before D, which is stricter than an acceptance time before 09:25 ET on N.

**Verdict: no variant beats the coin flip or the base rate at any horizon, and no block improves on the version without it.
The Cboe/FRED regime block makes it worse (1d and 21d, Holm p<0.05).**

| variant | h | Brier | base-rate Brier | t vs coin | p_holm coin | t vs base | delta vs without | p_holm without | beats coin | beats base | helps |
|---|---|---|---|---|---|---|---|---|---|---|---|
| base_v3 | 1d | 0.25016 | 0.25000 | -0.76 | 1.000 | -1.04 | +nan | nan | False | False | - |
| +iv | 1d | 0.25017 | 0.25000 | -0.94 | 1.000 | -1.16 | +0.00001 | 1.000 | False | False | False |
| +short | 1d | 0.25013 | 0.25000 | -0.62 | 1.000 | -0.87 | -0.00002 | 1.000 | False | False | False |
| +insider | 1d | 0.25016 | 0.25000 | -0.77 | 1.000 | -1.03 | +0.00000 | 1.000 | False | False | False |
| +ftd | 1d | 0.25016 | 0.25000 | -0.76 | 1.000 | -1.06 | +0.00000 | 1.000 | False | False | False |
| +regime | 1d | 0.25127 | 0.25000 | -2.80 | 0.142 | -3.36 | +0.00112 | 0.015 | False | False | False |
| v5_all | 1d | 0.25104 | 0.25000 | -2.47 | 0.348 | -3.02 | +0.00088 | nan | False | False | - |
| base_v3 | 5d | 0.25020 | 0.24963 | 0.03 | 1.000 | -0.65 | +nan | nan | False | False | - |
| +iv | 5d | 0.25007 | 0.24963 | 0.15 | 1.000 | -0.58 | -0.00013 | 1.000 | False | False | False |
| +short | 5d | 0.25021 | 0.24963 | 0.02 | 1.000 | -0.63 | +0.00001 | 1.000 | False | False | False |
| +insider | 5d | 0.25017 | 0.24963 | 0.05 | 1.000 | -0.64 | -0.00003 | 1.000 | False | False | False |
| +ftd | 5d | 0.25007 | 0.24963 | 0.14 | 1.000 | -0.54 | -0.00013 | 1.000 | False | False | False |
| +regime | 5d | 0.25219 | 0.24963 | -1.33 | 1.000 | -1.78 | +0.00199 | 1.000 | False | False | False |
| v5_all | 5d | 0.25209 | 0.24963 | -1.22 | 1.000 | -1.72 | +0.00189 | nan | False | False | - |
| base_v3 | 21d | 0.25348 | 0.24975 | -0.95 | 1.000 | -1.06 | +nan | nan | False | False | - |
| +iv | 21d | 0.25170 | 0.24975 | -0.71 | 1.000 | -0.80 | -0.00178 | 1.000 | False | False | False |
| +short | 21d | 0.25444 | 0.24975 | -1.05 | 1.000 | -1.17 | +0.00096 | 1.000 | False | False | False |
| +insider | 21d | 0.25343 | 0.24975 | -0.94 | 1.000 | -1.05 | -0.00005 | 1.000 | False | False | False |
| +ftd | 21d | 0.25309 | 0.24975 | -0.99 | 1.000 | -1.13 | -0.00039 | 1.000 | False | False | False |
| +regime | 21d | 0.27749 | 0.24975 | -2.76 | 0.155 | -2.73 | +0.02401 | 0.046 | False | False | False |
| v5_all | 21d | 0.26856 | 0.24975 | -2.33 | 0.497 | -2.31 | +0.01508 | nan | False | False | - |
| base_v3 | 63d | 0.25816 | 0.24945 | -0.62 | 1.000 | -0.78 | +nan | nan | False | False | - |
| +iv | 63d | 0.25882 | 0.24945 | -0.69 | 1.000 | -0.87 | +0.00066 | 1.000 | False | False | False |
| +short | 63d | 0.25819 | 0.24945 | -0.63 | 1.000 | -0.80 | +0.00003 | 1.000 | False | False | False |
| +insider | 63d | 0.25750 | 0.24945 | -0.62 | 1.000 | -0.81 | -0.00066 | 1.000 | False | False | False |
| +ftd | 63d | 0.25794 | 0.24945 | -0.67 | 1.000 | -0.87 | -0.00022 | 1.000 | False | False | False |
| +regime | 63d | 0.27034 | 0.24945 | -1.00 | 1.000 | -1.15 | +0.01218 | 1.000 | False | False | False |
| v5_all | 63d | 0.27315 | 0.24945 | -1.13 | 1.000 | -1.35 | +0.01499 | nan | False | False | - |
