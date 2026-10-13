# Institutional actions from titles

`institutional_actions` is an independent recognition result in news intake,
the saved Lane queue, compact dashboard records and one-shot Lane results.
It runs on the unchanged title and does not require an article body, ticker,
market sector or trading decision. Existing Lane labels and market verdicts
are unchanged.

Each candidate preserves the named authority, authority type, jurisdiction,
federal/state/territorial level, action category, action text, subject and stage.
Exact authority and action spans refer to the original title. Unnamed courts
and missing jurisdictions remain unknown; a company's domicile does not
establish the court's jurisdiction.

Stages distinguish proposed or uncertain actions, scheduled votes, committee
approval, chamber passage, enactment, interim court decisions and reported
decisions/actions. A reported decision is not primary-source verification,
proof of implementation or an effective date. Explicit negation is not
converted to an implemented action. An FDA-approved adjective is not a fresh
approval announcement.

US priority coverage includes named federal regulators, Federal Reserve,
executive actors, explicitly US courts and legislature, named state authorities
and Puerto Rico, Guam, US Virgin Islands, American Samoa and Northern Mariana
Islands authorities. Foreign and international institutions are retained.
Bare agency aliases use the documented authority table; this bounded rules
recognizer is not an exhaustive jurisdiction or legal entity resolver.

The recognizer returns candidates independently of Lane's market classifier.
It does not itself assign sectors, ticker exposures or stock direction. Empty
results mean no supported action was identified by these rules, not proof that
the title has no policy or judicial content. Complex clauses and unfamiliar
authorities require further coverage and independent title-only evaluation.

Validation: `PYTHONPATH=. python -m unittest discover -s scripts -p
'test_institutional_actions.py'`. The focused fixtures are development tests;
they are not frontier acceptance evidence or a measured accuracy score.
