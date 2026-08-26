import os

# catalyst_analysis.py still does os.environ["SEARXNG_URL"] at import time.
# Search itself is Grok-native; this only keeps the old module importable.
os.environ.setdefault("SEARXNG_URL", "unused")
