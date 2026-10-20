"""SEC filing text for the company record.

Stores the latest annual report, the quarterly reports filed after it, and the
8-K items that change the business in between. Text is plain, sectioned, and
gzipped. Source points are offsets into that text, not a second copy.

Raw HTML, financial-statement tables, and exhibit boilerplate are not stored.
"""
