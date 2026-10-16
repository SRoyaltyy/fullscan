# Self-test: fetch every IP-blocked official source through the deployed relay.
# Usage: python relay/selftest.py https://fullscan-primary-relay.<subdomain>.workers.dev
import io
import re
import sys
import urllib.parse
import urllib.request

BLOCKED = [
    ('FTC press releases', 'https://www.ftc.gov/news-events/news/press-releases/rss'),
    ('HHS news', 'https://www.hhs.gov/rss/news.xml'),
    ('FERC decisions/notices', 'https://www.ferc.gov/news-events/news/decisions-notices'),
    ('OSHA feeds page', 'https://www.osha.gov/rss/'),
    ('State press releases', 'https://www.state.gov/press-releases/feed/'),
    ('DOT newsroom RSS', 'https://www.transportation.gov/rss.xml'),
    ('FSIS feeds page', 'https://www.fsis.usda.gov/news-events/news-feeds-subscriptions'),
    ('NHTSA press releases', 'https://www.nhtsa.gov/press-releases'),
    ('METI English press list', 'https://www.meti.go.jp/english/press/'),
    ('JFTC press releases', 'https://www.jftc.go.jp/en/pressreleases/'),
    ('India PIB RSS hub', 'https://www.pib.gov.in/ViewRss.aspx?lang=1&reg=1'),
    ('RBNZ press releases', 'https://www.rbnz.govt.nz/news-and-events/news/press-releases'),
    ('ACCC media releases', 'https://www.accc.gov.au/media-release/rss.xml'),
    ('CURIA (EU court) RSS', 'https://curia.europa.eu/site/jcms/qua1_6621/en/rss'),
    ('Bundesbank Latest feed', 'https://www.bundesbank.de/service/rss/en/633306/feed.rss'),
    ('FCC headlines page', 'https://www.fcc.gov/news-events/headlines'),
]

def main():
    relay = sys.argv[1].rstrip('/')
    ok = 0
    for name, target in BLOCKED:
        url = relay + '/?url=' + urllib.parse.quote(target, safe='')
        req = urllib.request.Request(url, headers={'User-Agent': 'FullscanRelaySelftest/1.0'})
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                body = r.read(3_000_000)
                status = r.status
        except urllib.error.HTTPError as e:
            body = e.read(200_000)
            status = e.code
        except Exception as e:
            print(f'ERR  {name:28s} {type(e).__name__}: {e}')
            continue
        rss = bool(re.search(rb'<(rss|feed|RDF)[ >]', body))
        items = len(re.findall(rb'<(?:item|entry)[ >]', body))
        verdict = 'RSS' if rss else ('HTML' if status == 200 else '----')
        if status == 200:
            ok += 1
        print(f'{status}  {verdict:4s} items={items:4d} {name:28s} {target[:65]}')
    print(f'\n{ok}/{len(BLOCKED)} sources reachable through the relay')

if __name__ == '__main__':
    main()
