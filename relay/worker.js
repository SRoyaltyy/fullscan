// Fullscan primary-action fetch relay.
// Fetches official-source URLs from Cloudflare's edge network when the caller's
// IP range is blocked by agency WAFs (GitHub Actions / datacenter egress).
// Contract: GET ?url=<official-url> -> upstream status, content-type and body
// are passed through unchanged. Only official publication hosts are allowed,
// mirroring src/primary_actions.py OFFICIAL_SUFFIXES / OFFICIAL_DOMAINS.
export default {
  async fetch(request) {
    const incoming = new URL(request.url);
    const target = incoming.searchParams.get('url');
    if (!target) return new Response('usage: ?url=<official-source-url>', { status: 400 });
    let t;
    try { t = new URL(target); } catch { return new Response('invalid target url', { status: 400 }); }
    if (t.protocol !== 'https:' && t.protocol !== 'http:') return new Response('https/http only', { status: 400 });
    if (t.username || t.password) return new Response('credentials in target url forbidden', { status: 400 });
    if (['localhost', '127.0.0.1', '::1', '169.254.169.254'].includes(t.hostname)) return new Response('local target forbidden', { status: 400 });
    if (!allowed(t.hostname)) return new Response('target host is not an approved official source', { status: 403 });

    const headers = {
      // SEC fair access wants a contact email in the agent string. Other official
      // hosts accept the same declared identity.
      'User-Agent': 'Fullscan filing parser research@sroyaltyy.github.io',
      'Accept': 'application/json, application/atom+xml, application/rss+xml, text/html;q=0.8, */*;q=0.5',
    };
    for (const h of ['if-none-match', 'if-modified-since']) {
      if (request.headers.has(h)) headers[h] = request.headers.get(h);
    }
    const upstream = await fetch(t.toString(), { headers, redirect: 'follow' });
    const hopByHop = ['connection', 'transfer-encoding', 'keep-alive', 'upgrade', 'te', 'trailer', 'set-cookie'];
    const outHeaders = {};
    upstream.headers.forEach((v, k) => { if (!hopByHop.includes(k.toLowerCase())) outHeaders[k] = v; });
    const body = [101, 204, 205, 304].includes(upstream.status) ? null : upstream.body;
    return new Response(body, { status: upstream.status, headers: outHeaders });
  },
};

const SUFFIXES = ['.gov', '.gov.uk', '.gc.ca', '.gov.au', '.govt.nz', '.gov.hk', '.go.jp', '.go.kr', '.gov.in', '.gov.cn', '.europa.eu', '.gov.sg', '.gov.my', '.go.th', '.gov.ph', '.go.id', '.gov.br', '.gov.za', '.gouv.fr', '.gov.tw', '.parliament.uk'];
const DOMAINS = ['bankofengland.co.uk', 'bankofcanada.ca', 'rbi.org.in', 'snb.ch', 'riksbank.se', 'norges-bank.no', 'bundesbank.de', 'cnmc.es', 'banxico.org.mx', 'resbank.co.za', 'bis.org', 'wto.org', 'imf.org', 'worldbank.org', 'un.org', 'thegazette.co.uk', 'bot.or.th', 'bancaditalia.it', 'bnm.gov.my', 'newyorkfed.org', 'boj.or.jp', 'fca.org.uk', 'scc-csc.ca', 'sfc.hk', 'judiciary.hk', 'bundeskartellamt.de', 'autoritedelaconcurrence.fr'];

function allowed(host) {
  host = host.toLowerCase();
  return SUFFIXES.some((s) => host.endsWith(s)) || DOMAINS.some((d) => host === d || host.endsWith('.' + d));
}
