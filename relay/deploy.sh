# Deploys the Fullscan fetch relay to Cloudflare Workers.
# Needs CLOUDFLARE_API_TOKEN (Create-Your-Account > API token with Workers Scripts Edit permission).
# Usage:  CLOUDFLARE_API_TOKEN=xxx bash relay/deploy.sh
set -euo pipefail
cd "$(dirname "$0")"
if ! command -v npx >/dev/null 2>&1; then echo "npx (Node.js) is required"; exit 1; fi
export CLOUDFLARE_API_TOKEN="${CLOUDFLARE_API_TOKEN:?set CLOUDFLARE_API_TOKEN first}"
npx --yes wrangler@3 deploy worker.js --name fullscan-primary-relay --compatibility-date 2026-10-16
echo
echo "Deployed. Set the repo secret PRIMARY_ACTION_RELAY_URL to:"
echo "  https://fullscan-primary-relay.<your-cloudflare-subdomain>.workers.dev"
echo "then run: python relay/selftest.py <that-url>"
