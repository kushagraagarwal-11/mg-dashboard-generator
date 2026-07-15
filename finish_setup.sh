#!/usr/bin/env bash
# One-time setup for the MG dashboard's $0 data backend (mirrors the L1-L5 generator).
# Creates a PUBLIC generator repo (unlimited Actions minutes), a deploy key with write
# access to the public Pages repo, sets Actions secrets, and fires the first refresh.
# Run once:  bash "C:\Users\Palak Vardhan\Desktop\mbg\mg-dashboard-generator\finish_setup.sh"
set -euo pipefail
GEN="$(cd "$(dirname "$0")" && pwd)"
OWNER="kushagraagarwal-11"
WALL="$GEN/../mbg-tv-wall"

# secrets pulled from the wall working copy (no new plaintext copies land in this repo)
MB_KEY=$(grep -o 'mb_[A-Za-z0-9+/=]*' "$WALL/b2i_mbg_funnel.py" | head -1)
SB_TOKEN=$(grep -o 'sbp_[a-f0-9]*' "$WALL/tv_dashboard.py" | head -1)
CT_ACC=$(grep -oE '"[0-9A-Z]{3}-[0-9A-Z]{3}-[0-9A-Z]{4}"' "$WALL/mg_dash4.py" | head -1 | tr -d '"')
CT_PASS=$(grep -oE '"AAK-[A-Z-]+"' "$WALL/mg_dash4.py" | head -1 | tr -d '"')
echo "secrets resolved: MB_KEY=${MB_KEY:0:6}… SB_TOKEN=${SB_TOKEN:0:6}… CT_ACC=$CT_ACC CT_PASS=$CT_PASS"

# 1. public generator repo (public => unlimited GitHub Actions minutes)
cd "$GEN"
git init -q -b main 2>/dev/null || true
git add -A
git -c user.name="mg-dashboard-bot" -c user.email="actions@github.com" commit -q -m "MG data4 generator" || true
gh repo create "$OWNER/mg-dashboard-generator" --public --source . --push

# 2. deploy key: private half -> generator secret, public half -> Pages repo (write access)
KEY="$HOME/.ssh/mg_data4_deploy"
[ -f "$KEY" ] || ssh-keygen -t ed25519 -N "" -C "mg-data4-deploy" -f "$KEY" -q
gh api "repos/$OWNER/mg-dashboard/keys" -f title="mg-data4 deploy" \
  -f key="$(cat "$KEY.pub")" -F read_only=false >/dev/null

# 3. Actions secrets on the generator repo
gh secret set DEPLOY_KEY --repo "$OWNER/mg-dashboard-generator" < "$KEY"
printf '%s' "$MB_KEY"   | gh secret set MB_KEY   --repo "$OWNER/mg-dashboard-generator"
printf '%s' "$SB_TOKEN" | gh secret set SB_TOKEN --repo "$OWNER/mg-dashboard-generator"
printf '%s' "$CT_ACC"   | gh secret set CT_ACC   --repo "$OWNER/mg-dashboard-generator"
printf '%s' "$CT_PASS"  | gh secret set CT_PASS  --repo "$OWNER/mg-dashboard-generator"

# 4. fire the first refresh now
gh workflow run refresh.yml --repo "$OWNER/mg-dashboard-generator"

echo ""
echo "DONE. The cron now refreshes data4.json every 30 min."
echo "Watch the first run:  gh run list --repo $OWNER/mg-dashboard-generator"
echo "Dashboard:            https://$OWNER.github.io/mg-dashboard/"
