# MG dashboard data generator

GitHub Actions cron (every 30 min) that runs `mg_dash4.compute_dash4()` and publishes `data4.json` to the public Pages repo `kushagraagarwal-11/mg-dashboard` via a deploy key. Replaces the deleted Railway wall as the dashboard data backend ($0, static JSON).

Secrets (Actions): MB_KEY, SB_TOKEN, CT_ACC, CT_PASS, DEPLOY_KEY.
Run `finish_setup.sh` once to create the repo, deploy key, secrets, and first run.
