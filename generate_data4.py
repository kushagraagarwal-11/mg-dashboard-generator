# -*- coding: utf-8 -*-
"""Standalone generator for the MG dashboard's /data4 payload.
Runs mg_dash4.compute_dash4() and writes data4.json — published to the public
Pages repo (kushagraagarwal-11/mg-dashboard) by the GitHub Action.
Env required: MB_KEY, SB_TOKEN, CT_ACC, CT_PASS.
Replaces the deleted Railway wall as the data backend (static JSON, $0, unlimited
public-repo Actions minutes)."""
import json, mg_dash4

d = mg_dash4.compute_dash4()
if not d.get("tab1"):
    raise SystemExit("compute_dash4 returned no tab1 — aborting so we don't publish an empty file")
with open("data4.json", "w", encoding="utf-8") as f:
    json.dump(d, f, separators=(",", ":"))
import os
print("data4.json written: %d bytes | generated %s | enrolled %s | csp:%s noGH:%s csp_noGH:%s tab4_noGH:%s"
      % (os.path.getsize("data4.json"), d.get("generated"), d.get("enrolled"),
         bool(d.get("csp")), bool(d.get("noGH")), bool(d.get("csp_noGH")), bool(d.get("tab4_noGH"))))
