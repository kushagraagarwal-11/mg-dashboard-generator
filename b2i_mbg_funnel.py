# -*- coding: utf-8 -*-
"""MBG B2I funnel, ENROLLED vs NON-ENROLLED CSPs, PRE vs POST, Daily Avg.
CANONICAL logic = Metabase card 11528 (fct_booking_window booking base -> connection -> TAS task).
WINDOWS (Ajinkya-style, no maturity cap):
  * POST = 1 Jul -> yesterday (N complete days).
  * PRE  = the SAME N days immediately before launch, ending 30 Jun (rolls back 1 day as POST grows).
  * Funnel read at CURRENT position (cur_depth) -- no per-booking maturity cap. PRE (older) is naturally
    more matured than recent POST bookings; that recency gap is real and shown (matches Ajinkya).
Stages: received=cur_depth>=2 . slot(CSP accepted/proposed)=3 . confirm(customer)=4 . install=6.
TATs: task->accept = median MIN (tcreated->ALLOCATION_ACCEPTED); booking->install = median hrs.
Cohort: task's CSP partner -> enrolled='E', any other real CSP='N'. installs_daily = NSM (installs by day)."""
import os, json, urllib.request, datetime
HERE = os.path.dirname(os.path.abspath(__file__))
TOKEN = os.environ.get("SUPABASE_TOKEN") or os.environ.get("SB_TOKEN")
if not TOKEN:
    TOKEN = open(os.path.join(HERE, "supabase_token.txt")).read().strip()
MB_KEY = os.environ["MB_KEY"]
AUDIT = "gonqnxpdtvjydppbrnie"; MG = "108a08d1-749a-4236-a0e9-fd4f1d3c6a27"
POST_START = "2026-07-01"; PRE_END = "2026-06-30"      # PRE ends the day before launch; start is rolling
LAUNCH = datetime.datetime(2026, 7, 1, 9, 0)
CONN_DAYS = 14

import time as _time
def _open_retry(req, timeout, tries=4):
    """urlopen with retry on transient failures (5xx / 429 / timeouts / conn errors)."""
    last=None
    for i in range(tries):
        try:
            return urllib.request.urlopen(req, timeout=timeout).read().decode()
        except Exception as e:
            last=e; code=getattr(e,"code",None)
            if code is not None and code<500 and code!=429:
                raise                                 # real 4xx -> don't retry
            if i<tries-1: _time.sleep(3*(i+1))
    raise last

def sb(q, ref):
    r = urllib.request.Request(f"https://api.supabase.com/v1/projects/{ref}/database/query",
        data=json.dumps({"query": q}).encode(),
        headers={"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json", "User-Agent": "curl/8.4.0"})
    return json.loads(_open_retry(r, 60))

def mb(q):
    r = urllib.request.Request("https://metabase.wiom.in/api/dataset",
        data=json.dumps({"database": 113, "type": "native", "native": {"query": q}}).encode(),
        headers={"x-api-key": MB_KEY, "Content-Type": "application/json"})
    j = json.loads(_open_retry(r, 200))
    return j["data"]["rows"] if j.get("data") else j

def enrolled_partners():
    fc = json.load(open(os.path.join(HERE, "frozen_cohort.json"), encoding="utf-8"))
    f1, f2 = set(fc["flow1"]), set(fc["flow2"])
    opt = set(r["pid"] for r in sb("select partner_id::text pid from mg_optins where program='MG' and first_opted_at is not null", AUDIT))
    f2p = "','".join(f2)
    done2 = set(r["pid"] for r in sb(f"select distinct partner_id::text pid from campaign_partners where campaign_id='{MG}' and partner_id in ('{f2p}') and scan_complete_at is not null", AUDIT))
    return (opt & f1) | (opt & f2 & done2)

def _base_ctes(pl, d_from):
    return f"""
WITH bookings AS (
  SELECT MOBILE mobile, TO_DATE(BOOKING_CONFIRM_DATE) booking_date, BOOKING_CONFIRM_TIME bt, NEXT_BOOKING_CONFIRM_TIME nb
  FROM PROD_DB.DBT.fct_booking_window WHERE BOOKING_CONFIRM_DATE >= '{d_from}'),
acc AS (SELECT b.*, dr.ACCOUNT_ID::STRING account_id, dr.LCO_ACCOUNT_ID lco
  FROM bookings b LEFT JOIN PROD_DB.DYNAMODB_read.BOOKING dr ON dr.MOBILE=b.mobile AND dr._FIVETRAN_DELETED=FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.mobile,b.booking_date ORDER BY dr.ADDED_TIME DESC NULLS LAST)=1),
acc_clean AS (SELECT * FROM acc WHERE lco IS NULL OR lco NOT IN (SELECT LCO_ACCOUNT_ID FROM PROD_DB.PUBLIC.TEST_LCO_ACCOUNT_ID WHERE LCO_ACCOUNT_ID IS NOT NULL)),
ma AS (SELECT DISTINCT MOBILE mobile, ACCOUNT_ID::STRING account_id FROM PROD_DB.DYNAMODB.BOOKING WHERE ACCOUNT_ID IS NOT NULL AND MOBILE>'5999999999'),
conn AS (SELECT a.mobile, a.booking_date, c.CONNECTION_ID FROM acc_clean a
  JOIN ma ON ma.mobile=a.mobile
  JOIN PROD_DB.CSP_CONNECTION_LIFECYCLE_SERVICE_CSP_CONNECTION_LIFECYCLE_SERVICE.CONNECTION_EVENT_HISTORY ceh
    ON ceh.EVENT_TYPE='CONNECTION_REQUEST' AND ceh._FIVETRAN_DELETED=FALSE
   AND ceh.EVENT_TIMESTAMP BETWEEN DATEADD(hour,-2,DATEADD(minute,-330,a.bt)) AND DATEADD(hour,24*{CONN_DAYS},DATEADD(minute,-330,a.bt))
   AND (a.nb IS NULL OR DATEADD(minute,330,ceh.EVENT_TIMESTAMP)<a.nb)
  JOIN PROD_DB.CSP_CONNECTION_LIFECYCLE_SERVICE_CSP_CONNECTION_LIFECYCLE_SERVICE.CONNECTIONS c
    ON c.CONNECTION_ID=ceh.CONNECTION_ID AND c.CUSTOMER_ID::STRING=ma.account_id AND c._fivetran_active=TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.mobile,a.booking_date ORDER BY ceh.EVENT_TIMESTAMP)=1),
tl AS (SELECT CONNECTION_ID, CURRENT_STATE cs, CSP_ID csp_id, PROPOSED_SLOT_DATE psd, CONFIRMED_SLOT_AT csa, INSTALLATION_COMPLETED_AT ica, EXECUTOR_ID exid,
    MAX(IFF(OTP_VERIFIED=TRUE OR INSTALLATION_COMPLETED_AT IS NOT NULL OR COMPLETED_STEP>=7,1,0)) OVER (PARTITION BY CONNECTION_ID) inst_any
  FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE ETL_CURRENT=TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONNECTION_ID ORDER BY UPDATED_AT DESC)=1),
tcr AS (SELECT CONNECTION_ID, MIN(CREATED_AT) tcreated FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE ETL_CURRENT=TRUE GROUP BY 1),
accev AS (SELECT CONNECTION_ID, MIN(EVENT_TIMESTAMP) accepted_at FROM PROD_DB.CSP_CONNECTION_LIFECYCLE_SERVICE_CSP_CONNECTION_LIFECYCLE_SERVICE.CONNECTION_EVENT_HISTORY WHERE EVENT_TYPE='ALLOCATION_ACCEPTED' AND _FIVETRAN_DELETED=FALSE GROUP BY 1),
asgn AS (SELECT CONNECTION_ID, MIN(IFF(CURRENT_STATE='TECHNICIAN_ASSIGNED', UPDATED_AT, NULL)) t_assign
  FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE CREATED_AT >= DATEADD(day,-3,'{d_from}') GROUP BY 1),
csp AS (SELECT CSP_ID, PARTNER_ID FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1),
base AS (SELECT a.booking_date,
    CASE WHEN csp.PARTNER_ID IN ('{pl}') THEN 'E' WHEN csp.PARTNER_ID IS NOT NULL THEN 'N' ELSE NULL END coh,
    CASE WHEN tl.inst_any=1 THEN 6
         WHEN tl.exid IS NOT NULL OR tl.cs IN ('TECHNICIAN_ASSIGNED','ARRIVED_AT_SITE','INSTALLATION_IN_PROGRESS_POST_FEE','AWAITING_CUSTOMER_OTP','FEE_COLLECTION_PENDING') THEN 5
         WHEN tl.csa IS NOT NULL OR tl.cs='AWAITING_TECHNICIAN_ASSIGNMENT' THEN 4
         WHEN tl.psd IS NOT NULL OR tl.cs='AWAITING_CUSTOMER_SLOT_CONFIRMATION' THEN 3
         WHEN tl.CONNECTION_ID IS NOT NULL THEN 2 ELSE 1 END cur_depth,
    IFF(accev.accepted_at IS NOT NULL AND DATEDIFF(minute,DATEADD(minute,-330,a.bt),accev.accepted_at)>=0, DATEDIFF(minute,DATEADD(minute,-330,a.bt),accev.accepted_at), NULL) tat_slot_m,
    IFF(tl.csa IS NOT NULL AND DATEDIFF(minute,DATEADD(minute,-330,a.bt),tl.csa)>=0, DATEDIFF(minute,DATEADD(minute,-330,a.bt),tl.csa), NULL) tat_conf_m,
    IFF(tl.inst_any=1 AND tl.ica IS NOT NULL AND DATEDIFF(minute,DATEADD(minute,-330,a.bt),tl.ica)>=0, DATEDIFF(minute,DATEADD(minute,-330,a.bt),tl.ica), NULL) tat_inst_m,
    DATEDIFF(minute, tcr.tcreated, accev.accepted_at) tat_ta,
    IFF(accev.accepted_at IS NOT NULL AND tl.csa IS NOT NULL AND DATEDIFF(minute,accev.accepted_at,tl.csa)>=0, DATEDIFF(minute,accev.accepted_at,tl.csa), NULL) tat_sc_m,
    IFF(tl.csa IS NOT NULL AND asgn.t_assign IS NOT NULL AND DATEDIFF(minute,tl.csa,asgn.t_assign)>=0, DATEDIFF(minute,tl.csa,asgn.t_assign), NULL) tat_ca_m,
    IFF(asgn.t_assign IS NOT NULL AND tl.inst_any=1 AND tl.ica IS NOT NULL AND DATEDIFF(minute,asgn.t_assign,tl.ica)>=0, DATEDIFF(minute,asgn.t_assign,tl.ica), NULL) tat_ai_m,
    IFF(tl.cs='DECLINED',1,0) declined
  FROM acc_clean a LEFT JOIN conn cn ON cn.mobile=a.mobile AND cn.booking_date=a.booking_date
  LEFT JOIN tl ON tl.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN tcr ON tcr.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN accev ON accev.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN asgn ON asgn.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN csp ON csp.CSP_ID=tl.csp_id)"""

def funnel_sql(pl, pre_start, pre_end, post_end):
    return _base_ctes(pl, pre_start) + f"""
SELECT CASE WHEN booking_date BETWEEN '{pre_start}' AND '{pre_end}' THEN 'PRE'
            WHEN booking_date BETWEEN '{POST_START}' AND '{post_end}' THEN 'POST' END period, coh,
  SUM(IFF(cur_depth>=2,1,0)) received, SUM(IFF(cur_depth>=3,1,0)) slot,
  SUM(IFF(cur_depth>=4,1,0)) confirm, SUM(IFF(cur_depth>=6,1,0)) install,
  ROUND(MEDIAN(IFF(cur_depth>=3 AND tat_ta>=0, tat_ta, NULL))) tat_accept,
  ROUND(MEDIAN(tat_slot_m)) tat_slot, ROUND(MEDIAN(tat_conf_m)) tat_confirm,
  ROUND(MEDIAN(tat_inst_m)) tat_install,
  SUM(IFF(cur_depth>=5,1,0)) assign, SUM(declined) decline,
  ROUND(MEDIAN(tat_sc_m)) tat_sc, ROUND(MEDIAN(tat_ca_m)) tat_ca, ROUND(MEDIAN(tat_ai_m)) tat_ai
FROM base WHERE coh IS NOT NULL GROUP BY 1,2 ORDER BY 1 DESC, 2"""

def decline_sql(pl, pre_start, pre_end, post_end):
    # CSP decline rate = of leads OFFERED to a CSP (candidates), how many the CSP DECLINED. Per-offer grain
    # (a decline gets re-allocated, so it never survives as the "latest" candidate -- must count all candidates).
    return f"""WITH csp AS (SELECT CSP_ID, PARTNER_ID FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1)
SELECT CASE WHEN TO_DATE(tc.CREATED_AT) BETWEEN '{pre_start}' AND '{pre_end}' THEN 'PRE'
            WHEN TO_DATE(tc.CREATED_AT) BETWEEN '{POST_START}' AND '{post_end}' THEN 'POST' END period,
  IFF(csp.PARTNER_ID IN ('{pl}'),'E','N') coh, COUNT(*) offers, SUM(IFF(tc.CURRENT_STATE='DECLINED',1,0)) declined
FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES tc JOIN csp ON csp.CSP_ID=tc.CSP_ID
WHERE tc.ETL_CURRENT=TRUE AND TO_DATE(tc.CREATED_AT) BETWEEN '{pre_start}' AND '{post_end}' GROUP BY 1,2"""

def installs_daily_sql(pl, d_from):
    return f"""
WITH ins AS (SELECT CONNECTION_ID, CSP_ID, INSTALLATION_COMPLETED_AT ica
  FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE ETL_CURRENT=TRUE AND INSTALLATION_COMPLETED_AT IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONNECTION_ID ORDER BY INSTALLATION_COMPLETED_AT)=1),
csp AS (SELECT CSP_ID, PARTNER_ID FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1)
SELECT TO_DATE(DATEADD(minute,330,ins.ica)) d,
  SUM(IFF(csp.PARTNER_ID IN ('{pl}'),1,0)) enr, COUNT(*) allc
FROM ins JOIN csp ON csp.CSP_ID=ins.CSP_ID
WHERE TO_DATE(DATEADD(minute,330,ins.ica)) >= '{d_from}' GROUP BY 1 ORDER BY 1"""

def hourly_installs_sql(pl, d_from):
    # installs by completion HOUR x DAY (IST) since d_from -- for the heatmap (enrolled + all CSPs)
    return f"""
WITH ins AS (SELECT CONNECTION_ID, CSP_ID, INSTALLATION_COMPLETED_AT ica
  FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE ETL_CURRENT=TRUE AND INSTALLATION_COMPLETED_AT IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONNECTION_ID ORDER BY INSTALLATION_COMPLETED_AT)=1),
csp AS (SELECT CSP_ID, PARTNER_ID FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1)
SELECT TO_DATE(DATEADD(minute,330,ins.ica)) d, DATE_PART(hour,DATEADD(minute,330,ins.ica))::int hr,
  SUM(IFF(csp.PARTNER_ID IN ('{pl}'),1,0)) enr, COUNT(*) allc
FROM ins JOIN csp ON csp.CSP_ID=ins.CSP_ID
WHERE TO_DATE(DATEADD(minute,330,ins.ica)) >= '{d_from}' GROUP BY 1,2 ORDER BY 1,2"""

def daily_funnel_sql(pl, d_from):
    # full funnel + stage TATs per booking-day, BOTH cohorts -- comparative trend charts + the cohort table
    return _base_ctes(pl, d_from) + f"""
SELECT booking_date, coh, SUM(IFF(cur_depth>=2,1,0)) received, SUM(IFF(cur_depth>=3,1,0)) slot,
  SUM(IFF(cur_depth>=4,1,0)) confirm, SUM(IFF(cur_depth>=5,1,0)) assign, SUM(IFF(cur_depth>=6,1,0)) install,
  ROUND(MEDIAN(IFF(cur_depth>=3 AND tat_ta>=0,tat_ta,NULL))) tat_task_slot, ROUND(MEDIAN(tat_sc_m)) tat_slot_confirm,
  ROUND(MEDIAN(tat_ca_m)) tat_confirm_assign, ROUND(MEDIAN(tat_ai_m)) tat_assign_install
FROM base WHERE coh IS NOT NULL AND booking_date >= '{d_from}' GROUP BY 1,2 ORDER BY 1,2"""

def compute_funnel(pl=None):
    if pl is None:
        pl = "','".join(enrolled_partners())
    num_csps = (pl.count("','") + 1) if pl else 0
    now = datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)
    post_end = now.date() - datetime.timedelta(days=1)          # complete days only
    if post_end < datetime.date.fromisoformat(POST_START):
        post_end = datetime.date.fromisoformat(POST_START)
    N = (post_end - datetime.date.fromisoformat(POST_START)).days + 1   # complete POST days
    pre_end = datetime.date.fromisoformat(PRE_END)
    pre_start = pre_end - datetime.timedelta(days=N - 1)        # PRE = same N days, ending 30 Jun (rolls back)
    rows = mb(funnel_sql(pl, pre_start.isoformat(), pre_end.isoformat(), post_end.isoformat()))
    if isinstance(rows, dict):
        raise RuntimeError("funnel query error: " + json.dumps(rows)[:300])
    days = {"PRE": float(N), "POST": float(N)}
    out = {"n_days": N, "days": days, "post_end": post_end.isoformat(),
           "pre_window": [pre_start.isoformat(), pre_end.isoformat()], "post_window": [POST_START, post_end.isoformat()],
           "PRE": {"enr": {}, "non": {}}, "POST": {"enr": {}, "non": {}}}
    for r in rows:
        p, coh = r[0], r[1]
        if not p or coh not in ("E", "N"):
            continue
        recv, slot, confirm, install, tat, tslot, tconf, tati, asg, dec, tsc, tca, tai = r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12], r[13], r[14]
        dv = days[p]; rc = recv or 1; sc = slot or 1; cc = confirm or 1
        d = {"received": recv, "received_d": round(recv/dv), "slot": slot, "slot_d": round(slot/dv),
             "confirm": confirm, "confirm_d": round(confirm/dv), "install": install, "install_d": round(install/dv),
             "assign": asg, "assign_d": round(asg/dv),
             # funnel conversion, all as % OF BOOKINGS RECEIVED (clean top-down funnel)
             "slot_pct": round(100*slot/rc, 1), "confirm_pct": round(100*confirm/rc, 1),
             "assign_pct": round(100*asg/rc, 1), "install_pct": round(100*install/rc, 1),
             # bookings per CSP per day (enrolled only)
             "per_csp_d": round(recv/dv/num_csps, 2) if num_csps else None,
             # stage-to-stage TATs (minutes): task->slot proposed->customer confirmed->tech assigned->installed
             "tat_task_slot": tat, "tat_slot_confirm": tsc, "tat_confirm_assign": tca, "tat_assign_install": tai,
             # legacy keys kept for the TV-wall Page 2 renderer
             "install_ratio": round(100*install/cc, 1), "tat_accept": tat, "tat_install": tati,
             "tat_slot": tslot, "tat_confirm": tconf, "sent_d": round(recv/dv)}
        out[p]["enr" if coh == "E" else "non"] = d
    # real CSP decline rate (per-offer, from the full candidate set) -> overrides the ~0 latest-candidate flag
    drows = mb(decline_sql(pl, pre_start.isoformat(), pre_end.isoformat(), post_end.isoformat()))
    if not isinstance(drows, dict):
        for r in drows:
            dp, dcoh, doff, ddec = r[0], r[1], r[2], r[3]
            if not dp or dcoh not in ("E", "N"):
                continue
            tgt = out[dp]["enr" if dcoh == "E" else "non"]
            if tgt:
                tgt["decline_offers"] = doff
                tgt["decline_count"] = ddec
                tgt["decline_pct"] = round(100 * ddec / (doff or 1), 1)
    for p in ("PRE", "POST"):
        out[p].update({k: v for k, v in out[p].get("enr", {}).items()})
    irows = mb(installs_daily_sql(pl, POST_START))
    idaily = []
    if not isinstance(irows, dict):
        for r in irows:
            dt = str(r[0])[:10]
            if dt <= post_end.isoformat():
                idaily.append({"d": dt[5:], "e": r[1], "a": r[2]})
    out["installs_daily"] = idaily
    # hourly installs heatmap (hour x day) for the current month
    month_start = now.date().replace(day=1).isoformat()
    hrows = mb(hourly_installs_sql(pl, month_start))
    hourly = []
    if not isinstance(hrows, dict):
        for r in hrows:
            hourly.append({"d": str(r[0])[:10], "hr": r[1], "e": r[2], "a": r[3]})
    out["hourly_installs"] = hourly
    out["month_start"] = month_start
    # daily funnel (per booking-date, last ~16 days) -> trend chart (POST) + D-1/2/3/7/14 cohort table
    df_from = (now.date() - datetime.timedelta(days=16)).isoformat()
    dfrows = mb(daily_funnel_sql(pl, df_from)); dfmap = {}; bdaily = []; ddaily = []; ddaily_n = []
    if not isinstance(dfrows, dict):
        for r in dfrows:
            dt = str(r[0])[:10]; coh = r[1]
            rec = {"d": dt[5:], "recv": r[2], "slot": r[3], "confirm": r[4], "assign": r[5], "install": r[6],
                   "t_ts": r[7], "t_sc": r[8], "t_ca": r[9], "t_ai": r[10]}
            if coh == "E":
                dfmap[dt] = {"recv": r[2], "slot": r[3], "confirm": r[4], "assign": r[5], "install": r[6]}
                if POST_START <= dt <= post_end.isoformat():
                    bdaily.append({"d": dt[5:], "r": r[2]}); ddaily.append(rec)
            elif coh == "N" and POST_START <= dt <= post_end.isoformat():
                ddaily_n.append(rec)
    out["bookings_daily"] = bdaily
    out["daily"] = ddaily
    out["daily_non"] = ddaily_n
    cohorts = []
    for lag in (1, 2, 3, 7, 14):
        dt = (now.date() - datetime.timedelta(days=lag)).isoformat()
        m = dfmap.get(dt)
        if not m:
            continue
        rc2 = m["recv"] or 1
        cohorts.append({"lag": lag, "date": dt[5:], "recv": m["recv"], "slot": m["slot"], "confirm": m["confirm"],
            "assign": m["assign"], "install": m["install"], "slot_pct": round(100*m["slot"]/rc2, 1),
            "confirm_pct": round(100*m["confirm"]/rc2, 1), "assign_pct": round(100*m["assign"]/rc2, 1),
            "install_pct": round(100*m["install"]/rc2, 1), "per_csp": round(m["recv"]/num_csps, 2) if num_csps else None})
    out["cohorts"] = cohorts
    out["num_csps"] = num_csps
    return out

if __name__ == "__main__":
    d = compute_funnel()
    print(f"N={d['n_days']}d | PRE {d['pre_window']} vs POST {d['post_window']}")
    for p in ("PRE", "POST"):
        for coh in ("enr", "non"):
            x = d[p].get(coh, {})
            if x:
                print(f"{p} {coh}: recv/d={x['received_d']} slot%={x['slot_pct']} confirm%={x['confirm_pct']} inst/d={x['install_d']} inst%(conf)={x['install_ratio']} accept={x['tat_accept']}min instTAT={x['tat_install']}h")
    print("installs_daily:", d["installs_daily"])
