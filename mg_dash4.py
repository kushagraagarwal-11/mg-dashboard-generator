# -*- coding: utf-8 -*-
"""/data4 for the MG dashboard rebuild — one funnel, four cuts.
Windows are PER-CSP ANCHORED: for each enrolled CSP, POST = its own enrolment date
(mg_optins.first_opted_at, IST date) -> yesterday; PRE = the same number of days
immediately before enrolment. Non-enrolled CSPs anchor at launch (1 Jul).
Funnel base = the canonical card-11528 logic (same as b2i_mbg_funnel): booking ->
connection -> latest TAS candidate; stages received/slot/confirm/assign/install,
stage TATs task->slot, slot->confirm, confirm->assign, assign->install.
Cuts: tab1 enrolled vs non-enrolled . tab2 flow-3 feedback category .
tab3 banner screen + engagement depth (Viewed/Scrolled/TappedGuarantee/Ticket, from
CleverTap profile events MBG_View_/MBG_Scroll_/MBG_TapGuarantee_/MBG_Ticket_) .
tab4 DoD + hour-x-day heatmaps for bookings and installs.
One Snowflake pass via GROUPING SETS; banner profiles fetched threaded (~90s).
"""
import os, json, datetime, urllib.request, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from b2i_mbg_funnel import sb, mb, enrolled_partners, AUDIT, MG

HERE = os.path.dirname(os.path.abspath(__file__))
CT_ACC, CT_PASS, REGION = os.environ["CT_ACC"], os.environ["CT_PASS"], "eu1"
PORTAL = "oobaxfbsmqhdaligebmg"
LAUNCH_D = "2026-07-01"
CONN_DAYS = 14
TEST_IDS = {10216, 39}
ANS_CODES = {"excited", "dontknow", "dontcare", "questions"}
CAT_LABELS = {"excited": "Excited", "questions": "Has Questions", "dontcare": "Indifferent",
              "dontknow": "Unaware", "not_answered": "Not Answered"}
SCREENS = ["keepgoing", "almost", "secured", "noleads"]

def _ist_today():
    return (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).date()

# ---------- cohort maps ----------
def anchors():
    """enrolled partner -> IST enrolment date (capped at launch)."""
    enr = enrolled_partners()
    rows = sb("select partner_id::text pid, to_char((first_opted_at + interval '330 minutes')::date,'YYYY-MM-DD') ad "
              "from mg_optins where program='MG' and first_opted_at is not null", AUDIT)
    ad = {r["pid"]: max(r["ad"], LAUNCH_D) for r in rows if r["pid"] in enr}
    for p in enr:
        ad.setdefault(p, LAUNCH_D)
    return ad

def feedback_cats(pl):
    """enrolled partner -> flow-3 category (latest owner/admin answer wins; default not_answered).
    Same logic as flow3_csp_results.py."""
    id2p = {}
    for ident, pid in mb(f"""select u.ID, d.PARTNER_ID::string from DBT_CSP.DIM_CSP d
      join CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_USER u on u.CSP_ID=d.CSP_ID
        and u.ROLE in ('OWNER','MANAGER','MANAGER_PLUS') and u.STATUS='ACTIVE'
      where d.ETL_CURRENT=True and d.PARTNER_ID::string in ('{pl}')"""):
        try: id2p[int(ident)] = pid
        except Exception: pass
    cat = {}
    for r in sb("select pid, screen from mbg_screen_log where flow='3' and pid not like 'RAW%' "
                "and (screen like 'f3a_%' or screen like 'f3b_%') order by ts", PORTAL):
        try: ident = int(r["pid"])
        except Exception: continue
        if ident in TEST_IDS: continue
        p = id2p.get(ident)
        if not p: continue
        code = r["screen"][4:]
        if code in ANS_CODES: cat[p] = code
    return cat, id2p

def banner_engagement(pl):
    """partner -> {screen, viewed, scrolled, tapguar, ticket} from CleverTap profile events,
    owner/admin identities merged per partner (any identity fired => partner counts)."""
    meta = mb(f"""select u.ID ident, d.PARTNER_ID::string pid from PROD_DB.DBT_CSP.DIM_CSP d
      join PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_USER u on u.CSP_ID=d.CSP_ID
        and u.ROLE in ('OWNER','MANAGER','MANAGER_PLUS') and u.STATUS='ACTIVE'
      where d.ETL_CURRENT=True and d.PARTNER_ID::string in ('{pl}')""")
    id2p = {str(r[0]): str(r[1]) for r in meta}
    def fetch(ident):
        try:
            r = json.loads(urllib.request.urlopen(urllib.request.Request(
                f"https://{REGION}.api.clevertap.com/1/profile.json?identity=" + urllib.parse.quote(ident),
                headers={"X-CleverTap-Account-Id": CT_ACC, "X-CleverTap-Passcode": CT_PASS}), timeout=30).read().decode())
            rec = r.get("record", {}); pd = rec.get("profileData", {}); ev = rec.get("events", {})
            def has(pref): return any(k.startswith(pref) and v.get("count", 0) for k, v in ev.items())
            vfs = [v.get("first_seen") for k, v in ev.items() if k.startswith("MBG_View_") and v.get("first_seen")]
            return ident, {"screen": pd.get("mbg_screen_real") or pd.get("mbg_screen", "") or "",
                           "viewed": has("MBG_View_"), "scrolled": has("MBG_Scroll_"),
                           "tapguar": has("MBG_TapGuarantee_"), "ticket": has("MBG_Ticket_"),
                           "view_fs": min(vfs) if vfs else None}
        except Exception:
            return ident, None
    with ThreadPoolExecutor(max_workers=14) as ex:
        data = dict(ex.map(fetch, id2p.keys()))
    out = {}
    for ident, d in data.items():
        if not d: continue
        p = id2p[ident]; cur = out.setdefault(p, {"screen": "", "viewed": 0, "scrolled": 0, "tapguar": 0, "ticket": 0, "view_fs": None})
        if d["screen"] and not cur["screen"]: cur["screen"] = d["screen"]
        for k in ("viewed", "scrolled", "tapguar", "ticket"):
            if d[k]: cur[k] = 1
        if d.get("view_fs") and (cur["view_fs"] is None or int(d["view_fs"]) < int(cur["view_fs"])):
            cur["view_fs"] = int(d["view_fs"])
    return out

# ---------- SQL ----------
def _values_map(ad, cat, eng, f2set, mgmap=None):
    """(pid, anchor, cat, screen, viewed, scrolled, tapped, ticket, flow, mg) VALUES rows for enrolled."""
    rows = []
    for p, a in ad.items():
        e = eng.get(p, {})
        scr = e.get("screen") or ""
        rows.append("('%s',DATE'%s','%s',%s,%s,%s,%s,%s,%s,%s)" % (
            p, a, cat.get(p, "not_answered"),
            ("'%s'" % scr) if scr in SCREENS else "NULL",
            ("%d" % e["viewed"]) if scr in SCREENS else "NULL",   # engagement flags only for targeted
            ("%d" % e["scrolled"]) if scr in SCREENS else "NULL",
            ("%d" % e["tapguar"]) if scr in SCREENS else "NULL",
            ("%d" % e["ticket"]) if scr in SCREENS else "NULL",
            "'f2'" if p in f2set else "'f1'",
            ("'%s'" % mgmap[p]) if (mgmap and p in mgmap) else "NULL"))
    return ",\n".join(rows)

def _base(vals, d_from, yest):
    # canonical card-11528 base (mirrors b2i_mbg_funnel._base_ctes) + per-CSP anchored period
    return f"""
WITH mgmap AS (SELECT * FROM (VALUES {vals}) AS t(pid, ad, cat, screen, viewed, scrolled, tapped, ticket, flow, mg)),
bookings AS (
  SELECT MOBILE mobile, TO_DATE(BOOKING_CONFIRM_DATE) booking_date, BOOKING_CONFIRM_TIME bt, NEXT_BOOKING_CONFIRM_TIME nb
  FROM PROD_DB.DBT.fct_booking_window WHERE BOOKING_CONFIRM_DATE >= '{d_from}'),
acc AS (SELECT b.*, dr.ACCOUNT_ID::STRING account_id, dr.LCO_ACCOUNT_ID lco, dr.GROUP_NAME flow
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
tl AS (SELECT CONNECTION_ID, CURRENT_STATE cs, CSP_ID csp_id, CREATED_AT cur_created, PROPOSED_SLOT_DATE psd, CONFIRMED_SLOT_AT csa, INSTALLATION_COMPLETED_AT ica, EXECUTOR_ID exid,
    MAX(IFF(OTP_VERIFIED=TRUE OR INSTALLATION_COMPLETED_AT IS NOT NULL OR COMPLETED_STEP>=7,1,0)) OVER (PARTITION BY CONNECTION_ID) inst_any
  FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE ETL_CURRENT=TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONNECTION_ID ORDER BY UPDATED_AT DESC)=1),
tcr AS (SELECT CONNECTION_ID, MIN(CREATED_AT) tcreated FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE ETL_CURRENT=TRUE GROUP BY 1),
accev AS (SELECT CONNECTION_ID, MIN(EVENT_TIMESTAMP) accepted_at FROM PROD_DB.CSP_CONNECTION_LIFECYCLE_SERVICE_CSP_CONNECTION_LIFECYCLE_SERVICE.CONNECTION_EVENT_HISTORY WHERE EVENT_TYPE='ALLOCATION_ACCEPTED' AND _FIVETRAN_DELETED=FALSE GROUP BY 1),
asgn AS (SELECT CONNECTION_ID, MIN(IFF(CURRENT_STATE='TECHNICIAN_ASSIGNED', UPDATED_AT, NULL)) t_assign
  FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES WHERE CREATED_AT >= DATEADD(day,-3,'{d_from}') GROUP BY 1),
csp AS (SELECT CSP_ID, PARTNER_ID FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1),
base AS (SELECT a.booking_date, DATE_PART(hour, a.bt)::int bhr, a.flow bflow, g.pid mgpid,
    CASE WHEN g.pid IS NOT NULL THEN 'E' WHEN csp.PARTNER_ID IS NOT NULL THEN 'N' ELSE NULL END coh,
    g.cat cat, g.screen screen, g.viewed viewed, g.scrolled scrolled, g.tapped tapped, g.ticket ticket, g.flow flow, g.mg mg,
    COALESCE(g.ad, DATE'{LAUNCH_D}') ad,
    CASE WHEN a.booking_date BETWEEN COALESCE(g.ad, DATE'{LAUNCH_D}') AND DATE'{yest}' THEN 'POST'
         WHEN a.booking_date BETWEEN DATEADD(day, -(DATEDIFF(day, COALESCE(g.ad, DATE'{LAUNCH_D}'), DATE'{yest}')+1), COALESCE(g.ad, DATE'{LAUNCH_D}'))
                                 AND DATEADD(day, -1, COALESCE(g.ad, DATE'{LAUNCH_D}')) THEN 'PRE' END period,
    CASE WHEN tl.inst_any=1 THEN 7
         WHEN tl.cs IN ('ARRIVED_AT_SITE','INSTALLATION_IN_PROGRESS_PRE_FEE','INSTALLATION_IN_PROGRESS_POST_FEE','AWAITING_CUSTOMER_OTP','FEE_COLLECTION_PENDING') THEN 6
         WHEN tl.exid IS NOT NULL OR tl.cs='TECHNICIAN_ASSIGNED' THEN 5
         WHEN tl.csa IS NOT NULL OR tl.cs='AWAITING_TECHNICIAN_ASSIGNMENT' THEN 4
         WHEN tl.psd IS NOT NULL OR tl.cs='AWAITING_CUSTOMER_SLOT_CONFIRMATION' THEN 3
         WHEN tl.CONNECTION_ID IS NOT NULL THEN 2 ELSE 1 END cur_depth,
    IFF(accev.accepted_at IS NOT NULL AND DATEDIFF(minute, DATEADD(minute,-330,a.bt), accev.accepted_at) BETWEEN 0 AND 2880,1,0) acc48,
    IFF(tl.csa IS NOT NULL AND DATEDIFF(minute, DATEADD(minute,-330,a.bt), tl.csa) BETWEEN 0 AND 2880,1,0) conf48,
    IFF(asgn.t_assign IS NOT NULL AND DATEDIFF(minute, DATEADD(minute,-330,a.bt), asgn.t_assign) BETWEEN 0 AND 2880,1,0) asgn48,
    IFF(tl.inst_any=1 AND tl.ica IS NOT NULL AND DATEDIFF(minute, DATEADD(minute,-330,a.bt), tl.ica) BETWEEN 0 AND 2880,1,0) inst48,
    IFF(accev.accepted_at IS NOT NULL, GREATEST(DATEDIFF(minute, tl.cur_created, accev.accepted_at), 0), NULL) tat_ta,
    IFF(accev.accepted_at IS NOT NULL AND tl.csa IS NOT NULL AND DATEDIFF(minute,accev.accepted_at,tl.csa)>=0, DATEDIFF(minute,accev.accepted_at,tl.csa), NULL) tat_sc_m,
    IFF(tl.csa IS NOT NULL AND asgn.t_assign IS NOT NULL AND DATEDIFF(minute,tl.csa,asgn.t_assign)>=0, DATEDIFF(minute,tl.csa,asgn.t_assign), NULL) tat_ca_m,
    IFF(asgn.t_assign IS NOT NULL AND tl.inst_any=1 AND tl.ica IS NOT NULL AND DATEDIFF(minute,asgn.t_assign,tl.ica)>=0, DATEDIFF(minute,asgn.t_assign,tl.ica), NULL) tat_ai_m,
    IFF(tl.inst_any=1, DATEADD(minute,330,tl.ica), NULL) inst_ts
  FROM acc_clean a LEFT JOIN conn cn ON cn.mobile=a.mobile AND cn.booking_date=a.booking_date
  LEFT JOIN tl ON tl.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN tcr ON tcr.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN accev ON accev.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN asgn ON asgn.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN csp ON csp.CSP_ID=tl.csp_id
  LEFT JOIN mgmap g ON g.pid = csp.PARTNER_ID::string)"""

_AGG = """SUM(IFF(cur_depth>=2,1,0)) received,
  SUM(IFF(cur_depth>=3 AND (acc48=1 OR conf48=1 OR asgn48=1 OR inst48=1),1,0)) slot,
  SUM(IFF(cur_depth>=4 AND (conf48=1 OR asgn48=1 OR inst48=1),1,0)) confirm,
  SUM(IFF(cur_depth>=5 AND (asgn48=1 OR inst48=1),1,0)) assign,
  SUM(IFF(cur_depth>=6 AND (asgn48=1 OR inst48=1),1,0)) reached,
  SUM(IFF(cur_depth>=7 AND inst48=1,1,0)) install,
  ROUND(MEDIAN(IFF(cur_depth>=3 AND acc48=1 AND tat_ta>=0, tat_ta, NULL))) tat_ta,
  ROUND(MEDIAN(IFF(conf48=1, tat_sc_m, NULL))) tat_sc,
  ROUND(MEDIAN(IFF(asgn48=1, tat_ca_m, NULL))) tat_ca,
  ROUND(MEDIAN(IFF(inst48=1, tat_ai_m, NULL))) tat_ai"""

def cuts_sql(vals, d_from, yest, gh_where=''):
    return _base(vals, d_from, yest) + f"""
SELECT period, coh, cat, screen, viewed, scrolled, tapped, ticket, flow, mg,
  GROUPING(coh) g_coh, GROUPING(cat) g_cat, GROUPING(screen) g_scr, GROUPING(viewed) g_vw,
  GROUPING(scrolled) g_sc, GROUPING(tapped) g_tp, GROUPING(ticket) g_tk, GROUPING(flow) g_fl, GROUPING(mg) g_mg,
  {_AGG}
FROM base WHERE coh IS NOT NULL AND period IS NOT NULL {gh_where}
GROUP BY GROUPING SETS ((period, coh), (period, cat), (period, screen), (period, viewed),
                        (period, scrolled), (period, tapped), (period, ticket), (period, flow), (period, mg))"""


# ================= CSP-LEVEL grain (one row per booking x CSP the booking reached) =================
# A booking dispatched to N CSPs counts N times, once per CSP. Each (connection, csp) pair carries
# THAT csp's own furthest stage on the booking; 48h maturity measured from when the CSP received it.
_RANK_CSP = """CASE r.current_state
      WHEN 'AWAITING_SLOT_PROPOSAL' THEN 1
      WHEN 'SLOT_SELECTED' THEN 2 WHEN 'AWAITING_CUSTOMER_SLOT_CONFIRMATION' THEN 2
      WHEN 'SLOT_CONFIRMED_BY_CUSTOMER' THEN 3 WHEN 'SLOT_AUTO_CONFIRMED' THEN 3 WHEN 'AWAITING_TECHNICIAN_ASSIGNMENT' THEN 3
      WHEN 'TECHNICIAN_ASSIGNED' THEN 4 WHEN 'ARRIVED_AT_SITE' THEN 5
      WHEN 'INSTALLATION_IN_PROGRESS_PRE_FEE' THEN 6 WHEN 'FEE_COLLECTION_PENDING' THEN 6
      WHEN 'INSTALLATION_IN_PROGRESS_POST_FEE' THEN 7 WHEN 'AWAITING_CUSTOMER_OTP' THEN 7
      WHEN 'CONNECTION_ACTIVE' THEN 8 WHEN 'RATING_PENDING' THEN 8 ELSE 0 END"""

def _base_csp(vals, d_from, yest):
    return f"""
WITH mgmap AS (SELECT * FROM (VALUES {vals}) AS t(pid, ad, cat, screen, viewed, scrolled, tapped, ticket, flow, mg)),
bookings AS (
  SELECT MOBILE mobile, TO_DATE(BOOKING_CONFIRM_DATE) booking_date, BOOKING_CONFIRM_TIME bt, NEXT_BOOKING_CONFIRM_TIME nb
  FROM PROD_DB.DBT.fct_booking_window WHERE BOOKING_CONFIRM_DATE >= '{d_from}'),
acc AS (SELECT b.*, dr.ACCOUNT_ID::STRING account_id, dr.LCO_ACCOUNT_ID lco, dr.GROUP_NAME flow
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
cspcand AS (
  SELECT r.connection_id, r.csp_id,
    MIN(r.created_at) recv,
    MIN(IFF({_RANK_CSP}>=2, r.updated_at, NULL)) slot_ts,
    MIN(IFF({_RANK_CSP}>=3, r.updated_at, NULL)) conf_ts,
    MIN(IFF({_RANK_CSP}>=4, r.updated_at, NULL)) asgn_ts,
    MIN(IFF({_RANK_CSP}>=5, r.updated_at, NULL)) reached_ts,
    MIN(IFF({_RANK_CSP}>=8, r.updated_at, NULL)) inst_ts_raw,
    MAX({_RANK_CSP}) maxrank
  FROM PROD_DB.CSP_TAS_SERVICE_CSP_TAS_SERVICE.INSTALL_EXECUTION_CANDIDATES r
  WHERE r.connection_id IN (SELECT CONNECTION_ID FROM conn) GROUP BY 1,2),
alloc AS (SELECT CONNECTION_ID, ASSIGNED_AT, ACCEPTANCE_TIMESTAMP
  FROM PROD_DB.CSP_DEMAND_ALLOCATION_SERVICE_CSP_DEMAND_ALLOCATION_SERVICE.CONNECTION_ALLOCATIONS
  WHERE _fivetran_active=TRUE AND ACCEPTANCE_TIMESTAMP IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONNECTION_ID ORDER BY ASSIGNED_AT DESC NULLS LAST)=1),
csp AS (SELECT CSP_ID, PARTNER_ID FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1),
base AS (SELECT a.booking_date, DATE_PART(hour, a.bt)::int bhr, a.flow bflow, g.pid mgpid,
    CASE WHEN g.pid IS NOT NULL THEN 'E' WHEN csp.PARTNER_ID IS NOT NULL THEN 'N' ELSE NULL END coh,
    g.cat cat, g.screen screen, g.viewed viewed, g.scrolled scrolled, g.tapped tapped, g.ticket ticket, g.flow flow, g.mg mg,
    COALESCE(g.ad, DATE'{LAUNCH_D}') ad,
    CASE WHEN a.booking_date BETWEEN COALESCE(g.ad, DATE'{LAUNCH_D}') AND DATE'{yest}' THEN 'POST'
         WHEN a.booking_date BETWEEN DATEADD(day, -(DATEDIFF(day, COALESCE(g.ad, DATE'{LAUNCH_D}'), DATE'{yest}')+1), COALESCE(g.ad, DATE'{LAUNCH_D}'))
                                 AND DATEADD(day, -1, COALESCE(g.ad, DATE'{LAUNCH_D}')) THEN 'PRE' END period,
    CASE WHEN cc.maxrank>=8 THEN 7 WHEN cc.maxrank>=5 THEN 6 WHEN cc.maxrank>=4 THEN 5 WHEN cc.maxrank>=3 THEN 4 WHEN cc.maxrank>=2 THEN 3 ELSE 2 END cur_depth,
    IFF(cc.slot_ts IS NOT NULL AND DATEDIFF(minute, cc.recv, cc.slot_ts) BETWEEN 0 AND 2880,1,0) slot48,
    IFF(cc.conf_ts IS NOT NULL AND DATEDIFF(minute, cc.recv, cc.conf_ts) BETWEEN 0 AND 2880,1,0) conf48,
    IFF(cc.asgn_ts IS NOT NULL AND DATEDIFF(minute, cc.recv, cc.asgn_ts) BETWEEN 0 AND 2880,1,0) asgn48,
    IFF(cc.reached_ts IS NOT NULL AND DATEDIFF(minute, cc.recv, cc.reached_ts) BETWEEN 0 AND 2880,1,0) reached48,
    IFF(cc.inst_ts_raw IS NOT NULL AND DATEDIFF(minute, cc.recv, cc.inst_ts_raw) BETWEEN 0 AND 2880,1,0) inst48,
    IFF(cc.maxrank>=2 AND alloc.ACCEPTANCE_TIMESTAMP IS NOT NULL AND alloc.ASSIGNED_AT IS NOT NULL AND DATEDIFF(minute, alloc.ASSIGNED_AT, alloc.ACCEPTANCE_TIMESTAMP) BETWEEN 0 AND 2880, DATEDIFF(minute, alloc.ASSIGNED_AT, alloc.ACCEPTANCE_TIMESTAMP), NULL) tat_ta,
    IFF(cc.slot_ts IS NOT NULL AND cc.conf_ts IS NOT NULL AND DATEDIFF(minute,cc.slot_ts,cc.conf_ts)>=0, DATEDIFF(minute,cc.slot_ts,cc.conf_ts), NULL) tat_sc_m,
    IFF(cc.conf_ts IS NOT NULL AND cc.asgn_ts IS NOT NULL AND DATEDIFF(minute,cc.conf_ts,cc.asgn_ts)>=0, DATEDIFF(minute,cc.conf_ts,cc.asgn_ts), NULL) tat_ca_m,
    IFF(cc.asgn_ts IS NOT NULL AND cc.inst_ts_raw IS NOT NULL AND DATEDIFF(minute,cc.asgn_ts,cc.inst_ts_raw)>=0, DATEDIFF(minute,cc.asgn_ts,cc.inst_ts_raw), NULL) tat_ai_m
  FROM conn cn
  JOIN acc_clean a ON a.mobile=cn.mobile AND a.booking_date=cn.booking_date
  JOIN cspcand cc ON cc.connection_id=cn.CONNECTION_ID
  LEFT JOIN alloc ON alloc.CONNECTION_ID=cn.CONNECTION_ID
  LEFT JOIN csp ON csp.CSP_ID=cc.csp_id
  LEFT JOIN mgmap g ON g.pid = csp.PARTNER_ID::string)"""

_AGG_CSP = """SUM(IFF(cur_depth>=2,1,0)) received,
  SUM(IFF(cur_depth>=3 AND (slot48=1 OR conf48=1 OR asgn48=1 OR inst48=1),1,0)) slot,
  SUM(IFF(cur_depth>=4 AND (conf48=1 OR asgn48=1 OR inst48=1),1,0)) confirm,
  SUM(IFF(cur_depth>=5 AND (asgn48=1 OR inst48=1),1,0)) assign,
  SUM(IFF(cur_depth>=6 AND (reached48=1 OR inst48=1),1,0)) reached,
  SUM(IFF(cur_depth>=7 AND inst48=1,1,0)) install,
  ROUND(MEDIAN(IFF(slot48=1 AND tat_ta>=0, tat_ta, NULL))) tat_ta,
  ROUND(MEDIAN(IFF(conf48=1, tat_sc_m, NULL))) tat_sc,
  ROUND(MEDIAN(IFF(asgn48=1, tat_ca_m, NULL))) tat_ca,
  ROUND(MEDIAN(IFF(inst48=1, tat_ai_m, NULL))) tat_ai"""

def cuts_sql_csp(vals, d_from, yest, gh_where=''):
    return _base_csp(vals, d_from, yest) + f"""
SELECT period, coh, cat, screen, viewed, scrolled, tapped, ticket, flow, mg,
  GROUPING(coh) g_coh, GROUPING(cat) g_cat, GROUPING(screen) g_scr, GROUPING(viewed) g_vw,
  GROUPING(scrolled) g_sc, GROUPING(tapped) g_tp, GROUPING(ticket) g_tk, GROUPING(flow) g_fl, GROUPING(mg) g_mg,
  {_AGG_CSP}
FROM base WHERE coh IS NOT NULL AND period IS NOT NULL {gh_where}
GROUP BY GROUPING SETS ((period, coh), (period, cat), (period, screen), (period, viewed),
                        (period, scrolled), (period, tapped), (period, ticket), (period, flow), (period, mg))"""

def tab4_sql(vals, d_from, yest, gh_where=''):
    # DoD + hour-of-day, enrolled only, by booking date/hour + installs by completion date/hour
    return _base(vals, d_from, yest) + f"""
SELECT 'B' k, booking_date d, bhr hr, COUNT(*) n
FROM base WHERE coh='E' AND cur_depth>=2 {gh_where} GROUP BY 2,3
UNION ALL
SELECT 'I', TO_DATE(inst_ts), DATE_PART(hour, inst_ts)::int, COUNT(*)
FROM base WHERE coh='E' AND inst_ts IS NOT NULL AND TO_DATE(inst_ts) >= '{d_from}' {gh_where} GROUP BY 2,3
UNION ALL
SELECT 'J', TO_DATE(inst_ts), DATE_PART(hour, inst_ts)::int, COUNT(*)
FROM base WHERE coh='N' AND inst_ts IS NOT NULL AND TO_DATE(inst_ts) >= '{d_from}' {gh_where} GROUP BY 2,3
UNION ALL
SELECT 'K', booking_date, bhr, COUNT(*)
FROM base WHERE coh='E' AND cur_depth>=7 {gh_where} GROUP BY 2,3"""

def opens_sql(vals, d_from, yest):
    # app opens by role, classified into each CSP's own anchored PRE/POST window (aggregated in SQL)
    return f"""
WITH mgmap AS (SELECT * FROM (VALUES {vals}) AS t(pid, ad, cat, screen, viewed, scrolled, tapped, ticket, flow, mg)),
cm AS (SELECT DISTINCT csp_id, PARTNER_ID::string pid FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT
  WHERE _fivetran_active=TRUE AND PARTNER_ID::string IN (SELECT pid FROM mgmap)),
ev AS (SELECT c.pid, TO_DATE(TRY_TO_TIMESTAMP(e.TIMESTAMP)) dt,
    CASE WHEN UPPER(p.ROLE)='OWNER' THEN 'owner' WHEN UPPER(p.ROLE) IN ('MANAGER','MANAGER_PLUS') THEN 'mgr'
         WHEN UPPER(p.ROLE)='TECHNICIAN' THEN 'tech' END rg
  FROM PROD_DB.CLEVERTAP_CSP_API.EVENTS_DATA e
  JOIN PROD_DB.CLEVERTAP_CSP_API.PROFILE_DATA p ON e.CLEVERTAP_ID=p.CLEVERTAP_ID
  JOIN cm c ON p.CSPID=c.csp_id
  WHERE e.EVENT_NAME='App Launched' AND TRY_TO_TIMESTAMP(e.TIMESTAMP) >= '{d_from}'
    AND (e._FIVETRAN_DELETED=FALSE OR e._FIVETRAN_DELETED IS NULL) AND (p._FIVETRAN_DELETED=FALSE OR p._FIVETRAN_DELETED IS NULL)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY e.CLEVERTAP_ID,e.TIMESTAMP,e.EVENT_NAME ORDER BY e._FIVETRAN_SYNCED)=1)
SELECT CASE WHEN ev.dt BETWEEN g.ad AND DATE'{yest}' THEN 'POST'
            WHEN ev.dt BETWEEN DATEADD(day, -(DATEDIFF(day, g.ad, DATE'{yest}')+1), g.ad) AND DATEADD(day,-1,g.ad) THEN 'PRE' END period,
  ev.rg, COUNT(*) opens, COUNT(DISTINCT ev.pid||'|'||ev.dt) csp_days_active
FROM ev JOIN mgmap g ON g.pid=ev.pid
WHERE ev.rg IS NOT NULL GROUP BY 1,2 HAVING period IS NOT NULL"""

def view_action_sql(vrows):
    # per viewed partner: first lead ACTION (slot proposed or declined, any candidate of theirs)
    # AFTER their first banner view. view_fs = unix epoch (UTC); raw iec timestamps are UTC.
    return f"""
WITH vm AS (SELECT * FROM (VALUES {vrows}) AS t(pid, vts)),
csp AS (SELECT CSP_ID, PARTNER_ID::string pid FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT
        WHERE _fivetran_active=TRUE QUALIFY ROW_NUMBER() OVER (PARTITION BY CSP_ID ORDER BY 1)=1),
act AS (
  SELECT c.pid, MIN(r.updated_at) a_ts
  FROM PROD_DB.CSP_TAS_SERVICE_CSP_TAS_SERVICE.INSTALL_EXECUTION_CANDIDATES r
  JOIN csp c ON c.CSP_ID=r.csp_id
  JOIN vm ON vm.pid=c.pid
  WHERE r.created_at >= '2026-07-01'
    AND r.current_state IN ('SLOT_SELECTED','AWAITING_CUSTOMER_SLOT_CONFIRMATION','DECLINED',
        'TECHNICIAN_ASSIGNED','ARRIVED_AT_SITE','INSTALLATION_IN_PROGRESS_PRE_FEE',
        'INSTALLATION_IN_PROGRESS_POST_FEE','FEE_COLLECTION_PENDING','AWAITING_CUSTOMER_OTP',
        'CONNECTION_ACTIVE','RATING_PENDING')
    AND r.updated_at > TO_TIMESTAMP(vm.vts)
  GROUP BY 1)
SELECT vm.pid, ROUND(DATEDIFF(second, TO_TIMESTAMP(vm.vts), act.a_ts)/60.0) mins
FROM vm LEFT JOIN act ON act.pid=vm.pid"""

# ---------- lead drill (heatmap cell click) ----------
_drill_cache = {}
def lead_drill(d, hr):
    """All enrolled-cohort bookings confirmed on IST date d at hour hr (or hr='night' = 0:00-7:59),
    with the full journey: every partner the lead was sent to and what happened there. Mobiles masked."""
    key = f"{d}|{hr}"
    hit = _drill_cache.get(key)
    if hit and (datetime.datetime.utcnow() - hit[0]).total_seconds() < 600:
        return hit[1]
    hour_cond = "DATE_PART(hour, BOOKING_CONFIRM_TIME) <= 7" if str(hr) == "night" else f"DATE_PART(hour, BOOKING_CONFIRM_TIME) = {int(hr)}"
    enr = set(anchors())
    pl = "','".join(sorted(enr))
    q = f"""
WITH bookings AS (
  SELECT MOBILE mobile, TO_DATE(BOOKING_CONFIRM_DATE) booking_date, BOOKING_CONFIRM_TIME bt, NEXT_BOOKING_CONFIRM_TIME nb
  FROM PROD_DB.DBT.fct_booking_window
  WHERE BOOKING_CONFIRM_DATE = '{d}' AND {hour_cond}),
acc AS (SELECT b.*, dr.ACCOUNT_ID::STRING account_id, dr.LCO_ACCOUNT_ID lco, dr.GROUP_NAME flow
  FROM bookings b LEFT JOIN PROD_DB.DYNAMODB_read.BOOKING dr ON dr.MOBILE=b.mobile AND dr._FIVETRAN_DELETED=FALSE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY b.mobile,b.booking_date ORDER BY dr.ADDED_TIME DESC NULLS LAST)=1),
acc_clean AS (SELECT * FROM acc WHERE lco IS NULL OR lco NOT IN (SELECT LCO_ACCOUNT_ID FROM PROD_DB.PUBLIC.TEST_LCO_ACCOUNT_ID WHERE LCO_ACCOUNT_ID IS NOT NULL)),
ma AS (SELECT DISTINCT MOBILE mobile, ACCOUNT_ID::STRING account_id FROM PROD_DB.DYNAMODB.BOOKING
  WHERE ACCOUNT_ID IS NOT NULL AND MOBILE IN (SELECT mobile FROM bookings)),
conn AS (SELECT a.mobile, a.bt, c.CONNECTION_ID FROM acc_clean a
  JOIN ma ON ma.mobile=a.mobile
  JOIN PROD_DB.CSP_CONNECTION_LIFECYCLE_SERVICE_CSP_CONNECTION_LIFECYCLE_SERVICE.CONNECTION_EVENT_HISTORY ceh
    ON ceh.EVENT_TYPE='CONNECTION_REQUEST' AND ceh._FIVETRAN_DELETED=FALSE
   AND ceh.EVENT_TIMESTAMP BETWEEN DATEADD(hour,-2,DATEADD(minute,-330,a.bt)) AND DATEADD(hour,24*{CONN_DAYS},DATEADD(minute,-330,a.bt))
   AND (a.nb IS NULL OR DATEADD(minute,330,ceh.EVENT_TIMESTAMP)<a.nb)
  JOIN PROD_DB.CSP_CONNECTION_LIFECYCLE_SERVICE_CSP_CONNECTION_LIFECYCLE_SERVICE.CONNECTIONS c
    ON c.CONNECTION_ID=ceh.CONNECTION_ID AND c.CUSTOMER_ID::STRING=ma.account_id AND c._fivetran_active=TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.mobile,a.booking_date ORDER BY ceh.EVENT_TIMESTAMP)=1),
cand AS (SELECT * FROM PROD_DB.DBT_CSP.TAS_INSTALL_EXECUTION_CANDIDATES
  WHERE ETL_CURRENT=TRUE AND CONNECTION_ID IN (SELECT CONNECTION_ID FROM conn)),
tlast AS (SELECT CONNECTION_ID, CSP_ID FROM cand
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CONNECTION_ID ORDER BY UPDATED_AT DESC)=1),
cspm AS (SELECT CSP_ID, PARTNER_ID::string pid, MAX(NAME) nm FROM PROD_DB.CSP_GATEWAY_SERVICE_CSP_GATEWAY_SERVICE.CSP_ACCOUNT
  WHERE _fivetran_active=TRUE GROUP BY 1,2),
raw AS (SELECT EXECUTION_CANDIDATE_ID,
    MIN(IFF(current_state IN ('SLOT_SELECTED','AWAITING_CUSTOMER_SLOT_CONFIRMATION'), updated_at, NULL)) prop_ts,
    MIN(IFF(current_state='DECLINED', updated_at, NULL)) decl_ts,
    MIN(IFF(current_state='TECHNICIAN_ASSIGNED', updated_at, NULL)) asgn_ts,
    MIN(IFF(current_state IN ('CONNECTION_ACTIVE','RATING_PENDING'), updated_at, NULL)) inst_ts
  FROM PROD_DB.CSP_TAS_SERVICE_CSP_TAS_SERVICE.INSTALL_EXECUTION_CANDIDATES
  WHERE created_at >= DATEADD(day,-1,'{d}')
    AND EXECUTION_CANDIDATE_ID IN (SELECT EXECUTION_CANDIDATE_ID FROM cand) GROUP BY 1)
SELECT cn.mobile, DATEADD(minute,0,cn.bt) bt, cand.EXECUTION_CANDIDATE_ID,
  COALESCE(cm.nm,'?') csp_name, IFF(cm.pid IN ('{pl}'),1,0) is_enr,
  DATEADD(minute,330,cand.CREATED_AT) sent_ist, cand.CURRENT_STATE,
  DATEADD(minute,330,raw.prop_ts) prop_ist, DATEADD(minute,330,raw.decl_ts) decl_ist,
  DATEADD(minute,330,raw.asgn_ts) asgn_ist, DATEADD(minute,330,raw.inst_ts) inst_ist,
  cand.FAILURE_REASON, cand.FAILURE_SUBREASON_CODE, cand.REASON_CODE,
  DATEADD(minute,330,cand.UPDATED_AT) upd_ist
FROM conn cn
JOIN tlast tl ON tl.CONNECTION_ID=cn.CONNECTION_ID
JOIN cspm lm ON lm.CSP_ID=tl.CSP_ID AND lm.pid IN ('{pl}')
JOIN cand ON cand.CONNECTION_ID=cn.CONNECTION_ID
LEFT JOIN cspm cm ON cm.CSP_ID=cand.CSP_ID
LEFT JOIN raw ON raw.EXECUTION_CANDIDATE_ID=cand.EXECUTION_CANDIDATE_ID
ORDER BY cn.mobile, cand.CREATED_AT"""
    rows = mb(q)
    if isinstance(rows, dict):
        raise RuntimeError("drill query error: " + json.dumps(rows)[:200])
    def ts(x):
        s = str(x) if x else ""
        return s[5:16].replace("-", "/").replace("T", " ") if len(s) >= 16 else None   # "MM/DD HH:MM"
    leads = {}
    for r in rows:
        (mob, bt, cid, nm, is_enr, sent, cs, prop, decl, asgn, inst, fr, fsc, rc, upd) = r
        mm = str(mob); mm = mm[:2] + "XXXXX" + mm[-3:]
        L = leads.setdefault(str(mob), {"mobile": mm, "booked": ts(bt), "hops": []})
        L["hops"].append({"csp": nm, "enr": int(is_enr or 0), "sent": ts(sent), "state": cs,
                          "prop": ts(prop), "decl": ts(decl), "asgn": ts(asgn), "inst": ts(inst),
                          "reason": (fr or rc or "") + ((" / " + fsc) if fsc else ""), "upd": ts(upd)})
    out = {"d": d, "hr": ("night" if str(hr) == "night" else int(hr)), "leads": list(leads.values()), "n": len(leads)}
    _drill_cache[key] = (datetime.datetime.utcnow(), out)
    return out

# ---------- assembly ----------
def _grp(rows, idx_key, g_idx, keymap=None):
    """collect rows of one grouping set: g_idx = index of the GROUPING() col that must be 0."""
    out = {}
    for r in rows:
        period = r[0]
        gflags = r[10:19]
        if sum(1 for x in gflags if x == 0) != 1 or gflags[g_idx] != 0:
            continue
        key = r[1 + g_idx]
        if key is None:
            continue
        if keymap: key = keymap(key)
        (recv, slot, confirm, assign, reached, install, t_ta, t_sc, t_ca, t_ai) = r[19:29]
        out.setdefault(str(key), {})[period] = {
            "received": recv, "slot": slot, "confirm": confirm, "assign": assign, "reached": reached, "install": install,
            "reached_pct": round(100.0*reached/recv, 1) if recv else None,
            "slot_pct": round(100.0*slot/recv, 1) if recv else None,
            "confirm_pct": round(100.0*confirm/slot, 1) if slot else None,     # of slot proposed
            "assign_pct": round(100.0*assign/confirm, 1) if confirm else None, # of confirmed
            "install_pct": round(100.0*install/recv, 1) if recv else None,     # of received
            "tat_ta": t_ta, "tat_sc": t_sc, "tat_ca": t_ca, "tat_ai": t_ai}
    return out

def mg_cohorts_sql(vals, d_from, yest, gh_where=''):
    return _base_csp(vals, d_from, yest) + f"""
SELECT mgpid,
  SUM(IFF(cur_depth>=2,1,0)) received,
  SUM(IFF(cur_depth>=4 AND (conf48=1 OR asgn48=1 OR reached48=1 OR inst48=1),1,0)) confirmed,
  SUM(IFF(cur_depth>=7 AND inst48=1,1,0)) installed
FROM base WHERE coh='E' AND period='POST' AND mgpid IS NOT NULL {gh_where} GROUP BY 1"""

_MG_GATE = 0.60          # >=3 leads: install-conversion gate to keep the guarantee
_TOO_EARLY_DAYS = 7      # < this many live days -> projection too noisy, held as actuals

def _mg_class(conf, inst):
    if conf < 3: return "mg_lo"
    if conf and inst / conf >= _MG_GATE: return "mg_hi"
    return "no_mg"

def mg_cohorts(vals, d_from, yest, nmap, remaining, gh_where=''):
    """Per enrolled CSP -> (actual, projected) MG cohort. lead = Cx-confirmed (matured); conv = installs/confirmed.
       ACTUAL: to-date leads. PROJECTED (pro-rata to month-end): leads*(elapsed+remaining)/elapsed; conversion is
       scale-invariant so only the <3 line moves; CSPs with < _TOO_EARLY_DAYS live days held as 'too_early' actuals."""
    rows = mb(mg_cohorts_sql(vals, d_from, yest, gh_where))
    per = {p: (0, 0, 0) for p in nmap}     # received, confirmed, installed
    if not isinstance(rows, dict):
        for pid, recv, conf, inst in rows:
            if pid is not None and str(pid) in per:
                per[str(pid)] = (int(recv or 0), int(conf or 0), int(inst or 0))
    mg_map = {}; mgp_map = {}
    counts = {"mg_lo": 0, "mg_hi": 0, "no_mg": 0}
    pcounts = {"mg_lo": 0, "mg_hi": 0, "no_mg": 0, "too_early": 0}
    proj = {"bookings": 0.0, "installs": 0.0}
    for p, (recv, conf, inst) in per.items():
        a = _mg_class(conf, inst); mg_map[p] = a; counts[a] += 1
        el = nmap.get(p, 1)
        if el < _TOO_EARLY_DAYS:
            mgp_map[p] = "too_early"; pcounts["too_early"] += 1
            proj["bookings"] += recv; proj["installs"] += inst          # held at actuals
        else:
            f = (el + remaining) / el
            pl = conf * f
            pc = "mg_lo" if pl < 3 else ("mg_hi" if (conf and inst / conf >= _MG_GATE) else "no_mg")
            mgp_map[p] = pc; pcounts[pc] += 1
            proj["bookings"] += recv * f; proj["installs"] += inst * f   # =leads*conv*f (scale-invariant)
    proj = {"bookings": round(proj["bookings"]), "installs": round(proj["installs"])}
    return mg_map, counts, mgp_map, pcounts, proj

def mg_only_cuts_sql(vals, d_from, yest, gh_where=''):
    return _base(vals, d_from, yest) + f"""
SELECT period, mg, {_AGG}
FROM base WHERE coh='E' AND period IS NOT NULL AND mg IS NOT NULL {gh_where}
GROUP BY period, mg"""

def mg_only_cuts_sql_csp(vals, d_from, yest, gh_where=''):
    return _base_csp(vals, d_from, yest) + f"""
SELECT period, mg, {_AGG_CSP}
FROM base WHERE coh='E' AND period IS NOT NULL AND mg IS NOT NULL {gh_where}
GROUP BY period, mg"""

def _grp_mg(rows):
    out = {}
    if isinstance(rows, dict) or rows is None: return out
    for r in rows:
        period = r[0]; key = r[1]
        if key is None: continue
        (recv, slot, confirm, assign, reached, install, t_ta, t_sc, t_ca, t_ai) = r[2:12]
        out.setdefault(str(key), {})[period] = {
            "received": recv, "slot": slot, "confirm": confirm, "assign": assign, "reached": reached, "install": install,
            "reached_pct": round(100.0*reached/recv, 1) if recv else None,
            "tat_ta": t_ta, "tat_sc": t_sc, "tat_ca": t_ca, "tat_ai": t_ai}
    return out

def delhi_weather(d_from, d_to):
    """Per-day weather symbol for the DoD chart (Delhi). Open-Meteo, no key. Returns {day: emoji} or None on any failure."""
    try:
        url = ("https://api.open-meteo.com/v1/forecast?latitude=28.65&longitude=77.22"
               "&hourly=precipitation,cloud_cover&timezone=Asia%2FKolkata"
               "&start_date=" + d_from + "&end_date=" + d_to)
        j = json.loads(urllib.request.urlopen(url, timeout=20).read().decode())
        h = j["hourly"]; time = h["time"]; precip = h["precipitation"]; cloud = h["cloud_cover"]
        acc = {}
        for ts, pr, cc in zip(time, precip, cloud):
            day = ts[:10]; hr = int(ts[11:13]); pr = pr or 0
            a = acc.setdefault(day, {"day_rain": 0.0, "late_night": 0.0, "early_night": 0.0, "cloud": []})
            if 9 <= hr <= 18:
                a["day_rain"] += pr; a["cloud"].append(cc or 0)
            if hr >= 21:
                a["late_night"] += pr        # counts toward the NEXT day's morning
            if hr <= 6:
                a["early_night"] += pr       # counts toward THIS day's morning
        days = sorted(acc.keys()); out = {}
        for i, day in enumerate(days):
            a = acc[day]; prev = acc.get(days[i-1]) if i > 0 else None
            night = (prev["late_night"] if prev else 0) + a["early_night"]
            day_cloud = sum(a["cloud"]) / len(a["cloud"]) if a["cloud"] else 0
            if a["day_rain"] >= 15:   s = "⛈️"           # thunderstorm
            elif a["day_rain"] >= 2.5: s = "🌧️"      # rain cloud
            elif a["day_rain"] >= 0.3: s = "🌦️"      # sun behind rain
            elif night >= 1:          s = "🌧️🌙"  # rain + moon (wet poles)
            elif day_cloud >= 70:     s = "⛅"                  # sun behind cloud
            else:                     s = "☀️"           # sun
            out[day] = s
        return out
    except Exception:
        return None

def compute_dash4():
    today = _ist_today(); yest = today - datetime.timedelta(days=1)
    if yest < datetime.date.fromisoformat(LAUNCH_D):
        yest = datetime.date.fromisoformat(LAUNCH_D)
    ad = anchors()
    pl = "','".join(sorted(ad))
    cat, _ = feedback_cats(pl)
    eng = banner_engagement(pl)
    # per-CSP N (complete post days) -> csp-days per group; CSPs enrolled today (N<1) excluded from rates
    nmap = {p: (yest - datetime.date.fromisoformat(a)).days + 1 for p, a in ad.items()}
    nmap = {p: n for p, n in nmap.items() if n >= 1}
    n_max = max(nmap.values()) if nmap else 1
    d_from = (datetime.date.fromisoformat(LAUNCH_D) - datetime.timedelta(days=n_max + 1)).isoformat()
    d_from = min(d_from, "2026-06-15")   # tab4 history window
    import json as _json
    _fc = _json.load(open(os.path.join(HERE, "frozen_cohort.json"), encoding="utf-8"))
    f2set = set(_fc["flow2"])
    vals0 = _values_map(ad, cat, eng, f2set)                                    # mg=NULL, for classification pass
    _me = datetime.date(yest.year + (yest.month == 12), (yest.month % 12) + 1, 1) - datetime.timedelta(days=1)
    remaining = (_me - yest).days                                               # days left this month (monthly guarantee)
    mg_map, mg_counts, mgp_map, mgp_counts, mg_proj_totals = mg_cohorts(vals0, d_from, yest.isoformat(), nmap, remaining)
    vals = _values_map(ad, cat, eng, f2set, mg_map)                             # mg = ACTUAL class, for main cuts
    vals_proj = _values_map(ad, cat, eng, f2set, mgp_map)                       # mg = PROJECTED class, for light cuts
    _GHW = "AND (bflow NOT IN ('G','H') OR bflow IS NULL)"                       # gate re-run on non-G/H bookings
    mg_map_g, mg_counts_g, mgp_map_g, mgp_counts_g, mg_proj_totals_g = mg_cohorts(vals0, d_from, yest.isoformat(), nmap, remaining, _GHW)
    vals_g = _values_map(ad, cat, eng, f2set, mg_map_g)
    vals_proj_g = _values_map(ad, cat, eng, f2set, mgp_map_g)
    rows = mb(cuts_sql(vals, d_from, yest.isoformat()))
    if isinstance(rows, dict):
        raise RuntimeError("cuts query error: " + json.dumps(rows)[:300])

    def cspdays(pred):
        return sum(n for p, n in nmap.items() if pred(p))
    def csps(pred):
        return sum(1 for p in nmap if pred(p))
    def enrich(block, days, ncsp):
        for period, m in (block or {}).items():
            m["csp_days"] = days; m["csps"] = ncsp
            m["recv_per_csp_d"] = round(m["received"]/days, 2) if days else None
            m["inst_per_csp_d"] = round(m["install"]/days, 3) if days else None
        return block

    def scr_of(p): return (eng.get(p, {}).get("screen") or "")
    def targ(p): return scr_of(p) in SCREENS
    def _isf2(p): return p in f2set
    non_days = (yest - datetime.date.fromisoformat(LAUNCH_D)).days + 1

    def assemble(rws, mgm=None):
        _mgm = mgm if mgm is not None else mg_map
        a = {}
        coh = _grp(rws, "coh", 0)
        a["tab1"] = {"enr": enrich(coh.get("E"), cspdays(lambda p: True), len(nmap)),
                     "non": coh.get("N"), "non_days": non_days}
        flw = _grp(rws, "flow", 7)
        a["tab1"]["enr_f1"] = enrich(flw.get("f1"), cspdays(lambda p: not _isf2(p)), csps(lambda p: not _isf2(p)))
        a["tab1"]["enr_f2"] = enrich(flw.get("f2"), cspdays(_isf2), csps(_isf2))
        cats = _grp(rws, "cat", 1)
        a["tab2"] = [{"key": k, "label": CAT_LABELS.get(k, k),
                      "csps": csps(lambda p, k=k: cat.get(p, "not_answered") == k),
                      "data": enrich(cats.get(k), cspdays(lambda p, k=k: cat.get(p, "not_answered") == k),
                                     csps(lambda p, k=k: cat.get(p, "not_answered") == k))}
                     for k in ("excited", "questions", "dontcare", "dontknow", "not_answered")]
        scr = _grp(rws, "screen", 2)
        a["tab3_screens"] = [{"key": s, "csps": csps(lambda p, s=s: scr_of(p) == s),
                              "data": enrich(scr.get(s), cspdays(lambda p, s=s: scr_of(p) == s),
                                             csps(lambda p, s=s: scr_of(p) == s))}
                             for s in SCREENS]
        eng_cuts = []
        for gname, gi, want in (("Viewed", 3, 1), ("Not viewed", 3, 0), ("Scrolled", 4, 1),
                                ("Tapped Guarantee", 5, 1), ("Ticket tap", 6, 1)):
            blk = _grp(rws, "eng", gi).get(str(want))
            pred = (lambda p, gi=gi, want=want: targ(p) and
                    (eng.get(p, {}).get({3: "viewed", 4: "scrolled", 5: "tapguar", 6: "ticket"}[gi], 0) == want))
            eng_cuts.append({"key": gname, "csps": csps(pred), "data": enrich(blk, cspdays(pred), csps(pred))})
        a["tab3_engagement"] = eng_cuts
        a["targeted"] = csps(targ)
        mgc = _grp(rws, "mg", 8)
        a["tab_mg"] = [{"key": k, "label": L,
                        "csps": csps(lambda p, k=k: _mgm.get(p) == k),
                        "data": enrich(mgc.get(k), cspdays(lambda p, k=k: _mgm.get(p) == k),
                                       csps(lambda p, k=k: _mgm.get(p) == k))}
                       for k, L in (("mg_lo", "Getting MG · <3 leads"),
                                    ("mg_hi", "Getting MG · ≥3 leads"),
                                    ("no_mg", "Not getting MG"))]
        return a

    out = {"generated": (datetime.datetime.utcnow()+datetime.timedelta(hours=5, minutes=30)).strftime("%d %b %Y, %H:%M IST"),
           "yest": yest.isoformat(), "launch": LAUNCH_D,
           "enrolled": len(nmap), "anchored": True, "mg_counts": mg_counts, "mg_gate": int(_MG_GATE*100)}
    out.update(assemble(rows))
    try:                                                                       # non-enrolled Delhi CSP count (roster - enrolled)
        pl_enr = "','".join(sorted(nmap))
        dq = mb("SELECT COUNT(DISTINCT PARTNER_ID) FROM PROD_DB.DBT.AGG_PARTNER_FUNNEL "
                "WHERE PARTNER_CITY ILIKE '%delhi%' AND PARTNER_ID NOT IN ('" + pl_enr + "')")
        out["non_csps"] = int(dq[0][0]) if not isinstance(dq, dict) else None
    except Exception:
        out["non_csps"] = None
    _GH = "AND (bflow NOT IN ('G','H') OR bflow IS NULL)"
    def _try(fn, mgm=None):
        try:
            r = mb(fn())
            return assemble(r, mgm) if not isinstance(r, dict) else None
        except Exception:
            return None
    out["csp"] = _try(lambda: cuts_sql_csp(vals, d_from, yest.isoformat()))
    out["noGH"] = _try(lambda: cuts_sql(vals_g, d_from, yest.isoformat(), _GH), mg_map_g)
    out["csp_noGH"] = _try(lambda: cuts_sql_csp(vals_g, d_from, yest.isoformat(), _GH), mg_map_g)
    for _k in ("noGH", "csp_noGH"):
        if out.get(_k):
            out[_k]["mg_counts"] = mg_counts_g
            out[_k]["mgp_counts"] = mgp_counts_g
            out[_k]["mg_proj_totals"] = mg_proj_totals_g
    def assemble_mgp(cutrows, mgpm=None):
        _mgpm = mgpm if mgpm is not None else mgp_map
        g = _grp_mg(cutrows)
        return [{"key": k, "label": L,
                 "csps": csps(lambda p, k=k: _mgpm.get(p) == k),
                 "data": enrich(g.get(k), cspdays(lambda p, k=k: _mgpm.get(p) == k),
                                csps(lambda p, k=k: _mgpm.get(p) == k))}
                for k, L in (("mg_lo", "Getting MG · <3 leads"),
                             ("mg_hi", "Getting MG · ≥3 leads"),
                             ("no_mg", "Not getting MG"),
                             ("too_early", "Too early (<%d days)" % _TOO_EARLY_DAYS))]
    try:
        out["tab_mg_proj"] = assemble_mgp(mb(mg_only_cuts_sql(vals_proj, d_from, yest.isoformat())))
        if out.get("csp"): out["csp"]["tab_mg_proj"] = assemble_mgp(mb(mg_only_cuts_sql_csp(vals_proj, d_from, yest.isoformat())))
        if out.get("noGH"): out["noGH"]["tab_mg_proj"] = assemble_mgp(mb(mg_only_cuts_sql(vals_proj_g, d_from, yest.isoformat(), _GH)), mgp_map_g)
        if out.get("csp_noGH"): out["csp_noGH"]["tab_mg_proj"] = assemble_mgp(mb(mg_only_cuts_sql_csp(vals_proj_g, d_from, yest.isoformat(), _GH)), mgp_map_g)
    except Exception:
        out["tab_mg_proj"] = None
    out["mgp_counts"] = mgp_counts
    out["mg_proj_totals"] = mg_proj_totals
    out["too_early_days"] = _TOO_EARLY_DAYS
    out["month_remaining"] = remaining

    # after viewing the banner -> first action on any lead (slot proposed or declined)
    viewed = [(p, e["view_fs"]) for p, e in eng.items() if e.get("viewed") and e.get("view_fs") and p in nmap]
    va = {"n_viewed": len(viewed)}
    if viewed:
        vrows = ",".join("('%s',%d)" % (p, v) for p, v in viewed)
        arows = mb(view_action_sql(vrows))
        if not isinstance(arows, dict):
            mins = sorted(r[1] for r in arows if r[1] is not None and r[1] >= 0)
            va["acted"] = len(mins)
            va["acted_pct"] = round(100.0 * len(mins) / len(viewed), 1)
            if mins:
                va["median_min"] = mins[len(mins)//2]
                va["p75_min"] = mins[int(len(mins)*0.75)]
                va["within24h_pct"] = round(100.0 * sum(1 for m in mins if m <= 1440) / len(viewed), 1)
    out["tab3_view_action"] = va

    # app opens / day by role, anchored windows (same convention as recv_d: opens*csps/csp_days)
    total_days = cspdays(lambda p: True); n_all = len(nmap)
    ao = {"PRE": {}, "POST": {}}
    orows = mb(opens_sql(vals, d_from, yest.isoformat()))
    if not isinstance(orows, dict):
        for period, rg, opens, _act in orows:
            if period in ao and rg:
                ao[period][rg + "_d"] = round(opens * n_all / total_days) if total_days else None
        for period in ao:
            ao[period]["total_d"] = sum(v for v in ao[period].values() if v) or None
    out["app_opens"] = ao

    def build_tab4(t4rows):
        daily = {}; hourly = {"B": [], "I": []}
        if isinstance(t4rows, dict) or t4rows is None: return None
        km = {"B": "b", "I": "i", "J": "j", "K": "k"}; today_s = _ist_today().isoformat()
        for k, d, hr, n in t4rows:
            dt = str(d)[:10]
            if k not in km or dt > today_s: continue
            if k != "K" and dt <= yest.isoformat():
                daily.setdefault(dt, {"b": 0, "i": 0, "j": 0}); daily[dt][km[k]] += n
            if k in ("B", "I", "K"):
                hourly.setdefault(k, []).append({"d": dt, "hr": hr, "n": n})
        return {"daily": [{"d": k, "b": v["b"], "i": v["i"], "j": v.get("j", 0)} for k, v in sorted(daily.items())],
                "hourly_book": hourly.get("B", []), "hourly_inst": hourly.get("I", []),
                "hourly_binst": hourly.get("K", []), "from": d_from}
    out["tab4"] = build_tab4(mb(tab4_sql(vals, d_from, yest.isoformat())))
    try:
        out["tab4_noGH"] = build_tab4(mb(tab4_sql(vals, d_from, yest.isoformat(), _GH)))
    except Exception:
        out["tab4_noGH"] = None
    if out.get("tab4"):
        try:
            days = [x["d"] for x in out["tab4"]["daily"]]
            if days:
                ws = (datetime.date.fromisoformat(min(days)) - datetime.timedelta(days=1)).isoformat()
                out["tab4"]["weather"] = delhi_weather(ws, _ist_today().isoformat())  # None on failure -> frontend hides row
        except Exception:
            out["tab4"]["weather"] = None
    return out

if __name__ == "__main__":
    d = compute_dash4()
    print("enrolled", d["enrolled"], "| targeted", d["targeted"], "| yest", d["yest"])
    t1 = d["tab1"]
    for coh in ("enr", "non"):
        for p in ("PRE", "POST"):
            x = (t1.get(coh) or {}).get(p)
            if x: print(coh, p, {k: x[k] for k in ("received", "slot_pct", "confirm_pct", "assign_pct", "install_pct", "recv_per_csp_d", "tat_ta", "tat_ca", "tat_ai") if k in x})
    print("tab2:", [(c["key"], c["csps"], bool(c["data"])) for c in d["tab2"]])
    print("tab3 screens:", [(s["key"], s["csps"]) for s in d["tab3_screens"]])
    print("tab3 engagement:", [(e["key"], e["csps"]) for e in d["tab3_engagement"]])
    print("tab4 days:", len(d["tab4"]["daily"]), "| hourly cells B/I:", len(d["tab4"]["hourly_book"]), len(d["tab4"]["hourly_inst"]))
