# ----------------------------------------------------------------------------
# ARTHAN — Market Intelligence Engine v2
# Self-updating, self-verifying market numbers for every tracked mainboard IPO:
#   GMP (+ average), estimated listing price & %, live subscription
#   (QIB / sHNI / bHNI / Retail / Total), Kostak, Subject-to-Sauda,
#   IPO dates, allotment date (+ usual time hint), listing date.
#
# Design goals (in priority order):
#   1. Never show a number we can't defend — every value carries a
#      verification state computed from independent sources:
#        verified  = 2 independent sources agree (within tolerance)
#        single    = only one source publishes it right now
#        diverged  = sources disagree — keep freshest, flag it loudly
#      A value that fails the 2-sample stability gate is NOT written —
#      the previous verified value is kept instead (no scratching/drift).
#   2. Auto-attach to already-tracked IPOs by tolerant fuzzy matching,
#      so spelling variants never lose data.
#   3. Watch-list tokens (e.g. "davangere veeresh sahakara") scanned on
#      every refresh so a newly-announced IPO lights up the moment any
#      source publishes it.
#
# Sources (all plain server-rendered HTML — no JS execution, so parsing
# can never pick the wrong widget):
#   * IPOWatch GMP board      (gmp, est listing, dates, status, updated-ts)
#   * IPOWatch subscription   (QIB/NII/Retail/Total ×, closing, updated-ts)
#   * InvestorGain GMP page   (kostak / subject-to-sauda when published)
#   * existing in-server feeds (chittorgarh live subs + IPOJi subs/GMP)
#     act as the independent cross-check sources.
# ----------------------------------------------------------------------------

import json
import re
import time
from datetime import datetime, timedelta

IPOWATCH = "https://ipowatch.in"
IW_GMP_URL = IPOWATCH + "/ipo-grey-market-premium-latest-ipo-gmp/"
IW_SUB_URL = IPOWATCH + "/ipo-subscription-status-today/"
IG_GMP_URL = "https://www.investorgain.com/report/live-ipo-gmp/331/ipo/"

# time between the two samples of the stability gate (seconds)
STABILITY_GAP = 18

# tolerances
AGREE_TOL = 0.05      # <=5% difference  -> verified
DIVERGE_TOL = 0.08    # >8% difference   -> diverged (kept value = freshest)

WATCH_KV = "arthan_watch"          # json: [{tokens:[...], label:...}]
ISSUES_KV = "market_issues"        # json: [{sev,title,detail}]
GMP_BOARD_KV = "arthan_gmp_board"  # cached full board for the Market tab
SUB_BOARD_KV = "arthan_sub_board"
VERIFY_KV = "arthan_verify"


def _srv():
    import server  # lazy — server imports this module at boot
    return server


def _ua():
    return getattr(_srv(), "UA", {"User-Agent": "Mozilla/5.0"})


# ---------------------------------------------------------------------------
# low-level fetch + parse helpers
# ---------------------------------------------------------------------------

def _get(url, retries=2, timeout=25):
    requests = _srv().requests
    last = None
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=_ua(), timeout=timeout, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 5000:
                return r.text
            last = f"HTTP {r.status_code}"
        except Exception as e:  # noqa: BLE001 - network is unreliable by nature
            last = str(e)
        if attempt < retries:
            time.sleep(4 + attempt * 4)
    raise RuntimeError(last or "fetch failed")


def _cells(tr_html):
    return [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
            for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr_html, re.S)]


def _num(txt, default=None):
    try:
        return float(txt.replace(",", ""))
    except (TypeError, ValueError, AttributeError):
        return default


def _pct_delta(a, b):
    if a is None or b is None:
        return None
    if a == b:
        return 0.0
    base = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / base


# ---------------------------------------------------------------------------
# tolerant fuzzy matching between site names and our IPO names
# ---------------------------------------------------------------------------

STOP = {"ipo", "ltd", "limited", "india", "the", "of", "and", "co", "company"}


def _tokens(name):
    return {t for t in re.sub(r"[^a-z0-9 ]", " ", str(name).lower()).split()
            if t and t not in STOP}


def _norm(name):
    return re.sub(r"[^a-z0-9]", "", str(name).lower())


def match_name(target, candidates, threshold=0.55):
    """Return (best_candidate, score). candidates = iterable of names."""
    tset, tnorm = _tokens(target), _norm(target)
    best, best_score = None, 0.0
    for c in candidates:
        cset, cnorm = _tokens(c), _norm(c)
        if not cset:
            continue
        inter = len(tset & cset)
        union = len(tset | cset)
        score = inter / union if union else 0
        if tnorm and cnorm and (tnorm in cnorm or cnorm in tnorm):
            score = max(score, 0.75 if min(len(tnorm), len(cnorm)) > 5 else 0.4)
        if inter >= 2 and inter == len(tset):      # our tokens fully covered
            score = max(score, 0.85)
        if score > best_score:
            best, best_score = c, score
    return (best, best_score) if best_score >= threshold else (None, 0.0)


# ---------------------------------------------------------------------------
# source parsers — return dicts keyed by site name
# ---------------------------------------------------------------------------

def parse_iw_gmp(html):
    """IPOWatch GMP board -> {key: rec}. rec:
    name, slug(url), gmp, est_listing, est_pct, price, dates_txt, board,
    status, updated"""
    out = {}
    for tbl in re.findall(r"<table[^>]*>(.*?)</table>", html, re.S):
        if "Est. Listing" not in tbl or "IPO GMP" not in tbl:
            continue
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S):
            cells = _cells(tr)
            if len(cells) < 9 or "IPO Name" in cells[0]:
                continue
            link = re.search(r'href="(https://ipowatch\.in/[a-z0-9\-]+/)"', tr)
            est_m = re.search(r"₹\s*([\d,]+(?:\.\d+)?)\s*\(([\-\d.]+)%\)", cells[4])
            out[_norm(cells[0])] = {
                "name": cells[0],
                "url": link.group(1) if link else "",
                "gmp": _num(re.sub(r"[^\d.\-]", "", cells[1]) or "", 0.0),
                "price": _num(re.sub(r"[^\d.\-]", "", cells[3]) or "", 0.0),
                "est_listing": _num(est_m.group(1), None) if est_m else None,
                "est_pct": _num(est_m.group(2), None) if est_m else None,
                "dates_txt": cells[5],
                "board": "SME" if "sme" in cells[6].lower() else "Mainboard",
                "status": cells[7],
                "updated": cells[8],
            }
    return out


def parse_iw_sub(html):
    """IPOWatch subscription board -> {key: rec} with qib/nii/rii/total."""
    out = {}
    for tbl in re.findall(r"<table[^>]*>(.*?)</table>", html, re.S):
        if "QIB" not in tbl or "Retail" not in tbl:
            continue
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S):
            cells = _cells(tr)
            if len(cells) < 8 or cells[0] == "IPO":
                continue
            out[_norm(cells[0])] = {
                "name": cells[0],
                "board": "SME" if "sme" in cells[1].lower() else "Mainboard",
                "close_txt": cells[2],
                "qib": _num(cells[3]), "nii": _num(cells[4]),
                "rii": _num(cells[5]), "total": _num(cells[6]),
                "updated": cells[7],
            }
    return out


def parse_ig_kostak(html):
    """InvestorGain live-ipo-gmp rows (Kostak + Subject-to-Sauda). Their
    table is client-side rendered; if a server-rendered/embedded variant is
    present we extract it, otherwise return {} (callers show '—')."""
    out = {}
    plain = html  # rows may appear either raw or inside escaped payloads
    for tbl in re.findall(r"<table[^>]*>(.*?)</table>", plain, re.S):
        header = _cells("".join(re.findall(r"<tr[^>]*>.*?</tr>", tbl, re.S)[:1]))
        if not any("kostak" in h.lower() for h in header):
            continue
        idx = {h.lower(): n for n, h in enumerate(header)}
        def col(cells, *names):
            for nm in names:
                for h, n in idx.items():
                    if nm in h and n < len(cells):
                        return cells[n]
            return ""
        for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S)[1:]:
            cells = _cells(tr)
            if len(cells) < 4:
                continue
            nm = col(cells, "ipo")
            if not nm:
                continue
            out[_norm(nm)] = {
                "name": nm,
                "gmp": _num(re.sub(r"[^\d.\-]", "", col(cells, "gmp") or ""), None),
                "kostak": _num(re.sub(r"[^\d.\-]", "", col(cells, "kostak") or ""), None),
                "sauda": _num(re.sub(r"[^\d.\-]", "", col(cells, "sauda", "subj", "s2s") or ""), None),
            }
    return out


# ---------------------------------------------------------------------------
# stability gate: fetch the same page twice; a field may appear in the result
# only when both samples agree exactly (protects against half-rendered HTML,
# mid-update snapshots and layout swaps)
# ---------------------------------------------------------------------------

def stable_board(url, parser, fields):
    try:
        a = parser(_get(url))
        time.sleep(STABILITY_GAP)
        b = parser(_get(url))
    except Exception as e:  # noqa: BLE001
        return None, f"{e}"
    merged, unstable = {}, []
    for key, ra in a.items():
        rb = b.get(key)
        if rb is None:
            merged[key] = ra            # row vanished between samples: keep, single-source
            continue
        bad = [f for f in fields
               if (ra.get(f) is not None or rb.get(f) is not None) and ra.get(f) != rb.get(f)]
        if bad:
            unstable.append(f"{ra['name']} ({', '.join(bad)})")
            merged[key] = {**ra, "_unstable": bad}   # kept but only untouched fields are trusted
        else:
            merged[key] = ra
    return merged, unstable


# ---------------------------------------------------------------------------
# main refresh — the heart of the engine
# ---------------------------------------------------------------------------

def refresh_market(force=False):
    s = _srv()
    now = s.ist_now().isoformat(timespec="seconds")
    result = {"ok": True, "matched": 0, "verified": 0, "kept": 0,
              "watch_hits": [], "unstable": [], "errors": []}

    gmp_board, unstable_g = stable_board(
        IW_GMP_URL, parse_iw_gmp, ("gmp", "est_listing", "est_pct", "price"))
    if gmp_board is None:
        result["ok"] = False
        result["errors"].append(f"gmp board: {unstable_g}")
        gmp_board = {}
    else:
        result["unstable"] += unstable_g

    sub_board, unstable_s = stable_board(
        IW_SUB_URL, parse_iw_sub, ("qib", "nii", "rii", "total"))
    if sub_board is None:
        result["errors"].append(f"subscription board: {unstable_s}")
        sub_board = {}
    else:
        result["unstable"] += unstable_s

    try:
        kostak_board = parse_ig_kostak(_get(IG_GMP_URL, retries=1))
    except Exception:  # noqa: BLE001 - optional source
        kostak_board = {}

    # cache full boards for the Market tab (mainboard rows first)
    for kv_key, board in ((GMP_BOARD_KV, gmp_board), (SUB_BOARD_KV, sub_board)):
        try:
            rows_sorted = sorted(board.values(),
                                 key=lambda r: (r.get("board") != "Mainboard", r.get("name", "")))
            s.kv_set(kv_key, json.dumps({"at": now, "rows": rows_sorted})[:60000])
        except Exception:  # noqa: BLE001
            pass

    # ---------- attach to tracked IPOs ----------
    ipos = s.rows("SELECT * FROM ipos")
    gmp_names = [r["name"] for r in gmp_board.values()]
    sub_names = [r["name"] for r in sub_board.values()]
    kost_names = list(kostak_board.values()) and [r["name"] for r in kostak_board.values()]

    # legacy independent feeds already maintained by server scheduler:
    #   sub_json (CG live + IPOJi split) and gmp (IPOJi) — our cross-checks
    for ipo in ipos:
        if ipo.get("board") != "Mainboard":
            continue
        tgt = ipo["name"]
        g_hit, _ = match_name(tgt, gmp_names)
        s_hit, _ = match_name(tgt, sub_names)
        k_hit, _ = match_name(tgt, kost_names) if kost_names else (None, 0)
        g_rec = gmp_board.get(_norm(g_hit)) if g_hit else None
        s_rec = sub_board.get(_norm(s_hit)) if s_hit else None
        k_rec = kostak_board.get(_norm(k_hit)) if k_hit else {}
        if not g_rec and not s_rec and not k_rec:
            continue
        result["matched"] += 1

        legacy_sub = {}
        try:
            legacy_sub = json.loads(ipo.get("sub_json") or "{}")
        except (TypeError, ValueError):
            pass
        # independent cross-check value: the IPOJi/CG feed column (falls back
        # to the display column before the first feed lands)
        legacy_gmp = ipo.get("gmp_feed") or ipo.get("gmp") or 0

        def verdict(new, legacy):
            """compare new source vs independent in-app value"""
            if new is None:
                return "none"
            if legacy in (None, 0, ""):
                return "single"
            d = _pct_delta(new, float(legacy))
            if d is None:
                return "single"
            return "verified" if d <= AGREE_TOL else ("diverged" if d > DIVERGE_TOL else "verified")

        # ---- GMP group ----
        gmp_val, gmp_state = None, "none"
        if g_rec and "gmp" not in (g_rec.get("_unstable") or []):
            gmp_val = g_rec["gmp"]
            gmp_state = verdict(gmp_val, legacy_gmp)
            if gmp_state == "verified":
                result["verified"] += 1
            if gmp_state == "diverged":
                _issue(f"GMP differs across sources: {ipo['name']}",
                       f"IPOWatch ₹{gmp_val:+.0f} vs app feed ₹{legacy_gmp:+.0f} — showing the fresher IPOWatch value")
        elif legacy_gmp:
            gmp_val, gmp_state = legacy_gmp, "kept"
            result["kept"] += 1

        # ---- subscription group ----
        sub_out, sub_state = {}, "none"
        merged_sub = {
            "qib": s_rec.get("qib") if s_rec else None,
            "nii": s_rec.get("nii") if s_rec else None,
            "rii": s_rec.get("rii") if s_rec else None,
            "total": s_rec.get("total") if s_rec else None,
            "shni": legacy_sub.get("shni"),
            "bhni": legacy_sub.get("bhni"),
            "emp": legacy_sub.get("emp"),
        }
        # honour the stability gate: drop fields flagged unstable
        if s_rec and s_rec.get("_unstable"):
            for f in s_rec["_unstable"]:
                if f in merged_sub:
                    merged_sub[f] = legacy_sub.get(f)
        votes = []
        for f in ("qib", "nii", "rii", "total"):
            if merged_sub.get(f) is not None:
                votes.append(verdict(merged_sub[f], legacy_sub.get(f)))
        if votes:
            if "diverged" in votes:
                sub_state = "diverged"
                worst = max((f for f in ("qib", "nii", "rii", "total")),
                            key=lambda f: _pct_delta(merged_sub.get(f) or 0,
                                                     float(legacy_sub.get(f) or 0) if legacy_sub.get(f) else 1e9) or 9)
                _issue(f"Subscription sources disagree: {ipo['name']}",
                       f"worst on {worst}: IPOWatch {merged_sub.get(worst)}× vs app feed {legacy_sub.get(worst)}× — showing IPOWatch (fresh)")
            elif "verified" in votes:
                sub_state = "verified"
                result["verified"] += 1
            else:
                sub_state = "single"
        sub_out = {k: v for k, v in merged_sub.items() if v is not None}
        if sub_out:
            sub_out["src"] = "iw+cg+iji"

        # ---- kostak / subject-to-sauda ----
        kostak = k_rec.get("kostak") if k_rec else None
        sauda = k_rec.get("sauda") if k_rec else None

        # gmp 5-reading average from history
        gmp_avg = None
        try:
            hist = s.rows("SELECT gmp FROM gmp_hist WHERE ipo_id=? ORDER BY day DESC LIMIT 7", (ipo["id"],))
            if hist:
                gmp_avg = round(sum(h["gmp"] for h in hist) / len(hist), 2)
        except Exception:  # noqa: BLE001
            pass

        mkt = {
            "gmp": gmp_val, "gmp_avg": gmp_avg, "gmp_state": gmp_state,
            "est_listing": (g_rec or {}).get("est_listing"),
            "est_pct": (g_rec or {}).get("est_pct"),
            "kostak": kostak, "sauda": sauda,
            "sub": sub_out or None, "sub_state": sub_state,
            "iw_status": (g_rec or {}).get("status", ""),
            "allotment_hint": "evening (usually after 6 PM)",
            "updated_at": now,
        }
        run_pairs = [("mkt_json", json.dumps(mkt)), ("mkt_at", now)]
        # keep the legacy column in sync so every existing screen benefits
        if gmp_val is not None:
            run_pairs.append(("gmp", gmp_val))
            try:
                s.run("INSERT OR IGNORE INTO gmp_hist(ipo_id, day, gmp) VALUES(?,?,?)",
                      (ipo["id"], s.today_str(), gmp_val))
            except Exception:  # noqa: BLE001
                pass
        sets = ",".join(f"{k}=?" for k, _ in run_pairs)
        s.run(f"UPDATE ipos SET {sets} WHERE id=?", (*[v for _, v in run_pairs], ipo["id"]))
        result["matched"] = result["matched"]  # explicit for clarity

    # ---------- watch tokens (Davangere-style new IPO detection) ----------
    watch = _watch_list()
    all_site_names = gmp_names + sub_names
    for w in watch:
        hit, score = match_name(" ".join(w["tokens"]), all_site_names, threshold=0.6)
        prev = s.kv_get(f"arthan_watch_hit:{w['label']}")
        if hit and not prev:
            s.kv_set(f"arthan_watch_hit:{w['label']}", json.dumps({"name": hit, "at": now}))
            result["watch_hits"].append(w["label"])
            _issue(f"🚀 Watched IPO now published: {w['label']}", f"appears on market boards as '{hit}'",
                   sev="high")
            _auto_add_ipo_from_boards(hit, gmp_board, sub_board)

    s.kv_set("arthan_last", json.dumps({"at": now, "summary": result}))
    s.schedule_backup()
    return result


def _issue(title, detail, sev="med"):
    s = _srv()
    try:
        issues = json.loads(s.kv_get(ISSUES_KV, "[]") or "[]")
    except (TypeError, ValueError):
        issues = []
    # one live entry per issue title — update detail/time in place so a
    # persistent disagreement doesn't re-queue an alert every refresh
    for i in issues:
        if i.get("title") == title:
            i.update(detail=detail, sev=sev, at=s.ist_now().isoformat(timespec="seconds"))
            break
    else:
        issues.append({"sev": sev, "title": title, "detail": detail,
                       "at": s.ist_now().isoformat(timespec="seconds")})
    s.kv_set(ISSUES_KV, json.dumps(issues[-80:]))


def pop_issues():
    """Return queued data-quality issues (consumed into the alert feed)."""
    s = _srv()
    try:
        issues = json.loads(s.kv_get(ISSUES_KV, "[]") or "[]")
    except (TypeError, ValueError):
        issues = []
    if issues:
        s.kv_set(ISSUES_KV, "[]")
    return issues


def _watch_list():
    s = _srv()
    try:
        return json.loads(s.kv_get(WATCH_KV, "[]") or "[]")
    except (TypeError, ValueError):
        return []


def add_watch(tokens, label):
    s = _srv()
    wl = _watch_list()
    if not any(w["label"] == label for w in wl):
        wl.append({"tokens": list(tokens), "label": label})
        s.kv_set(WATCH_KV, json.dumps(wl))


def _auto_add_ipo_from_boards(site_name, gmp_board, sub_board):
    """Create an IPO row from board data the moment a watched IPO appears."""
    s = _srv()
    g = gmp_board.get(_norm(site_name)) or {}
    sub_b = sub_board.get(_norm(site_name)) or {}
    open_d, close_d = _dates_from_txt(g.get("dates_txt", ""))
    dup = next((i for i in s.rows("SELECT id,name FROM ipos")
                if match_name(i["name"], [site_name])[0]), None)
    if dup:
        return dup["id"]
    return s.run("""INSERT INTO ipos(name,registrar,open_date,close_date,price_min,price_max,
                    lot_size,board,source,notes) VALUES(?, 'Other', ?,?,?,?,?, 'Mainboard','auto-watch',?)""",
                 (g.get("name") or site_name, open_d, close_d,
                  0, g.get("price") or 0, 0,
                  "auto-added from watch-list; verify details"))


MONTH_TOKEN = {m: n + 1 for n, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}


def _dates_from_txt(txt):
    """'24-27 August' / '17-19 August' -> (open, close) in current year."""
    m = re.search(r"(\d{1,2})\s*-\s*(\d{1,2})\s+([A-Za-z]+)", txt or "")
    if not m:
        return "", ""
    yr = _srv().today_str()[:4]
    try:
        o = datetime(int(yr), MONTH_TOKEN[m.group(3)[:3].lower()], int(m.group(1)))
        c = datetime(int(yr), MONTH_TOKEN[m.group(3)[:3].lower()], int(m.group(2)))
        return o.date().isoformat(), c.date().isoformat()
    except (ValueError, KeyError):
        return "", ""


# ---------------------------------------------------------------------------
# daily deep verification — re-fetches everything fresh and reports
# ---------------------------------------------------------------------------

def daily_verification():
    s = _srv()
    res = refresh_market(force=True)
    issues = json.loads(s.kv_get(ISSUES_KV, "[]") or "[]")

    # date sanity vs stored dates: boards carry "17-19 August" style ranges
    gmp_board = json.loads(s.kv_get(GMP_BOARD_KV, "{}") or "{}").get("rows", [])
    names = [r["name"] for r in gmp_board]
    date_fixes = []
    for ipo in s.rows("SELECT * FROM ipos WHERE board='Mainboard'"):
        if s.ipo_status(ipo) == "listed":
            continue
        hit, _ = match_name(ipo["name"], names)
        if not hit:
            continue
        rec = next(r for r in gmp_board if r["name"] == hit)
        o, c = _dates_from_txt(rec.get("dates_txt", ""))
        upd = {}
        if o and (not ipo.get("open_date") or ipo["open_date"] != o):
            upd["open_date"] = o
        if c and (not ipo.get("close_date") or ipo["close_date"] != c):
            upd["close_date"] = c
        if upd:
            sets = ",".join(f"{k}=?" for k in upd)
            s.run(f"UPDATE ipos SET {sets} WHERE id=?", (*upd.values(), ipo["id"]))
            date_fixes.append(f"{ipo['name']}: {', '.join(f'{k}→{v}' for k, v in upd.items())}")

    summary = {
        "at": s.ist_now().isoformat(timespec="seconds"),
        "matched": res.get("matched"), "verified": res.get("verified"),
        "kept (unstable, old value retained)": res.get("kept"),
        "unstable_rows": res.get("unstable"), "errors": res.get("errors"),
        "date_corrections": date_fixes,
        "open_issues": len(issues),
    }
    s.kv_set(VERIFY_KV, json.dumps(summary))
    return summary


# ---------------------------------------------------------------------------
# payloads for the API
# ---------------------------------------------------------------------------

def market_payload():
    s = _srv()
    def board(key):
        try:
            return json.loads(s.kv_get(key, "{}") or "{}")
        except (TypeError, ValueError):
            return {}
    tracked = {}
    for ipo in s.rows("SELECT id, name, mkt_json, mkt_at FROM ipos WHERE mkt_json<>''"):
        try:
            tracked[ipo["id"]] = {"name": ipo["name"], "at": ipo["mkt_at"],
                                  **json.loads(ipo["mkt_json"])}
        except (TypeError, ValueError):
            continue
    last = json.loads(s.kv_get("arthan_last", "{}") or "{}")
    verify = json.loads(s.kv_get(VERIFY_KV, "{}") or "{}")
    watch = [{"label": w["label"],
              "found": json.loads(s.kv_get(f"arthan_watch_hit:{w['label']}", "null") or "null")}
             for w in _watch_list()]
    return {"gmp_board": board(GMP_BOARD_KV), "sub_board": board(SUB_BOARD_KV),
            "tracked": tracked, "last_refresh": last.get("at", ""),
            "last_summary": last.get("summary", {}), "verify": verify,
            "watch": watch}


def ipo_snapshot(iid):
    s = _srv()
    ipo = s.rows("SELECT * FROM ipos WHERE id=?", (iid,))
    if not ipo:
        return {"ok": False, "error": "unknown ipo"}
    ipo = ipo[0]
    mkt = {}
    try:
        mkt = json.loads(ipo.get("mkt_json") or "{}")
    except (TypeError, ValueError):
        pass
    hist = s.rows("SELECT day, gmp FROM gmp_hist WHERE ipo_id=? ORDER BY day DESC LIMIT 7", (iid,))
    legacy_sub = {}
    try:
        legacy_sub = json.loads(ipo.get("sub_json") or "{}")
    except (TypeError, ValueError):
        pass
    return {
        "ok": True,
        "ipo": {"id": ipo["id"], "name": ipo["name"], "board": ipo["board"],
                "open_date": ipo["open_date"], "close_date": ipo["close_date"],
                "allotment_date": ipo["allotment_date"], "listing_date": ipo["listing_date"],
                "price_min": ipo["price_min"], "price_max": ipo["price_max"],
                "lot_size": ipo["lot_size"], "status": s.ipo_status(ipo)},
        "market": mkt, "legacy_sub": legacy_sub, "gmp_history": hist,
        "allotment_time_hint": mkt.get("allotment_hint", "evening (usually after 6 PM)"),
    }


def ensure_default_watch():
    """Seed the watch-list on first boot (Davangere bank IPO the user asked for)."""
    if not _watch_list():
        add_watch(["davangere", "veeresh", "sahakara"], "Davangere Veeresh Sahakara Bank")
