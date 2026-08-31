#!/usr/bin/env python3
"""
IPO Command Center — multi-account Indian IPO tracker.
Single-file backend: FastAPI + SQLite.
Run:  python3 server.py   (then open http://localhost:8000)
"""

import base64
import hashlib
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import atexit
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

IST = timezone(timedelta(hours=5, minutes=30))


def ist_now():
    return datetime.now(IST)

import requests
from fastapi import FastAPI, HTTPException, Body, Response, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

BASE = Path(__file__).parent
DATA = BASE / "data"
DATA.mkdir(exist_ok=True)
DB = Path(os.environ.get("IPO_DB", str(DATA / "ipo.db")))

LOCK = threading.Lock()
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept-Language": "en-US,en;q=0.9"}

# ----------------------------------------------------------------------------
# DB
# ----------------------------------------------------------------------------

def get_db():
    con = sqlite3.connect(DB, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    with LOCK, get_db() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS accounts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            holder TEXT NOT NULL,
            pan TEXT DEFAULT '',
            cdsl TEXT DEFAULT '',
            broker TEXT DEFAULT '',
            bank TEXT DEFAULT '',
            upi TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS ipos(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            registrar TEXT DEFAULT 'Other',
            registrar_ref TEXT DEFAULT '',
            open_date TEXT DEFAULT '',
            close_date TEXT DEFAULT '',
            price_min REAL DEFAULT 0,
            price_max REAL DEFAULT 0,
            lot_size INTEGER DEFAULT 0,
            allotment_date TEXT DEFAULT '',
            listing_date TEXT DEFAULT '',
            board TEXT DEFAULT 'Mainboard',
            symbol TEXT DEFAULT '',
            cmp REAL DEFAULT 0,
            gmp REAL DEFAULT 0,
            source TEXT DEFAULT 'manual',
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS applications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ipo_id INTEGER NOT NULL REFERENCES ipos(id) ON DELETE CASCADE,
            account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            applied INTEGER DEFAULT 1,
            app_no TEXT DEFAULT '',
            lots INTEGER DEFAULT 1,
            category TEXT DEFAULT 'Retail',
            amount REAL DEFAULT 0,
            upi TEXT DEFAULT '',
            mandate_status TEXT DEFAULT 'pending',     -- pending approved rejected expired
            allotment TEXT DEFAULT 'pending',          -- pending allotted not_allotted
            allotted_qty INTEGER DEFAULT 0,
            refund TEXT DEFAULT 'na',                  -- na pending received
            sell_qty INTEGER DEFAULT 0,
            sell_price REAL DEFAULT 0,
            sold_on TEXT DEFAULT '',
            checked_note TEXT DEFAULT '',
            updated_at TEXT DEFAULT (datetime('now','localtime')),
            UNIQUE(ipo_id, account_id)
        );
        CREATE INDEX IF NOT EXISTS idx_app_ipo ON applications(ipo_id);
        CREATE TABLE IF NOT EXISTS push_subs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            endpoint TEXT UNIQUE,
            sub_json TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS kv(k TEXT PRIMARY KEY, v TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS gmp_hist(
            ipo_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            gmp REAL DEFAULT 0,
            PRIMARY KEY (ipo_id, day)
        );
        """)
init_db()


def migrate():
    with LOCK, get_db() as con:
        for ddl in ("ALTER TABLE accounts ADD COLUMN auth_mode TEXT DEFAULT ''",
                    "ALTER TABLE ipos ADD COLUMN cmp_at TEXT DEFAULT ''",
                    "ALTER TABLE ipos ADD COLUMN sub_json TEXT DEFAULT ''",
                    "ALTER TABLE ipos ADD COLUMN sub_at TEXT DEFAULT ''",
                    "ALTER TABLE ipos ADD COLUMN mkt_json TEXT DEFAULT ''",
                    "ALTER TABLE ipos ADD COLUMN mkt_at TEXT DEFAULT ''",
                    "ALTER TABLE ipos ADD COLUMN gmp_feed REAL DEFAULT 0",
                    "ALTER TABLE accounts ADD COLUMN sort INTEGER DEFAULT 0"):
            try:
                con.execute(ddl)
            except sqlite3.OperationalError:
                pass
        con.execute("UPDATE accounts SET sort=id*10 WHERE sort=0")   # one-time backfill
        con.commit()


migrate()

import arthan  # noqa: E402 — market-intelligence engine (self-verifying IPO data)


# ----------------------------------------------------------------------------
# backup / restore
# Render free tier has an EPHEMERAL filesystem: the SQLite file vanishes on
# redeploys/restarts. So: (1) every change triggers a debounced backup saved
# locally AND pushed to the user's private GitHub repo (when GITHUB_TOKEN and
# GITHUB_REPO env vars are set); (2) on boot with an empty DB we auto-restore
# from GitHub, then local file, then the seed backup that ships in the repo.
# ----------------------------------------------------------------------------

BACKUP_TABLES = ("accounts", "ipos", "applications", "push_subs", "kv")
# mirror paths follow the DB location (identical in production — DB lives in
# DATA — but a test box pointing IPO_DB elsewhere no longer inherits the
# production mirror file as its "local backup" restore source)
LOCAL_BACKUP = DB.parent / "live-backup.json"
SEED_BACKUP = DB.parent / "seed-backup.json"
_GH_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
_GH_REPO = os.environ.get("GITHUB_REPO", "").strip()  # "username/repo"
_GH_PATH = "data/live-backup.json"
_GH_BRANCH = "data-backup"  # backups live on this branch so Render's builds (main) never see them —
                            # every backup commit on main used to burn paid pipeline minutes AND
                            # restart the live app mid-session (the cause of the table-rollback bug)
_branch_state = {"checked": False, "ok": False}
_bak_timer = None
_bak_pending = False
_bak_last_ok = ""  # IST timestamp of the last write that became durable somewhere
                   # (GitHub push, or at least the on-disk local copy). Surfaced
                   # in /api/state so the phone can show a visible "saved" pulse —
                   # if a tap doesn't move this clock, the user can SEE it.


def _mark_backup_ok():
    global _bak_last_ok
    _bak_last_ok = ist_now().isoformat(timespec="seconds")


def export_backup() -> dict:
    with LOCK, get_db() as con:
        tables = {t: [dict(r) for r in con.execute(f"SELECT * FROM {t} ORDER BY rowid")]
                  for t in BACKUP_TABLES}
    return {"version": 1, "app": "ipo-command-center",
            "exported_at": ist_now().isoformat(timespec="seconds"),
            "tables": tables}


def restore_backup(payload: dict) -> dict:
    tables = (payload or {}).get("tables") or {}
    if not any(t in tables for t in BACKUP_TABLES):
        raise ValueError("not an IPO Command Center backup file")
    counts = {}
    with LOCK, get_db() as con:
        for t in BACKUP_TABLES:
            rows = tables.get(t) or []
            con.execute(f"DELETE FROM {t}")
            if rows:
                # column list = UNION of keys across all rows, in first-seen
                # order. (Old code trusted only row #1's keys: a backup whose
                # first row was emitted before later schema growth silently
                # DROPPED newer columns for every row — sell history could
                # have landed in the wrong field if column order drifted.)
                cols = []
                for r in rows:
                    for k in r.keys():
                        if k not in cols:
                            cols.append(k)
                q = f"INSERT INTO {t} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})"
                con.executemany(q, [[r.get(c) for c in cols] for r in rows])
            counts[t] = len(rows)
    return counts


def _gh_headers():
    return {"Authorization": f"Bearer {_GH_TOKEN}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "ipo-command-center"}


def _gh_branch_ready():
    """Ensure the backup branch exists (checked once per process). If the branch
    can't be verified/created, fall back to pushing to main — the data matters
    more than the deploy noise."""
    if _branch_state["checked"]:
        return _branch_state["ok"]
    _branch_state["checked"] = True
    try:
        base = f"https://api.github.com/repos/{_GH_REPO}/git"
        r = requests.get(f"{base}/ref/heads/{_GH_BRANCH}", headers=_gh_headers(), timeout=15)
        if r.status_code == 200:
            _branch_state["ok"] = True
            return True
        m = requests.get(f"{base}/ref/heads/main", headers=_gh_headers(), timeout=15)
        m.raise_for_status()
        main_sha = m.json()["object"]["sha"]
        c = requests.post(f"{base}/refs", headers=_gh_headers(), timeout=15,
                          json={"ref": f"refs/heads/{_GH_BRANCH}", "sha": main_sha})
        _branch_state["ok"] = c.status_code in (201, 422)  # 422 = race: already exists
        if not _branch_state["ok"]:
            print(f"[backup] branch create returned {c.status_code} (using main)", flush=True)
    except Exception as e:
        print("[backup] backup-branch check failed (using main):", e, flush=True)
        _branch_state["ok"] = False
    return _branch_state["ok"]


def _github_push(content: str):
    br = _GH_BRANCH if _gh_branch_ready() else "main"
    api = f"https://api.github.com/repos/{_GH_REPO}/contents/{_GH_PATH}"
    sha = None
    r = requests.get(api, headers=_gh_headers(), params={"ref": br}, timeout=15)
    if r.status_code == 200:
        sha = r.json().get("sha")
    body = {"message": "auto-backup (data branch, no deploy)", "branch": br,
            "content": base64.b64encode(content.encode()).decode()}
    if sha:
        body["sha"] = sha
    requests.put(api, headers=_gh_headers(), json=body, timeout=20).raise_for_status()


def _github_fetch():
    if not (_GH_TOKEN and _GH_REPO):
        return None
    api = f"https://api.github.com/repos/{_GH_REPO}/contents/{_GH_PATH}"
    # newest writes live on the data-backup branch; backups written by older
    # app versions only ever existed on main — read branch first, fall back so
    # nothing gets stranded
    best = None
    for params in ({"ref": _GH_BRANCH}, {}):
        # the branch is where new writes go, but a freshly-created branch can
        # momentarily hold an OLDER copy than main — always compare exported_at
        # and restore the newest valid payload
        try:
            r = requests.get(api, headers=_gh_headers(), params=params, timeout=15)
            if r.status_code != 200:
                continue
            d = json.loads(base64.b64decode(r.json()["content"]).decode())
            if not (isinstance(d, dict) and d.get("tables")):
                print(f"[backup] payload on ref {params.get('ref', 'main')} is not a backup — trying next", flush=True)
                continue
            if best is None or str(d.get("exported_at", "")) > str(best.get("exported_at", "")):
                best = d
        except Exception as e:
            print("[backup] GitHub fetch failed:", e, flush=True)
    return best


def _do_backup():
    global _bak_pending
    try:
        payload = json.dumps(export_backup(), ensure_ascii=False)
        try:
            LOCAL_BACKUP.write_text(payload, encoding="utf-8")
            _mark_backup_ok()
        except Exception:
            pass
        if _GH_TOKEN and _GH_REPO:
            try:
                _github_push(payload)
                _bak_pending = False
                _mark_backup_ok()
                print("[backup] pushed to GitHub", flush=True)
            except Exception as e:
                print("[backup] GitHub push failed (local copy kept):", e, flush=True)
    except Exception as e:
        print("[backup] failed:", e, flush=True)


def schedule_backup():
    global _bak_timer, _bak_pending
    _bak_pending = True
    if _bak_timer and _bak_timer.is_alive():
        return
    _bak_timer = threading.Timer(25.0, _do_backup)
    _bak_timer.daemon = True
    _bak_timer.start()


def _flush_backup_on_exit():
    """Container hosts (Render etc.) send SIGTERM before killing an instance.
    Uvicorn shuts down cleanly on SIGTERM, so this atexit hook runs — flush
    any not-yet-pushed changes so the restart can't silently roll them back
    (this is the last gap a plain 25 s debounce leaves open)."""
    global _bak_pending
    try:
        if _bak_pending and _GH_TOKEN and _GH_REPO:
            _github_push(json.dumps(export_backup(), ensure_ascii=False))
            _bak_pending = False
            _mark_backup_ok()
    except Exception:
        pass


atexit.register(_flush_backup_on_exit)


def backup_now():
    """Immediate (synchronous) backup — for sensitive one-time config changes
    (passcode, fingerprint credential) where even the normal ~25 s debounce
    window is too long: a free-host restart inside that window would silently
    roll the change back once the older backup is restored."""
    try:
        payload = json.dumps(export_backup(), ensure_ascii=False)
        try:
            LOCAL_BACKUP.write_text(payload, encoding="utf-8")
            _mark_backup_ok()
        except Exception:
            pass
        if _GH_TOKEN and _GH_REPO:
            try:
                _github_push(payload)
                _mark_backup_ok()
            except Exception as e:
                print("[backup] instant GitHub push failed (local copy kept):", e, flush=True)
    except Exception as e:
        print("[backup] instant backup failed:", e, flush=True)


def boot_restore():
    with LOCK, get_db() as con:
        n = con.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()["c"]
    if n:
        return
    candidates = [("GitHub", _github_fetch())]
    for path, label in ((LOCAL_BACKUP, "local backup"), (SEED_BACKUP, "seed backup")):
        try:
            if path.exists():
                candidates.append((label, json.loads(path.read_text(encoding="utf-8"))))
        except Exception:
            pass
    # restore the NEWEST payload available, never just the first working one —
    # an older restore that later gets auto-pushed would silently roll newer
    # data back (this is how a passcode change once got wiped).

    def _exported_at(item):
        try:
            return (item[1] or {}).get("exported_at") or ""
        except Exception:
            return ""
    candidates = sorted((c for c in candidates if c[1]), key=_exported_at, reverse=True)
    for label, payload in candidates:
        try:
            counts = restore_backup(payload)
            # seed the visible save pulse with the restored backup's own clock —
            # otherwise the header shows "💾 —" until this boot's first write
            global _bak_last_ok
            _bak_last_ok = (payload.get("exported_at") or "")
            print(f"[backup] restored from {label} (export {(payload.get('exported_at') or '?')[:19]}): {counts}",
                  flush=True)
            return
        except Exception as e:
            print(f"[backup] restore from {label} failed: {e}", flush=True)
    print("[backup] empty DB and no backup found — starting fresh", flush=True)


boot_restore()


# ----------------------------------------------------------------------------
# Web Push (VAPID) — real notifications on the phone even when the app is closed
# ----------------------------------------------------------------------------

# VAPID keys: (1) env vars VAPID_PUB / VAPID_PRIV win; (2) else a pair is
# generated once and persisted in kv (survives restarts); (3) in-repo default
# only as a last-resort fallback so config-less boots still run.

def _vapid_keys():
    pub, priv = os.environ.get("VAPID_PUB", ""), os.environ.get("VAPID_PRIV", "")
    if pub and priv:
        return pub.strip(), priv.strip()
    if _BAKED_VAPID_PUB and _BAKED_VAPID_PRIV:
        return _BAKED_VAPID_PUB, _BAKED_VAPID_PRIV    # existing installs: stable keys
    try:
        cached = kv_get("vapid_keys")
        if cached:
            j = json.loads(cached)
            if j.get("pub") and j.get("priv"):
                return j["pub"], j["priv"]            # fresh installs: generated once, persisted
    except Exception:
        pass
    try:
        from py_vapid import Vapid
        import base64
        v = Vapid()
        v.generate_keys()
        raw = v.public_key.public_numbers()
        x = raw.x.to_bytes(32, "big"); y = raw.y.to_bytes(32, "big")
        pub = base64.urlsafe_b64encode(b"\x04" + x + y).rstrip(b"=").decode()
        priv_val = v.private_key.private_numbers().private_value
        priv = base64.urlsafe_b64encode(priv_val.to_bytes(32, "big")).rstrip(b"=").decode()
        kv_set("vapid_keys", json.dumps({"pub": pub, "priv": priv}))
        print("[push] generated fresh VAPID keypair for this install", flush=True)
        return pub, priv
    except Exception as e:
        print("[push] VAPID generation failed:", e, flush=True)
        return "", ""

_BAKED_VAPID_PUB = ""
_BAKED_VAPID_PRIV = ""
VAPID_PUB, VAPID_PRIV = "", ""          # resolved lazily below
VAPID_SUB = "mailto:ipo-command-center@localhost"


def vapid_keys():
    global VAPID_PUB, VAPID_PRIV
    if not VAPID_PUB:
        VAPID_PUB, VAPID_PRIV = _vapid_keys()
    return VAPID_PUB, VAPID_PRIV


def send_push(title: str, body: str = "", url: str = "/"):
    """Fire a push notification to every subscribed device. Never raises."""
    try:
        from pywebpush import webpush
    except ImportError:
        print("[push] pywebpush not installed — skipping", flush=True)
        return
    subs = rows("SELECT * FROM push_subs")
    dead = []
    for s in subs:
        try:
            webpush(subscription_info=json.loads(s["sub_json"]),
                    data=json.dumps({"title": title, "body": body, "url": url}),
                    vapid_private_key=vapid_keys()[1],
                    vapid_claims={"sub": VAPID_SUB}, timeout=12)
        except Exception as e:
            print(f"[push] failed ({str(e)[:70]})", flush=True)
            if "410" in str(e) or "404" in str(e):
                dead.append(s["id"])
    for i in dead:
        run("DELETE FROM push_subs WHERE id=?", (i,))
    if subs:
        print(f"[push] sent to {len(subs) - len(dead)}/{len(subs)} devices: {title[:40]}", flush=True)

# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def rows(sql, args=()):
    with LOCK, get_db() as con:
        cur = con.execute(sql, args)
        return [dict(r) for r in cur.fetchall()]


def run(sql, args=()):
    with LOCK, get_db() as con:
        cur = con.execute(sql, args)
        con.commit()
        return cur.lastrowid


def norm_name(s: str) -> str:
    s = s.upper()
    s = re.sub(r"\b(LIMITED|LTD|IPO|INDIA|IND)\b\.?", "", s)
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s


# --- input coercion: garbage in a numeric/date field used to explode as a
#     500 (also a stored-self-XSS path, since raw values were echoed back into
#     HTML inputs). Reject cleanly with 400 instead. --------------------------
def _f(v, field="number", lo=None, hi=None):
    try:
        x = float(v)
    except (TypeError, ValueError):
        raise HTTPException(400, f"{field} must be a number")
    if x != x or x in (float("inf"), float("-inf")):
        raise HTTPException(400, f"{field} must be a finite number")
    if lo is not None and x < lo:
        raise HTTPException(400, f"{field} looks too small")
    if hi is not None and x > hi:
        raise HTTPException(400, f"{field} looks too large")
    return x


def _i(v, field="number", lo=None, hi=None):
    x = _f(v, field, lo, hi)
    if x != int(x):
        raise HTTPException(400, f"{field} must be a whole number")
    return int(x)


_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def _dt(v, field="date"):
    v = (str(v) if v is not None else "").strip()
    if not v:
        return ""
    if not _DATE_RE.fullmatch(v):
        raise HTTPException(400, f"{field} must be YYYY-MM-DD")
    try:
        date.fromisoformat(v)
    except ValueError:
        raise HTTPException(400, f"{field} is not a real calendar date")
    return v


def today_str():
    return ist_now().date().isoformat()


def ipo_status(ipo: dict) -> str:
    t = today_str()
    od, cd, ld = ipo.get("open_date") or "", ipo.get("close_date") or "", ipo.get("listing_date") or ""
    if ld and t >= ld:
        return "listed"
    if od and t < od:
        return "upcoming"
    if cd and od <= t <= cd:
        return "open"
    if cd and t > cd:
        # closed — results are "out" once ANY applied account has a verdict;
        # rows left pending are review-only (missing PAN/BO ID) and must not
        # keep the whole IPO in RESULT AWAITED forever.
        decided = rows("""SELECT COUNT(*) c FROM applications
                          WHERE ipo_id=? AND applied=1 AND allotment IN ('allotted','not_allotted')""",
                       (ipo["id"],))
        if decided and decided[0]["c"]:
            return "allotment_done"
        # no verdicts tracked here at all (e.g. an IPO you never applied in):
        # the result PHASE still ends on allotment day — the chip must not say
        # "awaited" for a month after results are public.
        ad = ipo.get("allotment_date") or cd
        return "allotment_done" if t > ad else "result_pending"
    return "open" if od else "upcoming"


REGISTRAR_LINKS = {
    "Link Intime": "https://in.mpms.mufg.com/Initial_Offer/public-issues.html",
    "KFin": "https://ipostatus.kfintech.com/",
    "Bigshare": "https://ipo.bigshareonline.com/ipo_status.html",
    "Other": "",
}

# ----------------------------------------------------------------------------
# Registrar allotment engines (best-effort, graceful fallbacks)
# ----------------------------------------------------------------------------

_cache = {"bigshare_companies": (0, []), "kfin_companies": (0, []), "mufg_companies": (0, [])}

# --- MUFG Intime (prev Link Intime) — verified 2026-08-14 ----------------------
MUFG_BASE = "https://in.mpms.mufg.com/Initial_Offer/IPO.aspx/"

# --- KFin (ipostatus.kfintech.com) — verified 2026-08-14 ----------------------
KFIN_API = "https://0uz601ms56.execute-api.ap-south-1.amazonaws.com/prod/api/query?type="
KFIN_FALLBACK = [{"id": "81387868980", "name": "MOLBIO DIAGNOSTICS LIMITED"},
                 {"id": "94818267561", "name": "DHOOT TRANSMISSION LIMITED"}]


def kfin_companies(force=False):
    ts, comps = _cache["kfin_companies"]
    if not force and comps and time.time() - ts < 1800:
        return comps
    # the SPA embeds its IPO dropdown as JSON inside its JS bundle
    r = requests.get("https://ipostatus.kfintech.com/", headers=UA, timeout=15)
    m = re.search(r'src="\.?(/static/js/main\.[0-9a-f]+\.js)"', r.text)
    if not m:
        raise ValueError("KFin bundle not found")
    r = requests.get("https://ipostatus.kfintech.com" + m.group(1), headers=UA, timeout=30)
    raw = None
    for m in re.finditer(r"JSON\.parse\('((?:[^'\\]|\\.)*)'\)", r.text):
        if "clientId" in m.group(1):
            raw = m.group(1); break
    if not raw:
        raise ValueError("KFin company list not found in bundle")
    data = json.loads(raw.encode().decode("unicode_escape"))
    comps = [{"id": str(c["clientId"]), "name": c["name"]} for c in data]
    merged = {c["id"]: c for c in (comps + KFIN_FALLBACK)}
    out = list(merged.values())
    _cache["kfin_companies"] = (time.time(), out)
    print(f"[kfin] {len(out)} companies loaded", flush=True)
    return out

RESULT_TEMPLATES = {
    "ok": "ok", "not_found": "not_found", "manual": "manual", "error": "error",
}


def bigshare_companies(force=False):
    ts, comps = _cache["bigshare_companies"]
    if not force and comps and time.time() - ts < 1800:
        return comps
    r = requests.get("https://ipo.bigshareonline.com/", headers=UA, timeout=15)
    r.raise_for_status()
    m = re.search(r'<select id="ddlCompany">(.*?)</select>', r.text, re.S)
    comps = []
    if m:
        for val, name in re.findall(r'<option value="(\d+)">([^<]+)</option>', m.group(1)):
            comps.append({"id": val, "name": name.strip()})
    _cache["bigshare_companies"] = (time.time(), comps)
    return comps




def _bigshare_captcha_blocked(iid: int, mark: bool = False) -> bool:
    """Bigshare's status form is captcha-gated (CaptchaToken/CaptchaAnswer +
    ResultToken in its POST) — no app can auto-read it. Remember per-IPO so the
    auto-sweeps stop burning 15 requests × 48×/day into a wall, while manual
    taps still get ONE honest attempt."""
    try:
        j = json.loads(kv_get("bigshare_captcha", "{}") or "{}")
    except (TypeError, ValueError):
        j = {}
    if mark:
        j[str(iid)] = today_str()
        kv_set("bigshare_captcha", json.dumps(j))
    return str(iid) in j


def _bigshare_captcha_result(ipo):
    return {"status": "captcha",
            "note": "Bigshare asks a captcha before every search — no app can auto-read it. "
                    "Open the registrar page, check each PAN, then tap the Allotment cell of that row here.",
            "link": REGISTRAR_LINKS["Bigshare"], "matched_company": ipo.get("name", "")}

def check_bigshare(ipo, acc, force=False):
    """Return dict(status, allotted_qty, note, matched_company)."""
    if _bigshare_captcha_blocked(ipo.get("id")):
        return _bigshare_captcha_result(ipo)
    try:
        comps = bigshare_companies(force=force)
    except Exception as e:
        _bigshare_captcha_blocked(ipo.get("id"), mark=True)
        print(f"[bigshare] page probe failed, treating as captcha-walled: {e}", flush=True)
        return _bigshare_captcha_result(ipo)
    target = norm_name(ipo["name"])
    match = None
    if ipo.get("registrar_ref"):
        match = next((c for c in comps if c["id"] == str(ipo["registrar_ref"]).strip()), None)
    if not match:
        # exact-normalised then substring match
        for c in comps:
            if norm_name(c["name"]) == target:
                match = c; break
        if not match:
            for c in comps:
                n = norm_name(c["name"])
                if target and (target in n or n in target):
                    match = c; break
    if not match:
        return {"status": "manual", "note": "IPO not found in Bigshare live list (allotment not out yet, or different registrar?)",
                "link": REGISTRAR_LINKS["Bigshare"]}

    pan = (acc.get("pan") or "").strip().upper()
    cdsl = (acc.get("cdsl") or "").strip()
    if not pan and not cdsl:
        return {"status": "manual", "note": "No PAN or CDSL BO ID stored for this account", "link": REGISTRAR_LINKS["Bigshare"]}

    payload = {"Applicationno": (acc.get("app_no") or "").strip(),
               "Company": match["id"],
               "SelectionType": "PN" if pan else "BN",
               "PanNo": pan,
               "txtcsdl": cdsl, "txtDPID": "", "txtClId": "",
               "ddlType": "", "lang": "en"}
    try:
        r = requests.post("https://ipo.bigshareonline.com/Data.aspx/FetchIpodetails",
                          json=payload, timeout=20,
                          headers={**UA, "Content-Type": "application/json; charset=utf-8",
                                   "X-Requested-With": "XMLHttpRequest"})
        r.raise_for_status()
        d = r.json().get("d")
    except Exception as e:
        blob_err = str(e).lower()
        if "captcha" in blob_err or "500" in blob_err or "error" in blob_err:
            _bigshare_captcha_blocked(ipo.get("id"), mark=True)
            print(f"[bigshare] API rejected without captcha ({e}) — flagging captcha-walled", flush=True)
            return _bigshare_captcha_result(ipo)
        return {"status": "manual", "note": f"Bigshare API error ({e})", "link": REGISTRAR_LINKS["Bigshare"]}

    b0 = json.dumps(d).lower()
    if "captcha" in b0 or "invalid" in b0:
        _bigshare_captcha_blocked(ipo.get("id"), mark=True)
        print("[bigshare] response demands captcha — flagging captcha-walled", flush=True)
        return _bigshare_captcha_result(ipo)
    if isinstance(d, str):
        d = {"raw": d}
    if not isinstance(d, dict):
        _bigshare_captcha_blocked(ipo.get("id"), mark=True)
        return _bigshare_captcha_result(ipo)

    blob = json.dumps(d)
    if "No data found" in blob:
        return {"status": "not_found", "note": f"No record for this PAN/BO ID at {match['name']} — usually means NOT allotted (confirm once)", "matched_company": match["name"]}

    # try to dig out allotted shares count from known field names
    qty = 0
    for k, v in d.items():
        kl = str(k).lower()
        if any(w in kl for w in ("allot", "share", "qty", "quantity")):
            nums = re.findall(r"\d+", str(v).replace(",", ""))
            if nums:
                qty = max(qty, max(int(x) for x in nums))
    if qty > 0:
        return {"status": "ok", "allotted_qty": qty, "note": f"Allotted {qty} shares", "matched_company": match["name"]}
    # record exists but couldn't parse qty — treat as allotted if any identifying field came back
    if any(str(v).strip() for v in d.values() if isinstance(v, str) and "not" not in str(v).lower()[:6]):
        return {"status": "manual", "note": "Record found but shares unclear — verify once: " + blob[:160], "matched_company": match["name"], "link": REGISTRAR_LINKS["Bigshare"]}
    return {"status": "not_found", "note": "Empty record — likely not allotted", "matched_company": match["name"]}


def check_kfintech(ipo, acc, force=False):
    """KFin's ipostatus site (React SPA) calls an open AWS API-Gateway:
       GET .../api/query?type=pan|dpclid   headers: reqparam=<PAN|BOID>, client_id=<ipo id>
       200 -> [{Appln_No, Name, DP_CLID, Pan_No, App_Shares, All_Shares}, ...]
       404 -> {"error":"Record Not Found"}  (not allotted / not out yet)
       The company->clientId map is embedded in the site's JS bundle; we scrape
       and cache it, with a baked-in fallback for known IDs."""
    pan = (acc.get("pan") or "").strip().upper()
    cdsl = re.sub(r"\D", "", acc.get("cdsl") or "")
    if not pan and not cdsl:
        return {"status": "manual", "note": "No PAN or CDSL BO ID stored for this account",
                "link": REGISTRAR_LINKS["KFin"]}
    try:
        comps = kfin_companies(force=force)
    except Exception as e:
        return {"status": "manual", "note": f"KFin site unreachable ({e})", "link": REGISTRAR_LINKS["KFin"]}

    target = norm_name(ipo["name"])
    match = None
    if ipo.get("registrar_ref"):
        match = next((c for c in comps if c["id"] == str(ipo["registrar_ref"]).strip()), None)
    if not match:
        for c in comps:
            if norm_name(c["name"]) == target:
                match = c; break
        if not match:
            for c in comps:
                n = norm_name(c["name"])
                if target and (target in n or n in target):
                    match = c; break
    if not match:
        return {"status": "manual",
                "note": "IPO not in KFin live list yet (allotment not published, or different registrar?)",
                "link": REGISTRAR_LINKS["KFin"]}

    if pan:
        qtype, reqparam = "pan", pan
    else:
        if len(cdsl) != 16:
            return {"status": "manual",
                    "note": f"CDSL BO ID should be exactly 16 digits (stored value has {len(cdsl)}) — fix it in Accounts",
                    "link": REGISTRAR_LINKS["KFin"]}
        qtype, reqparam = "dpclid", cdsl
    hdr = {**UA, "reqparam": reqparam, "client_id": str(match["id"]),
           "Origin": "https://ipostatus.kfintech.com", "Referer": "https://ipostatus.kfintech.com/"}
    r = None
    for attempt in (1, 2):
        try:
            r = requests.get(KFIN_API + qtype, headers=hdr, timeout=15)
            if r.status_code in (429, 500, 502, 503, 504) and attempt == 1:
                time.sleep(2.5); continue
            break
        except requests.RequestException as e:
            if attempt == 2:
                return {"status": "manual", "note": f"KFin API error ({e})", "link": REGISTRAR_LINKS["KFin"]}
            time.sleep(2.5)
    if r is None:
        return {"status": "manual", "note": "KFin API no response", "link": REGISTRAR_LINKS["KFin"]}

    if r.status_code == 404:
        # KFin shows All_Shares=0 records for genuine non-allotments, so a 404
        # means "no application under this identifier" — needs human review
        # (PAN typo, or the application never reached the registrar).
        return {"status": "manual",
                "note": f"KFin has NO record under this PAN/BO ID at {match['name']} — check the PAN/BO ID is typed right; if correct, verify the application in Groww",
                "matched_company": match["name"], "link": REGISTRAR_LINKS["KFin"]}
    if r.status_code != 200:
        return {"status": "manual", "note": f"KFin API HTTP {r.status_code} — use one-click link",
                "link": REGISTRAR_LINKS["KFin"]}
    try:
        data = r.json()
    except ValueError:
        return {"status": "manual", "note": "KFin returned non-JSON — verify: " + r.text[:140],
                "link": REGISTRAR_LINKS["KFin"]}
    if isinstance(data, dict):
        # real payload: {"data": [ {All_Shares, App_Shares, Appln_No, ...}, ... ]}
        data = data.get("data") if isinstance(data.get("data"), list) else [data]
    if not isinstance(data, list) or not data:
        return {"status": "not_found", "note": f"KFin: empty result — likely not allotted",
                "matched_company": match["name"]}
    qty = 0
    applied = 0
    for rec in data:
        try:
            qty += int(str(rec.get("All_Shares") or "0").replace(",", "") or 0)
        except (TypeError, ValueError):
            pass
        try:
            applied += int(str(rec.get("App_Shares") or "0").replace(",", "") or 0)
        except (TypeError, ValueError):
            pass
    if qty > 0:
        return {"status": "ok", "allotted_qty": qty,
                "note": f"KFin: ALLOTTED {qty} shares (applied {applied or '?'}, {len(data)} record(s))",
                "matched_company": match["name"]}
    return {"status": "not_found",
            "note": f"KFin: applied {applied or '?'} shares, allotted 0 — NOT allotted ({match['name']})",
            "matched_company": match["name"]}


def mufg_companies(force=False):
    ts, comps = _cache["mufg_companies"]
    if not force and comps and time.time() - ts < 1800:
        return comps
    r = requests.post(MUFG_BASE + "GetDetails", json={}, headers=UA, timeout=20)
    d = r.json().get("d", "")
    rows = re.findall(r"<company_id>\s*(\d+)\s*</company_id>\s*<companyname>\s*([^<]+?)\s*</companyname>", d, re.I)
    if not rows:
        raise ValueError("MUFG company list empty")
    comps = [{"id": i, "name": n.replace(" - IPO", "")} for i, n in rows]
    _cache["mufg_companies"] = (time.time(), comps)
    print(f"[mufg] {len(comps)} companies loaded", flush=True)
    return comps


def check_linkintime(ipo, acc, force=False):
    """MUFG Intime (prev Link Intime) — new portal does NOT block servers.
       Chain: POST GetDetails (company list) -> generateToken -> SearchOnPan
       {clientid, PAN, IFSC:'', CHKVAL:'1', token} ; response d = XML rows with
       <ALLOT> (allotted qty) and <SHARES> (applied). Empty dataset = no record."""
    pan = (acc.get("pan") or "").strip().upper()
    if not pan:
        return {"status": "manual", "note": "MUFG auto-check needs PAN (only BO ID stored) — use one-click link",
                "link": REGISTRAR_LINKS["Link Intime"]}
    try:
        comps = mufg_companies(force=force)
    except Exception as e:
        return {"status": "manual", "note": f"MUFG portal unreachable ({e})", "link": REGISTRAR_LINKS["Link Intime"]}
    target = norm_name(ipo["name"])
    match = None
    if ipo.get("registrar_ref"):
        match = next((c for c in comps if c["id"] == str(ipo["registrar_ref"]).strip()), None)
    if not match:
        for c in comps:
            if norm_name(c["name"]) == target:
                match = c; break
        if not match:
            for c in comps:
                n = norm_name(c["name"])
                if target and (target in n or n in target):
                    match = c; break
    if not match:
        return {"status": "manual", "note": "IPO not in MUFG live list yet (allotment not published?)",
                "link": REGISTRAR_LINKS["Link Intime"]}
    try:
        tok = requests.post(MUFG_BASE + "generateToken", json={}, headers=UA, timeout=15).json()["d"]
        payload = {"clientid": match["id"], "PAN": pan, "IFSC": "", "CHKVAL": "1", "token": tok}
        r = requests.post(MUFG_BASE + "SearchOnPan", data=json.dumps(payload),
                          headers={**UA, "Content-Type": "application/json; charset=utf-8"}, timeout=20)
        x = r.json().get("d", "")
    except Exception as e:
        return {"status": "manual", "note": f"MUFG API error ({e})", "link": REGISTRAR_LINKS["Link Intime"]}
    rows_xml = re.findall(r"<Table>(.*?)</Table>", x, re.S)
    if not rows_xml:
        return {"status": "manual",
                "note": f"MUFG has NO record under this PAN at {match['name']} — check PAN typed right; if correct, verify the application in Groww",
                "matched_company": match["name"], "link": REGISTRAR_LINKS["Link Intime"]}
    allot = sum(int(v) for row in rows_xml
                for t, v in re.findall(r"<(ALLOT)>\s*(\d+)\s*</\1>", row))
    applied = sum(int(v) for row in rows_xml
                  for t, v in re.findall(r"<(SHARES)>\s*(\d+)\s*</\1>", row))
    if allot > 0:
        return {"status": "ok", "allotted_qty": allot,
                "note": f"MUFG: ALLOTTED {allot} shares (applied {applied or '?'}, {len(rows_xml)} record(s))",
                "matched_company": match["name"]}
    return {"status": "not_found",
            "note": f"MUFG: applied {applied or '?'} shares, allotted 0 — NOT allotted ({match['name']})",
            "matched_company": match["name"]}


ENGINES = {"Bigshare": check_bigshare, "KFin": check_kfintech, "Link Intime": check_linkintime}


# ----------------------------------------------------------------------------
# Live IPO list scraping (InvestorGain + Chittorgarh, best-effort merge)
# ----------------------------------------------------------------------------

MONTH_MAP = {m.lower(): i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}


def _clean(x):
    x = re.sub(r"<[^>]+>", " ", x)
    x = x.replace("&#8377;", "₹").replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", x).strip()


def _mkdate(y, m, d):
    try:
        return date(int(y), int(m), int(d)).isoformat()
    except ValueError:
        return ""


def _parse_issue_dates(txt):
    """'11 to 13 Aug, 2026' / '28 Aug to 1 Sep, 2026' / 'Aug 28 – Sep 01, 2026' -> (open, close).
    Cross-month ranges used to slip past the range regex and hit the single-day
    fallback, returning (close, close) — ESDS/Priority/Purple showed a 1-day
    "window" and stayed 'upcoming' through their real open day."""
    SEP = r"(?:to|-|–|—|till|until)"
    ORD = r"(?:st|nd|rd|th)?"
    # day-first cross-month: 28 Aug to 1 Sep, 2026 | 28th Aug - 1st Sep
    m = re.search(rf"(\d{{1,2}}){ORD}\s+([A-Za-z]{{3}})\w*\s*{SEP}\s*(\d{{1,2}}){ORD}\s+([A-Za-z]{{3}})\w*[\s,]+(\d{{4}})", txt)
    if m:
        d1, mo1, d2, mo2, y = int(m.group(1)), m.group(2), int(m.group(3)), m.group(4), m.group(5)
        return _mkdate(y, MONTH_MAP.get(mo1[:3].lower()), d1), _mkdate(y, MONTH_MAP.get(mo2[:3].lower()), d2)
    # month-first cross-month: Aug 28 to Sep 1, 2026
    m = re.search(rf"([A-Za-z]{{3}})\w*\s+(\d{{1,2}}){ORD}\s*{SEP}\s*([A-Za-z]{{3}})\w*\s+(\d{{1,2}}){ORD}[\s,]+(\d{{4}})", txt)
    if m:
        mo1, d1, mo2, d2, y = m.group(1), int(m.group(2)), m.group(3), int(m.group(4)), m.group(5)
        return _mkdate(y, MONTH_MAP.get(mo1[:3].lower()), d1), _mkdate(y, MONTH_MAP.get(mo2[:3].lower()), d2)
    # same month: 11 to 13 Aug, 2026
    m = re.search(rf"(\d{{1,2}})\s*{SEP}\s*(\d{{1,2}})\s+([A-Za-z]{{3}})\w*[\s,]+(\d{{4}})", txt)
    if not m:
        m = re.search(r"(\d{1,2})\s+([A-Za-z]{3})\w*[\s,]+(\d{4})", txt)  # single day
        if m:
            d, mo, y = int(m.group(1)), MONTH_MAP.get(m.group(2)[:3].lower()), m.group(3)
            return _mkdate(y, mo, d), _mkdate(y, mo, d)
        return "", ""
    d1, d2, mo, y = int(m.group(1)), int(m.group(2)), MONTH_MAP.get(m.group(3)[:3].lower()), m.group(4)
    return _mkdate(y, mo, d1), _mkdate(y, mo, d2)


def _parse_loose_date(txt):
    """'Tue, Aug 18, 2026 ...' -> iso"""
    m = re.search(r"([A-Za-z]{3})\w*[\s,]+(\d{1,2})[\s,]+(\d{4})", txt)
    if not m:
        return ""
    return _mkdate(m.group(3), MONTH_MAP.get(m.group(1)[:3].lower()), int(m.group(2)))


def _registrar_from(html):
    low = html.lower()
    if "linkintime" in low or "link intime" in low or "mufg" in low:
        return "Link Intime"
    if "kfintech" in low or "kfin technologies" in low or "karvy" in low:
        return "KFin"
    if "bigshare" in low:
        return "Bigshare"
    return "Other"


def scrape_chittorgarh(max_detail=20):
    """Mainboard IPO dashboard -> detail pages. Returns list of IPO dicts."""
    r = requests.get("https://www.chittorgarh.com/ipo/", headers=UA, timeout=15)
    r.raise_for_status()
    out, seen = [], set()
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, re.S):
        link = re.search(r'href="(/ipo/[a-z0-9\-]+/\d+/)"', tr)
        if not link:
            continue
        cell = _clean(re.search(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S).group(1))
        m = re.match(r"(.+?)\s*(?:\s+[A-Z]{1,3})?\s+(\d{1,2}\s*-\s*\d{1,2}\s+[A-Za-z]{3})\w*\s*$", cell)
        name = m.group(1).strip() if m else re.sub(r"\s*\d.*$", "", cell).strip()
        date_txt = m.group(2) if m else ""
        if not name or link.group(1) in seen:
            continue
        seen.add(link.group(1))
        out.append({"name": name, "dash_dates": date_txt,
                    "url": "https://www.chittorgarh.com" + link.group(1)})
        if len(out) >= max_detail:
            break
    # enrich from detail pages
    for item in out:
        try:
            d = requests.get(item["url"], headers=UA, timeout=12).text
            facts = {}
            for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", d, re.S):
                tds = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)
                if len(tds) >= 2:
                    facts[_clean(tds[0])] = _clean(tds[1])
            item["open_date"], item["close_date"] = _parse_issue_dates(facts.get("IPO Date", "") or item["dash_dates"])
            item["listing_date"] = _parse_loose_date(facts.get("Listing Date", ""))
            band = [float(x.replace(",", "")) for x in re.findall(r"\d[\d,]*(?:\.\d+)?", facts.get("Price Band", ""))]
            item["price_min"], item["price_max"] = (min(band), max(band)) if band else (0, 0)
            lotm = re.search(r"(\d[\d,]*)", facts.get("Lot Size", ""))
            item["lot_size"] = int(lotm.group(1).replace(",", "")) if lotm else 0
            item["registrar"] = _registrar_from(d)
            item["gmp"] = 0.0  # GMP handled by ARTHAN market engine
            # allotment date from the timetable row ("Tentative Allotment ... Mon, Aug 17, 2026")
            am = (re.search(r'title="Tentative Allotment"[^<]*</a></span><span[^>]*>([^<]+)<', d, re.S)
                  or re.search(r">Allotment</(?:a|span)></span><span[^>]*>([^<]+)<", d, re.S)
                  or re.search(r"(?i)allotment[^<]*</t[dh]>\s*<td[^>]*>(.*?)</td>", d, re.S))
            item["allotment_date"] = _parse_loose_date(am.group(1)) if am else ""
        except Exception:
            item.setdefault("open_date", ""); item.setdefault("close_date", "")
            item.setdefault("registrar", "Other"); item.setdefault("gmp", 0.0)
            item.setdefault("price_min", 0); item.setdefault("price_max", 0); item.setdefault("lot_size", 0)
        # allotment date comes from the detail-page timetable; else blank (user-editable)
        item.setdefault("allotment_date", "")
        item["board"] = "Mainboard"
    return out


# ----------------------------------------------------------------------------
# GMP auto-refresh (IPO Ji) + live prices (Yahoo Finance)
# ----------------------------------------------------------------------------

def scrape_gmp():
    """IPO Ji GMP table -> [{name, board, gmp, gmp_pct, est_listing, price_min, price_max, status}]"""
    r = requests.get("https://www.ipoji.com/ipo-gmp", headers=UA, timeout=18)
    r.raise_for_status()
    tbl = re.search(r"<table[^>]*>(.*?)</table>", r.text, re.S)
    if not tbl:
        return []
    out = []
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl.group(1), re.S):
        cells = [re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", c)).strip()
                 for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, re.S)]
        if len(cells) < 7 or cells[0].strip().lower() == "ipo":
            continue
        raw, typ, band, gmp_cell = cells[0], cells[1], cells[2], cells[3]
        # name like 'Milky Mist Dairy Food IPO Mainboard Open'
        name = re.sub(r"\s*IPO\s*(Mainboard|BSE SME|NSE SME|SME)?\s*(Open|Closed|Upcoming|Allotment Out|Listed)?\s*$",
                      "", raw, flags=re.I).strip()
        name = re.sub(r"\s+IPO$", "", name, flags=re.I).strip()
        gmp_m = re.search(r"([\-+]?)\s*₹?\s*(\d[\d,]*(?:\.\d+)?)", gmp_cell)
        gmp = 0.0
        if gmp_m and "—" not in gmp_cell:
            gmp = float(gmp_m.group(2).replace(",", ""))
            if gmp_m.group(1) == "-":
                gmp = -gmp
        nums = [float(x.replace(",", "")) for x in re.findall(r"\d[\d,]*(?:\.\d+)?", band)]
        out.append({"name": name,
                    "board": "SME" if "sme" in typ.lower() else "Mainboard",
                    "gmp": gmp,
                    "price_min": min(nums) if nums else 0,
                    "price_max": max(nums) if nums else 0,
                    "status": re.search(r"(Open|Closed|Upcoming|Listed|Allotment)", raw, re.I).group(1) if re.search(r"(Open|Closed|Upcoming|Listed|Allotment)", raw, re.I) else ""})
    return out


def _ipoji_subs():
    """Consolidated live subscription board incl. sHNI/bHNI split."""
    r = requests.get("https://www.ipoji.com/ipo-subscription-status",
                     headers=UA, timeout=30)
    r.raise_for_status()
    recs = {}
    for tr in re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, re.S):
        cells = [re.sub(r"<[^>]+>|\s+", " ", c).strip()
                 for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)]
        if len(cells) < 10:
            continue
        name = re.sub(r"\s{2,}", " ", re.sub(r"\b(NSE|BSE)\b", "", cells[0])).strip()
        def f(v):
            try:
                return float(v)
            except (TypeError, ValueError):
                return None
        recs[norm_name(name)] = {"name": name, "close_txt": cells[1],
                                 "qib": f(cells[2]), "shni": f(cells[3]),
                                 "bhni": f(cells[4]), "nii": f(cells[5]),
                                 "rii": f(cells[6]), "emp": f(cells[7]),
                                 "others": f(cells[8]), "total": f(cells[9])}
    return recs


def _chittorgarh_live_subs():
    """Chittorgarh's own exchange-sourced live subscription, per open IPO.
       Live table carries QIB / NII / Retail / Total (no HNI split)."""
    r = requests.get("https://www.chittorgarh.com/ipo/", headers=UA, timeout=20,
                     allow_redirects=True)
    r.raise_for_status()
    links = re.findall(r'href="(/ipo/([a-z0-9-]+)-ipo/(\d+))[^"]*"[^>]*>([^<]{4,60})<', r.text)
    out = {}
    sess = requests.Session()
    sess.headers.update(UA)
    for path, slug, iid, label in links[:18]:
        try:
            p = sess.get(f"https://www.chittorgarh.com/ipo_subscription/{slug}-ipo/{iid}/",
                         timeout=20, allow_redirects=True)
            for tbl in re.findall(r"<table[^>]*>(.*?)</table>", p.text, re.S):
                if "Subscription (times)" not in tbl:
                    continue
                rec = {}
                for row in re.findall(r"<tr[^>]*>(.*?)</tr>", tbl, re.S):
                    cells = [re.sub(r"<[^>]+>|\s+", " ", c).strip()
                             for c in re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.S)]
                    if len(cells) < 2:
                        continue
                    key, val = cells[0].lower(), cells[1]
                    try:
                        num = float(re.sub(r"[^0-9.]", "", val) or "nan")
                    except ValueError:
                        continue
                    if "qualified" in key:      rec["qib"] = num
                    elif "non institutional" in key: rec["nii"] = num
                    elif "retail" in key:       rec["rii"] = num
                    elif "employee" in key:     rec["emp"] = num
                    elif "total" in key:        rec["total"] = num
                if rec:
                    lab = re.sub(r"\s*\(BSE\)|\s*\(NSE\)|\s*IPO\s*$", "", label, flags=re.I)
                    out[norm_name(lab)] = rec
                break
        except Exception:
            continue
    return out


def refresh_subscriptions():
    """Merge feeds every 15 min: Chittorgarh (exchange-sourced) wins for
       QIB/NII/Retail/Total; IPO Ji supplies the sHNI/bHNI split + finals."""
    ipos_local = rows("SELECT id, name, sub_json FROM ipos")
    if not ipos_local:
        return {"ok": True, "matched": 0}
    iji = {}
    cg = {}
    errors = []
    try:
        iji = _ipoji_subs()
    except Exception as e:
        errors.append(f"ipoji: {e}")
    try:
        cg = _chittorgarh_live_subs()
    except Exception as e:
        errors.append(f"chittorgarh: {e}")
    matched = 0
    now = ist_now().isoformat(timespec="seconds")
    for ipo in ipos_local:
        t = norm_name(ipo["name"])
        def find(pool):
            hit = pool.get(t)
            if hit:
                return hit
            return next((v for k, v in pool.items() if t and (t in k or k in t)), None)
        rec_ij = find(iji) or {}
        rec_cg = find(cg) or {}
        if not rec_ij and not rec_cg:
            continue
        merged = {"qib": None, "shni": None, "bhni": None, "nii": None,
                  "rii": None, "emp": None, "others": None, "total": None,
                  "src": ("cg+iji" if rec_cg and rec_ij else "cg" if rec_cg else "iji")}
        for k in ("shni", "bhni", "emp", "others"):
            merged[k] = rec_ij.get(k)
        merged["total"] = rec_ij.get("total")   # IPOJi total = base
        for k in ("qib", "nii", "rii", "total"):
            if rec_cg.get(k) is not None:
                merged[k] = rec_cg[k]           # Chittorgarh wins when live
            elif rec_ij.get(k) is not None:
                merged[k] = rec_ij[k]
        payload = json.dumps(merged)
        if payload != (ipo.get("sub_json") or ""):
            run("UPDATE ipos SET sub_json=?, sub_at=? WHERE id=?", (payload, now, ipo["id"]))
            schedule_backup()
        matched += 1
    print(f"[sub] matched {matched}/{len(ipos_local)} (cg:{len(cg)} iji:{len(iji)})", flush=True)
    return {"ok": True, "matched": matched, "chittorgarh": len(cg), "ipoji": len(iji), "errors": errors}


def _ipoji_listing_date(name: str) -> str:
    """Fetch the authoritative listing date from IPOJi's per-IPO page.
    Every page embeds JSON-LD PropertyValues for the whole IPO calendar
    (machine-readable, no fragile table scraping)."""
    slug = re.sub(r"\b(limited|ltd|private|pvt)\b", "", name.lower())
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
    for cand in (slug + "-ipo", slug):
        try:
            t = requests.get(f"https://www.ipoji.com/ipo/{cand}", headers=UA, timeout=15).text
        except Exception:
            continue
        m = re.search(r'"IPO listing date"\s*,\s*"value"\s*:\s*"(\d{4}-\d{2}-\d{2})"', t)
        if m:
            return m.group(1)
    return ""


def recover_stale_listings(limit=12):
    """Auto-heal the RESULT AWAITED stragglers: an IPO whose tentative listing
    date wasn't published when the scrapers first saw it drops off the source
    dashboards once it closes, so nothing ever back-fills listing_date and the
    chip sticks at RESULT AWAITED forever. Sweep those rows via IPOJi's
    per-IPO calendar, then let the CMP cycle pick up live prices.

    Failure cooldown: a name IPOJi can't resolve gets 5 attempts, then is
    paused for 7 days so it can't starve newer IPOs of their daily sweep."""
    t = today_str()
    stale = rows("""SELECT id, name FROM ipos
                    WHERE COALESCE(listing_date,'')='' AND COALESCE(close_date,'')<>''
                      AND close_date < ? ORDER BY close_date ASC LIMIT ?""", (t, limit))
    try:
        fails = json.loads(kv_get("ipo_recover_fails", "{}") or "{}")
    except (TypeError, ValueError):
        fails = {}
    cutoff = (ist_now().date() - timedelta(days=7)).isoformat()
    fixed = []
    changed = False
    for i in stale:
        f = fails.get(str(i["id"]))
        if f and f[0] >= 5 and f[1] >= cutoff:
            continue  # in cooldown after repeated unrecoverable failures
        ld = _ipoji_listing_date(i["name"])
        if not ld:
            fails[str(i["id"])] = [(f[0] if f else 0) + 1, t]
            changed = True
            continue
        # never clobber a date the user set meanwhile (or another writer raced in)
        run("UPDATE ipos SET listing_date=? WHERE id=? AND COALESCE(listing_date,'')=''",
            (ld, i["id"]))
        if str(i["id"]) in fails:
            del fails[str(i["id"])]
            changed = True
        fixed.append({"name": i["name"], "listing_date": ld})
    if changed:
        kv_set("ipo_recover_fails", json.dumps(fails))
    if fixed or changed:
        schedule_backup()
    return fixed


def refresh_gmp():
    """Update gmp for tracked IPOs. Returns {'matched':n,'total':n}."""
    try:
        scraped = scrape_gmp()
    except Exception as e:
        return {"ok": False, "error": str(e)}
    scraped = [s for s in scraped if s["board"] == "Mainboard" and s["name"]]
    matched = 0
    for ipo in rows("SELECT id,name,gmp,listing_date FROM ipos"):
        if ipo.get("listing_date") and today_str() > ipo["listing_date"]:
            continue  # already listed — GMP irrelevant
        tgt = norm_name(ipo["name"])
        hit = None
        for s in scraped:
            ns = norm_name(s["name"])
            if ns == tgt or (len(tgt) > 5 and (tgt in ns or ns in tgt)):
                hit = s; break
        if hit:
            # IPOJi value lands in gmp_feed (cross-check store). The displayed
            # ipos.gmp is owned by the ARTHAN engine whenever it manages the IPO
            # (single flicker-free number); otherwise the legacy feed writes it.
            run("""UPDATE ipos SET gmp_feed=?,
                   gmp=CASE WHEN mkt_json='' THEN ? ELSE gmp END WHERE id=?""",
                (hit["gmp"], hit["gmp"], ipo["id"]))
            matched += 1
    return {"ok": True, "matched": matched, "total": len(scraped)}


def yahoo_price(symbol):
    r = requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}.NS"
                     if not symbol.endswith((".NS", ".BO")) else
                     f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
                     headers=UA, timeout=10)
    r.raise_for_status()
    meta = r.json()["chart"]["result"][0]["meta"]
    return float(meta.get("regularMarketPrice") or 0), meta.get("symbol", symbol)


def resolve_symbol(name):
    """Best-effort: find NSE symbol from company name via Yahoo search."""

    def _search(q):
        r = requests.get("https://query1.finance.yahoo.com/v1/finance/search",
                         params={"q": q, "quotesCount": 6, "newsCount": 0, "enableFuzzyQuery": True},
                         headers=UA, timeout=10)
        r.raise_for_status()
        return [x for x in r.json().get("quotes", []) if str(x.get("symbol", "")).endswith(".NS")]

    q = re.sub(r"\b(limited|ltd|india)\b", "", name, flags=re.I).strip()
    try:
        cands = _search(q)
        # the Indian ticker often only surfaces for the FULL company name —
        # e.g. "LEAP" returns US/HK noise, "LEAP India" returns LEAPIND.NS
        if not cands and q.lower() != name.strip().lower():
            cands = _search(name.strip()) or cands
        tgt = norm_name(name)
        for c in cands:
            cn = norm_name(c.get("shortname") or c.get("longname") or c.get("symbol", ""))
            if cn == tgt or (tgt and (tgt in cn or cn in tgt)):
                return c["symbol"].replace(".NS", "")
        return cands[0]["symbol"].replace(".NS", "") if cands else ""
    except Exception:
        return ""


def fetch_cmp_for(ipo):
    """Resolve symbol if needed, fetch CMP, persist. Returns (cmp, symbol, err)."""
    sym = (ipo.get("symbol") or "").strip()
    if not sym:
        sym = resolve_symbol(ipo["name"])
        if sym:
            run("UPDATE ipos SET symbol=? WHERE id=?", (sym, ipo["id"]))
    if not sym:
        return 0, "", "could not resolve NSE symbol — set it manually in Edit IPO"
    try:
        price, _ = yahoo_price(sym)
    except Exception as e:
        return 0, sym, str(e)
    if price:
        run("UPDATE ipos SET cmp=?, cmp_at=datetime('now') WHERE id=?", (price, ipo["id"]))
    return price, sym, ""


def refresh_listing_cmps():
    """Auto-refresh CMP for IPOs listed today or recently (keeps P&L live)."""
    t = today_str()
    due = rows("""SELECT * FROM ipos WHERE listing_date<>'' AND listing_date<=?
                  AND date(listing_date)>=date(?, '-14 day')""", (t, t))
    out = []
    for ipo in due:
        cmp_, sym, err = fetch_cmp_for(ipo)
        if cmp_:
            out.append({"name": ipo["name"], "cmp": cmp_, "symbol": sym})
    return out


_alert_seen = set()


def compute_alerts():
    t = today_str()
    alerts = []
    ipos = rows("SELECT * FROM ipos")
    apps = rows("""SELECT a.*, ac.holder, ac.auth_mode FROM applications a
                   JOIN accounts ac ON ac.id=a.account_id WHERE a.applied=1""")
    nm = {i["id"]: i for i in ipos}
    for a in apps:
        ipo = nm.get(a["ipo_id"])
        if not ipo:
            continue
        in_window = (ipo.get("open_date") or "9999") <= t <= (ipo.get("close_date") or "")
        if a["mandate_status"] == "pending" and in_window:
            alerts.append({"sev": "high", "kind": "mandate", "ipo_id": ipo["id"],
                           "title": "UPI mandate pending",
                           "detail": f"{a['holder']} — {ipo['name']} • ₹{a['amount']:,.0f}"})
        if a["allotment"] == "not_allotted" and a["refund"] == "pending":
            alerts.append({"sev": "med", "kind": "refund", "ipo_id": ipo["id"],
                           "title": "Refund/unblock pending",
                           "detail": f"{a['holder']} — {ipo['name']} • ₹{a['amount']:,.0f}"})
    for ipo in ipos:
        st = ipo_status(ipo)
        if ipo.get("allotment_date") and ipo["allotment_date"] <= t and st == "result_pending":
            pending_n = sum(1 for a in apps if a["ipo_id"] == ipo["id"] and a["allotment"] == "pending")
            if pending_n:
                alerts.append({"sev": "high", "kind": "allotment", "ipo_id": ipo["id"],
                               "title": f"Allotment check due: {ipo['name']}",
                               "detail": f"{pending_n} application(s) still pending — run auto-check ({ipo['registrar']})"})
        if ipo.get("listing_date") == t:
            open_pos = [a for a in apps if a["ipo_id"] == ipo["id"] and a["allotment"] == "allotted"
                        and (a["allotted_qty"] or 0) > (a["sell_qty"] or 0)]
            alerts.append({"sev": "high", "kind": "listing", "ipo_id": ipo["id"],
                           "title": f"🔔 {ipo['name']} lists today",
                           # keep detail STABLE (no CMP) — a changing detail makes
                           # the dedupe key change and re-pushes every CMP tick
                           "detail": "Opens ~10 AM — CMP auto-updates in the app" +
                                     (f" • open positions in {len(open_pos)} account(s)" if open_pos else "")})
            tpin = sorted({a["holder"] for a in open_pos if (a.get("auth_mode") or "") == "TPIN"})
            if tpin:
                alerts.append({"sev": "high", "kind": "tpin", "ipo_id": ipo["id"],
                               "title": "CDSL TPIN authorization needed to sell today",
                               "detail": f"{ipo['name']}: {', '.join(tpin)}"})
        # listing tomorrow → prep hint for GMP vs allocation
        if ipo.get("listing_date") and ipo["listing_date"] > t:
            delta = (date.fromisoformat(ipo["listing_date"]) - date.fromisoformat(t)).days
            if delta == 1:
                alerts.append({"sev": "info", "kind": "listing_soon", "ipo_id": ipo["id"],
                               "title": f"{ipo['name']} lists tomorrow",
                               "detail": f"GMP ₹{ipo['gmp'] or 0} • ensure DDPI/TPIN ready on allotted accounts"})
    # data-quality findings from the ARTHAN market engine (last 24 h only, newest 6)
    try:
        cutoff = (ist_now() - timedelta(hours=24)).isoformat(timespec="seconds")
        issues = [x for x in json.loads(kv_get("market_issues", "[]") or "[]")
                  if x.get("at", "") >= cutoff][-6:]
        for x in issues:
            alerts.append({"sev": x.get("sev", "med"), "kind": "data_check",
                           "title": x["title"], "detail": x["detail"]})
    except Exception:
        pass
    return alerts


def sync_live_ipos():
    """Merge scraped IPOs into DB by fuzzy name. Returns summary dict."""
    summary = {"fetched": 0, "added": 0, "updated": 0, "errors": []}
    scraped = []
    try:
        scraped = scrape_chittorgarh()
        summary["fetched"] = len(scraped)
    except Exception as e:
        summary["errors"].append(f"Chittorgarh: {e}")
    existing = rows("SELECT * FROM ipos")
    for s in scraped:
        tgt = norm_name(s["name"])
        if not tgt:
            continue
        hit = None
        for e in existing:
            ne = norm_name(e["name"])
            if ne == tgt or (len(tgt) > 6 and (tgt in ne or ne in tgt)):
                hit = e; break
        if hit:
            # keep live data fresh: dates/price/lot can shift (postponements,
            # price-band fixes) — refresh changed fields until the IPO lists;
            # never touch an already-listed IPO's history
            upd = {}
            STATUS_FROZEN = ipo_status(hit) == "listed"
            for f in ("open_date", "close_date", "allotment_date", "listing_date",
                      "price_min", "price_max", "lot_size"):
                nv = s.get(f)
                if not nv:
                    continue
                if not hit.get(f):
                    upd[f] = nv                        # fill empties as before
                elif not STATUS_FROZEN and str(nv) != str(hit.get(f) or ""):
                    upd[f] = nv                        # correct stale values
            if s.get("gmp"):
                upd["gmp"] = s["gmp"]
            if hit.get("registrar") in ("Other", "") and s.get("registrar") not in ("Other", ""):
                upd["registrar"] = s["registrar"]
            if upd:
                sets = ",".join(f"{k}=?" for k in upd)
                run(f"UPDATE ipos SET {sets} WHERE id=?", (*upd.values(), hit["id"]))
                summary["updated"] += 1
        else:
            run("""INSERT INTO ipos(name,registrar,open_date,close_date,price_min,price_max,
                   lot_size,allotment_date,listing_date,board,gmp,source)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,'live')""",
                (s["name"], s.get("registrar", "Other"), s.get("open_date", ""), s.get("close_date", ""),
                 s.get("price_min", 0), s.get("price_max", 0), s.get("lot_size", 0),
                 s.get("allotment_date", ""), s.get("listing_date", ""), "Mainboard", s.get("gmp", 0)))
            summary["added"] += 1
    try:
        summary["listings_recovered"] = recover_stale_listings()
    except Exception as e:
        summary["listings_recovered"] = f"skipped ({e})"
    return summary


# ----------------------------------------------------------------------------
# FastAPI app
# ----------------------------------------------------------------------------

app = FastAPI(title="IPO Command Center")
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")


# ----------------------------------------------------------------------------
# simple passcode gate (protects PAN/account data on the public tunnel)
# ----------------------------------------------------------------------------

CONF_FILE = DATA / "config.json"
if CONF_FILE.exists():
    CONF = json.loads(CONF_FILE.read_text())
else:
    CONF = {"passcode": f"{secrets.randbelow(900000) + 100000}"}
    print(f"[boot] FIRST-RUN PASSCODE: {CONF['passcode']} — log in once, then change it in Settings", flush=True)
    try:
        CONF_FILE.write_text(json.dumps(CONF))
    except Exception:
        pass  # ephemeral filesystems (e.g. Render free) — env/default still work

_ENV_PASSCODE = os.environ.get("PASSCODE", "").strip()


def _passcode() -> str:
    """Resolution: PASSCODE env (ops-pinned) > kv (user-changed via app, backed
    up to GitHub so it survives redeploys) > config file/default."""
    if _ENV_PASSCODE:
        return _ENV_PASSCODE
    try:
        k = kv_get("app_passcode")
        if k:
            return k
    except Exception:
        pass
    return CONF["passcode"]


def _auth_token() -> str:
    return hashlib.sha256((_passcode() + "-ipo-center").encode()).hexdigest()[:48]


# --- sliding sessions with server-side auto-lock ---------------------------
# The auth cookie is a RANDOM token (never derived from the passcode, so there
# is nothing to brute-force), remembered in memory with a last-activity stamp.
# If no request touches the session for SESS_TTL seconds, it is dead — that is
# the auto-lock: the app stops sending requests the moment it is closed (its
# heartbeat only runs while visible), so the lock engages ~5 min after closing.
# In-memory on purpose: a host restart simply asks for one unlock (1 tap with
# fingerprint) instead of trusting year-old cookies.
SESS_TTL = 300          # 5 minutes of no activity -> locked
SESS_MAX = 12           # phone + desktop + a few spares; oldest evicted
_sessions = {}          # token -> last activity epoch


def _sess_new() -> str:
    tok = secrets.token_urlsafe(32)
    now = time.time()
    if len(_sessions) >= SESS_MAX:
        for t, _ in sorted(_sessions.items(), key=lambda kv: kv[1])[:SESS_MAX // 2]:
            _sessions.pop(t, None)
    _sessions[tok] = now
    return tok


def _sess_ok(tok: str) -> bool:
    if not tok:
        return False
    now = time.time()
    ts = _sessions.get(tok)
    if ts is None or now - ts > SESS_TTL:
        _sessions.pop(tok, None)
        return False
    _sessions[tok] = now   # slide: any activity resets the 5-minute clock
    return True


def _sess_kill(tok: str):
    _sessions.pop(tok or "", None)


UNLOCK_HTML = r"""<!DOCTYPE html><html><head><script>try{if(localStorage.getItem("ic-theme")==="light")document.documentElement.dataset.theme="light";}catch(_){}</script><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>IPO Center — Unlock</title>
<style>
:root{color-scheme:dark;--bg:#070d14;--card:#0f1824;--line:#1e2d3e;--txt:#e9eff6;--mut:#8fa3b8;--acc:#14b8a6;--acc2:#2dd4bf;--gold:#e3b23c;--input:#0b1420;--red:#f87171;
--glow:radial-gradient(1200px 620px at 72% -12%,rgba(20,184,166,.09),transparent 62%);--ring:0 0 0 3px rgba(20,184,166,.28);--ease:cubic-bezier(.2,.7,.3,1)}
:root[data-theme=light]{color-scheme:light;--bg:#f2f6f7;--card:#ffffff;--line:#dae3e8;--txt:#13212d;--mut:#5b7181;--acc:#0d9488;--acc2:#14b8a6;--gold:#a97e14;--input:#f3f7f8;--red:#dc2626;
--glow:radial-gradient(1200px 620px at 72% -12%,rgba(13,148,136,.06),transparent 62%);--ring:0 0 0 3px rgba(13,148,136,.22)}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);background-image:var(--glow);background-attachment:fixed;color:var(--txt);
font-family:system-ui,-apple-system,'Segoe UI',Roboto,Arial,sans-serif;display:flex;align-items:center;justify-content:center;
min-height:100vh;-webkit-font-smoothing:antialiased;padding:20px}
.box{background:var(--card);border:1px solid var(--line);border-radius:22px;padding:36px 30px;text-align:center;max-width:350px;width:100%;
box-shadow:inset 0 1px 0 rgba(255,255,255,.03),0 18px 48px rgba(0,0,0,.45);animation:pop .3s var(--ease)}
@keyframes pop{from{opacity:0;transform:scale(.96) translateY(8px)}to{opacity:1;transform:none}}
.mark{width:62px;height:62px;margin:0 auto 4px;border-radius:17px;display:flex;align-items:center;justify-content:center;font-size:30px;
background:linear-gradient(140deg,rgba(20,184,166,.28),rgba(227,178,60,.14));border:1px solid rgba(20,184,166,.45);
box-shadow:0 0 0 6px rgba(20,184,166,.07)}
h2{font-size:18px;font-weight:800;letter-spacing:-.2px;margin:12px 0 4px}
h2 span{background:linear-gradient(100deg,var(--gold),#f2cd6b);-webkit-background-clip:text;background-clip:text;color:transparent}
.sub{color:var(--mut);font-size:12.5px;line-height:1.55}
input{background:var(--input);border:1px solid var(--line);color:var(--txt);border-radius:13px;padding:14px;font-size:21px;width:100%;
text-align:center;letter-spacing:9px;margin:16px 0 12px;transition:border-color .16s var(--ease),box-shadow .16s var(--ease);
font-variant-numeric:tabular-nums}
input:focus{outline:none;border-color:var(--acc);box-shadow:var(--ring)}
button{background:linear-gradient(135deg,var(--acc),var(--acc2));border:none;color:#fff;border-radius:12px;padding:13px;width:100%;
font-size:15px;font-weight:700;letter-spacing:.2px;cursor:pointer;box-shadow:0 2px 10px rgba(20,184,166,.3);
transition:filter .16s var(--ease),transform .12s var(--ease)}
button:hover{filter:brightness(1.07)}
button:active{transform:scale(.97)}
button:focus-visible{outline:none;box-shadow:var(--ring)}
.err{color:var(--red);font-size:12.5px;min-height:18px;margin-top:10px;font-weight:600}
.hint{color:var(--mut);font-size:11px;margin-top:14px;line-height:1.55}</style></head><body>
<div class="box"><div class="mark">📈</div><h2>IPO <span>Command Center</span></h2>
<p class="sub">Enter your 6-digit passcode<br><span style="opacity:.8">Auto-locks 5 minutes after you close the app</span></p>
<input id="c" inputmode="numeric" maxlength="6" autocomplete="off" autofocus>
<button onclick="go()">Unlock</button>
<button id="fp" style="display:none;background:transparent;border:1px solid var(--acc);color:var(--acc2);margin-top:10px;box-shadow:none" onclick="fp()">👆 Unlock with fingerprint</button><div class="err" id="e"></div>
<p class="hint">Forgot it? If you set a PASSCODE in your hosting dashboard, change it there. Otherwise check the hosting logs for the line "FIRST-RUN PASSCODE".</p></div>
<script>
const go=async()=>{const r=await fetch('/api/unlock',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code:document.getElementById('c').value})});
if(r.ok)location.href='/';else{let m='Wrong passcode — try again';try{const j=await r.json();if(j.detail)m=j.detail;}catch(_){}document.getElementById('e').textContent=m;}};
document.getElementById('c').addEventListener('keydown',e=>{if(e.key==='Enter')go()});
const b64d=s=>{const p=atob(s.replace(/-/g,'+').replace(/_/g,'/'));const a=new Uint8Array(p.length);for(let i=0;i<p.length;i++)a[i]=p.charCodeAt(i);return a.buffer;};
const b64e=b=>btoa(String.fromCharCode(...new Uint8Array(b))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
fetch('/api/webauthn/status').then(r=>r.json()).then(j=>{if(j.enabled)document.getElementById('fp').style.display='block';}).catch(()=>{});
const fp=async()=>{try{
  const o=await (await fetch('/api/webauthn/login/options',{method:'POST'})).json();
  o.challenge=b64d(o.challenge);o.allowCredentials=(o.allowCredentials||[]).map(c=>({type:c.type,id:b64d(c.id)}));
  const a=await navigator.credentials.get({publicKey:o});
  const r=await fetch('/api/webauthn/login/verify',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id:a.id,clientDataJSON:b64e(a.response.clientDataJSON),authenticatorData:b64e(a.response.authenticatorData),signature:b64e(a.response.signature)})});
  if(r.ok)location.href='/';else document.getElementById('e').textContent='Fingerprint not recognized — use passcode';
}catch(_){document.getElementById('e').textContent='Fingerprint cancelled — use passcode';}};
</script></body></html>"""

PUBLIC_PATHS = {"/manifest.json", "/sw.js", "/favicon.ico", "/api/unlock", "/healthz",
                "/api/webauthn/status", "/api/webauthn/login/options",
                "/api/webauthn/login/verify"}


def _authed(req) -> bool:
    return _sess_ok(req.cookies.get("ipo_auth") or "")


# --- forged-cookie brake: the auth token is derivable from the 6-digit
#     passcode in principle, so guessing cookies must be throttled too, not
#     just /api/unlock. Only requests that PRESENT a wrong cookie count (normal
#     logged-out browsing sends no cookie at all and is never punished).
_cookie_fails = {}   # ip -> [timestamps]
_cookie_until = {}   # ip -> locked-until epoch
_gcookie_fails = []  # global rolling window (rotating-IP attackers)


def _cookie_brake(ip: str) -> bool:
    """Return True when this IP/global context is locked out for cookie guessing."""
    now = time.time()
    _gcookie_fails[:] = [t for t in _gcookie_fails if now - t < 600]
    if len(_gcookie_fails) >= 2000:
        return True
    if now < _cookie_until.get(ip, 0):
        return True
    fails = [t for t in _cookie_fails.get(ip, []) if now - t < 600]
    fails.append(now)
    _cookie_fails[ip] = fails
    _gcookie_fails.append(now)
    if len(fails) >= 200:
        _cookie_until[ip] = now + 600
        _cookie_fails.pop(ip, None)
        return True
    return False


@app.middleware("http")
async def passcode_gate(request, call_next):
    p = request.url.path
    if p in PUBLIC_PATHS or p.startswith("/static"):
        return await call_next(request)
    if p.startswith("/bs/"):
        # the assisted Bigshare page + its proxy carry account PANs — same
        # session gate as the rest of the app; assets/XHR just get a 401
        if _authed(request):
            return await call_next(request)
        if request.cookies.get("ipo_auth") and _cookie_brake(_client_ip(request)):
            return JSONResponse({"ok": False, "error": "too many tries — wait 10 minutes"},
                                status_code=429)
        if p.startswith("/bs/p/"):
            return JSONResponse({"ok": False, "error": "locked"}, status_code=401)
        return HTMLResponse(UNLOCK_HTML)
    if p.startswith("/assist/"):
        # assisted KFin/MUFG one-tap pages hold PANs + verdict controls too —
        # exact same session wall as /bs/ (their /api/assist/query data call is
        # already covered by the "/api" branch below)
        if _authed(request):
            return await call_next(request)
        if request.cookies.get("ipo_auth") and _cookie_brake(_client_ip(request)):
            return JSONResponse({"ok": False, "error": "too many tries — wait 10 minutes"},
                                status_code=429)
        return HTMLResponse(UNLOCK_HTML)
    if p == "/" or p.startswith("/api"):
        if _authed(request):
            resp = await call_next(request)
            if (request.method in ("POST", "PUT", "DELETE") and resp.status_code < 400
                    and p not in ("/api/unlock", "/api/backup", "/api/ping")):
                try:
                    schedule_backup()
                except Exception:
                    pass
            return resp
        if request.cookies.get("ipo_auth") and _cookie_brake(_client_ip(request)):
            return JSONResponse({"ok": False, "error": "too many tries — wait 10 minutes"},
                                status_code=429)
        if p.startswith("/api"):
            return JSONResponse({"ok": False, "error": "locked"}, status_code=401)
        return HTMLResponse(UNLOCK_HTML)
    return await call_next(request)


@app.middleware("http")
async def security_headers(request, call_next):
    """Baseline web hardening on every response. The app is all inline
    JS/CSS (single-file UI), so CSP keeps 'unsafe-inline' but still blocks
    external script loads, exfil to other origins, objects and framing."""
    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "no-referrer")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
        "connect-src 'self'; font-src 'self' data:; object-src 'none'; "
        "base-uri 'self'; frame-ancestors 'none'; form-action 'self'")
    if request.url.path.startswith("/bs/"):
        # the proxied Bigshare page pulls its own jQuery/fonts/icons from
        # public CDNs and loads the captcha as a data: image — allow https:
        resp.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self' 'unsafe-inline' https:; "
            "style-src 'self' 'unsafe-inline' https:; img-src 'self' data: https:; "
            "connect-src 'self'; font-src 'self' data: https:; "
            "object-src 'none'; base-uri 'self'")
    return resp


# --- unlock rate limiting: 5 wrong tries -> locked for 5 minutes (per IP),
#     plus a global brake (40 wrong tries in 10 min from anywhere -> 5-min global
#     lock) so rotating IPs can't brute force the passcode either ---
_unlock_fails = {}   # ip -> [timestamps of recent wrong attempts]
_unlock_until = {}   # ip -> locked-until epoch
_global_fails = []   # all wrong attempts, rolling
_global_until = 0.0


def _client_ip(request):
    # Render's edge APPENDS the real client IP to X-Forwarded-For, so the LAST
    # entry is the trustworthy one — the FIRST entry is whatever the caller
    # claimed (spoofable, which used to make per-IP rate limits meaningless).
    fwd = request.headers.get("x-forwarded-for", "")
    if fwd:
        parts = [p.strip() for p in fwd.split(",") if p.strip()]
        if parts:
            return parts[-1]
    return request.client.host if request.client else "?"


# ---------------------------------------------------------------------------
# Bigshare assisted manual check.
# Bigshare gates every search behind a human-read captcha, so no engine can
# auto-read it. /bs/open serves the registrar's REAL page through us — keeping
# it same-origin so the captcha image + search AJAX keep working — with the
# company, selection type and PAN/BO-ID already filled in. The user only reads
# one picture and taps SEARCH. Everything the page needs afterwards (assets,
# Captcha.ashx, Data.aspx/FetchIpodetails) flows through /bs/p/... which
# forwards to Bigshare, so their cookies/rate-limits behave exactly like a
# direct visit. We save the typing — never the human check.
# ---------------------------------------------------------------------------
BS_ORIGIN = "https://ipo.bigshareonline.com"
BS_PAGE = BS_ORIGIN + "/ipo_status.html"
_BS_BAD = re.compile(r"[^A-Za-z0-9._~/%-]")


def _bs_esc(s: str) -> str:
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _bs_rewrite_cookie(sc: str) -> str:
    """Re-scope an upstream Set-Cookie to our origin so the browser returns it
    only under /bs (and the proxy then forwards it to Bigshare)."""
    parts = [p.strip() for p in sc.split(";")
             if p.strip() and not p.strip().lower().startswith(("domain=", "path=", "samesite"))]
    parts.append("Path=/bs")
    parts.append("SameSite=Lax")
    return "; ".join(parts)


def _bs_set_cookies(resp, upstream) -> None:
    try:
        raw = upstream.raw.headers.getlist("set-cookie")
    except Exception:
        raw = []
    if not raw:  # fallback when raw headers are unavailable (mocks/tests)
        try:
            raw = [f"{k}={v}" for k, v in upstream.cookies.items()]
        except Exception:
            raw = []
    for sc in raw:
        resp.raw_headers.append((b"set-cookie", _bs_rewrite_cookie(sc).encode()))


def _bs_company_options(html: str):
    m = re.search(r'<select id="ddlCompany">(.*?)</select>', html, re.S)
    if not m:
        return []
    return [{"id": v, "name": n.strip()} for v, n in
            re.findall(r'<option value="(\d+)">([^<]+)</option>', m.group(1))]


def _bs_match_company(options, ipo):
    if ipo.get("registrar_ref"):
        hit = next((c for c in options if c["id"] == str(ipo["registrar_ref"]).strip()), None)
        if hit:
            return hit
    target = norm_name(ipo["name"])
    for c in options:
        if norm_name(c["name"]) == target:
            return c
    for c in options:
        n = norm_name(c["name"])
        if target and (target in n or n in target):
            return c
    return None


# Injected at the end of the proxied page: waits for the company list, selects
# our IPO, picks PAN (or BO ID) mode, fills the value, lands the cursor on the
# captcha box and pins a guidance bar. Pure vanilla JS — jQuery may lag behind.
_BS_FILL_SCRIPT = """<script>(function(){
var F=window.__BSFILL__||{},tries=0;
function bar(html){var d=document.getElementById('ipoassistbar');
if(!d){d=document.createElement('div');d.id='ipoassistbar';
d.style.cssText='position:fixed;left:8px;right:8px;bottom:8px;z-index:2147483000;background:#0f1824;color:#e9eff6;border:1px solid #14b8a6;border-radius:12px;padding:12px 40px 12px 14px;font:14px/1.55 system-ui,sans-serif;box-shadow:0 8px 30px rgba(0,0,0,.55)';
var x=document.createElement('button');x.textContent='\\u2715';
x.style.cssText='position:absolute;top:8px;right:10px;background:none;border:0;color:#8fa3b8;font-size:17px;cursor:pointer';
x.onclick=function(){d.remove();};
var s=document.createElement('span');d.appendChild(s);d.appendChild(x);
(document.body||document.documentElement).appendChild(d);}
d.querySelector('span').innerHTML=html;}
function go(){var sel=document.getElementById('ddlCompany');
if(!sel){if(++tries<60)setTimeout(go,300);return;}
if((sel.options||[]).length<2){if(++tries<60)setTimeout(go,300);return;}
var okCo=false;
if(F.companyId){sel.value=F.companyId;okCo=(sel.value===F.companyId);}
if(!okCo&&F.companyName){var toks=String(F.companyName).toUpperCase().split(/[^A-Z0-9]+/).filter(function(w){return w.length>3});
for(var i=0;i<sel.options.length;i++){var t=(sel.options[i].text||'').toUpperCase();
if(toks.length&&toks.every(function(w){return t.indexOf(w)>=0})){sel.selectedIndex=i;okCo=true;break;}}}
try{sel.dispatchEvent(new Event('change',{bubbles:true}));}catch(e){}
var st=document.getElementById('SelectionType');
if(st&&F.selType){st.value=F.selType;try{st.dispatchEvent(new Event('change',{bubbles:true}));}catch(e){}}
var f2='';
if(F.selType==='PN'&&F.pan){var p=document.getElementById('txtpan');if(p){p.value=F.pan;f2='PAN <b>'+F.pan+'</b> filled';}}
else if(F.selType==='BN'&&F.cdsl){var ty=document.getElementById('ddlType');if(ty){ty.value='CDSL';}
var c=document.getElementById('txtcsdl');if(c){c.value=F.cdsl;f2='BO ID <b>'+F.cdsl+'</b> filled';}}
var co=sel.options[sel.selectedIndex]?sel.options[sel.selectedIndex].text:'';
bar((okCo?'\\u2705 <b>'+co+'</b> selected':'\\u26A0 Company not in the list yet (allotment link may not be live) \\u2014 pick <b>'+(F.companyName||'')+'</b> yourself')
+'<br>'+(f2?f2+' for <b>'+(F.accName||'')+'</b>.':'No PAN/BO ID stored for <b>'+(F.accName||'')+'</b> \\u2014 type it yourself.')
+'<br>Now just read the captcha picture and tap <b>SEARCH</b>.');
var cap=document.getElementById('captcha-input');
if(cap){setTimeout(function(){try{cap.scrollIntoView({block:'center'});cap.focus();}catch(e){}},700);}
// once a result (or the "No Record" popup) is on screen, offer the one-tap verdict
var poll=setInterval(function(){
var dp=document.getElementById('dPrint');
var shown=dp&&dp.offsetHeight>0&&dp.offsetParent!==null;
var noRec=/No Record Found/i.test(document.body.innerText||'');
if(shown||noRec){clearInterval(poll);offerVerdict();}
},800);
setTimeout(function(){clearInterval(poll);},180000);}
if(document.readyState==='loading'){document.addEventListener('DOMContentLoaded',go);}else{go();}
function offerVerdict(){
var d=document.getElementById('ipoassistbar');if(!d)return;
if(document.getElementById('ipoassist-mark'))return;
var w=document.createElement('div');w.id='ipoassist-mark';
w.style.cssText='margin-top:9px;display:flex;gap:6px;flex-wrap:wrap';
bar('Result is on screen \u2014 send it back with ONE tap:');
var a=document.createElement('button');a.textContent='\u2705 Mark ALLOTTED';
var n=document.createElement('button');n.textContent='\u274C Mark NOT allotted';
[a,n].forEach(function(b){b.style.cssText='flex:1;min-width:130px;background:#14b8a6;color:#04110d;border:0;border-radius:9px;padding:10px;font-weight:700;font-size:13px;cursor:pointer'});
n.style.background='#f87171';n.style.color='#1d0505';
a.onclick=function(){saveVerdict(true)};n.onclick=function(){saveVerdict(false)};
w.appendChild(a);w.appendChild(n);d.appendChild(w);}
function saveVerdict(allotted){
var qty=0;
if(allotted){qty=parseInt(prompt('How many shares were allotted? (number in the result table)',String(F.lotSize||0))||'0',10);
if(!qty||qty<1){bar('\u26A0 Cancelled \u2014 no share count entered. Nothing was saved.');return;}}
bar('\u23F3 Saving verdict to the app\u2026');
fetch('/api/applications',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({ipo_id:F.ipoId,account_id:F.accId,applied:1,
allotment:allotted?'allotted':'not_allotted',allotted_qty:allotted?qty:0})})
.then(function(r){if(!r.ok)throw new Error('http '+r.status);return r.json();})
.then(function(){bar(allotted?('\u2705 Saved: <b>'+F.accName+'</b> = ALLOTTED '+qty+' sh. Tap CLEAR (form top) and check the next account.')
:('\u2705 Saved: <b>'+F.accName+'</b> = NOT allotted \u2014 refund auto-queued in the app. Next account when ready.'));})
.catch(function(){bar('\u26A0 Save failed (session expired?) \u2014 note the verdict and mark this row in the app yourself.');});}
})();</script>"""

_BS_DOWN_HTML = """<!doctype html><meta name="viewport" content="width=device-width,initial-scale=1">
<body style="font-family:system-ui,sans-serif;background:#070d14;color:#e9eff6;padding:26px;line-height:1.6">
<h2 style="margin:0 0 10px">Couldn&#39;t reach Bigshare just now</h2>
<p class="mut" style="color:#8fa3b8">__MSG__</p>
<p>Fallback: open the registrar page directly — pre-fill won&#39;t apply there, so pick the company and type your PAN yourself:<br>
<a style="color:#2dd4bf" href="https://ipo.bigshareonline.com/ipo_status.html">ipo.bigshareonline.com/ipo_status.html</a></p></body>"""


@app.get("/bs/open")
def bs_open(ipo: int = 0, acc: int = 0):
    ipo_r = rows("SELECT * FROM ipos WHERE id=?", (ipo,))
    acc_r = rows("SELECT * FROM accounts WHERE id=?", (acc,))
    if not ipo_r or not acc_r:
        raise HTTPException(404, "unknown ipo/account")
    i, a = ipo_r[0], acc_r[0]
    if (i.get("registrar") or "").strip().lower() != "bigshare":
        raise HTTPException(400, "assisted check exists only for Bigshare IPOs")
    try:
        r = requests.get(BS_PAGE, headers=UA, timeout=25)
        r.raise_for_status()
    except Exception as e:
        print(f"[bs-assist] upstream fetch failed: {e}", flush=True)
        return HTMLResponse(_BS_DOWN_HTML.replace("__MSG__", _bs_esc(e)), status_code=502)
    page = r.text
    comp = _bs_match_company(_bs_company_options(page), i)
    pan = (a.get("pan") or "").strip().upper()
    cdsl = re.sub(r"\D", "", a.get("cdsl") or "")
    fill = {"ipoName": i["name"], "accName": a.get("holder", ""),
            "ipoId": i["id"], "accId": a["id"], "lotSize": i.get("lot_size") or 0,
            "companyId": comp["id"] if comp else "",
            "companyName": comp["name"] if comp else i["name"],
            "selType": "PN" if pan else ("BN" if cdsl else ""),
            "pan": pan, "cdsl": cdsl}
    # every same-page reference (css/js/img/XHR) must stay inside the proxy:
    # <base> rewrites RELATIVE urls to /bs/p/x/... ('x/' absorbs '../' hops and
    # is stripped by the proxy); absolute https:// links are left untouched
    if "<base" not in page.lower():
        m = re.search(r"<head[^>]*>", page, re.I)
        if m:
            page = page[:m.end()] + '<base href="/bs/p/x/">' + page[m.end():]
    inject = "<script>window.__BSFILL__=" + json.dumps(fill) + ";</script>" + _BS_FILL_SCRIPT
    low = page.lower()
    if "</body>" in low:
        idx = low.rfind("</body>")
        page = page[:idx] + inject + page[idx:]
    else:
        page += inject
    resp = HTMLResponse(page)
    _bs_set_cookies(resp, r)
    print(f"[bs-assist] opened for ipo={ipo} acc={acc} company={fill['companyId'] or '-'}", flush=True)
    return resp


def _bs_rewrite_location(loc: str, base_url: str) -> str:
    from urllib.parse import urljoin, urlparse
    absu = urljoin(base_url, loc)
    p = urlparse(absu)
    if p.netloc and p.netloc.lower() != urlparse(BS_ORIGIN).netloc:
        return absu  # leaving Bigshare entirely — let the browser go directly
    out = "/bs/p" + (p.path if p.path.startswith("/") else "/" + p.path)
    return out + (("?" + p.query) if p.query else "")


@app.api_route("/bs/p/{upath:path}", methods=["GET", "POST", "HEAD"])
async def bs_proxy(upath: str, request: Request):
    if upath.startswith("x/"):
        upath = upath[2:]
    if not upath or ".." in upath or _BS_BAD.search(upath):
        raise HTTPException(400, "bad upstream path")
    url = BS_ORIGIN + "/" + upath
    if request.url.query:
        url += "?" + request.url.query
    ck = {k: v for k, v in request.cookies.items() if k != "ipo_auth"}
    hdrs = dict(UA)
    hdrs["Referer"] = BS_PAGE
    for h in ("content-type", "x-requested-with", "accept"):
        v = request.headers.get(h)
        if v:
            hdrs[h.title()] = v
    body = await request.body() if request.method == "POST" else None
    try:
        r = await run_in_threadpool(
            lambda: requests.request(request.method, url, data=body, headers=hdrs,
                                     cookies=ck, timeout=30, allow_redirects=False))
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Bigshare unreachable: {e}"}, status_code=502)
    resp = Response(content=r.content, status_code=r.status_code)
    if r.headers.get("content-type"):
        resp.headers["content-type"] = r.headers["content-type"]
    if r.headers.get("location"):
        resp.headers["location"] = _bs_rewrite_location(r.headers["location"], url)
    if r.headers.get("retry-after"):
        resp.headers["retry-after"] = r.headers["retry-after"]
    _bs_set_cookies(resp, r)
    return resp


# ----------------------------------------------------------------------------
# Assisted ONE-TAP allotment check for KFin + MUFG (Link Intime).
# These registrars do NOT gate searches behind a captcha, so instead of
# proxying their sites (React SPA / ASP.NET webservices — fragile in an iframe)
# we run that single account's query server-side with the same engines the
# auto-check uses, force-fresh, and show the registrar's OWN answer on a clean
# app page. The human reads the live record once and files the verdict with a
# single tap — the identical save path as the Bigshare assisted flow
# (/api/applications → batch-#6 refund invariants kick in automatically).
# Auto-check stays untouched; this exists for "let me confirm it myself"
# moments and for rows the sweep flags manual/error.
# ----------------------------------------------------------------------------
_ASSIST_REGS = {
    "kfin":        {"label": "KFin",               "engine": "KFin",
                    "link": REGISTRAR_LINKS["KFin"]},
    "link intime": {"label": "MUFG (Link Intime)", "engine": "Link Intime",
                    "link": REGISTRAR_LINKS["Link Intime"]},
    "mufg":        {"label": "MUFG (Link Intime)", "engine": "Link Intime",
                    "link": REGISTRAR_LINKS["Link Intime"]},
}
_ASSIST_FORCE_AT = {}   # ipo id -> epoch of last forced company-list refresh


def _assist_meta(ipo: dict):
    return _ASSIST_REGS.get((ipo.get("registrar") or "").strip().lower())


@app.get("/api/assist/query")
def assist_query(ipo: int = 0, acc: int = 0):
    """Run the registrar engine for ONE account, force-fresh company list, and
    return the raw verdict the assisted page renders. Writes nothing — the
    human still taps to save, exactly like the Bigshare captcha flow."""
    ipo_r = rows("SELECT * FROM ipos WHERE id=?", (ipo,))
    acc_r = rows("SELECT * FROM accounts WHERE id=?", (acc,))
    if not ipo_r or not acc_r:
        raise HTTPException(404, "unknown ipo/account")
    i, meta_acc = ipo_r[0], acc_r[0]
    meta = _assist_meta(i)
    if not meta:
        raise HTTPException(400, "no assisted engine for this registrar")
    # On allotment day a stale "not published yet" company list is worthless, so
    # manual taps always WANT force=True — but the human hops through 15
    # accounts in a row, and force=True re-scrapes the company source every
    # time. Cap the heavy refresh at one per IPO per 2 minutes.
    last = _ASSIST_FORCE_AT.get(str(ipo), 0.0)
    force = (time.time() - last) > 120
    if force:
        _ASSIST_FORCE_AT[str(ipo)] = time.time()
    try:
        res = ENGINES[meta["engine"]](i, meta_acc, force=force)
    except Exception as e:
        print(f"[assist] engine crashed for ipo={ipo} acc={acc}: {e}", flush=True)
        res = {"status": "error", "note": f"{meta['label']} check failed ({e})",
               "link": meta["link"]}
    res = dict(res or {})
    res.setdefault("link", meta["link"])
    res["registrar"] = meta["label"]
    return res


_ASSIST_PAGE = """<!doctype html>
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Assisted check — __REG__</title>
<body style="margin:0;background:#070d14;color:#e9eff6;font:15px/1.55 system-ui,sans-serif">
<div style="max-width:640px;margin:0 auto;padding:18px 14px 40px">
  <div style="color:#14b8a6;font-weight:700;letter-spacing:.4px;text-transform:uppercase;font-size:12px">__REG__ assisted check · one tap</div>
  <h2 style="margin:6px 0 14px">__IPO__</h2>
  <div style="background:#0f1824;border:1px solid #223247;border-radius:14px;padding:13px 15px;margin-bottom:12px">
    <div style="display:flex;justify-content:space-between;gap:10px"><span style="color:#8fa3b8">Account</span><b>__ACC__</b></div>
    <div style="display:flex;justify-content:space-between;gap:10px;margin-top:5px"><span style="color:#8fa3b8">PAN</span><b>__PAN__</b></div>
  </div>
  <div id="res" style="background:#0f1824;border:1px solid #223247;border-radius:14px;padding:16px 15px;margin-bottom:12px;color:#8fa3b8">
    ⏳ Reading the live record from __REG__… (a few seconds)
  </div>
  <div id="acts" style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:14px"></div>
  <div style="background:#0f1824;border:1px solid #223247;border-radius:14px;padding:13px 15px">
    <div style="color:#8fa3b8;font-size:13px;margin-bottom:8px">Accounts applied to this IPO — hop to the next one without going back:
      <span id="nextlink"></span></div>
    <div id="chips" style="display:flex;gap:7px;flex-wrap:wrap">__CHIPS__</div>
  </div>
  <div style="margin-top:16px;font-size:13px;color:#8fa3b8">
    Doubt the answer? <a href="__REG_LINK__" target="_blank" rel="noopener" style="color:#2dd4bf">Open the __REG__ site yourself ↗</a>
  </div>
</div>
<script>
var F=__FILL__;
function esc(s){var d=document.createElement('div');d.textContent=String(s==null?'':s);return d.innerHTML;}
function render(d){
  var res=document.getElementById('res'),acts=document.getElementById('acts');
  var qty=+d.allotted_qty||0;
  var head='',cls='#223247';
  acts.innerHTML='';
  if(d.status==='ok'){
    cls='#1d7a4f';res.style.borderColor=cls;
    head='<div style="font-size:20px;font-weight:800;color:#6ee7b7">\\u2705 ALLOTTED '+(qty?qty+' shares':'')+'</div>';
    acts.appendChild(mkBtn('\\u2705 Save ALLOTTED'+(qty?' '+qty+' sh':''),function(){save(true,qty)},false));
  }else if(d.status==='not_found'){
    cls='#a33';res.style.borderColor=cls;
    head='<div style="font-size:20px;font-weight:800;color:#f87171">\\u274C NOT allotted</div>';
    acts.appendChild(mkBtn('\\u274C Save NOT allotted',function(){save(false,0)},true));
  }else{
    cls='#8a6d1d';res.style.borderColor=cls;
    head='<div style="font-size:20px;font-weight:800;color:#fbbf24">\\ud83d\\udd90 Needs your eyes</div>'+
         '<div style="color:#8fa3b8;margin-top:6px">'+esc(d.note||'The registrar did not give a clear answer.')+'</div>'+
         '<div style="color:#8fa3b8;margin-top:6px">Check it once on the manual site link below if you like, then file it:</div>';
    acts.appendChild(mkBtn('\\u2705 Mark ALLOTTED',function(){save(true,0)},false));
    acts.appendChild(mkBtn('\\u274C Mark NOT allotted',function(){save(false,0)},true));
  }
  res.innerHTML=head+
    (d.status!=='manual'&&d.status!=='error'&&d.note?'<div style="color:#8fa3b8;margin-top:6px">'+esc(d.note)+'</div>':'')+
    (d.matched_company?'<div style="color:#5f7488;margin-top:6px;font-size:13px">registrar list: '+esc(d.matched_company)+'</div>':'');
}
function mkBtn(label,fn,bad){
  var b=document.createElement('button');
  b.textContent=label;
  b.style.cssText='flex:1;min-width:150px;border:0;border-radius:10px;padding:13px 10px;font-weight:800;font-size:14px;cursor:pointer;'+
    (bad?'background:#f87171;color:#1d0505':'background:#14b8a6;color:#04110d');
  b.onclick=function(){b.disabled=true;fn();};
  return b;
}
function save(allotted,qty){
  if(allotted){
    qty=parseInt(prompt('How many shares were allotted? (number in the result)',String(qty||F.lotSize||0))||'0',10);
    if(!qty||qty<1){alert('Cancelled — no share count entered. Nothing was saved.');document.getElementById('acts').innerHTML='';render({status:'manual',note:'Save cancelled — pick the verdict again when ready.'});return;}
  }
  var res=document.getElementById('res');
  res.innerHTML='\\u23F3 Saving verdict to the app\\u2026';
  fetch('/api/applications',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({ipo_id:F.ipoId,account_id:F.accId,applied:1,
      allotment:allotted?'allotted':'not_allotted',allotted_qty:allotted?qty:0})})
  .then(function(r){if(!r.ok)throw new Error('http '+r.status);return r.json();})
  .then(function(){
    res.style.borderColor='#1d7a4f';
    res.innerHTML='\\u2705 Saved: <b>'+esc(F.accName)+'</b> = '+(allotted?('ALLOTTED '+qty+' sh'):'NOT allotted \\u2014 refund auto-queued')+'.';
    var chip=document.getElementById('chip-'+F.accId);if(chip){chip.style.background='#14b8a6';chip.style.color='#04110d';}
    var nxt=F.nextAcc?document.getElementById('chip-'+F.nextAcc):null;
    var nl=document.getElementById('nextlink');
    if(nxt&&N_ACC){nl.innerHTML='Next up: <b>'+esc(N_ACC)+'</b> \\u2192';nxt.style.boxShadow='0 0 0 2px #fbbf24';}
    else{nl.innerHTML='That was the last pending account — you\\u2019re done here. \\u2705';}
  })
  .catch(function(){res.innerHTML='\\u26A0 Save failed (session expired?) \\u2014 note the verdict and mark this row in the app yourself.';});
}
var N_ACC=__NEXT_NAME__;
fetch('/api/assist/query?ipo='+F.ipoId+'&acc='+F.accId)
  .then(function(r){if(!r.ok)throw new Error('http '+r.status);return r.json();})
  .then(function(d){render(d);})
  .catch(function(e){render({status:'error',note:'Could not reach the check ('+e.message+'). Use the manual site link below.'});});
</script>
"""


@app.get("/assist/open")
def assist_open(ipo: int = 0, acc: int = 0):
    ipo_r = rows("SELECT * FROM ipos WHERE id=?", (ipo,))
    acc_r = rows("SELECT * FROM accounts WHERE id=?", (acc,))
    if not ipo_r or not acc_r:
        raise HTTPException(404, "unknown ipo/account")
    i, a = ipo_r[0], acc_r[0]
    meta = _assist_meta(i)
    if not meta:
        reg = (i.get("registrar") or "").strip()
        if reg.lower() == "bigshare":
            raise HTTPException(400, "Bigshare IPOs use their own assisted page — tap the 🔗 name in the Applications table (it opens /bs/open).")
        raise HTTPException(400, f"No assisted one-tap check for registrar '{reg or 'unknown'}' — use the registrar link on the IPO.")

    # account chips: everyone who applied to this IPO (mirrors the Applications
    # table roster), so the user can hop through the whole family list without
    # going back after every verdict
    accs = rows("SELECT id, holder, active FROM accounts ORDER BY sort, id")
    apps = rows("SELECT account_id, applied, allotment FROM applications WHERE ipo_id=?", (ipo,))
    amap = {x["account_id"]: x for x in apps}
    chips, applied_ids = [], []
    for x in accs:
        ap = amap.get(x["id"])
        if not (x["active"] or ap):
            continue
        if not ap or not ap["applied"]:
            continue
        applied_ids.append(x["id"])
        icon = "✅" if ap["allotment"] == "allotted" else ("❌" if ap["allotment"] == "not_allotted" else "⏳")
        cur = (x["id"] == acc)
        style = ("background:#0e2f28;color:#6ee7b7;border-color:#14b8a6;font-weight:800" if cur
                 else "background:#131f30;color:#c8d6e5;border-color:#223247")
        chips.append(
            f'<a id="chip-{x["id"]}" href="/assist/open?ipo={ipo}&acc={x["id"]}" '
            f'style="border:1px solid;border-radius:20px;padding:7px 13px;text-decoration:none;font-size:13px;{style}">'
            f'{icon} {_bs_esc(x["holder"])}</a>')
    next_acc = None
    pending_ids = [aid for aid in applied_ids
                   if (amap[aid]["allotment"] or "pending") == "pending" and aid != acc]
    if acc in applied_ids:
        ordered = applied_ids[applied_ids.index(acc) + 1:] + applied_ids[:applied_ids.index(acc)]
        next_acc = next((aid for aid in ordered if aid in pending_ids), None)
    if next_acc is None:
        next_acc = next((aid for aid in pending_ids), None) if pending_ids else None
    next_name = next((x["holder"] for x in accs if x["id"] == next_acc), "")

    fill = {"ipoId": i["id"], "accId": a["id"], "ipoName": i["name"],
            "accName": a.get("holder", ""), "lotSize": i.get("lot_size") or 0,
            "pan": (a.get("pan") or "").strip().upper(), "nextAcc": next_acc}
    fill_js = json.dumps(fill).replace("</", "<\\/")
    page = (_ASSIST_PAGE
            .replace("__REG__", _bs_esc(meta["label"]))
            .replace("__IPO__", _bs_esc(i["name"]))
            .replace("__ACC__", _bs_esc(a.get("holder", "")))
            .replace("__PAN__", _bs_esc(fill["pan"] or "— (no PAN stored)"))
            .replace("__CHIPS__", "".join(chips) or '<span style="color:#8fa3b8">Nobody has an applied row on this IPO yet.</span>')
            .replace("__REG_LINK__", _bs_esc(meta["link"]))
            .replace("__FILL__", fill_js)
            .replace("__NEXT_NAME__", json.dumps(next_name).replace("</", "<\\/")))
    print(f"[assist] opened for ipo={ipo} acc={acc} reg={meta['label']}", flush=True)
    return HTMLResponse(page)


@app.post("/api/unlock")
def unlock(request: Request, b: dict = Body(...)):
    global _global_until
    ip = _client_ip(request)
    now = time.time()
    code_ok = secrets.compare_digest(str(b.get("code", "")).strip(), _passcode())
    # A CORRECT code always opens the lock — even while brakes are engaged.
    # Attackers spamming wrong codes must never be able to lock the OWNER out
    # (denial-of-service via the rate limiter itself); they don't know the
    # code, so letting correct attempts through gives them nothing.
    if not code_ok:
        if now < _global_until:
            mins = max(1, int((_global_until - now + 59) // 60))
            raise HTTPException(429, f"Service temporarily locked — try again in {mins} min.")
        until = _unlock_until.get(ip, 0)
        if now < until:
            mins = max(1, int((until - now + 59) // 60))
            raise HTTPException(429, f"Too many wrong attempts — locked. Try again in {mins} min.")
        _global_fails.append(now)
        _global_fails[:] = [t for t in _global_fails if now - t < 600]
        if len(_global_fails) >= 40:
            _global_until = now + 300
            _global_fails.clear()
            raise HTTPException(429, "Service temporarily locked for 5 minutes")
        fails = [t for t in _unlock_fails.get(ip, []) if now - t < 600]
        fails.append(now)
        _unlock_fails[ip] = fails
        if len(fails) >= 5:
            _unlock_until[ip] = now + 300
            _unlock_fails.pop(ip, None)
            raise HTTPException(429, "Too many wrong attempts — LOCKED for 5 minutes")
        raise HTTPException(401, f"Wrong passcode — {5 - len(fails)} tries left before a 5-min lock")
    # success: clear counters
    _unlock_fails.pop(ip, None)
    _unlock_until.pop(ip, None)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ipo_auth", _sess_new(), max_age=30 * 86400,
                    httponly=True, samesite="lax",
                    secure=(request.url.scheme == "https"))
    return resp


@app.post("/api/logout")
def api_logout(request: Request):
    """Kill this device's session server-side (a stolen cookie dies with it),
       then clear the cookie. Other signed-in devices are unaffected."""
    _sess_kill(request.cookies.get("ipo_auth") or "")
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("ipo_auth", httponly=True, samesite="lax")
    return resp


@app.post("/api/ping")
def api_ping():
    """Heartbeat from the open app (keeps the 5-minute auto-lock from sliding
       while you're actively using it). Returns 401 once the session expired,
       which is what makes the lock screen appear."""
    return {"ok": True}


# ----------------------------------------------------------------------------
# fingerprint / screen-lock unlock (WebAuthn, optional layer over the passcode)
# registration needs a logged-in session; login endpoints are public (they ARE
# the second auth path). Falls back to passcode if anything is unavailable.
# ----------------------------------------------------------------------------

def _b64e(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def _b64d(s: str) -> bytes:
    return base64.urlsafe_b64decode(s.replace("-", "+").replace("_", "/")
                                    + "=" * (-len(s) % 4))


def _wa_server(host: str):
    try:
        from fido2.server import Fido2Server
        from fido2.webauthn import PublicKeyCredentialRpEntity
        return Fido2Server(PublicKeyCredentialRpEntity(id=host, name="IPO Command Center"))
    except Exception:
        return None


def _wa_cred():
    try:
        return json.loads(kv_get("wa_cred", "null") or "null")
    except (TypeError, ValueError):
        return None


def _wa_challenge():
    """Start a fresh ceremony. Challenges are stored in a SET (not one slot —
    WebAuthn spec: login challenges MUST NOT be reused), each expiring in
    5 min, and every challenge is consumed (one-time use) by _wa_take_state,
    which kills replay of a captured assertion outright."""
    from fido2.utils import websafe_encode
    challenge = secrets.token_bytes(32)
    try:
        st = json.loads(kv_get("wa_state", "{}") or "{}")
        if not isinstance(st, dict):
            st = {}
    except (TypeError, ValueError):
        st = {}
    now = time.time()
    st = {k: v for k, v in st.items()
          if isinstance(v, dict) and v.get("exp", 0) > now}
    st[websafe_encode(challenge)] = {"exp": now + 300, "uv": "preferred"}
    if len(st) > 20:  # cap: keep the newest entries only
        st = dict(sorted(st.items(), key=lambda kv: kv[1].get("exp", 0))[-20:])
    kv_set("wa_state", json.dumps(st))
    return challenge


def _wa_extract_challenge(b64_client: str):
    """Pull the challenge field out of a submitted clientDataJSON."""
    try:
        cd = json.loads(_b64d(b64_client).decode("utf-8", "replace"))
        return cd.get("challenge")
    except Exception:
        return None


def _wa_take_state(challenge_b64: str):
    """One-time use: returns the fido2-shaped state for this exact challenge
    (and deletes it), else None. Replay of a previously-completed ceremony is
    rejected here because its challenge no longer exists."""
    try:
        st = json.loads(kv_get("wa_state", "{}") or "{}")
        if not isinstance(st, dict):
            return None
    except (TypeError, ValueError):
        return None
    now = time.time()
    st = {k: v for k, v in st.items()
          if isinstance(v, dict) and v.get("exp", 0) > now}
    ent = st.pop(challenge_b64, None)
    kv_set("wa_state", json.dumps(st))
    if not ent:
        return None
    return {"challenge": challenge_b64,
            "user_verification": ent.get("uv", "preferred")}


def _wa_err(phase: str, e: Exception):
    """Remember the last ceremony failure (visible via /api/webauthn/diag) so a
    phone-only user can be debugged without server-log access."""
    try:
        kv_set("wa_lasterr", json.dumps({
            "phase": phase, "err": f"{e.__class__.__name__}: {str(e)[:200]}",
            "at": ist_now().isoformat(timespec="seconds")}))
    except Exception:
        pass


@app.get("/api/webauthn/status")
def wa_status():
    return {"enabled": _wa_cred() is not None}


@app.get("/api/webauthn/diag")
def wa_diag(request: Request):
    return {"registered": _wa_cred() is not None,
            "rp_id": request.url.hostname,
            "last_error": json.loads(kv_get("wa_lasterr", "null") or "null")}


@app.post("/api/webauthn/register/options")
def wa_register_options(request: Request):
    srv = _wa_server(request.url.hostname)
    if srv is None:
        raise HTTPException(503, "biometric library unavailable on server")
    challenge = _wa_challenge()
    return {"challenge": _b64e(challenge), "rp": {"name": "IPO Command Center"},
            "user": {"id": _b64e(b"owner"), "name": "owner", "displayName": "Owner"},
            "pubKeyCredParams": [{"type": "public-key", "alg": -7},
                                 {"type": "public-key", "alg": -257}],
            "timeout": 60000,
            "authenticatorSelection": {"authenticatorAttachment": "platform",
                                       "userVerification": "preferred"}}


@app.post("/api/webauthn/register/verify")
def wa_register_verify(request: Request, b: dict = Body(...)):
    srv = _wa_server(request.url.hostname)
    state = _wa_take_state(_wa_extract_challenge(b.get("clientDataJSON", "")) or "")
    if srv is None or state is None:
        raise HTTPException(400, "registration session expired — tap the button again")
    try:
        from fido2.webauthn import CollectedClientData, AttestationObject
        import fido2.cbor as cbor
        auth_data = srv.register_complete(
            state,
            CollectedClientData(_b64d(b["clientDataJSON"])),
            AttestationObject(_b64d(b["attestationObject"])))
        cd = auth_data.credential_data
        if cd is None:
            raise ValueError("authenticator sent no credential data")
        kv_set("wa_cred", json.dumps({"cred_id": _b64e(cd.credential_id),
                                      "key": _b64e(cbor.encode(cd.public_key)),
                                      "counter": int(getattr(auth_data, "counter", 0) or 0)}))
        kv_set("wa_lasterr", "null")
    except Exception as e:  # noqa: BLE001
        _wa_err("register", e)
        raise HTTPException(400, f"server rejected this fingerprint ({e.__class__.__name__}: {str(e)[:90]})")
    schedule_backup()
    backup_now()  # never lose the credential to a free-host restart
    return {"ok": True}


@app.post("/api/webauthn/login/options")
def wa_login_options(request: Request):
    cred = _wa_cred()
    if not cred:
        raise HTTPException(404, "no fingerprint registered on this server")
    challenge = _wa_challenge()
    return {"challenge": _b64e(challenge), "timeout": 60000,
            "userVerification": "preferred",
            "allowCredentials": [{"type": "public-key", "id": cred["cred_id"]}]}


@app.post("/api/webauthn/login/verify")
def wa_login_verify(request: Request, b: dict = Body(...)):
    srv = _wa_server(request.url.hostname)
    cred = _wa_cred()
    state = _wa_take_state(_wa_extract_challenge(b.get("clientDataJSON", "")) or "")
    if srv is None or not cred or state is None:
        raise HTTPException(400, "no active fingerprint challenge — tap again")
    try:
        from fido2.webauthn import (CollectedClientData, AuthenticatorData,
                                    AttestedCredentialData, Aaguid)
        from fido2.cose import CoseKey
        import fido2.cbor as cbor
        key = CoseKey.parse(cbor.decode(_b64d(cred["key"])))
        stored = AttestedCredentialData.create(Aaguid.NONE, _b64d(cred["cred_id"]), key)
        returned_auth_data = AuthenticatorData(_b64d(b["authenticatorData"]))
        srv.authenticate_complete(state, [stored],
                                  _b64d(cred["cred_id"]),
                                  CollectedClientData(_b64d(b["clientDataJSON"])),
                                  returned_auth_data,
                                  _b64d(b["signature"]))
        # signature counter: a strictly-increasing counter is the standard
        # cloned-authenticator tripwire (both zero == authenticator doesn't
        # support counters -> skip, per WebAuthn spec). fido2 1.1.x returns the
        # CREDENTIAL from authenticate_complete, so the counter comes from the
        # submitted auth_data we already parsed.
        new_cnt = int(getattr(returned_auth_data, "counter", 0) or 0)
        old_cnt = int(cred.get("counter") or 0)
        if (new_cnt or old_cnt) and new_cnt <= old_cnt:  # spec: must strictly increase when supported
            _wa_err("login", RuntimeError(f"sign counter not increasing {old_cnt} -> {new_cnt} (cloned key?)"))
            raise HTTPException(401, "fingerprint security check failed (counter regression)")
        if new_cnt != old_cnt:
            cred["counter"] = new_cnt
            kv_set("wa_cred", json.dumps(cred))
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001
        _wa_err("login", e)
        raise HTTPException(401, f"fingerprint check failed ({e.__class__.__name__})")
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ipo_auth", _sess_new(), max_age=30 * 86400,
                    httponly=True, samesite="lax",
                    secure=(request.url.scheme == "https"))
    return resp


@app.post("/api/passcode")
def change_passcode(request: Request, b: dict = Body(...)):
    """In-app passcode change (survives redeploys via the GitHub backup)."""
    if _ENV_PASSCODE:
        raise HTTPException(400, "passcode is pinned by the server's PASSCODE setting — change it in the hosting dashboard instead")
    cur = str(b.get("current", "")).strip()
    new = str(b.get("new", "")).strip()
    if not secrets.compare_digest(cur, _passcode()):
        raise HTTPException(403, "current passcode is wrong")
    if not re.fullmatch(r"\d{6}", new):
        raise HTTPException(400, "new passcode must be exactly 6 digits")
    kv_set("app_passcode", new)
    schedule_backup()
    backup_now()  # never lose a passcode change to a free-host restart
    # keep THIS device logged in: re-issue its cookie against the new code
    # (every other signed-in device is logged out and will need the new code)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("ipo_auth", _sess_new(), max_age=30 * 86400,
                    httponly=True, samesite="lax",
                    secure=(request.url.scheme == "https"))
    return resp


@app.get("/healthz")
def healthz():
    # public, ultra-light — used by uptime monitors to keep the free host awake
    return {"ok": True, "time": ist_now().isoformat(timespec="seconds")}


@app.get("/api/backup")
def backup_download():
    payload = export_backup()
    fname = "ipo-backup-" + ist_now().strftime("%Y%m%d-%H%M") + ".json"
    return JSONResponse(payload, headers={"Content-Disposition": f'attachment; filename="{fname}"'})


@app.get("/api/push/vapid")
def push_vapid():
    return {"ok": True, "key": vapid_keys()[0]}


@app.post("/api/push/subscribe")
def push_subscribe(b: dict = Body(...)):
    sub = b.get("subscription") or {}
    ep = sub.get("endpoint") or ""
    if not ep:
        raise HTTPException(400, "subscription.endpoint missing")
    run("""INSERT INTO push_subs(endpoint, sub_json) VALUES(?,?)
           ON CONFLICT(endpoint) DO UPDATE SET sub_json=excluded.sub_json""",
        (ep, json.dumps(sub)))
    schedule_backup()
    return {"ok": True}


@app.delete("/api/push/subscribe")
def push_unsubscribe(b: dict = Body(...)):
    ep = ((b or {}).get("endpoint") or "")
    if ep:
        run("DELETE FROM push_subs WHERE endpoint=?", (ep,))
    return {"ok": True}


@app.post("/api/push/test")
def push_test():
    subs = rows("SELECT COUNT(*) c FROM push_subs")[0]["c"]
    if not subs:
        raise HTTPException(400, "no subscribed devices — tap 'Enable phone alerts' on your phone first")
    send_push("🔔 IPO Center", "Notifications are ON — you'll get allotment & mandate alerts here.")
    return {"ok": True, "devices": subs}


@app.post("/api/backup")
def backup_restore(b: dict = Body(...)):
    try:
        counts = restore_backup(b)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        raise HTTPException(400, f"restore failed: {e}")
    schedule_backup()
    return {"ok": True, "counts": counts}


@app.get("/manifest.json")
def manifest():
    return Response((BASE / "static" / "manifest.json").read_text(encoding="utf-8"),
                    media_type="application/manifest+json")


@app.get("/sw.js")
def service_worker():
    return Response((BASE / "static" / "sw.js").read_text(encoding="utf-8"),
                    media_type="text/javascript",
                    headers={"Cache-Control": "no-cache"})


@app.get("/", response_class=HTMLResponse)
def home():
    return (BASE / "static" / "index.html").read_text(encoding="utf-8")


_qr_cache = {"url": "", "b64": ""}


def mobile_info():
    """Current public tunnel URL + QR (data-URI), regenerated when URL changes."""
    try:
        url = (DATA / "public-url.txt").read_text().strip()
    except Exception:
        url = ""
    if not url:
        return {"url": "", "qr": ""}
    if _qr_cache["url"] != url:
        try:
            import base64, io, segno
            buf = io.BytesIO()
            segno.make(url).save(buf, kind="png", scale=4, dark="#0b1220", light="white", border=3)
            _qr_cache.update(url=url, b64="data:image/png;base64," + base64.b64encode(buf.getvalue()).decode())
        except Exception:
            _qr_cache.update(url=url, b64="")
    return {"url": url, "qr": _qr_cache["b64"]}


@app.get("/api/state")
def state():
    ipos = rows("SELECT * FROM ipos ORDER BY COALESCE(NULLIF(open_date,''),'9999') ASC, id DESC")
    for i in ipos:
        i["status"] = ipo_status(i)
        agg = rows("""SELECT COUNT(*) applied_cnt,
                             SUM(CASE WHEN mandate_status='approved'
                                       AND allotment<>'allotted'
                                       AND refund<>'received'
                                       AND COALESCE(sell_qty,0)=0 THEN amount ELSE 0 END) blocked_amd,
                             SUM(CASE WHEN allotment='allotted' THEN allotted_qty ELSE 0 END) allotted_qty,
                             SUM(CASE WHEN allotment='allotted' THEN amount ELSE 0 END) invested
                      FROM applications WHERE ipo_id=? AND applied=1""", (i["id"],))[0]
        i["applied_cnt"] = agg["applied_cnt"] or 0
        i["blocked_amt"] = agg["blocked_amd"] or 0
        i["allotted_qty"] = agg["allotted_qty"] or 0
        i["invested"] = agg["invested"] or 0
        i["registrar_link"] = REGISTRAR_LINKS.get(i["registrar"], "")
        try:
            i["sub"] = json.loads(i["sub_json"]) if i.get("sub_json") else None
        except (TypeError, ValueError):
            i["sub"] = None
        try:
            i["mkt"] = json.loads(i["mkt_json"]) if i.get("mkt_json") else None
        except (TypeError, ValueError):
            i["mkt"] = None
    try:
        _charges_month_roll()  # keep recurring monthly charges current
    except Exception as e:
        print("[charges] roll failed:", e, flush=True)
    return {
        "accounts": rows("SELECT * FROM accounts ORDER BY sort, holder, id"),
        "ipos": ipos,
        "applications": rows("SELECT * FROM applications"),
        "charges": charges_store(),
        "today": today_str(),
        "mobile": mobile_info(),
        "backup_at": _bak_last_ok,  # visible save pulse ("" until first write of this boot)
    }


# ---- accounts ----
@app.post("/api/accounts/{aid}/move")
def move_account(aid: int, b: dict = Body(...)):
    """Move one account up/down in the custom Applicants order (Accounts tab)."""
    direction = -1 if str(b.get("dir")) in ("-1", "up") else 1
    accts = rows("SELECT id, sort FROM accounts ORDER BY sort, holder, id")
    idx = next((i for i, a in enumerate(accts) if a["id"] == aid), None)
    if idx is None:
        raise HTTPException(404, "account not found")
    j = idx + direction
    if 0 <= j < len(accts):
        a, o = accts[idx], accts[j]
        with LOCK, get_db() as con:  # normalize then swap
            for n, x in enumerate(accts):
                con.execute("UPDATE accounts SET sort=? WHERE id=?", ((n + 1) * 10, x["id"]))
            con.execute("UPDATE accounts SET sort=? WHERE id=?", ((j + 1) * 10, a["id"]))
            con.execute("UPDATE accounts SET sort=? WHERE id=?", ((idx + 1) * 10, o["id"]))
            con.commit()
    return {"ok": True}


@app.post("/api/accounts")
def create_account(b: dict = Body(...)):
    holder = (b.get("holder") or "").strip()[:60]
    if not holder:
        raise HTTPException(400, "holder name required")
    pan = (b.get("pan") or "").strip().upper()
    if pan and not re.fullmatch(r"[A-Z]{5}\d{4}[A-Z]", pan):
        raise HTTPException(400, "PAN format looks wrong (ABCDE1234F)")
    cdsl = (b.get("cdsl") or "").strip()
    if cdsl and not re.fullmatch(r"\d{16}", cdsl):
        raise HTTPException(400, "CDSL BO ID must be 16 digits")
    nxt = (rows("SELECT COALESCE(MAX(sort),0) m FROM accounts")[0]["m"] or 0) + 10
    aid = run("INSERT INTO accounts(holder,pan,cdsl,broker,bank,upi,auth_mode,notes,active,sort) VALUES(?,?,?,?,?,?,?,?,?,?)",
              (holder, pan, cdsl, str(b.get("broker", ""))[:60], str(b.get("bank", ""))[:60],
               str(b.get("upi", ""))[:80],
               str(b.get("auth_mode", ""))[:10], str(b.get("notes", ""))[:300],
               1 if b.get("active", True) else 0, nxt))
    return {"ok": True, "id": aid}


@app.put("/api/accounts/{aid}")
def update_account(aid: int, b: dict = Body(...)):
    pan = (b.get("pan") or "").strip().upper()
    cdsl = (b.get("cdsl") or "").strip()
    if pan and not re.fullmatch(r"[A-Z]{5}\d{4}[A-Z]", pan):
        raise HTTPException(400, "PAN format looks wrong")
    if cdsl and not re.fullmatch(r"\d{16}", cdsl):
        raise HTTPException(400, "CDSL BO ID must be 16 digits")
    run("""UPDATE accounts SET holder=?,pan=?,cdsl=?,broker=?,bank=?,upi=?,auth_mode=?,notes=?,active=? WHERE id=?""",
        (b.get("holder", "").strip()[:60], pan, cdsl, str(b.get("broker", ""))[:60],
         str(b.get("bank", ""))[:60], str(b.get("upi", ""))[:80],
         str(b.get("auth_mode", ""))[:10], str(b.get("notes", ""))[:300],
         1 if b.get("active", True) else 0, aid))
    return {"ok": True}


@app.delete("/api/accounts/{aid}")
def delete_account(aid: int):
    # FK CASCADE removes this account's application rows too — report how much
    # history went with it so the UI confirm can say the real cost.
    n = rows("SELECT COUNT(*) c FROM applications WHERE account_id=?", (aid,))[0]["c"]
    run("DELETE FROM accounts WHERE id=?", (aid,))
    return {"ok": True, "removed_applications": n}


# ---- ipos ----
@app.post("/api/ipos")
def create_ipo(b: dict = Body(...)):
    name = (b.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "IPO name required")
    iid = run("""INSERT INTO ipos(name,registrar,registrar_ref,open_date,close_date,price_min,price_max,
                 lot_size,allotment_date,listing_date,board,symbol,cmp,gmp,source,notes)
                 VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'manual',?)""",
              (name, b.get("registrar", "Other"), b.get("registrar_ref", ""), _dt(b.get("open_date"), "open date"),
               _dt(b.get("close_date"), "close date"), _f(b.get("price_min") or 0, "price min", 0, 1e6),
               _f(b.get("price_max") or 0, "price max", 0, 1e6),
               _i(b.get("lot_size") or 0, "lot size", 0, 1e8), _dt(b.get("allotment_date"), "allotment date"),
               _dt(b.get("listing_date"), "listing date"),
               b.get("board", "Mainboard"), b.get("symbol", ""), _f(b.get("cmp") or 0, "CMP", 0, 1e7),
               _f(b.get("gmp") or 0, "GMP", -1e5, 1e6), b.get("notes", "")))
    return {"ok": True, "id": iid}


@app.put("/api/ipos/{iid}")
def update_ipo(iid: int, b: dict = Body(...)):
    # partial-friendly: only keys actually sent are written — a partial update
    # can never blank the rest of the row.
    cur_l = rows("SELECT * FROM ipos WHERE id=?", (iid,))
    if not cur_l:
        raise HTTPException(404, "ipo not found")
    cur = cur_l[0]

    def g(k):
        return b[k] if k in b and b[k] is not None else cur.get(k)

    def gf(k, field, lo=None, hi=None):   # validate only when the caller sent it
        return _f(g(k) or 0, field, lo, hi) if k in b else (cur.get(k) or 0)

    def gi(k, field, lo=None, hi=None):
        return _i(g(k) or 0, field, lo, hi) if k in b else (cur.get(k) or 0)

    def gd(k, field):
        return _dt(g(k), field) if k in b else (cur.get(k) or "")

    run("""UPDATE ipos SET name=?,registrar=?,registrar_ref=?,open_date=?,close_date=?,price_min=?,price_max=?,
           lot_size=?,allotment_date=?,listing_date=?,board=?,symbol=?,cmp=?,gmp=?,notes=? WHERE id=?""",
        (str(g("name") or "").strip(), str(g("registrar") or "Other"), str(g("registrar_ref") or ""),
         gd("open_date", "open date"), gd("close_date", "close date"), gf("price_min", "price min", 0, 1e6),
         gf("price_max", "price max", 0, 1e6), gi("lot_size", "lot size", 0, 1e8),
         gd("allotment_date", "allotment date"),
         gd("listing_date", "listing date"), g("board") or "Mainboard", g("symbol") or "",
         gf("cmp", "CMP", 0, 1e7), gf("gmp", "GMP", -1e5, 1e6), g("notes") or "", iid))
    return {"ok": True}


@app.delete("/api/ipos/{iid}")
def delete_ipo(iid: int):
    run("DELETE FROM ipos WHERE id=?", (iid,))
    return {"ok": True}


@app.post("/api/ipos/sync_live")
def api_sync_live():
    try:
        out = {"ok": True, **sync_live_ipos()}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, 500)
    threading.Thread(target=arthan.refresh_market, daemon=True).start()
    return out


# ---- applications ----
def upsert_app(ipo_id, acc_id, fields: dict, auto_amount=True):
    ex = rows("SELECT * FROM applications WHERE ipo_id=? AND account_id=?", (ipo_id, acc_id))
    acct = rows("SELECT * FROM accounts WHERE id=?", (acc_id,))
    ipo = rows("SELECT * FROM ipos WHERE id=?", (ipo_id,))
    if not ipo:
        raise HTTPException(404, "ipo not found")
    ipo = ipo[0]
    base = ex[0] if ex else {}
    merged = {**base, **{k: v for k, v in fields.items() if v is not None}}

    # auto-fill UPI from account default
    if not merged.get("upi") and acct:
        merged["upi"] = acct[0].get("upi", "")
    # auto amount from lots * lot_size * upper band
    if auto_amount and merged.get("lots") and ipo.get("lot_size") and ipo.get("price_max"):
        merged["amount"] = round(int(merged["lots"]) * ipo["lot_size"] * ipo["price_max"], 2)
    # sensible refund defaults
    if merged.get("allotment") == "allotted":
        merged["refund"] = "na"
    elif merged.get("allotment") == "not_allotted" and merged.get("refund") in ("na", "", None):
        merged["refund"] = "pending"
    # stamp the sale date when a sale is first recorded on a row that has none —
    # month-to-month profit reports depend on this (fallback: listing month)
    if (merged.get("sell_qty") or 0) > 0 and not merged.get("sold_on"):
        merged["sold_on"] = today_str()

    if ex:
        sets = ",".join(f"{k}=?" for k in merged if k != "id")
        vals = [merged[k] for k in merged if k != "id"]
        run(f"UPDATE applications SET {sets}, updated_at=datetime('now','localtime') WHERE id=?", (*vals, ex[0]["id"]))
        return ex[0]["id"]
    cols = ["ipo_id", "account_id"] + [k for k in merged if k not in ("id", "ipo_id", "account_id", "updated_at")]
    vals = [ipo_id, acc_id] + [merged[k] for k in cols[2:]]
    # atomic upsert: a second writer racing the same (ipo, account) pair lands
    # as an UPDATE instead of a UNIQUE-constraint 500 on the user's screen
    sets = ", ".join(f"{k}=excluded.{k}" for k in cols[2:])
    return run(f"""INSERT INTO applications({','.join(cols)})
                   VALUES({','.join('?' * len(cols))})
                   ON CONFLICT(ipo_id, account_id) DO UPDATE SET {sets},
                   updated_at=datetime('now','localtime')""", vals)


@app.post("/api/applications")
def save_application(b: dict = Body(...)):
    ipo_id = _i(b.get("ipo_id"), "ipo_id"); acc_id = _i(b.get("account_id"), "account_id")
    fields = {}
    if "applied" in b:
        fields["applied"] = _i(b["applied"], "applied", 0, 1)
    if "lots" in b:
        fields["lots"] = _i(b["lots"], "lots", 0, 10000)
    if "amount" in b:
        fields["amount"] = _f(b["amount"], "amount", 0, 1e9)
    if "allotted_qty" in b:
        fields["allotted_qty"] = _i(b["allotted_qty"], "allotted qty", 0, 1e8)
    if "sell_qty" in b:
        fields["sell_qty"] = _i(b["sell_qty"], "sold qty", 0, 1e8)
    if "sell_price" in b:
        fields["sell_price"] = _f(b["sell_price"], "sell price", 0, 1e7)
    if "sold_on" in b:
        fields["sold_on"] = _dt(b["sold_on"], "sold-on date")
    for k in ("mandate_status",):
        if k in b:
            if b[k] not in ("pending", "approved", "rejected", "expired"):
                raise HTTPException(400, f"bad mandate status '{b[k]}'")
            fields[k] = b[k]
    for k in ("allotment",):
        if k in b:
            if b[k] not in ("pending", "allotted", "not_allotted"):
                raise HTTPException(400, f"bad allotment status '{b[k]}'")
            fields[k] = b[k]
    for k in ("refund",):
        if k in b:
            if b[k] not in ("na", "pending", "received"):
                raise HTTPException(400, f"bad refund status '{b[k]}'")
            fields[k] = b[k]
    for k in ("app_no", "category", "upi", "checked_note"):
        if k in b and b[k] is not None:
            fields[k] = str(b[k])[:120]
    # Cross-field invariants — the Allotment and Refund dropdowns must never
    # drift apart (they used to: marking not_allotted left refund at n/a so the
    # money stayed "blocked" forever; marking allotted left refund pending so
    # shares AND money counted at once). a 'received' refund is never rewound.
    if fields.get("allotment") == "allotted":
        fields["refund"] = "na"          # money became shares — no refund to track
    elif fields.get("allotment") == "not_allotted" and fields.get("refund") in (None, "na"):
        fields["refund"] = "pending"     # money must come back — start tracking
    aid = upsert_app(ipo_id, acc_id, fields, auto_amount=("amount" not in b))
    # UPI rotation: the per-application ("used") UPI is stored on this row only.
    # The account's DEFAULT UPI never changes from here — edit it in Accounts.
    return {"ok": True, "id": aid}


@app.post("/api/ipos/{iid}/mandates")
def set_all_mandates(iid: int, b: dict = Body(...)):
    status = b.get("status", "approved")
    if status not in ("pending", "approved", "rejected", "expired"):
        raise HTTPException(400, "bad status")
    with LOCK, get_db() as con:
        cur = con.execute("UPDATE applications SET mandate_status=?, updated_at=datetime('now','localtime') WHERE ipo_id=? AND applied=1", (status, iid))
        con.commit()
        n = cur.rowcount
    if n:
        backup_now()  # instant push — one-tap bulk writes must survive a free-host hard kill
    return {"ok": True, "updated": n}


@app.post("/api/ipos/{iid}/funds_unblocked")
def unblock_all_funds(iid: int):
    """One tap: every pending refund for this IPO -> received (funds unblocked).
       Only rows already waiting on a refund flip — allotted rows (money
       debited) and undecided rows are never touched."""
    with LOCK, get_db() as con:
        cur = con.execute(
            "UPDATE applications SET refund='received', "
            "updated_at=datetime('now','localtime') "
            "WHERE ipo_id=? AND applied=1 AND refund='pending'", (iid,))
        con.commit()
        n = cur.rowcount
    if n:
        # Instant push, not the 25 s debounce — a hard-kill inside that window
        # once rolled a successful "All funds unblocked" tap back (toast said
        # done, table/dashboard reverted to the older backup on restart).
        backup_now()
    return {"ok": True, "unblocked": n}


@app.post("/api/ipos/{iid}/resolve_pending_allotments")
def resolve_pending_allotments(iid: int):
    """IPO decided (listed or allotment done) but some rows still say
    'pending'? Each stale row keeps its amount counted as blocked forever and
    the table never looks finished. One tap marks every undecided applied row
    NOT allotted and queues its refund — only rows that genuinely got no
    shares should be swept (the button tells the user so)."""
    ipo_l = rows("SELECT * FROM ipos WHERE id=?", (iid,))
    if not ipo_l:
        raise HTTPException(404, "ipo not found")
    st = ipo_status(ipo_l[0])
    if st not in ("listed", "allotment_done"):
        raise HTTPException(400, f"still in '{st}' — clean pending rows only after the result/listing is out")
    with LOCK, get_db() as con:
        cur = con.execute(
            """UPDATE applications SET allotment='not_allotted', allotted_qty=0,
               refund=CASE WHEN refund='na' THEN 'pending' ELSE refund END,
               checked_note='pending cleared: marked not allotted after result (cleanup)',
               updated_at=datetime('now','localtime')
               WHERE ipo_id=? AND applied=1 AND allotment='pending'""", (iid,))
        con.commit()
        n = cur.rowcount
    if n:
        backup_now()  # verdict-class write — never let a hard-kill roll it back
    return {"ok": True, "updated": n, "ipo_status": st}


@app.post("/api/ipos/{iid}/sold_all")
def sold_all(iid: int, b: dict = Body(default={})):
    """One tap: book every allotted, not-fully-sold share of this IPO at one
       price (defaults to the IPO's CMP). Rows already fully sold are skipped."""
    price = b.get("price")
    ipo_l = rows("SELECT * FROM ipos WHERE id=?", (iid,))
    if not ipo_l:
        raise HTTPException(404, "ipo not found")
    if price is None:
        price = ipo_l[0].get("cmp") or 0
    try:
        price = float(price or 0)
    except (TypeError, ValueError):
        raise HTTPException(400, "bad price")
    if price <= 0:
        raise HTTPException(400, "no price given and CMP not set for this IPO")
    with LOCK, get_db() as con:
        cur = con.execute(
            "UPDATE applications SET sell_qty=allotted_qty, sell_price=?, "
            "sold_on=?, updated_at=datetime('now','localtime') "
            "WHERE ipo_id=? AND allotment='allotted' AND allotted_qty>COALESCE(sell_qty,0)",
            (price, today_str(), iid))  # IST calendar day — Render hosts run UTC,
                                        # so SQL 'localtime' stamped midnight sales a day early
        con.commit()
        n = cur.rowcount
    if n:
        backup_now()  # instant push — same hard-kill rollback class as funds_unblocked
    return {"ok": True, "sold_rows": n, "price": price}


@app.get("/api/export/pnl.csv")
def export_pnl_csv():
    """Full book as CSV — one tap, opens in Excel/Sheets on the phone."""
    import csv
    import io

    def _csafe(v):
        # spreadsheet formula-injection guard: a free-text cell opening with
        # = + - @ would EXECUTE as a formula when the user opens the CSV in
        # Excel/Sheets. Prefix with ' so it stays plain text.
        if isinstance(v, str) and v[:1] in ("=", "+", "-", "@", "\t"):
            return "'" + v
        return v

    ipos_ = {i["id"]: i for i in rows("SELECT * FROM ipos")}
    accs_ = {a["id"]: a for a in rows("SELECT * FROM accounts")}
    apps_ = rows("SELECT * FROM applications WHERE applied=1")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["IPO", "Listing date", "Account", "Allotment", "Lots",
                "Blocked (Rs)", "Allotted qty", "Cost per share (Rs)",
                "Refund", "CMP (Rs)", "Sold qty", "Sold avg (Rs)",
                "Sold on", "Booked P&L (Rs)", "Open qty", "Unrealized P&L (Rs)",
                "Total P&L (Rs)"])
    for a in apps_:
        i = ipos_.get(a["ipo_id"], {})
        ac = accs_.get(a["account_id"], {})
        qty = a.get("allotted_qty") or 0
        sq, sp = a.get("sell_qty") or 0, a.get("sell_price") or 0
        basis = qty or sq                     # sold shares' cost basis (see summaries)
        cost = round(a["amount"] / basis, 2) if basis else 0
        booked = round((sp - cost) * sq, 2) if a.get("allotment") == "allotted" and sq else 0
        openq = max(0, qty - sq)
        cmp_ = i.get("cmp") or 0
        unreal = round((cmp_ - cost) * openq, 2) if a.get("allotment") == "allotted" else 0
        w.writerow([_csafe(i.get("name", "?")), i.get("listing_date", ""), _csafe(ac.get("holder", "?")),
                    a.get("allotment", ""), a.get("lots", ""), a.get("amount", ""),
                    qty or "", cost or "", a.get("refund", ""), cmp_ or "",
                    sq or "", sp or "", a.get("sold_on", ""),
                    booked or "", openq or "", unreal or "",
                    round(booked + unreal, 2) if (booked or unreal) else ""])
    # ---- report sections: charges + month / FY / calendar-year profits ----
    months, fys, cys = _pnl_period_summaries()
    w.writerow([])
    w.writerow(["CARD & OTHER CHARGES (they eat into profit)"])
    w.writerow(["Month", "Note", "Amount (Rs)"])
    for x in sorted(charges_store()["items"], key=lambda z: z.get("date") or ""):
        w.writerow([(x.get("date") or "")[:7], _csafe(x.get("note", "")), x.get("amount", "")])
    for title, pool, label in (("PROFITS MONTH TO MONTH (booked minus charges)", months, "Month"),
                               ("PROFITS YEAR TO YEAR — FINANCIAL YEAR (Apr-Mar)", fys, "FY"),
                               ("PROFITS YEAR TO YEAR — CALENDAR YEAR", cys, "Year")):
        w.writerow([])
        w.writerow([title])
        w.writerow([label, "Booked P&L (Rs)", "Charges (Rs)", "Net (Rs)"])
        tot = [0.0, 0.0]
        for k in sorted(pool.keys(), reverse=True):
            b2, c2 = pool[k]
            tot[0] += b2
            tot[1] += c2
            w.writerow([k, round(b2, 2), round(c2, 2), round(b2 - c2, 2)])
        w.writerow(["TOTAL", round(tot[0], 2), round(tot[1], 2), round(tot[0] - tot[1], 2)])
    w.writerow([])
    w.writerow(["(sales without a date are counted in their IPO's listing month)"])
    return Response(buf.getvalue(), media_type="text/csv",
                    headers={"Content-Disposition":
                             f'attachment; filename="ipo-pnl-{today_str()}.csv"'})


# ---- monthly charges (credit-card fees & other recurring costs) -------------
# Stored in kv: {"items":[{id,date,amount,note,tpl?}], "templates":[{id,amount,note,since}],
#                "skipped":["<tpl>:<YYYY-MM>", ...]}  -> month entries the user deleted

def charges_store():
    try:
        j = json.loads(kv_get("charges", "{}") or "{}")
    except (TypeError, ValueError):
        j = {}
    if not isinstance(j, dict):
        j = {}
    j.setdefault("items", [])
    j.setdefault("templates", [])
    j.setdefault("skipped", [])
    return j


def _charges_save(j):
    kv_set("charges", json.dumps(j))


def _month_iter(since: str, upto: str):
    y, m = int(since[:4]), int(since[5:7])
    for _ in range(24):  # safety cap: at most 24 months of backfill
        yield f"{y:04d}-{m:02d}"
        if f"{y:04d}-{m:02d}" >= upto:
            return
        m += 1
        if m == 13:
            y, m = y + 1, 1


def _charges_month_roll():
    """Materialize recurring templates into one entry per month (filling gaps —
    a month the app was never opened still owes the card charge)."""
    j = charges_store()
    cur = today_str()[:7]
    changed = False
    skipped = set(j["skipped"])
    have = {(x.get("tpl"), (x.get("date") or "")[:7]) for x in j["items"]}
    for t in j["templates"]:
        since = (t.get("since") or cur)
        for m in _month_iter(since, cur):
            if (t["id"], m) in have or f"{t['id']}:{m}" in skipped:
                continue
            j["items"].append({"id": f"{t['id']}-{m}", "date": m + "-01",
                               "amount": t["amount"], "note": t.get("note", ""),
                               "tpl": t["id"]})
            have.add((t["id"], m))
            changed = True
    if changed:
        j["items"].sort(key=lambda x: x.get("date") or "")
        _charges_save(j)
        try:
            schedule_backup()  # auto-added charges are real data — back them up
        except Exception:
            pass


@app.get("/api/charges")
def list_charges():
    _charges_month_roll()
    return charges_store()


@app.post("/api/charges")
def add_charge(b: dict = Body(...)):
    try:
        amount = round(float(b.get("amount")), 2)
    except (TypeError, ValueError):
        raise HTTPException(400, "amount required")
    if not amount or amount <= 0:
        raise HTTPException(400, "amount must be positive")
    if amount > 5_000_000:
        raise HTTPException(400, "amount looks wrong — monthly charges shouldn't exceed ₹50,00,000")
    note = str(b.get("note") or "").strip()[:80]
    month = str(b.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        month = today_str()[:7]
    j = charges_store()
    if b.get("repeat"):
        tid = "tpl-" + secrets.token_hex(3)
        j["templates"].append({"id": tid, "amount": amount, "note": note, "since": month})
        _charges_save(j)
        _charges_month_roll()
    else:
        j["items"].append({"id": "chg-" + secrets.token_hex(3), "date": month + "-01",
                           "amount": amount, "note": note})
        j["items"].sort(key=lambda x: x.get("date") or "")
        _charges_save(j)
    schedule_backup()
    return charges_store()


@app.delete("/api/charges/item/{cid}")
def delete_charge_item(cid: str):
    j = charges_store()
    victim = next((x for x in j["items"] if x.get("id") == cid), None)
    if victim and victim.get("tpl"):
        # remember the deletion so the month roller doesn't recreate it
        key = f"{victim['tpl']}:{(victim.get('date') or '')[:7]}"
        if key not in j["skipped"]:
            j["skipped"].append(key)
    j["items"] = [x for x in j["items"] if x.get("id") != cid]
    _charges_save(j)
    schedule_backup()
    return charges_store()


@app.delete("/api/charges/tpl/{tid}")
def delete_charge_template(tid: str):
    """Stop a recurring monthly charge — past month entries stay as history."""
    j = charges_store()
    j["templates"] = [t for t in j["templates"] if t.get("id") != tid]
    _charges_save(j)
    schedule_backup()
    return charges_store()


def _pnl_period_summaries():
    """Booked P&L and charges grouped by month / financial year (Apr-Mar,
    Indian) / calendar year. Used by the CSV export; the UI runs the same
    math client-side."""
    ipos_ = {i["id"]: i for i in rows("SELECT * FROM ipos")}
    apps_ = rows("""SELECT * FROM applications
                    WHERE applied=1 AND allotment='allotted' AND sell_qty>0""")
    months = {}
    for a in apps_:
        ipo = ipos_.get(a["ipo_id"], {})
        m = (a.get("sold_on") or ipo.get("listing_date") or "")[:7]
        if not m:
            continue
        sq = a.get("sell_qty") or 0
        # cost basis per SOLD share: when a row is partially sold the stored
        # amount covers all shares, so divide by allotted_qty; if the qty was
        # never recorded the amount maps to the sold shares directly.
        basis = (a.get("allotted_qty") or 0) or sq
        cost = a["amount"] / basis if basis else 0
        booked = round(((a.get("sell_price") or 0) - cost) * sq, 2)
        months.setdefault(m, [0.0, 0.0])[0] += booked
    for x in charges_store()["items"]:
        m = (x.get("date") or "")[:7]
        if m:
            months.setdefault(m, [0.0, 0.0])[1] += float(x.get("amount") or 0)

    def fy(m):
        y, mm = int(m[:4]), int(m[5:7])
        s = y if mm >= 4 else y - 1
        return f"FY {s}-{(s + 1) % 100:02d}"

    fys, cys = {}, {}
    for m, (b, c) in months.items():
        for pool, key in ((fys, fy(m)), (cys, m[:4])):
            o = pool.setdefault(key, [0.0, 0.0])
            o[0] += b
            o[1] += c
    return months, fys, cys


@app.post("/api/ipos/{iid}/apply")
def apply_bulk(iid: int, b: dict = Body(...)):
    accs = rows("SELECT * FROM accounts WHERE active=1")
    ids = b.get("account_ids") or [a["id"] for a in accs]
    lots = _i(b.get("lots") or 1, "lots", 1, 10000)
    n = 0
    for a in accs:
        if a["id"] not in ids:
            continue
        upsert_app(iid, a["id"], {"applied": 1, "lots": lots,
                                  "category": b.get("category", "Retail"),
                                  "upi": a.get("upi", "")})
        n += 1
    if n:
        backup_now()  # instant push — bulk apply marks are one-tap, high-value writes
    return {"ok": True, "applied": n}


def run_allotment_checks(ipo: dict, force: bool = False) -> list:
    """Sweep every applied account through the registrar engine, commit verdicts,
       and return per-account result dicts.
       force=True (human tap) bypasses the 30-min company-list cache: on allotment
       day the registrar page updates instantly, and a cached "not published yet"
       list must never block a manual check. Background sweeps keep the cache."""
    engine = ENGINES.get(ipo["registrar"])
    apps = rows("""SELECT a.*, ac.holder, ac.pan, ac.cdsl FROM applications a
                   JOIN accounts ac ON ac.id=a.account_id
                   WHERE a.ipo_id=? AND a.applied=1""", (ipo["id"],))
    results = []
    for a in apps:
        if engine is None:
            res = {"status": "manual", "note": "Unknown registrar — check manually", "link": REGISTRAR_LINKS.get(ipo["registrar"], "")}
        else:
            try:
                res = engine(ipo, a, force=force)
            except Exception as e:
                res = {"status": "error", "note": str(e), "link": REGISTRAR_LINKS.get(ipo["registrar"], "")}
        # commit definitive outcomes
        if res.get("status") == "ok":
            run("""UPDATE applications SET allotment='allotted', allotted_qty=?, refund='na',
                   checked_note=?, updated_at=datetime('now','localtime') WHERE id=?""",
                (int(res.get("allotted_qty") or 0), res.get("note", ""), a["id"]))
        elif res.get("status") == "not_found":
            run("""UPDATE applications SET allotment='not_allotted', allotted_qty=0,
                   refund=CASE WHEN refund='na' THEN 'pending' ELSE refund END,
                   checked_note=?, updated_at=datetime('now','localtime') WHERE id=?""",
                (res.get("note", ""), a["id"]))
        else:
            run("UPDATE applications SET checked_note=?, updated_at=datetime('now','localtime') WHERE id=?",
                (res.get("note", ""), a["id"]))
        results.append({"application_id": a["id"], "account_id": a["account_id"],
                        "holder": a["holder"], **{k: res.get(k) for k in
                        ("status", "allotted_qty", "note", "link", "matched_company") if res.get(k) is not None}})
    return results


def notify_alotment_result(ipo: dict, results: list):
    if not push_prefs().get("allotment", True):
        return
    decided = [r for r in results if r.get("status") in ("ok", "not_found")]
    if not decided:
        return
    wins = [r for r in results if r.get("status") == "ok"]
    body = ("✅ " + "; ".join(f"{r['holder']} {r.get('allotted_qty') or ''}sh" for r in wins)) if wins \
        else "No allotments this time — refunds will unblock in 1–2 days."
    send_push(f"📢 {ipo['name'][:28]} — allotment out!",
              f"{body}  ({len(wins)}/{len(results)} allotted)")


@app.post("/api/ipos/{iid}/check_allotment")
def check_allotment(iid: int):
    ipo = rows("SELECT * FROM ipos WHERE id=?", (iid,))
    if not ipo:
        raise HTTPException(404, "ipo not found")
    ipo = ipo[0]
    apps = rows("SELECT 1 x FROM applications WHERE ipo_id=? AND applied=1 LIMIT 1", (iid,))
    if not apps:
        raise HTTPException(400, "No applications recorded for this IPO yet")
    results = run_allotment_checks(ipo, force=True)  # human tap — never serve a stale "not out yet"
    if results:
        backup_now()  # verdicts drive push alerts — never let a restart re-notify or roll them back
    try:
        notify_alotment_result(ipo, results)
    except Exception as e:
        print("[push] allotment notify failed:", e, flush=True)
    return {"ok": True, "results": results}


@app.get("/api/alerts")
def api_alerts():
    try:
        return {"ok": True, "alerts": compute_alerts(), "ts": ist_now().strftime("%d %b %Y, %I:%M %p IST")}
    except Exception as e:
        return {"ok": False, "alerts": [], "error": str(e)}


@app.get("/api/market")
def api_market():
    return arthan.market_payload()


@app.get("/api/market/ipo/{iid}")
def api_market_ipo(iid: int):
    return arthan.ipo_snapshot(iid)


@app.post("/api/market/refresh")
def api_market_refresh():
    # one tap refreshes EVERYTHING the user sees: the ARTHAN boards +
    # per-IPO GMP, then the legacy feeds that fill the IPO section's
    # subscription numbers and the cross-check GMP column.
    out = arthan.refresh_market(force=True, gap=6)  # manual tap: quicker gate, stays under host time limit
    def _legacy():  # slow multi-page feeds — catch up in the background
        try:
            print("[refresh tap] subs:", refresh_subscriptions(), flush=True)
            print("[refresh tap] gmp :", refresh_gmp(), flush=True)
        except Exception as e:  # noqa: BLE001
            print("[refresh tap] legacy error:", e, flush=True)
    threading.Thread(target=_legacy, daemon=True).start()
    out["ipo_subs"] = "refreshing in background"
    out["ipo_gmp"] = "refreshing in background"
    return out


_VERIFY_BUSY = {"on": False}


@app.get("/api/market/verify")
def api_market_verify():
    # Deep verification needs ~40s (two source samples + a gate pause),
    # longer than the hosting request limit — so run it in the background.
    # The stored report (shown on the Verification tab) updates when done.
    stored = json.loads(kv_get("arthan_verify", "{}") or "{}")
    queued = False
    if not _VERIFY_BUSY["on"]:
        _VERIFY_BUSY["on"] = True
        queued = True

        def _bg():
            try:
                for attempt in (1, 2):  # retry once — boot-time refreshes can briefly lock the DB
                    try:
                        rep = arthan.daily_verification(gap=6)
                        print("[verify] done:", rep, flush=True)
                        break
                    except Exception as e:  # noqa: BLE001
                        print(f"[verify] attempt {attempt} failed:", e, flush=True)
                        time.sleep(20)
            finally:
                _VERIFY_BUSY["on"] = False
        threading.Thread(target=_bg, daemon=True).start()
    return {"ok": True, "queued": queued,
            "note": "verification running in background (~45s) — the Verification tab updates automatically",
            "last_report": stored}


@app.post("/api/gmp/refresh")
def api_gmp_refresh():
    return refresh_gmp()


@app.post("/api/ipos/{iid}/fetch_cmp")
def api_fetch_cmp(iid: int):
    ipo = rows("SELECT * FROM ipos WHERE id=?", (iid,))
    if not ipo:
        raise HTTPException(404, "ipo not found")
    cmp_, sym, err = fetch_cmp_for(ipo[0])
    if not cmp_:
        raise HTTPException(400, f"price unavailable ({err or 'no quote yet'})")
    ipo = rows("SELECT * FROM ipos WHERE id=?", (iid,))[0]
    issue = ipo.get("price_max") or 0
    pct = (cmp_ - issue) / issue * 100 if issue else 0
    return {"ok": True, "cmp": cmp_, "symbol": sym, "chg_pct": round(pct, 2)}


# quiet common probes
@app.get("/favicon.ico")
def favicon():
    return FileResponse(BASE / "static" / "icons" / "icon-512.png")


# ----------------------------------------------------------------------------
# background scheduler: GMP (15 min), subscription (15 min), IPO list sync
# (daily), listing-day CMP (15 min), auto-allotment sweep (30 min),
# alert push notifications (15 min)
# ----------------------------------------------------------------------------

def kv_get(k, default=None):
    r = rows("SELECT v FROM kv WHERE k=?", (k,))
    return r[0]["v"] if r else default


def kv_set(k, v):
    run("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))


def push_new_alerts():
    """Push high-severity alerts to subscribed phones (diffed against seen set)."""
    if not rows("SELECT 1 x FROM push_subs LIMIT 1"):
        return
    alerts = compute_alerts()
    prev = kv_get("seen_alerts")
    seen = set(json.loads(prev)) if prev else set()
    if prev is None:
        kv_set("seen_alerts", json.dumps(sorted({f"{a['kind']}|{a['title']}|{a['detail']}" for a in alerts})))
        return  # first run: seed without spamming
    fresh = []
    allow_push = push_prefs().get("high_alert", True)
    for a in alerts:
        key = f"{a['kind']}|{a['title']}|{a['detail']}"
        if key not in seen:
            seen.add(key)
            if allow_push and a.get("sev") == "high":
                fresh.append(a)
    kv_set("seen_alerts", json.dumps(sorted(seen)[-400:]))
    if not fresh:
        return
    if len(fresh) == 1:
        send_push(fresh[0]["title"], fresh[0]["detail"])
    else:
        send_push(f"🔔 {len(fresh)} new IPO alerts",
                  " • ".join(a["title"] for a in fresh[:3]) + ("…" if len(fresh) > 3 else ""))


PUSH_PREF_DEFAULT = {"allotment": True, "close_today": True, "mandate": True,
                     "listing": True, "gmp_swing": True, "high_alert": True}


def push_prefs():
    try:
        d = json.loads(kv_get("push_prefs", "{}") or "{}")
    except (TypeError, ValueError):
        d = {}
    return {**PUSH_PREF_DEFAULT,
            **{k: bool(v) for k, v in d.items() if k in PUSH_PREF_DEFAULT}}


@app.get("/api/notifications/prefs")
def api_get_push_prefs():
    return push_prefs()


@app.post("/api/notifications/prefs")
def api_set_push_prefs(b: dict = Body(...)):
    cur = push_prefs()
    for k in PUSH_PREF_DEFAULT:
        if k in b:
            cur[k] = bool(b[k])
    kv_set("push_prefs", json.dumps(cur))
    return cur


def smart_alerts():
    """Convenience push pack — time-windowed nudges, each at most once/day:
       closes-today (morning), mandates-pending (evening), lists-tomorrow
       (evening), sharp GMP swing on applied IPOs (any refresh cycle)."""
    if not rows("SELECT 1 x FROM push_subs LIMIT 1"):
        return
    pref = push_prefs()
    now = ist_now()
    hm = now.hour * 60 + now.minute
    today = now.date().isoformat()
    tomorrow = (now.date() + timedelta(days=1)).isoformat()
    log = json.loads(kv_get("push_log", "{}") or "{}")
    changed_log = False

    def once(key):
        return log.get(key) != today

    ipos_ = rows("SELECT * FROM ipos")
    apps_ = rows("SELECT * FROM applications WHERE applied=1")
    accs_active = len(rows("SELECT 1 x FROM accounts WHERE active=1"))

    def fire(key, title, body):
        nonlocal changed_log
        send_push(title, body)
        log[key] = today
        changed_log = True

    # 1) closes today — morning heads-up (09:00-11:30)
    if pref["close_today"] and 540 <= hm <= 690:
        for i in ipos_:
            if ipo_status(i) == "open" and i.get("close_date") == today and once(f"close:{i['id']}"):
                sub = (json.loads(i.get("sub_json") or "{}")).get("total")
                applied = sum(1 for a in apps_ if a["ipo_id"] == i["id"])
                fire(f"close:{i['id']}", f"⏰ {i['name'][:28]} closes TODAY",
                     f"GMP ₹{(i['gmp'] or 0):.0f}" + (f" • SUB {sub}×" if sub else "") +
                     f" • applied {applied}/{accs_active} — apply before ~1 PM")
    # 2) mandates still pending — evening nag (17:00-18:30)
    if pref["mandate"] and 1020 <= hm <= 1110:
        by_ipo = {}
        for a in apps_:
            if a.get("mandate_status") == "pending":
                by_ipo[a["ipo_id"]] = by_ipo.get(a["ipo_id"], 0) + 1
        for iid, n in by_ipo.items():
            i = next((x for x in ipos_ if x["id"] == iid), None)
            if i and ipo_status(i) == "open" and once(f"mand:{iid}"):
                fire(f"mand:{iid}", f"⏳ {n} UPI mandate{'s' if n > 1 else ''} pending — {i['name'][:20]}",
                     "Approve the collect requests on the phones before the issue closes.")
    # 3) lists tomorrow — evening heads-up (18:00-20:00)
    if pref["listing"] and 1080 <= hm <= 1200:
        for i in ipos_:
            if i.get("listing_date") == tomorrow and once(f"list:{i['id']}"):
                sh = sum((a.get("allotted_qty") or 0) for a in apps_
                         if a["ipo_id"] == i["id"] and a.get("allotment") == "allotted")
                est = ""
                if i.get("gmp") and i.get("price_max"):
                    est = f"Last GMP ₹{i['gmp']:.0f} → est ₹{i['price_max'] + i['gmp']:.0f}."
                fire(f"list:{i['id']}", f"🔔 {i['name'][:28]} lists TOMORROW",
                     (f"You hold {sh} shares. " if sh else "") + est)
    # 4) sharp GMP swing (≥20% & ≥₹5) on IPOs you applied in — any cycle
    snap = json.loads(kv_get("gmp_snap", "{}") or "{}")
    snap_changed = False
    for i in ipos_:
        g = i.get("gmp") or 0
        key = str(i["id"])
        old = snap.get(key)
        if g > 0:
            snap[key] = g
            snap_changed = True
            if (pref["gmp_swing"] and old and abs(g - old) >= max(5, 0.2 * old) and once(f"gmp:{i['id']}")
                    and ipo_status(i) in ("open", "upcoming", "result_pending", "allotment_done")
                    and any(a["ipo_id"] == i["id"] for a in apps_)):
                arrow = "📈" if g > old else "📉"
                fire(f"gmp:{i['id']}", f"{arrow} {i['name'][:28]} GMP ₹{old:.0f} → ₹{g:.0f}",
                     "You have applications here — grey market moved sharply.")
    if snap_changed:
        kv_set("gmp_snap", json.dumps(snap))
    if changed_log:
        kv_set("push_log", json.dumps(dict(list(log.items())[-40:])))


def scheduler():
    last = {"gmp": 0.0, "sync": "", "sub": 0.0, "auto": 0.0, "push": 0.0,
            "market": 0.0, "verify": "", "smart": 0.0}
    while True:
        try:
            time.sleep(45)  # let the web server boot first
            now = time.time()
            # convenience push pack (closes/mandates/listings/GMP swings)
            if now - last["smart"] > 15 * 60:
                try:
                    smart_alerts()
                except Exception as e:
                    print("[sched] smart_alerts error:", e, flush=True)
                last["smart"] = now
            # ARTHAN market-intelligence refresh every ~30 min
            if now - last["market"] > 30 * 60:
                try:
                    res = arthan.refresh_market()
                    print("[sched] ARTHAN market refresh:", res, flush=True)
                except Exception as e:
                    print("[sched] ARTHAN error:", e, flush=True)
                last["market"] = now
            # daily deep verification after 18:15 IST
            if last["verify"] != today_str() and ist_now().hour >= 18:
                try:
                    res = arthan.daily_verification()
                    print("[sched] ARTHAN daily verification:", res, flush=True)
                except Exception as e:
                    print("[sched] ARTHAN verify error:", e, flush=True)
                last["verify"] = today_str()
            # GMP refreshed every 15 min during market activity
            if now - last["gmp"] > 15 * 60:
                res = refresh_gmp()
                print("[sched] GMP refresh:", res, flush=True)
                last["gmp"] = now
            # live subscription status (QIB/sHNI/bHNI/Retail) every ~15 min
            if now - last["sub"] > 15 * 60:
                res = refresh_subscriptions()
                print("[sched] subscription refresh:", res, flush=True)
                last["sub"] = now
            # full IPO list sync once a day (IST date)
            if last["sync"] != today_str():
                res = sync_live_ipos()
                print("[sched] IPO sync:", res, flush=True)
                last["sync"] = today_str()
            # auto allotment sweep: closed IPOs whose results aren't in yet (~30 min)
            if now - last["auto"] > 30 * 60:
                try:
                    for p in rows("SELECT * FROM ipos"):
                        if ipo_status(p) != "result_pending" or p["registrar"] not in ENGINES:
                            continue
                        if p["registrar"] == "Bigshare" and _bigshare_captcha_blocked(p["id"]):
                            continue  # captcha wall — auto-sweeping is pointless, user checks manually
                        if not rows("""SELECT 1 x FROM applications
                                       WHERE ipo_id=? AND applied=1 AND allotment='pending' LIMIT 1""",
                                    (p["id"],)):
                            continue
                        rs = run_allotment_checks(p)
                        notify_alotment_result(p, rs)
                except Exception as e:
                    print("[sched] auto-allotment error:", e, flush=True)
                last["auto"] = now
            # push new high-severity alerts to phones
            if now - last["push"] > 15 * 60:
                try:
                    push_new_alerts()
                except Exception as e:
                    print("[sched] push-alerts error:", e, flush=True)
                last["push"] = now
            # listing-day prices every 15 min
            updated = refresh_listing_cmps()
            if updated:
                print("[sched] CMP updates:", updated, flush=True)
            time.sleep(15 * 60)
        except Exception as e:
            print("[sched] error:", e, flush=True)
            time.sleep(300)


arthan.ensure_default_watch()  # safe here: kv helpers are defined by now
threading.Thread(target=scheduler, daemon=True).start()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)),
                log_level="warning")
