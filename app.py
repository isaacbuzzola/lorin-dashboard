"""LORIN // SPINOFF — production status dashboard.

Standalone FastAPI app that pulls live data from ShotGrid (Flow Production
Tracking) for project 354 and serves a status dashboard with three tabs:
Status (pipeline), Comp Review (uploaded Version movies) and Questões (open
items, persisted in SQLite).

Data is pulled on demand (Refresh -> POST /api/refresh) and cached to disk so
the page loads instantly from the last snapshot. Version movies/thumbnails use
short-lived presigned S3 URLs, so they are served through /api/media/{id},
which fetches a fresh URL at click time and 302-redirects to it.

Endpoints:
  GET  /                    -> dashboard (index.html)
  GET  /api/data            -> latest cached snapshot (pulls once if none)
  POST /api/refresh         -> live pull from ShotGrid, update cache, return it
  GET  /api/media/{vid}     -> 302 to a fresh presigned movie/thumb URL
  GET  /api/questions       -> list open items
  POST /api/questions       -> add an item
  PATCH  /api/questions/{id}-> edit / resolve / reopen
  DELETE /api/questions/{id}-> remove
"""

import os
import re
import json
import time
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from collections import Counter, OrderedDict, defaultdict

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
load_dotenv(BASE_DIR / ".env")

SG_URL = os.environ["SHOTGRID_URL"].rstrip("/")
SG_SCRIPT = os.environ["SHOTGRID_SCRIPT_NAME"]
SG_KEY = os.environ["SHOTGRID_SCRIPT_KEY"]
PROJECT_ID = int(os.environ.get("LORIN_PROJECT_ID", "354"))
SNAPSHOT_FILE = BASE_DIR / "snapshot.json"
DB_FILE = BASE_DIR / "questions.db"
MEDIA_DIR = BASE_DIR / "media"
MEDIA_DIR.mkdir(exist_ok=True)
VIDEO_EXT = {".mp4", ".mov", ".webm", ".m4v"}


def _preview_media():
    """Return the featured full-film preview (top-level video in /media)."""
    vids = sorted(p for p in MEDIA_DIR.iterdir()
                  if p.is_file() and p.suffix.lower() in VIDEO_EXT)
    if not vids:
        return None
    p = vids[0]
    return {"url": f"/media/{p.name}", "name": p.name}

# ── Status system ────────────────────────────────────────────────────────────
STATUS_META = OrderedDict([
    ("wtg",  {"label": "Waiting to Start", "color": "#5b626d", "bucket": "todo"}),
    ("rds",  {"label": "Ready to Start",   "color": "#4d8fd6", "bucket": "todo"}),
    ("opn",  {"label": "Open",             "color": "#7d8590", "bucket": "todo"}),
    ("ip",   {"label": "In Progress",      "color": "#e0a63a", "bucket": "progress"}),
    ("ajt",  {"label": "Ajust",            "color": "#e8743b", "bucket": "progress"}),
    ("ft",   {"label": "FrameTest",        "color": "#7ee0d3", "bucket": "progress"}),
    ("rev",  {"label": "Pending Review",   "color": "#b083f0", "bucket": "progress"}),
    ("rnd",  {"label": "Rendering",        "color": "#3fbdd6", "bucket": "progress"}),
    ("frnd", {"label": "For Render",       "color": "#2b93a8", "bucket": "progress"}),
    ("fmv",  {"label": "For Mov",          "color": "#5fd39a", "bucket": "done"}),
    ("cmpt", {"label": "Complete",         "color": "#46c266", "bucket": "done"}),
    ("fin",  {"label": "Final",            "color": "#2f9e46", "bucket": "done"}),
])
STATUS_ORDER = list(STATUS_META.keys())
UNKNOWN = {"label": "Unknown", "color": "#3a3f47", "bucket": "todo"}

SHOT_STEP_ORDER = ["Layout", "Cam", "Animation", "AnimSh", "FX", "CFX",
                   "Lighting", "Comp", "Render", "Online"]
ASSET_STEP_ORDER = ["Model", "Rig", "Fur", "LookDev", "Turn"]

_token = {"v": None, "exp": 0.0}
_token_lock = threading.Lock()
_cache = {"snapshot": None}
_refresh_lock = threading.Lock()


# ── ShotGrid ─────────────────────────────────────────────────────────────────
def _get_token() -> str:
    now = time.time()
    if _token["v"] and _token["exp"] > now + 30:
        return _token["v"]
    with _token_lock:
        if _token["v"] and _token["exp"] > now + 30:
            return _token["v"]
        r = requests.post(
            f"{SG_URL}/api/v1.1/auth/access_token",
            data={"client_id": SG_SCRIPT, "client_secret": SG_KEY,
                  "grant_type": "client_credentials"},
            headers={"Accept": "application/json"}, timeout=20,
        )
        r.raise_for_status()
        d = r.json()
        _token["v"] = d["access_token"]
        _token["exp"] = now + float(d.get("expires_in", 600))
        return _token["v"]


def _search(entity: str, filters: list, fields: list, page_size: int = 500) -> list:
    token = _get_token()
    out, page = [], 1
    while True:
        r = requests.post(
            f"{SG_URL}/api/v1.1/entity/{entity}/_search",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/vnd+shotgun.api3_array+json",
            },
            json={"filters": filters, "fields": fields,
                  "page": {"size": page_size, "number": page}},
            timeout=45,
        )
        r.raise_for_status()
        batch = r.json().get("data", []) or []
        out.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
        if page > 60:
            break
    return out


def _rel(row: dict, name: str) -> dict:
    return (row.get("relationships", {}).get(name, {}) or {}).get("data") or {}


def _order_columns(seen: set, preferred: list) -> list:
    cols = [c for c in preferred if c in seen]
    cols += sorted(c for c in seen if c not in preferred)
    return cols


def _bucket_of(code: str) -> str:
    return STATUS_META.get(code, UNKNOWN)["bucket"]


_SHOT_RE = re.compile(r"sh0*([0-9]+)", re.I)


def _shot_from_code(code: str):
    m = _SHOT_RE.search(code or "")
    return f"SHOT_{int(m.group(1)):04d}" if m else None


def build_snapshot() -> dict:
    pf = [["project", "is", {"type": "Project", "id": PROJECT_ID}]]
    tasks = _search("Task", pf, ["content", "step", "sg_status_list", "entity"])
    shots = _search("Shot", pf, ["code", "image"])
    assets = _search("Asset", pf, ["code", "sg_asset_type", "image"])
    versions = _search("Version", pf,
                       ["code", "sg_status_list", "image", "sg_uploaded_movie",
                        "entity", "sg_task", "created_at"])

    shot_names = {s["id"]: s["attributes"].get("code") or f"Shot {s['id']}" for s in shots}
    shot_thumb = {s["id"]: bool(s["attributes"].get("image")) for s in shots}
    asset_names = {a["id"]: a["attributes"].get("code") or f"Asset {a['id']}" for a in assets}
    asset_types = {a["id"]: (a["attributes"].get("sg_asset_type") or "Other") for a in assets}
    asset_thumb = {a["id"]: bool(a["attributes"].get("image")) for a in assets}

    status_counts = Counter()
    step_counts = Counter()
    step_bucket = defaultdict(Counter)   # step (content) -> bucket counts
    step_status = defaultdict(Counter)   # step (content) -> status-code counts
    step_scope = {}                      # step (content) -> 'shot' | 'asset'
    shot_rows = {sid: {"id": sid, "name": nm, "has_thumb": shot_thumb.get(sid, False), "cells": {}}
                 for sid, nm in shot_names.items()}
    asset_rows = {aid: {"id": aid, "name": nm, "type": asset_types.get(aid, "Other"),
                        "has_thumb": asset_thumb.get(aid, False), "cells": {}}
                  for aid, nm in asset_names.items()}
    shot_cols, asset_cols = set(), set()

    for t in tasks:
        attr = t["attributes"]
        code = attr.get("sg_status_list") or "wtg"
        content = (attr.get("content") or "—").strip()
        ent = _rel(t, "entity")
        status_counts[code] += 1
        step_counts[content] += 1
        step_bucket[content][_bucket_of(code)] += 1
        step_status[content][code] += 1
        etype, eid = ent.get("type"), ent.get("id")
        if etype == "Shot" and eid in shot_rows:
            shot_rows[eid]["cells"][content] = code
            shot_cols.add(content)
            step_scope.setdefault(content, "shot")
        elif etype == "Asset" and eid in asset_rows:
            asset_rows[eid]["cells"][content] = code
            asset_cols.add(content)
            step_scope.setdefault(content, "asset")

    total = sum(status_counts.values())
    buckets = Counter()
    for code, n in status_counts.items():
        buckets[_bucket_of(code)] += n
    done = buckets.get("done", 0)
    progress_pct = round(100 * done / total, 1) if total else 0.0

    def rows_sorted(rows):
        return [{"id": r["id"], "name": r["name"], "has_thumb": r.get("has_thumb", False),
                 "cells": r["cells"], **({"type": r["type"]} if "type" in r else {})}
                for r in sorted(rows.values(), key=lambda x: str(x["name"])) if r["cells"]]

    # Comp Review shows ONLY comp versions (task = Comp) — no Turn/other media.
    vlist = []
    for v in versions:
        a = v["attributes"]
        task_name = (_rel(v, "sg_task").get("name") or "").strip()
        if task_name.lower() != "comp":
            continue
        mov = a.get("sg_uploaded_movie") or {}
        ent = _rel(v, "entity")
        code = a.get("code")
        vlist.append({
            "id": v["id"],
            "code": code,
            "shot": ent.get("name") or _shot_from_code(code),
            "status": a.get("sg_status_list"),
            "has_thumb": bool(a.get("image")),
            "has_movie": bool(mov),
            "created_at": a.get("created_at"),
        })
    vlist.sort(key=lambda x: str(x["code"]))

    # Per-layer (pipeline step) breakdown, ordered shot-steps then asset-steps.
    layers = []
    seen_steps = set()

    def _add_layer(step):
        bc = step_bucket.get(step)
        if not bc:
            return
        tot = sum(bc.values())
        layers.append({
            "step": step,
            "scope": step_scope.get(step, "shot"),
            "total": tot,
            "done": bc.get("done", 0),
            "progress": bc.get("progress", 0),
            "todo": bc.get("todo", 0),
            "pct_done": round(100 * bc.get("done", 0) / tot, 1) if tot else 0.0,
            "pct_prog": round(100 * bc.get("progress", 0) / tot, 1) if tot else 0.0,
            "by_status": dict(step_status.get(step, {})),
        })
        seen_steps.add(step)

    for s in SHOT_STEP_ORDER + ASSET_STEP_ORDER:
        if s in step_bucket and s not in seen_steps:
            _add_layer(s)
    for s in sorted(step_bucket):
        if s not in seen_steps:
            _add_layer(s)

    return {
        "project": {"id": PROJECT_ID, "name": "1881_LORIN_SPINOFF",
                    "label": "LORIN // SPINOFF", "status": "Active"},
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {"tasks": total, "shots": len(shots), "assets": len(assets),
                   "versions": len(vlist)},
        "status_meta": STATUS_META,
        "status_order": [c for c in STATUS_ORDER if status_counts.get(c)]
                        + [c for c in status_counts if c not in STATUS_META],
        "status_counts": dict(status_counts),
        "buckets": {"done": buckets.get("done", 0),
                    "progress": buckets.get("progress", 0),
                    "todo": buckets.get("todo", 0)},
        "progress_pct": progress_pct,
        "steps": dict(step_counts),
        "layers": layers,
        "preview": _preview_media(),
        "shots": {"columns": _order_columns(shot_cols, SHOT_STEP_ORDER),
                  "rows": rows_sorted(shot_rows)},
        "assets": {"columns": _order_columns(asset_cols, ASSET_STEP_ORDER),
                   "rows": rows_sorted(asset_rows)},
        "versions": vlist,
    }


def refresh() -> dict:
    with _refresh_lock:
        snap = build_snapshot()
        _cache["snapshot"] = snap
        try:
            SNAPSHOT_FILE.write_text(json.dumps(snap), encoding="utf-8")
        except Exception:
            pass
        return snap


def load_cache():
    if _cache["snapshot"]:
        return _cache["snapshot"]
    if SNAPSHOT_FILE.exists():
        try:
            _cache["snapshot"] = json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8"))
        except Exception:
            _cache["snapshot"] = None
    return _cache["snapshot"]


def _entity_image(etype: str, eid: int):
    token = _get_token()
    r = requests.get(
        f"{SG_URL}/api/v1.1/entity/{etype}/{eid}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={"fields": "image"}, timeout=20,
    )
    if r.status_code != 200:
        return None
    return (r.json().get("data") or {}).get("attributes", {}).get("image")


def _media_url(vid: int, kind: str):
    field = "sg_uploaded_movie" if kind == "movie" else "image"
    token = _get_token()
    r = requests.get(
        f"{SG_URL}/api/v1.1/entity/Version/{vid}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={"fields": field}, timeout=20,
    )
    if r.status_code != 200:
        return None
    val = (r.json().get("data") or {}).get("attributes", {}).get(field)
    if isinstance(val, dict):
        return val.get("url")
    return val  # image is a plain URL string


# ── Questions store ──────────────────────────────────────────────────────────
def _db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS questions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            text TEXT NOT NULL,
            author TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT,
            resolved_at TEXT)""")


init_db()


class QIn(BaseModel):
    text: str
    author: str | None = None


class QPatch(BaseModel):
    text: str | None = None
    status: str | None = None


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(title="Lorin Spinoff Dashboard")
app.mount("/fonts", StaticFiles(directory=str(BASE_DIR / "fonts")), name="fonts")
app.mount("/media", StaticFiles(directory=str(MEDIA_DIR)), name="media")


@app.get("/", response_class=HTMLResponse)
def index():
    return (BASE_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/api/data")
def api_data():
    snap = load_cache() or refresh()
    return JSONResponse(snap)


@app.post("/api/refresh")
def api_refresh():
    return JSONResponse(refresh())


@app.get("/api/media/{vid}")
def api_media(vid: int, kind: str = "movie"):
    url = _media_url(vid, "movie" if kind == "movie" else "thumb")
    if not url:
        raise HTTPException(404, "media not found")
    return RedirectResponse(url)


@app.get("/api/thumb/{etype}/{eid}")
def api_thumb(etype: str, eid: int):
    if etype not in ("Shot", "Asset", "Version"):
        raise HTTPException(404, "bad entity type")
    url = _entity_image(etype, eid)
    if not url:
        raise HTTPException(404, "no thumbnail")
    return RedirectResponse(url)


@app.get("/api/questions")
def q_list():
    with _db() as c:
        rows = c.execute(
            "SELECT * FROM questions ORDER BY (status='resolved') ASC, id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/questions")
def q_add(q: QIn):
    text = (q.text or "").strip()
    if not text:
        raise HTTPException(400, "empty text")
    author = (q.author or "").strip() or None
    with _db() as c:
        cur = c.execute(
            "INSERT INTO questions(text,author,status,created_at) VALUES(?,?,'open',?)",
            (text, author, _now()))
        row = c.execute("SELECT * FROM questions WHERE id=?", (cur.lastrowid,)).fetchone()
    return dict(row)


@app.patch("/api/questions/{qid}")
def q_patch(qid: int, q: QPatch):
    with _db() as c:
        cur = c.execute("SELECT * FROM questions WHERE id=?", (qid,)).fetchone()
        if not cur:
            raise HTTPException(404, "not found")
        text = q.text.strip() if q.text is not None else cur["text"]
        status = q.status if q.status in ("open", "resolved") else cur["status"]
        resolved_at = cur["resolved_at"]
        if status == "resolved" and cur["status"] != "resolved":
            resolved_at = _now()
        if status == "open":
            resolved_at = None
        c.execute("UPDATE questions SET text=?,status=?,resolved_at=? WHERE id=?",
                  (text, status, resolved_at, qid))
        row = c.execute("SELECT * FROM questions WHERE id=?", (qid,)).fetchone()
    return dict(row)


@app.delete("/api/questions/{qid}")
def q_del(qid: int):
    with _db() as c:
        c.execute("DELETE FROM questions WHERE id=?", (qid,))
    return {"ok": True}


@app.get("/api/health")
def health():
    return {"ok": True, "project": PROJECT_ID}
