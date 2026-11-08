#!/usr/bin/env python3
"""Space Launch Weather high-resolution guidance (public repo).

Runs hourly in GitHub Actions. Reads the list of points (pads, viewing spots, recovery areas)
that the main pipeline publishes, decodes the newest runs of these sources at each point's
nearest grid point, and writes one JSON file per source back to the hi-res bucket:

  rrfs     RRFS deterministic, 3 km, hourly to 24 h then 3-hourly to 84 h
  refs     RRFS ensemble members m001-m005 (the REFS membership), to 60 h
  refsct   SPC REFS calibrated thunder, 1 h and 4 h periods, about 60 h

Per member and hour: 2 m temperature and dew point, 10 m and 80 m wind, gust, total / low /
middle / high cloud cover, visibility, CAPE and the hourly precipitation rate.

Only the GRIB messages that are needed are downloaded, by byte range, using each file's
inventory (.idx): from AWS when the inventory matches, otherwise from NOMADS at a polite
request rate. A source whose newest run is already stored for every point is skipped, so
most hourly runs take a minute or two.

This repo is public: it holds no forecasts, keys or limits, only the decoder. Its one secret
(an R2 token) is scoped to the hi-res bucket and stored as a GitHub Actions secret.

Environment
  STORAGE=local | r2           local writes to ./out for testing
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET (the hi-res bucket)
  HIRES_BUDGET_MIN             stop starting new work after this many minutes (default 40)
"""
import json
import math
import os
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np

try:
    import pygrib
except ImportError:  # tests inject a stand-in
    pygrib = None

NOMADS = "https://nomads.ncep.noaa.gov/pub/data/nccf/com"
AWS = "https://noaa-rrfs-ops-pds.s3.amazonaws.com"
RRFS_BASES = [f"{NOMADS}/rrfs/prod", f"{NOMADS}/rrfs/v1.0", f"{NOMADS}/rrfs/para"]
SPC_BASES = [f"{NOMADS}/spc_post/prod", f"{NOMADS}/spc_post/para"]
UA = {"User-Agent": "space-launch-weather-hires (GitHub Actions)"}
BUDGET_S = float(os.environ.get("HIRES_BUDGET_MIN", "40")) * 60
MAX_GRID_DIST_DEG = 0.25
T0 = time.time()
LOG = lambda *a: print(*a, flush=True)  # noqa: E731


def time_left():
    return BUDGET_S - (time.time() - T0)


# --------------------------------------------------------------------------- storage
class Store:
    def __init__(self):
        self.local = os.environ.get("STORAGE", "r2").lower() == "local"
        if self.local:
            self.root = os.environ.get("LOCAL_OUT", "out")
            os.makedirs(self.root, exist_ok=True)
        else:
            import boto3
            self.bucket = os.environ["R2_BUCKET"]
            self.s3 = boto3.client("s3", endpoint_url=f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
                                   aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
                                   aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto")

    def get(self, key):
        try:
            if self.local:
                with open(os.path.join(self.root, key)) as fp:
                    return json.load(fp)
            return json.loads(self.s3.get_object(Bucket=self.bucket, Key=key)["Body"].read())
        except Exception:  # noqa: BLE001 - missing is normal
            return None

    def put(self, key, obj):
        data = json.dumps(obj, separators=(",", ":")).encode()
        if self.local:
            with open(os.path.join(self.root, key), "wb") as fp:
                fp.write(data)
        else:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="application/json")


# --------------------------------------------------------------------------- HTTP
_nomads_lock = threading.Lock()
_nomads_last = [0.0]


def http(url, rng=None, timeout=60, tries=3):
    """GET. Returns bytes, or None on 403/404 or repeated failure."""
    for attempt in range(tries):
        if "nomads.ncep.noaa.gov" in url:
            with _nomads_lock:  # NOMADS asks for a modest request rate
                wait = 0.6 - (time.time() - _nomads_last[0])
                if wait > 0:
                    time.sleep(wait)
                _nomads_last[0] = time.time()
        req = urllib.request.Request(url, headers=UA)
        if rng:
            req.add_header("Range", f"bytes={rng[0]}-{rng[1] if rng[1] is not None else ''}")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code in (403, 404):
                return None
        except Exception as e:  # noqa: BLE001
            if attempt == tries - 1:
                LOG(f"  {url} failed: {e}")
        time.sleep(2 * (attempt + 1))
    return None


# --------------------------------------------------------------------------- GRIB access
def parse_idx(raw):
    recs = []
    for line in raw.decode("utf-8", "replace").splitlines():
        parts = line.split(":")
        if len(parts) < 6 or not parts[1].isdigit():
            continue
        recs.append({"start": int(parts[1]), "var": parts[3], "level": parts[4], "fcst": parts[5]})
    for i, r in enumerate(recs):
        r["end"] = recs[i + 1]["start"] - 1 if i + 1 < len(recs) else None
    return recs


def is_grib(b):
    return bool(b) and b[:4] == b"GRIB"


class Fetcher:
    """Reads inventories and byte ranges, preferring AWS and falling back to NOMADS."""

    def __init__(self):
        self.aws_ok = True

    def inventory(self, suffix, bases):
        if self.aws_ok:
            raw = http(f"{AWS}/{suffix}.idx", timeout=30)
            if raw:
                return parse_idx(raw), f"{AWS}/{suffix}"
        for b in bases:
            raw = http(f"{b}/{suffix}.idx", timeout=30)
            if raw:
                return parse_idx(raw), f"{b}/{suffix}"
        return None, None

    def message(self, url, rec, suffix):
        rng = (rec["start"], rec["end"])
        if "nomads" in url and self.aws_ok:
            b = http(f"{AWS}/{suffix}", rng=rng, timeout=60)
            if is_grib(b):
                return b
            if b is not None:
                LOG("  AWS bytes do not match the NOMADS inventory; using NOMADS")
                self.aws_ok = False
        b = http(url, rng=rng, timeout=90)
        return b if is_grib(b) else None


_grid_cache = {}
_grid_lock = threading.Lock()
SEEN = {"inside": set(), "outside": set()}


def site_indices(msg, sites):
    values = msg.values
    try:
        key = (values.shape, round(float(msg["latitudeOfFirstGridPointInDegrees"]), 4),
               round(float(msg["longitudeOfFirstGridPointInDegrees"]), 4))
    except Exception:  # noqa: BLE001
        key = (values.shape,)
    with _grid_lock:
        if key in _grid_cache:
            return _grid_cache[key]
        lats, lons = msg.latlons()
        lats = np.asarray(lats, dtype=float).ravel()
        lons = ((np.asarray(lons, dtype=float).ravel() + 180.0) % 360.0) - 180.0
        out = {}
        for s in sites:
            coslat = math.cos(math.radians(s["latitude"]))
            d2 = (lats - s["latitude"]) ** 2 + ((lons - s["longitude"]) * coslat) ** 2
            i = int(np.argmin(d2))
            out[s["id"]] = i if math.sqrt(d2[i]) <= MAX_GRID_DIST_DEG else None
        _grid_cache[key] = out
        return out


def point_values(msg, sites):
    vals = np.ma.filled(np.ma.asarray(msg.values, dtype=float), np.nan).ravel()
    idx = site_indices(msg, sites)
    out = {}
    for s in sites:
        i = idx.get(s["id"])
        SEEN["outside" if i is None else "inside"].add(s["id"])
        v = None if i is None else float(vals[i])
        out[s["id"]] = None if v is None or math.isnan(v) else v
    return out


def decode_one(b):
    return pygrib.fromstring(b)


# --------------------------------------------------------------------------- RRFS / REFS
FIELDS = [  # key, GRIB variable, level (prefix match)
    ("t", "TMP", "2 m above ground"), ("td", "DPT", "2 m above ground"),
    ("u10", "UGRD", "10 m above ground"), ("v10", "VGRD", "10 m above ground"),
    ("gust", "GUST", "surface"),
    ("u80", "UGRD", "80 m above ground"), ("v80", "VGRD", "80 m above ground"),
    ("tcc", "TCDC", "entire atmosphere"), ("lcc", "LCDC", "low cloud layer"),
    ("mcc", "MCDC", "middle cloud layer"), ("hcc", "HCDC", "high cloud layer"),
    ("vis", "VIS", "surface"), ("cape", "CAPE", "surface"),
]
OUT_VARS = ("temperature_2m", "dew_point_2m", "wind_speed_10m", "wind_gusts_10m", "wind_speed_80m",
            "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high", "visibility", "cape",
            "precipitation")
_INSTANT = re.compile(r"^(\d+) (hour|day) fcst$")
_PERIOD = re.compile(r"^(\d+)-(\d+) (hour|day) (acc|max|ave) fcst$")


def _hours(n, unit):
    return int(n) * (24 if unit == "day" else 1)


def find_record(recs, var, level, fh):
    for r in recs:
        if r["var"] != var or not r["level"].startswith(level):
            continue
        m = _INSTANT.match(r["fcst"])
        if m and _hours(m.group(1), m.group(2)) == fh:
            return r
        m = _PERIOD.match(r["fcst"])
        if m and m.group(4) in ("max", "ave") and _hours(m.group(2), m.group(3)) == fh:
            return r
    return None


def precip_records(recs, fh):
    out = []
    for r in recs:
        if r["var"] != "APCP" or not r["level"].startswith("surface"):
            continue
        m = _PERIOD.match(r["fcst"])
        if m and m.group(4) == "acc":
            a, b = _hours(m.group(1), m.group(3)), _hours(m.group(2), m.group(3))
            if b == fh:
                out.append((r, a, b))
    return sorted(out, key=lambda x: x[2] - x[1])


def cycles(now, step=6, back=5):
    c0 = now.replace(minute=0, second=0, microsecond=0)
    c0 -= timedelta(hours=c0.hour % step)
    return [c0 - timedelta(hours=step * i) for i in range(back)]


def rrfs_suffix(cyc, fh, member=None):
    d, cc = cyc.strftime("%Y%m%d"), cyc.strftime("%H")
    if member is None:
        return f"rrfs.{d}/{cc}/rrfs.t{cc}z.2dfld.3km.f{fh:03d}.conus.grib2"
    m = f"m{member:03d}"
    return f"rrfsens.{d}/{cc}/{m}/rrfs.t{cc}z.{m}.2dfldnomads.3km.f{fh:03d}.conus.grib2"


def forecast_hours(maxh):
    return list(range(1, 25)) + list(range(27, maxh + 1, 3))


def newest_cycle(fx, now, maxh, member=None):
    for cyc in cycles(now):
        recs, _ = fx.inventory(rrfs_suffix(cyc, maxh, member), RRFS_BASES)
        if recs:
            return cyc
    return None


def run_rrfs(fx, cyc, maxh, sites, member=None):
    """Decode one RRFS run (deterministic or one member) at every site -> times, {site: {var: [...]}}."""
    hours = forecast_hours(maxh)

    def one_hour(fh):
        suffix = rrfs_suffix(cyc, fh, member)
        recs, url = fx.inventory(suffix, RRFS_BASES)
        if not recs:
            return fh, None
        got = {}
        for key, var, level in FIELDS:
            r = find_record(recs, var, level, fh)
            if r is None:
                continue
            b = fx.message(url, r, suffix)
            if b:
                got[key] = point_values(decode_one(b), sites)
        precip = []
        for r, a, bh in precip_records(recs, fh):
            b = fx.message(url, r, suffix)
            if b:
                precip.append((a, bh, point_values(decode_one(b), sites)))
                break
        return fh, (got, precip)

    with ThreadPoolExecutor(max_workers=6 if fx.aws_ok else 2) as pool:
        results = dict(pool.map(one_hour, hours))

    times = [int((cyc + timedelta(hours=fh)).timestamp() * 1000) for fh in hours]
    per_site = {s["id"]: {k: [] for k in OUT_VARS} for s in sites}
    totals = {s["id"]: {0: 0.0} for s in sites}
    prev_fh = 0
    rnd = lambda v, n=1: None if v is None else round(v, n)  # noqa: E731
    for fh in hours:
        got, precip = results.get(fh) or ({}, [])
        for s in sites:
            sid = s["id"]
            g = lambda k: (got.get(k) or {}).get(sid)  # noqa: E731
            t, td, u10, v10, u80, v80 = g("t"), g("td"), g("u10"), g("v10"), g("u80"), g("v80")
            row = per_site[sid]
            row["temperature_2m"].append(None if t is None else round(t - 273.15, 2))
            row["dew_point_2m"].append(None if td is None else round(td - 273.15, 2))
            row["wind_speed_10m"].append(None if u10 is None or v10 is None else round(math.hypot(u10, v10), 2))
            row["wind_gusts_10m"].append(rnd(g("gust"), 2))
            row["wind_speed_80m"].append(None if u80 is None or v80 is None else round(math.hypot(u80, v80), 2))
            for k, out in (("tcc", "cloud_cover"), ("lcc", "cloud_cover_low"), ("mcc", "cloud_cover_mid"), ("hcc", "cloud_cover_high")):
                row[out].append(rnd(g(k)))
            row["visibility"].append(None if g("vis") is None else round(g("vis")))
            row["cape"].append(None if g("cape") is None else round(g("cape")))
            rate = None
            if precip:
                a, bh, vals = precip[0]
                v = vals.get(sid)
                if v is not None:
                    if a == 0:
                        totals[sid][fh] = v
                        start_total = totals[sid].get(prev_fh)
                        if start_total is not None and fh > prev_fh:
                            rate = max(0.0, (v - start_total) / (fh - prev_fh))
                    else:
                        rate = max(0.0, v / (bh - a))
                        if prev_fh in totals[sid]:
                            totals[sid][fh] = totals[sid][prev_fh] + v
            row["precipitation"].append(None if rate is None else round(rate, 3))
        prev_fh = fh
    found = sum(1 for fh in hours if results.get(fh))
    return times, per_site, found, len(hours)


def has_data(row):
    return any(v is not None for v in row["temperature_2m"])


def build_rrfs(fx, cyc, sites):
    LOG(f"RRFS {cyc:%Y-%m-%d %HZ}")
    times, per_site, found, total = run_rrfs(fx, cyc, 84, sites)
    LOG(f"  {found}/{total} forecast hours")
    if found < total // 2:
        return None
    return {sid: {"times": times, "members": [row]} for sid, row in per_site.items() if has_data(row)}


def build_refs(fx, cyc, sites):
    LOG(f"REFS members {cyc:%Y-%m-%d %HZ}")
    out = {s["id"]: {"times": None, "members": []} for s in sites}
    for m in range(1, 6):
        if time_left() < 120:
            LOG("  time budget reached; keeping the members finished so far")
            break
        times, per_site, found, total = run_rrfs(fx, cyc, 60, sites, member=m)
        LOG(f"  m{m:03d}: {found}/{total} forecast hours")
        if found < total // 2:
            continue
        for sid, row in per_site.items():
            if has_data(row):
                out[sid]["times"] = times
                out[sid]["members"].append(row)
    out = {sid: p for sid, p in out.items() if len(p["members"]) >= 3}
    return out or None


# --------------------------------------------------------------------------- calibrated thunder
def latest_thunder_files(word, subdir, now):
    for day in (now, now - timedelta(days=1)):
        for base in SPC_BASES:
            url = f"{base}/spc_post.{day:%Y%m%d}/{subdir}/"
            raw = http(url, timeout=30)
            if not raw:
                continue
            names = sorted(set(re.findall(r'href="([^"/]+\.grib2)"', raw.decode("utf-8", "replace"))))
            names = [n for n in names if word in n] or names
            by_cycle = {}
            for n in names:
                m = re.search(r"\.t(\d\d)z\.", n)
                if m:
                    by_cycle.setdefault(int(m.group(1)), []).append(n)
            if by_cycle:
                cc = max(by_cycle)
                return datetime(day.year, day.month, day.day, cc, tzinfo=timezone.utc), [url + n for n in by_cycle[cc][:150]]
    return None, []


def build_thunder(word, cyc, urls, sites):
    LOG(f"{word.upper()} {cyc:%Y-%m-%d %HZ}: {len(urls)} files")
    periods = {s["id"]: {} for s in sites}
    for url in urls:
        if time_left() < 60:
            break
        raw = http(url, timeout=90)
        if not is_grib(raw):
            continue
        with tempfile.NamedTemporaryFile(suffix=".grib2") as tmp:
            tmp.write(raw)
            tmp.flush()
            grbs = pygrib.open(tmp.name)
            try:
                for msg in grbs:
                    m = re.match(r"^(\d+)-(\d+)$", str(msg["stepRange"]))
                    if not m:
                        continue
                    a, b = int(m.group(1)), int(m.group(2))
                    if b - a not in (1, 4):
                        continue
                    end = int((msg.analDate.replace(tzinfo=timezone.utc) + timedelta(hours=b)).timestamp() * 1000)
                    vals = point_values(msg, sites)
                    finite = np.ma.filled(np.ma.asarray(msg.values, dtype=float), np.nan)
                    scale = 100.0 if np.nanmax(finite) <= 1.0 else 1.0  # fraction or percent
                    for sid, v in vals.items():
                        if v is not None:
                            periods[sid][(end, b - a)] = round(max(0.0, min(100.0, v * scale)), 1)
            finally:
                grbs.close()
    out = {sid: {"periods": [{"end": e, "hours": h, "p": p} for (e, h), p in sorted(d.items())]}
           for sid, d in periods.items() if d}
    return out or None


# --------------------------------------------------------------------------- main
def run_key(cyc):
    return cyc.strftime("%Y-%m-%dT%H:%MZ")


def main(store=None, now=None):
    if pygrib is None:
        LOG("pygrib is not installed (pip install pygrib).")
        return 2
    store = store or Store()
    listing = store.get("sites.json") or {}
    sites = [s for s in listing.get("sites", []) if 15 <= s["latitude"] <= 60 and -140 <= s["longitude"] <= -50]
    if not sites:
        LOG("No points inside the RRFS domain in sites.json yet; the main pipeline writes it.")
        return 0
    now = now or datetime.now(timezone.utc)
    fx = Fetcher()
    LOG(f"{len(sites)} points")
    status = {"run_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "sources": {}}

    def stored(source):
        d = store.get(f"{source}.json") or {}
        return d.get("run"), set((d.get("sites") or {}).keys()) | set(d.get("outside") or [])

    def current(source, cyc):
        run, ids = stored(source)
        return run == run_key(cyc) and {s["id"] for s in sites} <= ids

    def write(source, cyc, payload):
        store.put(f"{source}.json", {"source": source, "run": run_key(cyc), "generated": status["run_at"],
                                     "sites": payload, "outside": sorted(SEEN["outside"] - SEEN["inside"])})
        status["sources"][source] = {"run": run_key(cyc), "points": len(payload)}
        LOG(f"  stored {source} {run_key(cyc)} for {len(payload)} points")

    jobs = [("refsct", "thunder"), ("rrfs", "members"), ("refs", "members")]
    for source, _kind in jobs:
        if time_left() < 120:
            LOG(f"Time budget reached before {source}.")
            break
        SEEN["inside"].clear()
        SEEN["outside"].clear()
        try:
            if source == "refsct":
                cyc, urls = latest_thunder_files("refsct", "refsct", now)
                if not urls:
                    LOG("refsct: no files found"); continue
                if current(source, cyc):
                    LOG("refsct: newest run already stored"); status["sources"][source] = {"run": run_key(cyc), "current": True}; continue
                payload = build_thunder("refsct", cyc, urls, sites)
            else:
                maxh = 84 if source == "rrfs" else 60
                cyc = newest_cycle(fx, now, maxh, member=None if source == "rrfs" else 1)
                if not cyc:
                    LOG(f"{source}: no complete run found"); continue
                if current(source, cyc):
                    LOG(f"{source}: newest run already stored"); status["sources"][source] = {"run": run_key(cyc), "current": True}; continue
                payload = (build_rrfs if source == "rrfs" else build_refs)(fx, cyc, sites)
        except Exception as e:  # noqa: BLE001 - one bad source should not stop the others
            LOG(f"{source} failed: {e}")
            continue
        if payload:
            write(source, cyc, payload)
        else:
            LOG(f"{source}: no usable data")
    store.put("status.json", status)
    LOG(f"Done in {time.time() - T0:.0f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
