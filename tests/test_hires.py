"""Offline test: the decoder against simulated NOMADS/AWS files. python -m pytest -q"""
import json, os, re, sys, types
from datetime import datetime, timezone
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import slw_hires as P  # noqa: E402

LAT = np.arange(24, 31, 0.05); LON = np.arange(-83, -77, 0.05)
LONS, LATS = np.meshgrid(LON, LAT)


class Msg:
    def __init__(self, var, val, step=None, anal=None):
        self.values = np.full(LATS.shape, float(val)); self.step = step; self.analDate = anal
    def latlons(self): return LATS, LONS
    def __getitem__(self, k):
        return {"stepRange": self.step, "latitudeOfFirstGridPointInDegrees": 24.0,
                "longitudeOfFirstGridPointInDegrees": -83.0}[k]


CT = {}
VALS = {"TMP": 300.15, "DPT": 293.15, "UGRD": 3.0, "VGRD": 4.0, "GUST": 12.0, "TCDC": 55.0,
        "LCDC": 20.0, "MCDC": 35.0, "HCDC": 50.0, "VIS": 16000.0, "CAPE": 900.0}


def idx_for(fh):
    recs = [("TMP", "2 m above ground"), ("DPT", "2 m above ground"), ("UGRD", "10 m above ground"),
            ("VGRD", "10 m above ground"), ("GUST", "surface"), ("UGRD", "80 m above ground"),
            ("VGRD", "80 m above ground"), ("TCDC", "entire atmosphere (considered as a single layer)"),
            ("LCDC", "low cloud layer"), ("MCDC", "middle cloud layer"), ("HCDC", "high cloud layer"),
            ("VIS", "surface"), ("CAPE", "surface")]
    recs = [(v, l, f"{fh} hour fcst") for v, l in recs]
    recs.append(("APCP", "surface", f"0-{fh} hour acc fcst" if fh % 24 else f"0-{fh // 24} day acc fcst"))
    if fh <= 24:
        recs.append(("APCP", "surface", f"{fh - 1}-{fh} hour acc fcst"))
    lines = [f"{n + 1}:{n * 100}:d=2027021012:{v}:{l}:{f}:" for n, (v, l, f) in enumerate(recs)]
    return ("\n".join(lines) + "\n").encode(), recs


calls = {"http": 0}


def fake_http(url, rng=None, timeout=60, tries=3):
    calls["http"] += 1
    if "rrfs" in url and "20270210/12" in url:
        if "amazonaws" in url and url.endswith(".idx"):
            return None
        if "/rrfs/para/" not in url and "amazonaws" not in url:
            return None
        fh = int(re.search(r"\.f(\d{3})\.", url).group(1))
        raw, recs = idx_for(fh)
        if url.endswith(".idx"):
            return raw
        v, l, f = recs[rng[0] // 100]
        val = (fh * 1.0 if f.startswith("0-") else 1.0) if v == "APCP" else VALS[v]
        return b"GRIB" + f"{v}|{val}".encode() + b"7777"
    if "spc_post" in url and url.endswith("/") and "20270210" in url and "/para/" in url and "/refsct/" in url:
        return b'<a href="spc_post.t12z.refsct.f001.grib2">a</a>'
    if url.endswith("spc_post.t12z.refsct.f001.grib2"):
        anal = datetime(2027, 2, 10, 12)
        CT[b"GRIB-R"] = [Msg("T", 0.2, "1-2", anal), Msg("T", 0.4, "0-4", anal), Msg("T", 0.9, "0-24", anal)]
        return b"GRIB-R"
    return None


class Opened:
    def __init__(self, path): self.msgs = CT[open(path, "rb").read()]
    def __iter__(self): return iter(self.msgs)
    def close(self): pass


def test_decoder_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setenv("STORAGE", "local"); monkeypatch.setenv("LOCAL_OUT", str(tmp_path))
    monkeypatch.setattr(P, "http", fake_http)
    monkeypatch.setattr(P, "pygrib", types.SimpleNamespace(
        fromstring=lambda b: Msg(*b[4:-4].decode().split("|")), open=lambda p: Opened(p)))
    P._grid_cache.clear()
    store = P.Store()
    store.put("sites.json", {"sites": [{"id": "pad:SLC-40", "latitude": 28.56, "longitude": -80.58},
                                       {"id": "view:cape:0", "latitude": 28.41, "longitude": -80.59},
                                       {"id": "pad:SLC-4E", "latitude": 34.63, "longitude": -120.61}]})
    now = datetime(2027, 2, 10, 18, 30, tzinfo=timezone.utc)
    assert P.main(store, now) == 0
    rrfs, refs, ct = store.get("rrfs.json"), store.get("refs.json"), store.get("refsct.json")
    assert rrfs["run"] == refs["run"] == "2027-02-10T12:00Z"
    m = rrfs["sites"]["pad:SLC-40"]["members"][0]
    assert m["temperature_2m"][0] == 27.0 and m["dew_point_2m"][0] == 20.0
    assert m["wind_speed_10m"][0] == 5.0 and m["wind_gusts_10m"][0] == 12.0
    assert (m["cloud_cover"][0], m["cloud_cover_low"][0], m["cloud_cover_mid"][0], m["cloud_cover_high"][0]) == (55, 20, 35, 50)
    assert m["precipitation"][0] == 1.0 and m["precipitation"][24] == 1.0      # hourly rate, then from 3 h totals
    assert len(refs["sites"]["view:cape:0"]["members"]) == 5
    assert "pad:SLC-4E" in rrfs["outside"]                                     # Vandenberg is off this fake grid
    per = ct["sites"]["pad:SLC-40"]["periods"]
    assert [(p["hours"], p["p"]) for p in per] == [(1, 20.0), (4, 40.0)]          # fraction -> percent, 24 h dropped
    # second run: everything current, nothing re-downloaded beyond the checks
    calls["http"] = 0
    assert P.main(store, now) == 0
    assert calls["http"] < 20
    assert store.get("status.json")["sources"]["rrfs"]["current"] is True
