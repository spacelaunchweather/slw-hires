# slw-hires

Public decoder for Space Launch Weather's high-resolution guidance: NOAA's RRFS, the five REFS
ensemble members and SPC REFS calibrated thunder, at each launch pad, viewing spot and recovery
area. It runs hourly in GitHub Actions and writes one JSON file per source to a dedicated R2
bucket, where the main (private) pipeline picks them up.

Nothing here is secret or proprietary: no forecasts, limits or customer data, just a GRIB
decoder. It's public so GitHub Actions minutes are free.

## Files in the bucket
| Key | Written by | What |
|---|---|---|
| `sites.json` | the main pipeline | the points to decode: `{"sites": [{"id", "latitude", "longitude"}]}` |
| `rrfs.json`, `refs.json` | this repo | `{run, sites: {id: {times, members: [{var: [...]}]}}, outside: [...]}` |
| `refsct.json` | this repo | `{run, sites: {id: {periods: [{end, hours, p}]}}}` |
| `status.json` | this repo | the newest run of each source, for the studio's pipeline panel |

Per member and hour: temperature and dew point (°C), 10 m and 80 m wind and gust (m/s), total,
low, middle and high cloud (%), visibility (m), CAPE (J/kg) and precipitation rate (mm/h).

## Setup
1. Cloudflare → R2 → create a bucket `space-launch-weather-hires`, and an API token with
   **Object Read & Write on that bucket only**.
2. In this repo: Settings → Secrets and variables → Actions → add `R2_ACCOUNT_ID`,
   `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and `R2_BUCKET` = `space-launch-weather-hires`.
3. Actions tab → **High-resolution guidance** → Run workflow. The first run decodes everything
   (15–40 minutes); later runs skip what's already stored.

Test offline: `pip install -r requirements.txt && python -m pytest -q`.

Data: NOAA/NCEP RRFS and REFS, and NOAA SPC calibrated thunder (public domain), from NOMADS and
the NOAA Open Data Dissemination program on AWS.
