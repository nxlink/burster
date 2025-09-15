# burster

Utilities to populate and swap RADIUS policy tables from a plans source DB. Updated for Python 3 with `.env`-based configuration and a virtual environment.

## Setup

- Create venv and install deps:
  - `bash scripts/setup_venv.sh`
  - `source .venv/bin/activate`
- Configure env vars:
  - `cp .env.example .env`
  - Fill in DB creds and burster settings in `.env`.

## Configuration (.env)

- BB DB: `BBDB_HOST`, `BBDB_DB`, `BBDB_USER`, `BBDB_PASS`
- RADIUS DB: `RADDB_HOST`, `RADDB_DB`, `RADDB_USER`, `RADDB_PASS`
- Burster: `BURSTER_SBP`, `BURSTER_BURST_PERIOD`, `BURSTER_BOOST_PERC`, `BURSTER_SESSION_TIMEOUT`, `BURSTER_FRAMED_POOL`
- Optional: `BURSTER_PERCENT` (default `100`), `BURSTER_CONFIG_PATH` (legacy INI merge; not required)
- Deploy: `DEPLOY_REMOTE`, `DEPLOY_SOURCE_DIR` (default `.`), `DEPLOY_EXCLUDE_FILE` (default `/etc/deploy-exclude.txt`)

All parameters are read from `.env`. The legacy `burster.cfg` has been removed; you can still point to an INI with `BURSTER_CONFIG_PATH` if desired—`.env` values take precedence.

## Running

- Activate venv: `source .venv/bin/activate`
- Run job: `python burster.py -p 100` or set `BURSTER_PERCENT` in `.env`
- Shows a progress bar and logs high-level progress to syslog and stderr.

### Logging

- Controlled by `BURSTER_LOG_LEVEL` (e.g., `INFO`, `DEBUG`).
- Logs to syslog via `/dev/log` when available, otherwise UDP `localhost:514`.

## Notes

- Driver: uses PyMySQL with a `MySQLdb` compatibility shim for minimal native deps.
- Secrets: `.env` is in `.gitignore` and should not be committed.
- Deployment script was removed; deploy/copy files using your own process (e.g., rsync or CI).
