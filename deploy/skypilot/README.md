# Runbook: Airflow on a temporary DigitalOcean VM

The full guide, with diagrams, security notes and troubleshooting, is
[docs/34-deploy-digitalocean-skypilot.md](../../docs/34-deploy-digitalocean-skypilot.md).
This page is the short version.

## Files

| File | Purpose |
|---|---|
| `airflow-do.yaml` | SkyPilot task: droplet size, setup (Docker, firewall, secrets) and run (`docker compose up`) |
| `docker-compose.remote.yaml` | Binds the UI (8080) and Postgres (5432) to `127.0.0.1` on the VM, so nothing is public |
| `../../scripts/cloud_expiry.sh` | Prints when the VM will delete itself |
| `../../.cloud.mk` (git-ignored, you create it) | Your machine's settings, e.g. `SKY := /path/to/sky` |

## First time on a new computer

1. `pip install "skypilot[do]"`. If `sky` isn't on your PATH, put `SKY := /full/path/to/sky` in `.cloud.mk`.
2. Copy the DigitalOcean token from Bitwarden to doctl's config (never printed):

   ```bash
   ( umask 077; mkdir -p "$HOME/Library/Application Support/doctl"; printf 'access-token: %s\n' "$(bws secret list -o json | jq -r '.[] | select(.key=="DIGITALOCEAN_ACCESS_TOKEN") | .value')" > "$HOME/Library/Application Support/doctl/config.yaml" )
   ```

3. `make cloud-check` should print `DO: enabled`.

## Every session

```bash
make cloud-up          # ~6-8 min; the VM deletes itself after 45 min (CLOUD_TTL=...)
```

```bash
make cloud-tunnel      # keep open; UI at http://localhost:8080
```

```bash
make cloud-password    # login: airflow / <this password>
```

| Need | Command |
|---|---|
| Time left | `make cloud-expiry` |
| 45 more minutes from now | `make cloud-extend` |
| Push DAG changes | `make cloud-sync` |
| Delete now | `make cloud-down` |
| Confirm nothing is billing | `bws run -- doctl compute droplet list` and `bws run -- doctl compute volume list` should both be empty (the disk may take ~30 s) |

## If something goes wrong

| Symptom | Fix |
|---|---|
| `command not found: sky` | Set `SKY` in `.cloud.mk`, or activate SkyPilot's venv |
| Setup failed: `Could not get lock /var/lib/dpkg/lock-frontend` | Run `make cloud-up` again (same VM; setup waits for boot-time updates) |
| Ctrl-C during launch, VM still created | `sky api cancel <request-id>`, then `make cloud-down` |
| Scheduler `unhealthy` right after launch | Normal for about a minute; it starts last |
| Port 8080 busy locally | `make cloud-tunnel TUNNEL_PORT=18080` |
| Tunnel says `Operation timed out`, or `Could not resolve hostname airflow-do` | The VM's time ran out (or it was deleted); `make cloud-status` shows no clusters |
