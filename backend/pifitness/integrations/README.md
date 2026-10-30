# pifitness.integrations

Per-service login and credential-management integrations.

Each service (Garmin, Spotify, future additions) gets its own subpackage here
implementing a shared interface. Centralizes:

- Token/session lifecycle (reuse if valid -> refresh if expired -> re-auth if refresh fails)
- Provider-change isolation (Spotify API changes touch spotify/ only; Garmin changes touch garmin/ only)
- Rate-limit backoff (per-service, so a 429 from one does not stall the other)

## Not yet populated

Scaffolded in INT-0. Subpackages land in later blocks. See docs/reorg/blocks/ for the plan.