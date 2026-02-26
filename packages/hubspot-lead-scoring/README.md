# HubSpot Lead Scoring

Event-driven lead scoring pipeline for HubSpot leads. Listens for webhook events on the Lead object, resolves associated Contact and Company for enrichment, runs scoring modules, and stores results locally.

## How it works

1. **Webhook listener** receives HubSpot `lead.creation` and `lead.propertyChange` events
2. **HubSpot client** fetches Lead → associated Contact (for forms, title, email) → Company (for size/revenue)
3. **Lead type router** classifies using Lead's `hs_lead_type` first, then falls back to Contact analytics/form data
4. **Scoring modules** run based on lead type:
   - **Opportunity Size** (all types) — form submission signals + company size/revenue
   - **Message Analysis** (inbound only, if message exists) — Claude API analyzes free-text message
   - **Person/Role Lookup** (all types) — title from HubSpot, or Claude web search fallback
5. **Composite score** (0-100) computed with configurable weights per lead type
6. **SQLite database** stores the scored record locally, keyed by Lead ID

### Data flow

```
Lead (primary object)
  → associated Contact (enrichment: job title, email, forms, analytics source)
  → associated Company (enrichment: employee count, revenue, industry)
```

Lead properties take precedence over Contact properties when both exist.

### Lead type routing

| Lead Type | Modules Run | Weights |
|-----------|-------------|---------|
| Inbound (form submission) | Size + Message + Person | 0.3 / 0.4 / 0.3 |
| Product (signup/trial) | Size + Person | 0.5 / 0.5 |
| Event (conference) | Size + Person | 0.5 / 0.5 |
| Other | Size + Person | 0.5 / 0.5 |

Weights are configured in `config.yaml` and can be changed without code edits.

## Setup

### 1. Install dependencies

```bash
cd packages/hubspot-lead-scoring
pip install -r requirements.txt
```

### 2. Configure environment

All environment variables live in the **root `.env`** file (shared across the monorepo). Add these to your root `.env`:

```
ANTHROPIC_API_KEY=sk-ant-...    # Required for Claude scoring modules
WEBHOOK_PORT=3000               # Optional, default 3000
WEBHOOK_SECRET=                 # Optional, for HubSpot signature verification
DATABASE_PATH=./scores.db       # Optional, default ./scores.db
```

`HUBSPOT_ACCESS_TOKEN` should already be set in the root `.env`. See the root `.env.example` for all available variables.

### 3. Run the webhook server

```bash
python server.py
```

The server starts on `http://localhost:3000` (or your configured port).

### 4. Expose locally with ngrok (for HubSpot webhooks)

HubSpot needs a public URL to send webhooks. Use ngrok to tunnel:

```bash
ngrok http 3000
```

Then configure the ngrok HTTPS URL in your HubSpot app's webhook subscriptions:
- URL: `https://<your-ngrok-id>.ngrok.io/webhook`
- Subscriptions: `lead.creation`, `lead.propertyChange`

**Note:** Lead API requires Sales Hub Professional or Enterprise.

## Manual test mode

Score a single lead without needing live webhooks:

```bash
# Basic usage
python cli.py --lead=12345

# With raw inputs in output
python cli.py --lead=12345 --verbose
```

This fetches the Lead from HubSpot, resolves its associated Contact and Company, runs the full pipeline, prints the result, and stores it in the database.

## Inspecting scores

The server exposes read endpoints:

```bash
# List recent scores
curl http://localhost:3000/scores

# Get a specific score
curl http://localhost:3000/scores/12345
```

Or query the SQLite database directly:

```bash
sqlite3 scores.db "SELECT hubspot_record_id, lead_type, score, scored_at FROM scored_records ORDER BY scored_at DESC LIMIT 10;"
```

## Configuration

### Scoring weights (`config.yaml`)

Edit `config.yaml` to tune weights, scoring tiers, and seniority mappings. All changes take effect on the next scoring run without restarting the server.

### Prompt template (`prompts/message_analysis.txt`)

The Claude prompt for message analysis lives in its own file. Edit it to adjust what signals are extracted and how they're scored.

## File structure

```
hubspot-lead-scoring/
  server.py                  # Flask webhook server
  cli.py                     # Manual test CLI
  config.yaml                # Scoring weights and thresholds
  requirements.txt           # Python dependencies
  prompts/
    message_analysis.txt     # Claude prompt template
  scoring/
    __init__.py
    config.py                # YAML config loader
    database.py              # SQLite storage layer
    hubspot_client.py        # HubSpot API client (Lead → Contact → Company)
    claude_client.py         # Anthropic API client
    router.py                # Lead type classification + module routing
    pipeline.py              # Orchestrator with dedup + error handling
    score_opportunity_size.py # Opportunity size scoring module
    score_message.py         # Message analysis scoring module
    score_person_role.py     # Person/role lookup scoring module
```

## Error handling

- If a scoring module fails, the pipeline still computes a score from the modules that succeeded
- Weights are automatically redistributed across successful modules
- Errors are logged to stderr and recorded in the `raw_inputs.errors` field
- Duplicate webhook events for the same lead within 60s (configurable) are deduplicated
