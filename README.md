# GTM Intelligence MCP Server

MCP server + Flask dashboard connecting HubSpot CRM with Databricks (Definitive Healthcare). Provides Sales and CS teams with lookalike prospecting and account health analysis.

## Architecture

```
Vercel (Frontend)              Railway (Backend)                    Clay
┌──────────────────┐           ┌──────────────────────────┐       ┌──────────────┐
│  index.html      │──HTTPS──> │  server.py (Flask)        │       │  HTTP API    │
│  (static React)  │           │  ├── /api/deals           │       │  Table       │
│                  │           │  ├── /api/lookalikes       │       └──────────────┘
│                  │           │  ├── /api/filters          │             │
│                  │           │  ├── /api/clay-seed        │  trigger    │
│ [Search          │  poll     │  ├── /api/check-clay-search│ ──────────>│
│  Contacts] ─────>│────────>  │  ├── /api/trigger-clay-    │            │
│                  │           │  │    search                │  callback  │
│ [show contacts]  │<────────  │  ├── /api/clay-contact-    │ <──────────│
│                  │           │  │    result                │
│                  │           │  ├── /api/clay-search-      │
│                  │           │  │    status/:key           │
│                  │           │  └── /health               │
│                  │           │                            │
│                  │           │  HubSpot API ──────>       │
│                  │           │  Databricks SQL ────>      │
│                  │           │  OpenAI API ────────>      │
│                  │           │  Redis (optional) ──>      │
└──────────────────┘           └──────────────────────────┘
```

## Local Development

1. Clone and set up:
   ```bash
   git clone https://github.com/liaoarthur/vibe-coding-gtme-rs.git
   cd vibe-coding-gtme-rs
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. Create `.env` from template:
   ```bash
   cp .env.example .env
   # Fill in your API keys
   ```

3. Run the Flask server:
   ```bash
   python server.py --flask-only
   # Server runs on http://localhost:5001
   ```

4. Open `dashboard.html` in your browser (or serve it locally).

## Deployment

### Backend: Railway

1. Create a new project on [Railway](https://railway.app)
2. Connect your GitHub repo
3. Set environment variables in Railway dashboard:

   | Variable | Description |
   |----------|-------------|
   | `HUBSPOT_ACCESS_TOKEN` | HubSpot private app token |
   | `HUBSPOT_PORTAL_ID` | HubSpot portal ID |
   | `DATABRICKS_TOKEN` | Databricks personal access token |
   | `DATABRICKS_SERVER_HOSTNAME` | Databricks server hostname |
   | `DATABRICKS_HTTP_PATH` | Databricks SQL warehouse HTTP path |
   | `OPENAI_API_KEY` | OpenAI API key (optional) |
   | `CLAY_WEBHOOK_URL` | Clay webhook URL (optional) |
   | `ALLOWED_ORIGINS` | Your Vercel URL, e.g. `https://your-app.vercel.app` |
   | `FLASK_DEBUG` | `false` |

4. Railway auto-detects `railway.json` and deploys with gunicorn
5. Note your Railway URL (e.g. `https://your-app.up.railway.app`)
6. Verify: `curl https://your-app.up.railway.app/health`

**Optional: Redis add-on**
- Add Redis from Railway's add-on marketplace
- Railway auto-sets `REDIS_HOST` and `REDIS_PORT`

### Frontend: Vercel

1. Create a new project on [Vercel](https://vercel.com)
2. Connect your GitHub repo
3. Set **Framework Preset** to "Other"
4. Set environment variable:

   | Variable | Value |
   |----------|-------|
   | `NEXT_PUBLIC_API_BASE_URL` | Your Railway URL (e.g. `https://your-app.up.railway.app`) |

5. Deploy — Vercel runs `inject-env.js` at build time to bake the API URL into the HTML
6. Copy your Vercel URL and add it to Railway's `ALLOWED_ORIGINS`

### Post-Deploy Checklist

- [ ] Railway `/health` returns `{"status": "healthy"}`
- [ ] Vercel dashboard loads and shows deals
- [ ] Lookalike search returns results
- [ ] Clay seed buttons work (if `CLAY_WEBHOOK_URL` is set)
- [ ] Clay contact search triggers and polls correctly (if `CLAY_WEBHOOK_URL` is set)
- [ ] Railway `ALLOWED_ORIGINS` includes your Vercel domain

## MCP Server Mode

For use with Claude Desktop, add to your MCP config:
```json
{
  "mcpServers": {
    "gtm-intelligence": {
      "command": "python",
      "args": ["/absolute/path/to/server.py"]
    }
  }
}
```

## Clay Contact Search (HTTP API Integration)

The Clay Contact Search feature lets users search for contacts at a company via Clay's enrichment engine. Results are cached and deduplicated so repeated searches don't waste Clay credits.

### How It Works

```
1. User clicks "Search Contacts" on a deal card
2. Frontend → POST /api/check-clay-search (check cache)
3. If cached → show results immediately + "Re-run?" prompt
4. If not cached → POST /api/trigger-clay-search → sends data to Clay webhook
5. Clay enriches the company → POSTs contacts back to /api/clay-contact-result
6. Frontend polls GET /api/clay-search-status/<key> every 3 seconds
7. Contacts are deduplicated by email and LinkedIn URL
8. Results cached in-memory + Redis (30-day TTL)
```

### Setting Up the Clay Table

1. **Create a Clay table** with an HTTP API trigger (webhook input)

2. **Add enrichment columns** in Clay to find contacts for the company (e.g., "Find People at Company", LinkedIn search, etc.)

3. **Add an HTTP API output column** (the callback) with these settings:

   | Setting | Value |
   |---------|-------|
   | **Endpoint URL** | `https://web-production-768f7.up.railway.app/api/clay-contact-result` |
   | **Method** | `POST` |
   | **Headers** | `Content-Type: application/json` |

4. **Configure the callback body** — the `company_key` field is critical and must be passed through from the webhook input:

   **Single contact per row (recommended):**
   ```json
   {
     "company_key": "{{company_key}}",
     "name": "{{Full Name}}",
     "email": "{{Work Email}}",
     "phone": "{{Phone Number}}",
     "title": "{{Job Title}}",
     "linkedin": "{{LinkedIn URL}}"
   }
   ```

   **Batch format (if sending multiple contacts at once):**
   ```json
   {
     "company_key": "{{company_key}}",
     "contacts": [
       {
         "name": "{{Full Name}}",
         "email": "{{Work Email}}",
         "phone": "{{Phone Number}}",
         "title": "{{Job Title}}",
         "linkedin": "{{LinkedIn URL}}"
       }
     ]
   }
   ```

   Replace `{{variable}}` with your actual Clay column references.

5. **Set the `CLAY_WEBHOOK_URL` environment variable** in Railway to your Clay table's webhook URL (the URL Clay gives you for the HTTP API trigger).

### Company Key

The `company_key` is an MD5 hash of `company_name|state|city` (all lowercased and trimmed). It's generated by the backend and sent to Clay as part of the webhook payload. **Clay must pass this value through unchanged** in the callback so the backend can match contacts to the original search request.

### Deduplication

Contacts are deduplicated before being stored:
- If a contact's **email** matches an existing contact → skip (keep original)
- If a contact's **LinkedIn URL** matches an existing contact → skip (keep original)
- New contacts get a `first_seen` timestamp; existing contacts retain their original `first_seen`

### Caching

- **In-memory**: Always active, keyed by `company_key`
- **Redis**: If configured (Railway Redis add-on), persists across server restarts with 30-day TTL
- **Multi-worker**: With `gunicorn --workers 2`, Redis ensures cache coherence across workers
- **Re-run**: Users can force a re-search which clears the cache and triggers a fresh Clay search
