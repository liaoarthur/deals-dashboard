# GTM Engineering Monorepo

Monorepo for go-to-market engineering tools. Shared utilities in `packages/core`, applications in their own packages.

## Repository Structure

```
deals-dashboard/
├── packages/
│   ├── core/                              # Shared Python library (pip: gtm-core)
│   │   ├── cache.py                       # In-memory cache helpers
│   │   ├── databricks.py                  # Databricks connection factory
│   │   ├── contacts.py                    # Contact dedup + validation
│   │   ├── hubspot.py                     # HubSpot API wrappers + deal search
│   │   ├── definitive.py                  # Definitive Healthcare queries
│   │   ├── specialty.py                   # LLM-powered specialty expansion
│   │   ├── scoring.py                     # Similarity scoring engine
│   │   └── lookalikes.py                  # Lookalike matching engine
│   │
│   └── lookalike-prospecting/             # Deals dashboard application
│       ├── server.py                      # Flask app + MCP server
│       ├── requirements.txt               # Python deps
│       ├── generate_password.py           # Auth password hash generator
│       ├── test_databricks.py             # Databricks connectivity test
│       └── static/                        # Frontend (served by Vercel)
│           ├── index.html                 # Dashboard UI
│           ├── login.html                 # Login page
│           └── inject-env.js              # Vercel build-time env injection
│
├── requirements.txt                       # Root: chains core + app deps
├── Procfile                               # Railway entry point
├── railway.json                           # Railway deploy config
├── vercel.json                            # Vercel deploy config
├── pyproject.toml                         # Workspace metadata
├── .env.example                           # Environment variable docs
└── .gitignore
```

## Architecture

```
Vercel (Frontend)              Railway (Backend)                    Clay
┌──────────────────┐           ┌──────────────────────────┐       ┌──────────────┐
│  static/         │──HTTPS──> │  server.py (Flask)        │       │  HTTP API    │
│  index.html      │           │  ├── /api/deals           │       │  Table       │
│  login.html      │           │  ├── /api/lookalikes       │       └──────────────┘
│                  │           │  ├── /api/filters          │             │
│                  │           │  ├── /api/clay-seed        │  trigger    │
│                  │  poll     │  ├── /api/check-clay-search│ ──────────>│
│                  │────────>  │  ├── /api/trigger-clay-    │            │
│                  │           │  │    search                │  callback  │
│                  │<────────  │  ├── /api/clay-contact-    │ <──────────│
│                  │           │  │    result                │
│                  │           │  └── /health               │
│                  │           │                            │
│                  │           │  imports from core:         │
│                  │           │  ├── HubSpot API wrappers   │
│                  │           │  ├── Databricks queries     │
│                  │           │  ├── Similarity scoring     │
│                  │           │  └── OpenAI specialty LLM   │
└──────────────────┘           └──────────────────────────┘
```

## Local Development

1. Clone and set up:
   ```bash
   git clone https://github.com/liaoarthur/vibe-coding-gtme-rs.git
   cd vibe-coding-gtme-rs
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt   # installs core + app deps
   ```

2. Create `.env` from template:
   ```bash
   cp .env.example .env
   # Fill in your API keys
   ```

3. Run the Flask server:
   ```bash
   cd packages/lookalike-prospecting
   python server.py --flask-only
   # Server runs on http://localhost:5001
   ```

## Deployment

### Backend: Railway

1. Create a new project on [Railway](https://railway.app)
2. Connect your GitHub repo
3. Set environment variables (see `.env.example` for full list):

   | Variable | Description |
   |----------|-------------|
   | `HUBSPOT_ACCESS_TOKEN` | HubSpot private app token |
   | `DATABRICKS_TOKEN` | Databricks personal access token |
   | `DATABRICKS_SERVER_HOSTNAME` | Databricks server hostname |
   | `DATABRICKS_HTTP_PATH` | Databricks SQL warehouse HTTP path |
   | `OPENAI_API_KEY` | OpenAI API key (optional) |
   | `CLAY_WEBHOOK_URL` | Clay webhook URL (optional) |
   | `ALLOWED_ORIGINS` | Your Vercel URL |
   | `SECRET_KEY` | Session signing key |
   | `AUTH_USERS` | `email:passwordhash` pairs (see [Authentication](#authentication)) |

4. Railway auto-detects `requirements.txt` at root and deploys with gunicorn
5. Verify: `curl https://your-app.up.railway.app/health`

### Frontend: Vercel

1. Create a new project on [Vercel](https://vercel.com)
2. Connect your GitHub repo, set **Framework Preset** to "Other"
3. Set `NEXT_PUBLIC_API_BASE_URL` to your Railway URL
4. Deploy — Vercel runs `inject-env.js` at build time
5. Add your Vercel URL to Railway's `ALLOWED_ORIGINS`

## Authentication

Flask session-based auth with secure cross-origin cookies.

1. Generate a password hash:
   ```bash
   python packages/lookalike-prospecting/generate_password.py mypassword
   ```

2. Set `AUTH_USERS` in Railway:
   ```
   AUTH_USERS=admin@co.com:pbkdf2:sha256:...,user@co.com:pbkdf2:sha256:...
   ```

3. Set `SECRET_KEY` in Railway for stable session signing.

### Auth Details
- `POST /api/login` — validates credentials, sets session cookie (24h expiry)
- `GET /api/check-auth` — frontend verifies session on page load
- `POST /api/logout` — clears session
- All API routes require `@require_auth` except `/health` and `/api/clay-contact-result`
- Rate limiting: 5 login attempts per IP per 60 seconds
- Cross-origin cookies: `SameSite=None`, `Secure=True`, `HttpOnly=True`

## MCP Server Mode

For Claude Desktop, add to your MCP config:
```json
{
  "mcpServers": {
    "gtm-intelligence": {
      "command": "python",
      "args": ["/path/to/packages/lookalike-prospecting/server.py"]
    }
  }
}
```

## Adding a New Package

1. Create `packages/your-project/` with its own `requirements.txt`:
   ```
   -e ../../packages/core
   flask>=3.0.0
   ```
2. Import shared utilities: `from core.hubspot import search_hubspot_deals`
3. Add its own Railway service or run as a standalone script
