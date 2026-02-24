import asyncio
import os
import sys
import json
import requests
import secrets
import time

from datetime import datetime, timedelta
import redis
from flask import Flask, request, jsonify, session
from flask_cors import CORS
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from collections import defaultdict

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from dotenv import load_dotenv

# ─── Core library imports (shared across GTM projects) ──────────────────────
from core.cache import get_cache_key, get_cached_result, set_cached_result
from core.databricks import get_databricks_connection
from core.contacts import deduplicate_contacts, has_valid_contact_info
from core.hubspot import (
    DEAL_PROPERTIES, FILTER_PROPERTIES,
    get_hubspot_mappings, get_owner_name,
    get_specialty_property_info, get_specialty_label,
    search_hubspot_deals,
)
from core.definitive import get_organizations_from_definitive, get_organization_contacts
from core.specialty import get_definitive_specialties, get_expanded_specialties
from core.scoring import score_single_org, calculate_similarity_score, is_specialty_similar, get_match_reasons
from core.lookalikes import find_lookalikes_from_company_data

# Load credentials from .env file
load_dotenv()

# Load environment variables
HUBSPOT_API_KEY = os.getenv('HUBSPOT_ACCESS_TOKEN')

# Check if API key exists
if not HUBSPOT_API_KEY:
    raise ValueError("HUBSPOT_API_KEY environment variable not set")

# Clay webhook for company discovery (optional)
CLAY_WEBHOOK_URL = os.getenv('CLAY_WEBHOOK_URL')

# Create the server
app = Server("gtm-mcp-server")
flask_app = Flask(__name__)

# ─── Session & Authentication Configuration ────────────────────────────────────
# SECRET_KEY: used for signing session cookies. Generate one with:
#   python -c "import secrets; print(secrets.token_hex(32))"
# Set as SECRET_KEY env var in Railway. Falls back to random key (sessions won't
# survive server restarts without a stable key).
flask_app.secret_key = os.getenv('SECRET_KEY', secrets.token_hex(32))
flask_app.config.update(
    SESSION_COOKIE_SECURE=True,       # Only send cookie over HTTPS
    SESSION_COOKIE_HTTPONLY=True,      # JavaScript can't read the cookie (XSS protection)
    SESSION_COOKIE_SAMESITE='None',    # Required for cross-origin Vercel→Railway cookies
    PERMANENT_SESSION_LIFETIME=timedelta(hours=24),  # Session expires after 24 hours
)

# AUTH_USERS: comma-separated email:passwordhash pairs.
# Generate hashes with: python generate_password.py <password>
# Format: "admin@co.com:pbkdf2:sha256:...,user@co.com:pbkdf2:sha256:..."
AUTH_USERS = {}
_raw_users = os.getenv('AUTH_USERS', '')
if _raw_users:
    for entry in _raw_users.split(','):
        entry = entry.strip()
        if ':' in entry:
            # Split on first colon only — password hashes contain colons
            email, pwhash = entry.split(':', 1)
            AUTH_USERS[email.strip().lower()] = pwhash.strip()
    print(f"[AUTH] Loaded {len(AUTH_USERS)} user(s): {', '.join(AUTH_USERS.keys())}", file=sys.stderr)
else:
    print("[AUTH] WARNING: AUTH_USERS not set — no users can log in", file=sys.stderr)

# ─── Rate Limiting (login endpoint) ────────────────────────────────────────────
# Simple in-memory rate limiter: max 5 login attempts per IP per 60 seconds.
_login_attempts = defaultdict(list)  # { ip: [timestamp, timestamp, ...] }
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW_SECONDS = 60


def _is_rate_limited(ip):
    """Check if an IP has exceeded login attempt limit."""
    now = time.time()
    # Prune old attempts outside the window
    _login_attempts[ip] = [t for t in _login_attempts[ip] if now - t < _LOGIN_WINDOW_SECONDS]
    return len(_login_attempts[ip]) >= _LOGIN_MAX_ATTEMPTS


def _record_login_attempt(ip):
    """Record a login attempt for rate limiting."""
    _login_attempts[ip].append(time.time())


# ─── Authentication Decorator ──────────────────────────────────────────────────
def require_auth(f):
    """
    Decorator to protect endpoints. Checks for a valid session.
    Returns 401 JSON if not authenticated — frontend catches this and redirects to login.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('authenticated'):
            return jsonify({'error': 'Unauthorized', 'code': 'AUTH_REQUIRED'}), 401
        return f(*args, **kwargs)
    return decorated


# CORS: allow Vercel frontend domain + localhost for dev
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', 'https://deals-dashboard-rho.vercel.app').split(',')
CORS(flask_app, origins=ALLOWED_ORIGINS, supports_credentials=True)
# CORS(flask_app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}}, supports_credentials=True)

# Redis setup
try:
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        decode_responses=True,
        socket_connect_timeout=2
    )
    redis_client.ping()
    REDIS_ENABLED = True
except:
    redis_client = None
    REDIS_ENABLED = False

# ─── Clay Contact Search Cache ─────────────────────────────────────────────────
# Stores contacts returned from Clay searches, keyed by the lookalike company's
# domain (website). The domain is sent to Clay in the webhook and Clay sends it
# back in the callback, so it's the natural key for matching contacts to companies.
_clay_search_cache = {}
_clay_search_ttl = 2592000  # 30 days


def get_company_key(domain):
    """
    Use the raw website/domain from Definitive Healthcare as the cache key.
    Only strips surrounding whitespace — no other normalization.
    Whatever Databricks gives us is what Clay gets and what we key on.
    """
    return (domain or '').strip()


def _save_clay_cache_to_redis(company_key, data):
    """Persist a Clay search cache entry to Redis (if available) with 30-day TTL."""
    if not REDIS_ENABLED or not redis_client:
        return
    try:
        redis_client.setex(
            f"clay_search:{company_key}",
            _clay_search_ttl,
            json.dumps(data, default=str)
        )
    except Exception as e:
        print(f"[CLAY CACHE] Redis save failed for {company_key}: {e}", file=sys.stderr)


def _load_clay_cache_from_redis(company_key):
    """
    Load a Clay search cache entry from Redis on in-memory cache miss.
    Handles multi-worker scenarios where worker A wrote the entry but worker B
    needs to read it. Returns the parsed dict or None.
    """
    if not REDIS_ENABLED or not redis_client:
        return None
    try:
        raw = redis_client.get(f"clay_search:{company_key}")
        if raw:
            data = json.loads(raw)
            # Hydrate in-memory cache so subsequent reads are fast
            _clay_search_cache[company_key] = data
            return data
    except Exception as e:
        print(f"[CLAY CACHE] Redis load failed for {company_key}: {e}", file=sys.stderr)
    return None


def _get_clay_search(company_key):
    """
    Look up a Clay search entry: check in-memory first, then Redis.
    Returns the cache dict or None.
    """
    # In-memory check
    entry = _clay_search_cache.get(company_key)
    if entry:
        return entry
    # Redis fallback (handles multi-worker)
    return _load_clay_cache_from_redis(company_key)


# ========================================
# Get Deals (with ALL stages support)
# ========================================

@flask_app.route('/api/deals', methods=['GET'])
@require_auth
def get_deals():
    """
    Get deals with filters
    Only shows deals from Sales - Global and Expansion pipelines
    
    Query params:
    - days_back (default: 14)
    - deal_stage
    - specialty_mcp_use, country, billing_state, billing_city
    - min_seats, min_tso, product, pipeline
    """

    # Parse filters
    days_back = request.args.get('days_back', 14, type=int)
    deal_stage = request.args.get('deal_stage')
    specialty = request.args.get('specialty_mcp_use')
    country = request.args.get('country')
    billing_state = request.args.get('billing_state')
    billing_city = request.args.get('billing_city')
    min_seats = request.args.get('min_seats', type=int)
    min_tso = request.args.get('min_tso', type=int)
    product = request.args.get('product')
    pipeline = request.args.get('pipeline')

    # Get mappings
    mappings = get_hubspot_mappings()

    # Build HubSpot filters
    cutoff_date_start = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)
    cutoff_date_end = int(datetime.now().timestamp() * 1000)
    
    # Create filter groups for Sales - Global OR Expansion (OR logic)
    filter_groups = []
    
    # Filter group 1: Sales - Global + all other filters
    filters_1 = [
        {
            "propertyName": "closedate",
            "operator": "BETWEEN",
            "value": cutoff_date_start,
            "highValue": cutoff_date_end
        },
        {
            "propertyName": "pipeline",
            "operator": "EQ",
            "value": "74974043"  # Sales - Global
        }
    ]
    
    # Filter group 2: Expansion + all other filters
    filters_2 = [
        {
            "propertyName": "closedate",
            "operator": "BETWEEN",
            "value": cutoff_date_start,
            "highValue": cutoff_date_end
        },
        {
            "propertyName": "pipeline",
            "operator": "EQ",
            "value": "779936085"  # Expansion
        }
    ]
    
    # Add all other filters to BOTH groups (so they apply with AND logic)
    additional_filters = []
    
    if deal_stage:
        additional_filters.append({
            "propertyName": "dealstage",
            "operator": "EQ",
            "value": deal_stage
        })
    
    if country:
        additional_filters.append({
            "propertyName": "country",
            "operator": "EQ",
            "value": country
        })
    
    if billing_state:
        additional_filters.append({
            "propertyName": "billing_state",
            "operator": "EQ",
            "value": billing_state
        })
    
    if billing_city:
        additional_filters.append({
            "propertyName": "billing_city",
            "operator": "CONTAINS_TOKEN",
            "value": billing_city
        })
    
    if product:
        additional_filters.append({
            "propertyName": "product",
            "operator": "EQ",
            "value": product
        })
    
    if specialty:
        additional_filters.append({
            "propertyName": "specialty_mcp_use",
            "operator": "CONTAINS_TOKEN",
            "value": specialty
        })
    
    # If user selected a specific pipeline in dropdown, only use that one
    if pipeline:
        if pipeline.isdigit():
            pipeline_id = pipeline
        else:
            pipeline_id = mappings['pipelines'].get(pipeline, pipeline)
        
        # Override - use only the selected pipeline
        filters_1 = [
            {
                "propertyName": "closedate",
                "operator": "BETWEEN",
                "value": cutoff_date_start,
                "highValue": cutoff_date_end
            },
            {
                "propertyName": "pipeline",
                "operator": "EQ",
                "value": pipeline_id
            }
        ]
        filters_1.extend(additional_filters)
        filter_groups = [{"filters": filters_1}]
    else:
        # Use both Sales - Global and Expansion
        filters_1.extend(additional_filters)
        filters_2.extend(additional_filters)
        filter_groups = [
            {"filters": filters_1},
            {"filters": filters_2}
        ]
    
    # Query HubSpot
    try:
        response = requests.post(
            'https://api.hubapi.com/crm/v3/objects/deals/search',
            headers={
                'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                "filterGroups": filter_groups,
                "properties": DEAL_PROPERTIES,
                "sorts": [{"propertyName": "closedate", "direction": "DESCENDING"}],
                "limit": 100
            }
        )
        
        if response.status_code != 200:
            return jsonify({'error': 'Failed to fetch deals from HubSpot', 'details': response.text}), 500
        
        deals_raw = response.json().get('results', [])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    # Batch-fetch associated company LC City / LC US State for deals missing billing location
    company_ids = set()
    for deal in deals_raw:
        cid = deal['properties'].get('associated_company_id')
        if cid:
            company_ids.add(str(cid))

    company_lc_map = {}  # company_id -> { lc_city, lc_us_state }
    if company_ids:
        try:
            batch_inputs = [{"id": cid} for cid in company_ids]
            # HubSpot batch read supports up to 100 at a time
            for i in range(0, len(batch_inputs), 100):
                batch_chunk = batch_inputs[i:i+100]
                comp_resp = requests.post(
                    'https://api.hubapi.com/crm/v3/objects/companies/batch/read',
                    headers={
                        'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                        'Content-Type': 'application/json'
                    },
                    json={
                        "inputs": batch_chunk,
                        "properties": ["lc_city", "lc_us_state", "domain"]
                    }
                )
                if comp_resp.status_code == 200:
                    for comp in comp_resp.json().get('results', []):
                        comp_props = comp.get('properties', {})
                        company_lc_map[str(comp['id'])] = {
                            'lc_city': comp_props.get('lc_city') or None,
                            'lc_us_state': comp_props.get('lc_us_state') or None,
                            'domain': comp_props.get('domain') or None
                        }
        except Exception as e:
            print(f"DEBUG: Error fetching company LC data: {e}", file=sys.stderr)

    # Format deals
    deals = []
    for deal in deals_raw:
        props = deal['properties']

        # Parse numeric values
        seats = int(float(props.get('seats_subscribed') or 0))
        tso = int(float(props.get('total_serviceable_opportunity') or 0))
        comms_seats = int(float(props.get('comms_seats') or 0))
        evidence_seats = int(float(props.get('evidence_seats') or 0))
        amount = float(props.get('amount_in_home_currency') or 0)

        # Apply client-side filters
        if min_seats and seats < min_seats:
            continue
        if min_tso and tso < min_tso:
            continue

        # Get owner name
        owner_id = props.get('hubspot_owner_id')
        owner_name = get_owner_name(owner_id) if owner_id else None

        # Get pipeline/stage names
        pipeline_id = props.get('pipeline')
        pipeline_name = mappings['pipelines'].get(pipeline_id, pipeline_id)
        dealstage_id = props.get('dealstage')
        dealstage_name = mappings['stages'].get(dealstage_id, dealstage_id)

        # Get LC City / LC US State from associated company (fallback for billing location)
        company_id = props.get('associated_company_id')
        lc_data = company_lc_map.get(str(company_id), {}) if company_id else {}

        deals.append({
            'deal_id': deal['id'],
            'deal_name': props.get('dealname'),
            'deal_url': f"https://app.hubspot.com/contacts/{os.getenv('HUBSPOT_PORTAL_ID')}/deal/{deal['id']}",
            'associated_company_id': props.get('associated_company_id'),
            'associated_company_name': props.get('associated_company_name'),
            'associated_contact_email': props.get('associated_contact_email'),
            'associated_contact_id': props.get('associated_contact_id'),
            'deal_segment': props.get('deal_segment'),
            'deal_category': props.get('deal_category'),
            'deal_type': props.get('deal_type__new'),
            'owner_id': owner_id,
            'owner_name': owner_name,
            'pipeline_id': pipeline_id,
            'pipeline_name': pipeline_name,
            'dealstage_id': dealstage_id,
            'dealstage_name': dealstage_name,
            'close_date': props.get('closedate'),
            'create_date': props.get('createdate'),
            'amount': amount,
            'country': props.get('country'),
            'billing_city': props.get('billing_city'),
            'billing_state': props.get('billing_state'),
            'billing_zip': props.get('billing_zip'),
            'lc_city': lc_data.get('lc_city'),
            'lc_us_state': lc_data.get('lc_us_state'),
            'company_domain': lc_data.get('domain'),
            'product': props.get('product'),
            'ehr': props.get('ehr'),
            'seats': seats,
            'comms_seats': comms_seats,
            'evidence_seats': evidence_seats,
            'tso': tso,
            'specialty_mcp_use': props.get('specialty_mcp_use'),
            'is_closed_won': props.get('hs_is_closed_won') == 'true',
            'is_closed_lost': props.get('hs_is_closed_lost') == 'true',
            'is_deal_closed': props.get('is_deal_closed') == 'true',
            'lead_source': props.get('lead_source')
        })
    
    return jsonify(deals)

@flask_app.route('/api/filters', methods=['GET'])
@require_auth
def get_filter_options():
    """Get correlated filter values based on current filters"""
    
    # Get ALL pipelines and stages
    mappings = get_hubspot_mappings()

    # Get all filter params
    days_back = request.args.get('days_back', 14, type=int)
    deal_stage = request.args.get('deal_stage')
    specialty = request.args.get('specialty_mcp_use')
    country = request.args.get('country')
    billing_state = request.args.get('billing_state')
    billing_city = request.args.get('billing_city')
    product = request.args.get('product')
    pipeline = request.args.get('pipeline')
    
    try:
        cutoff = int((datetime.now() - timedelta(days=days_back)).timestamp() * 1000)
        
        # Build filters based on current selection
        filters_sales = [
            {
                "propertyName": "closedate",
                "operator": "GTE",
                "value": str(cutoff)
            },
            {
                "propertyName": "pipeline",
                "operator": "EQ",
                "value": "74974043"  # Sales - Global
            }
        ]
        
        filters_expansion = [
            {
                "propertyName": "closedate",
                "operator": "GTE",
                "value": str(cutoff)
            },
            {
                "propertyName": "pipeline",
                "operator": "EQ",
                "value": "779936085"  # Expansion
            }
        ]
        
        # Add current filters to both groups
        additional_filters = []
        
        if deal_stage:
            additional_filters.append({
                "propertyName": "dealstage",
                "operator": "EQ",
                "value": deal_stage
            })
        
        if country:
            additional_filters.append({
                "propertyName": "country",
                "operator": "EQ",
                "value": country
            })
        
        if billing_state:
            additional_filters.append({
                "propertyName": "billing_state",
                "operator": "EQ",
                "value": billing_state
            })
        
        if billing_city:
            additional_filters.append({
                "propertyName": "billing_city",
                "operator": "CONTAINS_TOKEN",
                "value": billing_city
            })
        
        if product:
            additional_filters.append({
                "propertyName": "product",
                "operator": "EQ",
                "value": product
            })
        
        if specialty:
            additional_filters.append({
                "propertyName": "specialty_mcp_use",
                "operator": "CONTAINS_TOKEN",
                "value": specialty
            })
        
        filters_sales.extend(additional_filters)
        filters_expansion.extend(additional_filters)
        
        # If user selected specific pipeline, only use that
        if pipeline:
            pipeline_id = pipeline if pipeline.isdigit() else mappings['pipelines'].get(pipeline, pipeline)
            filter_groups = [{
                "filters": [
                    {
                        "propertyName": "closedate",
                        "operator": "GTE",
                        "value": str(cutoff)
                    },
                    {
                        "propertyName": "pipeline",
                        "operator": "EQ",
                        "value": pipeline_id
                    }
                ] + additional_filters
            }]
        else:
            filter_groups = [
                {"filters": filters_sales},
                {"filters": filters_expansion}
            ]
        
        response = requests.post(
            'https://api.hubapi.com/crm/v3/objects/deals/search',
            headers={
                'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                "filterGroups": filter_groups,
                "properties": [
                    "specialty_mcp_use", "country", "billing_state",
                    "billing_city", "product", "pipeline", "dealstage"
                ],
                "limit": 200
            }
        )

        if response.status_code != 200:
            return jsonify({'error': 'Failed to fetch filter options', 'details': response.text}), 500
        
        deals = response.json().get('results', [])
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
    # Extract unique values
    specialties = set()
    countries = set()
    billing_states = set()
    billing_cities = set()
    products = set()
    
    for deal in deals:
        props = deal['properties']
        
        # Specialty - split by semicolon
        if props.get('specialty_mcp_use'):
            specialty_value = props['specialty_mcp_use']
            for spec in str(specialty_value).split(';'):
                spec_trimmed = spec.strip()
                if spec_trimmed:
                    specialties.add(spec_trimmed)
        
        if props.get('country'):
            countries.add(props['country'])
        
        if props.get('billing_state'):
            billing_states.add(props['billing_state'])
        
        if props.get('billing_city'):
            billing_cities.add(props['billing_city'])
        
        if props.get('product'):
            products.add(props['product'])
    
    # Build pipeline options
    
    pipeline_options = []
    seen_pipeline_ids = set()
    for key, value in mappings['pipelines'].items():
        if isinstance(key, str) and key not in seen_pipeline_ids:
            pipeline_options.append({
                'id': mappings['pipelines'].get(key),
                'name': key
            })
            seen_pipeline_ids.add(mappings['pipelines'].get(key))

    allowed_pipelines = ['Sales - Global', 'Expansion']
    pipeline_options = [
        p for p in pipeline_options 
        if p['name'] in allowed_pipelines
    ]

    # DEAL STAGES: Only show stages from Sales - Global and Expansion pipelines
    deal_stage_options = [
        stage for stage in mappings['stage_list']
        if stage['pipeline'] in allowed_pipelines
    ]
    
    # Format for response
    deal_stage_options_formatted = [
        {
            'id': stage['id'],
            'label': stage['label'],
            'pipeline': stage['pipeline'],
            'pipeline_id': stage['pipeline_id']
        }
        for stage in deal_stage_options
    ]

    # Build deal stage options
    # deal_stage_options = [
    #     {
    #         'id': stage['id'],
    #         'label': stage['label'],
    #         'pipeline': stage['pipeline'],
    #         'pipeline_id': stage['pipeline_id']
    #     }
    #     for stage in mappings['stage_list']
    # ]
    
    filter_options = {
        'specialties': sorted(specialties),
        'countries': sorted(countries),
        'billing_states': sorted(billing_states),
        'billing_cities': sorted(billing_cities),
        'products': sorted(products),
        'pipelines': sorted(pipeline_options, key=lambda x: x['name']),
        'deal_stages': deal_stage_options_formatted
    }
    
    return jsonify(filter_options)

# ─── Authentication Endpoints ──────────────────────────────────────────────────

@flask_app.route('/api/login', methods=['POST'])
def login():
    """
    Authenticate a user with email and password.
    Sets a signed session cookie on success.

    POST body (JSON):
    - email (required)
    - password (required)

    Rate limited: 5 attempts per IP per 60 seconds.
    """
    ip = request.remote_addr or 'unknown'

    # Rate limiting check
    if _is_rate_limited(ip):
        print(f"[AUTH] Rate limited: {ip}", file=sys.stderr)
        return jsonify({'error': 'Too many login attempts. Try again in 1 minute.'}), 429

    data = request.get_json()
    if not data:
        _record_login_attempt(ip)
        return jsonify({'error': 'Request body is required'}), 400

    email = (data.get('email') or '').strip().lower()
    password = data.get('password') or ''

    if not email or not password:
        _record_login_attempt(ip)
        return jsonify({'error': 'Email and password are required'}), 400

    # Look up user
    stored_hash = AUTH_USERS.get(email)
    if not stored_hash or not check_password_hash(stored_hash, password):
        _record_login_attempt(ip)
        print(f"[AUTH] Failed login attempt for '{email}' from {ip}", file=sys.stderr)
        return jsonify({'error': 'Invalid email or password'}), 401

    # Success — set session
    session.permanent = True  # Use PERMANENT_SESSION_LIFETIME (24h)
    session['authenticated'] = True
    session['email'] = email
    session['login_time'] = datetime.utcnow().isoformat()

    print(f"[AUTH] Successful login: {email} from {ip}", file=sys.stderr)
    return jsonify({'success': True, 'email': email})


@flask_app.route('/api/logout', methods=['POST'])
def logout():
    """Clear the session and log the user out."""
    email = session.get('email', 'unknown')
    session.clear()
    print(f"[AUTH] Logged out: {email}", file=sys.stderr)
    return jsonify({'success': True})


@flask_app.route('/api/check-auth', methods=['GET'])
def check_auth():
    """
    Check if the current session is authenticated.
    Called by the frontend on page load to decide whether to show
    the dashboard or redirect to the login page.
    """
    if session.get('authenticated'):
        return jsonify({
            'authenticated': True,
            'email': session.get('email')
        })
    return jsonify({'authenticated': False}), 401


@flask_app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'redis': REDIS_ENABLED,
        'openai': bool(os.getenv('OPENAI_API_KEY')),
        'clay': CLAY_WEBHOOK_URL is not None,
        'auth_users_configured': len(AUTH_USERS) > 0,
    })

@flask_app.route('/api/clay-seed', methods=['POST'])
@require_auth
def clay_seed():
    """
    Send seed data to a Clay table via webhook.
    Supports both deal-level seeding and per-lookalike-company seeding.

    POST body (JSON) — all fields are forwarded to Clay webhook.
    Common fields:
    - state (required): state for the company/deal
    - company_name, company_domain, city, specialty
    - source: 'lookalike' for per-company seeding

    Lookalike-specific fields:
    - physician_count, ehr, definitive_id, similarity_score
    """
    if not CLAY_WEBHOOK_URL:
        return jsonify({'error': 'CLAY_WEBHOOK_URL not configured'}), 500

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    state = data.get('state')
    if not state:
        return jsonify({'error': 'state is required'}), 400

    # Pass through all fields from request to Clay webhook
    payload = {k: v for k, v in data.items() if v is not None and v != ''}

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"[CLAY SEED] Sending to Clay webhook:", file=sys.stderr)
    print(f"  {json.dumps(payload, indent=2)}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    try:
        resp = requests.post(
            CLAY_WEBHOOK_URL,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        if resp.status_code in (200, 201, 202):
            return jsonify({'success': True, 'message': 'Seed sent to Clay'})
        else:
            return jsonify({'error': f'Clay webhook returned {resp.status_code}', 'details': resp.text}), 502
    except requests.exceptions.Timeout:
        return jsonify({'error': 'Clay webhook timed out'}), 504
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ─── Clay Contact Search Endpoints ─────────────────────────────────────────────
# These endpoints implement a caching layer for Clay contact searches.
# The company_key is the lookalike company's DOMAIN (website), normalized.
# This is what gets sent to Clay in the webhook, and what Clay sends back.
#
# Flow: check-cache → trigger-search → Clay calls back → poll for results
#
# 1. Frontend POSTs to /api/check-clay-search with { domain } to check cache
# 2. If not cached, frontend POSTs to /api/trigger-clay-search to kick off Clay
# 3. Clay enriches the data and POSTs contacts back to /api/clay-contact-result
# 4. Frontend polls /api/clay-search-status/<domain> every 3s until status="complete"

@flask_app.route('/api/check-clay-search', methods=['POST'])
@require_auth
def check_clay_search():
    """
    Check if a lookalike company has been searched before in Clay.
    Returns cached contacts if found, or {found: false} if not.

    POST body (JSON):
    - domain (required): the lookalike company's website/domain
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    domain = (data.get('domain') or '').strip()
    if not domain:
        return jsonify({'error': 'domain is required'}), 400

    company_key = get_company_key(domain)
    if not company_key:
        return jsonify({'error': 'Invalid domain'}), 400

    print(f"[CLAY CHECK] Checking cache for domain={company_key}", file=sys.stderr)

    # Look up in-memory cache, then Redis
    entry = _get_clay_search(company_key)

    if entry:
        print(f"[CLAY CHECK] Cache HIT — status={entry.get('status')}, contacts={len(entry.get('contacts', []))}", file=sys.stderr)
        return jsonify({
            'found': True,
            'company_key': company_key,
            'status': entry.get('status', 'complete'),
            'searched_at': entry.get('searched_at'),
            'contact_count': len(entry.get('contacts', [])),
            'contacts': entry.get('contacts', [])
        })
    else:
        print(f"[CLAY CHECK] Cache MISS for domain={company_key}", file=sys.stderr)
        return jsonify({
            'found': False,
            'company_key': company_key
        })


@flask_app.route('/api/trigger-clay-search', methods=['POST'])
@require_auth
def trigger_clay_search():
    """
    Trigger a new Clay contact search for a lookalike company.
    Sends company data to the Clay webhook with the domain as company_key.
    Clay will enrich it and POST contacts back to /api/clay-contact-result.

    POST body (JSON):
    - domain (required): the lookalike company's website/domain (used as key)
    - company_name (optional): company name for context
    - state (optional): state abbreviation
    - city (optional): city name
    - specialty (optional): medical specialty
    - force (optional, bool): if true, re-run even if cached results exist
    """
    if not CLAY_WEBHOOK_URL:
        return jsonify({'error': 'CLAY_WEBHOOK_URL not configured — cannot trigger Clay search'}), 500

    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    domain = (data.get('domain') or '').strip()
    if not domain:
        return jsonify({'error': 'domain is required'}), 400

    company_key = get_company_key(domain)
    if not company_key:
        return jsonify({'error': 'Invalid domain'}), 400

    company_name = (data.get('company_name') or '').strip()
    state = (data.get('state') or '').strip()
    city = (data.get('city') or '').strip()
    specialty = (data.get('specialty') or '').strip()
    force = data.get('force', False)

    # If not forcing, check if we already have results
    if not force:
        existing = _get_clay_search(company_key)
        if existing and existing.get('status') == 'complete':
            print(f"[CLAY TRIGGER] Already cached for domain={company_key}, returning cached flag", file=sys.stderr)
            return jsonify({
                'already_cached': True,
                'company_key': company_key,
                'message': 'Results already cached — use check-clay-search to retrieve'
            })

    # Create/reset cache entry with "searching" status
    now = datetime.utcnow().isoformat()
    cache_entry = {
        'domain': company_key,
        'company_name': company_name,
        'state': state,
        'city': city,
        'specialty': specialty,
        'searched_at': now,
        'status': 'searching',
        'contacts': []  # Start empty; Clay will fill via callback
    }
    _clay_search_cache[company_key] = cache_entry
    _save_clay_cache_to_redis(company_key, cache_entry)

    # Build payload for Clay webhook — company_key IS the domain
    # Clay must pass company_key back unchanged in its callback
    payload = {
        'company_key': company_key,
        'company_name': company_name,
        'domain': company_key,
        'state': state,
        'city': city,
        'specialty': specialty,
        'source': 'clay_contact_search',
    }

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"[CLAY TRIGGER] Sending search request to Clay webhook:", file=sys.stderr)
    print(f"  company_key (domain): {company_key}", file=sys.stderr)
    print(f"  payload: {json.dumps(payload, indent=2)}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    try:
        resp = requests.post(
            CLAY_WEBHOOK_URL,
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        if resp.status_code in (200, 201, 202):
            return jsonify({
                'success': True,
                'company_key': company_key,
                'message': 'Search triggered — poll /api/clay-search-status for results'
            })
        else:
            # Reset status since Clay didn't accept the request
            cache_entry['status'] = 'error'
            _clay_search_cache[company_key] = cache_entry
            return jsonify({
                'error': f'Clay webhook returned {resp.status_code}',
                'details': resp.text
            }), 502
    except requests.exceptions.Timeout:
        cache_entry['status'] = 'error'
        _clay_search_cache[company_key] = cache_entry
        return jsonify({'error': 'Clay webhook timed out'}), 504
    except Exception as e:
        cache_entry['status'] = 'error'
        _clay_search_cache[company_key] = cache_entry
        return jsonify({'error': str(e)}), 500


@flask_app.route('/api/clay-contact-result', methods=['POST'])
def clay_contact_result():
    """
    Receive enriched contacts from Clay's HTTP API callback.
    Called by Clay after it processes a contact search request.

    The company_key in the payload is the DOMAIN that was sent to Clay.
    Clay must pass it back unchanged so we can match contacts to the company.

    Clay sends one row per contact with these keys:
       {
         "company_key": "acme.com",
         "full_name": "Dr. Jane Smith",
         "title": "Chief Medical Officer",
         "email": "jane@acme.com",
         "phone": "555-0123",
         "linkedin": "https://linkedin.com/in/janesmith"
       }

    Fields are normalized to internal names: full_name → name.
    Contacts are deduplicated by email or LinkedIn URL before merging.
    """
    data = request.get_json()
    if not data:
        return jsonify({'error': 'Request body is required'}), 400

    raw_key = (data.get('company_key') or '').strip()
    if not raw_key:
        return jsonify({'error': 'company_key (domain) is required'}), 400

    # Normalize the domain key in case Clay passes it back slightly differently
    company_key = get_company_key(raw_key)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"[CLAY CALLBACK] Received contact data for domain={company_key}", file=sys.stderr)
    print(f"  Raw payload: {json.dumps(data, indent=2, default=str)}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # Map Clay field names → internal field names
    # Clay sends: full_name, title, email, phone, linkedin
    # We store:   name,      title, email, phone, linkedin
    CLAY_FIELD_MAP = {
        'full_name': 'name',
        'title': 'title',
        'email': 'email',
        'phone': 'phone',
        'linkedin': 'linkedin',
        # Also accept our internal names in case of direct API calls
        'name': 'name',
    }

    def _normalize_contact(raw):
        """Convert a Clay contact dict to our internal format."""
        contact = {}
        for clay_key, internal_key in CLAY_FIELD_MAP.items():
            val = raw.get(clay_key)
            if val and str(val).strip():
                # Don't overwrite if we already have this field (name vs full_name)
                if internal_key not in contact:
                    contact[internal_key] = str(val).strip()
        return contact

    # Parse contacts — Clay sends one row per webhook call (single-contact format)
    incoming_contacts = []
    if 'contacts' in data and isinstance(data['contacts'], list):
        # Batch format (rare): { company_key, contacts: [...] }
        for raw in data['contacts']:
            c = _normalize_contact(raw)
            if c:
                incoming_contacts.append(c)
    else:
        # Single-contact format (standard Clay row): { company_key, full_name, title, ... }
        c = _normalize_contact(data)
        if c:
            incoming_contacts = [c]

    if not incoming_contacts:
        print(f"[CLAY CALLBACK] No contacts in payload for domain={company_key}", file=sys.stderr)
        # Still mark as complete even with 0 contacts (Clay found nothing)
        entry = _get_clay_search(company_key)
        if entry:
            entry['status'] = 'complete'
            _clay_search_cache[company_key] = entry
            _save_clay_cache_to_redis(company_key, entry)
        return jsonify({'success': True, 'contact_count': 0, 'message': 'No contacts in payload'})

    # Get or create cache entry
    entry = _get_clay_search(company_key)
    if not entry:
        # Edge case: callback arrived but we don't have a cache entry
        # (e.g., server restarted, or different worker without Redis)
        print(f"[CLAY CALLBACK] No cache entry for domain={company_key} — creating new entry", file=sys.stderr)
        entry = {
            'domain': company_key,
            'company_name': data.get('company_name', ''),
            'searched_at': datetime.utcnow().isoformat(),
            'status': 'searching',
            'contacts': []
        }

    # Deduplicate and merge new contacts with existing ones
    existing_contacts = entry.get('contacts', [])
    merged_contacts = deduplicate_contacts(existing_contacts, incoming_contacts)

    new_count = len(merged_contacts) - len(existing_contacts)
    print(f"[CLAY CALLBACK] Merged: {len(existing_contacts)} existing + {new_count} new = {len(merged_contacts)} total", file=sys.stderr)

    # Update cache entry
    entry['contacts'] = merged_contacts
    entry['status'] = 'complete'
    _clay_search_cache[company_key] = entry
    _save_clay_cache_to_redis(company_key, entry)

    return jsonify({
        'success': True,
        'contact_count': len(merged_contacts),
        'new_contacts': new_count,
        'message': f'Stored {len(merged_contacts)} contacts ({new_count} new)'
    })


@flask_app.route('/api/clay-search-status/<path:company_key>', methods=['GET'])
@require_auth
def clay_search_status(company_key):
    """
    Poll for Clay search results by domain.
    Frontend calls this every 3 seconds after triggering a search.

    URL param: company_key — the normalized domain (e.g., "acme.com")

    Returns the current state of a Clay search:
    - status: "searching" (still waiting for Clay) or "complete" (contacts received)
    - contacts: list of contacts found so far
    - searched_at: when the search was initiated
    - contact_count: number of contacts found
    """
    # Normalize in case of slight URL encoding differences
    company_key = get_company_key(company_key)

    entry = _get_clay_search(company_key)

    if not entry:
        return jsonify({
            'found': False,
            'status': 'not_found',
            'message': 'No search found for this domain'
        }), 404

    return jsonify({
        'found': True,
        'status': entry.get('status', 'searching'),
        'searched_at': entry.get('searched_at'),
        'contact_count': len(entry.get('contacts', [])),
        'contacts': entry.get('contacts', [])
    })


@flask_app.route('/api/lookalikes', methods=['GET'])
@require_auth
def get_lookalikes():
    """
    Find lookalike organizations from Definitive Healthcare for a deal.

    Query params:
    - billing_state (required): state from the deal
    - billing_city: city from the deal
    - specialty: medical specialty from the deal
    - page (default: 1)
    - page_size (default: 10)
    - filter_specialty: filter results to this specialty
    - filter_min_match: minimum match % (e.g. 85)
    - filter_city: filter results to this city
    - filter_state: filter results to this state
    """
    billing_state = request.args.get('billing_state')
    billing_city = request.args.get('billing_city')
    specialty = request.args.get('specialty')
    page = request.args.get('page', 1, type=int)
    page_size = request.args.get('page_size', 10, type=int)

    # Client-side filter params
    filter_specialty = request.args.get('filter_specialty', '').strip()
    filter_min_match = request.args.get('filter_min_match', '', type=str).strip()
    filter_city = request.args.get('filter_city', '').strip()
    filter_state = request.args.get('filter_state', '').strip()

    if not billing_state:
        return jsonify({'error': 'billing_state is required'}), 400

    company_data = {
        'billing_state': billing_state,
    }
    if billing_city:
        company_data['billing_city'] = billing_city
    if specialty:
        company_data['specialty'] = specialty

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"[LOOKALIKES] Incoming query params:", file=sys.stderr)
    print(f"  billing_state = {repr(billing_state)}", file=sys.stderr)
    print(f"  billing_city  = {repr(billing_city)}", file=sys.stderr)
    print(f"  specialty     = {repr(specialty)}", file=sys.stderr)
    print(f"[LOOKALIKES] company_data being passed to find_lookalikes_from_company_data:", file=sys.stderr)
    print(f"  {json.dumps(company_data, indent=2)}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # Fetch ALL results (page=1, page_size=99999) so we can filter + paginate server-side
    result = find_lookalikes_from_company_data(
        company_data,
        similarity_threshold=85,
        include_contacts=True,
        page=1,
        page_size=99999,
        use_cache=True
    )

    if 'error' in result:
        return jsonify(result), 400

    def format_contact(c, contact_type):
        """Map raw contact dict to clean frontend fields with all available contact info."""
        entry = {
            'name': f"{c.get('FIRST_NAME', '')} {c.get('LAST_NAME', '')}".strip(),
            'type': contact_type,
            'business_email': c.get('BUSINESS_EMAIL') or None,
            'direct_email_primary': c.get('DIRECT_EMAIL_PRIMARY') or None,
            'direct_email_secondary': c.get('DIRECT_EMAIL_SECONDARY') or None,
            'mobile_phone_primary': c.get('MOBILE_PHONE_PRIMARY') or None,
            'mobile_phone_secondary': c.get('MOBILE_PHONE_SECONDARY') or None,
        }
        if contact_type == 'physician':
            entry['title'] = c.get('PRIMARY_SPECIALTY')
        else:
            entry['title'] = c.get('TITLE')
            entry['linkedin'] = c.get('LINKEDIN_PROFILE') or None
        return entry

    # Map ALL results to frontend-friendly format
    all_lookalikes = []
    for org in result.get('lookalike_organizations', []):
        contacts = []
        for p in org.get('physicians', []):
            contacts.append(format_contact(p, 'physician'))
        for e in org.get('executives', []):
            contacts.append(format_contact(e, 'executive'))

        all_lookalikes.append({
            'name': org.get('physician_group_name'),
            'city': org.get('city'),
            'state': org.get('state'),
            'country': 'United States',
            'specialty': org.get('combined_main_specialty'),
            'physician_count': org.get('physician_count'),
            'similarity_score': org.get('similarity_score'),
            'match_reasons': org.get('match_reasons'),
            'definitive_id': org.get('definitive_id'),
            'ehr': org.get('ambulatory_emr'),
            'website': org.get('website'),
            'hs_id': org.get('hs_id'),
            'contacts': contacts,
        })

    # Build filter options from the FULL unfiltered set (including expanded specialties)
    expanded_specialties = company_data.get('_expanded_specialties', [])
    all_specialties_set = set()
    all_cities_set = set()
    all_states_set = set()
    for org in all_lookalikes:
        if org.get('specialty'):
            all_specialties_set.add(org['specialty'])
        if org.get('city'):
            all_cities_set.add(org['city'])
        if org.get('state'):
            all_states_set.add(org['state'])
    # Also include LLM-expanded specialties as filterable options
    for spec in expanded_specialties:
        all_specialties_set.add(spec)

    filter_options = {
        'specialties': sorted(all_specialties_set),
        'cities': sorted(all_cities_set),
        'states': sorted(all_states_set),
    }

    # Apply filters to the full set
    filtered = all_lookalikes
    if filter_specialty:
        filtered = [o for o in filtered if o.get('specialty', '').lower() == filter_specialty.lower()]
    if filter_min_match:
        try:
            min_val = int(filter_min_match)
            filtered = [o for o in filtered if (o.get('similarity_score') or 0) >= min_val]
        except ValueError:
            pass
    if filter_city:
        filtered = [o for o in filtered if o.get('city', '').lower() == filter_city.lower()]
    if filter_state:
        filtered = [o for o in filtered if o.get('state', '').upper() == filter_state.upper()]

    # Paginate the filtered results
    total_filtered = len(filtered)
    total_pages = (total_filtered + page_size - 1) // page_size if total_filtered > 0 else 0
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_results = filtered[start_idx:end_idx]

    return jsonify({
        'lookalikes': page_results,
        'total_matches': total_filtered,
        'total_unfiltered': len(all_lookalikes),
        'page': page,
        'page_size': page_size,
        'total_pages': total_pages,
        'expanded_specialties': expanded_specialties,
        'filter_options': filter_options,
    })

# Tell Claude what tools are available
@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # ========== HUBSPOT TOOLS ==========
        Tool(
            name="search_hubspot_deals",
            description="Search HubSpot deals with filters. Returns deals with associated company data. Use this to find recent closed won deals, deals by location, specialty, pipeline, seats, TSO, amount, etc.",
            inputSchema={
                "type": "object",
                "properties": {
                    "deal_stage": {"type": "string", "description": "Deal stage (e.g., 'closedwon', 'closedlost')"},
                    "is_closed_won": {"type": "boolean", "description": "Filter for closed won deals (true/false)"},
                    "country": {"type": "string", "description": "Country filter"},
                    "state": {"type": "string", "description": "Billing state (e.g., 'TX', 'CA')"},
                    "city": {"type": "string", "description": "Billing city"},
                    "specialty": {"type": "string", "description": "Medical specialty"},
                    "pipeline": {"type": "string", "description": "Pipeline ID or name"},
                    "min_seats": {"type": "integer", "description": "Minimum seats subscribed"},
                    "max_seats": {"type": "integer", "description": "Maximum seats subscribed"},
                    "min_tso": {"type": "integer", "description": "Minimum total serviceable opportunity"},
                    "max_tso": {"type": "integer", "description": "Maximum total serviceable opportunity"},
                    "min_amount": {"type": "number", "description": "Minimum deal amount in home currency"},
                    "max_amount": {"type": "number", "description": "Maximum deal amount in home currency"},
                    "days_back": {"type": "integer", "description": "Filter deals closed in last N days (e.g., 14 for past 2 weeks)"},
                    "limit": {"type": "integer", "default": 50, "description": "Max results (max 100)"}
                }
            }
        ),
        # ========== NEW: ORGANIZATION SEARCH ==========
        Tool(
            name="get_organizations_from_definitive",
            description="Search for healthcare organizations in Definitive database by physician group name, state, or city. Returns firmographic details including definitive_id, physician_count, combined_main_specialty, ambulatory_emr, and hs_id (HubSpot record ID if it exists). Use this to find organizations before getting their contacts.",
            inputSchema={
                "type": "object",
                "properties": {
                    "company_name": {"type": "string", "description": "Physician group name to search for (partial matches supported)"},
                    "state": {"type": "string", "description": "State abbreviation (e.g., 'CA', 'NY')"},
                    "city": {"type": "string", "description": "City name"},
                    "limit": {"type": "integer", "default": 10, "description": "Maximum number of results"}
                }
            }
        ),
        # ========== UPDATED: CONTACT SEARCH BY DEFINITIVE_ID ==========
        Tool(
            name="get_organization_contacts_by_id",
            description="Get physicians and/or executives for an organization using its definitive_id (from get_organizations_from_definitive). Returns separate lists: Physicians include PRIMARY_SPECIALTY, EXECUTIVE_FLAG, BUSINESS_EMAIL, DIRECT_EMAIL (primary/secondary), MOBILE_PHONE (primary/secondary), and physician_group_name. Executives include PHYSICIAN_LEADER flag, TITLE, LINKEDIN_PROFILE, BUSINESS_EMAIL, DIRECT_EMAIL (primary/secondary), MOBILE_PHONE (primary/secondary), and physician_group_name. Requires definitive_id as input.",
            inputSchema={
                "type": "object",
                "properties": {
                    "definitive_id": {"type": "string", "description": "The definitive ID of the organization"},
                    "contact_type": {
                        "type": "string",
                        "description": "Type of contacts: 'physicians', 'executives', or 'both'",
                        "enum": ["physicians", "executives", "both"],
                        "default": "both"
                    },
                    "limit": {"type": "integer", "default": 50, "description": "Max results per contact type"}
                },
                "required": ["definitive_id"]
            }
        ),
        # ========== DATABRICKS TOOLS ==========
        Tool(
            name="search_healthcare_providers",
            description="Search healthcare organizations from Definitive physician companies data by specialty, state, or city",
            inputSchema={
                "type": "object",
                "properties": {
                    "specialty": {"type": "string", "description": "Combined main specialty"},
                    "state": {"type": "string", "description": "State abbreviation"},
                    "city": {"type": "string", "description": "City name"},
                    "limit": {"type": "integer", "default": 20}
                }
            }
        ),
        Tool(
            name="query_databricks",
            description="Run custom SQL on Databricks (SELECT only)",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "SQL SELECT query"},
                    "limit": {"type": "integer", "default": 50}
                },
                "required": ["query"]
            }
        ),
        Tool(
            name="get_databricks_table_schema",
            description="Get schema for a Databricks table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "enum": [
                            "prod_analytics_global.exposure.sales__definitive_affiliates",
                            "prod_analytics_global.exposure.sales__definitive_physician_companies",
                            "prod_analytics_global.ad_hoc.us_phys_report",
                            "prod_analytics_global.ad_hoc.us_executive_report"
                        ]
                    }
                },
                "required": ["table_name"]
            }
        ),
        # ========== LOOKALIKE ANALYSIS ==========
        Tool(
            name="find_lookalikes_from_company_data",
            description="EXHAUSTIVELY find ALL similar organizations in Definitive Healthcare based on company data (from HubSpot deals). Searches the entire database for matches in the specified state and specialty. Returns ALL organizations that meet the similarity threshold, not just a sample. Use Claude's HubSpot connector first to get deal/company info, then pass that data here. Location weighted higher than specialty in scoring. Returns organization details including definitive_id, physician_group_name, physician_count, ambulatory_emr, hs_id, and contacts (physicians and executives with valid contact info only). Supports pagination and caching for efficiency.",
            inputSchema={
                "type": "object",
                "properties": {
                    "company_data": {
                        "type": "object",
                        "description": "Company information from HubSpot. Include any available fields - at minimum need state. Common fields: company_name, state, city, specialty, domain, etc.",
                        "properties": {
                            "company_name": {"type": "string", "description": "Company name"},
                            "billing_state": {"type": "string", "description": "State from deal billing (priority 1)"},
                            "billing_city": {"type": "string", "description": "City from deal billing (priority 1)"},
                            "lc_us_state": {"type": "string", "description": "State from company LC US State field (priority 2)"},
                            "lc_city": {"type": "string", "description": "City from company LC City field (priority 2)"},
                            "specialty": {"type": "string", "description": "Medical specialty, industry type, or any descriptor of what the company does"},
                            "deal_name": {"type": "string", "description": "Deal name for reference"},
                            "deal_amount": {"type": "string", "description": "Deal amount for reference"},
                            "deal_id": {"type": "string", "description": "HubSpot deal ID for reference"}
                        },
                        "required": []
                    },
                    "similarity_threshold": {
                        "type": "integer",
                        "description": "Minimum similarity score (0-100). Default: 85 (high-quality matches only)",
                        "default": 85
                    },
                    "max_results": {
                        "type": "integer",
                        "description": "Maximum number of lookalike organizations to return (after scoring and sorting). Set to null or omit for unlimited (returns ALL matches). Default: null (unlimited)",
                        "default": None
                    },
                    "include_contacts": {
                        "type": "boolean",
                        "description": "Whether to include contacts for each organization. Default: true",
                        "default": True
                    },
                    "page": {
                        "type": "integer",
                        "description": "Page number for pagination (1-indexed). Default: 1",
                        "default": 1
                    },
                    "page_size": {
                        "type": "integer",
                        "description": "Number of results per page. Default: 10",
                        "default": 10
                    },
                    "use_cache": {
                        "type": "boolean",
                        "description": "Whether to use cached results if available (1 hour TTL). Default: true",
                        "default": True
                    }
                },
                "required": ["company_data"]
            }
        )
    ]

# Tool implementations
@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    
    # ========== HUBSPOT ==========
    if name == "search_hubspot_deals":
        try:
            deals = search_hubspot_deals(
                deal_stage=arguments.get("deal_stage"),
                is_closed_won=arguments.get("is_closed_won"),
                country=arguments.get("country"),
                state=arguments.get("state"),
                city=arguments.get("city"),
                specialty=arguments.get("specialty"),
                pipeline=arguments.get("pipeline"),
                min_seats=arguments.get("min_seats"),
                max_seats=arguments.get("max_seats"),
                min_tso=arguments.get("min_tso"),
                max_tso=arguments.get("max_tso"),
                min_amount=arguments.get("min_amount"),
                max_amount=arguments.get("max_amount"),
                days_back=arguments.get("days_back"),
                limit=arguments.get("limit", 50)
            )
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "total": len(deals),
                    "deals": deals
                }, indent=2, default=str)
            )]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    # ========== NEW ORGANIZATION SEARCH ==========
    elif name == "get_organizations_from_definitive":
        try:
            company_name = arguments.get("company_name")
            state = arguments.get("state")
            city = arguments.get("city")
            limit = arguments.get("limit", 10)
            
            organizations = get_organizations_from_definitive(company_name, state, city, limit)
            
            return [TextContent(
                type="text", 
                text=json.dumps({
                    "total": len(organizations), 
                    "organizations": organizations
                }, indent=2, default=str)
            )]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    # ========== UPDATED CONTACT SEARCH BY ID ==========
    elif name == "get_organization_contacts_by_id":
        try:
            definitive_id = arguments.get("definitive_id")
            contact_type = arguments.get("contact_type", "both")
            limit = arguments.get("limit", 50)
            
            contacts = get_organization_contacts(definitive_id, contact_type, limit)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "definitive_id": definitive_id,
                    "contact_type": contact_type,
                    "physician_count": len(contacts.get("physicians", [])),
                    "executive_count": len(contacts.get("executives", [])),
                    "contacts": contacts
                }, indent=2, default=str)
            )]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    # ========== DATABRICKS ==========
    elif name == "get_databricks_table_schema":
        try:
            table_name = arguments.get("table_name")
            conn = get_databricks_connection()
            cursor = conn.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            results = cursor.fetchall()
            schema = [{"column": row[0], "type": row[1], "comment": row[2] if len(row) > 2 else None} for row in results]
            cursor.close()
            conn.close()
            return [TextContent(type="text", text=json.dumps({"table": table_name, "columns": schema}, indent=2))]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    elif name == "search_healthcare_providers":
        try:
            specialty = arguments.get("specialty")
            state = arguments.get("state")
            city = arguments.get("city")
            limit = arguments.get("limit", 20)
            
            query = "SELECT * FROM prod_analytics_global.exposure.sales__definitive_physician_companies WHERE 1=1"
            if specialty:
                query += f" AND LOWER(combined_main_specialty) LIKE LOWER('%{specialty}%')"
            if state:
                query += f" AND UPPER(state) = '{state.upper()}'"
            if city:
                query += f" AND LOWER(city) LIKE LOWER('%{city}%')"
            query += f" LIMIT {limit}"
            
            conn = get_databricks_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            providers = [dict(zip(columns, row)) for row in results]
            cursor.close()
            conn.close()
            
            return [TextContent(type="text", text=json.dumps({"total": len(providers), "providers": providers}, indent=2, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    elif name == "query_databricks":
        try:
            query = arguments.get("query")
            limit = arguments.get("limit", 50)
            
            if not query.strip().upper().startswith("SELECT"):
                return [TextContent(type="text", text="Error: Only SELECT queries allowed")]
            if "LIMIT" not in query.upper():
                query = f"{query} LIMIT {limit}"
            
            conn = get_databricks_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            data = [dict(zip(columns, row)) for row in results]
            cursor.close()
            conn.close()
            
            return [TextContent(type="text", text=json.dumps({"rows": len(data), "data": data}, indent=2, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    # ========== LOOKALIKE ANALYSIS ==========
    elif name == "find_lookalikes_from_company_data":
        try:
            company_data = arguments.get("company_data", {})
            similarity_threshold = arguments.get("similarity_threshold", 85)  # Default to 85% for high-quality matches
            max_results = arguments.get("max_results", None)  # None = unlimited
            include_contacts = arguments.get("include_contacts", True)
            page = arguments.get("page", 1)
            page_size = arguments.get("page_size", 10)
            use_cache = arguments.get("use_cache", True)
            
            result = find_lookalikes_from_company_data(
                company_data, 
                similarity_threshold, 
                max_results, 
                include_contacts,
                page,
                page_size,
                use_cache
            )
            return [TextContent(type="text", text=json.dumps(result, indent=2, default=str))]
        except Exception as e:
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    raise ValueError(f"Unknown tool: {name}")

# Start the server
if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == '--flask-only':
        # Running Flask dashboard standalone (Railway sets PORT env var)
        port = int(os.getenv('PORT', 5001))
        debug = os.getenv('FLASK_DEBUG', 'false').lower() == 'true'
        flask_app.run(host='0.0.0.0', port=port, debug=debug)
    else:
        # Running as MCP server (for Claude)
        from mcp.server.stdio import stdio_server
        import asyncio

        async def main():
            async with stdio_server() as (read_stream, write_stream):
                await app.run(
                    read_stream,
                    write_stream,
                    app.create_initialization_options()
                )

        asyncio.run(main())