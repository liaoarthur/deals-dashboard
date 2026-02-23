import asyncio
import os
import sys
import json
import requests

from datetime import datetime, timedelta
import redis
from flask import Flask, request, jsonify
from flask_cors import CORS

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from databricks import sql
from dotenv import load_dotenv
from functools import lru_cache
from multiprocessing import Pool, cpu_count
import hashlib
from hubspot import HubSpot
from openai import OpenAI

# Load credentials from .env file
load_dotenv()

# Load environment variables
HUBSPOT_API_KEY = os.getenv('HUBSPOT_ACCESS_TOKEN')

# Check if API key exists
if not HUBSPOT_API_KEY:
    raise ValueError("HUBSPOT_API_KEY environment variable not set")

# OpenAI client for LLM-powered specialty expansion (optional — degrades gracefully)
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
openai_client = None
if OPENAI_API_KEY:
    openai_client = OpenAI(api_key=OPENAI_API_KEY)

# Clay webhook for company discovery (optional)
CLAY_WEBHOOK_URL = os.getenv('CLAY_WEBHOOK_URL')

# Create the server
app = Server("gtm-mcp-server")
flask_app = Flask(__name__)
# CORS: allow Vercel frontend domain + localhost for dev
ALLOWED_ORIGINS = os.getenv('ALLOWED_ORIGINS', 'http://localhost:3000,http://localhost:5001').split(',')
CORS(flask_app, resources={r"/api/*": {"origins": ALLOWED_ORIGINS}}, supports_credentials=True)

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

# OPTIMIZATION: Simple in-memory cache for query results
_query_cache = {}
_cache_ttl = 3600  # 1 hour cache

def get_cache_key(query_type, **kwargs):
    """Generate a cache key from query parameters"""
    key_string = f"{query_type}:" + json.dumps(kwargs, sort_keys=True)
    return hashlib.md5(key_string.encode()).hexdigest()

def get_cached_result(cache_key):
    """Get cached result if available and not expired"""
    if cache_key in _query_cache:
        result, timestamp = _query_cache[cache_key]
        import time
        if time.time() - timestamp < _cache_ttl:
            return result
    return None

def set_cached_result(cache_key, result):
    """Cache a result with timestamp"""
    import time
    _query_cache[cache_key] = (result, time.time())

# LLM specialty expansion cache — longer TTL since medical knowledge is stable
_specialty_expansion_cache = {}
_specialty_expansion_ttl = 86400  # 24 hours

# Cache for the full list of distinct Definitive Healthcare specialties
_definitive_specialties_cache = {
    "specialties": None,
    "timestamp": 0
}
_definitive_specialties_ttl = 86400  # 24 hours

def has_valid_contact_info(contact, contact_type="physician"):
    """
    Check if contact has at least one valid piece of contact information.
    Returns True only if contact has LinkedIn URL, direct email, or mobile phone.
    """
    if contact_type == "executive":
        # For executives, require LinkedIn OR direct email OR mobile
        has_linkedin = contact.get("LINKEDIN_PROFILE") and str(contact.get("LINKEDIN_PROFILE")).strip()
        has_direct_email = (
            (contact.get("DIRECT_EMAIL_PRIMARY") and str(contact.get("DIRECT_EMAIL_PRIMARY")).strip()) or
            (contact.get("DIRECT_EMAIL_SECONDARY") and str(contact.get("DIRECT_EMAIL_SECONDARY")).strip())
        )
        has_mobile = (
            (contact.get("MOBILE_PHONE_PRIMARY") and str(contact.get("MOBILE_PHONE_PRIMARY")).strip()) or
            (contact.get("MOBILE_PHONE_SECONDARY") and str(contact.get("MOBILE_PHONE_SECONDARY")).strip())
        )
        return has_linkedin or has_direct_email or has_mobile
    else:
        # For physicians, require direct email OR mobile
        has_direct_email = (
            (contact.get("DIRECT_EMAIL_PRIMARY") and str(contact.get("DIRECT_EMAIL_PRIMARY")).strip()) or
            (contact.get("DIRECT_EMAIL_SECONDARY") and str(contact.get("DIRECT_EMAIL_SECONDARY")).strip())
        )
        has_mobile = (
            (contact.get("MOBILE_PHONE_PRIMARY") and str(contact.get("MOBILE_PHONE_PRIMARY")).strip()) or
            (contact.get("MOBILE_PHONE_SECONDARY") and str(contact.get("MOBILE_PHONE_SECONDARY")).strip())
        )
        return has_direct_email or has_mobile

def score_single_org(args):
    """
    Worker function for parallel similarity scoring.
    Takes tuple of (company_data, org) and returns (org, score, reasons)
    """
    company_data, org = args
    score = calculate_similarity_score(company_data, org)
    reasons = get_match_reasons(company_data, org, score)
    return (org, score, reasons)

# Function to connect to Databricks
def get_databricks_connection():
    """Connect to Databricks SQL Warehouse"""
    try:
        return sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN")
        )
    except Exception as e:
        raise Exception(f"Failed to connect to Databricks: {str(e)}")

# ============================================
# HubSpot Property Mappings
# ============================================

# Properties to fetch from HubSpot
DEAL_PROPERTIES = [
    # Identifiers
    "hs_object_id",
    "dealname",
    
    # Company/Contact associations
    "associated_company_id",
    "associated_company_name",
    "associated_contact_email",
    "associated_contact_id",
    
    # Deal info
    "deal_segment",
    "deal_type__new",
    "dealstage",
    "pipeline",
    "deal_category",
    "hubspot_owner_id",
    
    # Dates
    "closedate",
    "createdate",
    
    # Financial
    "amount_in_home_currency",
    
    # Location
    "country",
    "billing_city",
    "billing_state",
    "billing_zip",
    
    # Product/Service
    "product",
    "ehr",
    "seats_subscribed",
    "comms_seats",
    "evidence_seats",
    "total_serviceable_opportunity",
    
    # Medical
    "specialty_mcp_use",
    
    # Status flags
    "hs_is_closed_won",
    "hs_is_closed_lost",
    "is_deal_closed",
    
    # Source
    "lead_source"
]

# Filterable properties
FILTER_PROPERTIES = {
    'specialty_mcp_use': 'specialty_mcp_use',
    'country': 'country',
    'close_date': 'closedate',
    'seats': 'seats_subscribed',
    'tso': 'total_serviceable_opportunity',
    'billing_city': 'billing_city',
    'billing_state': 'billing_state',
    'product': 'product'
}

@lru_cache(maxsize=1)
def get_hubspot_mappings():
    """Fetch deal stages and pipelines"""
    try:
        response = requests.get(
            'https://api.hubapi.com/crm/v3/pipelines/deals',
            headers={'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        )
        
        if response.status_code != 200:
            return {'stages': {}, 'pipelines': {}, 'stage_list': []}
        
        pipelines = response.json()['results']
        
        stage_map = {}
        pipeline_map = {}
        stage_list = []  # List of all stages with metadata
        
        for pipeline in pipelines:
            pipeline_map[pipeline['label']] = pipeline['id']
            pipeline_map[pipeline['id']] = pipeline['label']
            
            for stage in pipeline['stages']:
                stage_map[stage['label']] = stage['id']
                stage_map[stage['id']] = stage['label']
                
                # Add to stage list with full metadata
                stage_list.append({
                    'id': stage['id'],
                    'label': stage['label'],
                    'pipeline': pipeline['label'],
                    'pipeline_id': pipeline['id']
                })
        
        return {
            'stages': stage_map,
            'pipelines': pipeline_map,
            'stage_list': stage_list
        }
    except Exception as e:
        print(f"Error fetching HubSpot mappings: {e}")
        return {'stages': {}, 'pipelines': {}, 'stage_list': []}


@lru_cache(maxsize=100)
def get_owner_name(owner_id):
    if not owner_id:
        return None
    
    try:
        
        response = requests.get(
            f'https://api.hubapi.com/crm/v3/owners/{owner_id}',
            headers={'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        )
        
        
        if response.status_code == 200:
            owner = response.json()
            name = f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip()
            return name
    except Exception as e:
        print(f"DEBUG: Error = {e}", file=sys.stderr)
    
    return owner_id

@lru_cache(maxsize=1)
def get_specialty_property_info():
    """
    Fetch specialty property metadata from HubSpot.
    Returns: dict with is_enumeration flag and value->label mapping
    """
    try:
        response = requests.get(
            'https://api.hubapi.com/crm/v3/properties/deals',
            headers={'Authorization': f'Bearer {HUBSPOT_API_KEY}'}
        )
        
        if response.status_code == 200:
            props_data = response.json()
            
            # Find ALL properties named 'specialty_mcp_use'
            specialty_props = [
                p for p in props_data.get('results', []) 
                if p['name'] == 'specialty_mcp_use'
            ]
            

            # Prefer enumeration type if multiple exist
            for prop in specialty_props:
                if prop.get('type') == 'string':
                    # Build mapping: internal_value -> display_label
                    mapping = {
                        option['value']: option['label'] 
                        for option in prop.get('options', [])
                    }
                    return {
                        'is_enumeration': True,
                        'mapping': mapping
                    }
                           
    except Exception as e:
        print(f"Error fetching specialty property: {e}", file=sys.stderr)
    
    return {'is_enumeration': False, 'mapping': {}}

def get_specialty_label(internal_value):
    """
    Return specialty value as-is from specialty_mcp_use field.
    This field is already formatted for display.
    """
    if not internal_value:
        return None
    
    prop_info = get_specialty_property_info()
    mapping = prop_info['mapping']

    # Handle multi-select
    if ';' in str(internal_value):
        values = [v.strip() for v in str(internal_value).split(';')]
        # If value is already a label (not in mapping keys), keep as-is
        labels = []
        for v in values:
            if v in mapping:
                labels.append(mapping[v])  # Map internal -> label
            elif v in mapping.values():
                labels.append(v)  # Already a label, keep it
            else:
                labels.append(v)  # Unknown, keep as-is
        return '; '.join(labels)
    
    # Single value
    value_str = str(internal_value)
    if value_str in mapping:
        return mapping[value_str]  # Internal value -> label
    elif value_str in mapping.values():
        return value_str  # Already a label
    else:
        return value_str  # Unknown, return as-is
    


# ========================================
# NEW: Get Deals (with ALL stages support)
# ========================================

@flask_app.route('/api/deals', methods=['GET'])
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

#@flask_app.route('/api/deals/raw', methods=['GET'])
# def get_deals_raw():
#     """Return raw HubSpot response - no formatting"""
#     cutoff_date = int((datetime.now() - timedelta(days=14)).timestamp() * 1000)
    
#     response = requests.post(
#         'https://api.hubapi.com/crm/v3/objects/deals/search',
#         headers={
#             'Authorization': f'Bearer {HUBSPOT_API_KEY}',
#             'Content-Type': 'application/json'
#         },
#         json={
#             "filterGroups": [{
#                 "filters": [
#                     {"propertyName": "closedate", "operator": "GTE", "value": str(cutoff_date)},
#                     {"propertyName": "country", "operator": "EQ", "value": "United States"},
#                     {"propertyName": "hs_is_closed_won", "operator": "EQ", "value": "true"},
#                     {"propertyName": "pipeline", "operator": "EQ", "value": "74974043"}
#                 ]
#             }],
#             "properties": [
#                 "specialty", "billing_city", "billing_state", 
#             ],
#             "limit": 5
#         }
#     )
    
#     return jsonify(response.json())

# @flask_app.route('/api/filters', methods=['GET'])
# def get_filter_options():
#     """Get all available filter values including ALL deal stages"""
    
#     # Check cache
#     if REDIS_ENABLED:
#         cached = redis_client.get('filter_options')
#         if cached:
#             try:
#                 return jsonify(json.loads(cached))
#             except:
#                 pass
    
#     try:
#         # Fetch recent deals for data-driven filters (specialty, location, product)
#         response = requests.post(
#             'https://api.hubapi.com/crm/v3/objects/deals/search',
#             headers={
#                 'Authorization': f'Bearer {HUBSPOT_API_KEY}',
#                 'Content-Type': 'application/json'
#             },
#             json={
#                 "filterGroups": [{
#                     "filters": [{
#                         "propertyName": "closedate",
#                         "operator": "GTE",
#                         "value": str(int((datetime.now() - timedelta(days=90)).timestamp() * 1000))
#                     }]
#                 }],
#                 "properties": [
#                     "specialty_mcp_use", "country", "billing_state",
#                     "billing_city", "product"
#                 ],
#                 "limit": 100
#             }
#         )
        
#         if response.status_code != 200:
#             return jsonify({'error': 'Failed to fetch filter options'}), 500
        
#         deals = response.json().get('results', [])
        
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500
    
#     # Extract unique values from DEALS (data-driven)
#     specialties = set()
#     countries = set()
#     billing_states = set()
#     billing_cities = set()
#     products = set()
    
#     for deal in deals:
#         props = deal['properties']

#         if props.get('specialty'):
#             specialties.add(props['specialty_mcp_use'])
        
#         if props.get('country'):
#             countries.add(props['country'])
        
#         if props.get('billing_state'):
#             billing_states.add(props['billing_state'])
        
#         if props.get('billing_city'):
#             billing_cities.add(props['billing_city'])
        
#         if props.get('product'):
#             products.add(props['product'])
    
#     # Get ALL pipelines and stages from HubSpot API (not from deals)
#     mappings = get_hubspot_mappings()
    
#     # Build pipeline options from ALL available pipelines
#     pipeline_options = []
#     seen_pipeline_ids = set()
#     for key, value in mappings['pipelines'].items():
#         # Only add each pipeline once (skip the reverse mapping)
#         if isinstance(key, str) and key not in seen_pipeline_ids:
#             pipeline_options.append({
#                 'id': mappings['pipelines'].get(key),  # Get the ID
#                 'name': key  # The label
#             })
#             seen_pipeline_ids.add(mappings['pipelines'].get(key))
    
#     # Build deal stage options from ALL available stages
#     deal_stage_options = [
#         {
#             'id': stage['id'],
#             'label': stage['label'],
#             'pipeline': stage['pipeline'],
#             'pipeline_id': stage['pipeline_id']
#         }
#         for stage in mappings['stage_list']
#     ]
    
#     filter_options = {
#         'specialties': sorted(specialties),
#         'countries': sorted(countries),
#         'billing_states': sorted(billing_states),
#         'billing_cities': sorted(billing_cities),
#         'products': sorted(products),
#         'pipelines': sorted(pipeline_options, key=lambda x: x['name']),
#         'deal_stages': sorted(deal_stage_options, key=lambda x: x['label'])
#     }
    
#     # Cache for 1 hour
#     if REDIS_ENABLED:
#         redis_client.setex('filter_options', 3600, json.dumps(filter_options))
    
#     return jsonify(filter_options)

#     uses calculated cutoff vs 14 days as above
@flask_app.route('/api/filters', methods=['GET'])
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

@flask_app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'redis': REDIS_ENABLED,
        'openai': openai_client is not None,
        'clay': CLAY_WEBHOOK_URL is not None,
    })

# NEW: Function to get organizations from Definitive with firmographic details
def get_organizations_from_definitive(company_name=None, state=None, city=None, limit=10):
    """
    Search for organizations in Definitive Healthcare and get their firmographic details.
    Returns organizations with their definitive_id and key business metrics.
    """
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT 
            city,
            combined_main_specialty,
            definitive_id,
            physician_count,
            physician_group_name,
            state,
            zip_code,
            ambulatory_emr,
            hs_id
        FROM prod_analytics_global.exposure.sales__definitive_physician_companies
        WHERE 1=1
        """
        
        params = []
        if company_name:
            query += " AND LOWER(physician_group_name) LIKE LOWER(?)"
            params.append(f"%{company_name}%")
        if state:
            query += " AND UPPER(state) = UPPER(?)"
            params.append(state)
        if city:
            query += " AND LOWER(city) LIKE LOWER(?)"
            params.append(f"%{city}%")
        
        query += " LIMIT ?"
        params.append(limit)
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        organizations = [dict(zip(columns, row)) for row in results]
        
        cursor.close()
        conn.close()
        
        return organizations
    except Exception as e:
        return []

# UPDATED: Function to get contacts using definitive_id from two separate sources
def get_organization_contacts(definitive_id, contact_type="both", limit=50):
    """
    Get contacts (physicians and/or executives) for an organization using definitive_id.
    
    Args:
        definitive_id: The definitive ID of the organization
        contact_type: "physicians", "executives", or "both" (default)
        limit: Maximum number of results per contact type
    
    Returns:
        Dictionary with physicians and/or executives lists
    """
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        
        results = {
            "physicians": [],
            "executives": []
        }
        
        # Query physicians if requested
        if contact_type in ["physicians", "both"]:
            physicians_query = """
            SELECT 
                p.FIRST_NAME,
                p.LAST_NAME,
                p.PRIMARY_SPECIALTY,
                p.EXECUTIVE_FLAG,
                p.BUSINESS_EMAIL,
                p.DIRECT_EMAIL_PRIMARY,
                p.DIRECT_EMAIL_SECONDARY,
                p.MOBILE_PHONE_PRIMARY,
                p.MOBILE_PHONE_SECONDARY,
                p.DEFINITIVE_ID,
                c.physician_group_name
            FROM prod_analytics_global.ad_hoc.us_phys_report p
            JOIN prod_analytics_global.exposure.sales__definitive_physician_companies c
                ON p.DEFINITIVE_ID = c.definitive_id
            WHERE p.DEFINITIVE_ID = ?
            LIMIT ?
            """
            
            cursor.execute(physicians_query, (definitive_id, limit))
            phys_results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            results["physicians"] = [dict(zip(columns, row)) for row in phys_results]
        
        # Query executives if requested
        if contact_type in ["executives", "both"]:
            executives_query = """
            SELECT 
                e.FIRST_NAME,
                e.LAST_NAME,
                e.PHYSICIAN_LEADER,
                e.BUSINESS_EMAIL,
                e.DIRECT_EMAIL_PRIMARY,
                e.DIRECT_EMAIL_SECONDARY,
                e.MOBILE_PHONE_PRIMARY,
                e.MOBILE_PHONE_SECONDARY,
                e.DEFINITIVE_ID,
                c.physician_group_name,
                e.TITLE,
                e.LINKEDIN_PROFILE
            FROM prod_analytics_global.ad_hoc.us_executive_report e
            JOIN prod_analytics_global.exposure.sales__definitive_physician_companies c
                ON e.DEFINITIVE_ID = c.definitive_id
            WHERE e.DEFINITIVE_ID = ?
            LIMIT ?
            """
            
            cursor.execute(executives_query, (definitive_id, limit))
            exec_results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            results["executives"] = [dict(zip(columns, row)) for row in exec_results]
        
        cursor.close()
        conn.close()
        
        return results
    except Exception as e:
        return {"physicians": [], "executives": [], "error": str(e)}

# ========================================
# LLM-Powered Specialty Expansion
# ========================================

def get_definitive_specialties():
    """
    Fetch all distinct combined_main_specialty values from the Definitive table.
    Cached for 24 hours. Returns a list of strings.
    """
    import time
    now = time.time()

    if (_definitive_specialties_cache["specialties"] is not None
            and now - _definitive_specialties_cache["timestamp"] < _definitive_specialties_ttl):
        return _definitive_specialties_cache["specialties"]

    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT combined_main_specialty
            FROM prod_analytics_global.exposure.sales__definitive_physician_companies
            WHERE combined_main_specialty IS NOT NULL
              AND TRIM(combined_main_specialty) != ''
            ORDER BY combined_main_specialty
        """)
        results = cursor.fetchall()
        specialties = [row[0] for row in results]
        cursor.close()
        conn.close()

        _definitive_specialties_cache["specialties"] = specialties
        _definitive_specialties_cache["timestamp"] = now

        return specialties
    except Exception as e:
        print(f"Error fetching Definitive specialties: {e}", file=sys.stderr)
        return _definitive_specialties_cache.get("specialties") or []


def get_expanded_specialties(input_specialty):
    """
    Use Claude to find medically related specialties from the Definitive Healthcare
    specialty list. Returns a list of related specialty strings.

    Falls back to empty list if Claude API is unavailable.
    Cached for 24 hours per input specialty.
    """
    import time

    if not input_specialty or not input_specialty.strip():
        return []

    cache_key = input_specialty.strip().lower()

    # Check cache
    if cache_key in _specialty_expansion_cache:
        result, timestamp = _specialty_expansion_cache[cache_key]
        if time.time() - timestamp < _specialty_expansion_ttl:
            return result

    # If no OpenAI client, return empty (graceful degradation)
    if not openai_client:
        return []

    # Fetch real Definitive specialties
    definitive_specialties = get_definitive_specialties()
    if not definitive_specialties:
        return []

    specialty_list_str = "\n".join(f"- {s}" for s in definitive_specialties)

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            max_tokens=1024,
            messages=[{
                "role": "user",
                "content": f"""Given the medical specialty "{input_specialty}", identify other specialties from the following list that are medically related. "Medically related" means:
- Subspecialties or parent specialties (e.g., Endocrinology is a subspecialty of Internal Medicine)
- Specialties that commonly treat the same conditions (e.g., Endocrinology and Diabetes)
- Specialties with significant clinical overlap

IMPORTANT: Only return specialties from this exact list. Do not make up specialties.

Available specialties:
{specialty_list_str}

Return ONLY a JSON array of the top 2 most relevant related specialty strings, ranked by relevance. Nothing else.
Do NOT include the input specialty "{input_specialty}" itself or close spelling variants of it.
If no related specialties exist, return an empty array [].

Example output format: ["Internal Medicine", "Diabetes"]"""
            }]
        )

        response_text = response.choices[0].message.content.strip()

        # Handle markdown code blocks
        if response_text.startswith("```"):
            response_text = response_text.split("\n", 1)[1]
            response_text = response_text.rsplit("```", 1)[0].strip()

        related = json.loads(response_text)

        # Validate: only keep specialties that actually exist in the Definitive list
        definitive_set = set(s.lower() for s in definitive_specialties)
        validated = [
            s for s in related
            if isinstance(s, str) and s.lower() in definitive_set
        ][:2]  # Cap at top 2 most relevant

        _specialty_expansion_cache[cache_key] = (validated, time.time())
        print(f"LLM specialty expansion: '{input_specialty}' -> {validated}", file=sys.stderr)
        return validated

    except Exception as e:
        print(f"Error in LLM specialty expansion for '{input_specialty}': {e}", file=sys.stderr)
        # Cache failure for 5 minutes to avoid hammering the API
        _specialty_expansion_cache[cache_key] = ([], time.time() - _specialty_expansion_ttl + 300)
        return []


# ========================================
# Similarity Scoring Function
# ========================================

def calculate_similarity_score(company_data, definitive_org):
    """
    Calculate similarity score between company and Definitive org.

    Tier-based scoring (highest match wins):
      Tier 1: same city + same state + same specialty     → 95%
      Tier 2: same state + same specialty                 → 85%
      Tier 3: same city + same state + similar specialty  → 75%
      Tier 4: same city + same state                      → 65%
      Tier 5: same state + similar specialty              → 55%

    "Same specialty" = exact or fuzzy spelling match against deal specialties.
    "Similar specialty" = medically related via LLM expansion.
    State match is always required — no state match → 0.
    """

    # Extract company data with HubSpot-specific field names
    company_state = (
        str(company_data.get("billing_state", "")).upper().strip() or
        str(company_data.get("lc_us_state", "")).upper().strip() or
        str(company_data.get("state", "")).upper().strip()
    )
    company_city = (
        str(company_data.get("billing_city", "")).lower().strip() or
        str(company_data.get("lc_city", "")).lower().strip() or
        str(company_data.get("city", "")).lower().strip()
    )

    # For specialty, try multiple field names and split on semicolons
    company_specialty_raw = (
        str(company_data.get("specialty", "")).strip() or
        str(company_data.get("specialties", "")).strip() or
        str(company_data.get("primary_specialty", "")).strip()
    )
    company_specialties = [s.strip().lower() for s in company_specialty_raw.split(';') if s.strip()]

    # Extract definitive org data
    org_state = str(definitive_org.get("state", "")).upper().strip()
    org_city = str(definitive_org.get("city", "")).lower().strip()
    org_specialty = str(definitive_org.get("combined_main_specialty", "")).lower().strip()

    # State match is required — no state match means 0
    state_match = bool(company_state and org_state and company_state == org_state)
    if not state_match:
        return 0

    # City match
    city_match = bool(company_city and org_city and company_city in org_city)

    # Specialty matching: same (exact/fuzzy) vs similar (LLM-expanded)
    same_specialty = False
    similar_specialty = False

    if org_specialty:
        # Check exact and fuzzy match against deal's own specialties
        for spec in company_specialties:
            if spec in org_specialty or org_specialty in spec:
                same_specialty = True
                break
            elif is_specialty_similar(spec, org_specialty):
                same_specialty = True
                break

        # Check medically related (LLM expansion) only if no direct match
        if not same_specialty:
            expanded = company_data.get('_expanded_specialties', [])
            for exp_spec in expanded:
                if exp_spec.lower() in org_specialty or org_specialty in exp_spec.lower():
                    similar_specialty = True
                    break
                elif is_specialty_similar(exp_spec.lower(), org_specialty):
                    similar_specialty = True
                    break

    # Tier-based scoring
    if city_match and same_specialty:
        return 95  # Tier 1: same city + same state + same specialty
    elif same_specialty:
        return 85  # Tier 2: same state + same specialty
    elif city_match and similar_specialty:
        return 75  # Tier 3: same city + same state + similar specialty
    elif city_match:
        return 65  # Tier 4: same city + same state
    elif similar_specialty:
        return 55  # Tier 5: same state + similar specialty
    else:
        return 0   # State-only match with no city or specialty — not useful

def is_specialty_similar(spec1, spec2):
    """
    Check if two specialties are similar using fuzzy matching.
    Handles variations like: cardiology/cardiologist, pediatrics/pediatrician, etc.
    """
    # Common medical specialty variations
    specialty_roots = {
        'cardio': ['cardiology', 'cardiologist', 'cardiac'],
        'pediatr': ['pediatrics', 'pediatrician', 'pediatric'],
        'orthoped': ['orthopedics', 'orthopedic', 'orthopaedic'],
        'dermat': ['dermatology', 'dermatologist', 'dermatological'],
        'neurol': ['neurology', 'neurologist', 'neurological'],
        'oncol': ['oncology', 'oncologist'],
        'gastro': ['gastroenterology', 'gastroenterologist'],
        'pulmon': ['pulmonology', 'pulmonologist', 'pulmonary'],
        'nephr': ['nephrology', 'nephrologist'],
        'endocrin': ['endocrinology', 'endocrinologist'],
        'rheumat': ['rheumatology', 'rheumatologist'],
        'urol': ['urology', 'urologist'],
        'ophthal': ['ophthalmology', 'ophthalmologist'],
        'psych': ['psychiatry', 'psychiatrist', 'psychiatric', 'psychology', 'psychologist'],
        'anesth': ['anesthesiology', 'anesthesiologist'],
        'radiol': ['radiology', 'radiologist', 'radiological'],
        'pathol': ['pathology', 'pathologist'],
        'emergency': ['emergency medicine', 'emergency', 'er'],
        'family': ['family medicine', 'family practice', 'family physician'],
        'internal': ['internal medicine', 'internist'],
        'surgery': ['surgery', 'surgeon', 'surgical']
    }
    
    # Check if either specialty contains a common root
    for root, variations in specialty_roots.items():
        spec1_match = any(var in spec1 for var in variations)
        spec2_match = any(var in spec2 for var in variations)
        
        if spec1_match and spec2_match:
            return True
    
    # Check for simple word overlap (at least 4 characters)
    words1 = [w for w in spec1.split() if len(w) >= 4]
    words2 = [w for w in spec2.split() if len(w) >= 4]
    
    for w1 in words1:
        for w2 in words2:
            if w1 in w2 or w2 in w1:
                return True
    
    return False

# Analyze company and find lookalikes
def find_lookalikes_from_company_data(company_data, similarity_threshold=85, max_results=None, include_contacts=True, page=1, page_size=10, use_cache=True):
    """
    Find similar organizations based on provided company data.
    This allows Claude's HubSpot connector to fetch deal data, then pass it here.
    
    Args:
        company_data: dict - Pass ANY properties from HubSpot deal. Will use what's available.
                      Location priority: billing_state/billing_city from deal, 
                      then lc_us_state/lc_city from company
                      Specialty: specialty or specialties field (fuzzy matched)
        similarity_threshold: minimum score to include (0-100)
        max_results: max number of results to return total (None = unlimited, returns ALL matches)
        include_contacts: whether to fetch contacts for each organization
        page: page number for pagination (1-indexed)
        page_size: number of results per page
        use_cache: whether to use cached results if available
    
    Returns:
        Dict with paginated list of similar organizations with similarity scores and contacts
    """
    
    # Extract state with priority: billing_state from deal, then lc_us_state from company
    state = (
        company_data.get("billing_state") or 
        company_data.get("lc_us_state") or
        company_data.get("state")
    )
    
    if not state:
        return {
            "error": "Missing required field: state",
            "hint": "Pass billing_state from deal or lc_us_state from associated company",
            "received_fields": list(company_data.keys()) if isinstance(company_data, dict) else ["Not a dict"]
        }
    
    # Try multiple possible specialty field names
    specialty_raw = (
        company_data.get("specialty") or
        company_data.get("specialties") or
        company_data.get("primary_specialty")
    )

    # Split semicolon-delimited specialties (e.g. "General Practice;Specialist")
    specialty_list = []
    if specialty_raw:
        specialty_list = [s.strip() for s in str(specialty_raw).split(';') if s.strip()]

    # Keep original for cache key compatibility; use first specialty as primary
    specialty = specialty_list[0] if specialty_list else None

    # LLM-based specialty expansion: find medically related specialties
    expanded_specialties = []
    for spec in specialty_list:
        expanded = get_expanded_specialties(spec)
        for exp_spec in expanded:
            if exp_spec.lower() not in [s.lower() for s in specialty_list]:
                expanded_specialties.append(exp_spec)

    # Deduplicate expanded list
    seen = set()
    unique_expanded = []
    for spec in expanded_specialties:
        if spec.lower() not in seen:
            seen.add(spec.lower())
            unique_expanded.append(spec)
    expanded_specialties = unique_expanded

    # Store expanded specialties on company_data so scoring functions can access them
    company_data['_expanded_specialties'] = expanded_specialties

    # Resolve effective city
    city = (
        company_data.get("billing_city") or
        company_data.get("lc_city") or
        company_data.get("city")
    )

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"[LOOKALIKES ENGINE] Resolved search parameters:", file=sys.stderr)
    print(f"  state              = {repr(state)}", file=sys.stderr)
    print(f"  city               = {repr(city)}", file=sys.stderr)
    print(f"  specialty_raw      = {repr(specialty_raw)}", file=sys.stderr)
    print(f"  specialty_list     = {specialty_list}", file=sys.stderr)
    print(f"  expanded_specialties = {expanded_specialties}", file=sys.stderr)
    print(f"  all company_data keys = {list(company_data.keys())}", file=sys.stderr)
    print(f"{'='*60}\n", file=sys.stderr)

    # OPTIMIZATION: Check cache first
    cache_key = get_cache_key(
        "find_lookalikes",
        state=state,
        specialty=specialty,
        expanded_specialties=sorted([s.lower() for s in expanded_specialties]),
        threshold=similarity_threshold,
        max_results=max_results,
        include_contacts=include_contacts
    )
    
    if use_cache:
        cached = get_cached_result(cache_key)
        if cached:
            # Apply pagination to cached results
            total_orgs = cached["total_matches"]
            start_idx = (page - 1) * page_size
            end_idx = start_idx + page_size
            
            return {
                **cached,
                "page": page,
                "page_size": page_size,
                "total_pages": (total_orgs + page_size - 1) // page_size,
                "lookalike_organizations": cached["lookalike_organizations"][start_idx:end_idx]
            }
    
    conn = get_databricks_connection()
    cursor = conn.cursor()
    
    # EXHAUSTIVE SEARCH: No artificial limits, get ALL matching organizations
    # Only filter by state (required) and optionally by specialty
    if include_contacts:
        query = f"""
        WITH filtered_orgs AS (
            SELECT
                definitive_id,
                physician_group_name,
                combined_main_specialty,
                state,
                city,
                zip_code,
                physician_count,
                ambulatory_emr,
                hs_id,
                website
            FROM prod_analytics_global.exposure.sales__definitive_physician_companies
            WHERE UPPER(state) = '{state.upper()}'
            {("AND (" + " OR ".join(f"LOWER(combined_main_specialty) LIKE LOWER('%{s}%')" for s in (specialty_list + expanded_specialties)) + ")") if (specialty_list or expanded_specialties) else ""}
        ),
        physicians AS (
            SELECT 
                p.FIRST_NAME as phys_first_name,
                p.LAST_NAME as phys_last_name,
                p.PRIMARY_SPECIALTY,
                p.EXECUTIVE_FLAG,
                p.BUSINESS_EMAIL as phys_business_email,
                p.DIRECT_EMAIL_PRIMARY as phys_direct_email_primary,
                p.DIRECT_EMAIL_SECONDARY as phys_direct_email_secondary,
                p.MOBILE_PHONE_PRIMARY as phys_mobile_primary,
                p.MOBILE_PHONE_SECONDARY as phys_mobile_secondary,
                p.DEFINITIVE_ID as phys_definitive_id,
                c.physician_group_name as phys_group_name
            FROM prod_analytics_global.ad_hoc.us_phys_report p
             JOIN prod_analytics_global.exposure.sales__definitive_physician_companies c
                ON p.DEFINITIVE_ID = c.definitive_id
            WHERE p.DEFINITIVE_ID IN (SELECT definitive_id FROM filtered_orgs)
              AND (
                p.DIRECT_EMAIL_PRIMARY IS NOT NULL 
                OR p.DIRECT_EMAIL_SECONDARY IS NOT NULL 
                OR p.MOBILE_PHONE_PRIMARY IS NOT NULL 
                OR p.MOBILE_PHONE_SECONDARY IS NOT NULL
              )
        ),
        executives AS (
            SELECT 
                e.FIRST_NAME as exec_first_name,
                e.LAST_NAME as exec_last_name,
                e.PHYSICIAN_LEADER,
                e.BUSINESS_EMAIL as exec_business_email,
                e.DIRECT_EMAIL_PRIMARY as exec_direct_email_primary,
                e.DIRECT_EMAIL_SECONDARY as exec_direct_email_secondary,
                e.MOBILE_PHONE_PRIMARY as exec_mobile_primary,
                e.MOBILE_PHONE_SECONDARY as exec_mobile_secondary,
                e.DEFINITIVE_ID as exec_definitive_id,
                c.physician_group_name as exec_group_name,
                e.TITLE,
                e.LINKEDIN_PROFILE
            FROM prod_analytics_global.ad_hoc.us_executive_report e
            JOIN prod_analytics_global.exposure.sales__definitive_physician_companies c
                ON e.DEFINITIVE_ID = c.definitive_id 
            WHERE e.DEFINITIVE_ID IN (SELECT definitive_id FROM filtered_orgs)
              AND (
                e.LINKEDIN_PROFILE IS NOT NULL 
                OR e.DIRECT_EMAIL_PRIMARY IS NOT NULL 
                OR e.DIRECT_EMAIL_SECONDARY IS NOT NULL 
                OR e.MOBILE_PHONE_PRIMARY IS NOT NULL 
                OR e.MOBILE_PHONE_SECONDARY IS NOT NULL
              )
        )
        SELECT 
            o.*,
            p.phys_first_name,
            p.phys_last_name,
            p.PRIMARY_SPECIALTY,
            p.EXECUTIVE_FLAG,
            p.phys_business_email,
            p.phys_direct_email_primary,
            p.phys_direct_email_secondary,
            p.phys_mobile_primary,
            p.phys_mobile_secondary,
            p.phys_group_name,
            e.exec_first_name,
            e.exec_last_name,
            e.PHYSICIAN_LEADER,
            e.exec_business_email,
            e.exec_direct_email_primary,
            e.exec_direct_email_secondary,
            e.exec_mobile_primary,
            e.exec_mobile_secondary,
            e.exec_group_name,
            e.TITLE,
            e.LINKEDIN_PROFILE
        FROM filtered_orgs o
        LEFT JOIN physicians p ON o.definitive_id = p.phys_definitive_id
        LEFT JOIN executives e ON o.definitive_id = e.exec_definitive_id
        """
    else:
        # If not including contacts, just get ALL organizations matching criteria
        query = f"""
        SELECT
            definitive_id,
            physician_group_name,
            combined_main_specialty,
            state,
            city,
            zip_code,
            physician_count,
            ambulatory_emr,
            hs_id,
            website
        FROM prod_analytics_global.exposure.sales__definitive_physician_companies
        WHERE UPPER(state) = '{state.upper()}'
        {("AND (" + " OR ".join(f"LOWER(combined_main_specialty) LIKE LOWER('%{s}%')" for s in (specialty_list + expanded_specialties)) + ")") if (specialty_list or expanded_specialties) else ""}
        """
    
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in results]
    except Exception as e:
        cursor.close()
        conn.close()
        return {
            "error": f"Failed to query Definitive Healthcare: {str(e)}",
            "company_data": company_data
        }
    
    cursor.close()
    conn.close()
    
    # Process results in-memory
    if include_contacts and rows:
        # Group rows by organization
        org_map = {}
        for row in rows:
            def_id = row.get("definitive_id")
            
            # Initialize organization if not seen
            if def_id not in org_map:
                org_map[def_id] = {
                    "definitive_id": def_id,
                    "physician_group_name": row.get("physician_group_name"),
                    "combined_main_specialty": row.get("combined_main_specialty"),
                    "state": row.get("state"),
                    "city": row.get("city"),
                    "zip_code": row.get("zip_code"),
                    "physician_count": row.get("physician_count"),
                    "ambulatory_emr": row.get("ambulatory_emr"),
                    "hs_id": row.get("hs_id"),
                    "website": row.get("website"),
                    "physicians": [],
                    "executives": []
                }
            
            # Add physician if present and has valid contact info
            if row.get("phys_first_name"):
                phys = {
                    "FIRST_NAME": row.get("phys_first_name"),
                    "LAST_NAME": row.get("phys_last_name"),
                    "PRIMARY_SPECIALTY": row.get("PRIMARY_SPECIALTY"),
                    "EXECUTIVE_FLAG": row.get("EXECUTIVE_FLAG"),
                    "BUSINESS_EMAIL": row.get("phys_business_email"),
                    "DIRECT_EMAIL_PRIMARY": row.get("phys_direct_email_primary"),
                    "DIRECT_EMAIL_SECONDARY": row.get("phys_direct_email_secondary"),
                    "MOBILE_PHONE_PRIMARY": row.get("phys_mobile_primary"),
                    "MOBILE_PHONE_SECONDARY": row.get("phys_mobile_secondary"),
                    "physician_group_name": row.get("phys_group_name")
                }
                # Double-check valid contact info and avoid duplicates
                if has_valid_contact_info(phys, "physician") and phys not in org_map[def_id]["physicians"]:
                    org_map[def_id]["physicians"].append(phys)
            
            # Add executive if present and has valid contact info
            if row.get("exec_first_name"):
                exec_data = {
                    "FIRST_NAME": row.get("exec_first_name"),
                    "LAST_NAME": row.get("exec_last_name"),
                    "PHYSICIAN_LEADER": row.get("PHYSICIAN_LEADER"),
                    "BUSINESS_EMAIL": row.get("exec_business_email"),
                    "DIRECT_EMAIL_PRIMARY": row.get("exec_direct_email_primary"),
                    "DIRECT_EMAIL_SECONDARY": row.get("exec_direct_email_secondary"),
                    "MOBILE_PHONE_PRIMARY": row.get("exec_mobile_primary"),
                    "MOBILE_PHONE_SECONDARY": row.get("exec_mobile_secondary"),
                    "physician_group_name": row.get("exec_group_name"),
                    "TITLE": row.get("TITLE"),
                    "LINKEDIN_PROFILE": row.get("LINKEDIN_PROFILE")
                }
                # Double-check valid contact info and avoid duplicates
                if has_valid_contact_info(exec_data, "executive") and exec_data not in org_map[def_id]["executives"]:
                    org_map[def_id]["executives"].append(exec_data)
        
        orgs = list(org_map.values())
    else:
        # No contacts requested, just convert rows to org format
        orgs = [{
            "definitive_id": row.get("definitive_id"),
            "physician_group_name": row.get("physician_group_name"),
            "combined_main_specialty": row.get("combined_main_specialty"),
            "state": row.get("state"),
            "city": row.get("city"),
            "zip_code": row.get("zip_code"),
            "physician_count": row.get("physician_count"),
            "ambulatory_emr": row.get("ambulatory_emr"),
            "hs_id": row.get("hs_id"),
            "website": row.get("website"),
            "physicians": [],
            "executives": []
        } for row in rows]
    
    # Lower threshold when LLM expansion is active so medically related (State+MedRelated=75) results appear
    effective_threshold = 55 if expanded_specialties else 65

    # Parallel similarity scoring using multiprocessing
    try:
        # Prepare arguments for parallel processing
        scoring_args = [(company_data, org) for org in orgs]
        
        # Use multiprocessing pool (limit to reasonable number of workers)
        num_workers = min(cpu_count(), len(orgs), 8)  # Max 8 workers
        
        if num_workers > 1 and len(orgs) > 10:  # Only use parallel for larger datasets
            with Pool(num_workers) as pool:
                scoring_results = pool.map(score_single_org, scoring_args)
        else:
            # For small datasets, serial processing is faster (no overhead)
            scoring_results = [score_single_org(args) for args in scoring_args]
        
        # Build scored organizations - EXHAUSTIVE: include ALL that meet threshold
        scored_orgs = []
        for org, score, reasons in scoring_results:
            if score >= effective_threshold:
                org["similarity_score"] = score
                org["match_reasons"] = reasons
                scored_orgs.append(org)

    except Exception as e:
        # Fallback to serial processing if parallel fails
        print(f"Parallel processing failed, using serial: {e}")
        scored_orgs = []
        for org in orgs:
            score = calculate_similarity_score(company_data, org)
            if score >= effective_threshold:
                org["similarity_score"] = score
                org["match_reasons"] = get_match_reasons(company_data, org, score)
                scored_orgs.append(org)
    
    # Sort by similarity score
    scored_orgs.sort(key=lambda x: x["similarity_score"], reverse=True)
    
    # Apply max_results limit if specified (after sorting)
    if max_results is not None:
        scored_orgs = scored_orgs[:max_results]
    
    # Prepare full result for caching
    full_result = {
        "source_company": company_data,
        "total_matches": len(scored_orgs),
        "similarity_threshold": similarity_threshold,
        "lookalike_organizations": scored_orgs
    }
    
    # Cache the full result
    if use_cache:
        set_cached_result(cache_key, full_result)
    
    # Apply pagination
    total_orgs = len(scored_orgs)
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    
    return {
        "source_company": company_data,
        "total_matches": total_orgs,
        "similarity_threshold": similarity_threshold,
        "page": page,
        "page_size": page_size,
        "total_pages": (total_orgs + page_size - 1) // page_size if total_orgs > 0 else 0,
        "lookalike_organizations": scored_orgs[start_idx:end_idx]
    }

def get_match_reasons(company_data, org_data, score):
    """Generate human-readable match reasons based on tier score"""
    tier_labels = {
        95: "Same city, same state, same specialty",
        85: "Same state, same specialty",
        75: "Same city, same state, similar specialty",
        65: "Same city, same state",
        55: "Same state, similar specialty",
    }

    reasons = []

    # Add tier label
    tier_label = tier_labels.get(score)
    if tier_label:
        reasons.append(tier_label)

    # Add specifics
    org_state = str(org_data.get("state", "")).upper().strip()
    org_city = str(org_data.get("city", "")).strip()
    org_specialty = str(org_data.get("combined_main_specialty", "")).strip()

    details = []
    if org_city:
        details.append(org_city)
    if org_state:
        details.append(org_state)
    if org_specialty:
        details.append(org_specialty)

    if details:
        reasons.append(" · ".join(details))

    return reasons

@flask_app.route('/api/clay-seed', methods=['POST'])
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

@flask_app.route('/api/lookalikes', methods=['GET'])
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