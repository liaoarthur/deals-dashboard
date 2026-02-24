"""HubSpot API wrappers: pipeline/stage mappings, owner lookup, specialty labels."""

import os
import sys
import requests
from functools import lru_cache
from datetime import datetime, timedelta

HUBSPOT_API_KEY = os.getenv("HUBSPOT_ACCESS_TOKEN")

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


def search_hubspot_deals(deal_stage=None, is_closed_won=None, country=None,
                         state=None, city=None, specialty=None, pipeline=None,
                         min_seats=None, max_seats=None, min_tso=None, max_tso=None,
                         min_amount=None, max_amount=None, days_back=None,
                         limit=50):
    """
    Search HubSpot deals with filters. Used by both Flask routes and MCP tools.
    Returns a list of formatted deal dicts.
    """
    if not HUBSPOT_API_KEY:
        raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable not set")

    mappings = get_hubspot_mappings()

    # Build filters
    filters = []
    if days_back:
        cutoff = int((datetime.now() - timedelta(days=int(days_back))).timestamp() * 1000)
        filters.append({"propertyName": "closedate", "operator": "GTE", "value": str(cutoff)})
    if deal_stage:
        filters.append({"propertyName": "dealstage", "operator": "EQ", "value": str(deal_stage)})
    if is_closed_won is not None:
        filters.append({"propertyName": "hs_is_closed_won", "operator": "EQ", "value": str(is_closed_won).lower()})
    if country:
        filters.append({"propertyName": "country", "operator": "EQ", "value": str(country)})
    if state:
        filters.append({"propertyName": "billing_state", "operator": "EQ", "value": str(state)})
    if city:
        filters.append({"propertyName": "billing_city", "operator": "EQ", "value": str(city)})
    if specialty:
        filters.append({"propertyName": "specialty_mcp_use", "operator": "EQ", "value": str(specialty)})
    if min_seats:
        filters.append({"propertyName": "seats_subscribed", "operator": "GTE", "value": str(min_seats)})
    if max_seats:
        filters.append({"propertyName": "seats_subscribed", "operator": "LTE", "value": str(max_seats)})
    if min_tso:
        filters.append({"propertyName": "total_serviceable_opportunity", "operator": "GTE", "value": str(min_tso)})
    if max_tso:
        filters.append({"propertyName": "total_serviceable_opportunity", "operator": "LTE", "value": str(max_tso)})
    if min_amount:
        filters.append({"propertyName": "amount_in_home_currency", "operator": "GTE", "value": str(min_amount)})
    if max_amount:
        filters.append({"propertyName": "amount_in_home_currency", "operator": "LTE", "value": str(max_amount)})

    # Build filter groups — if a pipeline is specified, filter to that pipeline;
    # otherwise search across Sales-Global and Expansion pipelines
    if pipeline:
        filter_groups = [{"filters": filters + [{"propertyName": "pipeline", "operator": "EQ", "value": str(pipeline)}]}]
    else:
        # Default: search Sales-Global + Expansion pipelines
        sales_pipeline_id = mappings['pipelines'].get('Sales - Global')
        expansion_pipeline_id = mappings['pipelines'].get('Expansion')
        filter_groups = []
        for pid in [sales_pipeline_id, expansion_pipeline_id]:
            if pid:
                filter_groups.append({
                    "filters": filters + [{"propertyName": "pipeline", "operator": "EQ", "value": pid}]
                })
        if not filter_groups:
            # Fallback: no pipeline filter
            filter_groups = [{"filters": filters}]

    body = {
        "filterGroups": filter_groups,
        "properties": DEAL_PROPERTIES,
        "limit": min(int(limit), 100),
        "sorts": [{"propertyName": "closedate", "direction": "DESCENDING"}]
    }

    response = requests.post(
        'https://api.hubapi.com/crm/v3/objects/deals/search',
        headers={
            'Authorization': f'Bearer {HUBSPOT_API_KEY}',
            'Content-Type': 'application/json'
        },
        json=body
    )

    if response.status_code != 200:
        raise Exception(f"HubSpot API error: {response.status_code} — {response.text}")

    results = response.json().get('results', [])

    # Format deals
    deals = []
    for deal in results:
        props = deal.get('properties', {})
        owner_name = get_owner_name(props.get('hubspot_owner_id'))
        stage_label = mappings['stages'].get(props.get('dealstage'), props.get('dealstage'))
        pipeline_label = mappings['pipelines'].get(props.get('pipeline'), props.get('pipeline'))
        specialty_label = get_specialty_label(props.get('specialty_mcp_use'))

        deals.append({
            "deal_id": props.get("hs_object_id"),
            "deal_name": props.get("dealname"),
            "deal_stage": stage_label,
            "pipeline": pipeline_label,
            "owner_name": owner_name,
            "close_date": props.get("closedate"),
            "amount": props.get("amount_in_home_currency"),
            "country": props.get("country"),
            "billing_state": props.get("billing_state"),
            "billing_city": props.get("billing_city"),
            "specialty": specialty_label,
            "product": props.get("product"),
            "seats_subscribed": props.get("seats_subscribed"),
            "total_serviceable_opportunity": props.get("total_serviceable_opportunity"),
            "is_closed_won": props.get("hs_is_closed_won"),
        })

    return deals
