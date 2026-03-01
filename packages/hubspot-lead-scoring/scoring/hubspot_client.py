"""HubSpot API client — fetch lead, contact, form submission, and company data.

Flow: Lead (primary) → associated Contact (enrichment + forms) → associated Company (size/revenue)
"""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

HUBSPOT_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN")
BASE_URL = "https://api.hubapi.com"


def _headers():
    if not HUBSPOT_TOKEN:
        raise ValueError("HUBSPOT_ACCESS_TOKEN environment variable not set")
    return {
        "Authorization": f"Bearer {HUBSPOT_TOKEN}",
        "Content-Type": "application/json",
    }


# ─── Lead ─────────────────────────────────────────────────────────────────────

LEAD_PROPERTIES = [
    "hs_lead_name", "hs_lead_type", "lead_trigger", "hs_lead_label", "hs_lead_status",
    "hs_pipeline", "hs_pipeline_stage",
    # Scoring-relevant Lead properties
    "team_size",
    "user_role",
    "message__form_submission_",
    "hs_associated_company_name",
    "hs_primary_associated_object_name",
]


def get_lead(lead_id):
    """Fetch a single lead with all scoring-relevant properties."""
    url = f"{BASE_URL}/crm/v3/objects/leads/{lead_id}"
    resp = requests.get(url, headers=_headers(), params={
        "properties": ",".join(LEAD_PROPERTIES),
    })
    resp.raise_for_status()
    return resp.json()


def get_associated_contact_from_lead(lead_id):
    """Resolve the contact associated with a lead. Returns contact_id or None."""
    url = f"{BASE_URL}/crm/v3/objects/leads/{lead_id}/associations/contacts"
    resp = requests.get(url, headers=_headers())
    if resp.status_code != 200:
        print(f"[hubspot] Could not fetch contact association for lead {lead_id}: {resp.status_code}", file=sys.stderr)
        return None

    results = resp.json().get("results", [])
    if not results:
        return None

    return results[0].get("toObjectId") or results[0].get("id")


def write_lead_score(lead_id, tier_display, rationale):
    """
    Write scoring results back to the HubSpot Lead record.

    Updates two custom properties:
    - gtme_lead_score: single-line text, e.g. "A-Priority [87]"
    - gtme_lead_score_details: multi-line text, the rationale narrative
    """
    url = f"{BASE_URL}/crm/v3/objects/leads/{lead_id}"
    payload = {
        "properties": {
            "gtme_lead_score": tier_display,
            "gtme_lead_score_details": rationale,
        }
    }

    try:
        resp = requests.patch(url, headers=_headers(), json=payload)
        resp.raise_for_status()
        print(f"[hubspot] Wrote score to lead {lead_id}: {tier_display}", file=sys.stderr)
        return True
    except requests.exceptions.HTTPError as e:
        print(
            f"[hubspot] Failed to write score to lead {lead_id}: "
            f"{e.response.status_code} {e.response.text}",
            file=sys.stderr,
        )
        return False
    except Exception as e:
        print(f"[hubspot] Failed to write score to lead {lead_id}: {e}", file=sys.stderr)
        return False


def get_associated_lead_from_contact(contact_id):
    """Resolve the lead associated with a contact. Returns lead_id or None."""
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/leads"
    resp = requests.get(url, headers=_headers())
    if resp.status_code != 200:
        print(f"[hubspot] Could not fetch lead association for contact {contact_id}: {resp.status_code}", file=sys.stderr)
        return None

    results = resp.json().get("results", [])
    if not results:
        return None

    return results[0].get("toObjectId") or results[0].get("id")


def get_associated_company_from_lead(lead_id):
    """Resolve the company associated with a lead. Returns company_id or None."""
    url = f"{BASE_URL}/crm/v3/objects/leads/{lead_id}/associations/companies"
    resp = requests.get(url, headers=_headers())
    if resp.status_code != 200:
        return None

    results = resp.json().get("results", [])
    if not results:
        return None

    return results[0].get("toObjectId") or results[0].get("id")


# ─── Contact ─────────────────────────────────────────────────────────────────

CONTACT_PROPERTIES = [
    "email", "firstname", "lastname", "jobtitle", "company",
    "phone", "hs_lead_status", "lifecyclestage",
    "hs_analytics_source", "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "linkedin", "hs_linkedinid",
    "numemployees", "annualrevenue",
    "recent_conversion_event_name", "hs_latest_source",
    "message", "hs_content_membership_notes",
    "contact_specialty__f_2_",
    # Inbound scoring properties
    "organization_size",
    "organisation_size__product_",
    "company_employee_size_range__c_",
    "organisation_type__product_",
    "lc_job_title",
    "db_session_count",
    "db_last_active_date",
]


def get_contact(contact_id):
    """Fetch a single contact with all scoring-relevant properties."""
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    resp = requests.get(url, headers=_headers(), params={
        "properties": ",".join(CONTACT_PROPERTIES),
    })
    resp.raise_for_status()
    return resp.json()


# ─── Form submissions ────────────────────────────────────────────────────────

def get_form_submissions(contact_id):
    """Return list of form submissions for a contact (via the v1 forms API)."""
    url = f"{BASE_URL}/contacts/v1/contact/vid/{contact_id}/profile"
    resp = requests.get(url, headers=_headers())
    if resp.status_code != 200:
        print(f"[hubspot] Could not fetch form submissions for contact {contact_id}: {resp.status_code}", file=sys.stderr)
        return []

    data = resp.json()
    submissions = []
    for entry in data.get("form-submissions", []):
        fields = {}
        for f in entry.get("form-fields", []):
            fields[f.get("name", "")] = f.get("value", "")
        submissions.append({
            "form_id": entry.get("form-id"),
            "title": entry.get("title", ""),
            "timestamp": entry.get("timestamp"),
            "fields": fields,
        })

    return submissions


# ─── Company ──────────────────────────────────────────────────────────────────

COMPANY_PROPERTIES = "name,domain,numberofemployees,annualrevenue,industry,city,state,country,db_company_session_count,number_of_heidi_users"


def get_company(company_id):
    """Fetch a company by ID."""
    url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}"
    resp = requests.get(url, headers=_headers(), params={
        "properties": COMPANY_PROPERTIES,
    })
    if resp.status_code != 200:
        return None
    return resp.json()


def get_associated_company_from_contact(contact_id):
    """Fetch the primary associated company for a contact."""
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/companies"
    resp = requests.get(url, headers=_headers())
    if resp.status_code != 200:
        return None

    results = resp.json().get("results", [])
    if not results:
        return None

    company_id = results[0].get("toObjectId") or results[0].get("id")
    if not company_id:
        return None

    return get_company(company_id)


# ─── Lead search (backlog) ───────────────────────────────────────────────────

def search_unscored_leads(batch_size=10):
    """
    Search for the oldest unscored inbound US leads from the past 3 months.

    Filters (AND):
      - lead_trigger = "Inbound Marketing Qualified Lead" (HubSpot label: "Inbound Lead")
      - country = "United States"
      - hs_createdate >= 3 months ago
      - gtme_lead_score has no value (not yet scored)

    Returns (lead_ids, total_matching) sorted oldest-first, up to batch_size.
    """
    three_months_ago = (
        datetime.now(timezone.utc) - timedelta(days=90)
    ).strftime("%Y-%m-%dT00:00:00.000Z")

    url = f"{BASE_URL}/crm/v3/objects/leads/search"
    body = {
        "filterGroups": [{
            "filters": [
                {"propertyName": "lead_trigger", "operator": "EQ",
                 "value": "Inbound Marketing Qualified Lead"},
                {"propertyName": "country", "operator": "EQ",
                 "value": "United States"},
                {"propertyName": "hs_createdate", "operator": "GTE",
                 "value": three_months_ago},
                {"propertyName": "gtme_lead_score", "operator": "NOT_HAS_PROPERTY"},
            ]
        }],
        "sorts": [{"propertyName": "hs_createdate", "direction": "ASCENDING"}],
        "properties": ["hs_lead_name", "hs_createdate"],
        "limit": min(batch_size, 100),
    }

    resp = requests.post(url, headers=_headers(), json=body)
    resp.raise_for_status()
    data = resp.json()

    results = data.get("results", [])
    lead_ids = [r["id"] for r in results]
    total = data.get("total", len(lead_ids))

    print(f"[hubspot] Search found {total} unscored leads, returning batch of {len(lead_ids)}", file=sys.stderr)
    return lead_ids, total


# ─── Full context bundle ─────────────────────────────────────────────────────

def fetch_lead_context(lead_id, company_id=None):
    """
    Fetch everything needed for scoring, starting from a Lead object:
    Lead → associated Contact (for enrichment + form submissions) → Company (for size/revenue).

    If company_id is provided (e.g. from webhook), it's used directly instead of
    association lookup.

    Returns a unified context dict with lead as primary.
    """
    # 1. Fetch Lead
    lead = get_lead(lead_id)
    lead_props = lead.get("properties", {})

    # 2. Resolve associated Contact
    contact_id = get_associated_contact_from_lead(lead_id)
    contact_props = {}
    form_submissions = []

    if contact_id:
        try:
            contact = get_contact(contact_id)
            contact_props = contact.get("properties", {})
        except Exception as e:
            print(f"[hubspot] Failed to fetch contact {contact_id} for lead {lead_id}: {e}", file=sys.stderr)

        form_submissions = get_form_submissions(contact_id)

    # 3. Resolve Company
    company_props = {}
    if company_id:
        # Company ID provided directly (e.g. from webhook)
        company = get_company(company_id)
        if company:
            company_props = company.get("properties", {})
    else:
        # Association-based resolution: Lead → Company, fallback Contact → Company
        resolved_company_id = get_associated_company_from_lead(lead_id)
        if resolved_company_id:
            company = get_company(resolved_company_id)
            if company:
                company_props = company.get("properties", {})
        elif contact_id:
            company = get_associated_company_from_contact(contact_id)
            if company:
                company_props = company.get("properties", {})

    # 4. Merge properties — lead props take precedence, contact fills gaps
    merged_props = {**contact_props, **{k: v for k, v in lead_props.items() if v is not None}}

    return {
        "lead_id": lead_id,
        "contact_id": contact_id,
        "lead_properties": lead_props,
        "properties": merged_props,
        "form_submissions": form_submissions,
        "company": company_props,
    }
