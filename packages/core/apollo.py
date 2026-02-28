"""Apollo.io People Search API client."""

import os
import sys

import requests

APOLLO_API_KEY = os.getenv('APOLLO_API_KEY')
APOLLO_BASE_URL = 'https://api.apollo.io/api/v1'


def search_apollo_contacts(domain, per_page=25):
    """
    Search Apollo for contacts at a company domain.

    Args:
        domain: Company domain (e.g. "acme.com")
        per_page: Number of results (default 25, max 100)

    Returns:
        List of contacts in internal format: [{name, title, email, phone, linkedin, source}]
        Returns empty list on error or if API key not configured.
    """
    if not APOLLO_API_KEY:
        return []

    payload = {
        'api_key': APOLLO_API_KEY,
        'q_organization_domains': domain,
        'page': 1,
        'per_page': per_page,
    }

    try:
        resp = requests.post(
            f'{APOLLO_BASE_URL}/mixed_people/search',
            json=payload,
            headers={'Content-Type': 'application/json'},
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"[APOLLO] API returned {resp.status_code}: {resp.text[:200]}", file=sys.stderr)
            return []

        people = resp.json().get('people', [])
        return [_normalize_apollo_contact(p) for p in people if p]
    except requests.exceptions.Timeout:
        print("[APOLLO] Request timed out", file=sys.stderr)
        return []
    except Exception as e:
        print(f"[APOLLO] Error: {e}", file=sys.stderr)
        return []


def _normalize_apollo_contact(person):
    """Convert Apollo person dict to internal contact format."""
    name_parts = [person.get('first_name', ''), person.get('last_name', '')]
    name = ' '.join(p for p in name_parts if p).strip()

    phone = None
    phone_numbers = person.get('phone_numbers') or []
    if phone_numbers:
        phone = phone_numbers[0].get('raw_number')

    return {
        'name': name or None,
        'title': person.get('title') or None,
        'email': person.get('email') or None,
        'phone': phone,
        'linkedin': person.get('linkedin_url') or None,
        'source': 'apollo',
    }
