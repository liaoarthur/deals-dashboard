"""Claude API client — shared Anthropic SDK wrapper for scoring modules."""

import os
import json
import sys
from functools import lru_cache

_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client

    from anthropic import Anthropic

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")

    _client = Anthropic(api_key=api_key)
    return _client


def analyze_message(prompt, message_text):
    """
    Send a message to Claude for analysis. Returns parsed JSON response.
    Uses claude-sonnet-4-20250514 as specified.
    """
    client = _get_client()

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[
                {"role": "user", "content": prompt.replace("{{MESSAGE}}", message_text)}
            ],
        )

        text = response.content[0].text

        # Extract JSON from response (handle markdown code blocks)
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        return json.loads(text)

    except Exception as e:
        print(f"[claude] Message analysis failed: {e}", file=sys.stderr)
        return None


def lookup_person(name, company, email=""):
    """
    Use Claude with web search to look up a person's title/role.
    Returns structured dict with title, seniority, is_clinical, and confidence.
    """
    client = _get_client()

    # Build extra context from email
    extra_context = ""
    domain = ""
    if email:
        domain = email.split('@')[-1] if '@' in email else ""
        extra_context = f"Email: {email}\nDomain: {domain}\n"

    # Build site search hint from email domain
    site_search = f'site:{domain} "{name}"' if domain else f'"{company}" staff OR leadership OR directory'

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
            messages=[{
                "role": "user",
                "content": (
                    f"I need to find the job title and role of this person. You MUST perform web searches — do not guess or return unknown without searching first.\n\n"
                    f"Name: {name}\n"
                    f"Organization: {company}\n"
                    f"{extra_context}\n"
                    f"IMPORTANT: Try these searches in order before giving up:\n"
                    f'1. "{name} {company}"\n'
                    f'2. "{name}" LinkedIn\n'
                    f'3. {site_search}\n\n'
                    f"Even if one search returns no results, try the next one. Search result snippets often contain titles — look for patterns like "
                    f'"Director of...", "VP of...", "Manager at...", "Dr.", "MD", etc. in the search result text.\n\n'
                    f"After searching, return ONLY a JSON object with these fields:\n"
                    f'  "title": their job title (string or null if truly not found after all searches),\n'
                    f'  "seniority": one of "founder", "c_suite", "clinical_leader", "vp", "director", "manager", "senior", "individual", "unknown",\n'
                    f'  "is_clinical": true if the person holds a clinical/medical role (physician, doctor, surgeon, nurse practitioner, etc.), false otherwise,\n'
                    f'  "is_decision_maker": true if this person can make purchasing or organizational decisions (e.g., practice owner, administrator, department head, director, C-suite), false otherwise,\n'
                    f'  "notable_achievements": a brief string describing any notable leadership positions, board memberships, awards, or significant roles (or null if none found),\n'
                    f'  "linkedin_url": their LinkedIn profile URL if found (string or null),\n'
                    f'  "confidence": "high", "medium", or "low"\n'
                    f"Return only the JSON object, no other text."
                ),
            }],
        )

        # Extract the final text block (after tool use)
        text = None
        for block in response.content:
            if block.type == "text":
                text = block.text

        if not text:
            return None

        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        return json.loads(text)

    except Exception as e:
        print(f"[claude] Person lookup failed for {name} at {company}: {e}", file=sys.stderr)
        return None


@lru_cache(maxsize=500)
def lookup_company(company_name):
    """
    Use Claude with web search to evaluate a healthcare organization.
    Returns structured dict with org_type, estimated_size, is_notable, and confidence.
    """
    client = _get_client()

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 3}],
            messages=[{
                "role": "user",
                "content": (
                    f'Search for "{company_name}" and determine what kind of organization it is. '
                    f"Return ONLY a JSON object with these fields:\n"
                    f'  "org_type": one of "hospital_system", "academic_medical", "mso_aco_gpo", "physician_group", "clinic", "allied_health", "healthtech_vendor", "non_healthcare", "unknown".\n'
                    f'    Classification guide:\n'
                    f'    - "hospital_system": hospitals, health systems, integrated delivery networks\n'
                    f'    - "academic_medical": university hospitals, academic medical centers, teaching hospitals\n'
                    f'    - "mso_aco_gpo": management services organizations (MSOs), accountable care organizations (ACOs), group purchasing organizations (GPOs), independent practice associations (IPAs)\n'
                    f'    - "physician_group": physician practices, specialty clinics (e.g., cardiology practice, orthopedic clinic, dermatology group), medical groups\n'
                    f'    - "clinic": standalone clinics, urgent care, community health centers\n'
                    f'    - "allied_health": physical therapy, chiropractic, behavioral health, dental practices\n'
                    f'    - "healthtech_vendor": EMR/EHR companies, healthtech SaaS, health IT vendors, companies seeking integration/partnerships\n'
                    f'    - "non_healthcare": organizations with no healthcare connection\n'
                    f'  "estimated_size": one of "large" (500+ providers or major regional/national system), "medium" (50-500 providers), "small" (under 50 providers), "unknown",\n'
                    f'  "is_notable": true if the organization is nationally or regionally recognized (e.g., Mayo Clinic, Cleveland Clinic, Kaiser, Johns Hopkins, major university hospitals, large regional health systems), false otherwise,\n'
                    f'  "confidence": "high", "medium", or "low"\n'
                    f"Return only the JSON object, no other text."
                ),
            }],
        )

        # Extract the final text block (after tool use)
        text = None
        for block in response.content:
            if block.type == "text":
                text = block.text

        if not text:
            return None

        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        return json.loads(text)

    except Exception as e:
        print(f"[claude] Company lookup failed for {company_name}: {e}", file=sys.stderr)
        return None
