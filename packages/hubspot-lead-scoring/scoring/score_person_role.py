"""Person/Role Lookup scoring module.

Priority order:
1. Title already in HubSpot data → score directly
2. LinkedIn URL available → note for enrichment, extract what we can
3. Claude API web search → look up person + company
"""

import re
import sys
from functools import lru_cache

from .config import get_person_role_config
from .claude_client import lookup_person


@lru_cache(maxsize=500)
def _cached_lookup(name, company):
    """Cache person lookups to avoid redundant API calls."""
    return lookup_person(name, company)


def score(context):
    """Compute person/role sub-score (0-100)."""
    config = get_person_role_config()
    props = context.get("properties", {})

    title = (props.get("jobtitle") or "").strip()
    firstname = (props.get("firstname") or "").strip()
    lastname = (props.get("lastname") or "").strip()
    company_name = (props.get("company") or "").strip()
    linkedin = (props.get("linkedin") or props.get("hs_linkedinid") or "").strip()

    full_name = f"{firstname} {lastname}".strip()

    # ─── Strategy 1: Title already in HubSpot ─────────────────────────────
    if title:
        seniority = _classify_title(title, config)
        score_value = config.get("seniority_scores", {}).get(seniority, 25)
        print(f"[person_role] title='{title}' seniority={seniority} score={score_value} (from HubSpot)", file=sys.stderr)
        return score_value

    # ─── Strategy 2: LinkedIn URL available ───────────────────────────────
    if linkedin:
        print(f"[person_role] LinkedIn URL found: {linkedin} (noted for future enrichment)", file=sys.stderr)
        # We can't scrape LinkedIn, but note it. Fall through to web search.

    # ─── Strategy 3: Claude web search ────────────────────────────────────
    if full_name and company_name:
        result = _cached_lookup(full_name, company_name)
        if result:
            seniority = result.get("seniority", "unknown")
            score_value = config.get("seniority_scores", {}).get(seniority, 25)
            print(f"[person_role] web lookup: seniority={seniority} score={score_value} confidence={result.get('confidence')}", file=sys.stderr)
            return score_value

    # ─── Fallback ─────────────────────────────────────────────────────────
    fallback = config.get("seniority_scores", {}).get("unknown", 25)
    print(f"[person_role] fallback score={fallback}", file=sys.stderr)
    return fallback


def _classify_title(title, config):
    """Map a job title string to a seniority level using word-boundary matching."""
    title_lower = title.lower()
    keywords_map = config.get("title_keywords", {})

    # Check in priority order (most senior first)
    priority = ["founder", "c_suite", "vp", "director", "manager", "senior", "individual"]
    for level in priority:
        keywords = keywords_map.get(level, [])
        for kw in keywords:
            # Use word-boundary regex to avoid substring false positives
            # (e.g. "cto" inside "director")
            pattern = r'(?:^|[\s,/&\-])' + re.escape(kw) + r'(?:$|[\s,/&\-])'
            if re.search(pattern, title_lower):
                return level

    return "unknown"
