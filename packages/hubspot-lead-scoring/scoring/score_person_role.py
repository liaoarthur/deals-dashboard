"""Person/Role Lookup scoring module.

Priority order:
1. user_role from Lead object → classify directly
2. jobtitle from Contact → classify directly
3. LinkedIn URL available → note for enrichment, fall through
4. Claude web search (name + company from Lead) → look up person
5. Claude web search (name + company + email) → enhanced lookup if #4 was low confidence

Web search always runs as a supplementary signal when name + company are available,
even when a title is already known. This lets us assess the person's importance/influence
beyond just seniority classification, and stores the result for cross-module use.
"""

import re
import sys
from functools import lru_cache

from .config import get_person_role_config
from .claude_client import lookup_person


@lru_cache(maxsize=500)
def _cached_lookup(name, company, email=""):
    """Cache person lookups to avoid redundant API calls."""
    return lookup_person(name, company, email)


def score(context):
    """Compute person/role sub-score (0-100)."""
    config = get_person_role_config()
    props = context.get("properties", {})
    lead_props = context.get("lead_properties", {})

    # Sources for title/role (Lead properties take priority)
    user_role = (props.get("user_role") or "").strip()
    jobtitle = (props.get("jobtitle") or "").strip()

    # Sources for name (Lead properties first, then Contact)
    lead_name = (lead_props.get("hs_primary_associated_object_name") or "").strip()
    firstname = (props.get("firstname") or "").strip()
    lastname = (props.get("lastname") or "").strip()
    contact_name = f"{firstname} {lastname}".strip()
    full_name = lead_name or contact_name

    # Sources for company — prefer the most descriptive name
    lead_company = (lead_props.get("hs_associated_company_name") or "").strip()
    contact_company = (props.get("company") or "").strip()
    hub_company = (context.get("company", {}).get("name") or "").strip()
    company_name = _best_company_name(lead_company, contact_company, hub_company)

    # Email for enhanced search
    email = (props.get("email") or "").strip()
    linkedin = (props.get("linkedin") or props.get("hs_linkedinid") or "").strip()

    # ─── Step 1: Get base score from title (user_role or jobtitle) ───────
    base_score = None
    base_seniority = "unknown"

    if user_role:
        seniority = classify_title(user_role, config)
        if seniority != "unknown":
            base_seniority = seniority
            base_score = config.get("seniority_scores", {}).get(seniority, 25)
            print(f"[person_role] user_role='{user_role}' seniority={seniority} score={base_score} (from Lead)", file=sys.stderr)

    if base_score is None and jobtitle:
        base_seniority = classify_title(jobtitle, config)
        base_score = config.get("seniority_scores", {}).get(base_seniority, 25)
        print(f"[person_role] jobtitle='{jobtitle}' seniority={base_seniority} score={base_score} (from Contact)", file=sys.stderr)

    # ─── Step 2: Note LinkedIn if available ──────────────────────────────
    if linkedin:
        print(f"[person_role] LinkedIn URL found: {linkedin} (noted for future enrichment)", file=sys.stderr)

    # ─── Step 3: Always attempt web search for supplementary signal ──────
    lookup_result = None
    if full_name and company_name:
        lookup_result = _cached_lookup(full_name, company_name, "")
        if (not lookup_result or lookup_result.get("confidence") == "low") and email:
            lookup_result = _cached_lookup(full_name, company_name, email)

    # Store lookup result for cross-module use (specialty_company needs person notability)
    context.setdefault("_enrichment", {})["person_lookup"] = lookup_result

    # ─── Step 4: If no base score from title, use lookup result ──────────
    if base_score is None and lookup_result and lookup_result.get("confidence") != "low":
        return _score_from_lookup(lookup_result, config, "web_search")

    if base_score is None:
        base_score = config.get("seniority_scores", {}).get("unknown", 25)
        print(f"[person_role] fallback score={base_score}", file=sys.stderr)

    # ─── Step 5: Apply importance boost from web search ──────────────────
    if lookup_result and lookup_result.get("confidence") != "low":
        boost = _importance_boost(lookup_result, config)
        if boost > 0:
            old_score = base_score
            base_score = min(100, base_score + boost)
            print(
                f"[person_role] importance boost +{boost} ({old_score} → {base_score}) "
                f"decision_maker={lookup_result.get('is_decision_maker')} "
                f"notable={bool(lookup_result.get('notable_achievements'))} "
                f"clinical_leader={lookup_result.get('is_clinical') and lookup_result.get('seniority') in ('clinical_leader', 'c_suite', 'founder')}",
                file=sys.stderr,
            )

    return base_score


def _score_from_lookup(result, config, strategy):
    """Extract score from a Claude lookup result."""
    seniority = result.get("seniority", "unknown")
    score_value = config.get("seniority_scores", {}).get(seniority, 25)

    # Also apply importance boost for lookup-only scores
    boost = _importance_boost(result, config)
    score_value = min(100, score_value + boost)

    print(
        f"[person_role] web lookup ({strategy}): seniority={seniority} "
        f"score={score_value} (base + boost={boost}) confidence={result.get('confidence')} "
        f"is_clinical={result.get('is_clinical')} is_decision_maker={result.get('is_decision_maker')}",
        file=sys.stderr,
    )
    return score_value


def _importance_boost(result, config):
    """Boost score based on web search findings about person's importance."""
    boost = 0
    if result.get("is_decision_maker"):
        boost += config.get("decision_maker_boost", 10)
    if result.get("notable_achievements"):
        boost += config.get("notable_person_boost", 5)
    if result.get("is_clinical") and result.get("seniority") in ("clinical_leader", "c_suite", "founder"):
        boost += config.get("clinical_leadership_boost", 5)
    return boost


def _looks_like_domain(name):
    """Check if a string looks like a bare domain (e.g., 'esmc.org') rather than an org name."""
    return bool(re.match(r'^[\w\-]+\.\w{2,6}$', name))


def _best_company_name(*candidates):
    """Pick the most descriptive company name from multiple sources.
    Prefers longer, non-domain names over bare domains like 'esmc.org'."""
    real_names = [c for c in candidates if c and not _looks_like_domain(c)]
    if real_names:
        return max(real_names, key=len)
    # Fall back to whatever is available
    return next((c for c in candidates if c), "")


def classify_title(title, config):
    """Map a job title string to a seniority level using word-boundary matching."""
    title_lower = title.lower()
    keywords_map = config.get("title_keywords", {})

    # Check in priority order (most senior first, clinical_leader before c_suite)
    priority = ["founder", "clinical_leader", "c_suite", "vp", "director", "manager", "senior", "individual"]
    for level in priority:
        keywords = keywords_map.get(level, [])
        for kw in keywords:
            # Use word-boundary regex to avoid substring false positives
            # (e.g. "cto" inside "director")
            pattern = r'(?:^|[\s,/&\-])' + re.escape(kw) + r'(?:$|[\s,/&\-])'
            if re.search(pattern, title_lower):
                return level

    return "unknown"
