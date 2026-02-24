"""Lookalike organization matching engine.

This is the main business logic function that finds similar healthcare organizations
from Definitive Healthcare data based on location and specialty matching.
"""

import sys
from multiprocessing import Pool, cpu_count

from core.cache import get_cache_key, get_cached_result, set_cached_result
from core.databricks import get_databricks_connection
from core.contacts import has_valid_contact_info
from core.specialty import get_expanded_specialties
from core.scoring import score_single_org, calculate_similarity_score, get_match_reasons


def find_lookalikes_from_company_data(company_data, similarity_threshold=85, max_results=None,
                                      include_contacts=True, page=1, page_size=10, use_cache=True):
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
