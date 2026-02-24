"""Similarity scoring engine for healthcare organization matching."""


def score_single_org(args):
    """
    Worker function for parallel similarity scoring.
    Takes tuple of (company_data, org) and returns (org, score, reasons)
    """
    company_data, org = args
    score = calculate_similarity_score(company_data, org)
    reasons = get_match_reasons(company_data, org, score)
    return (org, score, reasons)


def calculate_similarity_score(company_data, definitive_org):
    """
    Calculate similarity score between company and Definitive org.

    Tier-based scoring (highest match wins):
      Tier 1: same city + same state + same specialty     -> 95%
      Tier 2: same state + same specialty                 -> 85%
      Tier 3: same city + same state + similar specialty  -> 75%
      Tier 4: same city + same state                      -> 65%
      Tier 5: same state + similar specialty              -> 55%

    "Same specialty" = exact or fuzzy spelling match against deal specialties.
    "Similar specialty" = medically related via LLM expansion.
    State match is always required -- no state match -> 0.
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

    # State match is required -- no state match means 0
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
        return 0   # State-only match with no city or specialty -- not useful


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
        reasons.append(" . ".join(details))

    return reasons
