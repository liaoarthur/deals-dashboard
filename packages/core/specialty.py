"""LLM-powered specialty expansion for medical specialty matching."""

import json
import os
import sys
import time

from openai import OpenAI

from core.databricks import get_databricks_connection

# OpenAI client for LLM-powered specialty expansion (optional — degrades gracefully)
_openai_api_key = os.getenv('OPENAI_API_KEY')
_openai_client = OpenAI(api_key=_openai_api_key) if _openai_api_key else None

# LLM specialty expansion cache — longer TTL since medical knowledge is stable
_specialty_expansion_cache = {}
_specialty_expansion_ttl = 86400  # 24 hours

# Cache for the full list of distinct Definitive Healthcare specialties
_definitive_specialties_cache = {
    "specialties": None,
    "timestamp": 0
}
_definitive_specialties_ttl = 86400  # 24 hours


def get_definitive_specialties():
    """
    Fetch all distinct combined_main_specialty values from the Definitive table.
    Cached for 24 hours. Returns a list of strings.
    """
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
    if not input_specialty or not input_specialty.strip():
        return []

    cache_key = input_specialty.strip().lower()

    # Check cache
    if cache_key in _specialty_expansion_cache:
        result, timestamp = _specialty_expansion_cache[cache_key]
        if time.time() - timestamp < _specialty_expansion_ttl:
            return result

    # If no OpenAI client, return empty (graceful degradation)
    if not _openai_client:
        return []

    # Fetch real Definitive specialties
    definitive_specialties = get_definitive_specialties()
    if not definitive_specialties:
        return []

    specialty_list_str = "\n".join(f"- {s}" for s in definitive_specialties)

    try:
        response = _openai_client.chat.completions.create(
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
