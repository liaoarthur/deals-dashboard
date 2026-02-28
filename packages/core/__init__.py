"""GTM Intelligence Core Library.

Shared utilities for healthcare GTM analysis including Databricks access,
HubSpot integration, Definitive Healthcare queries, and lookalike scoring.

Import directly from submodules:
    from core.cache import get_cache_key
    from core.hubspot import search_hubspot_deals
    from core.lookalikes import find_lookalikes_from_company_data
"""
from dotenv import load_dotenv
load_dotenv()

__all__ = [
    "apollo",
    "cache",
    "contacts",
    "databricks",
    "definitive",
    "hubspot",
    "lookalikes",
    "scoring",
    "specialty",
]
