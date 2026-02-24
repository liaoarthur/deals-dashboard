"""Definitive Healthcare organization and contact queries via Databricks."""

from core.databricks import get_databricks_connection


def get_organizations_from_definitive(company_name=None, state=None, city=None, limit=10):
    """
    Search for organizations in Definitive Healthcare and get their firmographic details.
    Returns organizations with their definitive_id and key business metrics.
    """
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()

        query = """
        SELECT
            city,
            combined_main_specialty,
            definitive_id,
            physician_count,
            physician_group_name,
            state,
            zip_code,
            ambulatory_emr,
            hs_id
        FROM prod_analytics_global.exposure.sales__definitive_physician_companies
        WHERE 1=1
        """

        params = []
        if company_name:
            query += " AND LOWER(physician_group_name) LIKE LOWER(?)"
            params.append(f"%{company_name}%")
        if state:
            query += " AND UPPER(state) = UPPER(?)"
            params.append(state)
        if city:
            query += " AND LOWER(city) LIKE LOWER(?)"
            params.append(f"%{city}%")

        query += " LIMIT ?"
        params.append(limit)

        cursor.execute(query, params)
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        organizations = [dict(zip(columns, row)) for row in results]

        cursor.close()
        conn.close()

        return organizations
    except Exception as e:
        return []


def get_organization_contacts(definitive_id, contact_type="both", limit=50):
    """
    Get contacts (physicians and/or executives) for an organization using definitive_id.

    Args:
        definitive_id: The definitive ID of the organization
        contact_type: "physicians", "executives", or "both" (default)
        limit: Maximum number of results per contact type

    Returns:
        Dictionary with physicians and/or executives lists
    """
    try:
        conn = get_databricks_connection()
        cursor = conn.cursor()

        results = {
            "physicians": [],
            "executives": []
        }

        # Query physicians if requested
        if contact_type in ["physicians", "both"]:
            physicians_query = """
            SELECT
                p.FIRST_NAME,
                p.LAST_NAME,
                p.PRIMARY_SPECIALTY,
                p.EXECUTIVE_FLAG,
                p.BUSINESS_EMAIL,
                p.DIRECT_EMAIL_PRIMARY,
                p.DIRECT_EMAIL_SECONDARY,
                p.MOBILE_PHONE_PRIMARY,
                p.MOBILE_PHONE_SECONDARY,
                p.DEFINITIVE_ID,
                c.physician_group_name
            FROM prod_analytics_global.ad_hoc.us_phys_report p
            JOIN prod_analytics_global.exposure.sales__definitive_physician_companies c
                ON p.DEFINITIVE_ID = c.definitive_id
            WHERE p.DEFINITIVE_ID = ?
            LIMIT ?
            """

            cursor.execute(physicians_query, (definitive_id, limit))
            phys_results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            results["physicians"] = [dict(zip(columns, row)) for row in phys_results]

        # Query executives if requested
        if contact_type in ["executives", "both"]:
            executives_query = """
            SELECT
                e.FIRST_NAME,
                e.LAST_NAME,
                e.PHYSICIAN_LEADER,
                e.BUSINESS_EMAIL,
                e.DIRECT_EMAIL_PRIMARY,
                e.DIRECT_EMAIL_SECONDARY,
                e.MOBILE_PHONE_PRIMARY,
                e.MOBILE_PHONE_SECONDARY,
                e.DEFINITIVE_ID,
                c.physician_group_name,
                e.TITLE,
                e.LINKEDIN_PROFILE
            FROM prod_analytics_global.ad_hoc.us_executive_report e
            JOIN prod_analytics_global.exposure.sales__definitive_physician_companies c
                ON e.DEFINITIVE_ID = c.definitive_id
            WHERE e.DEFINITIVE_ID = ?
            LIMIT ?
            """

            cursor.execute(executives_query, (definitive_id, limit))
            exec_results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            results["executives"] = [dict(zip(columns, row)) for row in exec_results]

        cursor.close()
        conn.close()

        return results
    except Exception as e:
        return {"physicians": [], "executives": [], "error": str(e)}
