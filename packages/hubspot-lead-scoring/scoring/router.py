"""Lead type router — determines which scoring modules to run."""


def classify_lead_type(context):
    """
    Determine lead type from context. Returns one of:
    'inbound', 'product', 'event', 'other'

    Checks Lead properties first (hs_lead_type), then falls back to
    Contact analytics properties and form submissions.

    Lead type is a ROUTER — it decides which modules execute,
    not a score itself.
    """
    lead_props = context.get("lead_properties", {})
    props = context.get("properties", {})
    form_submissions = context.get("form_submissions", [])

    # ─── Check Lead object's own type field first ─────────────────────────
    hs_lead_type = (lead_props.get("hs_lead_type") or "").lower()
    if hs_lead_type:
        if hs_lead_type in ("inbound",):
            return "inbound"
        if hs_lead_type in ("product", "product_qualified"):
            return "product"
        if hs_lead_type in ("event", "conference", "trade_show"):
            return "event"

    # ─── Fall back to form submissions + analytics source ─────────────────
    source = (props.get("hs_analytics_source") or "").lower()
    source_detail = (props.get("hs_analytics_source_data_1") or "").lower()
    latest_source = (props.get("hs_latest_source") or "").lower()

    # Inbound: has a form submission (contact us, demo request, etc.)
    if form_submissions:
        form_titles = [s.get("title", "").lower() for s in form_submissions]
        product_keywords = ["signup", "sign up", "trial", "free trial", "register", "create account"]

        if any(kw in title for title in form_titles for kw in product_keywords):
            return "product"

        return "inbound"

    # Product: signup/trial signals from analytics source
    product_sources = ["product", "app", "signup", "trial"]
    if any(kw in source for kw in product_sources) or any(kw in source_detail for kw in product_sources):
        return "product"

    # Event/conference: trade show, event, conference sources
    event_keywords = ["event", "conference", "trade show", "tradeshow", "webinar", "meetup"]
    all_source_text = f"{source} {source_detail} {latest_source}"
    if any(kw in all_source_text for kw in event_keywords):
        return "event"

    return "other"


def get_modules_for_lead_type(lead_type, context):
    """
    Return list of scoring module names to run for this lead type.
    Message analysis only runs if there's a form submission with a free-text message.
    specialty_company always runs.
    """
    modules = ["opportunity_size", "person_role", "specialty_company"]

    if lead_type == "inbound":
        has_message = _has_analyzable_message(context)
        if has_message:
            modules.append("message_analysis")

    return modules


def _has_analyzable_message(context):
    """Check if the lead/contact has a free-text message worth analyzing."""
    props = context.get("properties", {})

    # Check Lead's form submission message property (highest priority)
    lead_message = (props.get("message__form_submission_") or "").strip()
    if lead_message and len(lead_message) > 10:
        return True

    # Check direct message property
    message = props.get("message") or props.get("hs_content_membership_notes") or ""
    if message.strip() and len(message.strip()) > 10:
        return True

    # Check form submission fields for message-like content
    for submission in context.get("form_submissions", []):
        fields = submission.get("fields", {})
        for key, value in fields.items():
            if any(kw in key.lower() for kw in ["message", "comment", "note", "description", "detail", "inquiry"]):
                if value and len(value.strip()) > 10:
                    return True

    return False


def extract_message_text(context):
    """Extract the best available message text for analysis."""
    props = context.get("properties", {})

    # Lead's form submission message (highest priority)
    lead_message = (props.get("message__form_submission_") or "").strip()
    if lead_message and len(lead_message) > 10:
        return lead_message

    # Direct message property
    message = props.get("message") or props.get("hs_content_membership_notes") or ""
    if message.strip() and len(message.strip()) > 10:
        return message.strip()

    # Form submission fields
    for submission in context.get("form_submissions", []):
        fields = submission.get("fields", {})
        for key, value in fields.items():
            if any(kw in key.lower() for kw in ["message", "comment", "note", "description", "detail", "inquiry"]):
                if value and len(value.strip()) > 10:
                    return value.strip()

    return ""
