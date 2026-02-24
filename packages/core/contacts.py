"""Contact deduplication and validation helpers."""

from datetime import datetime


def deduplicate_contacts(existing_contacts, new_contacts):
    """
    Merge new contacts into existing list, deduplicating by email or LinkedIn URL.
    - If a new contact's email OR linkedin matches an existing contact, skip it.
    - Preserves the earlier first_seen date on existing contacts.
    - Adds first_seen = now for genuinely new contacts.
    Returns the merged list.
    """
    # Build lookup sets from existing contacts for fast dedup
    seen_emails = set()
    seen_linkedins = set()
    for c in existing_contacts:
        if c.get('email'):
            seen_emails.add(c['email'].strip().lower())
        if c.get('linkedin'):
            seen_linkedins.add(c['linkedin'].strip().lower())

    merged = list(existing_contacts)  # copy
    now = datetime.utcnow().isoformat()

    for contact in new_contacts:
        email = (contact.get('email') or '').strip().lower()
        linkedin = (contact.get('linkedin') or '').strip().lower()

        # Skip if we already have this contact (by email or linkedin)
        is_duplicate = False
        if email and email in seen_emails:
            is_duplicate = True
        if linkedin and linkedin in seen_linkedins:
            is_duplicate = True

        if not is_duplicate:
            contact['first_seen'] = contact.get('first_seen', now)
            merged.append(contact)
            if email:
                seen_emails.add(email)
            if linkedin:
                seen_linkedins.add(linkedin)

    return merged


def has_valid_contact_info(contact, contact_type="physician"):
    """
    Check if contact has at least one valid piece of contact information.
    Returns True only if contact has LinkedIn URL, direct email, or mobile phone.
    """
    if contact_type == "executive":
        # For executives, require LinkedIn OR direct email OR mobile
        has_linkedin = contact.get("LINKEDIN_PROFILE") and str(contact.get("LINKEDIN_PROFILE")).strip()
        has_direct_email = (
            (contact.get("DIRECT_EMAIL_PRIMARY") and str(contact.get("DIRECT_EMAIL_PRIMARY")).strip()) or
            (contact.get("DIRECT_EMAIL_SECONDARY") and str(contact.get("DIRECT_EMAIL_SECONDARY")).strip())
        )
        has_mobile = (
            (contact.get("MOBILE_PHONE_PRIMARY") and str(contact.get("MOBILE_PHONE_PRIMARY")).strip()) or
            (contact.get("MOBILE_PHONE_SECONDARY") and str(contact.get("MOBILE_PHONE_SECONDARY")).strip())
        )
        return has_linkedin or has_direct_email or has_mobile
    else:
        # For physicians, require direct email OR mobile
        has_direct_email = (
            (contact.get("DIRECT_EMAIL_PRIMARY") and str(contact.get("DIRECT_EMAIL_PRIMARY")).strip()) or
            (contact.get("DIRECT_EMAIL_SECONDARY") and str(contact.get("DIRECT_EMAIL_SECONDARY")).strip())
        )
        has_mobile = (
            (contact.get("MOBILE_PHONE_PRIMARY") and str(contact.get("MOBILE_PHONE_PRIMARY")).strip()) or
            (contact.get("MOBILE_PHONE_SECONDARY") and str(contact.get("MOBILE_PHONE_SECONDARY")).strip())
        )
        return has_direct_email or has_mobile
