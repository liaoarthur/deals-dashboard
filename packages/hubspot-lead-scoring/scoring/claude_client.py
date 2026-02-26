"""Claude API client — shared Anthropic SDK wrapper for scoring modules."""

import os
import json
import sys

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


def lookup_person(name, company):
    """
    Use Claude with web search to look up a person's title/role.
    Returns structured dict with title and seniority info.
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
                    f'Search for "{name}" at "{company}" and find their current job title and role. '
                    f"Return ONLY a JSON object with these fields:\n"
                    f'  "title": their job title (string or null),\n'
                    f'  "seniority": one of "founder", "c_suite", "vp", "director", "manager", "senior", "individual", "unknown",\n'
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
