"""Message Analysis scoring module.

Only runs when a form submission with free-text message exists.
Sends the message to Claude API for a holistic buying-intent evaluation.
"""

import os
import sys

from .claude_client import analyze_message


def _load_prompt():
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'prompts', 'message_analysis.txt')
    with open(prompt_path, 'r') as f:
        return f.read()


def score(message_text):
    """Analyze message text and return a 0-100 intent score."""
    if not message_text or len(message_text.strip()) < 10:
        print("[message_analysis] No message to analyze, skipping", file=sys.stderr)
        return None

    prompt = _load_prompt()
    result = analyze_message(prompt, message_text)

    if result is None:
        print("[message_analysis] Claude analysis returned None", file=sys.stderr)
        return None

    intent_score = result.get("intent_score")
    if intent_score is None:
        print("[message_analysis] No intent_score in response", file=sys.stderr)
        return None

    final_score = max(0, min(100, int(intent_score)))
    signal = result.get("signal_summary", "")

    print(f"[message_analysis] score={final_score} signal='{signal}'", file=sys.stderr)
    return final_score
