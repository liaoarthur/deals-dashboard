"""Message Analysis scoring module.

Only runs when a form submission with free-text message exists.
Sends the message to Claude API for structured analysis.
"""

import os
import sys

from .claude_client import analyze_message


def _load_prompt():
    prompt_path = os.path.join(os.path.dirname(__file__), '..', 'prompts', 'message_analysis.txt')
    with open(prompt_path, 'r') as f:
        return f.read()


def score(message_text):
    """Analyze message text and return a 0-100 sub-score."""
    if not message_text or len(message_text.strip()) < 10:
        print("[message_analysis] No message to analyze, skipping", file=sys.stderr)
        return None

    prompt = _load_prompt()
    result = analyze_message(prompt, message_text)

    if result is None:
        print("[message_analysis] Claude analysis returned None", file=sys.stderr)
        return None

    # Compute weighted average of the five dimensions
    dimensions = ["intent_clarity", "urgency", "budget_signals", "product_fit", "specificity"]
    dimension_weights = {
        "intent_clarity": 0.25,
        "urgency": 0.15,
        "budget_signals": 0.20,
        "product_fit": 0.25,
        "specificity": 0.15,
    }

    total = 0
    weight_sum = 0
    for dim in dimensions:
        val = result.get(dim)
        if val is not None:
            w = dimension_weights.get(dim, 0.2)
            total += val * w
            weight_sum += w

    if weight_sum == 0:
        return None

    final_score = int(total / weight_sum)
    final_score = max(0, min(100, final_score))

    print(f"[message_analysis] score={final_score} raw={result}", file=sys.stderr)
    return final_score
