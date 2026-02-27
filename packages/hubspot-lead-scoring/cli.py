"""Manual test CLI — score a single lead from the command line.

Usage:
    python cli.py --lead=<hubspot_lead_id>
    python cli.py --lead=12345 --verbose
"""

import argparse
import json
import sys

from scoring.pipeline import score_lead


def main():
    parser = argparse.ArgumentParser(description="Score a single HubSpot lead")
    parser.add_argument("--lead", required=True, help="HubSpot lead ID to score")
    parser.add_argument("--verbose", action="store_true", help="Include raw inputs in output")
    args = parser.parse_args()

    print(f"Scoring lead {args.lead}...\n", file=sys.stderr)

    result = score_lead(args.lead)

    if result is None:
        print("No result — lead may have been dedup-skipped.", file=sys.stderr)
        sys.exit(1)

    # Human-friendly summary
    print(f"  {result.get('tier_display', 'N/A')}", file=sys.stderr)
    print(f"  {result.get('rationale', '')}\n", file=sys.stderr)

    # Clean output for display
    output = dict(result)
    if not args.verbose:
        output.pop("raw_inputs", None)

    print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
