"""HubSpot Lead Scoring Pipeline.

Event-driven lead scoring for HubSpot contacts. Scores contacts 0-100
based on opportunity size, message analysis, and person/role signals.
"""
from dotenv import load_dotenv
load_dotenv()
